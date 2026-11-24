"""The canonical transcript: the control plane of a session.

A transcript is a sequence of steps, each one model output plus the results of
the tool calls it made. It is the memo key of every model turn (as a prefix)
and the value stored in ``use_state("transcript")`` for the next run. Nothing
run-specific enters it: no hints, no timing, no replay bookkeeping. CocoIndex
fingerprints every dataclass field, so a field here is a field in the key.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from .spec import Json


@dataclass(frozen=True)
class ToolCall:
    tool: str
    args: dict[str, Json]


@dataclass(frozen=True)
class ModelOutput:
    """One model turn: tool calls (possibly several, run in parallel) or a final answer."""

    calls: tuple[ToolCall, ...] = ()
    answer: str | None = None

    @property
    def is_finish(self) -> bool:
        return self.answer is not None


@dataclass(frozen=True)
class ToolResult:
    tool: str
    ok: bool
    value: Json


@dataclass(frozen=True)
class Step:
    output: ModelOutput
    results: tuple[ToolResult, ...]


@dataclass(frozen=True)
class Transcript:
    steps: tuple[Step, ...] = ()

    def __len__(self) -> int:
        return len(self.steps)


# Arguments are cut to this many characters in dumps and logs; a pick reissues
# the exact arguments anyway.
ELIDE_CHARS = 160


def describe_output(out: ModelOutput) -> str:
    """One line for a turn: ``finish: <answer>`` or the calls, arguments elided."""
    if out.is_finish:
        return f"finish: {elide(out.answer or '')}"
    return "; ".join(f"{c.tool}({elide(canonical_json(c.args))})" for c in out.calls)


def elide(s: str, n: int = ELIDE_CHARS) -> str:
    return s if len(s) <= n else s[: n - 1] + "…"


# A child's task is quoted in the plan to this many characters.
_TASK_CHARS = 100


def describe_action(call: ToolCall) -> str:
    """One line for a tool call in the plan: the tool and what it names, never its payload."""
    a = call.args if isinstance(call.args, dict) else {}
    tool = call.tool
    if tool in ("open_source", "open_target") and "name" in a:
        prefix = "src" if tool == "open_source" else "tgt"
        return f"{tool} {prefix}:{a['name']} {a.get('path', '')}".rstrip()
    if tool in ("observe", "call_agent", "start_agent") and "key" in a:
        inputs = ", ".join(str(h) for h in a.get("inputs", []))
        return f"{tool} {a['key']}" + (f" ← {inputs}" if inputs else "")
    if tool == "for_each" and "key" in a:
        inputs = ", ".join(str(h) for h in a.get("inputs", []))
        return f"for_each {a['key']}: {a.get('source', '?')}" + (
            f" → {inputs}" if inputs else ""
        )
    if tool == "reduce" and "key" in a:
        return f"reduce {a['key']}: {a.get('collection', '?')}"
    if tool == "declare" and "key" in a:
        value = a.get("value")
        ref = (
            f" ← {value}" if isinstance(value, str) and value.startswith("res:") else ""
        )
        return f"declare {a.get('target', '?')} {a['key']}{ref}"
    if tool == "call" and "handle" in a:
        return f"call {a['handle']} {a.get('method', '?')}"
    if not a:
        return tool
    return f"{tool}({elide(canonical_json(a), 60)})"


def task_of(call: ToolCall) -> str | None:
    """A child's task or question, elided for the plan."""
    if not isinstance(call.args, dict):
        return None
    task = call.args.get("task") or call.args.get("question")
    return elide(" ".join(task.split()), _TASK_CHARS) if isinstance(task, str) else None


def same_action(a: ModelOutput, b: ModelOutput) -> bool:
    """Action identity: the whole turn — same calls in the same order, same answer.

    Identity is per turn, not per call: a turn whose parallel batch differs in
    one call is a different action and gets no result comparison.
    """
    return a.calls == b.calls and a.answer == b.answer


def canonical_json(value: Json) -> str:
    """Deterministic JSON: sorted keys, no whitespace, non-ASCII kept as is."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def digest_json(value: Json) -> str:
    """A hex digest of the canonical form, for comparing a value across runs."""
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def normalize_json(value: Json) -> Json:
    """Round-trip through JSON so stored and in-memory values compare equal.

    Tool results are compared across runs after a serialization round trip
    (tuples become lists, ints stay ints, …). Normalizing at the boundary means
    equality in memory agrees with equality after storage.
    """
    return json.loads(canonical_json(value))


# A call whose arguments were not a JSON object is kept in canonical form as
# {INVALID_ARGS_KEY: <the raw arguments>}: the transcript and the turn memo
# stay dict-typed (they are decoded with that type on the next run), the
# original input is preserved, and the registry reports it as the model's error.
INVALID_ARGS_KEY = "$invalid"


def canonical_output(output: ModelOutput) -> ModelOutput:
    """Make a model's raw output storable: every call's arguments become a dict."""
    calls = tuple(
        c
        if isinstance(c.args, dict)
        else ToolCall(c.tool, {INVALID_ARGS_KEY: normalize_json(c.args)})
        for c in output.calls
    )
    return (
        ModelOutput(calls=calls, answer=output.answer)
        if calls != output.calls
        else output
    )
