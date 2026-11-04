"""The canonical transcript: the control plane of a session.

A transcript is a sequence of steps, each one model output plus the results of
the tool calls it made. It is the memo key of every model turn (as a prefix)
and the value stored in ``use_state("transcript")`` for the next run. Nothing
run-specific enters it: no hints, no timing, no replay bookkeeping. CocoIndex
fingerprints every dataclass field, so a field here is a field in the key.
"""

from __future__ import annotations

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


def same_action(a: ModelOutput, b: ModelOutput) -> bool:
    """Action identity: the whole turn — same calls in the same order, same answer.

    Identity is per turn, not per call: a turn whose parallel batch differs in
    one call is a different action and gets no result comparison.
    """
    return a.calls == b.calls and a.answer == b.answer


def canonical_json(value: Json) -> str:
    """Deterministic JSON: sorted keys, no whitespace, non-ASCII kept as is."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


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
