"""A ``ModelClient`` over LiteLLM. Sketch: rendering is real, the API call is untested.

Rendering rules that matter for incrementality (DESIGN.md §4):

- The system prompt, the task and the input manifest come first, so the
  provider's prompt cache covers them.
- The dump is rendered at ``dump.at_step`` and the loop never moves it once
  the model has seen it; diffs and notes render after the step they refer to,
  retry notes included.
- Dump lines mark actions that carry literal payloads (a final answer, a
  ``declare`` whose value is not a handle), since reissuing those repeats old
  content. Arguments are elided; a pick reissues the exact arguments anyway.
"""

from __future__ import annotations

import json
from typing import Any

from .handles import is_handle
from .hints import RunHints
from .spec import SessionSpec, ToolSpec
from .transcript import ModelOutput, ToolCall, Transcript, canonical_json

Message = dict[str, Any]

_ELIDE_CHARS = 160


def render_task(spec: SessionSpec) -> str:
    if not spec.inputs:
        return spec.task
    lines = [spec.task, "", "Inputs available to you (use their handle names):"]
    for card in spec.inputs:
        lines.append(f"  {card.handle}  ({card.kind}): {', '.join(card.methods)}")
    return "\n".join(lines)


def render_messages(
    spec: SessionSpec, prefix: Transcript, hints: RunHints
) -> list[Message]:
    messages: list[Message] = [
        {"role": "system", "content": spec.role.system_prompt},
        {"role": "user", "content": render_task(spec)},
    ]
    notes_at = {n.at_step for n in hints.notes}
    for i, step in enumerate(prefix.steps):
        if hints.dump is not None and hints.dump.at_step == i:
            messages.append({"role": "user", "content": render_dump(hints)})
        if i in notes_at:
            messages.extend(_notes(hints, i))
        messages.append(
            {"role": "assistant", "content": canonical_json(_output_json(step.output))}
        )
        if step.results:
            messages.append(
                {
                    "role": "user",
                    "content": canonical_json(
                        [
                            {"tool": r.tool, "ok": r.ok, "value": r.value}
                            for r in step.results
                        ]
                    ),
                }
            )
        for d in hints.diffs:
            if d.at_step == i:
                messages.append(
                    {
                        "role": "user",
                        "content": "[replay] result differs from last run for "
                        f"{d.call.tool}({canonical_json(d.call.args)}):\n"
                        f"  before: {canonical_json(d.before)}\n"
                        f"  after:  {canonical_json(d.after)}",
                    }
                )
    end = len(prefix.steps)
    if hints.dump is not None and hints.dump.at_step == end:
        messages.append({"role": "user", "content": render_dump(hints)})
    if end in notes_at:
        messages.extend(_notes(hints, end))
    return messages


def _notes(hints: RunHints, at_step: int) -> list[Message]:
    return [
        {"role": "user", "content": f"[replay] {n.text}"}
        for n in hints.notes
        if n.at_step == at_step
    ]


def render_dump(hints: RunHints) -> str:
    assert hints.dump is not None
    lines = ["[replay] Last run continued from here with:"]
    for j, out in enumerate(hints.dump.outputs):
        n = hints.dump.first_step + j
        mark = (
            "    <- literal content; review before reuse"
            if has_literal_payload(out)
            else ""
        )
        if out.is_finish:
            lines.append(f"  [{n}] finish: {_elide(out.answer or '')}{mark}")
        else:
            calls = "; ".join(
                f"{c.tool}({_elide(canonical_json(c.args))})" for c in out.calls
            )
            lines.append(f"  [{n}] {calls}{mark}")
    lines.append(
        "Call replay(from_step=N) to reissue [N] and the following until a result differs; "
        "replay(from_step=N, steps=K) for at most K; or act on your own."
    )
    return "\n".join(lines)


def has_literal_payload(out: ModelOutput) -> bool:
    """A final answer, or a declare whose value is text rather than a handle."""
    if out.is_finish:
        return True
    return any(
        c.tool == "declare" and not is_handle(c.args.get("value", ""))
        for c in out.calls
    )


def _elide(s: str, n: int = _ELIDE_CHARS) -> str:
    return s if len(s) <= n else s[: n - 1] + "…"


def _output_json(out: ModelOutput) -> Any:
    if out.is_finish:
        return {"answer": out.answer}
    return {"calls": [{"tool": c.tool, "args": c.args} for c in out.calls]}


def tools_as_openai(tools: tuple[ToolSpec, ...]) -> list[dict[str, Any]]:
    return [
        {
            "type": "function",
            "function": {
                "name": t.name,
                "description": t.description,
                "parameters": t.parameters,
            },
        }
        for t in tools
    ]


class LiteLLMClient:
    """Calls ``litellm.acompletion`` with the rendered messages and tool definitions.

    The model is ``spec.role.model``, which ``HarnessConfig.role()`` has made
    concrete, so the model identity is part of the turn's memo key. Arguments
    that are not a JSON object are passed through as is; the registry and the
    replay resolver report them to the model as errors.
    """

    async def complete(
        self,
        spec: SessionSpec,
        tools: tuple[ToolSpec, ...],
        prefix: Transcript,
        hints: RunHints,
    ) -> ModelOutput:
        import litellm  # heavy optional dependency; imported where used

        response = await litellm.acompletion(
            model=spec.role.model,
            messages=render_messages(spec, prefix, hints),
            tools=tools_as_openai(tools),
            tool_choice="auto",
            temperature=0,
        )
        message = response.choices[0].message
        tool_calls = getattr(message, "tool_calls", None) or []
        return parse_response(
            message.content,
            [(tc.function.name, tc.function.arguments) for tc in tool_calls],
        )


def parse_response(
    content: str | None, tool_calls: list[tuple[str, str | None]]
) -> ModelOutput:
    """Turn a provider message into a ``ModelOutput``.

    ``tool_calls`` are ``(name, raw JSON arguments)`` pairs. Arguments that are
    not valid JSON are kept as the raw string: ``canonical_output`` then stores
    the call as ``{"$invalid": …}`` and the registry reports it to the model as
    its error, instead of the session failing on a decode exception.
    """
    if not tool_calls:
        return ModelOutput(answer=content or "")
    calls = []
    for name, raw in tool_calls:
        try:
            args = json.loads(raw or "{}")
        except json.JSONDecodeError:
            args = raw
        calls.append(ToolCall(name, args))
    return ModelOutput(calls=tuple(calls))
