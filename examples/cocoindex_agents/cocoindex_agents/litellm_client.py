"""A ``ModelClient`` over LiteLLM.

Rendering rules that matter for incrementality (DESIGN.md §4):

- The system prompt, the task and the input manifest come first, so the
  provider's prompt cache covers them.
- The transcript is rendered as the provider's native tool protocol: an
  assistant message carrying ``tool_calls`` and one ``tool`` message per
  result. Rendered as prose, models re-issue actions whose results they were
  already shown (observed with Claude Sonnet 5.5 on the first real-model
  drive). Call ids are positional (``call_<step>_<index>``): they are never
  stored, so they cannot leak into a memo key.
- The dump is rendered at ``dump.at_step`` and the loop never moves it once
  the model has seen it; diffs and notes render after the step they refer to,
  retry notes included.
- Dump lines mark actions that carry literal payloads (a final answer, a
  ``declare`` whose value is not a handle), since reissuing those repeats old
  content. Arguments are elided; a pick reissues the exact arguments anyway.
  Literal payloads themselves are shown in full: the model is told to review
  them before reuse, and on the first real-model drive a planner that could
  not read its previous plan reworded it, which recomputed everything
  downstream, while a worker reissued an elided ``declare`` it could not
  review and then found it wrong.
- A document a tool result refers to (``documents.py``) is attached as a
  ``user`` message right after the step's ``tool`` messages: a text line
  naming it, then LiteLLM's provider-neutral ``file`` content part with the
  bytes inline as a data URL, which LiteLLM passes to OpenAI as is and turns
  into Anthropic's ``document`` block and Gemini's inline data. A user
  message is the one place all three providers accept a document; it goes
  after every result of the step, because the tool messages must directly
  follow the assistant turn that called them. The bytes are the same every
  run, so the message is byte-identical and the prompt cache keeps it.
"""

from __future__ import annotations

import json
import logging
import pathlib
from datetime import timedelta
from typing import Any

from .documents import Document, Documents
from .handles import is_handle
from .hints import RunHints
from .spec import SessionSpec, ToolSpec
from .trace import Usage
from .transcript import (
    ModelOutput,
    Step,
    ToolCall,
    Transcript,
    canonical_json,
    elide,
)

Message = dict[str, Any]

logger = logging.getLogger(__name__)
# The rendered request of every call, at DEBUG: what the model actually saw,
# for prompt work. Enabled separately because it is large.
messages_logger = logging.getLogger(__name__ + ".messages")

# Bound for one model turn including its transient retries; the run's own
# deadline, when shorter, wins.
_LLM_TIMEOUT = timedelta(minutes=10)


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
        messages.extend(_step_messages(i, step, hints.documents))
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


def _step_messages(i: int, step: Step, documents: Documents) -> list[Message]:
    """One step as the provider sees it: the model's turn, one ``tool`` message per
    result, then the documents the results refer to."""
    if step.output.is_finish:
        return [{"role": "assistant", "content": step.output.answer}]
    ids = [f"call_{i}_{j}" for j in range(len(step.output.calls))]
    turn: Message = {
        "role": "assistant",
        "content": None,
        "tool_calls": [
            {
                "id": call_id,
                "type": "function",
                "function": {"name": c.tool, "arguments": canonical_json(c.args)},
            }
            for call_id, c in zip(ids, step.output.calls)
        ],
    }
    results: list[Message] = [
        {
            "role": "tool",
            "tool_call_id": call_id,
            "content": canonical_json({"ok": r.ok, "value": r.value}),
        }
        for call_id, r in zip(ids, step.results)
    ]
    attached = [
        _document_message(r.tool, doc)
        for r in step.results
        if r.ok
        for doc in documents.referenced_by(r.value)
    ]
    return [turn, *results, *attached]


def _document_message(tool: str, doc: Document) -> Message:
    return {
        "role": "user",
        "content": [
            {
                "type": "text",
                "text": f"The document {doc.key} ({doc.media_type}, {len(doc.data)} bytes) "
                f"that the {tool} result above refers to:",
            },
            {
                "type": "file",
                "file": {
                    "filename": pathlib.PurePosixPath(doc.key).name,
                    "file_data": doc.data_url(),
                },
            },
        ],
    }


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
            body = "\n".join(
                f"      {line}" for line in (out.answer or "").splitlines()
            )
            lines.append(f"  [{n}] finish:{mark}\n{body}".rstrip())
        else:
            calls = "; ".join(f"{c.tool}({_dump_args(c)})" for c in out.calls)
            lines.append(f"  [{n}] {calls}{mark}")
    lines.append(
        "Call replay(from_step=N) to reissue [N] and the following until a result differs; "
        "replay(from_step=N, steps=K) for at most K; or act on your own."
    )
    return "\n".join(lines)


def has_literal_payload(out: ModelOutput) -> bool:
    """A final answer, or a declare whose value is text rather than a handle."""
    return out.is_finish or any(_declares_literal(c) for c in out.calls)


def _declares_literal(call: ToolCall) -> bool:
    return call.tool == "declare" and not is_handle(call.args.get("value", ""))


def _dump_args(call: ToolCall) -> str:
    """Elided, except a literal payload: the model is told to review it before reuse."""
    text = canonical_json(call.args)
    return text if _declares_literal(call) else elide(text)


def _describe_message(m: Message) -> str:
    if "tool_calls" in m:
        calls = "; ".join(
            f"{tc['function']['name']}({tc['function']['arguments']})"
            for tc in m["tool_calls"]
        )
        return f"--- assistant (tool calls)\n{calls}"
    content = m["content"]
    if isinstance(content, list):
        # Content parts: text as is, a file by name and size rather than its data URL.
        content = "\n".join(
            part["text"]
            if part["type"] == "text"
            else f"<file {part['file']['filename']}: {len(part['file']['file_data'])} chars>"
            for part in content
        )
    return f"--- {m['role']}\n{content}"


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

    The model is ``spec.role.model``, which ``AgentsConfig.role()`` has made
    concrete, so the model identity is part of the turn's memo key. Arguments
    that are not a JSON object are passed through as is; the registry and the
    replay resolver report them to the model as errors.

    ``usage`` sums the tokens of every call made through this client, as the
    provider reported them (the prompt-cache share included); it becomes
    ``None`` for good once a response carries no usage, so an unknown is
    never shown as a zero. Calls are counted by the session traces, not here.
    """

    def __init__(self) -> None:
        self.usage: Usage | None = Usage()

    async def complete(
        self,
        spec: SessionSpec,
        tools: tuple[ToolSpec, ...],
        prefix: Transcript,
        hints: RunHints,
    ) -> ModelOutput:
        import litellm  # heavy optional dependency; imported where used

        # The engine's LiteLLM retry: transient provider errors (429, 5xx,
        # transport resets) are retried with backoff inside a deadline that
        # min-nests with the run's; the model turn never sees them. Private
        # today; promote it when this package leaves examples/.
        from cocoindex.ops.litellm import _retry_litellm_call

        logger.info(
            "model call: role=%s step=%d model=%s",
            spec.role.name,
            len(prefix),
            spec.role.model,
        )
        messages = render_messages(spec, prefix, hints)
        if messages_logger.isEnabledFor(logging.DEBUG):
            messages_logger.debug(
                "%s step %d request:\n%s",
                spec.role.name,
                len(prefix),
                "\n".join(_describe_message(m) for m in messages),
            )
        response = await _retry_litellm_call(
            lambda: litellm.acompletion(
                model=spec.role.model,
                messages=messages,
                tools=tools_as_openai(tools),
                tool_choice="auto",
                # Determinism where the provider offers it; dropped where it
                # does not (Claude 5.5 accepts only its default temperature).
                temperature=0,
                drop_params=True,
            ),
            "cocoindex_agents.llm_step",
            _LLM_TIMEOUT,
        )
        self._account(getattr(response, "usage", None))
        message = response.choices[0].message
        tool_calls = getattr(message, "tool_calls", None) or []
        return parse_response(
            message.content,
            [(tc.function.name, tc.function.arguments) for tc in tool_calls],
        )

    def _account(self, usage: Any) -> None:
        if self.usage is None:
            return
        if usage is None or getattr(usage, "prompt_tokens", None) is None:
            self.usage = None  # unknown, and unknown stays unknown
            return
        details = getattr(usage, "prompt_tokens_details", None)
        self.usage += Usage(
            input_tokens=usage.prompt_tokens,
            output_tokens=getattr(usage, "completion_tokens", 0) or 0,
            cached_input_tokens=getattr(details, "cached_tokens", 0) or 0,
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
