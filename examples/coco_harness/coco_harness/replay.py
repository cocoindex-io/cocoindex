"""The memoized model turn, and the replay meta tool it resolves.

``llm_step`` is mounted once per turn as a component
(``use_mount(component_subpath("turn", k), llm_step, spec, tools, prefix, hints)``):

- The memo key is ``(spec, tools, prefix)``. ``hints`` is excluded.
- A component per turn (rather than an inline memoized call) means each turn
  commits on its own, so a session that fails at turn 40 keeps turns 0–39
  (DESIGN.md §7).
- When ``hints.replay`` is set, the body returns that output with no model
  call — and the memo entry is still written, which is what lets the next run
  hit on fast-forwarded turns. The loop checks ``scratch.replayed`` to tell
  that from a memo hit.
- A ``replay(from_step, steps?)`` meta call is resolved here into the picked
  action, so the memo and the transcript only ever hold real actions. The
  pick's provenance goes back to the loop through ``hints.scratch``.
"""

from __future__ import annotations

from dataclasses import dataclass

import cocoindex as coco

from .hints import REPLAY_TOOL, Dump, Note, RunHints
from .model import MODEL_CLIENT
from .spec import SessionSpec, ToolSpec
from .transcript import ModelOutput, Transcript, canonical_output, same_action

# An unresolvable pick is shown back to the model once; a second one fails the turn.
_MAX_PICK_ATTEMPTS = 2


@dataclass(frozen=True)
class Resolution:
    output: ModelOutput
    picked_step: int | None = None
    steps: int | None = None
    invalid: str | None = None


def resolve_replay(output: ModelOutput, dump: Dump | None) -> Resolution:
    """Turn a ``replay(from_step, steps?)`` meta call into the picked action.

    Returns the picked output as if the model had issued it, plus the step
    number it came from (the loop aligns and continues from exactly that
    occurrence). Anything malformed — including arguments that are not an
    object — is reported in ``invalid`` and the model is asked again; a meta
    call never reaches the transcript.
    """
    if not any(c.tool == REPLAY_TOOL for c in output.calls):
        return Resolution(output)
    if len(output.calls) != 1:
        return Resolution(output, invalid="replay() must be the only call in its turn")
    args = output.calls[0].args
    if not isinstance(args, dict):
        return Resolution(output, invalid="replay() arguments must be a JSON object")
    from_step = args.get("from_step")
    if isinstance(from_step, bool) or not isinstance(from_step, int):
        return Resolution(output, invalid="from_step must be an integer")
    if dump is None:
        return Resolution(output, invalid="there is no [replay] block in this session")
    index = dump.index_of_step(from_step)
    if index is None:
        return Resolution(
            output,
            invalid=f"from_step {from_step} is outside [{dump.first_step}, {dump.last_step}]",
        )
    steps = args.get("steps")
    if steps is not None and (
        isinstance(steps, bool) or not isinstance(steps, int) or steps < 1
    ):
        return Resolution(output, invalid="steps must be an integer >= 1")
    return Resolution(output=dump.outputs[index], picked_step=from_step, steps=steps)


@coco.fn(memo=True, memo_key={"hints": None})
async def llm_step(
    spec: SessionSpec,
    tools: tuple[ToolSpec, ...],
    prefix: Transcript,
    hints: RunHints,
) -> ModelOutput:
    scratch = hints.scratch
    if hints.replay is not None:
        scratch.replayed = True
        return hints.replay.output

    client = coco.use_context(MODEL_CLIENT)
    attempt_hints = hints
    last_invalid = ""
    for _ in range(_MAX_PICK_ATTEMPTS):
        output = canonical_output(
            await client.complete(spec, tools, prefix, attempt_hints)
        )
        scratch.inferred = True
        resolution = resolve_replay(output, hints.dump)
        if resolution.invalid is None:
            if resolution.picked_step is not None:
                scratch.picked_step = resolution.picked_step
                scratch.pick_steps = resolution.steps
            return resolution.output
        last_invalid = resolution.invalid
        note = Note(len(prefix), f"invalid replay call ignored: {resolution.invalid}")
        scratch.notes.append(note)  # the loop keeps it in the rendered history
        attempt_hints = attempt_hints.add_note(note)
    raise ValueError(
        f"the model issued an unresolvable replay call {_MAX_PICK_ATTEMPTS} times: {last_invalid}"
    )


def find_same_action(
    transcript: Transcript, output: ModelOutput, start: int
) -> int | None:
    """Index of the first step at or after ``start`` whose output is the same action.

    Best-effort alignment for actions the model composed itself after a
    divergence (picks carry exact provenance and do not use this). With
    repeated identical actions it pairs the first unmatched occurrence, so
    the diff it enables is advisory.
    """
    for i in range(max(start, 0), len(transcript.steps)):
        if same_action(transcript.steps[i].output, output):
            return i
    return None
