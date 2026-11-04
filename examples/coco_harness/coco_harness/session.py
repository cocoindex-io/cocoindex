"""The agent loop: a session is a component; each turn is a memoized child component.

Three entry points share one loop:

- ``run_open_session(spec, handles)`` — not memoized. The body re-runs every
  time; its turns replay from their per-turn memos and its tools re-execute.
- ``run_closed_session(item, spec, handles)`` — memoized on ``(item, spec,
  handles)``. A worker over one file: unchanged inputs → skipped entirely,
  children and target states carried over. Its role may only have
  ``closed_ok`` tools and ``handles`` only ``closed_input_ok`` objects (the
  mounting tool checks; the loop checks again).
- ``run_reduce_session(members, spec)`` — memoized on the members' contents.

The replay contract (DESIGN.md §4) as the loop implements it:

* ``prev`` is the transcript stored by this component's last *successful*
  run. ``cursor`` is the index in ``prev`` of the step expected next; it
  starts at 0 and follows alignment.
* Every turn is ``use_mount(turn/k, llm_step, ...)``. ``TurnScratch`` (inside
  ``hints``, excluded from the key) tells the loop what happened: nothing
  (memo hit), inference, a harness replay, or a pick with its step number.
* Until the first inference, the dump passed is "``prev`` from ``cursor``
  on", positioned at the current turn. It costs nothing on a hit. At the
  first inference it is frozen where the model saw it and never moves again
  this run; a changed result freezes it one position later (the next turn
  will infer).
* Alignment, in order: a pick → exactly the picked step; a harness replay
  that ran, or a hit that returned the same action → the replayed step;
  otherwise ``prev[cursor]`` if it is the same action; otherwise the first
  same action at or after ``cursor`` (advisory). Aligned steps set ``cursor``
  past themselves and have their results compared; differences become diffs.
* A requested replay can be answered by an existing memo instead (an entry a
  failed run committed for this exact prefix). That entry is the model's own
  earlier decision for this state and wins: the loop executes it, re-aligns
  it like any composed action, stops fast-forwarding, and notes the
  interruption for the model.
* Fast-forward: after a pick (or a replay that ran and matched), the next
  step of ``prev`` is replayed through ``llm_step`` with ``hints.replay`` — so
  a memo entry is still written — until a result differs, the pick's
  ``steps`` budget runs out, or the next step is a final answer (then a note
  asks the model to confirm).
* Stored: ``(output, results)`` pairs only.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import replace

import cocoindex as coco

from .handles import HandleObject, HandleTable
from .hints import Diff, Dump, Note, Replay, RunHints, TurnScratch, replay_tool_spec
from .replay import find_same_action, llm_step
from .spec import Item, SessionInput, SessionResult, SessionSpec
from .tools import HARNESS, SessionContext, ToolRegistry
from .transcript import ModelOutput, Step, ToolCall, ToolResult, Transcript, same_action


@coco.fn
async def run_open_session(
    spec: SessionSpec, handles: Mapping[str, HandleObject] | None = None
) -> SessionResult:
    return await _session_loop(spec, None, handles or {}, closed=False)


@coco.fn(memo=True)
async def run_closed_session(
    item: Item,
    spec: SessionSpec,
    handles: Mapping[str, HandleObject] | None = None,
) -> SessionResult:
    return await _session_loop(spec, item, handles or {}, closed=True)


@coco.fn(memo=True)
async def run_reduce_session(
    members: tuple[str, ...], spec: SessionSpec
) -> SessionResult:
    return await _session_loop(spec, members, {}, closed=True)


@coco.fn
async def _session_loop(
    spec: SessionSpec,
    input: SessionInput,
    handles: Mapping[str, HandleObject],
    *,
    closed: bool,
) -> SessionResult:
    config = coco.use_context(HARNESS)
    registry: ToolRegistry = config.tools.restrict(spec.role.tool_names)
    _check_spec(spec, registry, handles, closed=closed)
    tools = (*spec.tools, replay_tool_spec())
    table = HandleTable()
    for name, obj in handles.items():
        table.seed(name, obj)
    ctx = SessionContext(spec=spec, input=input, handles=table, config=config)

    prev_handle = coco.use_state(
        "transcript", initial_value=Transcript(), type_hint=Transcript
    )
    prev: Transcript = prev_handle.value

    steps: list[Step] = []
    hints = RunHints()
    dump_frozen = False
    cursor = 0
    ff_next: int | None = None  # index in prev to replay next turn
    ff_budget: int | None = None  # replays left after a bounded pick; None = unbounded

    for k in range(spec.role.max_turns):
        prefix = Transcript(tuple(steps))
        if not dump_frozen:
            hints = replace(hints, dump=_dump_from(prev, cursor, at_step=k))
        scratch = TurnScratch()
        replay = Replay(prev.steps[ff_next].output) if ff_next is not None else None
        output: ModelOutput = await coco.use_mount(
            coco.component_subpath("turn", k),
            llm_step,
            spec,
            tools,
            prefix,
            replace(hints, scratch=scratch, replay=replay),
        )
        requested, ff_next = ff_next, None
        for note in scratch.notes:
            hints = hints.add_note(note)
        if scratch.inferred:
            dump_frozen = True

        # Requested provenance is not actual provenance: a memo hit may have
        # answered instead of the replay body, with a different action.
        replayed: int | None = None
        if requested is not None:
            if scratch.replayed or same_action(output, prev.steps[requested].output):
                replayed = requested
            else:
                hints = hints.add_note(
                    Note(
                        k + 1,
                        f"fast-forward interrupted at [{requested}]: this session had already "
                        "decided a different action for this exact state, and it was executed",
                    )
                )

        if output.answer is not None:
            steps.append(Step(output, ()))
            prev_handle.value = Transcript(tuple(steps))
            return SessionResult(answer=output.answer, num_steps=len(steps))

        results = await _execute_all(registry, ctx, output)
        steps.append(Step(output, results))

        if scratch.picked_step is not None:
            aligned: int | None = scratch.picked_step
        elif replayed is not None:
            aligned = replayed
        elif cursor < len(prev) and same_action(prev.steps[cursor].output, output):
            aligned = cursor
        else:
            aligned = find_same_action(prev, output, cursor)

        diffs: list[Diff] = []
        if aligned is not None:
            cursor = aligned + 1
            diffs = _diffs(k, output, prev.steps[aligned].results, results)
            for d in diffs:
                hints = hints.add_diff(d)
        if diffs and not dump_frozen:
            dump_frozen = True
            hints = replace(hints, dump=_dump_from(prev, cursor, at_step=k + 1))

        if scratch.picked_step is not None:
            ff_budget = None if scratch.pick_steps is None else scratch.pick_steps - 1
        if (
            aligned is not None
            and not diffs
            and (scratch.picked_step is not None or replayed is not None)
            and (ff_budget is None or ff_budget > 0)
        ):
            nxt = aligned + 1
            if nxt < len(prev):
                if prev.steps[nxt].output.answer is not None:
                    hints = hints.add_note(
                        Note(
                            k + 1,
                            f"fast-forward reached last run's final answer [{nxt}]; "
                            "confirm or revise",
                        )
                    )
                else:
                    ff_next = nxt
                    if ff_budget is not None:
                        ff_budget -= 1

    raise RuntimeError(
        f"session {spec.role.name!r} exceeded {spec.role.max_turns} turns"
    )


def _check_spec(
    spec: SessionSpec,
    registry: ToolRegistry,
    handles: Mapping[str, HandleObject],
    *,
    closed: bool,
) -> None:
    """Programming errors, not model errors: raise so the component fails loudly."""
    if spec.role.model == "default":
        raise ValueError(
            "SessionSpec has an unresolved model; build specs with HarnessConfig.spec()"
        )
    if spec.tools != registry.specs():
        raise ValueError(
            "SessionSpec.tools does not match the registry; build specs with HarnessConfig.spec()"
        )
    if closed:
        open_tools = [t.spec.name for t in registry.tools() if not t.closed_ok]
        if open_tools:
            raise ValueError(
                f"closed session role {spec.role.name!r} has tools that read beyond its "
                f"inputs: {open_tools}; mount it as an open session"
            )
        bad = [h for h, obj in handles.items() if not obj.closed_input_ok]
        if bad:
            raise ValueError(f"closed session received open inputs: {bad}")


def _dump_from(prev: Transcript, start: int, *, at_step: int) -> Dump | None:
    if start >= len(prev):
        return None
    return Dump(
        at_step=at_step,
        first_step=start,
        outputs=tuple(s.output for s in prev.steps[start:]),
    )


async def _execute_all(
    registry: ToolRegistry, ctx: SessionContext, output: ModelOutput
) -> tuple[ToolResult, ...]:
    """Run one turn's calls concurrently against a snapshot of the handle table.

    Results keep call order. A call cannot see a handle a sibling registers in
    the same turn, and two siblings registering the same name both fail. Two
    calls that would mount children under the same key both fail before
    anything runs. Every call is drained before an infrastructure failure
    propagates.
    """
    calls = output.calls
    results: list[ToolResult | None] = [None] * len(calls)
    for i in _duplicate_keys(registry, calls):
        results[i] = ToolResult(
            calls[i].tool,
            False,
            "key used by another call in this turn; keys must be unique",
        )
    pending = [i for i in range(len(calls)) if results[i] is None]
    forks = {i: ctx.handles.fork() for i in pending}
    outcomes = await asyncio.gather(
        *(registry.execute(replace(ctx, handles=forks[i]), calls[i]) for i in pending),
        return_exceptions=True,
    )
    for outcome in outcomes:
        if isinstance(outcome, BaseException):
            raise outcome
    for i, outcome in zip(pending, outcomes):
        assert isinstance(outcome, ToolResult)
        results[i] = outcome
    conflicts = ctx.handles.merge([forks[i] for i in pending])
    for j in conflicts:
        i = pending[j]
        results[i] = ToolResult(
            calls[i].tool,
            False,
            "handle name registered twice in one turn; give each call its own name",
        )
    return tuple(r for r in results if r is not None)


def _duplicate_keys(registry: ToolRegistry, calls: tuple[ToolCall, ...]) -> set[int]:
    by_key: dict[str, list[int]] = {}
    for i, call in enumerate(calls):
        t = registry.get(call.tool)
        if t is None or t.key_arg is None or not isinstance(call.args, dict):
            continue
        key = call.args.get(t.key_arg)
        if isinstance(key, str):
            by_key.setdefault(key, []).append(i)
    return {i for idxs in by_key.values() if len(idxs) > 1 for i in idxs}


def _diffs(
    step: int,
    output: ModelOutput,
    before: tuple[ToolResult, ...],
    after: tuple[ToolResult, ...],
) -> list[Diff]:
    out: list[Diff] = []
    for call, b, a in zip(output.calls, before, after):
        if b != a:
            out.append(Diff(step, call, b.value, a.value))
    if len(before) != len(after):
        out.append(Diff(step, output.calls[0], len(before), len(after)))
    return out
