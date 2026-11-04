"""The agent loop: a session is a memoized component; each turn is a memoized child component.

Three entry points share one loop:

- ``run_open_session(spec, handles)`` — memoized on ``(spec, handles)`` plus
  what the run **read**: a tool that reads beyond the session's arguments (a
  source method, a ``for_each`` walk) records the read with
  ``coco.use_data_dep``, and a contained failure is recorded as a read that
  never validates. Unchanged spec, handles and reads → skipped entirely, turns
  and children carried over; otherwise the body re-runs, its turns replay from
  their per-turn memos and its tools re-execute. A session whose child
  recorded a read re-runs its body too (the engine's ``cannot_skip``), while
  that child is skipped on its own entry.
- ``run_closed_session(item, spec, handles)`` — memoized on ``(item, spec,
  handles)``. A worker over one file: unchanged inputs → skipped entirely,
  children and target states carried over. Its role may only have
  ``closed_ok`` tools and ``handles`` only ``closed_input_ok`` objects (the
  mounting tool checks; the loop checks again), so it records nothing and
  never flags the session that fanned it out.
- ``run_reduce_session(members, spec)`` — memoized on the members' contents.

The replay contract (DESIGN.md §4) lives in ``_ReplayState``; the loop only
sequences turns. Failures (DESIGN.md §7): in an open session an environment
error from a tool becomes a result the model reads, because the tool runs
again next run and a changed outcome is an ordinary divergence; in a closed
session it fails the component, because a memoized transcript must not carry
a fact about the environment. Deadline expiry and cancellation always pass
through.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from dataclasses import dataclass, field, replace

import cocoindex as coco

from .errors import record_contained_failure
from .handles import HandleObject, HandleTable
from .hints import Diff, Dump, Note, Replay, RunHints, TurnScratch, replay_tool_spec
from .replay import find_same_action, llm_step
from .spec import Item, SessionInput, SessionResult, SessionSpec
from .tools import HARNESS, SessionContext, ToolRegistry
from .transcript import ModelOutput, Step, ToolCall, ToolResult, Transcript, same_action

logger = logging.getLogger(__name__)


@coco.fn(memo=True)
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


@dataclass
class _ReplayState:
    """Where this run stands against ``prev``, the last successful transcript.

    * ``cursor``: index in ``prev`` of the step expected next; follows alignment.
    * Until the first inference, the dump passed to a turn is "``prev`` from
      ``cursor`` on", positioned at the current turn: unused on a hit, what
      the model needs on a miss. The first inference freezes it where the
      model saw it; a changed result freezes it one position later.
    * Alignment, in order: a pick → the picked step; a harness replay that
      ran, or a hit that returned the same action → the replayed step;
      ``prev[cursor]`` if it is the same action; else the first same action at
      or after ``cursor`` (advisory). An aligned step moves ``cursor`` past
      itself and has its results compared; differences become diffs.
    * A requested replay answered by an existing memo with a different action
      is the session's own earlier decision and wins: it is executed, aligned
      like a composed action, fast-forward stops, and the model is told.
    * Fast-forward continues after a pick or a matching replay until a result
      differs, the pick's ``steps`` budget runs out, or the next step is a
      final answer (then a note asks the model to confirm).
    """

    prev: Transcript
    hints: RunHints = field(default_factory=RunHints)
    cursor: int = 0
    dump_frozen: bool = False
    ff_next: int | None = None  # index in prev to replay next turn
    ff_budget: int | None = None  # replays left after a bounded pick; None = unbounded

    def turn_hints(self, k: int) -> tuple[RunHints, TurnScratch]:
        if not self.dump_frozen:
            self.hints = replace(
                self.hints, dump=_dump_from(self.prev, self.cursor, at_step=k)
            )
        scratch = TurnScratch()
        replay = (
            Replay(self.prev.steps[self.ff_next].output)
            if self.ff_next is not None
            else None
        )
        return replace(self.hints, scratch=scratch, replay=replay), scratch

    def after_turn(
        self, k: int, scratch: TurnScratch, output: ModelOutput
    ) -> int | None:
        """Fold the turn's scratch in; return the ``prev`` index this turn replayed, if any."""
        requested, self.ff_next = self.ff_next, None
        for note in scratch.notes:
            self.hints = self.hints.add_note(note)
        if scratch.inferred:
            self.dump_frozen = True
        if requested is None:
            return None
        if scratch.replayed or same_action(output, self.prev.steps[requested].output):
            return requested
        # Requested provenance is not actual provenance: a memo hit answered
        # with a different action, and that earlier decision wins.
        self.hints = self.hints.add_note(
            Note(
                k + 1,
                f"fast-forward interrupted at [{requested}]: this session had already "
                "decided a different action for this exact state, and it was executed",
            )
        )
        return None

    def align(
        self,
        k: int,
        scratch: TurnScratch,
        replayed: int | None,
        output: ModelOutput,
        results: tuple[ToolResult, ...],
    ) -> None:
        if scratch.picked_step is not None:
            aligned: int | None = scratch.picked_step
        elif replayed is not None:
            aligned = replayed
        elif self.cursor < len(self.prev) and same_action(
            self.prev.steps[self.cursor].output, output
        ):
            aligned = self.cursor
        else:
            aligned = find_same_action(self.prev, output, self.cursor)

        diffs: list[Diff] = []
        if aligned is not None:
            self.cursor = aligned + 1
            diffs = _diffs(k, output, self.prev.steps[aligned].results, results)
            for d in diffs:
                self.hints = self.hints.add_diff(d)
        if diffs and not self.dump_frozen:
            self.dump_frozen = True
            self.hints = replace(
                self.hints, dump=_dump_from(self.prev, self.cursor, at_step=k + 1)
            )

        if scratch.picked_step is not None:
            self.ff_budget = (
                None if scratch.pick_steps is None else scratch.pick_steps - 1
            )
        continuing = (
            aligned is not None
            and not diffs
            and (scratch.picked_step is not None or replayed is not None)
            and (self.ff_budget is None or self.ff_budget > 0)
        )
        if not continuing or aligned is None:
            return
        nxt = aligned + 1
        if nxt >= len(self.prev):
            return
        if self.prev.steps[nxt].output.answer is not None:
            self.hints = self.hints.add_note(
                Note(
                    k + 1,
                    f"fast-forward reached last run's final answer [{nxt}]; confirm or revise",
                )
            )
        else:
            self.ff_next = nxt
            if self.ff_budget is not None:
                self.ff_budget -= 1


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
    state = _ReplayState(prev=prev_handle.value)
    steps: list[Step] = []

    for k in range(spec.role.max_turns):
        turn_hints, scratch = state.turn_hints(k)
        output: ModelOutput = await coco.use_mount(
            coco.component_subpath("turn", k),
            llm_step,
            spec,
            tools,
            Transcript(tuple(steps)),
            turn_hints,
        )
        replayed = state.after_turn(k, scratch, output)

        if output.answer is not None:
            steps.append(Step(output, ()))
            prev_handle.value = Transcript(tuple(steps))
            return SessionResult(answer=output.answer, num_steps=len(steps))

        results = await _execute_all(registry, ctx, output, contain=not closed)
        steps.append(Step(output, results))
        state.align(k, scratch, replayed, output, results)

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


def is_environment_error(exc: BaseException) -> bool:
    """An error a session may contain as a result: anything but deadline expiry and cancellation."""
    return isinstance(exc, Exception) and not isinstance(
        exc, coco.DeadlineExceededError
    )


async def _execute_all(
    registry: ToolRegistry, ctx: SessionContext, output: ModelOutput, *, contain: bool
) -> tuple[ToolResult, ...]:
    """Run one turn's calls concurrently against a snapshot of the handle table.

    Results keep call order. A call cannot see a handle a sibling registers in
    the same turn, and two siblings registering the same name both fail. Two
    calls that would mount children under the same key both fail before
    anything runs. Every call is drained before a failure propagates.

    With ``contain`` (open sessions), an environment error becomes an
    ``ok=False`` result naming the error class; the model reads it, the
    session records it so its memo is not reused, and the tool runs again
    next run. Without it (closed sessions), the first error propagates — all
    of them, as an ``ExceptionGroup``, when there are several.
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
    failures: list[BaseException] = []
    for i, outcome in zip(pending, outcomes):
        if isinstance(outcome, ToolResult):
            results[i] = outcome
        elif contain and is_environment_error(outcome):
            logger.warning(
                "tool %s failed in session %r: %s: %s",
                calls[i].tool,
                ctx.spec.role.name,
                type(outcome).__name__,
                outcome,
            )
            record_contained_failure(f"tool:{calls[i].tool}", outcome)
            results[i] = ToolResult(
                calls[i].tool, False, f"{type(outcome).__name__}: {outcome}"
            )
        else:
            assert isinstance(outcome, BaseException)
            failures.append(outcome)
    if failures:
        raise_all(failures)
    conflicts = ctx.handles.merge([forks[i] for i in pending])
    for j in conflicts:
        i = pending[j]
        results[i] = ToolResult(
            calls[i].tool,
            False,
            "handle name registered twice in one turn; give each call its own name",
        )
    return tuple(r for r in results if r is not None)


def raise_all(failures: list[BaseException]) -> None:
    """Raise one failure as is, several as a group; cancellation first."""
    for exc in failures:
        if not isinstance(exc, Exception):
            raise exc
    if len(failures) == 1:
        raise failures[0]
    raise ExceptionGroup(
        "several tool calls failed",
        [exc for exc in failures if isinstance(exc, Exception)],
    )


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
