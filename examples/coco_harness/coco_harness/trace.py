"""What a run reused and what it recomputed, and the plan it chose.

Every session returns a ``SessionTrace`` inside its ``SessionResult``: the
actions it took, how each turn was served (hit, replayed, picked, inferred),
its children's traces, the files it declared, and two tallies — ``total``,
the subtree as if nothing were cached, and ``spent``, what this run actually
did. A skipped session executes no Python, so its stored trace, returned by
the engine with the memo value, is the only account of its subtree: the
parent marks it reused (``as_reused``), which zeroes ``spent`` and keeps
``total``. Reuse is thus priced as the producer ran it, never estimated.

Nothing here enters a memo key: the trace is a return value, the scratch
objects are excluded from keys (``memo_key={"scratch": None}``), and the
``last_run`` state a session keeps to explain *why* it re-ran is component
state like the transcript. A run with ``--plan`` stores exactly what a run
without it stores; the flags only render.

Rendering follows one rule set: totals mean "would have cost with no cache";
the reused share is shown only when it is non-zero; an unknown prints as
``?`` and never gets a percentage.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field, replace
from typing import NamedTuple


# Items of a fan-out or reduce that are shown one by one: those that did not
# simply hit, up to this many. The rest are counted.
MAX_ITEMIZED = 20


# --- counting -----------------------------------------------------------------


@dataclass(frozen=True)
class Tally:
    """Addable counts of work. ``files`` counts declared target files."""

    sessions: int = 0
    model_calls: int = 0
    tool_calls: int = 0
    files: int = 0

    def __add__(self, other: Tally) -> Tally:
        return Tally(
            self.sessions + other.sessions,
            self.model_calls + other.model_calls,
            self.tool_calls + other.tool_calls,
            self.files + other.files,
        )


@dataclass(frozen=True)
class Usage:
    """Tokens of this run's model calls, as the provider reported them.

    ``cached_input_tokens`` is the provider's prompt-cache share of the input.
    Unknown is represented by the absence of a ``Usage`` (``None``), never by
    a zero.
    """

    input_tokens: int = 0
    output_tokens: int = 0
    cached_input_tokens: int = 0

    def __add__(self, other: Usage) -> Usage:
        return Usage(
            self.input_tokens + other.input_tokens,
            self.output_tokens + other.output_tokens,
            self.cached_input_tokens + other.cached_input_tokens,
        )


# --- the trace ----------------------------------------------------------------


@dataclass(frozen=True)
class DeclaredFile:
    filename: str
    written: bool  # content differs from what this session declared last run


@dataclass(frozen=True)
class ChildTrace:
    """A child a tool mounted: one session, a fan-out, a reduce, or a background job.

    A fan-out or reduce itemizes only the members that did not simply hit, up
    to ``MAX_ITEMIZED``; ``counts`` covers all of them. ``total`` and ``spent``
    cover every member, itemized or not.
    """

    kind: str  # "session" | "fan_out" | "reduce" | "background"
    key: str
    role: str
    session: SessionTrace | None = None  # kind == "session"
    plan: str | None = None  # a planner's answer, the plan the parent built on
    items: tuple[ItemTrace, ...] = ()  # fan_out / reduce: the itemized members
    counts: Counts = field(default_factory=lambda: Counts())
    total: Tally = field(default_factory=Tally)
    spent: Tally = field(default_factory=Tally)


@dataclass(frozen=True)
class ItemTrace:
    key: str
    session: SessionTrace


@dataclass(frozen=True)
class Counts:
    """Members of a fan-out or reduce by what happened to them this run."""

    new: int = 0
    ran: int = 0
    skipped: int = 0
    failed: int = 0

    @property
    def members(self) -> int:
        return self.new + self.ran + self.skipped + self.failed


@dataclass(frozen=True)
class ActionTrace:
    text: str  # "open_source src:docs ./docs", "answer"
    ok: bool = True
    differs: bool = False  # the result differs from last run's for the same action
    task: str | None = None  # a child's task, elided
    child: ChildTrace | None = None
    declared: DeclaredFile | None = None  # a declare's file


@dataclass(frozen=True)
class StepTrace:
    served: str  # "hit" | "replayed" | "picked" | "inferred"
    from_step: int | None  # the previous transcript's step a replay or pick came from
    calls: int  # model calls this turn made (retries included)
    actions: tuple[ActionTrace, ...]
    notes: tuple[str, ...] = ()  # what the loop told the model at this step

    @property
    def tool_calls(self) -> int:
        return sum(1 for a in self.actions if a.text != ANSWER)


ANSWER = "answer"


@dataclass(frozen=True)
class SessionTrace:
    """One session's run, or its stored account when the session was skipped."""

    role: str
    status: str  # "new" | "ran" | "skipped"
    reason: str  # why it ran, or why it was skipped
    steps: tuple[StepTrace, ...]
    failures: tuple[str, ...] = ()  # contained this run; the session re-runs next run
    reads: int = 0  # recorded reads: a session that reads flags its parent
    total: Tally = field(default_factory=Tally)
    spent: Tally = field(default_factory=Tally)

    def as_reused(self, reason: str) -> SessionTrace:
        """The stored trace of a session the engine skipped: nothing spent, all carried over."""
        return replace(
            self, status="skipped", reason=reason, failures=(), spent=Tally()
        )

    @property
    def declared(self) -> tuple[DeclaredFile, ...]:
        return tuple(
            a.declared for s in self.steps for a in s.actions if a.declared is not None
        )

    @property
    def turns_by_served(self) -> Counter[str]:
        return Counter(s.served for s in self.steps)

    def children(self) -> list[ChildTrace]:
        return [a.child for s in self.steps for a in s.actions if a.child is not None]


# --- run-time collection ------------------------------------------------------


@dataclass
class SessionScratch:
    """Passed to a session component and excluded from its memo key.

    Untouched when the engine served the session from its memo, which is how
    the mounting tool recognizes a skipped child (the same device as
    ``TurnScratch`` for turns).
    """

    ran: bool = False


@dataclass(frozen=True)
class Record:
    """Something a session read or declared, by name, with a digest of its content.

    Names: ``walk of src:docs``, ``sample of src:invoices``, ``read of src:docs
    a.md``, or a declared filename.
    """

    name: str
    digest: str


@dataclass(frozen=True)
class LastRun:
    """What a session read and declared the last time its body ran (``use_state``).

    Compared with this run's reads to name the read that changed. ``recorded``
    distinguishes "nothing read" from "no record" (the state's initial value).
    """

    recorded: bool = False
    reads: tuple[Record, ...] = ()
    declared: tuple[Record, ...] = ()
    failed: bool = False

    def declared_digests(self) -> dict[str, str]:
        return {d.name: d.digest for d in self.declared} if self.recorded else {}


@dataclass
class TraceSink:
    """What the tools of one session run report, folded into its trace at the end.

    ``last_declared`` is what the session declared the last time its body ran,
    so a declaration is told written from unchanged as it happens.
    """

    last_declared: dict[str, str] = field(default_factory=dict)
    reads: list[Record] = field(default_factory=list)
    declared: list[Record] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)

    def note_read(self, name: str, digest: str) -> None:
        self.reads.append(Record(name, digest))

    def note_declared(self, filename: str, digest: str) -> DeclaredFile:
        self.declared.append(Record(filename, digest))
        return DeclaredFile(
            filename, written=self.last_declared.get(filename) != digest
        )

    def note_failure(self, where: str) -> None:
        self.failures.append(where)


@dataclass
class ActionSink:
    """What one tool call reports: the child it mounted, or the file it declared."""

    child: ChildTrace | None = None
    declared: DeclaredFile | None = None


def answer_step(
    served: str, from_step: int | None, calls: int, notes: tuple[str, ...] = ()
) -> StepTrace:
    return StepTrace(served, from_step, calls, (ActionTrace(ANSWER),), notes)


def session_tallies(steps: tuple[StepTrace, ...]) -> tuple[Tally, Tally]:
    """``(total, spent)`` of a session that ran, its children's tallies folded in."""
    children = [a.child for s in steps for a in s.actions if a.child is not None]
    declared = [a.declared for s in steps for a in s.actions if a.declared is not None]
    total = Tally(
        sessions=1,
        # A hit turn is priced at one call: what it would cost with no cache.
        model_calls=sum(max(1, s.calls) for s in steps),
        tool_calls=sum(s.tool_calls for s in steps),
        files=len(declared),
    )
    spent = Tally(
        sessions=1,
        model_calls=sum(s.calls for s in steps),
        tool_calls=sum(s.tool_calls for s in steps),
        files=sum(1 for d in declared if d.written),
    )
    for c in children:
        total += c.total
        spent += c.spent
    return total, spent


def child_session(
    key: str,
    trace: SessionTrace,
    *,
    ran: bool,
    skipped_reason: str,
    plan: str | None = None,
) -> ChildTrace:
    """The node for one open child; ``ran`` is the child's scratch."""
    if not ran:
        trace = trace.as_reused(skipped_reason)
    return ChildTrace(
        "session",
        key,
        trace.role,
        session=trace,
        plan=plan,
        counts=_counts([trace]),
        total=trace.total,
        spent=trace.spent,
    )


class Member(NamedTuple):
    """One member of a fan-out or reduce, as its mounting tool saw it."""

    key: str
    trace: SessionTrace
    ran: bool  # the member's scratch: False means the engine skipped it


def child_members(
    kind: str,
    key: str,
    role: str,
    members: list[Member],
    *,
    failed: int,
    skipped_reason: str,
) -> ChildTrace:
    """The node for a fan-out or a reduce."""
    traces = {
        m.key: m.trace if m.ran else m.trace.as_reused(skipped_reason) for m in members
    }
    total = spent = Tally()
    for t in traces.values():
        total += t.total
        spent += t.spent
    itemized = [
        ItemTrace(k, t) for k, t in sorted(traces.items()) if t.status != "skipped"
    ]
    return ChildTrace(
        kind,
        key,
        role,
        items=tuple(itemized[:MAX_ITEMIZED]),
        counts=replace(_counts(traces.values()), failed=failed),
        total=total,
        spent=spent,
    )


def _counts(traces: Iterable[SessionTrace]) -> Counts:
    by_status = Counter(t.status for t in traces)
    return Counts(
        new=by_status["new"], ran=by_status["ran"], skipped=by_status["skipped"]
    )


# --- reasons ------------------------------------------------------------------


def explain_run(
    *,
    is_new: bool,
    spec_changed: bool,
    input_changed: bool,
    input_name: str,
    reads: list[Record],
    last: LastRun,
    children: list[ChildTrace],
) -> tuple[str, str]:
    """``(status, reason)`` of a session whose body ran.

    The order is the order of evidence: a new session, a changed contract, a
    changed input, a changed read, a failure absorbed last run, an open child
    whose own read changed (the engine validates a descendant's reads from its
    entry and re-runs the parent when one fails), and — honestly — no record.
    """
    if is_new:
        return "new", ""
    if spec_changed:
        return "ran", "task, role or tools changed"
    if input_changed:
        return "ran", f"{input_name} changed"
    if last.recorded:
        before = {r.name: r.digest for r in last.reads}
        for r in reads:
            if r.name not in before:
                return "ran", f"{r.name} is new"
            if before[r.name] != r.digest:
                return "ran", f"{r.name} changed"
    if last.failed:
        return "ran", "absorbed a failure last run"
    if any(_reads_data(c) for c in children):
        return "ran", "an open child's read changed"
    return "ran", "reason not recorded"


def _reads_data(child: ChildTrace) -> bool:
    return (
        child.kind == "session"
        and child.session is not None
        and child.session.reads > 0
    )
