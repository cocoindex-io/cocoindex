"""The run report: the root's trace plus this run's tokens and wall time, and its two renderings.

``render_stats_line`` is the one ``stats:`` line; ``render_plan`` is the plan
as a tree, the control plane turn by turn and the data plane as counts. Both
read stored traces as they are: a skipped subtree shows its structure and
totals, never per-turn detail it no longer has.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass

from .trace import (
    ChildTrace,
    DeclaredFile,
    SessionTrace,
    StepTrace,
    Tally,
    Usage,
)

_LEFT_WIDTH = 48


@dataclass(frozen=True)
class RunReport:
    """One run as the runner sees it: the root's trace, and this run's tokens and time."""

    name: str
    root: SessionTrace
    model: str
    wall_seconds: float
    usage: Usage | None  # None: the client did not report usage

    @classmethod
    def from_result(
        cls,
        name: str,
        trace: SessionTrace,
        *,
        ran: bool,
        model: str,
        wall_seconds: float,
        usage: Usage | None,
    ) -> RunReport:
        root = trace if ran else trace.as_reused("nothing it read changed")
        return cls(name, root, model, wall_seconds, usage)

    @property
    def total(self) -> Tally:
        return self.root.total

    @property
    def spent(self) -> Tally:
        return self.root.spent

    def model_calls_by_role(self) -> dict[str, int]:
        out: Counter[str] = Counter()
        for t in _sessions(self.root):
            if t.status != "skipped":
                out[t.role] += sum(s.calls for s in t.steps)
        return dict(sorted(out.items()))

    def failures(self) -> list[str]:
        return [f for t in _sessions(self.root) for f in t.failures]


def _sessions(trace: SessionTrace) -> list[SessionTrace]:
    """Every session trace in the tree, itemized members included."""
    out = [trace]
    for child in trace.children():
        if child.session is not None:
            out.extend(_sessions(child.session))
        for item in child.items:
            out.extend(_sessions(item.session))
    return out


# --- rendering ----------------------------------------------------------------


def render_stats_line(report: RunReport) -> str:
    """The one ``stats:`` line.

    Each count is what the run would have cost with no cache, followed by the
    reused share only when something was reused. Tokens are this run's only
    (a turn memo stores no usage) and are omitted when no call was made.
    """
    total, spent = report.total, report.spent
    segments = [
        _segment("model calls", total.model_calls, spent.model_calls, "reused"),
        _segment("sessions", total.sessions, spent.sessions, "skipped"),
        _segment("tool calls", total.tool_calls, spent.tool_calls, "reused"),
    ]
    if spent.model_calls:
        segments.append(_tokens(report.usage))
    segments.append(_segment("files", total.files, spent.files, "unchanged"))
    reruns = _reruns(report.failures())
    if reruns:
        segments.append(f"re-runs next run: {reruns}")
    segments.append(f"{report.wall_seconds:.1f}s")
    return "stats: " + ", ".join(segments)


def _segment(name: str, total: int, spent: int, label: str) -> str:
    reused = total - spent
    if reused <= 0:
        return f"{name} {total}"
    return f"{name} {total} ({label} {reused} / {round(100 * reused / total)}%)"


def _tokens(usage: Usage | None) -> str:
    if usage is None:
        return "spent tokens ?"
    cached = ""
    if usage.cached_input_tokens and usage.input_tokens:
        share = round(100 * usage.cached_input_tokens / usage.input_tokens)
        cached = f" (provider-cached {usage.cached_input_tokens} / {share}%)"
    return f"spent tokens {usage.input_tokens} in{cached}, {usage.output_tokens} out"


def _reruns(failures: list[str]) -> str:
    tools = sum(1 for f in failures if f.startswith("tool:"))
    children = len(failures) - tools
    parts = []
    if tools:
        parts.append(f"{tools} contained tool failure{'s' if tools != 1 else ''}")
    if children:
        parts.append(f"{children} failed child{'ren' if children != 1 else ''}")
    return ", ".join(parts)


def render_plan(report: RunReport) -> str:
    """The plan as a tree: the control plane turn by turn, the data plane as counts.

    Everything under a skipped session is *stored*: it shows its structure and
    what the plan built on, never per-turn tags or counts it no longer has.
    """
    lines = [f"plan: {report.name} · {report.model} · {report.wall_seconds:.1f}s"]
    _render_session(
        lines, report.root, indent=0, label=report.root.role, full=True, stored=False
    )
    return "\n".join(lines)


def _line(lines: list[str], indent: int, left: str, right: list[str]) -> None:
    left = " " * indent + left
    tags = " · ".join(t for t in right if t)
    if not tags:
        lines.append(left.rstrip())
    elif len(left) < _LEFT_WIDTH:
        lines.append(f"{left:<{_LEFT_WIDTH}} {tags}")
    else:
        lines.append(f"{left}  {tags}")


def _status_tag(t: SessionTrace) -> str:
    if t.status == "new":
        return "new"
    if t.status == "skipped":
        return f"skipped: {t.reason}"
    return f"re-ran: {t.reason}"


def _turns_tag(t: SessionTrace, *, stored: bool) -> str:
    n = len(t.steps)
    turns = f"{n} turn{'s' if n != 1 else ''}"
    if stored:
        return turns
    by = t.turns_by_served
    detail = ", ".join(
        f"{by[k]} {k}" for k in ("hit", "replayed", "picked", "inferred") if by[k]
    )
    calls = sum(s.calls for s in t.steps)
    return f"{turns}: {detail} · {calls} call{'s' if calls != 1 else ''}"


def _render_session(
    lines: list[str],
    t: SessionTrace,
    *,
    indent: int,
    label: str,
    full: bool,
    stored: bool,
) -> None:
    """``full``: the control plane, step by step; else a summary line plus its files.

    ``stored``: this session lies under a skipped one, so its trace is its
    producer's, not this run's.
    """
    skipped = t.status == "skipped"
    tags = [
        "reused" if stored else _status_tag(t),
        _turns_tag(t, stored=stored or skipped),
    ]
    if skipped and not stored and t.total.tool_calls:
        tags.append(f"{t.total.tool_calls} tool calls reused")
    stored = stored or skipped
    _line(lines, indent, label, tags)
    if not full:
        for d in t.declared:
            _line(lines, indent + 5, f"declare {d.filename}", [_file_tag(d, stored)])
        return
    for k, step in enumerate(t.steps):
        for j, action in enumerate(step.actions):
            left = f"{k:>2}  {action.text}" if j == 0 else f"    {action.text}"
            tags = []
            if j == 0 and not stored:
                tags.append(_served_tag(step))
            if not action.ok:
                tags.append("failed")
            if action.differs and not stored:
                tags.append("result differs from last run")
            if action.declared is not None:
                tags.append(_file_tag(action.declared, stored))
            if action.child is not None:
                tags.append(_child_tag(action.child, stored))
            _line(lines, indent + 2, left, tags)
            if action.task:
                lines.append(" " * (indent + 7) + f'"{action.task}"')
            if action.child is not None:
                _render_child(lines, action.child, indent + 5, stored)
        if not stored:
            for note in step.notes:
                lines.append(" " * (indent + 7) + f"[{note}]")


def _render_child(lines: list[str], c: ChildTrace, indent: int, stored: bool) -> None:
    if c.kind == "session" and c.session is not None:
        _render_session(
            lines,
            c.session,
            indent=indent,
            label=f"└─ {c.role}",
            full=True,
            stored=stored,
        )
        if c.plan is not None:
            for i, text in enumerate(c.plan.splitlines() or [""]):
                prefix = "plan: " if i == 0 else "      "
                lines.append(" " * (indent + 5) + prefix + text)
    if stored:
        return  # the itemized members are the producer's; this run reused them all
    for item in c.items:
        _render_session(
            lines,
            item.session,
            indent=indent,
            label=f"└─ {item.key}",
            full=False,
            stored=False,
        )


def _child_tag(c: ChildTrace, stored: bool) -> str:
    if c.kind == "background":
        return "background: not tracked"
    if c.kind == "session":
        return ""
    n = c.counts.members
    if stored or c.counts.skipped == n:
        return f"{c.role} × {n}: all reused"
    parts = []
    if c.counts.new:
        parts.append(f"{c.counts.new} new")
    if c.counts.ran:
        parts.append(f"{c.counts.ran} re-ran")
    if c.counts.skipped:
        parts.append(f"{c.counts.skipped} skipped")
    if c.counts.failed:
        parts.append(f"{c.counts.failed} failed")
    return f"{c.role} × {n}: {', '.join(parts)}"


def _served_tag(step: StepTrace) -> str:
    if step.from_step is not None:
        return f"{step.served} [{step.from_step}]"
    return step.served


def _file_tag(d: DeclaredFile, stored: bool) -> str:
    return "unchanged" if stored or not d.written else "written"
