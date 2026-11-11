"""Test doubles: a scripted model for the ``ModelClient`` seam, and a tool
registry that counts executions.

The point of this package is to spend less inference, so tests assert on
inference counts: "edit 1 of N files → that worker's content turn re-runs,
nothing else". The scripted model sees exactly what the real client sees
(spec, transcript prefix, hints) and records it for assertions. Tool
executions show whether a session's body ran at all: a skipped session
executes no tool. Users testing their own flows need the same doubles, which
is why they ship with the package; test-only tools live under ``tests/``.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field

from .hints import RunHints
from .spec import SessionSpec, ToolSpec
from .tools import SessionContext, Tool, ToolRegistry
from .transcript import ModelOutput, ToolCall, ToolResult, Transcript

Script = Callable[[SessionSpec, Transcript, RunHints], ModelOutput]


@dataclass
class Seen:
    role: str
    step: int
    hints: RunHints


@dataclass
class ScriptedModel:
    """A ``ModelClient`` driven by a plain function; counts and records every call."""

    script: Script
    calls: int = 0
    seen: list[Seen] = field(default_factory=list)

    async def complete(
        self,
        spec: SessionSpec,
        tools: tuple[ToolSpec, ...],
        prefix: Transcript,
        hints: RunHints,
    ) -> ModelOutput:
        self.calls += 1
        self.seen.append(Seen(spec.role.name, len(prefix.steps), hints))
        return self.script(spec, prefix, hints)

    def calls_for(self, role: str) -> int:
        return sum(1 for s in self.seen if s.role == role)

    def reset(self) -> None:
        self.calls = 0
        self.seen.clear()


class CountingRegistry(ToolRegistry):
    """A registry that counts executions per tool, across ``restrict()``.

    Tests assert on model calls to see inference, and on tool executions to
    see whether a session's body ran at all: a skipped session executes no
    tool.
    """

    def __init__(
        self, tools: Iterable[Tool] = (), executed: Counter[str] | None = None
    ) -> None:
        super().__init__(tools)
        self.executed: Counter[str] = executed if executed is not None else Counter()

    def restrict(self, names: Iterable[str]) -> ToolRegistry:
        return CountingRegistry(super().restrict(names).tools(), self.executed)

    async def execute(self, ctx: SessionContext, call: ToolCall) -> ToolResult:
        self.executed[call.tool] += 1
        return await super().execute(ctx, call)

    def reset(self) -> None:
        self.executed.clear()
