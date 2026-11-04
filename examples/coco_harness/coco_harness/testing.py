"""A scripted model: the test double for the ``ModelClient`` seam.

The point of the harness is to spend less inference, so tests assert on
inference counts: "edit 1 of N files → that worker's content turn re-runs,
nothing else". The scripted model sees exactly what the real client sees
(spec, transcript prefix, hints) and records it for assertions. Users testing
their own flows need the same double, which is why it ships with the package;
test-only tools live under ``tests/``.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

from .hints import RunHints
from .spec import SessionSpec, ToolSpec
from .transcript import ModelOutput, Transcript

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
