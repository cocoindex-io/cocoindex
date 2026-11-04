"""Test doubles: a scripted model that counts its calls, and fake tools.

The point of the harness is to spend less inference, so tests assert on
inference counts: "edit 1 of N files → that worker's content turn re-runs,
nothing else". The scripted model sees exactly what the real client sees
(spec, transcript prefix, hints) and records it for assertions.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field

from .hints import RunHints
from .spec import Json, SessionSpec, ToolSpec
from .tools import SessionContext, Tool, tool
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


def probe_tool(values: Mapping[str, Json]) -> Tool:
    """``probe(name)`` returns ``values[name]``; mutate ``values`` between runs to force a divergence.

    A value that is an ``Exception`` instance is raised instead: a ``ToolError``
    reaches the model as a failed result, anything else fails the session.
    """

    @tool(
        "probe",
        "Read a probe value by name.",
        {
            "type": "object",
            "properties": {"name": {"type": "string"}},
            "required": ["name"],
        },
    )
    async def probe(ctx: SessionContext, args: dict[str, Json]) -> Json:
        value = values[args["name"]]
        if isinstance(value, Exception):
            raise value
        return value

    return probe
