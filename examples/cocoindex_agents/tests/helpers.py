"""Test-only tools. The scripted model and the counting registry live in
``cocoindex_agents.testing``."""

from __future__ import annotations

from collections.abc import Mapping

import cocoindex as coco

from cocoindex_agents import Tool, tool
from cocoindex_agents.spec import Json
from cocoindex_agents.tools import SessionContext
from cocoindex_agents.transcript import canonical_json

# The mapping behind the live probe tool, read by the kind's validator. One
# probe tool is live at a time: a test builds its own.
_probe_values: Mapping[str, object] = {}


def _probe_state(value: object) -> str:
    """What a probe read observed, as the record's state; an exception is one too."""
    if isinstance(value, Exception):
        return f"raise {type(value).__name__}: {value}"
    return canonical_json(value)


def _validate_probe(key: str, prev: str) -> coco.MemoStateOutcome:
    state = _probe_state(_probe_values[key])
    return coco.MemoStateOutcome(state=state, memo_valid=state == prev)


PROBE = coco.DataDep("cocoindex_agents.tests.probe", validate=_validate_probe)


def probe_tool(values: Mapping[str, object]) -> Tool:
    """``probe(name)`` returns ``values[name]``; mutate ``values`` between runs to force a divergence.

    A value that is an ``Exception`` instance is raised instead: a ``ToolError``
    reaches the model as a failed result, a ``DeadlineExceededError`` fails the
    run, anything else is an environment error (contained in an open session,
    fatal in a closed one).

    The probe is a source: it reads beyond the session's arguments, so each
    read is recorded as a data dependency, which is what lets an unchanged
    session be skipped and a mutated value re-run it.
    """
    global _probe_values
    _probe_values = values

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
        coco.use_data_dep(PROBE, key=args["name"], state=_probe_state(value))
        if isinstance(value, Exception):
            raise value
        return value

    return probe


def explode_tool(errors: Mapping[str, Exception]) -> Tool:
    """``explode(key)`` raises ``errors[key]`` when present, else returns "fine".

    ``closed_ok``: it reads nothing, so closed workers may have it — which is
    how a test makes one worker fail.
    """

    @tool(
        "explode",
        "Fail for some keys.",
        {
            "type": "object",
            "properties": {"key": {"type": "string"}},
            "required": ["key"],
        },
        closed_ok=True,
    )
    async def explode(ctx: SessionContext, args: dict[str, Json]) -> Json:
        if args["key"] in errors:
            raise errors[args["key"]]
        return "fine"

    return explode
