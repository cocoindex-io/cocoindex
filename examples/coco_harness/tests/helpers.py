"""Test-only tools. The scripted model lives in ``coco_harness.testing``."""

from __future__ import annotations

from collections.abc import Mapping

from coco_harness import Tool, tool
from coco_harness.spec import Json
from coco_harness.tools import SessionContext


def probe_tool(values: Mapping[str, object]) -> Tool:
    """``probe(name)`` returns ``values[name]``; mutate ``values`` between runs to force a divergence.

    A value that is an ``Exception`` instance is raised instead: a ``ToolError``
    reaches the model as a failed result, a ``DeadlineExceededError`` fails the
    run, anything else is an environment error (contained in an open session,
    fatal in a closed one).
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
