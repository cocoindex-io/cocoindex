"""The fixed tool set: registry, per-session context, and the harness config.

Tools are fixed for a whole run and selected per role by name. A tool handler
runs in the session component's *body* (never inside the memoized model turn),
so it may mount children, declare target states and read state.

Two flags per tool define what a role may do:

- ``closed_ok``: the tool reads nothing beyond the session's item and its
  seeded inputs, so a closed (memoized) session may have it. Tools that open
  sources or mount children are not ``closed_ok``.
- ``key_arg``: the argument whose value names the child's slot (its component
  path, ``child/<key>``). Two calls with the same key in one turn, or across
  turns of one session, fail as tool errors instead of reaching the engine's
  duplicate-path check.

Handlers are wrapped in ``coco.fn`` so the engine tracks their logic; a
handler's own code edit invalidates the closed sessions that ran it. Methods of
handle objects and other helpers are not tracked: bump ``HarnessConfig.version``
for those.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field, replace
from typing import cast

import cocoindex as coco

from ..errors import ToolError
from ..handles import HandleTable
from ..spec import InputCard, Json, RoleSpec, SessionInput, SessionSpec, ToolSpec
from ..transcript import (
    INVALID_ARGS_KEY,
    ToolCall,
    ToolResult,
    canonical_json,
    normalize_json,
)

__all__ = [
    "HARNESS",
    "HarnessConfig",
    "SessionContext",
    "Tool",
    "ToolError",
    "ToolHandler",
    "ToolRegistry",
    "tool",
    "validate_args",
]


@dataclass
class SessionContext:
    """What a tool handler sees: the session, its input, its handles, the harness."""

    spec: SessionSpec
    input: SessionInput
    handles: HandleTable
    config: HarnessConfig
    used_slots: set[str] = field(default_factory=set)

    def claim_slot(self, key: str) -> None:
        """Reserve a child key for this session; a repeat is the model's error."""
        if key in self.used_slots:
            raise ToolError(
                f"key {key!r} was already used by an earlier call in this session"
            )
        self.used_slots.add(key)


ToolHandler = Callable[[SessionContext, dict[str, Json]], Awaitable[Json]]


@dataclass(frozen=True)
class Tool:
    spec: ToolSpec
    handler: ToolHandler
    closed_ok: bool = False
    key_arg: str | None = None


def tool(
    name: str,
    description: str,
    parameters: dict[str, Json] | None = None,
    *,
    closed_ok: bool = False,
    key_arg: str | None = None,
) -> Callable[[ToolHandler], Tool]:
    """Decorator: turn an ``async def handler(ctx, args)`` into a ``Tool``."""

    def wrap(handler: ToolHandler) -> Tool:
        spec = ToolSpec(
            name=name,
            description=description,
            parameters=parameters or {"type": "object", "properties": {}},
        )
        tracked = cast(ToolHandler, coco.fn(handler))
        return Tool(spec=spec, handler=tracked, closed_ok=closed_ok, key_arg=key_arg)

    return wrap


_JSON_TYPES: dict[str, type | tuple[type, ...]] = {
    "string": str,
    "integer": int,
    "number": (int, float),
    "boolean": bool,
    "array": list,
    "object": dict,
}


def validate_args(spec: ToolSpec, args: object) -> dict[str, Json]:
    """Check model-supplied arguments against the tool's schema; failures are the model's.

    Shape (an object), required names, unknown names, declared primitive types
    and enums. Deliberately shallow: nested schemas are the handler's business.
    """
    if not isinstance(args, dict):
        raise ToolError("arguments must be a JSON object")
    if INVALID_ARGS_KEY in args:
        raise ToolError(
            f"arguments must be a JSON object; got {canonical_json(args[INVALID_ARGS_KEY])}"
        )
    schema = spec.parameters
    props: dict[str, Json] = schema.get("properties", {})
    for name in schema.get("required", []):
        if name not in args:
            raise ToolError(f"missing argument {name!r}")
    for name, value in args.items():
        prop = props.get(name)
        if prop is None:
            raise ToolError(f"unknown argument {name!r}")
        expected = _JSON_TYPES.get(prop.get("type", ""))
        if expected is not None:
            bad_bool = prop.get("type") in ("integer", "number") and isinstance(
                value, bool
            )
            if bad_bool or not isinstance(value, expected):
                raise ToolError(f"argument {name!r} must be of type {prop['type']}")
        if "enum" in prop and value not in prop["enum"]:
            raise ToolError(f"argument {name!r} must be one of {prop['enum']}")
    return args


class ToolRegistry:
    def __init__(self, tools: Iterable[Tool] = ()) -> None:
        self._tools: dict[str, Tool] = {}
        for t in tools:
            if t.spec.name in self._tools:
                raise ValueError(f"duplicate tool {t.spec.name!r}")
            self._tools[t.spec.name] = t

    def restrict(self, names: Iterable[str]) -> ToolRegistry:
        picked: list[Tool] = []
        for name in names:
            if name not in self._tools:
                raise ValueError(f"role refers to unknown tool {name!r}")
            picked.append(self._tools[name])
        return ToolRegistry(picked)

    def specs(self) -> tuple[ToolSpec, ...]:
        # Sorted so the tuple — part of every memo key — does not depend on
        # registration order.
        return tuple(self._tools[name].spec for name in sorted(self._tools))

    def tools(self) -> tuple[Tool, ...]:
        return tuple(self._tools[name] for name in sorted(self._tools))

    def get(self, name: str) -> Tool | None:
        return self._tools.get(name)

    async def execute(self, ctx: SessionContext, call: ToolCall) -> ToolResult:
        """Run one call.

        Argument validation failures and ``ToolError`` are the model's and
        become ``ok=False`` results. Any other exception is an infrastructure
        failure and propagates, failing the session component so nothing of it
        is committed.
        """
        t = self._tools.get(call.tool)
        if t is None:
            return ToolResult(call.tool, False, f"unknown tool {call.tool!r}")
        try:
            args = validate_args(t.spec, call.args)
            value = await t.handler(ctx, args)
        except ToolError as exc:
            return ToolResult(call.tool, False, str(exc))
        return ToolResult(call.tool, True, normalize_json(value))


@dataclass
class HarnessConfig:
    """Roles, tools, default model and version for one app. Provided through ``HARNESS``.

    Build every ``SessionSpec`` with ``spec()`` so it carries the concrete
    model id, the role's tool specs and the version — the three things that
    must invalidate a session when they change.
    """

    roles: dict[str, RoleSpec]
    tools: ToolRegistry = field(default_factory=ToolRegistry)
    default_model: str = "unspecified"
    version: int = 1

    def role(self, name: str) -> RoleSpec:
        try:
            role = self.roles[name]
        except KeyError:
            raise ToolError(
                f"unknown role {name!r}; known: {sorted(self.roles)}"
            ) from None
        if role.model == "default":
            role = replace(role, model=self.default_model)
        return role

    def spec(
        self, role_name: str, task: str, inputs: tuple[InputCard, ...] = ()
    ) -> SessionSpec:
        role = self.role(role_name)
        return SessionSpec(
            role=role,
            task=task,
            tools=self.tools.restrict(role.tool_names).specs(),
            inputs=inputs,
            version=self.version,
        )

    def open_tools(self, role: RoleSpec) -> list[str]:
        """Names of the role's tools that are not ``closed_ok``; empty means the role may run closed."""
        return [
            t.spec.name
            for t in self.tools.restrict(role.tool_names).tools()
            if not t.closed_ok
        ]


HARNESS: coco.ContextKey[HarnessConfig] = coco.ContextKey("coco_harness.config")
