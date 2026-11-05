"""The fixed tool set: registry, per-session context, and the agents config.

Tools are fixed for a whole run and selected per role by name. A tool handler
runs in the session component's *body* (never inside the memoized model turn),
so it may mount children, declare target states and read state.

Two flags per tool define what a role may do:

- ``closed_ok``: the tool reads nothing beyond the session's item and its
  seeded inputs, so a closed session may have it. Tools that open sources or
  mount children are not ``closed_ok``.
- ``key_arg``: the argument whose value names the child's slot (its component
  path, ``child/<key>``). Two calls with the same key in one turn, or across
  turns of one session, fail as tool errors instead of reaching the engine's
  duplicate-path check.

Every session is memoized, so a tool that reads beyond the session's
arguments must **record** each read with ``coco.use_data_dep`` (the source
handles do; see ``sources.py``): the engine reuses the session's memo only
while every recorded read validates, and a read that is not recorded is a
read the engine cannot see. A tool that records nothing is treated as closed.

Handlers are wrapped in ``coco.fn`` so the engine tracks their logic; a
handler's own code edit invalidates the sessions that ran it. Methods of
handle objects and other helpers are not tracked: bump ``AgentsConfig.version``
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
from ..trace import ActionSink, TraceSink
from ..transcript import (
    INVALID_ARGS_KEY,
    ToolCall,
    ToolResult,
    canonical_json,
    normalize_json,
)

__all__ = [
    "AGENTS_CONFIG",
    "AgentsConfig",
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
    """What a tool handler sees: the session, its input, its handles, the config.

    ``trace`` collects what the session's tools report for its trace (reads,
    declared files, contained failures); ``action`` is this call's own sink,
    where a tool that mounts a child leaves the child's trace.
    """

    spec: SessionSpec
    input: SessionInput
    handles: HandleTable
    config: AgentsConfig
    used_slots: set[str] = field(default_factory=set)
    trace: TraceSink = field(default_factory=TraceSink)
    action: ActionSink = field(default_factory=ActionSink)

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
        become ``ok=False`` results. Any other exception is an environment
        failure (DESIGN.md §7): it propagates to the session loop, which
        reports it as a result in an open session and fails a closed one.
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


@dataclass(frozen=True)
class AgentsConfig:
    """Roles, tools, default model and version for one app. Provided through ``AGENTS_CONFIG``.

    Build every ``SessionSpec`` with ``spec()`` so it carries the concrete
    model id, the role's tool specs and the version — the three things that
    must invalidate a session when they change.

    A session also derives its children's specs from the config it reads
    through ``AGENTS_CONFIG``, so the key detects changes and the config's memo key
    is everything a spec can be built from. The config is frozen because its
    fingerprint is taken when it is provided: an edit is a new config
    (``dataclasses.replace``), provided anew.
    """

    roles: dict[str, RoleSpec]
    tools: ToolRegistry = field(default_factory=ToolRegistry)
    default_model: str = "unspecified"
    version: int = 2

    def __coco_memo_key__(self) -> object:
        return (
            "agents_config",
            tuple(sorted(self.roles.items())),
            self.tools.specs(),
            self.default_model,
            self.version,
        )

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


AGENTS_CONFIG: coco.ContextKey[AgentsConfig] = coco.ContextKey(
    "cocoindex_agents.config", detect_change=True
)
