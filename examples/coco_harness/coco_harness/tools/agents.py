"""Subagent tools: call_agent, start_agent, for_each, observe, reduce.

Each maps to one CocoIndex primitive (DESIGN.md §3). Every model-named child
lives under ``child/<key>`` (items and reduce groups nest below that), so a
key used once per session means one engine path per child and distinct keys
cannot alias; renaming a key recomputes that subtree and cleans up the old
one. A key reused within a session is a tool error, never an engine error.
``inputs`` names the parent's handles a child may see; the child's spec lists
them as cards so its model knows their names.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import replace

import cocoindex as coco

from ..errors import ToolError
from ..handles import HandleObject, MethodSpec, ResultValue, Volatility, input_card
from ..session import run_closed_session, run_open_session, run_reduce_session
from ..spec import InputCard, Item, Json, SessionResult, SessionSpec
from . import SessionContext, tool
from .reduce import Group, GroupItem, ReduceError, hierarchical_reduce, leaf_item
from .sources import LocalDirSource
from .stats import format_range, widen_count

_DEFAULT_REDUCE_THRESHOLD_TOKENS = 6000
# A reducer output over half the budget is re-asked once, at its own slot.
_REDUCE_ATTEMPTS = 2

_INPUTS_SCHEMA: dict[str, Json] = {"type": "array", "items": {"type": "string"}}


class Collection:
    """A ``coll:`` handle: the outputs of a ``for_each(collect=True)``.

    Lives only for the run. The card shows a widened count, never the contents;
    ``reduce`` consumes it. Not an input for closed sessions.
    """

    kind = "collection"
    closed_input_ok = False

    def __init__(self, entries: list[tuple[str, str]]) -> None:
        self.entries = sorted(entries)

    def methods(self) -> tuple[MethodSpec, ...]:
        return (
            MethodSpec("stats", "Widened item count.", Volatility.STABLE),
            MethodSpec("get", "One item's output by key.", Volatility.PER_ITEM),
        )

    async def call(self, method: str, args: dict[str, Json]) -> Json:
        if method == "stats":
            return {"items": format_range(widen_count(len(self.entries)))}
        if method == "get":
            for key, value in self.entries:
                if key == args.get("key"):
                    return value
            raise ToolError(f"no item {args.get('key')!r}")
        raise ToolError(f"unknown method {method!r} on a collection")


def _child_inputs(
    ctx: SessionContext, args: dict[str, Json], *, closed: bool
) -> tuple[dict[str, HandleObject], tuple[InputCard, ...]]:
    # The shallow validator does not type array items, so the names are coerced here.
    names = [str(h) for h in args.get("inputs", [])]
    handles = ctx.handles.resolve(names)
    if closed:
        for name, obj in handles.items():
            if not obj.closed_input_ok:
                raise ToolError(
                    f"{name!r} ({obj.kind}) cannot be an input of a closed session: its "
                    "contents are not covered by the session's memo key. Use call_agent "
                    "or observe for an open session."
                )
    cards = tuple(input_card(name, obj) for name, obj in handles.items())
    return handles, cards


def _closed_spec(
    ctx: SessionContext, role_name: str, task: str, cards: tuple[InputCard, ...]
) -> SessionSpec:
    role = ctx.config.role(role_name)
    open_tools = ctx.config.open_tools(role)
    if open_tools:
        raise ToolError(
            f"role {role_name!r} has tools that read beyond a closed session's inputs "
            f"({open_tools}); use call_agent for an open session"
        )
    return ctx.config.spec(role_name, task, inputs=cards)


def _result(ctx: SessionContext, name: str, text: str) -> Json:
    """Every result gets a res: handle; the transcript sees the handle and a size, not the text."""
    handle = ctx.handles.register("res", name, ResultValue(text))
    return {"result": handle, "chars": format_range(widen_count(len(text)))}


@tool(
    "call_agent",
    "Run a subagent with `task` and wait for it. `key` names it stably across runs; "
    "`inputs` lists handles it may use. Returns a res: handle to its answer.",
    {
        "type": "object",
        "properties": {
            "key": {"type": "string"},
            "task": {"type": "string"},
            "role": {"type": "string"},
            "inputs": _INPUTS_SCHEMA,
        },
        "required": ["key", "task"],
    },
    key_arg="key",
)
async def call_agent(ctx: SessionContext, args: dict[str, Json]) -> Json:
    key: str = args["key"]
    handles, cards = _child_inputs(ctx, args, closed=False)
    child = ctx.config.spec(args.get("role", "worker"), args["task"], inputs=cards)
    ctx.claim_slot(key)  # after validation: a rejected call does not consume its key
    result = await coco.use_mount(
        coco.component_subpath("child", key), run_open_session, child, handles
    )
    return _result(ctx, key, result.answer)


@tool(
    "start_agent",
    "Start a subagent in the background. It can only act through targets in `inputs`; "
    "you get a job: handle and never its answer.",
    {
        "type": "object",
        "properties": {
            "key": {"type": "string"},
            "task": {"type": "string"},
            "role": {"type": "string"},
            "inputs": _INPUTS_SCHEMA,
        },
        "required": ["key", "task"],
    },
    key_arg="key",
)
async def start_agent(ctx: SessionContext, args: dict[str, Json]) -> Json:
    key: str = args["key"]
    handles, cards = _child_inputs(ctx, args, closed=False)
    child = ctx.config.spec(args.get("role", "worker"), args["task"], inputs=cards)
    ctx.claim_slot(key)
    await coco.mount(
        coco.component_subpath("child", key), run_open_session, child, handles
    )
    return {"job": f"job:{key}"}


@tool(
    "observe",
    "Ask a planner to look at data (it may list and sample freely) and return a plan. "
    "Only the plan comes back; keep it stable.",
    {
        "type": "object",
        "properties": {
            "key": {"type": "string"},
            "question": {"type": "string"},
            "inputs": _INPUTS_SCHEMA,
        },
        "required": ["key", "question"],
    },
    key_arg="key",
)
async def observe(ctx: SessionContext, args: dict[str, Json]) -> Json:
    key: str = args["key"]
    handles, cards = _child_inputs(ctx, args, closed=False)
    child = ctx.config.spec("planner", args["question"], inputs=cards)
    ctx.claim_slot(key)
    result = await coco.use_mount(
        coco.component_subpath("child", key), run_open_session, child, handles
    )
    return {"plan": result.answer}


@tool(
    "for_each",
    "Run one closed worker per item of a source. `key` names the fan-out stably; `inputs` "
    "lists target/result handles the workers may use. With collect=true the answers are "
    "gathered into a coll: handle (you see a count, not the contents).",
    {
        "type": "object",
        "properties": {
            "source": {"type": "string"},
            "key": {"type": "string"},
            "task": {"type": "string"},
            "role": {"type": "string"},
            "inputs": _INPUTS_SCHEMA,
            "collect": {"type": "boolean"},
        },
        "required": ["source", "key", "task"],
    },
    key_arg="key",
)
async def for_each(ctx: SessionContext, args: dict[str, Json]) -> Json:
    key: str = args["key"]
    src = ctx.handles.get(args["source"])
    if not isinstance(src, LocalDirSource):
        raise ToolError("for_each needs a src: handle")
    handles, cards = _child_inputs(ctx, args, closed=True)
    child = _closed_spec(ctx, args.get("role", "worker"), args["task"], cards)

    async def items() -> AsyncIterator[tuple[str, Item]]:
        async for item_key, file in src.walker().items():
            yield item_key, Item(item_key, file)

    ctx.claim_slot(key)
    if not args.get("collect", True):
        await coco.mount_each(
            coco.component_subpath("child", key),
            run_closed_session,
            items(),
            child,
            handles,
        )
        return {"mounted": key}

    async def one(kv: tuple[str, Item]) -> tuple[str, SessionResult]:
        item_key, item = kv
        result = await coco.use_mount(
            coco.component_subpath("child", key, item_key),
            run_closed_session,
            item,
            child,
            handles,
        )
        return item_key, result

    results = await coco.map(one, items())
    coll = Collection([(k, r.answer) for k, r in results])
    handle = ctx.handles.register("coll", key, coll)
    return ctx.handles.card(handle, items=format_range(widen_count(len(coll.entries))))


@tool(
    "reduce",
    "Combine every item of a coll: handle into one answer with `task`, hierarchically. "
    "Returns a res: handle to the answer.",
    {
        "type": "object",
        "properties": {
            "collection": {"type": "string"},
            "key": {"type": "string"},
            "task": {"type": "string"},
            "threshold_tokens": {"type": "integer"},
        },
        "required": ["collection", "key", "task"],
    },
    key_arg="key",
)
async def reduce(ctx: SessionContext, args: dict[str, Json]) -> Json:
    key: str = args["key"]
    coll = ctx.handles.get(args["collection"])
    if not isinstance(coll, Collection):
        raise ToolError("reduce needs a coll: handle")
    child = _closed_spec(ctx, "reducer", args["task"], ())
    threshold: int = args.get("threshold_tokens", _DEFAULT_REDUCE_THRESHOLD_TOKENS)
    if threshold < 2:
        raise ToolError("threshold_tokens must be at least 2")
    half = threshold // 2

    items: list[GroupItem[str]] = [
        leaf_item(item_key, _estimate_tokens(text), text)
        for item_key, text in coll.entries
    ]

    async def reduce_group(group: Group[str]) -> str:
        # Slot: the group's prefix (unique across levels, DESIGN §6) plus the
        # attempt. Reuse needs the same slot and the same members (§7).
        members = tuple(m.payload for m in group.members)
        spec = child
        for attempt in range(_REDUCE_ATTEMPTS):
            result = await coco.use_mount(
                coco.component_subpath("child", key, group.prefix or "root", attempt),
                run_reduce_session,
                members,
                spec,
            )
            if _estimate_tokens(result.answer) <= half:
                return result.answer
            spec = replace(
                child,
                task=child.task
                + f"\n\nYour previous answer was too long. Answer in at most {half * 4} characters.",
            )
        raise ToolError(
            f"reducer output exceeded {half} tokens {_REDUCE_ATTEMPTS} times"
        )

    ctx.claim_slot(key)
    try:
        answer = await hierarchical_reduce(
            items, threshold, reduce_group, _estimate_tokens
        )
    except ReduceError as exc:
        raise ToolError(f"reduce {key!r} failed: {exc}") from exc
    return _result(ctx, key, answer)


def _estimate_tokens(text: str) -> int:
    return max(1, len(text) // 4)
