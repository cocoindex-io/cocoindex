"""Subagent tools: call_agent, start_agent, for_each, observe, reduce.

Each mounts its child with a ``SessionScratch`` (excluded from the child's
memo key) and leaves the child's trace in ``ctx.action``: untouched scratch
means the engine skipped the child, and its stored trace is marked reused.

Each maps to one CocoIndex primitive (DESIGN.md §3). Every model-named child
lives under ``child/<key>`` (items and reduce groups nest below that), so a
key used once per session means one engine path per child and distinct keys
cannot alias; renaming a key recomputes that subtree and cleans up the old
one. A key reused within a session is a tool error, never an engine error.
``inputs`` names the parent's handles a child may see; the child's spec lists
them as cards so its model knows their names.

A child that fails committed nothing and runs again next run; its failure is
reported to the parent model as data (DESIGN.md §7): ``call_agent`` and
``reduce`` fail as tool errors naming the child, ``for_each`` records failed
items in the collection.
"""

from __future__ import annotations

from dataclasses import replace

import cocoindex as coco

from ..errors import ToolError, record_contained_failure
from ..handles import HandleObject, MethodSpec, ResultValue, Volatility, input_card
from ..session import (
    is_environment_error,
    run_closed_session,
    run_open_session,
    run_reduce_session,
)
from ..spec import InputCard, Item, Json, SessionSpec
from ..stable_hash import stable_hash64
from ..trace import (
    ChildTrace,
    SessionScratch,
    SessionTrace,
    child_members,
    child_session,
)
from ..transcript import canonical_json
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

    Lives only for the run. The card shows a widened count and how many items
    failed, never the contents; ``reduce`` consumes the successes. Not an input
    for closed sessions. Its memo key is its contents: a child given a
    collection is reused only while every item's output is unchanged, since
    nothing a validator could re-observe from a stored key stands behind it.
    """

    kind = "collection"
    closed_input_ok = False

    def __init__(self, entries: list[tuple[str, str]], failed: dict[str, str]) -> None:
        self.entries = sorted(entries)
        self.failed = dict(sorted(failed.items()))

    def __coco_memo_key__(self) -> object:
        return (
            "collection",
            stable_hash64(canonical_json(self.entries), canonical_json(self.failed)),
        )

    def methods(self) -> tuple[MethodSpec, ...]:
        return (
            MethodSpec(
                "stats",
                "Widened item count and the number of failed items.",
                Volatility.STABLE,
            ),
            MethodSpec(
                "get", "One item's output by key.", Volatility.PER_ITEM, args=("key",)
            ),
            MethodSpec(
                "failed", "The failed items and their errors.", Volatility.STABLE
            ),
        )

    async def call(self, method: str, args: dict[str, Json]) -> Json:
        if method == "stats":
            return self.card_fields()
        if method == "get":
            for key, value in self.entries:
                if key == args.get("key"):
                    return value
            raise ToolError(f"no item {args.get('key')!r}")
        if method == "failed":
            return self.failed
        raise ToolError(f"unknown method {method!r} on a collection")

    def card_fields(self) -> dict[str, Json]:
        return {
            "items": format_range(widen_count(len(self.entries))),
            "failed": len(self.failed),
        }


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


def _result(ctx: SessionContext, name: str, text: str, **fields: Json) -> Json:
    """Every result gets a res: handle; the transcript sees the handle and a size, not the text."""
    handle = ctx.handles.register("res", name, ResultValue(text))
    return {"result": handle, "chars": format_range(widen_count(len(text))), **fields}


def _contain_child_failure(
    ctx: SessionContext, key: str, exc: BaseException
) -> ToolError:
    """A child's environment failure, as the parent model sees it.

    Recorded in the parent, so the parent re-runs and the child runs again
    next run (DESIGN.md §7).
    """
    record_contained_failure(ctx.trace, f"child:{key}", exc)
    return ToolError(f"child {key!r} failed: {type(exc).__name__}: {exc}")


_OPEN_SKIPPED = "nothing it read changed"
_CLOSED_SKIPPED = "input unchanged"


@tool(
    "call_agent",
    "Run one open subagent with `task` and wait for it. `key` names it stably across "
    "runs; `inputs` lists the handles it may use. Returns a res: handle to its answer, "
    "never the text. For one piece of work over the whole data; per-file work is "
    "for_each.",
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
    scratch = SessionScratch()
    try:
        result = await coco.use_mount(
            coco.component_subpath("child", key),
            run_open_session,
            child,
            handles,
            scratch,
        )
    except Exception as exc:
        if not is_environment_error(exc):
            raise
        raise _contain_child_failure(ctx, key, exc) from exc
    ctx.action.child = child_session(
        key, result.trace, ran=scratch.ran, skipped_reason=_OPEN_SKIPPED
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
    ctx.action.child = ChildTrace("background", key, child.role.name)
    return {"job": f"job:{key}"}


@tool(
    "observe",
    "Ask a planner to look at the data and answer `question` with a plan. Give it the "
    "source in `inputs`; it may list and sample freely. Use it before a fan-out for a "
    "choice the data decides (a table's columns, the categories, a format the task "
    "leaves open). Only the plan's text comes back; put it verbatim into the tasks "
    "that depend on it.",
    {
        "type": "object",
        "properties": {
            "key": {"type": "string"},
            "question": {"type": "string"},
            "role": {"type": "string"},
            "inputs": _INPUTS_SCHEMA,
        },
        "required": ["key", "question"],
    },
    key_arg="key",
)
async def observe(ctx: SessionContext, args: dict[str, Json]) -> Json:
    key: str = args["key"]
    handles, cards = _child_inputs(ctx, args, closed=False)
    child = ctx.config.spec(args.get("role", "planner"), args["question"], inputs=cards)
    ctx.claim_slot(key)
    scratch = SessionScratch()
    try:
        result = await coco.use_mount(
            coco.component_subpath("child", key),
            run_open_session,
            child,
            handles,
            scratch,
        )
    except Exception as exc:
        if not is_environment_error(exc):
            raise
        raise _contain_child_failure(ctx, key, exc) from exc
    ctx.action.child = child_session(
        key,
        result.trace,
        ran=scratch.ran,
        skipped_reason=_OPEN_SKIPPED,
        plan=result.answer,
    )
    return {"plan": result.answer}


@tool(
    "for_each",
    "Run one closed worker per file of a source, in parallel. `key` names the fan-out "
    "stably across runs; `task` is every worker's instruction (each reads its own file "
    "with read_input); `inputs` lists the handles the workers may use: the target they "
    "write to, a result they must follow. With collect=true (the default) the answers are "
    "gathered into a coll: handle for reduce; its card shows a widened count and how many "
    "workers failed, never the answers. collect=false runs the workers in the background "
    "and returns no card.",
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
    # The walk is this session's read of the source: recorded, so a changed
    # file re-runs the session and the workers are probed (DESIGN.md §7).
    listing = await src.listing()
    ctx.trace.note_read(f"walk of {args['source']}", listing.digest)
    items = listing.items

    ctx.claim_slot(key)
    if not args.get("collect", True):
        await coco.mount_each(
            coco.component_subpath("child", key),
            run_closed_session,
            items.items(),
            child,
            handles,
        )
        ctx.action.child = ChildTrace("background", key, child.role.name)
        return {"mounted": key}

    failed: dict[str, str] = {}
    traces: list[tuple[str, SessionTrace, bool]] = []

    async def one(kv: tuple[str, Item]) -> tuple[str, str | None]:
        item_key, item = kv
        scratch = SessionScratch()
        try:
            result = await coco.use_mount(
                coco.component_subpath("child", key, item_key),
                run_closed_session,
                item,
                child,
                handles,
                scratch,
            )
        except Exception as exc:
            if not is_environment_error(exc):
                raise
            # The worker committed nothing and runs again next run; the parent
            # model sees the failure in the collection, not as a crash.
            record_contained_failure(ctx.trace, f"child:{key}/{item_key}", exc)
            failed[item_key] = f"{type(exc).__name__}: {exc}"
            return item_key, None
        traces.append((item_key, result.trace, scratch.ran))
        return item_key, result.answer

    results = await coco.map(one, items.items())
    coll = Collection([(k, a) for k, a in results if a is not None], failed)
    handle = ctx.handles.register("coll", key, coll)
    ctx.action.child = child_members(
        "fan_out",
        key,
        child.role.name,
        traces,
        failed=len(failed),
        skipped_reason=_CLOSED_SKIPPED,
    )
    return ctx.handles.card(handle, **coll.card_fields())


@tool(
    "reduce",
    "Combine every successful item of a coll: handle into one text with `task`, "
    "hierarchically, so any number of items fits. Returns a res: handle to the text and "
    'how many failed items were left out; write it with declare(value="res:<key>").',
    {
        "type": "object",
        "properties": {
            "collection": {"type": "string"},
            "key": {"type": "string"},
            "task": {"type": "string"},
            "role": {"type": "string"},
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
    child = _closed_spec(ctx, args.get("role", "reducer"), args["task"], ())
    threshold: int = args.get("threshold_tokens", _DEFAULT_REDUCE_THRESHOLD_TOKENS)
    if threshold < 2:
        raise ToolError("threshold_tokens must be at least 2")
    half = threshold // 2

    items: list[GroupItem[str]] = [
        leaf_item(item_key, _estimate_tokens(text), text)
        for item_key, text in coll.entries
    ]

    traces: list[tuple[str, SessionTrace, bool]] = []

    async def reduce_group(group: Group[str]) -> str:
        # Slot: the group's prefix (unique across levels, DESIGN §6) plus the
        # attempt. Reuse needs the same slot and the same members (§7).
        members = tuple(m.payload for m in group.members)
        spec = child
        for attempt in range(_REDUCE_ATTEMPTS):
            slot = group.prefix or "root"
            scratch = SessionScratch()
            result = await coco.use_mount(
                coco.component_subpath("child", key, slot, attempt),
                run_reduce_session,
                members,
                spec,
                scratch,
            )
            traces.append((f"{slot}/{attempt}", result.trace, scratch.ran))
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
    except Exception as exc:
        if not is_environment_error(exc):
            raise
        raise _contain_child_failure(ctx, key, exc) from exc
    ctx.action.child = child_members(
        "reduce",
        key,
        child.role.name,
        traces,
        failed=0,
        skipped_reason="members unchanged",
    )
    return _result(ctx, key, answer, omitted=len(coll.failed))


def _estimate_tokens(text: str) -> int:
    return max(1, len(text) // 4)
