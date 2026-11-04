"""Target handles and ``declare``. v1: a local directory of files.

Declared target states are owned by the component whose body runs the tool —
the calling session — so they are cleaned up when that session's path goes
away, exactly like hand-written CocoIndex code. A target handle may be passed
to a closed session as an input: it is write-only from the session's point of
view, and its memo key is the target's own. ``declare`` is ``closed_ok``;
``open_target`` is not (a closed session only writes to targets it was given).
"""

from __future__ import annotations

import pathlib

import cocoindex as coco
from cocoindex.connectors import localfs

from ..errors import ToolError
from ..handles import MethodSpec, ResultValue, Volatility, is_handle
from ..spec import Json
from . import SessionContext, tool


class LocalDirTargetHandle:
    """A ``tgt:`` handle over a mounted ``localfs.DirTarget``."""

    kind = "localfs_dir"
    closed_input_ok = True

    def __init__(
        self, path: pathlib.Path, target: localfs.DirTarget[coco.ResolvedS]
    ) -> None:
        self.path = path
        self.target = target

    def __coco_memo_key__(self) -> object:
        return ("localfs_dir", self.target.__coco_memo_key__())

    def methods(self) -> tuple[MethodSpec, ...]:
        return (
            MethodSpec(
                "declare_file",
                "Declare a file (args: filename, content). Prefer the declare tool.",
                Volatility.STABLE,
            ),
        )

    async def call(self, method: str, args: dict[str, Json]) -> Json:
        if method == "declare_file":
            filename, content = args.get("filename"), args.get("content")
            if not isinstance(filename, str) or not isinstance(content, str):
                raise ToolError("declare_file needs string `filename` and `content`")
            try:
                self.target.declare_file(filename, content)
            except ValueError as exc:  # e.g. an absolute or escaping filename
                raise ToolError(str(exc)) from None
            return {"declared": filename}
        raise ToolError(f"unknown method {method!r} on a directory target")


@tool(
    "open_target",
    "Mount a target and get a handle (tgt:<name>). Kinds: localfs_dir (args: path).",
    {
        "type": "object",
        "properties": {
            "kind": {"type": "string", "enum": ["localfs_dir"]},
            "name": {"type": "string"},
            "path": {"type": "string"},
        },
        "required": ["kind", "name", "path"],
    },
)
async def open_target(ctx: SessionContext, args: dict[str, Json]) -> Json:
    path = pathlib.Path(args["path"])
    target = await localfs.mount_dir_target(path)
    handle = ctx.handles.register(
        "tgt", args["name"], LocalDirTargetHandle(path, target)
    )
    return ctx.handles.card(handle, path=str(path))


@tool(
    "declare",
    "Declare a target state: for a directory target, a file `key` with `value`. "
    "`value` is literal text, or a res: handle that resolves to this run's result.",
    {
        "type": "object",
        "properties": {
            "target": {"type": "string"},
            "key": {"type": "string"},
            "value": {"type": "string"},
        },
        "required": ["target", "key", "value"],
    },
    closed_ok=True,
)
async def declare(ctx: SessionContext, args: dict[str, Json]) -> Json:
    obj = ctx.handles.get(args["target"])
    value: str = args["value"]
    if is_handle(value):
        # A reference must resolve to a result; a stale or wrong-kind one is an
        # error, never literal content.
        res = ctx.handles.get(value)
        if not isinstance(res, ResultValue):
            raise ToolError(f"{value!r} is a {res.kind} handle, not a result")
        value = res.text
    return await obj.call("declare_file", {"filename": args["key"], "content": value})
