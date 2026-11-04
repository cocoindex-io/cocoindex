"""Source handles. v1: a local directory. Other connectors follow the same shape.

A source handle exposes stable observations (widened ``stats``, hash-stable
``sample``), per-item reads, and — for planners only — the volatile ``keys``.
A source is an *open* dependency (its contents are not an argument of anyone),
so it can only be an input of open sessions, and ``open_source`` is not a
``closed_ok`` tool.
"""

from __future__ import annotations

import pathlib

from cocoindex.connectors import localfs
from cocoindex.resources.file import PatternFilePathMatcher

from ..errors import ToolError
from ..handles import MethodSpec, Volatility, int_arg
from ..spec import Item, Json
from . import SessionContext, tool
from .stats import bottom_k, format_range, widen_count

_SAMPLE_HEAD_CHARS = 400


class LocalDirSource:
    """A ``src:`` handle over ``localfs.walk_dir``."""

    kind = "localfs"
    closed_input_ok = False

    def __init__(self, root: pathlib.Path, patterns: tuple[str, ...]) -> None:
        self.root = root
        self.patterns = patterns

    def walker(self) -> localfs.DirWalker:
        matcher = (
            PatternFilePathMatcher(included_patterns=list(self.patterns))
            if self.patterns
            else None
        )
        return localfs.walk_dir(self.root, recursive=True, path_matcher=matcher)

    async def files(self) -> dict[str, localfs.File]:
        """One walk: every file by its key, in key order."""
        found = {key: file async for key, file in self.walker().items()}
        return dict(sorted(found.items()))

    def methods(self) -> tuple[MethodSpec, ...]:
        return (
            MethodSpec(
                "stats",
                "Widened file count (a range, not an exact number).",
                Volatility.STABLE,
            ),
            MethodSpec(
                "sample",
                "`k` representative files (default 5) with the head of each; the set is "
                "hash-stable, so it barely changes when files are added.",
                Volatility.STABLE,
            ),
            MethodSpec("read", "Full text of one file by key.", Volatility.PER_ITEM),
            MethodSpec(
                "keys",
                "Every file key (optionally under `prefix`). Volatile: planners only.",
                Volatility.VOLATILE,
            ),
        )

    async def call(self, method: str, args: dict[str, Json]) -> Json:
        files = await self.files()
        if method == "stats":
            # Target (DESIGN.md §5): hysteresis against the previous bucket, kept in
            # use_state on the session component. Not implemented yet.
            return {"files": format_range(widen_count(len(files)))}
        if method == "sample":
            k = int_arg(args, "k", 5)
            return [
                {
                    "key": key,
                    "head": (await files[key].read_text())[:_SAMPLE_HEAD_CHARS],
                }
                for key in bottom_k(files, k)
            ]
        if method == "read":
            file_key = args.get("key")
            if not isinstance(file_key, str):
                raise ToolError("read needs a string `key`")
            if file_key not in files:
                raise ToolError(f"no file {file_key!r} under {self.root}")
            return await files[file_key].read_text()
        if method == "keys":
            prefix = str(args.get("prefix", ""))
            return [k for k in files if k.startswith(prefix)]
        raise ToolError(f"unknown method {method!r} on a localfs source")


@tool(
    "open_source",
    "Open a data source and get a handle (src:<name>) plus a card of its methods. "
    "Kinds: localfs (args: path, patterns?).",
    {
        "type": "object",
        "properties": {
            "kind": {"type": "string", "enum": ["localfs"]},
            "name": {"type": "string"},
            "path": {"type": "string"},
            "patterns": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["kind", "name", "path"],
    },
)
async def open_source(ctx: SessionContext, args: dict[str, Json]) -> Json:
    src = LocalDirSource(pathlib.Path(args["path"]), tuple(args.get("patterns", ())))
    handle = ctx.handles.register("src", args["name"], src)
    count = len(await src.files())
    return ctx.handles.card(
        handle,
        root=str(src.root),
        patterns=list(src.patterns),
        files=format_range(widen_count(count)),
    )


@tool(
    "call",
    "Call a method on an opened handle (see the handle's card for methods).",
    {
        "type": "object",
        "properties": {
            "handle": {"type": "string"},
            "method": {"type": "string"},
            "args": {"type": "object"},
        },
        "required": ["handle", "method"],
    },
    closed_ok=True,  # only ever reaches seeded handles in a closed session
)
async def call_method(ctx: SessionContext, args: dict[str, Json]) -> Json:
    obj = ctx.handles.get(args["handle"])
    method: str = args["method"]
    allowed_volatile = ctx.spec.role.name == "planner"
    for m in obj.methods():
        if (
            m.name == method
            and m.volatility is Volatility.VOLATILE
            and not allowed_volatile
        ):
            raise ToolError(
                f"{method!r} is volatile and only available inside observe(); "
                "use stats/sample, or ask a planner"
            )
    return await obj.call(method, dict(args.get("args", {})))


@tool(
    "read_input",
    "Read this session's input: for a file, {key, text}; for a reduce, the list of partial results.",
    closed_ok=True,
)
async def read_input(ctx: SessionContext, args: dict[str, Json]) -> Json:
    if ctx.input is None:
        raise ToolError("this session has no input; it is an open session")
    if isinstance(ctx.input, Item):
        return {"key": ctx.input.key, "text": await ctx.input.file.read_text()}
    return list(ctx.input)
