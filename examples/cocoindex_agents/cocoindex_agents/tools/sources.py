"""Source handles and the tools over them: a local directory, a Google Drive
folder (``drive.py``), a web search or a set of web pages (``web.py``). The
contract all keep is ``source.Source``; this module holds the local kind's
records and walk, and the ``open_source``, ``call`` and ``read_input`` tools.

The local kind's records: an ``OBSERVATION`` is keyed by the root, the
patterns and the call, and validated by walking again; a ``FILE_READ`` is
keyed by the file's path, its state the mtime and content digest (``None``
for a file that did not exist), and an unchanged mtime validates without
reading the file.
"""

from __future__ import annotations

import asyncio
import json
import os
import pathlib
from dataclasses import dataclass
from typing import Any

import cocoindex as coco
from cocoindex.connectors import localfs
from cocoindex.resources.file import PatternFilePathMatcher

from ..documents import Document, resource_media_type
from ..errors import ToolError
from ..handles import ResultValue, Volatility, is_handle
from ..spec import Item, Json
from ..transcript import digest_json, normalize_json
from . import SessionContext, drive, tool, web
from .source import Source, content_of, digest_bytes, digest_of_json, item_key_arg

# mtime and digest of a file that was read; ``None`` when it did not exist.
FileState = tuple[int, bytes] | None


@dataclass(frozen=True)
class Observation:
    """The key of a local ``OBSERVATION`` record: which source, which call."""

    root: str
    patterns: tuple[str, ...]
    method: str
    args: str  # canonical JSON of the call's arguments


async def _validate_observation(key: Observation, prev: bytes) -> coco.MemoStateOutcome:
    """Observe again; valid iff the result is byte-identical to what was recorded."""
    src = LocalDirSource(pathlib.Path(key.root), key.patterns)
    digest = digest_of_json(await src.observe(key.method, json.loads(key.args)))
    return coco.MemoStateOutcome(state=digest, memo_valid=digest == prev)


async def _validate_file_read(key: str, prev: FileState) -> coco.MemoStateOutcome:
    """An unchanged mtime validates without reading; otherwise the digest decides.

    The state handed back on a miss does not matter: the body re-runs and
    records the read afresh.
    """
    path = pathlib.Path(key)
    mtime = await asyncio.to_thread(_mtime_ns, path)
    if prev is None or mtime is None:
        return coco.MemoStateOutcome(
            state=prev, memo_valid=prev is None and mtime is None
        )
    if mtime == prev[0]:
        return coco.MemoStateOutcome(state=prev, memo_valid=True)
    try:
        digest = digest_bytes(await asyncio.to_thread(path.read_bytes))
    except OSError:  # vanished between the stat and the read
        return coco.MemoStateOutcome(state=prev)
    return coco.MemoStateOutcome(state=(mtime, digest), memo_valid=digest == prev[1])


OBSERVATION = coco.DataDep(
    "cocoindex_agents.localfs.observation", validate=_validate_observation
)
FILE_READ = coco.DataDep("cocoindex_agents.localfs.file", validate=_validate_file_read)


def _mtime_ns(path: pathlib.Path) -> int | None:
    try:
        return os.stat(path).st_mtime_ns
    except (FileNotFoundError, NotADirectoryError):
        return None


class LocalDirSource(Source[localfs.File]):
    """A ``src:`` handle over ``localfs.walk_dir``."""

    kind = "localfs"
    observation = OBSERVATION

    def __init__(self, root: pathlib.Path, patterns: tuple[str, ...]) -> None:
        self.root = root
        self.patterns = patterns

    def __coco_memo_key__(self) -> object:
        return ("localfs", str(self.root), self.patterns)

    def _matcher(self) -> PatternFilePathMatcher | None:
        return (
            PatternFilePathMatcher(included_patterns=list(self.patterns))
            if self.patterns
            else None
        )

    def walker(self) -> localfs.DirWalker:
        return localfs.walk_dir(self.root, recursive=True, path_matcher=self._matcher())

    async def resources(self) -> dict[str, localfs.File]:
        found = {key: file async for key, file in self.walker().items()}
        return dict(sorted(found.items()))

    def observation_key(self, method: str, args: str) -> object:
        return Observation(str(self.root), self.patterns, method, args)

    async def entries(self, resources: dict[str, localfs.File]) -> Json:
        def stat_all() -> list[tuple[str, int, int]]:
            out: list[tuple[str, int, int]] = []
            for key, file in resources.items():
                st = os.stat(file.file_path.resolve())
                out.append((key, st.st_mtime_ns, st.st_size))
            return out

        return await asyncio.to_thread(stat_all)

    async def read(self, args: dict[str, Json]) -> str | Document:
        """Resolved from the key directly rather than through a walk, so the
        record covers this file and not the listing: an edit elsewhere under
        the root does not touch the session.
        """
        file_key = item_key_arg(args)
        path = self._path_of(file_key)
        # Stat before reading, so the recorded mtime is never newer than the bytes.
        mtime = await asyncio.to_thread(_mtime_ns, path)
        file = localfs.File(localfs.FilePath(path))
        state: FileState = None
        data = b""
        if mtime is not None:
            try:
                data = await file.read()
                state = (mtime, digest_bytes(data))
            except (FileNotFoundError, NotADirectoryError, IsADirectoryError):
                state = None  # vanished, or the key names a directory
        coco.use_data_dep(FILE_READ, key=str(path), state=state)
        if state is None:
            raise ToolError(f"no file {file_key!r} under {self.root}")
        return await content_of(file_key, file, data)

    def _path_of(self, file_key: str) -> pathlib.Path:
        """The path a walk key denotes; a key outside the root or the patterns is the model's error."""
        root = self.root.resolve()
        path = (root / file_key).resolve()
        relative = pathlib.PurePosixPath(file_key)
        matcher = self._matcher()
        if (
            not path.is_relative_to(root)
            or path == root
            or (matcher is not None and not matcher.is_file_included(relative))
        ):
            raise ToolError(f"no file {file_key!r} under {self.root}")
        return path


# What each kind of source takes besides ``kind`` and ``name``: the shallow
# validator admits every optional argument, and the handler holds each kind to
# its own, as ``call`` holds a method to the arguments its card lists.
_KIND_ARGS: dict[str, tuple[frozenset[str], frozenset[str]]] = {
    # kind: (required, optional)
    "localfs": (frozenset({"path"}), frozenset({"patterns"})),
    "google_drive": (frozenset({"path"}), frozenset({"patterns"})),
    "web_search": (frozenset({"queries"}), frozenset()),
    "web_pages": (frozenset({"urls"}), frozenset()),
}


@tool(
    "open_source",
    "Open a source of items and get a handle (src:<name>) plus a card of its methods. "
    "Kinds: localfs (args: path, patterns?) lists the files under a directory; "
    "google_drive (args: path = the Drive folder's link or id, patterns?) lists the "
    "files of a folder, read with the Google account signed in on this machine; "
    "web_search (args: queries = a list of search queries) lists the web pages the "
    "queries find; web_pages (args: urls = a list of res: handles whose text names "
    "URLs, or of URLs) lists those pages, minus the pages a source opened in an "
    "earlier turn already listed. Every item is read by a worker through read_input.",
    {
        "type": "object",
        "properties": {
            "kind": {
                "type": "string",
                "enum": ["localfs", "google_drive", "web_search", "web_pages"],
            },
            "name": {"type": "string"},
            "path": {"type": "string"},
            "patterns": {"type": "array", "items": {"type": "string"}},
            "queries": {"type": "array", "items": {"type": "string"}},
            "urls": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["kind", "name"],
    },
)
async def open_source(ctx: SessionContext, args: dict[str, Json]) -> Json:
    kind: str = args["kind"]
    required, optional = _KIND_ARGS[kind]
    given = set(args) - {"kind", "name"}
    missing = sorted(required - given)
    foreign = sorted(given - required - optional)
    if missing or foreign:
        takes = sorted(required | optional)
        detail = []
        if missing:
            detail.append(f"needs {missing}")
        if foreign:
            detail.append(f"does not take {foreign}")
        raise ToolError(f"{kind} {' and '.join(detail)}; it takes {takes}")
    src: Source[Any]
    where: dict[str, Json]
    if kind == "web_search":
        queries = tuple(_strings(args["queries"], "queries"))
        if not queries:
            raise ToolError("web_search needs at least one query")
        if not ctx.config.search_engine:
            raise RuntimeError(
                "no search engine is configured (AgentsConfig.search_engine)"
            )
        src = web.WebSearchSource(
            ctx.config.search_engine, queries, sink=ctx.trace, ctx=ctx
        )
        where = {"engine": ctx.config.search_engine, "queries": list(queries)}
    elif kind == "web_pages":
        named = _named_urls(ctx, _strings(args["urls"], "urls"))
        exclude_requested, exclude_final = web.exclusions(
            ctx.handles.objects().values()
        )
        src = web.WebPagesSource.admit(
            named, exclude_requested, exclude_final, sink=ctx.trace, ctx=ctx
        )
        where = {"urls": list(args["urls"])}
    else:
        patterns = tuple(_strings(args.get("patterns", ()), "patterns"))
        if kind == "google_drive":
            src = drive.GoogleDriveFolderSource(
                drive.folder_id_of(args["path"]), patterns
            )
            where = {"folder": src.folder_id, "patterns": list(patterns)}
        else:
            local = LocalDirSource(pathlib.Path(args["path"]), patterns)
            src = local
            where = {"root": str(local.root), "patterns": list(patterns)}
    handle = ctx.handles.register("src", args["name"], src)
    stats = await src.call("stats", {})
    ctx.trace.note_read(f"stats of {handle}", digest_json(stats))
    if isinstance(src, web.WebSource):
        ctx.action.details.extend(src.details())
    return ctx.handles.card(handle, **where, **stats)


def _strings(value: Json, name: str) -> list[str]:
    # The shallow validator does not type array items.
    if not isinstance(value, (list, tuple)) or not all(
        isinstance(v, str) for v in value
    ):
        raise ToolError(f"{name} must be a list of strings")
    return list(value)


def _named_urls(ctx: SessionContext, entries: list[str]) -> list[str]:
    """The URLs a ``web_pages`` source is given: the URLs in each result's text,
    then the literal URLs, canonical, deduplicated, in the order given."""
    named: list[str] = []
    for entry in entries:
        if is_handle(entry):
            res = ctx.handles.get(entry)
            if not isinstance(res, ResultValue):
                raise ToolError(f"{entry!r} is a {res.kind} handle, not a result")
            urls = web.urls_in(res.text)
        else:
            url = web.canonical_url(entry)
            if url is None:
                raise ToolError(
                    f"{entry!r} is neither a res: handle nor an http(s) URL"
                )
            urls = [url]
        named.extend(u for u in urls if u not in named)
    return named


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
    """Dispatch to the handle's method after checking it against the handle's card.

    The name, the volatility gate and the argument names are all checked here,
    for every handle kind, so an object's ``call`` only ever sees a method it
    advertises with arguments it takes.
    """
    obj = ctx.handles.get(args["handle"])
    method: str = args["method"]
    spec = next((m for m in obj.methods() if m.name == method), None)
    if spec is None:
        raise ToolError(f"unknown method {method!r} on a {obj.kind} handle")
    if spec.volatility is Volatility.VOLATILE and not ctx.spec.role.volatile_ok:
        raise ToolError(
            f"{method!r} is volatile and only available to planner roles; "
            "use stats/sample, or ask a planner with observe()"
        )
    call_args = dict(args.get("args", {}))
    unknown = sorted(set(call_args) - set(spec.args))
    if unknown:
        raise ToolError(
            f"{method} does not take {unknown}; it takes "
            f"{list(spec.args) if spec.args else 'no arguments'}"
        )
    result = await obj.call(method, call_args)
    if isinstance(result, Document):
        # The bytes stay with the session; the transcript gets the reference.
        result = ctx.documents.attach(result)
    if not obj.closed_input_ok:
        # A read beyond the session's arguments: noted so the next run can
        # name it when it is the read that changed.
        key = call_args.get("key")
        what = f"{method} of {args['handle']}" + (
            f" {key}" if isinstance(key, str) else ""
        )
        ctx.trace.note_read(what, digest_json(normalize_json(result)))
    return result


@tool(
    "read_input",
    "Read this session's input: for a text item, {key, text}; for a PDF, a reference "
    "{key, media_type, bytes, digest} with the document attached to your next message; "
    "for a reduce, the list of partial results.",
    closed_ok=True,
)
async def read_input(ctx: SessionContext, args: dict[str, Json]) -> Json:
    if ctx.input is None:
        raise ToolError("this session has no input; it is an open session")
    if isinstance(ctx.input, Item):
        media_type = resource_media_type(ctx.input.key, ctx.input.resource)
        if media_type is None:
            return {"key": ctx.input.key, "text": await ctx.input.resource.read_text()}
        data = await ctx.input.resource.read()
        return ctx.documents.attach(Document.of(ctx.input.key, media_type, data))
    return list(ctx.input)
