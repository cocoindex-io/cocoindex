"""Source handles and the tools over them: a local directory, or a Google Drive
folder (``drive.py``). The contract both keep is ``folder.FolderSource``; this
module holds the local kind's records and walk, and the ``open_source``,
``call`` and ``read_input`` tools.

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

from ..documents import Document, document_media_type
from ..errors import ToolError
from ..handles import Volatility
from ..spec import Item, Json
from ..transcript import digest_json, normalize_json
from . import SessionContext, drive, tool
from .folder import FolderSource, content_of, digest_bytes, digest_of_json, file_key_arg

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


class LocalDirSource(FolderSource[localfs.File]):
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

    async def files(self) -> dict[str, localfs.File]:
        found = {key: file async for key, file in self.walker().items()}
        return dict(sorted(found.items()))

    def observation_key(self, method: str, args: str) -> object:
        return Observation(str(self.root), self.patterns, method, args)

    async def entries(self, files: dict[str, localfs.File]) -> Json:
        def stat_all() -> list[tuple[str, int, int]]:
            out: list[tuple[str, int, int]] = []
            for key, file in files.items():
                st = os.stat(file.file_path.resolve())
                out.append((key, st.st_mtime_ns, st.st_size))
            return out

        return await asyncio.to_thread(stat_all)

    async def read(self, args: dict[str, Json]) -> str | Document:
        """Resolved from the key directly rather than through a walk, so the
        record covers this file and not the listing: an edit elsewhere under
        the root does not touch the session.
        """
        file_key = file_key_arg(args)
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


@tool(
    "open_source",
    "Open a data source and get a handle (src:<name>) plus a card of its methods. "
    "Kinds: localfs (args: path, patterns?); google_drive (args: path = the Drive "
    "folder's link or id, patterns?), read with the Google account signed in on "
    "this machine.",
    {
        "type": "object",
        "properties": {
            "kind": {"type": "string", "enum": ["localfs", "google_drive"]},
            "name": {"type": "string"},
            "path": {"type": "string"},
            "patterns": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["kind", "name", "path"],
    },
)
async def open_source(ctx: SessionContext, args: dict[str, Json]) -> Json:
    patterns = tuple(args.get("patterns", ()))
    src: FolderSource[Any]
    where: dict[str, Json]
    if args["kind"] == "google_drive":
        src = drive.GoogleDriveFolderSource(drive.folder_id_of(args["path"]), patterns)
        where = {"folder": src.folder_id}
    else:
        src = LocalDirSource(pathlib.Path(args["path"]), patterns)
        where = {"root": str(src.root)}
    handle = ctx.handles.register("src", args["name"], src)
    stats = await src.call("stats", {})
    ctx.trace.note_read(f"stats of {handle}", digest_json(stats))
    return ctx.handles.card(handle, **where, patterns=list(src.patterns), **stats)


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
    "Read this session's input: for a text file, {key, text}; for a PDF, a reference "
    "{key, media_type, bytes, digest} with the document attached to your next message; "
    "for a reduce, the list of partial results.",
    closed_ok=True,
)
async def read_input(ctx: SessionContext, args: dict[str, Json]) -> Json:
    if ctx.input is None:
        raise ToolError("this session has no input; it is an open session")
    if isinstance(ctx.input, Item):
        media_type = document_media_type(ctx.input.key)
        if media_type is None:
            return {"key": ctx.input.key, "text": await ctx.input.file.read_text()}
        data = await ctx.input.file.read()
        return ctx.documents.attach(Document.of(ctx.input.key, media_type, data))
    return list(ctx.input)
