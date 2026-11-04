"""Source handles. v1: a local directory. Other connectors follow the same shape.

A source handle exposes stable observations (widened ``stats``, hash-stable
``sample``), per-item reads, and — for planners only — the volatile ``keys``.
A source's contents are not an argument of anyone, so every read through it is
**recorded** as a data dependency of the session that made it
(``coco.use_data_dep``, DESIGN.md §7): the session is skipped next run only
while every recorded read validates. Two kinds of record:

- ``OBSERVATION`` — ``stats``, ``sample``, ``keys`` and the ``listing`` a
  ``for_each`` walks. Keyed by the source and the call; the state is a digest
  of what came back. Validated by observing again and comparing, so a change
  that leaves the observation byte-identical (a new file inside the same
  widened bucket, a sample that did not move) keeps the session skippable.
- ``FILE_READ`` — ``read(key)``. Keyed by the file's path; the state is its
  mtime and content digest, or ``None`` when it did not exist. An unchanged
  mtime validates without reading the file.

A source may only be an input of open sessions: a closed session's memo key
must cover everything it reads, and ``open_source`` is not ``closed_ok``.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import pathlib
from dataclasses import dataclass

import cocoindex as coco
from cocoindex.connectors import localfs
from cocoindex.resources.file import PatternFilePathMatcher

from ..errors import ToolError
from ..handles import MethodSpec, Volatility, int_arg
from ..spec import Item, Json
from ..transcript import canonical_json
from . import SessionContext, tool
from .stats import bottom_k, format_range, widen_count

_SAMPLE_HEAD_CHARS = 400

# What ``for_each`` walks: every file by key with its mtime and size, so an
# edit, a touch, an addition or a removal all re-run the fan-out's session.
# Not a model-visible method: ``call`` rejects it.
_LISTING = "listing"

# mtime and digest of a file that was read; ``None`` when it did not exist.
FileState = tuple[int, bytes] | None


@dataclass(frozen=True)
class Observation:
    """The key of an ``OBSERVATION`` record: which source, which call."""

    root: str
    patterns: tuple[str, ...]
    method: str
    args: str  # canonical JSON of the call's arguments


def _digest(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def _digest_json(value: Json) -> bytes:
    return _digest(canonical_json(value).encode("utf-8"))


async def _validate_observation(key: Observation, prev: bytes) -> coco.MemoStateOutcome:
    """Observe again; valid iff the result is byte-identical to what was recorded."""
    src = LocalDirSource(pathlib.Path(key.root), key.patterns)
    digest = _digest_json(await src.observe(key.method, json.loads(key.args)))
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
        digest = _digest(await asyncio.to_thread(path.read_bytes))
    except OSError:  # vanished between the stat and the read
        return coco.MemoStateOutcome(state=prev)
    return coco.MemoStateOutcome(state=(mtime, digest), memo_valid=digest == prev[1])


OBSERVATION = coco.DataDep(
    "coco_harness.localfs.observation", validate=_validate_observation
)
FILE_READ = coco.DataDep("coco_harness.localfs.file", validate=_validate_file_read)


def _mtime_ns(path: pathlib.Path) -> int | None:
    try:
        return os.stat(path).st_mtime_ns
    except (FileNotFoundError, NotADirectoryError):
        return None


class LocalDirSource:
    """A ``src:`` handle over ``localfs.walk_dir``.

    Its memo key is its identity (root and patterns), not its contents: what a
    session read through it is recorded per call, as the module docstring says.
    """

    kind = "localfs"
    closed_input_ok = False

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
        """One walk: every file by its key, in key order. Records nothing."""
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
                args=("k",),
            ),
            MethodSpec(
                "read",
                "Full text of one file by key.",
                Volatility.PER_ITEM,
                args=("key",),
            ),
            MethodSpec(
                "keys",
                "Every file key (optionally under `prefix`). Volatile: planners only.",
                Volatility.VOLATILE,
                args=("prefix",),
            ),
        )

    async def call(self, method: str, args: dict[str, Json]) -> Json:
        """A model-visible method: observe or read, and record what was seen."""
        if method == "read":
            return await self.read(args)
        if method not in {m.name for m in self.methods()}:
            raise ToolError(f"unknown method {method!r} on a localfs source")
        return await self._observe_recorded(method, args)

    async def listing(self) -> dict[str, Item]:
        """Every file as a ``for_each`` item, recorded as one observation of the walk."""
        files = await self.files()
        await self._observe_recorded(_LISTING, {}, files)
        return {key: Item(key, file) for key, file in files.items()}

    async def _observe_recorded(
        self,
        method: str,
        args: dict[str, Json],
        files: dict[str, localfs.File] | None = None,
    ) -> Json:
        result = await self.observe(method, args, files)
        coco.use_data_dep(
            OBSERVATION,
            key=Observation(
                str(self.root), self.patterns, method, canonical_json(args)
            ),
            state=_digest_json(result),
        )
        return result

    async def observe(
        self,
        method: str,
        args: dict[str, Json],
        files: dict[str, localfs.File] | None = None,
    ) -> Json:
        """Compute an observation without recording it (the validator's path).

        ``files`` is the walk to observe; a caller that already walked passes it.
        """
        if files is None:
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
        if method == "keys":
            prefix = str(args.get("prefix", ""))
            return [k for k in files if k.startswith(prefix)]
        if method == _LISTING:
            return await _listing(files)
        raise ValueError(f"not an observation: {method!r}")  # call() screens the model

    async def read(self, args: dict[str, Json]) -> str:
        """Full text of one file, recorded as a read of that file alone.

        Resolved from the key directly rather than through a walk, so the
        record covers this file and not the listing: an edit elsewhere under
        the root does not touch the session.
        """
        file_key = args.get("key")
        if not isinstance(file_key, str):
            raise ToolError("read needs a string `key`")
        path = self._path_of(file_key)
        # Stat before reading, so the recorded mtime is never newer than the bytes.
        mtime = await asyncio.to_thread(_mtime_ns, path)
        file = localfs.File(localfs.FilePath(path))
        state: FileState = None
        if mtime is not None:
            try:
                state = (mtime, _digest(await file.read()))
            except (FileNotFoundError, NotADirectoryError, IsADirectoryError):
                state = None  # vanished, or the key names a directory
        coco.use_data_dep(FILE_READ, key=str(path), state=state)
        if state is None:
            raise ToolError(f"no file {file_key!r} under {self.root}")
        return await file.read_text()  # decodes the bytes read above

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


async def _listing(files: dict[str, localfs.File]) -> list[tuple[str, int, int]]:
    def stat_all() -> list[tuple[str, int, int]]:
        out: list[tuple[str, int, int]] = []
        for key, file in files.items():
            st = os.stat(file.file_path.path)
            out.append((key, st.st_mtime_ns, st.st_size))
        return out

    return await asyncio.to_thread(stat_all)


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
    stats = await src.call("stats", {})
    return ctx.handles.card(
        handle, root=str(src.root), patterns=list(src.patterns), **stats
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
    return await obj.call(method, call_args)


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
