"""The contract a folder source keeps, whatever holds the folder (``sources.py``, ``drive.py``).

A source handle exposes stable observations (widened ``stats``, hash-stable
``sample``), per-item reads, and — for planners only — the volatile ``keys``.
A source's contents are not an argument of anyone, so every read through it is
**recorded** as a data dependency of the session that made it
(``coco.use_data_dep``, DESIGN.md §7): the session is skipped next run only
while every recorded read validates. Two kinds of record per source kind:

- an *observation* — ``stats``, ``sample``, ``keys`` and the ``listing`` a
  ``for_each`` walks. Keyed by the source and the call; the state is a digest
  of what came back. Validated by observing again and comparing, so a change
  that leaves the observation byte-identical (a new file inside the same
  widened bucket, a sample that did not move) keeps the session skippable.
- a *file read* — ``read(key)``. Keyed by the file; the state is its
  modification time and content digest, or ``None`` when it did not exist. An
  unchanged modification time validates without reading the file.

``FolderSource`` holds what both kinds share: the methods and their cards,
the recording, and the observations computed from a walk. A kind supplies the
walk, the record keys, the listing entries and the read.

A source may only be an input of open sessions: a closed session's memo key
must cover everything it reads, and ``open_source`` is not ``closed_ok``.

What a read returns is decided by the file's media type (``documents.py``):
text as text; a document (a PDF) as a reference, with the bytes attached to
the session for the client to render. ``sample`` shows a document's media
type and size in place of a text head.
"""

from __future__ import annotations

import hashlib
from abc import ABC, abstractmethod
from typing import Any, ClassVar, Generic, NamedTuple, TypeVar

import cocoindex as coco
from cocoindex.resources.file import FileLike

from ..documents import Document, document_media_type
from ..errors import ToolError
from ..handles import MethodSpec, Volatility, int_arg
from ..spec import Item, Json
from ..transcript import canonical_json, digest_json
from .stats import bottom_k, format_range, widen_count

_SAMPLE_HEAD_CHARS = 400

# What ``for_each`` walks: every file by key with what identifies its content,
# so an edit, a touch, an addition or a removal all re-run the fan-out's
# session. Not a model-visible method: ``call`` rejects it.
LISTING = "listing"


def digest_bytes(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def digest_of_json(value: Json) -> bytes:
    return digest_bytes(canonical_json(value).encode("utf-8"))


# The file type a kind's walk yields: ``localfs.File``, ``google_drive.DriveFile``.
FileT = TypeVar("FileT", bound=FileLike[Any])


class Listing(NamedTuple):
    items: dict[str, Item]
    digest: str  # of the walk, for the session's trace


class FolderSource(ABC, Generic[FileT]):
    """A ``src:`` handle over a folder of files, whatever holds the folder.

    Its memo key is its identity, not its contents: what a session read
    through it is recorded per call, as the module docstring says. A kind
    supplies ``files`` (the walk), ``observation_key`` (what identifies an
    observation of it in a record), ``entries`` (what the fan-out's walk
    records per file) and ``read``.
    """

    kind: ClassVar[str]
    observation: ClassVar[coco.DataDep]  # the kind's observation records
    closed_input_ok = False
    patterns: tuple[str, ...]

    @abstractmethod
    def __coco_memo_key__(self) -> object: ...

    @abstractmethod
    async def files(self) -> dict[str, FileT]:
        """One walk: every file by its key, in key order. Records nothing."""

    @abstractmethod
    def observation_key(self, method: str, args: str) -> object:
        """The record key of one observation (``args`` is the call's canonical JSON)."""

    @abstractmethod
    async def entries(self, files: dict[str, FileT]) -> Json:
        """What the fan-out's walk records: every file with what identifies its content."""

    @abstractmethod
    async def read(self, args: dict[str, Json]) -> str | Document:
        """One file, recorded as a read of that file alone: its text, or a document."""

    def methods(self) -> tuple[MethodSpec, ...]:
        return (
            MethodSpec(
                "stats",
                "Widened file count (a range, not an exact number).",
                Volatility.STABLE,
            ),
            MethodSpec(
                "sample",
                "`k` representative files (default 5) with the head of each text file; "
                "a PDF shows its media type and size instead. The set is hash-stable, "
                "so it barely changes when files are added.",
                Volatility.STABLE,
                args=("k",),
            ),
            MethodSpec(
                "read",
                "One file by key: its full text, or for a PDF a reference with the "
                "document attached to your next message.",
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
            raise ToolError(f"unknown method {method!r} on a {self.kind} source")
        return await self._observe_recorded(method, args)

    async def listing(self) -> Listing:
        """Every file as a ``for_each`` item, recorded as one observation of the walk."""
        files = await self.files()
        walk = await self._observe_recorded(LISTING, {}, files)
        return Listing(
            {key: Item(key, file) for key, file in files.items()}, digest_json(walk)
        )

    async def _observe_recorded(
        self,
        method: str,
        args: dict[str, Json],
        files: dict[str, FileT] | None = None,
    ) -> Json:
        result = await self.observe(method, args, files)
        coco.use_data_dep(
            self.observation,
            key=self.observation_key(method, canonical_json(args)),
            state=digest_of_json(result),
        )
        return result

    async def observe(
        self,
        method: str,
        args: dict[str, Json],
        files: dict[str, FileT] | None = None,
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
            return [
                await _sample_entry(key, files[key])
                for key in bottom_k(files, int_arg(args, "k", 5))
            ]
        if method == "keys":
            prefix = str(args.get("prefix", ""))
            return [k for k in files if k.startswith(prefix)]
        if method == LISTING:
            return await self.entries(files)
        raise ValueError(f"not an observation: {method!r}")  # call() screens the model


def file_key_arg(args: dict[str, Json]) -> str:
    file_key = args.get("key")
    if not isinstance(file_key, str):
        raise ToolError("read needs a string `key`")
    return file_key


async def content_of(key: str, file: FileLike[Any], data: bytes) -> str | Document:
    """What ``read`` returns for a file whose bytes ``data`` were just read: its
    text (decoded by the file, from the bytes it cached), or the document itself.

    A document is returned as is; the ``call`` tool attaches it to the
    session and returns its reference.
    """
    media_type = document_media_type(key)
    if media_type is None:
        return await file.read_text()
    return Document.of(key, media_type, data)


async def _sample_entry(key: str, file: FileLike[Any]) -> dict[str, Json]:
    """A text file's head; a document's media type and size (its content is not text)."""
    media_type = document_media_type(key)
    if media_type is None:
        return {"key": key, "head": (await file.read_text())[:_SAMPLE_HEAD_CHARS]}
    return {"key": key, "media_type": media_type, "bytes": await file.size()}
