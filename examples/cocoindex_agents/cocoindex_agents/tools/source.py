"""The contract every source keeps, whatever it lists (DESIGN.md §0, §3).

Vocabulary: a **resource** is one thing a worker can read whole — a local
file, a Drive file, a web page — with an identity that is global within its
kind's space and a content state its memo state carries; it is a ``FileLike``
(``cocoindex.resources.file``). A **listing** is how a session finds
resources: walking a folder, running searches, taking a list of URLs; it is
session-local and recorded as an observation. A **source** (``src:`` handle)
is a listing with four methods: widened ``stats``, hash-stable ``sample``,
per-item ``read``, and — for planners only — the volatile ``keys``. An
**item** is one listed resource under a source-local key, the unit of a
``for_each``.

A source's contents are not an argument of anyone, so every read through it
is **recorded** as a data dependency of the session that made it
(``coco.use_data_dep``, DESIGN.md §7): the session is skipped next run only
while every recorded read validates. Two kinds of record per source kind:

- an *observation* — ``stats``, ``sample``, ``keys`` and the ``listing`` a
  ``for_each`` walks. Keyed by the source and the call; the state is, by
  default, a digest of what came back, validated by observing again and
  comparing, so a change that leaves the observation byte-identical (a new
  item inside the same widened bucket, a sample that did not move) keeps the
  session skippable. A kind may record a richer state (``web.py`` records
  what its validator needs to reproduce a listing).
- a *resource read* — ``read(key)``. Keyed by the resource; the state is what
  identifies its content (a file's modification time and digest, a page's
  validators and digest), or ``None`` when it did not exist.

``Source`` holds what every kind shares: the methods and their cards, the
recording, and the observations computed from a listing. A kind supplies the
listing (``resources``), the record keys, the listing entries and the read.
The kinds: ``sources.LocalDirSource`` and ``drive.GoogleDriveFolderSource``
list files; ``web.WebSearchSource`` and ``web.WebPagesSource`` list pages.

A source may only be an input of open sessions: a closed session's memo key
must cover everything it reads, and ``open_source`` is not ``closed_ok``.

What a read returns is decided by the resource's media type
(``documents.py``): text as text; a document (a PDF) as a reference, with the
bytes attached to the session for the client to render. ``sample`` shows a
document's media type and size in place of a text head.
"""

from __future__ import annotations

import hashlib
from abc import ABC, abstractmethod
from typing import Any, ClassVar, Generic, NamedTuple, TypeVar

import cocoindex as coco
from cocoindex.resources.file import FileLike

from ..documents import Document, resource_media_type
from ..errors import ToolError
from ..handles import MethodSpec, Volatility, int_arg
from ..spec import Item, Json
from ..transcript import canonical_json, digest_json
from .stats import bottom_k, format_range, widen_count

_SAMPLE_HEAD_CHARS = 400

# What ``for_each`` walks: every item by key with what identifies its content,
# so an edit, a touch, an addition or a removal all re-run the fan-out's
# session. Not a model-visible method: ``call`` rejects it.
LISTING = "listing"


def digest_bytes(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def digest_of_json(value: Json) -> bytes:
    return digest_bytes(canonical_json(value).encode("utf-8"))


# The resource type a kind lists: ``localfs.File``, ``google_drive.DriveFile``,
# ``web.WebPage``.
ResourceT = TypeVar("ResourceT", bound=FileLike[Any])


class Listing(NamedTuple):
    items: dict[str, Item]
    digest: str  # of the walk, for the session's trace


class Source(ABC, Generic[ResourceT]):
    """A ``src:`` handle: a listing of resources, whatever it lists.

    Its memo key is its identity, not its contents: what a session read
    through it is recorded per call, as the module docstring says. A kind
    supplies ``resources`` (the listing), ``observation_key`` (what identifies
    an observation of it in a record), ``entries`` (what the fan-out's listing
    records per item), ``read``, and may override ``observation_state``.
    """

    kind: ClassVar[str]
    observation: ClassVar[coco.DataDep]  # the kind's observation records
    closed_input_ok = False
    patterns: tuple[str, ...]

    @abstractmethod
    def __coco_memo_key__(self) -> object: ...

    @abstractmethod
    async def resources(self) -> dict[str, ResourceT]:
        """One listing: every resource by its item key, in key order. Records nothing."""

    @abstractmethod
    def observation_key(self, method: str, args: str) -> object:
        """The record key of one observation (``args`` is the call's canonical JSON)."""

    @abstractmethod
    async def entries(self, resources: dict[str, ResourceT]) -> Json:
        """What the fan-out's listing records: every item with what identifies its content."""

    @abstractmethod
    async def read(self, args: dict[str, Json]) -> str | Document:
        """One resource, recorded as a read of it alone: its text, or a document."""

    def observation_state(self, method: str, result: Json) -> object:
        """The state recorded for an observation; a digest of the result by default."""
        return digest_of_json(result)

    def methods(self) -> tuple[MethodSpec, ...]:
        return (
            MethodSpec(
                "stats",
                "Widened item count (a range, not an exact number).",
                Volatility.STABLE,
            ),
            MethodSpec(
                "sample",
                "`k` representative items (default 5) with the head of each text item; "
                "a PDF shows its media type and size instead. The set is hash-stable, "
                "so it barely changes when items are added.",
                Volatility.STABLE,
                args=("k",),
            ),
            MethodSpec(
                "read",
                "One item by key: its full text, or for a PDF a reference with the "
                "document attached to your next message.",
                Volatility.PER_ITEM,
                args=("key",),
            ),
            MethodSpec(
                "keys",
                "Every item key (optionally under `prefix`). Volatile: planners only.",
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
        """Every resource as a ``for_each`` item, recorded as one observation of the listing."""
        resources = await self.resources()
        walk = await self._observe_recorded(LISTING, {}, resources)
        return Listing(
            {key: Item(key, resource) for key, resource in resources.items()},
            digest_json(walk),
        )

    async def _observe_recorded(
        self,
        method: str,
        args: dict[str, Json],
        resources: dict[str, ResourceT] | None = None,
    ) -> Json:
        result = await self.observe(method, args, resources)
        coco.use_data_dep(
            self.observation,
            key=self.observation_key(method, canonical_json(args)),
            state=self.observation_state(method, result),
        )
        return result

    async def observe(
        self,
        method: str,
        args: dict[str, Json],
        resources: dict[str, ResourceT] | None = None,
    ) -> Json:
        """Compute an observation without recording it (the validator's path).

        ``resources`` is the listing to observe; a caller that already listed passes it.
        """
        if resources is None:
            resources = await self.resources()
        if method == "stats":
            # Target (DESIGN.md §5): hysteresis against the previous bucket, kept in
            # use_state on the session component. Not implemented yet.
            return {"items": format_range(widen_count(len(resources)))}
        if method == "sample":
            return [
                await _sample_entry(key, resources[key])
                for key in bottom_k(resources, int_arg(args, "k", 5))
            ]
        if method == "keys":
            prefix = str(args.get("prefix", ""))
            return [k for k in resources if k.startswith(prefix)]
        if method == LISTING:
            return await self.entries(resources)
        raise ValueError(f"not an observation: {method!r}")  # call() screens the model


def item_key_arg(args: dict[str, Json]) -> str:
    key = args.get("key")
    if not isinstance(key, str):
        raise ToolError("read needs a string `key`")
    return key


async def content_of(key: str, resource: FileLike[Any], data: bytes) -> str | Document:
    """What ``read`` returns for a resource whose bytes ``data`` were just read: its
    text (decoded by the resource, from the bytes it cached), or the document itself.

    A document is returned as is; the ``call`` tool attaches it to the
    session and returns its reference.
    """
    media_type = resource_media_type(key, resource)
    if media_type is None:
        return await resource.read_text()
    return Document.of(key, media_type, data)


async def _sample_entry(key: str, resource: FileLike[Any]) -> dict[str, Json]:
    """A text item's head; a document's media type and size (its content is not text)."""
    media_type = resource_media_type(key, resource)
    if media_type is None:
        return {"key": key, "head": (await resource.read_text())[:_SAMPLE_HEAD_CHARS]}
    return {"key": key, "media_type": media_type, "bytes": await resource.size()}
