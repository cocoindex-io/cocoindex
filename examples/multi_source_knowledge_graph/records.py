"""The contract between sources and the shared pipeline.

A source lists its items as refs and fetches one into a record; everything
downstream of a record is shared (see graph.py). Two record types, two source
protocols: documents mention entities, entities are what they mention.
"""

from __future__ import annotations

from collections.abc import AsyncIterable
from dataclasses import dataclass
from typing import Protocol, TypeVar

import cocoindex as coco


@dataclass(frozen=True)
class Document:
    """One item of any document source."""

    source: str
    key: str  # stable within the source
    kind: str  # "doc" | "issue" | "pull_request" | "release"
    title: str
    text: str
    url: str
    author: str
    status: str
    updated_at: str
    # Entity keys the source already knows structurally, on top of what the
    # resolver finds in the text (e.g. the scope of a conventional-commit title).
    refs: tuple[str, ...] = ()


@dataclass(frozen=True)
class Entity:
    """One item of any entity source."""

    key: str
    name: str
    kind: str
    area: str
    depends_on: tuple[str, ...] = ()


RefT = TypeVar("RefT")


class DocumentSource(Protocol[RefT]):
    """``refs`` lists items with a change token; ``fetch`` loads one on a memo miss."""

    @property
    def name(self) -> str: ...

    def refs(self) -> AsyncIterable[tuple[coco.StableKey, RefT]]: ...

    async def fetch(self, ref: RefT) -> Document: ...


class EntitySource(Protocol[RefT]):
    @property
    def name(self) -> str: ...

    def refs(self) -> AsyncIterable[tuple[coco.StableKey, RefT]]: ...

    async def fetch(self, ref: RefT) -> Entity: ...
