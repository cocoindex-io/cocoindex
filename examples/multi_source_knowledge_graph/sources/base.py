"""The two source families, with their default processing.

A source lists its items as refs and fetches one into a record. The family
base supplies the rest: ``process`` (one memoized child component per ref —
fetch on a miss, hand the record to the graph) and ``ingest`` (the source's
own processing component, which mounts those children). A source module
writes ``refs()`` and ``fetch()``, and overrides ``process`` only when its
records need more than fetch-then-add. Either way it ends in the graph's
``add_*`` methods, so the targets and the write logic stay shared.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterable
from dataclasses import dataclass
from typing import Generic, Protocol, TypeVar

import cocoindex as coco

from graph import KnowledgeGraph
from records import Document, Entity

RefT = TypeVar("RefT")


class Source(Protocol):
    """What the registry needs: a name, and the ability to ingest into a graph."""

    @property
    def name(self) -> str: ...

    async def ingest(self, graph: KnowledgeGraph) -> None: ...


@dataclass(frozen=True)
class DocumentSource(ABC, Generic[RefT]):
    name: str

    @abstractmethod
    def refs(self) -> AsyncIterable[tuple[coco.StableKey, RefT]]:
        """List items cheaply; each ref carries a change token."""

    @abstractmethod
    async def fetch(self, ref: RefT) -> Document:
        """Load one item. Runs only when ``process`` misses its memo."""

    @coco.fn(memo=True)
    async def process(self, ref: RefT, graph: KnowledgeGraph) -> None:
        graph.add_document(await self.fetch(ref))

    @coco.fn
    async def ingest(self, graph: KnowledgeGraph) -> None:
        await coco.mount_each(
            coco.component_subpath("record"), self.process, self.refs(), graph
        )


@dataclass(frozen=True)
class EntitySource(ABC, Generic[RefT]):
    name: str

    @abstractmethod
    def refs(self) -> AsyncIterable[tuple[coco.StableKey, RefT]]:
        """List items cheaply; each ref carries a change token."""

    @abstractmethod
    async def fetch(self, ref: RefT) -> Entity:
        """Load one item. Runs only when ``process`` misses its memo."""

    @coco.fn(memo=True)
    async def process(self, ref: RefT, graph: KnowledgeGraph) -> None:
        graph.add_entity(await self.fetch(ref))

    @coco.fn
    async def ingest(self, graph: KnowledgeGraph) -> None:
        await coco.mount_each(
            coco.component_subpath("record"), self.process, self.refs(), graph
        )
