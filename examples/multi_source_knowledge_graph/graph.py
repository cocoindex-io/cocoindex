"""The shared phase: the targets every source writes to, and the steps every
record goes through on its way into them.

``KnowledgeGraph`` is mounted once and handed to every source. A source hands
it records; it resolves their references and declares the nodes and edges.
Both ``add_*`` methods are ``@coco.fn`` so an edit to this file re-syncs every
source through logic tracking.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import cocoindex as coco
from cocoindex.connectors import neo4j

from records import Document, Entity
from resolver import CATALOG


# ---------------------------------------------------------------------------
# Node schemas (dataclasses for declare_record)
# ---------------------------------------------------------------------------


@dataclass
class EntityNode:
    key: str  # primary key
    name: str
    kind: str
    area: str


@dataclass
class DocumentNode:
    id: str  # primary key — "<source>:<key>"
    source: str
    kind: str
    title: str
    url: str
    author: str
    status: str
    updated_at: str


# DEPENDS_ON and MENTIONS carry no payload — declared without a schema, so the
# connector derives each edge's identity from its endpoints.


class KnowledgeGraph:
    """Entity and Document nodes, DEPENDS_ON and MENTIONS edges, shared by all sources."""

    def __init__(
        self,
        entity_table: neo4j.TableTarget[EntityNode],
        document_table: neo4j.TableTarget[DocumentNode],
        depends_on_rel: neo4j.RelationTarget[Any],
        mentions_rel: neo4j.RelationTarget[Any],
    ) -> None:
        self._entity_table = entity_table
        self._document_table = document_table
        self._depends_on_rel = depends_on_rel
        self._mentions_rel = mentions_rel

    @classmethod
    async def mount(
        cls, db: coco.ContextKey[neo4j.ConnectionFactory]
    ) -> KnowledgeGraph:
        entity_table = await neo4j.mount_table_target(
            db,
            "Entity",
            await neo4j.TableSchema.from_class(EntityNode, primary_key="key"),
            primary_key="key",
        )
        document_table = await neo4j.mount_table_target(
            db,
            "Document",
            await neo4j.TableSchema.from_class(DocumentNode, primary_key="id"),
            primary_key="id",
        )
        depends_on_rel = await neo4j.mount_relation_target(
            db, "DEPENDS_ON", entity_table, entity_table
        )
        mentions_rel = await neo4j.mount_relation_target(
            db, "MENTIONS", document_table, entity_table
        )
        return cls(entity_table, document_table, depends_on_rel, mentions_rel)

    def __coco_memo_key__(self) -> object:
        return (
            self._entity_table,
            self._document_table,
            self._depends_on_rel,
            self._mentions_rel,
        )

    @coco.fn
    def add_entity(self, entity: Entity) -> None:
        """Declare the Entity node and a DEPENDS_ON edge per known reference."""
        self._entity_table.declare_record(
            row=EntityNode(
                key=entity.key, name=entity.name, kind=entity.kind, area=entity.area
            )
        )
        catalog = coco.use_context(CATALOG)
        for dep in entity.depends_on:
            if dep in catalog.keys:
                self._depends_on_rel.declare_relation(from_id=entity.key, to_id=dep)

    @coco.fn
    def add_document(self, doc: Document) -> None:
        """Declare the Document node and a MENTIONS edge per resolved entity."""
        doc_id = f"{doc.source}:{doc.key}"
        self._document_table.declare_record(
            row=DocumentNode(
                id=doc_id,
                source=doc.source,
                kind=doc.kind,
                title=doc.title,
                url=doc.url,
                author=doc.author,
                status=doc.status,
                updated_at=doc.updated_at,
            )
        )
        catalog = coco.use_context(CATALOG)
        mentioned = catalog.resolve(f"{doc.title}\n{doc.text}")
        mentioned.update(ref for ref in doc.refs if ref in catalog.keys)
        for key in mentioned:
            self._mentions_rel.declare_relation(from_id=doc_id, to_id=key)
