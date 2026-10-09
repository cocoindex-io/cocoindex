"""
Multi-Source Knowledge Graph (v1) — CocoIndex pipeline example, Neo4j.

Many kinds of source, one shared back half, one graph. The pipeline has two
phases:

  Per source type (sources/*.py): list items cheaply as refs, fetch one into
      a record on a memo miss, with whatever extraction that source needs.
  Shared (resolver.py, graph.py): resolve the record's references to entity
      keys and declare its node and edges into the targets every source
      writes to — the ``KnowledgeGraph``.

The component tree has two layers. Each source is a processing component at
/source/<name> that lists its refs, and every ref is a memoized child
component below it. A ref is identity plus a change token — a file's size
and mtime, a row's xmin, an issue's updated_at — so an unchanged item is a
memo hit and its content is never fetched. Adding a source means one adapter
in sources/ and one line in the registry; removing one unmounts its subtree
and the nodes and edges it owned.

Every node label is shared across sources: a Document is a docs page, an
issue, a pull request, or a release, and any Document can MENTIONS any Entity.

By default the catalog describes CocoIndex itself and the docs, issues, pull
requests, and releases are CocoIndex's own, so the result is an engineering
knowledge graph of this repository. Point the settings at your own catalog,
docs folder, and GitHub (or GitHub Enterprise) repo to build yours.

Index (one-shot catch-up):
    cocoindex update main
"""

from __future__ import annotations

import os
import pathlib
from collections.abc import AsyncIterator
from typing import Any

import asyncpg

import cocoindex as coco
from cocoindex.connectors import neo4j

from graph import KnowledgeGraph
from records import DocumentSource, EntitySource, RefT
from resolver import CATALOG, Catalog, CatalogRow
from sources.github import GitHubIssues, GitHubPullRequests, GitHubReleases
from sources.markdown_docs import MarkdownDocs
from sources.postgres_catalog import PostgresCatalog


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DATABASE_URL = os.environ.get(
    "POSTGRES_URL", "postgres://cocoindex:cocoindex@localhost/cocoindex"
)
CATALOG_TABLE = "catalog_components"
DOCS_DIR_PATH = pathlib.Path(os.environ.get("DOCS_DIR", "../../docs/src/content/docs"))
GITHUB_REPO = os.environ.get("GITHUB_REPO", "cocoindex-io/cocoindex")
GITHUB_MAX_ISSUES = int(os.environ.get("GITHUB_MAX_ISSUES", "1000"))
GITHUB_MAX_PULL_REQUESTS = int(os.environ.get("GITHUB_MAX_PULL_REQUESTS", "300"))

KG_DB = coco.ContextKey[neo4j.ConnectionFactory]("kg_db")
CATALOG_DB = coco.ContextKey[asyncpg.Pool]("catalog_db")
# Passing the docs folder as a ContextKey keeps file paths relative to it (the
# Document key) and memoization stable if the folder moves.
DOCS_DIR = coco.ContextKey[pathlib.Path]("docs_dir")


@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    builder.provide(
        KG_DB,
        neo4j.ConnectionFactory(
            uri=os.environ.get("NEO4J_URI", "bolt://localhost:7687"),
            auth=(
                os.environ.get("NEO4J_USER", "neo4j"),
                os.environ.get("NEO4J_PASSWORD", "cocoindex"),
            ),
            database=os.environ.get("NEO4J_DATABASE", "neo4j"),
        ),
    )
    builder.provide(DOCS_DIR, DOCS_DIR_PATH)
    async with asyncpg.create_pool(DATABASE_URL) as pool:
        builder.provide(CATALOG_DB, pool)
        rows = await pool.fetch(
            f"SELECT key, name, kind, area, aliases, depends_on FROM {CATALOG_TABLE}"
        )
        builder.provide(CATALOG, Catalog.from_rows(CatalogRow(**dict(r)) for r in rows))
        yield


# ---------------------------------------------------------------------------
# Per-ref processing: fetch, then hand the record to the shared phase
# ---------------------------------------------------------------------------


@coco.fn(memo=True)
async def process_document(
    ref: RefT, source: DocumentSource[RefT], graph: KnowledgeGraph
) -> None:
    # Memoized on the ref's change token: on a hit this body does not run and
    # nothing is fetched. A source type that needs more than fetch-then-add
    # gets its own processor and still ends in graph.add_document.
    graph.add_document(await source.fetch(ref))


@coco.fn(memo=True)
async def process_entity(
    ref: RefT, source: EntitySource[RefT], graph: KnowledgeGraph
) -> None:
    graph.add_entity(await source.fetch(ref))


# ---------------------------------------------------------------------------
# The source layer — one processing component per source, one child per ref
# ---------------------------------------------------------------------------


@coco.fn
async def ingest_documents(source: DocumentSource[Any], graph: KnowledgeGraph) -> None:
    await coco.mount_each(process_document, source.refs(), source, graph)


@coco.fn
async def ingest_entities(source: EntitySource[Any], graph: KnowledgeGraph) -> None:
    await coco.mount_each(process_entity, source.refs(), source, graph)


# ---------------------------------------------------------------------------
# App main
# ---------------------------------------------------------------------------


@coco.fn
async def app_main() -> None:
    # --- The registry. Adding a source to the graph is adding one line here ---
    document_sources: list[DocumentSource[Any]] = [
        MarkdownDocs("docs", DOCS_DIR),
        GitHubIssues("github_issues", GITHUB_REPO, GITHUB_MAX_ISSUES),
        GitHubPullRequests(
            "github_pull_requests", GITHUB_REPO, GITHUB_MAX_PULL_REQUESTS
        ),
        GitHubReleases("github_releases", GITHUB_REPO),
    ]
    entity_sources: list[EntitySource[Any]] = [
        PostgresCatalog("catalog", coco.use_context(CATALOG_DB), CATALOG_TABLE),
    ]

    # --- The shared phase: targets and steps every source writes through ---
    graph = await KnowledgeGraph.mount(KG_DB)

    # --- One processing component per source, under /source/<name> ---
    with coco.component_subpath("source"):
        for entity_source in entity_sources:
            await coco.mount(
                coco.component_subpath(entity_source.name),
                ingest_entities,
                entity_source,
                graph,
            )
        for document_source in document_sources:
            await coco.mount(
                coco.component_subpath(document_source.name),
                ingest_documents,
                document_source,
                graph,
            )


app = coco.App(
    coco.AppConfig(name="MultiSourceKnowledgeGraph"),
    app_main,
)
