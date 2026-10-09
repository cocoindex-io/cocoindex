"""
Multi-Source Knowledge Graph (v1) — CocoIndex pipeline example, Neo4j.

Build one knowledge graph of a software project from three kinds of source —
a component catalog in Postgres, a folder of Markdown docs, and the issues of
a GitHub repository — with no global merge step. Every catalog row, doc, and
issue is its own processing component that owns one node plus the edges it
discovers:

  Component nodes — one per catalog row (key, name, kind, area)
  Doc       nodes — one per Markdown file
  Issue     nodes — one per GitHub issue

  DEPENDS_ON  Component -> Component   (declared by the catalog row)
  DOCUMENTS   Doc       -> Component   (the doc names the component)
  MENTIONS    Issue     -> Component   (the issue names the component)

Each node label has exactly one source that owns it; the other sources only
declare edges into it, keyed by the component key. Mentions are matched by a
deliberately simple resolver — whole-word alias lookup against the catalog —
which is the one place to plug in a smarter resolver later.

By default the catalog describes CocoIndex itself and the docs and issues are
CocoIndex's own, so the result is an engineering knowledge graph of this
repository. Point the three settings at your own catalog, docs folder, and
GitHub (or GitHub Enterprise) repo to build yours.

Index (one-shot catch-up):
    cocoindex update main
"""

from __future__ import annotations

import functools
import os
import pathlib
import re
from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass
from typing import Any

import aiohttp
import asyncpg

import cocoindex as coco
from cocoindex.connectors import localfs, neo4j, postgres
from cocoindex.resources.file import PatternFilePathMatcher


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DATABASE_URL = os.environ.get(
    "POSTGRES_URL", "postgres://cocoindex:cocoindex@localhost/cocoindex"
)
CATALOG_TABLE = "catalog_components"
DOCS_DIR_PATH = pathlib.Path(os.environ.get("DOCS_DIR", "../../docs/src/content/docs"))
GITHUB_REPO = os.environ.get("GITHUB_REPO", "cocoindex-io/cocoindex")
GITHUB_API_URL = os.environ.get("GITHUB_API_URL", "https://api.github.com")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")
GITHUB_MAX_ISSUES = int(os.environ.get("GITHUB_MAX_ISSUES", "1000"))
_GITHUB_PAGE_SIZE = 100


# ---------------------------------------------------------------------------
# Catalog rows and the mention resolver
# ---------------------------------------------------------------------------


@dataclass
class CatalogRow:
    """A row of the component catalog table (the Postgres source)."""

    key: str
    name: str
    kind: str
    area: str
    aliases: list[str]
    depends_on: list[str]


@dataclass(frozen=True)
class Catalog:
    """Every way a component may be written, mapped to its key.

    ``resolve`` is the entity-resolution step of this pipeline: whole-word,
    case-insensitive alias matching. Replace it with an embedding or LLM
    resolver — or a call to an external oracle — without touching anything
    else; only the memo of the components that call it is invalidated.
    """

    aliases: tuple[tuple[str, str], ...]  # (alias, component key), sorted

    @classmethod
    def from_rows(cls, rows: Iterable[CatalogRow]) -> Catalog:
        pairs = {
            (alias.lower(), row.key)
            for row in rows
            for alias in (row.key, row.name, *row.aliases)
        }
        return cls(aliases=tuple(sorted(pairs)))

    def __coco_memo_key__(self) -> object:
        return self.aliases

    @functools.cached_property
    def _lookup(self) -> dict[str, str]:
        return dict(self.aliases)

    @functools.cached_property
    def _pattern(self) -> re.Pattern[str]:
        # Longest alias first so "oci object storage" wins over "oci".
        alternatives = "|".join(
            re.escape(alias) for alias in sorted(self._lookup, key=len, reverse=True)
        )
        # Letters and digits delimit a word; "_" and "-" do not, so
        # "postgres_target" and "amazon-s3" still match.
        return re.compile(rf"(?<![a-z0-9])(?:{alternatives})(?![a-z0-9])")

    def resolve(self, text: str) -> set[str]:
        """Component keys mentioned in ``text``."""
        return {self._lookup[m.group(0)] for m in self._pattern.finditer(text.lower())}


# ---------------------------------------------------------------------------
# Context keys and lifespan
# ---------------------------------------------------------------------------

KG_DB = coco.ContextKey[neo4j.ConnectionFactory]("kg_db")
CATALOG_DB = coco.ContextKey[asyncpg.Pool]("catalog_db")
# detect_change=True: when the catalog's aliases change, every doc and issue
# that resolved mentions against it is re-processed.
CATALOG = coco.ContextKey[Catalog]("catalog", detect_change=True)
# Passing the docs folder as a ContextKey keeps file paths relative to it (the
# Doc primary key) and memoization stable if the folder moves.
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
    async with asyncpg.create_pool(DATABASE_URL) as pool:
        builder.provide(CATALOG_DB, pool)
        rows = await pool.fetch(
            f"SELECT key, name, kind, area, aliases, depends_on FROM {CATALOG_TABLE}"
        )
        builder.provide(CATALOG, Catalog.from_rows(CatalogRow(**dict(r)) for r in rows))
        builder.provide(DOCS_DIR, DOCS_DIR_PATH)
        yield


# ---------------------------------------------------------------------------
# Neo4j node schemas (dataclasses for declare_record)
# ---------------------------------------------------------------------------


@dataclass
class Component:
    key: str  # primary key
    name: str
    kind: str
    area: str


@dataclass
class Doc:
    path: str  # primary key — path relative to the docs folder
    title: str


@dataclass
class Issue:
    number: int  # primary key
    title: str
    state: str
    url: str
    author: str
    updated_at: str


# DEPENDS_ON, DOCUMENTS and MENTIONS carry no payload — declared without a
# schema, so the connector derives each edge's identity from its endpoints.


# ---------------------------------------------------------------------------
# GitHub issues source
# ---------------------------------------------------------------------------


@dataclass
class GitHubIssue:
    """The parts of a GitHub issue the pipeline looks at."""

    number: int
    title: str
    body: str
    state: str
    url: str
    author: str
    updated_at: str


async def fetch_issues() -> AsyncIterator[tuple[int, GitHubIssue]]:
    """Yield ``(issue number, issue)`` for the repo's issues, newest update first.

    Uses the search endpoint so pull requests are excluded server-side (the
    plain issues listing mixes both in). Unauthenticated search allows ten
    requests per minute, enough for the 1,000-issue search cap.
    """
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"

    fetched = 0
    async with aiohttp.ClientSession(headers=headers) as session:
        for page in range(1, 1 + GITHUB_MAX_ISSUES // _GITHUB_PAGE_SIZE + 1):
            params: dict[str, str | int] = {
                "q": f"repo:{GITHUB_REPO} is:issue",
                "sort": "updated",
                "order": "desc",
                "per_page": _GITHUB_PAGE_SIZE,
                "page": page,
            }
            async with session.get(
                f"{GITHUB_API_URL}/search/issues", params=params
            ) as resp:
                payload = await resp.json()
                if resp.status != 200:
                    raise RuntimeError(
                        f"GitHub search failed ({resp.status}): {payload.get('message')}"
                    )
            items = payload["items"]
            for item in items:
                if fetched >= GITHUB_MAX_ISSUES:
                    return
                fetched += 1
                yield (
                    item["number"],
                    GitHubIssue(
                        number=item["number"],
                        title=item["title"],
                        body=item.get("body") or "",
                        state=item["state"],
                        url=item["html_url"],
                        author=item["user"]["login"],
                        updated_at=item["updated_at"],
                    ),
                )
            if len(items) < _GITHUB_PAGE_SIZE:
                return


# ---------------------------------------------------------------------------
# Per-item processing — one component per catalog row / doc / issue
# ---------------------------------------------------------------------------


@coco.fn(memo=True)
async def process_component(
    row: CatalogRow,
    component_table: neo4j.TableTarget[Component],
    depends_on_rel: neo4j.RelationTarget[Any],
) -> None:
    component_table.declare_record(
        row=Component(key=row.key, name=row.name, kind=row.kind, area=row.area)
    )
    for dep in row.depends_on:
        depends_on_rel.declare_relation(from_id=row.key, to_id=dep)


_FRONT_MATTER_TITLE_RE = re.compile(r"^title:\s*(.+?)\s*$", re.MULTILINE)
_HEADING_RE = re.compile(r"^#\s+(.+?)\s*$", re.MULTILINE)


def _doc_title(text: str, path: str) -> str:
    if m := _FRONT_MATTER_TITLE_RE.search(text) or _HEADING_RE.search(text):
        # Starlight front-matter titles mark emphasis with asterisks.
        return m.group(1).strip("\"'").replace("*", "")
    return pathlib.PurePosixPath(path).stem


@coco.fn(memo=True)
async def process_doc(
    file: localfs.File,
    doc_table: neo4j.TableTarget[Doc],
    documents_rel: neo4j.RelationTarget[Any],
) -> None:
    text = await file.read_text()
    path = file.file_path.path.as_posix()
    doc_table.declare_record(row=Doc(path=path, title=_doc_title(text, path)))
    for key in coco.use_context(CATALOG).resolve(text):
        documents_rel.declare_relation(from_id=path, to_id=key)


@coco.fn(memo=True)
async def process_issue(
    issue: GitHubIssue,
    issue_table: neo4j.TableTarget[Issue],
    mentions_rel: neo4j.RelationTarget[Any],
) -> None:
    issue_table.declare_record(
        row=Issue(
            number=issue.number,
            title=issue.title,
            state=issue.state,
            url=issue.url,
            author=issue.author,
            updated_at=issue.updated_at,
        )
    )
    for key in coco.use_context(CATALOG).resolve(f"{issue.title}\n{issue.body}"):
        mentions_rel.declare_relation(from_id=issue.number, to_id=key)


# ---------------------------------------------------------------------------
# App main
# ---------------------------------------------------------------------------


@coco.fn
async def app_main() -> None:
    # --- Node tables: each label has exactly one owning source below ---
    component_table = await neo4j.mount_table_target(
        KG_DB,
        "Component",
        await neo4j.TableSchema.from_class(Component, primary_key="key"),
        primary_key="key",
    )
    doc_table = await neo4j.mount_table_target(
        KG_DB,
        "Doc",
        await neo4j.TableSchema.from_class(Doc, primary_key="path"),
        primary_key="path",
    )
    issue_table = await neo4j.mount_table_target(
        KG_DB,
        "Issue",
        await neo4j.TableSchema.from_class(Issue, primary_key="number"),
        primary_key="number",
    )

    # --- Relation targets ---
    depends_on_rel = await neo4j.mount_relation_target(
        KG_DB, "DEPENDS_ON", component_table, component_table
    )
    documents_rel = await neo4j.mount_relation_target(
        KG_DB, "DOCUMENTS", doc_table, component_table
    )
    mentions_rel = await neo4j.mount_relation_target(
        KG_DB, "MENTIONS", issue_table, component_table
    )

    # --- Three sources, three connector kinds, one component per item ---
    catalog = postgres.PgTableSource(
        coco.use_context(CATALOG_DB), table_name=CATALOG_TABLE, row_type=CatalogRow
    )
    await coco.mount_each(
        coco.component_subpath("catalog"),
        process_component,
        catalog.fetch_rows().items(lambda row: row.key),
        component_table,
        depends_on_rel,
    )

    docs = localfs.walk_dir(
        DOCS_DIR,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md", "**/*.mdx"]),
    )
    await coco.mount_each(
        coco.component_subpath("docs"),
        process_doc,
        docs.items(),
        doc_table,
        documents_rel,
    )

    await coco.mount_each(
        coco.component_subpath("github"),
        process_issue,
        fetch_issues(),
        issue_table,
        mentions_rel,
    )


app = coco.App(
    coco.AppConfig(name="MultiSourceKnowledgeGraph"),
    app_main,
)
