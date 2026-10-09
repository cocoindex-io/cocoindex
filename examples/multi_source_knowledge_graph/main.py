"""
Multi-Source Knowledge Graph (v1) — CocoIndex pipeline example, Neo4j.

Many kinds of source, one pipeline, one graph. A source is a name plus two
methods — ``refs()`` lists its items cheaply, ``fetch(ref)`` loads one — and
everything after that point is shared:

  Document sources — a docs folder, GitHub issues, pull requests, releases
      -> Document record -> process_document(): resolve mentions, declare the
         Document node and its MENTIONS edges
  Entity sources   — a component catalog in Postgres
      -> Entity record   -> process_entity(): declare the Entity node and its
         DEPENDS_ON edges

The component tree has two layers. Each source is a processing component at
/source/<name> that lists its refs, and every ref is a memoized child
component below it. A ref is identity plus a change token — a file's size
and mtime, a row's xmin, an issue's updated_at — so an unchanged item is a
memo hit and its content is never fetched; ``fetch`` runs only on a miss, and
a source that can load several items at once declares it with batching so
concurrent misses become one request. Adding a source means adding one
adapter to the registry; removing one unmounts its subtree and the nodes and
edges it owned.

Every node label is shared across sources: a Document is a docs page, an
issue, a pull request, or a release, and any Document can MENTIONS any Entity.
Mentions are matched by a deliberately simple resolver — whole-word alias
lookup against the catalog — which is the one place to plug in a smarter one.

By default the catalog describes CocoIndex itself and the docs, issues, pull
requests, and releases are CocoIndex's own, so the result is an engineering
knowledge graph of this repository. Point the settings at your own catalog,
docs folder, and GitHub (or GitHub Enterprise) repo to build yours.

Index (one-shot catch-up):
    cocoindex update main
"""

from __future__ import annotations

import functools
import os
import pathlib
import re
from collections.abc import AsyncIterable, AsyncIterator, Iterable
from dataclasses import dataclass, field
from typing import Any, Protocol, TypeVar

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
GITHUB_MAX_PULL_REQUESTS = int(os.environ.get("GITHUB_MAX_PULL_REQUESTS", "300"))
_GITHUB_PAGE_SIZE = 100


# ---------------------------------------------------------------------------
# Context keys
# ---------------------------------------------------------------------------

KG_DB = coco.ContextKey[neo4j.ConnectionFactory]("kg_db")
CATALOG_DB = coco.ContextKey[asyncpg.Pool]("catalog_db")
# detect_change=True: when the catalog's aliases change, every document that
# resolved mentions against it is re-processed.
CATALOG = coco.ContextKey["Catalog"]("catalog", detect_change=True)
# Passing the docs folder as a ContextKey keeps file paths relative to it (the
# Document key) and memoization stable if the folder moves.
DOCS_DIR = coco.ContextKey[pathlib.Path]("docs_dir")


# ---------------------------------------------------------------------------
# Shared records and the source contract
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Document:
    """One item of any document source, in the shape the shared pipeline reads."""

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


# ---------------------------------------------------------------------------
# The mention resolver
# ---------------------------------------------------------------------------


@dataclass
class CatalogRow:
    """A full row of the component catalog table."""

    key: str
    name: str
    kind: str
    area: str
    aliases: list[str]
    depends_on: list[str]


@dataclass(frozen=True)
class Catalog:
    """Every way an entity may be written, mapped to its key.

    ``resolve`` is the entity-resolution step of this pipeline: whole-word,
    case-insensitive alias matching. Replace it with an embedding or LLM
    resolver — or a call to an external oracle — without touching anything
    else; only the memo of the documents that call it is invalidated.
    """

    aliases: tuple[tuple[str, str], ...]  # (alias, entity key), sorted

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
    def keys(self) -> frozenset[str]:
        return frozenset(self._lookup.values())

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
        """Entity keys mentioned in ``text``."""
        return {self._lookup[m.group(0)] for m in self._pattern.finditer(text.lower())}


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------


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
# Sources — one small adapter each; this is the only per-source code
# ---------------------------------------------------------------------------

_FRONT_MATTER_TITLE_RE = re.compile(r"^title:\s*(.+?)\s*$", re.MULTILINE)
_HEADING_RE = re.compile(r"^#\s+(.+?)\s*$", re.MULTILINE)


def _doc_title(text: str, path: str) -> str:
    if m := _FRONT_MATTER_TITLE_RE.search(text) or _HEADING_RE.search(text):
        # Starlight front-matter titles mark emphasis with asterisks.
        return m.group(1).strip("\"'").replace("*", "")
    return pathlib.PurePosixPath(path).stem


@dataclass(frozen=True)
class MarkdownDocs:
    """Markdown / MDX files under a folder."""

    name: str
    root: coco.ContextKey[pathlib.Path]

    def refs(self) -> AsyncIterable[tuple[coco.StableKey, localfs.File]]:
        # The ref is the File itself: it carries size and mtime and is
        # fingerprinted on them, so an unchanged file is never read.
        files = localfs.walk_dir(
            self.root,
            recursive=True,
            path_matcher=PatternFilePathMatcher(
                included_patterns=["**/*.md", "**/*.mdx"]
            ),
        )
        return files.items()

    @coco.fn
    async def fetch(self, file: localfs.File) -> Document:
        text = await file.read_text()
        path = file.file_path.path.as_posix()
        return Document(
            source=self.name,
            key=path,
            kind="doc",
            title=_doc_title(text, path),
            text=text,
            url="",
            author="",
            status="",
            updated_at="",
        )


@dataclass(frozen=True)
class GitHubRef:
    """A GitHub listing entry.

    REST listings carry the whole item, so the payload rides along for
    ``fetch``; only the key and version take part in memoization, so an
    unchanged item is a memo hit (and the search relevance score, which varies
    between runs, is ignored).
    """

    key: str
    version: str
    payload: dict[str, Any] = field(compare=False, hash=False, repr=False)

    def __coco_memo_key__(self) -> object:
        return (self.key, self.version)


def _github_session() -> aiohttp.ClientSession:
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    return aiohttp.ClientSession(headers=headers)


async def _github_json(resp: aiohttp.ClientResponse) -> Any:
    payload = await resp.json()
    if resp.status != 200:
        raise RuntimeError(
            f"GitHub request failed ({resp.status}): {payload.get('message')}"
        )
    return payload


async def _github_search(
    repo: str, qualifier: str, max_items: int
) -> AsyncIterator[tuple[coco.StableKey, GitHubRef]]:
    """Issues or pull requests of ``repo`` via the search endpoint, newest update
    first. The plain issues listing mixes both in; search filters server-side."""
    fetched = 0
    async with _github_session() as session:
        for page in range(1, max_items // _GITHUB_PAGE_SIZE + 2):
            params: dict[str, str | int] = {
                "q": f"repo:{repo} {qualifier}",
                "sort": "updated",
                "order": "desc",
                "per_page": _GITHUB_PAGE_SIZE,
                "page": page,
            }
            async with session.get(
                f"{GITHUB_API_URL}/search/issues", params=params
            ) as resp:
                items = (await _github_json(resp))["items"]
            for item in items:
                if fetched >= max_items:
                    return
                fetched += 1
                yield (
                    item["number"],
                    GitHubRef(str(item["number"]), item["updated_at"], item),
                )
            if len(items) < _GITHUB_PAGE_SIZE:
                return


def _github_item_document(
    source: str, kind: str, ref: GitHubRef, refs: tuple[str, ...] = ()
) -> Document:
    item = ref.payload
    return Document(
        source=source,
        key=ref.key,
        kind=kind,
        title=item["title"],
        text=item.get("body") or "",
        url=item["html_url"],
        author=item["user"]["login"],
        status=item["state"],
        updated_at=item["updated_at"],
        refs=refs,
    )


@dataclass(frozen=True)
class GitHubIssues:
    name: str
    repo: str
    max_items: int = GITHUB_MAX_ISSUES

    def refs(self) -> AsyncIterable[tuple[coco.StableKey, GitHubRef]]:
        return _github_search(self.repo, "is:issue", self.max_items)

    @coco.fn
    async def fetch(self, ref: GitHubRef) -> Document:
        return _github_item_document(self.name, "issue", ref)


_CONVENTIONAL_SCOPE_RE = re.compile(r"^\w+\(([^)]+)\)!?:")


@dataclass(frozen=True)
class GitHubPullRequests:
    name: str
    repo: str
    max_items: int = GITHUB_MAX_PULL_REQUESTS

    def refs(self) -> AsyncIterable[tuple[coco.StableKey, GitHubRef]]:
        return _github_search(self.repo, "is:pr", self.max_items)

    @coco.fn
    async def fetch(self, ref: GitHubRef) -> Document:
        # Source-specific extraction: a conventional-commit title such as
        # "fix(neo4j): ..." names the entity it touches outright.
        scope = _CONVENTIONAL_SCOPE_RE.match(ref.payload["title"])
        refs = (scope.group(1).strip().lower(),) if scope else ()
        return _github_item_document(self.name, "pull_request", ref, refs)


@dataclass(frozen=True)
class GitHubReleases:
    name: str
    repo: str

    async def refs(self) -> AsyncIterator[tuple[coco.StableKey, GitHubRef]]:
        async with _github_session() as session:
            async with session.get(
                f"{GITHUB_API_URL}/repos/{self.repo}/releases",
                params={"per_page": _GITHUB_PAGE_SIZE},
            ) as resp:
                releases = await _github_json(resp)
        for release in releases:
            if not release["draft"]:
                tag = release["tag_name"]
                yield tag, GitHubRef(tag, release["published_at"] or "", release)

    @coco.fn
    async def fetch(self, ref: GitHubRef) -> Document:
        release = ref.payload
        return Document(
            source=self.name,
            key=ref.key,
            kind="release",
            title=release["name"] or ref.key,
            text=release.get("body") or "",
            url=release["html_url"],
            author=release["author"]["login"],
            status="prerelease" if release["prerelease"] else "published",
            updated_at=ref.version,
        )


@dataclass
class CatalogRef:
    """A catalog listing entry: the key and the row's last-writer transaction id."""

    key: str
    xmin: int


@dataclass(frozen=True)
class PostgresCatalog:
    """Rows of a catalog table, each an entity with its dependencies."""

    name: str
    pool: asyncpg.Pool
    table: str

    def __coco_memo_key__(self) -> object:
        # The pool is a connection, not an input: memoize on what names the source.
        return (self.name, self.table)

    def refs(self) -> AsyncIterable[tuple[coco.StableKey, CatalogRef]]:
        # Two columns per row. xmin changes whenever the row is updated, so it
        # is a free change token: no updated_at column to maintain.
        rows = postgres.PgTableSource(
            self.pool,
            table_name=self.table,
            columns=["key", "xmin"],
            row_type=CatalogRef,
        )
        return rows.fetch_rows().items(lambda ref: ref.key)

    @coco.fn.as_async(batching=True, max_batch_size=100)
    async def fetch(self, refs: list[CatalogRef]) -> list[Entity]:
        # Called with one ref at a time; concurrent misses across refs are
        # batched into one query. Results line up with the input order. The
        # batched body runs outside any component context, which is why the
        # pool lives on the adapter rather than being looked up here.
        rows = await self.pool.fetch(
            f"SELECT key, name, kind, area, depends_on FROM {self.table}"
            " WHERE key = ANY($1)",
            [ref.key for ref in refs],
        )
        by_key = {row["key"]: row for row in rows}
        return [
            Entity(
                key=ref.key,
                name=by_key[ref.key]["name"],
                kind=by_key[ref.key]["kind"],
                area=by_key[ref.key]["area"],
                depends_on=tuple(by_key[ref.key]["depends_on"]),
            )
            for ref in refs
        ]


# ---------------------------------------------------------------------------
# Neo4j node schemas (dataclasses for declare_record)
# ---------------------------------------------------------------------------


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


@dataclass
class EntityNode:
    key: str  # primary key
    name: str
    kind: str
    area: str


# DEPENDS_ON and MENTIONS carry no payload — declared without a schema, so the
# connector derives each edge's identity from its endpoints.


# ---------------------------------------------------------------------------
# Shared processing — the same two functions for every source
# ---------------------------------------------------------------------------


@coco.fn(memo=True)
async def process_document(
    ref: RefT,
    source: DocumentSource[RefT],
    document_table: neo4j.TableTarget[DocumentNode],
    mentions_rel: neo4j.RelationTarget[Any],
) -> None:
    # Memoized on the ref's change token: on a hit this body does not run and
    # nothing is fetched.
    doc = await source.fetch(ref)
    doc_id = f"{doc.source}:{doc.key}"
    document_table.declare_record(
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
        mentions_rel.declare_relation(from_id=doc_id, to_id=key)


@coco.fn(memo=True)
async def process_entity(
    ref: RefT,
    source: EntitySource[RefT],
    entity_table: neo4j.TableTarget[EntityNode],
    depends_on_rel: neo4j.RelationTarget[Any],
) -> None:
    entity = await source.fetch(ref)
    entity_table.declare_record(
        row=EntityNode(
            key=entity.key, name=entity.name, kind=entity.kind, area=entity.area
        )
    )
    for dep in entity.depends_on:
        depends_on_rel.declare_relation(from_id=entity.key, to_id=dep)


# ---------------------------------------------------------------------------
# The source layer — one processing component per source, one child per ref
# ---------------------------------------------------------------------------


@coco.fn
async def ingest_documents(
    source: DocumentSource[Any],
    document_table: neo4j.TableTarget[DocumentNode],
    mentions_rel: neo4j.RelationTarget[Any],
) -> None:
    await coco.mount_each(
        process_document, source.refs(), source, document_table, mentions_rel
    )


@coco.fn
async def ingest_entities(
    source: EntitySource[Any],
    entity_table: neo4j.TableTarget[EntityNode],
    depends_on_rel: neo4j.RelationTarget[Any],
) -> None:
    await coco.mount_each(
        process_entity, source.refs(), source, entity_table, depends_on_rel
    )


# ---------------------------------------------------------------------------
# App main
# ---------------------------------------------------------------------------


@coco.fn
async def app_main() -> None:
    # --- The registry. Adding a source to the graph is adding one line here ---
    document_sources: list[DocumentSource[Any]] = [
        MarkdownDocs("docs", DOCS_DIR),
        GitHubIssues("github_issues", GITHUB_REPO),
        GitHubPullRequests("github_pull_requests", GITHUB_REPO),
        GitHubReleases("github_releases", GITHUB_REPO),
    ]
    entity_sources: list[EntitySource[Any]] = [
        PostgresCatalog("catalog", coco.use_context(CATALOG_DB), CATALOG_TABLE),
    ]

    # --- Node tables and relation targets, shared by every source ---
    entity_table = await neo4j.mount_table_target(
        KG_DB,
        "Entity",
        await neo4j.TableSchema.from_class(EntityNode, primary_key="key"),
        primary_key="key",
    )
    document_table = await neo4j.mount_table_target(
        KG_DB,
        "Document",
        await neo4j.TableSchema.from_class(DocumentNode, primary_key="id"),
        primary_key="id",
    )
    depends_on_rel = await neo4j.mount_relation_target(
        KG_DB, "DEPENDS_ON", entity_table, entity_table
    )
    mentions_rel = await neo4j.mount_relation_target(
        KG_DB, "MENTIONS", document_table, entity_table
    )

    # --- One processing component per source, under /source/<name> ---
    with coco.component_subpath("source"):
        for entity_source in entity_sources:
            await coco.mount(
                coco.component_subpath(entity_source.name),
                ingest_entities,
                entity_source,
                entity_table,
                depends_on_rel,
            )
        for document_source in document_sources:
            await coco.mount(
                coco.component_subpath(document_source.name),
                ingest_documents,
                document_source,
                document_table,
                mentions_rel,
            )


app = coco.App(
    coco.AppConfig(name="MultiSourceKnowledgeGraph"),
    app_main,
)
