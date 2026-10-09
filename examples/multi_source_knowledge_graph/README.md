<h1 align="center">Many sources, <em>one</em> pipeline, one knowledge graph.</h1>

<p align="center">
  <b>A Postgres catalog, a folder of Markdown docs, and the issues, pull requests, and releases of a GitHub repo land in one Neo4j graph.</b><br/>
  Every source is a small adapter; everything after it is shared — in plain async Python.
</p>

<p align="center">
  <strong>Star us&nbsp;❤️&nbsp;→</strong>&nbsp;<a href="https://github.com/cocoindex-io/cocoindex" title="Star CocoIndex on GitHub"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg" alt="Star CocoIndex on GitHub" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://cocoindex.io/docs/" title="CocoIndex documentation"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg" alt="CocoIndex documentation" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://discord.com/invite/zpA9S2DR7s" title="Join the CocoIndex Discord"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg" alt="Join the CocoIndex Discord" height="36" align="absmiddle"/></picture></a>
</p>

<div align="center">

[![stars](https://img.shields.io/github/stars/cocoindex-io/cocoindex?style=flat-square&label=stars&color=FB6A76)](https://github.com/cocoindex-io/cocoindex)
[![pypi](https://img.shields.io/pypi/v/cocoindex?style=flat-square&label=pypi&color=E59A63)](https://pypi.org/project/cocoindex/)
[![discord](https://img.shields.io/discord/1314801574169673738?style=flat-square&logo=discord&logoColor=white&label=discord&color=5865F2)](https://discord.com/invite/zpA9S2DR7s)
[![license](https://img.shields.io/badge/license-Apache--2.0-5B5BD6?style=flat-square)](https://opensource.org/licenses/Apache-2.0)

</div>

<br/>

Knowledge about a software system is spread over dozens of systems: a catalog says what exists, docs explain it, the issue tracker records what is broken, pull requests and release notes record what changed. The processing is mostly the same for all of them — find which entities a piece of text is about, link it to them — and only the shape of each source differs. This example is built in that shape. A **source** is a name plus an async generator of records; a **document** from any source goes through the same `process_document`; and the result is **one** Neo4j graph where a `Document` is a docs page, an issue, a pull request, or a release, and any of them can `MENTIONS` any `Entity`.

By default the catalog describes CocoIndex itself and the docs, issues, pull requests, and releases are CocoIndex's own, so you get an engineering knowledge graph of this repository. Point the settings at your own catalog, docs, and GitHub or GitHub Enterprise repo instead. No LLM, no API keys; the only services are Postgres and Neo4j.

## How it works

**Two record types are the contract between sources and the pipeline.** A `Document` carries source, key, kind, title, text, url, author, status, and `refs` — entity keys the source already knows structurally. An `Entity` carries key, name, kind, area, and dependencies. Everything a source does is produce these.

**One adapter per source, in two halves.** Each adapter is a small frozen dataclass with `refs()`, which lists the source's items cheaply, and `fetch(ref)`, which loads one item into a record. A ref is identity plus a change token — the `File` handle with its size and mtime, a catalog row's key and `xmin`, an issue number and its `updated_at` — and the shared processor is memoized on it, so an unchanged item is a memo hit and `fetch` never runs for it. `fetch` is a `@coco.fn`, so editing one adapter re-processes only that source's records. The GitHub adapters share one search helper and differ only in the qualifier and the field mapping; the pull-request adapter adds one line of source-specific extraction, reading the entity out of a conventional-commit title such as `fix(neo4j): …`. The registry is a list:

```python
document_sources: list[DocumentSource[Any]] = [
    MarkdownDocs("docs", DOCS_DIR),
    GitHubIssues("github_issues", GITHUB_REPO),
    GitHubPullRequests("github_pull_requests", GITHUB_REPO),
    GitHubReleases("github_releases", GITHUB_REPO),
]
entity_sources: list[EntitySource[Any]] = [
    PostgresCatalog("catalog", coco.use_context(CATALOG_DB), CATALOG_TABLE),
]
```

**Fetching is lazy, and batched where the source allows it.** The catalog adapter lists two columns per row, `key` and `xmin` (the row's last-writer transaction id, which changes on every update, so there is no `updated_at` column to maintain), and its `fetch` is declared with `batching=True`: the processor still calls it with one ref, and concurrent misses are coalesced into one `WHERE key = ANY($1)` query. A batched body runs outside any component context, so the adapter holds its connection pool rather than looking it up. The docs adapter's ref is the `File` itself, read only inside `fetch`. GitHub's REST listings already carry the item body, so the ref keeps the payload for `fetch` but fingerprints only the number and `updated_at`; a true metadata-only listing with batched body fetches is possible through GraphQL, which needs a token.

**Two shared processors.** `process_document` resolves mentions and declares the `Document` node plus its `MENTIONS` edges; `process_entity` declares the `Entity` node plus its `DEPENDS_ON` edges. Both are memoized per ref, so a changed issue or an edited page re-runs exactly one of them, and nothing else is even fetched.

```python
@coco.fn(memo=True)
async def process_document(ref: RefT, source: DocumentSource[RefT], document_table: neo4j.TableTarget[DocumentNode], mentions_rel: neo4j.RelationTarget[Any]) -> None:
    doc = await source.fetch(ref)   # only runs on a memo miss
    doc_id = f"{doc.source}:{doc.key}"
    document_table.declare_record(row=DocumentNode(id=doc_id, source=doc.source, kind=doc.kind, title=doc.title, ...))
    catalog = coco.use_context(CATALOG)
    mentioned = catalog.resolve(f"{doc.title}\n{doc.text}")
    mentioned.update(ref for ref in doc.refs if ref in catalog.keys)
    for key in mentioned:
        mentions_rel.declare_relation(from_id=doc_id, to_id=key)
```

**Two layers of processing components.** Each source is a [processing component](https://cocoindex.io/docs/programming_guide/processing_component/) at `/source/<name>` that lists its refs, and every ref is a memoized child below it:

```
/source/"catalog"/process_entity/"neo4j"
/source/"docs"/process_document/"connectors/neo4j.mdx"
/source/"github_issues"/process_document/2460
/source/"github_pull_requests"/process_document/2491
/source/"github_releases"/process_document/"v1.0.25"
```

```python
@coco.fn
async def ingest_documents(source: DocumentSource[Any], document_table, mentions_rel) -> None:
    await coco.mount_each(process_document, source.refs(), source, document_table, mentions_rel)

@coco.fn
async def app_main() -> None:
    ...  # the registry above, then the Entity and Document tables and the two relation targets, mounted once
    with coco.component_subpath("source"):
        for entity_source in entity_sources:
            await coco.mount(coco.component_subpath(entity_source.name), ingest_entities, entity_source, entity_table, depends_on_rel)
        for document_source in document_sources:
            await coco.mount(coco.component_subpath(document_source.name), ingest_documents, document_source, document_table, mentions_rel)
```

The source layer is what makes many sources manageable: sources enumerate in parallel, a source that fails (a rate limit, an unreachable API) fails in its own component while the others finish, and removing a source from the registry unmounts its subtree, taking every node and edge it owned with it.

**One rule keeps the sources independent.** Every node label has exactly one *kind* of source that owns it — entity sources own `Entity` nodes, document sources own `Document` nodes — and documents only ever declare edges into entities, keyed by the entity key. A document never declares or looks up an `Entity` node; the Neo4j connector merges the edge onto its endpoint, so the write order between sources never matters.

**Resolution is one seam.** `Catalog.resolve` does whole-word, case-insensitive alias matching against the catalog, which is provided as a [context value](https://cocoindex.io/docs/programming_guide/context/) with change detection on, so adding an alias re-processes the documents that depend on it. Replace it with an embedding search, an LLM, or a call to your own entity-resolution service and nothing else changes.

## Why it's worth a star ⭐

- **Adding a source is adding an adapter.** A `refs()` that lists and a `fetch()` that loads, one line in the registry. The pipeline, the targets, and the graph schema do not change.
- **A run with nothing changed fetches nothing.** One stat per file, one two-column query, the GitHub listings. No file reads, no row fetches, no conversions: the change token in each ref decides.
- **Shared labels, shared edges.** `MATCH (d:Document)-[:MENTIONS]->(e:Entity {key: 'postgres'})` returns docs pages, issues, pull requests, and releases in one result. Filter on `d.kind` when you want one of them.
- **No fan-in bottleneck.** Nothing collects "all entities" into one component. A million documents are a million small incremental units.
- **Sources are isolated.** Each enumerates in its own processing component; a failure or a removal is scoped to that source's subtree.
- **Incremental across all of them.** Edit a page, close an issue, cut a release, add a catalog row: exactly those records re-run, and vanished records take their nodes and edges with them.
- **Three connector kinds in one app.** A database table, a directory walk, and an HTTP API, with no connector needed for the API: any async generator of keyed records is a source.

## Run it

**1. Start Postgres and Neo4j:**

```sh
docker compose -f ../../dev/postgres.yaml up -d
docker run -d -p 7474:7474 -p 7687:7687 -e NEO4J_AUTH=neo4j/cocoindex --name cocoindex-neo4j neo4j:5.26-community
```

**2. Configure and install:**

```sh
cp .env.example .env     # defaults match the two containers above
pip install -e .
```

**3. Seed the catalog** — one row per CocoIndex component, with aliases and dependencies:

```sh
psql "$POSTGRES_URL" -f ./prepare_source_data.sql
```

**4. Build the graph** — reads the catalog, walks the repo's docs folder, fetches the repo's issues, pull requests, and releases, and syncs Neo4j:

```sh
cocoindex update main
```

One run stays within GitHub's unauthenticated search limit of ten requests a minute; set `GITHUB_TOKEN` in `.env` to run more often. Run it again and nothing is written: every record is memoized on its content.

**5. Explore the graph** — open [Neo4j Browser](http://localhost:7474) (`neo4j` / `cocoindex`). Every query below spans sources; that is the point of the shared labels:

```cypher
// One entity, seen from every kind of source
MATCH (d:Document)-[:MENTIONS]->(:Entity {key: 'postgres'})
RETURN d.kind AS kind, count(*) AS documents, collect(d.title)[..3] AS sample
ORDER BY documents DESC
```

```cypher
// For each entity: the last release that mentioned it and how many issues are still open
MATCH (e:Entity)<-[:MENTIONS]-(r:Document {kind: 'release'})
WITH e, max(r.updated_at) AS last_release
OPTIONAL MATCH (e)<-[:MENTIONS]-(i:Document {kind: 'issue', status: 'open'})
RETURN e.key AS entity, last_release, count(i) AS open_issues
ORDER BY open_issues DESC
```

```cypher
// Open issues and pull requests about anything the Rust SDK depends on, transitively
MATCH (:Entity {key: 'rust_sdk'})-[:DEPENDS_ON*1..]->(e:Entity)<-[:MENTIONS]-(d:Document {status: 'open'})
RETURN e.key AS entity, d.kind AS kind, d.key AS number, d.title AS title
ORDER BY entity, kind, number
```

```cypher
// Which area of the project do open issues cluster in?
MATCH (e:Entity)<-[:MENTIONS]-(i:Document {kind: 'issue', status: 'open'})
RETURN e.area AS area, count(DISTINCT i) AS open_issues, collect(DISTINCT e.key) AS entities
ORDER BY open_issues DESC
```

```cypher
// Documents that cut across the most entities, whatever their source
MATCH (d:Document)-[:MENTIONS]->(e:Entity)
WITH d, collect(e.key) AS entities
WHERE size(entities) >= 3
RETURN d.source AS source, d.title AS title, entities
ORDER BY size(entities) DESC LIMIT 10
```

## Make it yours

- **Add a source.** Write a dataclass with a `refs()` that yields `(key, ref)` pairs where the ref carries a change token, and a `@coco.fn` `fetch(ref)` that returns a `Document`, then append it to `DOCUMENT_SOURCES`. A Jira project or a Confluence space is the same shape, and both list items with version numbers and can fetch several per request, so their `fetch` can batch like the catalog's. A source with its own extraction step (an LLM prompt per source type, a parser for its format) does that work inside `fetch` and still returns a `Document`.
- **Add an entity source.** A service catalog, a service mesh export, or an HR directory yields refs and fetches `Entity` records, and goes in `ENTITY_SOURCES`. Each entity should come from exactly one source.
- **Point it at your data.** `DOCS_DIR` for any Markdown or MDX tree; `GITHUB_REPO`, `GITHUB_TOKEN`, and for GitHub Enterprise `GITHUB_API_URL=https://<host>/api/v3`; `prepare_source_data.sql` for your catalog, keeping a key, an `aliases` array, and a `depends_on` array.
- **Swap the resolver.** `Catalog.resolve` is where whole-word matching becomes an embedding search, an LLM call, or a lookup against your own entity-resolution service.

**What stays simple on purpose.** Each entity has one owning source, so people and teams are properties rather than nodes: a `Person` node mentioned by both an issue and a release would need a shared owner. Shared entity nodes are the next step for this example, built on shared processing components. Until then, one consequence of single ownership is worth knowing: removing a row from the catalog removes its `Entity` node and every edge into it from the other sources.

<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=7f27e85b-be3a-411a-b612-0b9d53711814&page=examples/multi_source_knowledge_graph" alt="" width="1" height="1" />
