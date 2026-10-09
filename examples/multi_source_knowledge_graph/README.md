<h1 align="center">One knowledge graph from <em>many</em> sources — no merge step.</h1>

<p align="center">
  <b>A component catalog in Postgres, a folder of Markdown docs, and the issues of a GitHub repo land in one Neo4j graph.</b><br/>
  Each row, file, and issue is its own incremental unit — in plain async Python.
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

Knowledge about a software system is scattered: the catalog says what exists and what depends on what, the docs explain it, the issue tracker records what is broken. This example reads all three — a Postgres table, a folder of Markdown, and GitHub issues fetched over the REST API — and writes **one** Neo4j graph where a `Component` node is reachable from the docs that describe it and the issues that mention it. There is no global "build the graph" pass: every catalog row, doc, and issue is its own [processing component](https://cocoindex.io/docs/programming_guide/processing_component/), so editing one doc or closing one issue re-syncs exactly that node and its edges.

By default the catalog describes CocoIndex itself and the docs and issues are CocoIndex's own, so you get an engineering knowledge graph of this repository. Three settings point it at your catalog, your docs, and your GitHub or GitHub Enterprise repo instead.

## How it works

The graph has three node labels and three edge types:

| Node | Owned by | Edges it declares |
|---|---|---|
| `Component {key, name, kind, area}` | one catalog row | `DEPENDS_ON → Component` |
| `Doc {path, title}` | one Markdown file | `DOCUMENTS → Component` |
| `Issue {number, title, state, url, author}` | one GitHub issue | `MENTIONS → Component` |

The rule that makes multiple sources compose without a merge step: **every node label has exactly one source that owns it, and the other sources only declare edges into it, keyed by the component key.** A doc never declares a `Component` node; it declares a `DOCUMENTS` edge whose target is a key. The Neo4j connector merges edges onto their endpoints, so write order between sources never matters.

Mentions are resolved by a deliberately simple resolver — whole-word, case-insensitive alias lookup against the catalog — in one method, `Catalog.resolve`. The catalog is provided as a [context value](https://cocoindex.io/docs/programming_guide/context/) with change detection on, so adding an alias re-processes the docs and issues that depend on it. Read it in [`main.py`](main.py):

```python
@coco.fn(memo=True)
async def process_doc(file: localfs.File, doc_table: neo4j.TableTarget[Doc], documents_rel: neo4j.RelationTarget[Any]) -> None:
    text = await file.read_text()
    path = file.file_path.path.as_posix()
    doc_table.declare_record(row=Doc(path=path, title=_doc_title(text, path)))
    for key in coco.use_context(CATALOG).resolve(text):
        documents_rel.declare_relation(from_id=path, to_id=key)

@coco.fn
async def app_main(docs_dir: pathlib.Path) -> None:
    component_table = await neo4j.mount_table_target(KG_DB, "Component", ..., primary_key="key")
    doc_table = await neo4j.mount_table_target(KG_DB, "Doc", ..., primary_key="path")
    issue_table = await neo4j.mount_table_target(KG_DB, "Issue", ..., primary_key="number")
    depends_on_rel = await neo4j.mount_relation_target(KG_DB, "DEPENDS_ON", component_table, component_table)
    documents_rel = await neo4j.mount_relation_target(KG_DB, "DOCUMENTS", doc_table, component_table)
    mentions_rel = await neo4j.mount_relation_target(KG_DB, "MENTIONS", issue_table, component_table)

    catalog = postgres.PgTableSource(coco.use_context(CATALOG_DB), table_name=CATALOG_TABLE, row_type=CatalogRow)
    await coco.mount_each(coco.component_subpath("catalog"), process_component,
                          catalog.fetch_rows().items(lambda row: row.key), component_table, depends_on_rel)
    docs = localfs.walk_dir(docs_dir, recursive=True, path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md", "**/*.mdx"]))
    await coco.mount_each(coco.component_subpath("docs"), process_doc, docs.items(), doc_table, documents_rel)
    await coco.mount_each(coco.component_subpath("github"), process_issue, fetch_issues(), issue_table, mentions_rel)
```

Three `mount_each` calls, three source kinds: a database table, a directory walk, and an async generator over a REST API. The GitHub source uses the search endpoint so pull requests are excluded server-side; the same code talks to GitHub Enterprise via `GITHUB_API_URL`. The docs folder is passed as a `ContextKey`, so `Doc` keys are paths relative to it and memoization survives moving the folder.

## Why it's worth a star ⭐

- **Heterogeneous sources, one graph.** Postgres, local files, and an HTTP API in one app, sharing one set of node tables. Any keyed async iterable is a source; no connector is needed for your internal APIs.
- **No fan-in bottleneck.** Nothing collects "all entities" into one component. Each item owns its node and edges, so a million issues are a million small incremental units, not one giant re-run.
- **Edges by key, not by lookup.** A doc declares `DOCUMENTS → "postgres"`; it never needs to find or create the `Component` node. Sources stay independent and write order is irrelevant.
- **Resolution is a single seam.** Swap `Catalog.resolve` for an embedding, LLM, or external resolver. Only the docs and issues whose mentions change are re-processed.
- **Incremental across all three.** Edit one doc, close one issue, add one catalog row: exactly those components re-run, and vanished items take their nodes and edges with them.

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

**3. Seed the component catalog** — one row per CocoIndex component, with aliases and dependencies:

```sh
psql "$POSTGRES_URL" -f ./prepare_source_data.sql
```

**4. Build the graph** — reads the catalog, walks the repo's docs folder, fetches the repo's issues (a few unauthenticated requests), and syncs Neo4j:

```sh
cocoindex update main
```

Run it again and nothing is written: every item is memoized on its content. Edit a doc page, or wait for an issue to change, and only that item re-syncs.

**5. Explore the graph** — open [Neo4j Browser](http://localhost:7474) (`neo4j` / `cocoindex`). These questions can only be answered because the three sources share component keys:

```cypher
// Open issues about anything the Rust SDK depends on, transitively
MATCH (:Component {key: 'rust_sdk'})-[:DEPENDS_ON*1..]->(c:Component)<-[:MENTIONS]-(i:Issue {state: 'open'})
RETURN c.key AS component, i.number AS issue, i.title AS title
ORDER BY component, issue
```

```cypher
// Connectors with open issues, and the docs pages that cover them
MATCH (d:Doc)-[:DOCUMENTS]->(c:Component {kind: 'connector'})<-[:MENTIONS]-(i:Issue {state: 'open'})
RETURN c.key AS connector, count(DISTINCT i) AS open_issues, collect(DISTINCT d.path)[..5] AS docs
ORDER BY open_issues DESC
```

```cypher
// Which area of the project do open issues cluster in?
MATCH (c:Component)<-[:MENTIONS]-(i:Issue {state: 'open'})
RETURN c.area AS area, count(DISTINCT i) AS open_issues, collect(DISTINCT c.key) AS components
ORDER BY open_issues DESC
```

```cypher
// Issues that cut across the most components
MATCH (i:Issue)-[:MENTIONS]->(c:Component)
WITH i, collect(c.key) AS components
WHERE size(components) >= 3
RETURN i.number AS issue, i.title AS title, components
ORDER BY size(components) DESC LIMIT 10
```

```cypher
// Everything the graph knows about one component
MATCH (c:Component {key: 'postgres'})
OPTIONAL MATCH (c)<-[:DOCUMENTS]-(d:Doc)
OPTIONAL MATCH (c)<-[:MENTIONS]-(i:Issue {state: 'open'})
RETURN c.name AS component, collect(DISTINCT d.path) AS docs, collect(DISTINCT i.number) AS open_issues
```

## Make it yours

- **Your catalog.** Replace `prepare_source_data.sql` with your service or component catalog. Keep a key column, an `aliases` array for the other ways people write each name, and a `depends_on` array if you have a dependency graph. Teams, tiers, and owners are plain columns.
- **Your docs.** Point `DOCS_DIR` at any Markdown or MDX tree — an export of a wiki works the same way as a repo's docs folder.
- **Your tickets.** Set `GITHUB_REPO`, a `GITHUB_TOKEN`, and for GitHub Enterprise `GITHUB_API_URL=https://<host>/api/v3`. A Jira or Confluence source is the same shape: an async generator yielding `(key, item)` pairs for `mount_each`.
- **Your resolver.** `Catalog.resolve` is where whole-word matching becomes an embedding search, an LLM call, or a lookup against your own entity-resolution service.

**What stays simple on purpose.** Every node label here has one source that owns it, so people and teams are properties rather than nodes: a `Person` node mentioned by both an issue and a doc would need a shared owner. Shared entity nodes are the next step for this example, built on shared processing components. Until then, one consequence of single ownership is worth knowing: removing a row from the catalog removes its `Component` node and every edge into it from the other sources.

<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=7f27e85b-be3a-411a-b612-0b9d53711814&page=examples/multi_source_knowledge_graph" alt="" width="1" height="1" />
