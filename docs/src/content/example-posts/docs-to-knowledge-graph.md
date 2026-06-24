---
title: Turn Docs into a Knowledge Graph
description: 'Build a concept knowledge graph from Markdown docs with CocoIndex V1 and Neo4j — LLM relationship extraction with instructor + LiteLLM, declarative graph targets, and incremental sync as docs change.'
slug: docs-to-knowledge-graph
image: https://cocoindex.io/blobs/docs-v1/img/examples/docs-to-knowledge-graph/cover.png
tags: [knowledge-graph, llm-extraction]
---

![Turn documentation into a knowledge graph with LLM extraction and CocoIndex](https://cocoindex.io/blobs/docs-v1/img/examples/docs-to-knowledge-graph/cover.png)

Documentation is a web of concepts pretending to be a list of files. "Incremental processing relies on change detection", "a target receives declared target states" — every page asserts relationships like these, but they're locked in prose. You can search docs for keywords; you can't ask how the concepts connect.

In this tutorial, we'll build a [CocoIndex](https://github.com/cocoindex-io/cocoindex) pipeline that turns a folder of Markdown docs into a concept knowledge graph in [Neo4j](https://neo4j.com/). For each document, an LLM extracts a summary plus a set of `(subject, predicate, object)` triples — *"engine detects source changes"*, *"triple becomes relationship in graph"* — and the triples become a property graph you can query in Cypher.

The whole pipeline is ordinary `async` Python and your own types. The heavy lifting — [incremental processing](https://cocoindex.io/docs/programming_guide/core_concepts/), change tracking, managed graph targets — runs in a Rust engine underneath, so editing one doc re-extracts only that doc, and the graph reconciles itself: no orphaned nodes, no stale edges, no cleanup scripts.

[→ View on GitHub](https://github.com/cocoindex-io/cocoindex/tree/main/examples/docs_to_knowledge_graph)

## Use cases

- **GraphRAG over your docs** — retrieval that follows concept relationships instead of (or alongside) vector similarity.
- **Agent memory and context** — give an agent a queryable map of your product's concepts and how they relate.
- **Docs navigation and gap analysis** — which pages cover which concepts, which concepts are mentioned everywhere but defined nowhere.

## What we're building

The graph schema is small — two node types, two relationship types:

![Graph schema: Document nodes connected to Entity nodes by MENTION edges, and Entity nodes connected to each other by RELATIONSHIP edges carrying the predicate](https://cocoindex.io/blobs/docs-v1/img/examples/docs-to-knowledge-graph/schema.png)

- **`Document`** nodes — one per Markdown file, keyed by filename, with an LLM-generated `title` and `summary`.
- **`Entity`** nodes — one per distinct concept named in any triple, keyed by the concept name.
- **`RELATIONSHIP`** edges — `Entity → Entity`, with the `predicate` stored as an edge property.
- **`MENTION`** edges — `Document → Entity`, recording which document named which concept.

Here's the result in Neo4j Browser, built from a docs folder — documents (cyan) at the center of the concepts (pink) they mention:

![The resulting graph in Neo4j Browser: Document and Entity nodes joined by MENTION and RELATIONSHIP edges](https://cocoindex.io/blobs/docs-v1/img/examples/docs-to-knowledge-graph/neo4j-browser.png)

## Why CocoIndex for knowledge graphs

A knowledge graph over living docs is exactly the kind of pipeline that's easy to demo and hard to keep correct:

- **LLM extraction is expensive.** [Memoization](https://cocoindex.io/docs/advanced_topics/memoization_keys/) caches every extraction by content — edit one doc and only that doc hits the LLM again. A no-change re-run makes zero LLM calls.
- **Graphs accumulate garbage.** Delete a doc, rename a concept, tighten a prompt — and a hand-rolled pipeline leaves orphaned nodes and stale edges behind. In CocoIndex, nodes and edges are [target states](https://cocoindex.io/docs/programming_guide/target_state/): you declare what should exist, and the engine inserts, updates, and deletes the difference.
- **Cross-document steps need cross-document tracking.** Entities are shared between docs, so they can't be owned by any single file's processing. The two-phase shape below — per-file fan-out, then one graph pass — maps directly onto CocoIndex's [processing components](https://cocoindex.io/docs/programming_guide/processing_component/).
- **Plain Python.** Extraction is [instructor](https://github.com/instructor-ai/instructor) over [LiteLLM](https://docs.litellm.ai/) with your own Pydantic models — swap in any provider, prompt, or schema.

## Pipeline overview

![CocoIndex flow: Markdown docs walked from the filesystem, per-doc LLM extraction declaring Document nodes and carrying triples forward, then a single graph-building pass declaring Entity nodes and edges into Neo4j](https://cocoindex.io/blobs/docs-v1/img/examples/docs-to-knowledge-graph/flow-v1.png)

The pipeline runs in two phases:

1. **Per-file extraction** — for each Markdown file: extract a summary and the relationship triples with an LLM. The `Document` node is declared here; the triples are carried forward.
2. **Graph building** — one pass over all triples declares the deduplicated `Entity` nodes and the `RELATIONSHIP` / `MENTION` edges.

You [declare the transformation](https://cocoindex.io/docs/programming_guide/core_concepts/) with native Python; CocoIndex works out what to insert, update, and delete. Think: **target_state = transformation(source_state)**.

## Define the graph schema

Nodes and edges are plain dataclasses. Each becomes a Neo4j label (or relationship type), with one field as the primary key:

```python title="main.py"
@dataclass
class Document:
    filename: str  # primary key
    title: str
    summary: str


@dataclass
class Entity:
    value: str  # primary key — the concept name


@dataclass
class Relationship:
    """RELATIONSHIP edge payload. ``id`` is a stable hash of the triple so the
    same (subject, predicate, object) always maps to a single edge; the
    ``predicate`` is stored as an edge property."""

    id: int
    predicate: str
```

`MENTION` carries no payload, so it gets no schema at all — the Neo4j connector derives its identity from the (document, entity) endpoints: one edge per pair.

## Shared resources: the lifespan

The [lifespan](https://cocoindex.io/docs/programming_guide/context/) provides what every step needs — the Neo4j connection factory and the LLM model id — once at startup, via [context keys](https://cocoindex.io/docs/programming_guide/context/):

```python title="main.py"
KG_DB = coco.ContextKey[neo4j.ConnectionFactory]("kg_db")
LLM_MODEL = coco.ContextKey[str]("llm_model", detect_change=True)


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
    builder.provide(LLM_MODEL, os.environ.get("LLM_MODEL", "openai/gpt-5-mini"))
    yield
```

Note `detect_change=True` on `LLM_MODEL`: the model id participates in change detection. Point `LLM_MODEL` at a different model and CocoIndex knows every memoized extraction is stale — the whole corpus re-extracts on the next run, with no cache to clear manually. The model is any [LiteLLM provider string](https://docs.litellm.ai/docs/providers); set `LLM_MODEL=ollama/llama3.2` to run extraction locally with no API key.

## LLM extraction

Extraction is typed end to end: Pydantic models describe what we want, instructor enforces them. The field descriptions double as instructions to the model:

```python title="main.py"
class ExtractedRelationship(pydantic.BaseModel):
    subject: str = pydantic.Field(
        description="The concept the statement is about, e.g. 'CocoIndex'."
    )
    predicate: str = pydantic.Field(
        description="How subject relates to object, e.g. 'supports'."
    )
    object: str = pydantic.Field(
        description="The related concept, e.g. 'Incremental Processing'."
    )


class RelationshipList(pydantic.BaseModel):
    relationships: list[ExtractedRelationship] = pydantic.Field(default_factory=list)
```

Two memoized functions call the LLM — one for the summary, one for the triples:

```python title="main.py"
@coco.fn(memo=True)
async def extract_relationships(content: str) -> list[Triple]:
    client = instructor.from_litellm(litellm.acompletion, mode=instructor.Mode.JSON)
    result = await client.chat.completions.create(
        model=coco.use_context(LLM_MODEL),
        response_model=RelationshipList,
        messages=[
            {"role": "system", "content": RELATIONSHIP_PROMPT},
            {"role": "user", "content": content},
        ],
    )
    validated = RelationshipList.model_validate(result.model_dump())
    return [Triple(r.subject, r.predicate, r.object) for r in validated.relationships]
```

[`@coco.fn(memo=True)`](https://cocoindex.io/docs/programming_guide/function/) is what makes iteration affordable: the result is cached keyed by the document content (and the function's own code). Unchanged docs never hit the LLM again. The prompt steers extraction toward *"concepts, not code"* — salient noun-phrase subjects and objects, short verb-phrase predicates, only relationships supported by the text.

## Phase 1: per-file extraction

![Phase 1 — one processing component per doc: each file goes through memoized LLM extraction, declares its Document node into Neo4j, and returns DocTriples for phase 2](https://cocoindex.io/blobs/docs-v1/img/examples/docs-to-knowledge-graph/stage-phase1.png)

`process_file` runs once per document: extract the summary, declare the `Document` node, extract the triples, and return them for phase 2.

```python title="main.py"
@coco.fn(memo=True)
async def process_file(
    file: localfs.File,
    document_table: neo4j.TableTarget[Document],
) -> DocTriples:
    content = await file.read_text()
    filename = file.file_path.path.as_posix()

    summary = await extract_summary(content)
    document_table.declare_record(
        row=Document(filename=filename, title=summary.title, summary=summary.summary)
    )

    triples = await extract_relationships(content)
    return DocTriples(filename=filename, triples=triples)
```

Each file runs as its own [processing component](https://cocoindex.io/docs/programming_guide/processing_component/), mounted in `app_main` and keyed by the file path:

```python title="main.py"
file_coros = []
async for path_key, file in files.items():
    file_coros.append(
        coco.use_mount(
            coco.component_subpath("file", path_key),
            process_file,
            file,
            document_table,
        )
    )
docs: list[DocTriples] = list(await asyncio.gather(*file_coros))
```

Why a component per file? Ownership. The component at `("file", path_key)` owns that document's `Document` node — if the file disappears, so does the component, and CocoIndex deletes its node (and the `MENTION` edges pointing from it) automatically. [`coco.use_mount`](https://cocoindex.io/docs/programming_guide/app/) returns each file's triples, and `asyncio.gather` runs all files concurrently.

## Phase 2: build the concept graph

![Phase 2 — a single build_graph component: all docs' triples are deduplicated into Entity nodes and RELATIONSHIP / MENTION edges, declared into Neo4j](https://cocoindex.io/blobs/docs-v1/img/examples/docs-to-knowledge-graph/stage-phase2.png)

A single component takes every file's triples and declares the cross-document parts of the graph: deduplicated `Entity` nodes and the two edge types.

```python title="main.py"
@coco.fn
async def build_graph(
    docs: list[DocTriples],
    entity_table: neo4j.TableTarget[Entity],
    relationship_rel: neo4j.RelationTarget[Relationship],
    mention_rel: neo4j.RelationTarget[Any],
) -> None:
    entities: set[str] = set()
    mentions: set[tuple[str, str]] = set()  # (filename, entity value)

    for doc in docs:
        for t in doc.triples:
            entities.add(t.subject)
            entities.add(t.object)
            mentions.add((doc.filename, t.subject))
            mentions.add((doc.filename, t.object))

            rel_id = await generate_id((t.subject, t.predicate, t.object))
            relationship_rel.declare_relation(
                from_id=t.subject,
                to_id=t.object,
                record=Relationship(id=rel_id, predicate=t.predicate),
            )

    for value in entities:
        entity_table.declare_record(row=Entity(value=value))

    for filename, entity in mentions:
        mention_rel.declare_relation(from_id=filename, to_id=entity)
```

Two details carry the correctness:

- **Stable edge identity.** [`generate_id`](https://cocoindex.io/docs/common_resources/id_generation/) hashes the triple, so the same `(subject, predicate, object)` always maps to the same edge — re-asserting a fact in another doc is a no-op, not a duplicate.
- **Entities live here, not in phase 1.** Concepts are shared across documents, so no single file's component can own them. The graph component owns the entity set as one target state; when the set of triples changes, CocoIndex diffs it — entities no longer named anywhere are deleted from Neo4j along with their edges.

This is plain Python doing set-dedup in memory — no framework abstractions. The declarative part is only at the boundary: `declare_record` / `declare_relation` say what should exist, and the engine reconciles.

## Wire it up: app_main

`app_main` mounts the targets and runs the two phases. Node tables come first, because relation targets are declared *between* two node tables — that's how the connector knows each edge's endpoint labels and keys:

```python title="main.py"
@coco.fn
async def app_main(sourcedir: pathlib.Path) -> None:
    document_table = await neo4j.mount_table_target(
        KG_DB,
        "Document",
        await neo4j.TableSchema.from_class(Document, primary_key="filename"),
        primary_key="filename",
    )
    entity_table = await neo4j.mount_table_target(
        KG_DB,
        "Entity",
        await neo4j.TableSchema.from_class(Entity, primary_key="value"),
        primary_key="value",
    )

    relationship_rel = await neo4j.mount_relation_target(
        KG_DB,
        "RELATIONSHIP",
        entity_table,
        entity_table,
        await neo4j.TableSchema.from_class(Relationship, primary_key="id"),
        primary_key="id",
    )
    mention_rel = await neo4j.mount_relation_target(
        KG_DB, "MENTION", document_table, entity_table
    )

    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md", "**/*.mdx"]),
    )
    # ... phase 1 fan-out (above), then:
    await coco.mount(
        coco.component_subpath("build_graph"),
        build_graph,
        docs,
        entity_table,
        relationship_rel,
        mention_rel,
    )


app = coco.App(
    coco.AppConfig(name="DocsToKnowledgeGraph"),
    app_main,
    sourcedir=pathlib.Path("./markdown_files"),
)
```

That's the entire pipeline — one file, ~200 lines.

## Run the pipeline

You'll need a Neo4j instance and an LLM key. Start Neo4j with Docker:

```sh
docker run -d \
  -p 7474:7474 -p 7687:7687 \
  -e NEO4J_AUTH=neo4j/cocoindex \
  --name cocoindex-neo4j \
  neo4j:5.26-community
```

Set up the environment and install:

```sh
cp .env.example .env   # fill in OPENAI_API_KEY (or set LLM_MODEL=ollama/llama3.2)
pip install -e .
```

The example ships a small `markdown_files/` folder of sample docs so it runs out of the box. Build the graph:

```sh
cocoindex update main
```

To graph your own docs, drop `.md` / `.mdx` files into `markdown_files/` — or point `sourcedir` at your real docs folder — and re-run.

## Explore the graph

Open [Neo4j Browser](http://localhost:7474) (`neo4j` / `cocoindex`) and ask the graph questions:

```cypher
// Everything
MATCH p=()-->() RETURN p LIMIT 200

// How concepts relate
MATCH (a:Entity)-[r:RELATIONSHIP]->(b:Entity)
RETURN a.value, r.predicate, b.value

// Concepts mentioned in the most documents
MATCH (d:Document)-[:MENTION]->(e:Entity)
RETURN e.value, count(DISTINCT d) AS docs
ORDER BY docs DESC LIMIT 10
```

## Incremental updates

This is where the declarative model pays for itself. You never compute a diff or write cleanup logic — change something, re-run `cocoindex update main`, and CocoIndex works out the minimum set of LLM calls and graph writes.

**Data changes.**

- **Edit one doc** — only that doc's component re-runs and re-extracts. If its triples changed, `build_graph` re-runs and diffs the graph: new entities and edges are inserted, ones no longer supported anywhere are deleted. Every other doc's extraction is served from the memo cache.
- **Add a doc** — one new component, one extraction, plus the graph diff.
- **Delete a doc** — its component disappears, so its `Document` node and `MENTION` edges are cleaned up automatically; concepts only that doc introduced vanish from the entity set on the next graph pass.
- **Nothing changed** — the run completes in a fraction of a second with zero LLM calls.

**Logic changes** are reconciled the same way:

- **Tighten the extraction prompt** — the function's code changed, so all docs re-extract; the graph then diffs against what's in Neo4j and applies only the difference.
- **Swap the LLM** — `LLM_MODEL` has `detect_change=True`, so changing the env var invalidates every memoized extraction. No cache to clear, no manual rebuild.

## Run it

The full, runnable example is in the CocoIndex repo: [examples/docs_to_knowledge_graph](https://github.com/cocoindex-io/cocoindex/tree/main/examples/docs_to_knowledge_graph).

One natural next step: the LLM will sometimes name the same concept two ways ("CocoIndex" vs "Cocoindex"). The [meeting notes graph example](https://github.com/cocoindex-io/cocoindex/tree/main/examples/meeting_notes_graph_neo4j) adds an embedding + LLM [entity-resolution](https://cocoindex.io/docs/ops/entity_resolution/) pass that collapses near-duplicates — it drops into this pipeline between the two phases. For a bigger end-to-end graph build (transcription, multi-entity schemas, polymorphic edges), see [Turn Podcasts into a Knowledge Graph](https://cocoindex.io/docs/examples/podcast-to-knowledge-graph).

Got a docs folder, a wiki, or a pile of specs you want to turn into a graph? Come tell us on [Discord](https://discord.com/invite/zpA9S2DR7s) — and if this was useful, [star CocoIndex on GitHub](https://github.com/cocoindex-io/cocoindex).
