# Build a Knowledge Graph for Docs — Neo4j (CocoIndex v1)

Turn a folder of Markdown documentation into a concept knowledge graph in
[Neo4j](https://neo4j.com/). For each document an LLM (via
[LiteLLM](https://docs.litellm.ai/) + [instructor](https://python.useinstructor.com/))
produces a short summary and a set of `(subject, predicate, object)` triples
about the concepts it covers — "concepts, not code" — and the triples become a
property graph.

This is the CocoIndex **v1** port of the blog post
[Build a Knowledge Graph for Documents](https://cocoindex.io/blogs/knowledge-graph-for-docs/).

Please drop [CocoIndex on Github](https://github.com/cocoindex-io/cocoindex) a
star to support us and stay tuned for more updates. Thank you so much 🥥🤗.
[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)

## What this builds

- `Document` nodes — one per Markdown file, keyed by filename, with an
  LLM-generated `title` and `summary`
- `Entity` nodes — one per distinct concept named in a triple, keyed by `value`
- Relationships:
  - `RELATIONSHIP` — `Entity → Entity`, with the `predicate` stored on the edge
  - `MENTION` — `Document → Entity`, recording which document named which concept

The flow watches the source folder and keeps the graph up to date
incrementally.

## How it works

The pipeline runs in two phases:

1. **Per-file extraction.** Read each Markdown file, extract a `DocumentSummary`
   (title + summary) and a list of relationship triples with LiteLLM +
   instructor. The `Document` node is declared in this phase; the triples are
   carried forward.
2. **Graph building.** A single pass declares the deduplicated `Entity` nodes
   and the `RELATIONSHIP` / `MENTION` edges across all documents. Each distinct
   triple is keyed by a stable hash, so re-asserting the same fact in another
   doc maps to the same edge.

CocoIndex reconciles changes incrementally — re-running after editing one doc
only re-extracts that doc, and the graph pass only re-runs when the set of
triples changes. To collapse near-identical entity names (e.g. "CocoIndex" vs
"Cocoindex"), add an entity-resolution pass like the one in
[`meeting_notes_graph_neo4j`](../meeting_notes_graph_neo4j).

## Prerequisites

- A running Neo4j 5.18+ instance:
  ```sh
  docker run -d \
    -p 7474:7474 -p 7687:7687 \
    -e NEO4J_AUTH=neo4j/cocoindex \
    --name cocoindex-neo4j \
    neo4j:5.26-community
  ```
  The browser UI is at <http://localhost:7474>; log in with `neo4j` /
  `cocoindex`.

- An LLM. Defaults to OpenAI (set `OPENAI_API_KEY`); set `LLM_MODEL` to any
  [LiteLLM provider](https://docs.litellm.ai/docs/providers) — e.g.
  `LLM_MODEL=ollama/llama3.2` to run the extraction locally with no API key.

## Environment

Copy `.env.example` to `.env` and fill in the blanks:

```sh
cp .env.example .env
set -a && source .env && set +a
```

## Run

Install dependencies:

```sh
uv pip install -e .
```

This example ships a small `markdown_files/` folder of sample concept docs so it
runs out of the box. Build/update the graph:

```sh
cocoindex update main
```

To index your own docs, drop `.md` / `.mdx` files into `markdown_files/` (or
point `sourcedir` in `main.py` at another directory — e.g. CocoIndex's own
`docs/`) and re-run.

## Browse the knowledge graph

Open Neo4j Browser at <http://localhost:7474>, log in, and run Cypher queries:

```cypher
// Everything
MATCH p=()-->() RETURN p LIMIT 200

// Concept-to-concept relationships
MATCH (a:Entity)-[r:RELATIONSHIP]->(b:Entity)
RETURN a.value, r.predicate, b.value

// Which documents mention which concepts
MATCH (d:Document)-[:MENTION]->(e:Entity)
RETURN d.filename, d.title, e.value

// Concepts mentioned in the most documents
MATCH (d:Document)-[:MENTION]->(e:Entity)
RETURN e.value, count(DISTINCT d) AS docs
ORDER BY docs DESC LIMIT 10
```

To wipe the graph between runs:

```cypher
MATCH (n) DETACH DELETE n
```
