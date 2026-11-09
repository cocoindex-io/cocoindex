# Knowledge Graph for Docs — Neo4j (Rust)

Rust port of the Python
[`docs_to_knowledge_graph`](../../docs_to_knowledge_graph) example.

An LLM reads each Markdown doc and emits *(subject, predicate, object)*
triples; the shared concept nodes and predicate edges become a graph you query
in Cypher. Point it at a docs folder, and it re-extracts only the doc you
edited, then reconciles the graph.

## How it works

Two node types, two relationship types:

- **`Document`** nodes — one per Markdown file, keyed by filename, with an
  LLM-generated `title` and `summary`.
- **`Entity`** nodes — one per distinct concept named in any triple, keyed by
  the concept name and **shared** across documents.
- **`RELATIONSHIP`** edges — `Entity → Entity`, with the `predicate` stored as
  an edge property; keyed by a stable hash of the triple (`generate_id`).
- **`MENTION`** edges — `Document → Entity`, recording which document named
  which concept; no payload, keyed by the endpoints.

Because entities are shared across documents, the pipeline runs in two phases:

1. `process_file`, one child component per document: declares the `Document`
   node and carries the doc's triples forward. The two LLM calls
   (`extract_summary`, `extract_relationships`) are memoized by content.
2. `build_graph`, a single component that owns the deduplicated `Entity` nodes
   and both edge types across all documents.

## Parallel to the Python example

| Concern          | Python                                                   | Rust (this example)                                          |
| ---------------- | -------------------------------------------------------- | ------------------------------------------------------------ |
| LLM extraction   | `instructor` + LiteLLM, Pydantic models                  | `ApiChatClient::chat_json` + `serde` structs                 |
| Per-doc phase    | `coco.use_mount(component_subpath("file", key), ...)`    | `ctx.mount_each(files, |(key, _)| "file/{key}", ...)`        |
| Graph phase      | `coco.mount(component_subpath("build_graph"), ...)`      | `ctx.mount(&"build_graph", ...)`                             |
| Node targets     | `neo4j.mount_node_target`                                | `neo4j::mount_table_target`                                  |
| Edge targets     | `neo4j.mount_relationship_target` / `declare_relationship` | `neo4j::mount_relation_target[_with_schema]` / `declare_relation[_record]` |
| Row schema       | Dataclass annotations                                    | `#[derive(SchemaFields)]` + `TableSchema::from_row`          |
| Edge identity    | `generate_id((subject, predicate, object))`              | `generate_id(ctx, &(subject, predicate, object))`            |
| Cache busting    | `ContextKey("llm_model", detect_change=True)`            | `context_key!(static LLM_MODEL: String, detect_change)`      |

The LLM goes through an OpenAI-compatible endpoint: `OPENAI_API_KEY`, and
`LLM_BASE_URL` for another provider (e.g. Ollama). The model id is a plain
model name (`gpt-5-mini`), not a LiteLLM provider string.

## Run

**1. Start Neo4j:**

```sh
docker run -d -p 7474:7474 -p 7687:7687 -e NEO4J_AUTH=neo4j/cocoindex --name cocoindex-neo4j neo4j:5.26-community
```

**2. Configure:**

```sh
export OPENAI_API_KEY=...
# optional: LLM_MODEL (default gpt-5-mini), LLM_BASE_URL,
# NEO4J_URI (default bolt://localhost:7687), NEO4J_USER / NEO4J_PASSWORD (default neo4j / cocoindex), NEO4J_DATABASE
```

**3. Build the graph** — the example ships a `markdown_files/` folder of sample
docs so it runs out of the box:

```sh
cargo run                         # graph ./markdown_files
cargo run -- /path/to/your/docs   # graph another folder (*.md, *.mdx)
```

**4. Explore the graph** — open [Neo4j Browser](http://localhost:7474)
(`neo4j` / `cocoindex`) and ask:

```cypher
-- How concepts relate
MATCH (a:Entity)-[r:RELATIONSHIP]->(b:Entity)
RETURN a.value, r.predicate, b.value

-- Concepts mentioned in the most documents
MATCH (d:Document)-[:MENTION]->(e:Entity)
RETURN e.value, count(DISTINCT d) AS docs
ORDER BY docs DESC LIMIT 10
```

Edit one doc and only that doc re-extracts; a no-change re-run makes zero LLM
calls. The LLM will sometimes name the same concept two ways ("CocoIndex" vs
"Cocoindex"); the [meeting notes graph example](../meeting_notes_graph_surrealdb)
adds an embedding + LLM entity-resolution pass that collapses near-duplicates.
