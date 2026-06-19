# Meeting Notes Graph — Neo4j (Rust)

Rust companion to the Python
[`meeting_notes_graph_neo4j`](../../meeting_notes_graph_neo4j) example.

It builds the same graph shape in Neo4j:

- `Meeting` nodes
- `Person` nodes
- `Task` nodes
- `ATTENDED`, `DECIDED`, and `ASSIGNED_TO` relationships

For local end-to-end testing this Rust example uses deterministic Markdown
parsing over `input/*.md` instead of Google Drive + LLM extraction. The target
side is native CocoIndex Rust: `cocoindex::neo4j` table and relation targets
with target-state reconciliation.

## Run

Start Neo4j:

```sh
docker run -d --name cocoindex-neo4j-rust \
  -p 7474:7474 -p 7687:7687 \
  -e NEO4J_AUTH=neo4j/cocoindex \
  neo4j:5.26-community
```

Build the graph:

```sh
export NEO4J_URI=bolt://localhost:7687
export NEO4J_USER=neo4j
export NEO4J_PASSWORD=cocoindex
export NEO4J_DATABASE=neo4j
cargo run
```

Inspect:

```cypher
MATCH p=()-->() RETURN p LIMIT 100
MATCH (p:Person)-[:ATTENDED]->(m:Meeting) RETURN p.name, m.time, m.note_file
MATCH (p:Person)-[:ASSIGNED_TO]->(t:Task) RETURN p.name, t.description
```
