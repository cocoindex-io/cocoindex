# Meeting Notes Graph — FalkorDB (Rust)

Rust companion to the Python
[`meeting_notes_graph_falkordb`](../../meeting_notes_graph_falkordb) example.

It builds the same graph shape in FalkorDB:

- `Meeting` nodes
- `Person` nodes
- `Task` nodes
- `ATTENDED`, `DECIDED`, and `ASSIGNED_TO` relationships

For local end-to-end testing this Rust example uses deterministic Markdown
parsing over `input/*.md` instead of Google Drive + LLM extraction. The target
side is native CocoIndex Rust: `cocoindex::falkordb` table and relation targets
with target-state reconciliation.

## Run

Start FalkorDB:

```sh
docker run -d --name cocoindex-falkordb-rust \
  -p 6379:6379 -p 3000:3000 \
  falkordb/falkordb:latest
```

Build the graph:

```sh
export FALKORDB_URI=falkor://localhost:6379
export FALKORDB_GRAPH=meeting_notes
cargo run
```

Inspect:

```sh
redis-cli GRAPH.QUERY meeting_notes "MATCH p=()-->() RETURN p"
redis-cli GRAPH.QUERY meeting_notes \
  "MATCH (p:Person)-[:ATTENDED]->(m:Meeting) RETURN p.name, m.time, m.note_file"
```
