# Conversation to Knowledge (Rust)

Rust port of the Python [`conversation_to_knowledge`](../../conversation_to_knowledge) example.

Turns podcast/interview sessions into a **knowledge graph in SurrealDB**:
sessions, statements, persons, techs, orgs, and the relationships between them.

**Pipeline:** read sources → (YouTube: `yt-dlp` + AssemblyAI diarized transcription | local transcript) → two LLM passes (identify speakers/metadata, then extract statements + mentioned person/tech/org) → entity resolution (embed names → cosine candidates → LLM confirm → union-find) → SurrealDB graph.

## How it maps to the Python example

| Step | Python | Rust (this example) |
|------|--------|---------------------|
| Read sources | `localfs.walk_dir` | `cocoindex::walk` (`*.txt` URLs, `*.json` transcripts) |
| Per-session incremental skip | `@coco.fn(memo=True)` | `#[cocoindex::function(memo)]` |
| Audio + transcription | `yt-dlp` + `assemblyai` SDK | `yt-dlp` (subprocess) + AssemblyAI REST (`reqwest`) |
| LLM extraction (2 passes) | `instructor` + `litellm` | `reqwest` → OpenAI JSON mode |
| Stable ids | `IdGenerator` | `cocoindex::IdGenerator` |
| Entity resolution | `ops.entity_resolution` (faiss + LLM) | hand-rolled: `fastembed` → cosine → LLM confirm → union-find |
| Graph store | `surrealdb` connector (`TableTarget`/`RelationTarget`) | `surrealdb` crate (`UPSERT` nodes + `RELATE` edges) |
| Embedder change-detection | `ContextKey(..., detect_change=True)` | `ContextKey::new_with_state(...)` |

### Design notes / where it differs

- **No declarative graph target in the Rust SDK.** Python's `TableTarget`/`RelationTarget` sync the graph incrementally. Here, the costly per-session fetch+LLM work is **memoized**, and phase 3 does an **idempotent full graph rebuild** (clear + recreate). The incremental win is the memo; the cheap rebuild keeps the graph correct.
- **Embedding model:** the entity-resolution embedder uses `fastembed` `all-MiniLM-L6-v2` (local ONNX) rather than Python's `snowflake-arctic-embed-xs` — any sentence embedder works for name dedup.
- **Two input modes** (both supported):
  - `input/*.txt` — one YouTube URL per line (real path; needs `yt-dlp`, `ffmpeg`, `ASSEMBLYAI_API_KEY`).
  - `input/*.json` — a pre-transcribed session (cheap, audio-free; see [`input/sample.json`](input/sample.json)). Great for trying the extract→resolve→graph half without audio.

## Prerequisites

- **SurrealDB** (graph store):
  ```sh
  docker run -d --name surrealdb -p 8787:8000 surrealdb/surrealdb:latest \
    start --user root --pass root surrealkv:/data/database
  ```
- **OpenAI API key**: `export OPENAI_API_KEY=sk-...` (override model with `LLM_MODEL`, default `gpt-4o-mini`).
- For the YouTube path only: `yt-dlp` + `ffmpeg` installed and `export ASSEMBLYAI_API_KEY=...`.
- The embedding model downloads automatically (fastembed) on first run.

Connection/config via env (defaults shown): `SURREALDB_URL=127.0.0.1:8787`, `SURREALDB_NS=cocoindex`, `SURREALDB_DB=yt_conversations`, `SURREALDB_USER=root`, `SURREALDB_PASS=root`.

## Usage

```sh
# Build the graph from the input directory (default ./input).
cargo run -- index            # or: cargo run -- index /path/to/input
```

Re-running skips fetch+LLM for unchanged sessions (memoized) and rebuilds the graph.

## Inspecting the graph

```sh
curl -s -X POST http://localhost:8787/sql \
  -H "surreal-ns: cocoindex" -H "surreal-db: yt_conversations" -u root:root \
  -d "SELECT name FROM person; SELECT ->statement_mentions->{tech,org} FROM statement LIMIT 5;"
```
