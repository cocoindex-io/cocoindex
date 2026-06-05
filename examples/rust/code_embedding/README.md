# Code Embedding (Rust)

Rust port of the Python [`code_embedding`](../../code_embedding) example.

Pipeline: **walk → detect language → tree-sitter chunk → embed → store in pgvector**, with a vector-similarity query mode.

## How it maps to the Python example

| Step | Python | Rust (this example) |
|------|--------|---------------------|
| Walk files | `localfs.walk_dir` | `cocoindex::walk` |
| Per-file incremental skip | `@coco.fn(memo=True)` | `#[cocoindex::function(memo)]` |
| Language detection | `detect_code_language` | `cocoindex_ops_text::prog_langs::detect_language` |
| Chunking | `RecursiveSplitter` | `cocoindex_ops_text::split::RecursiveChunker` |
| Embeddings | `SentenceTransformerEmbedder` (all-MiniLM-L6-v2) | `fastembed` `AllMiniLML6V2` — **the same model**, local ONNX |
| Embedder change-detection | `ContextKey(..., detect_change=True)` | `ContextKey::new_with_state("embedder", \|e\| e.model_name)` |
| Vector store | `postgres.TableTarget` + `declare_vector_index` | `cocoindex::postgres` `TableTarget` + `declare_vector_index` |
| Stable row ids | `IdGenerator.next_id(chunk.text)` | `IdGenerator::next_id(ctx, chunk_text)` |
| Query | pgvector `<=>` | pgvector `<=>` |

Chunking and language detection *do* exist in the engine (`cocoindex_ops_text`) but aren't re-exported by the `cocoindex` SDK crate, so we depend on that crate directly.

## Prerequisites

- **Postgres with the `pgvector` extension.** Quick start:
  ```sh
  docker run -d --name cocoindex-pg -p 5432:5432 \
    -e POSTGRES_USER=cocoindex -e POSTGRES_PASSWORD=cocoindex -e POSTGRES_DB=cocoindex \
    pgvector/pgvector:pg16
  ```
  Override the connection with `POSTGRES_URL` (default `postgres://cocoindex:cocoindex@localhost/cocoindex`).
- The embedding model is downloaded automatically by `fastembed` on first run (cached afterwards).

## Usage

```sh
# Index (incremental — unchanged files are skipped). Defaults to the repo root.
cargo run -- index            # or: cargo run -- index /path/to/code

# Query
cargo run -- query "how is memoization implemented"
```

Re-running `index` after editing a file re-embeds only that file; deleting a file
removes its rows on the next `index` through target reconciliation.
