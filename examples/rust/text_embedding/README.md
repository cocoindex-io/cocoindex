# Text Embedding (Rust)

Rust port of the Python [`text_embedding`](../../text_embedding) example.

Walks local markdown files, chunks each file (markdown-aware), embeds the chunks,
and stores them in Postgres/pgvector — then serves similarity search.

## Parallel to the Python example

| Concern          | Python                                   | Rust (this example)                                |
| ---------------- | ---------------------------------------- | -------------------------------------------------- |
| Source           | `localfs.walk_dir`                       | `cocoindex::fs::walk`                              |
| Per-file compute | `@coco.fn(memo=True) process_file`       | `#[cocoindex::function(memo)] process_file`         |
| Chunking         | `RecursiveSplitter` (markdown)           | `cocoindex_ops_text` `RecursiveChunker` (markdown)  |
| Embeddings       | `sentence-transformers/all-MiniLM-L6-v2` | `fastembed` `AllMiniLML6V2` (same model, 384-dim)   |
| Target           | `postgres.TableTarget` + pgvector index  | `postgres::TableTarget` + `declare_vector_index`    |

Incrementality: unchanged files are memo-skipped; chunks of a removed/edited file
are reconciled away (the managed `TableTarget` deletes orphaned rows).

## Run

```bash
export POSTGRES_URL=postgres://cocoindex:cocoindex@localhost/cocoindex   # pgvector-enabled

cargo run -- index                 # walk ./markdown_files -> chunk -> embed -> pgvector
cargo run -- query "your query"    # cosine similarity search
```
