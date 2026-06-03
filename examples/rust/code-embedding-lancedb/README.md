# Code Embedding with LanceDB (Rust)

Rust port of the Python [`code_embedding_lancedb`](../../code_embedding_lancedb) example.

Walks a source tree, detects each file's language, chunks it (tree-sitter-aware),
embeds the chunks, and stores them in **LanceDB** — then serves vector search.

Same pipeline as [`code-embedding`](../code-embedding), but the target is the
native `cocoindex::lancedb` connector instead of Postgres/pgvector.

## Parallel to the Python example

| Concern          | Python                                   | Rust (this example)                                |
| ---------------- | ---------------------------------------- | -------------------------------------------------- |
| Source           | `localfs.walk_dir`                       | `cocoindex::fs::walk`                              |
| Per-file compute | `@coco.fn(memo=True) process_file`       | `#[cocoindex::function(memo)] process_file`         |
| Language detect  | `detect_code_language`                   | `cocoindex_ops_text::prog_langs::detect_language`   |
| Chunking         | `RecursiveSplitter` (1000/300/300)       | `cocoindex_ops_text` `RecursiveChunker` (1000/300/300) |
| Embeddings       | `sentence-transformers/all-MiniLM-L6-v2` | `fastembed` `AllMiniLML6V2` (same model, 384-dim)   |
| Target           | `lancedb.mount_table_target`             | `cocoindex::lancedb::mount_table_target`            |

Incrementality: unchanged files are memo-skipped; chunks of a removed/edited file
are reconciled away by the managed LanceDB `TableTarget`.

> **Build dependency:** LanceDB pulls in crates that compile `.proto` files, so a
> `protoc` (protobuf) compiler must be on `PATH` (`brew install protobuf` /
> `apt-get install protobuf-compiler`).

## Run

```bash
cargo run -- index [SOURCE_DIR]    # default: the repository root
cargo run -- query "your query"    # LanceDB vector search

# LanceDB data dir defaults to ./lancedb_data (override with LANCEDB_URI)
```
