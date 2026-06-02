# Text Embedding with LanceDB (Rust)

Rust port of the Python [`text_embedding_lancedb`](../../text_embedding_lancedb)
example. Same pipeline as [`text-embedding`](../text-embedding), but the vector
store is **LanceDB** (an embedded, file-based vector database) via the native
`cocoindex::lancedb` connector instead of Postgres/pgvector.

## Parallel to the Python example

| Concern          | Python                                   | Rust (this example)                                |
| ---------------- | ---------------------------------------- | -------------------------------------------------- |
| Source           | `localfs.walk_dir`                       | `cocoindex::fs::walk`                              |
| Per-file compute | `@coco.fn(memo=True) process_file`       | `#[cocoindex::function(memo)] process_file`         |
| Chunking         | `RecursiveSplitter` (markdown)           | `cocoindex_ops_text` `RecursiveChunker` (markdown)  |
| Embeddings       | `sentence-transformers/all-MiniLM-L6-v2` | `fastembed` `AllMiniLML6V2` (same model, 384-dim)   |
| Target           | `lancedb.TableTarget`                    | `cocoindex::lancedb::LanceTableTarget`              |
| Vector search    | `table.search(vec)`                      | `cocoindex::lancedb::vector_search` (cosine)        |

The `cocoindex::lancedb` connector is a declarative two-level managed target
(table → rows), mirroring `postgres`: it creates the table to match the schema,
upserts changed rows, skips unchanged ones (fingerprint tracking), and deletes
rows that are no longer declared. It's built on the native Rust `lancedb` crate +
Arrow.

## Build requirement: `protoc`

The `lancedb` crate compiles Lance's protobuf definitions, so a **`protoc`**
binary is required to build this example (Lance does not vendor it). Install it
(`brew install protobuf`, or download a release from
<https://github.com/protocolbuffers/protobuf/releases>) and either put it on
`PATH` or point `PROTOC` at it:

```bash
export PROTOC=/path/to/protoc
```

## Run

```bash
# Optional: defaults to ./lancedb_data
export LANCEDB_URI=./lancedb_data

cargo run -- index                 # walk ./markdown_files -> chunk -> embed -> LanceDB
cargo run -- query "your query"    # LanceDB cosine vector search
```

No database server needed — LanceDB stores everything under `LANCEDB_URI`.
