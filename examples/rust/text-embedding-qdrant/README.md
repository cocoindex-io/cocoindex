# Text Embedding with Qdrant (Rust)

Rust port of the Python [`text_embedding_qdrant`](../../text_embedding_qdrant) example.

Same pipeline as [`text-embedding`](../text-embedding), but the vector store is
**Qdrant** via the native `cocoindex::qdrant` collection target.

## Parallel to the Python example

| Concern          | Python                                   | Rust (this example)                                |
| ---------------- | ---------------------------------------- | -------------------------------------------------- |
| Source           | `localfs.walk_dir`                       | `cocoindex::fs::walk`                              |
| Chunking         | `RecursiveSplitter` (markdown)           | `cocoindex_ops_text` `RecursiveChunker` (markdown)  |
| Embeddings       | `sentence-transformers/all-MiniLM-L6-v2` | `fastembed` `AllMiniLML6V2` (same model, 384-dim)   |
| Target           | `qdrant.CollectionTarget`                | `cocoindex::qdrant::CollectionTarget`              |
| Search           | `client.query_points(...)`               | `cocoindex::qdrant::vector_search` (cosine score)   |

The `cocoindex::qdrant` connector is a declarative two-level **managed target**
(collection → points) built on CocoIndex's public target-state facade: it
creates the collection to match the vector schema, upserts changed points, skips
unchanged ones (fingerprint tracking), deletes orphaned points, and recreates
the collection if the vector schema changes. It uses the native Rust
`qdrant-client` (gRPC).

## Build requirement: `protoc`

The `qdrant-client` crate compiles protobufs, so a `protoc` binary is required to
build this example. Install it (`brew install protobuf`, or download a release
from <https://github.com/protocolbuffers/protobuf/releases>) and put it on `PATH`
or point `PROTOC` at it.

## Run

Start Qdrant (e.g. `docker run -p 6333:6333 -p 6334:6334 qdrant/qdrant`), then:

```bash
export QDRANT_URL=http://localhost:6334   # default (gRPC)

cargo run -- index                 # walk ./markdown_files -> chunk -> embed -> Qdrant
cargo run -- query "your query"    # Qdrant cosine vector search
```
