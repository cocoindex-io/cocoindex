# Text Embedding with Turbopuffer (Rust)

Rust port of the Python [`text_embedding_turbopuffer`](../../text_embedding_turbopuffer)
example. Same pipeline as [`text-embedding`](../text-embedding), but the vector
store is **Turbopuffer** (a hosted vector database) via the native
`cocoindex::turbopuffer` namespace target.

## Parallel to the Python example

| Concern          | Python                                   | Rust (this example)                                |
| ---------------- | ---------------------------------------- | -------------------------------------------------- |
| Source           | `localfs.walk_dir`                       | `cocoindex::fs::walk`                              |
| Chunking         | `RecursiveSplitter` (markdown)           | `cocoindex_ops_text` `RecursiveChunker` (markdown)  |
| Embeddings       | `sentence-transformers/all-MiniLM-L6-v2` | `fastembed` `AllMiniLML6V2` (same model, 384-dim)   |
| Target           | `turbopuffer.NamespaceTarget`            | `cocoindex::turbopuffer::NamespaceTarget`          |
| Search           | `ns.query(rank_by=("vector","ANN",...))` | `cocoindex::turbopuffer::vector_search`            |

The `cocoindex::turbopuffer` connector is a declarative two-level **managed
target** (namespace → rows) built on CocoIndex's public target-state facade: it
upserts changed rows, skips unchanged ones (fingerprint tracking), deletes
orphaned rows, and clears the namespace if the vector schema changes. Turbopuffer
has no official Rust client, so this talks to its v2 HTTP API via `reqwest`.

## Run

Turbopuffer is a hosted service — set your API key (and region):

```bash
export TURBOPUFFER_API_KEY=...
export TURBOPUFFER_REGION=gcp-us-central1     # default
export TURBOPUFFER_NAMESPACE=TextEmbedding    # default

cargo run -- index                 # walk ./markdown_files -> chunk -> embed -> Turbopuffer
cargo run -- query "your query"    # Turbopuffer vector search (cosine score = 1 - distance)
```
