# Text Embedding (Rust)

Rust port of the Python [`text_embedding`](../../text_embedding) example.

Walks local markdown files, chunks each file (markdown-aware), embeds the chunks,
and stores them in Postgres/pgvector — then serves similarity search.

## Parallel to the Python example

| Concern          | Python                                   | Rust (this example)                                      |
| ---------------- | ---------------------------------------- | -------------------------------------------------------- |
| Source           | `localfs.walk_dir`                       | `resources::fs::walk_items`                              |
| Per-file compute | `coco.mount_each(process_file, ...)`     | `mount_each!(files, \|file\| process_file(ctx, file))`   |
| Chunking         | `RecursiveSplitter` (Markdown)           | `ops::text::RecursiveSplitter` (Markdown)                |
| Embeddings       | `SentenceTransformerEmbedder.embed`      | `SentenceTransformerEmbedder::embed(&ctx, text)`         |
| Target           | `postgres.TableTarget` + pgvector index  | `postgres::TableTarget` + `declare_vector_index`          |
| Row schema       | Dataclass annotations                    | `#[derive(SchemaFields)]` + runtime model dimension       |

Incrementality: `mount_each!` skips unchanged file components before their
function runs. The embedder memoizes individual texts and automatically groups
concurrent cache misses into batches of up to 64. Chunks from a removed or
edited file are reconciled away by the managed `TableTarget`.

## Run

```bash
export POSTGRES_URL=postgres://cocoindex:cocoindex@localhost/cocoindex   # pgvector-enabled

cargo run -- index                 # walk ./markdown_files -> chunk -> embed -> pgvector
cargo run -- query "your query"    # cosine similarity search
```
