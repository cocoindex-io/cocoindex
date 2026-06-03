# PDF Embedding (Rust)

Rust port of the Python [`pdf_embedding`](../../pdf_embedding) example.

Walks local PDFs, extracts their text, chunks it, embeds the chunks, and stores
them in Postgres/pgvector — then serves similarity search.

## Parallel to the Python example

| Concern          | Python                                   | Rust (this example)                                |
| ---------------- | ---------------------------------------- | -------------------------------------------------- |
| Source           | `localfs.walk_dir` (`**/*.pdf`)          | `cocoindex::fs::walk` (`**/*.pdf`)                 |
| PDF → text       | `docling` (PDF → Markdown, ML pipeline)  | `lopdf` text extraction                            |
| Per-file compute | `@coco.fn(memo=True) process_file`       | `#[cocoindex::function(memo)] process_file`         |
| Chunking         | `RecursiveSplitter` (markdown, 2000/500) | `cocoindex_ops_text` `RecursiveChunker` (markdown, 2000/500) |
| Embeddings       | `sentence-transformers/all-MiniLM-L6-v2` | `fastembed` `AllMiniLML6V2` (same model, 384-dim)   |
| Target           | `postgres.mount_table_target`            | `postgres::mount_table_target`                      |

**Deviation from Python:** Python converts PDFs to Markdown with `docling` (a
heavy ML document-understanding pipeline) and runs it on a `coco.GPU` runner.
There is no Rust equivalent, so this port extracts plain text with `lopdf` (the
same Rust-native PDF approach as `paper-metadata`). Extraction quality varies by
PDF, but everything downstream — chunking, embeddings, target, query — mirrors
Python. Like Python, no pgvector index is created (sequential cosine scan).

Target table is `coco_examples.pdf_embeddings` (`id` pk, `filename`,
`chunk_start`, `chunk_end`, `text`, `embedding vector(384)`).

## Run

```bash
export POSTGRES_URL=postgres://cocoindex:cocoindex@localhost/cocoindex   # pgvector-enabled

cargo run -- index                 # walk ./pdf_files -> extract -> chunk -> embed -> Postgres
cargo run -- query "your query"    # cosine similarity search
```
