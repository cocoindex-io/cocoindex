# Paper Metadata (Rust)

Rust port of the Python [`paper_metadata`](../../paper_metadata) example.

Walks local PDFs, extracts first-page text + page count, LLM-extracts
title/authors/abstract, embeds the title and abstract chunks, and stores
everything across three Postgres tables — then serves similarity search.

## Parallel to the Python example

| Concern          | Python                                       | Rust (this example)                                       |
| ---------------- | -------------------------------------------- | --------------------------------------------------------- |
| Source           | `localfs.walk_dir` (`**/*.pdf`, live)        | `cocoindex::fs::walk` (`**/*.pdf`)                        |
| Per-file compute | `@coco.fn(memo=True) process_file`           | `#[cocoindex::function(memo)] process_file`               |
| PDF parsing      | `pypdf` (first-page text + page count)       | `lopdf` (first-page text + page count)                    |
| LLM extraction   | `openai` chat completions (`gpt-4o`, JSON)   | OpenAI chat completions REST (`gpt-4o`, JSON mode)        |
| Chunking         | `RecursiveSplitter` + custom "abstract" lang | `RecursiveChunker` + `CustomLanguageConfig` ("abstract")  |
| Embeddings       | `sentence-transformers/all-MiniLM-L6-v2`     | `fastembed` `AllMiniLML6V2` (same model, 384-dim)         |
| Targets          | 3× `postgres.mount_table_target`             | 3× `postgres::mount_table_target`                         |

Tables (schema `coco_examples_v1`):

- `paper_metadata` — `filename` (pk), `title`, `authors` (jsonb), `abstract`, `num_pages`
- `author_papers` — (`author_name`, `filename`) pk
- `metadata_embeddings` — `id` (uuid pk), `filename`, `location`, `text`, `embedding vector(384)`

Incrementality: unchanged PDFs are memo-skipped; rows of a removed/edited PDF
are reconciled away (the managed `TableTarget`s delete orphaned rows).

**Deviation from Python:** embedding-row UUIDs are derived deterministically
from `(filename, location, text)` via the SDK's `UuidGenerator` (vs Python's
`uuid.uuid4()`), so re-runs are stable. Like Python, no pgvector index is
created — the query demo does a sequential cosine scan.

## Run

```bash
export POSTGRES_URL=postgres://cocoindex:cocoindex@localhost/cocoindex   # pgvector-enabled
export OPENAI_API_KEY=...

cargo run -- index                       # walk ./papers -> extract -> embed -> Postgres
cargo run -- query "attention mechanism" # cosine similarity search
```
