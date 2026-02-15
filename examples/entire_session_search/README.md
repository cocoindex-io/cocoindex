# Entire Session Search

Semantic search over AI coding sessions captured by [Entire](https://entire.io), powered by CocoIndex.

## What it does

- Reads Entire checkpoint data (transcripts, prompts, context summaries, metadata)
- Chunks and embeds text with SentenceTransformers, stores in Postgres with pgvector
- Provides cosine-similarity search across all your AI coding sessions

Because CocoIndex is incremental, re-running after new sessions only processes what changed.

## Prerequisites

- Postgres with pgvector extension
- [Entire](https://entire.io) installed with some captured sessions
- Python 3.11+

## Setup

**1. Check out the Entire checkpoint data:**

```sh
# From any repo where Entire is capturing sessions
git worktree add entire_checkpoints entire/checkpoints/v1
```

**2. Install deps:**

```sh
pip install -e .
```

**3. Set env vars (or edit `.env`):**

```sh
# .env
COCOINDEX_DB=postgres://cocoindex:cocoindex@localhost/cocoindex
POSTGRES_URL=postgres://cocoindex:cocoindex@localhost/cocoindex
```

## Run

Build the index:

```sh
cocoindex update main.py
```

Search your sessions:

```sh
python main.py query "how did I fix the auth bug"
```

Or start an interactive search:

```sh
python main.py query
```

## Configuration

| Variable | Default | Description |
|---|---|---|
| `COCOINDEX_DB` | (required) | Postgres URL for CocoIndex internal state |
| `POSTGRES_URL` | `postgres://cocoindex:cocoindex@localhost/cocoindex` | Postgres connection for embedding/metadata tables |
| `TABLE_EMBEDDINGS` | `session_embeddings` | Embeddings table name |
| `TABLE_METADATA` | `session_metadata` | Metadata table name |
| `PG_SCHEMA_NAME` | `entire` | Postgres schema |

## How it works

```mermaid
graph TD
    checkpoints[Entire Checkpoints] --> walk[walk_dir]
    walk --> mount_each[mount_each]
    mount_each --> process_file[<b>process_file</b>]
    process_file -->|full.jsonl| parse[parse_transcript]
    process_file -->|prompt.txt| embed_prompt[embed directly]
    process_file -->|context.md| split[RecursiveSplitter]
    process_file -->|metadata.json| meta_table[(session_metadata)]
    parse --> embed[SentenceTransformer]
    embed_prompt --> embed
    split --> embed
    embed --> emb_table[(session_embeddings)]
```

## Entire checkpoint layout

```
<shard>/<checkpoint_id>/<session_idx>/
  ├── metadata.json     # token counts, files touched, timestamps
  ├── full.jsonl        # conversation transcript
  ├── prompt.txt        # user's initial prompt
  ├── context.md        # AI-generated session summary
  └── content_hash.txt  # content fingerprint (skipped)
```
