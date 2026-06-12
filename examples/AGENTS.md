# AGENTS.md — CocoIndex examples

Guidance for AI coding agents (Claude Code, Cursor, etc.) working in this `examples/`
directory. Each subfolder is a self-contained, runnable CocoIndex **v1** app.

## Before you write CocoIndex code: install the skill

CocoIndex v1 is a fundamental redesign from v0. Without context, LLMs tend to
hallucinate the v0 flow-builder DSL and deprecated decorators. Install the
official skill first — it teaches the correct v1 API:

```sh
mkdir -p .claude/skills
git clone --depth=1 https://github.com/cocoindex-io/cocoindex.git /tmp/cocoindex-skill
cp -r /tmp/cocoindex-skill/skills/cocoindex .claude/skills/
```

The skill itself lives at [`skills/cocoindex/`](../skills/cocoindex) (SKILL.md +
`references/`). For Cursor, copy `SKILL.md` into `.cursor/rules/`. Full machine-readable
docs: <https://cocoindex.io/docs/llms.txt> (index) and
<https://cocoindex.io/docs/llms-full.txt> (everything). Raw Markdown is available
by replacing a docs page's trailing slash with `.md` (e.g.
`https://cocoindex.io/docs/programming_guide/core_concepts.md`).

## The v1 mental model

`target_state = transform(source_state)`. You declare what the target should look
like; the Rust engine keeps it in sync, reprocessing only what changed (state is
tracked in a local LMDB store — **no database is required for the engine itself**,
only when an example writes to one). Key APIs: `@coco.fn`, `mount` / `use_mount` /
`mount_each`, `ContextKey`, target-state declarations. See the skill for details.

## Running examples

Most Python examples are standalone projects with their own `pyproject.toml`:

```sh
cd <example_dir>
cp .env.example .env          # if present — fill in the blanks (see below)
pip install -e .              # or: uv pip install -e .
cocoindex update main         # catch-up: scan sources, sync, exit
cocoindex update -L main      # live mode: catch up, then watch for changes (where supported)
```

Use the example's README as the source of truth. Known exceptions:

- `multi_codebase_summarization`, `audio_to_text`,
  `patient_intake_extraction_baml`, `patient_intake_extraction_dspy`:
  `cocoindex update main.py`
- `conversation_to_knowledge`: `cocoindex update conv_knowledge.app`
- `image_search`, `image_search_colpali`: start the API with
  `python -m uvicorn api:app --reload --host 0.0.0.0 --port 8000`; it runs the
  CocoIndex app in live mode, then start the frontend from `frontend/`.
- `csv_to_kafka`, `kafka_to_lancedb`: `cocoindex update -L main.py`
- `rust/<example>`: follow that example's README. Many use
  `cargo run -- index` for indexing and `cargo run -- query "..."` for search;
  a few take custom paths or service-specific subcommands.

Some examples expose a query/CLI demo via `python main.py "<query>"`; check the
example's `README.md`. Examples that need extra services or a code-gen step
(e.g. `baml generate`) say so in their README.

## Environment / credentials

Required env vars are templated in each example's **`.env.example`** — `cp` it to
`.env` and fill in the blanks; both `python main.py` and the `cocoindex` CLI load
`.env` automatically. Common ones:

- `POSTGRES_URL` — for Postgres/pgvector targets. Local instance:
  `docker compose -f ../../dev/postgres.yaml up -d` from inside an example
  directory.
- `OPENAI_API_KEY` / `GEMINI_API_KEY` — for examples that call an LLM.
- Service-specific (`QDRANT_URL`, `LANCEDB_URI`, `NEO4J_*`, `KAFKA_*`,
  `GOOGLE_SERVICE_ACCOUNT_CREDENTIAL`, …) — see that example's `.env.example`.

Examples with no `.env.example` (e.g. `files_transform`, `pdf_to_markdown`) run
fully locally with no credentials.

**Never commit a real `.env`** — only the `.env.example` template.

## The examples

A walkthrough URL means there's a step-by-step guide at
`https://cocoindex.io/docs/examples/<slug>/`; otherwise start from the example's README.

### Vector indexes (embed → store → search by meaning)
- `text_embedding` — Markdown → pgvector; the simplest end-to-end index. *(walkthrough: text-embedding)*
- `code_embedding` — repo → Tree-sitter chunks → pgvector; query code in English. *(walkthrough: index-codebase)*
- `text_embedding_qdrant` / `text_embedding_lancedb` / `text_embedding_turbopuffer` — same flow, different vector store.
- `code_embedding_lancedb` — code chunks → LanceDB.
- `pdf_embedding` — PDFs → markdown → chunks → pgvector.
- `paper_metadata` — extract title/authors/abstract from PDFs → Postgres + embeddings.
- `amazon_s3_embedding` / `gdrive_text_embedding` / `oci_object_storage_embedding` — same flow, remote source (S3 / Google Drive / OCI).
- `postgres_source` — read from an existing Postgres table as the source.
- `entire_session_search` — semantic search over AI coding sessions captured by Entire.

### Multimodal
- `image_search` — CLIP embeddings + Qdrant, queried via FastAPI + React.
- `image_search_colpali` — ColPali multi-vector model + Qdrant MaxSim.
- `audio_to_text` — transcribe audio with LiteLLM → Postgres.

### Structured extraction (LLM / BAML / DSPy)
- `multi_codebase_summarization` — LLM per-file summaries across many repos. *(walkthrough: multi-codebase-summarization)*
- `hn_trending_topics` — scrape HackerNews → LLM topic extraction → Postgres.
- `patient_intake_extraction_baml` / `patient_intake_extraction_dspy` — structured PDF extraction with BAML / DSPy (Gemini vision).

### Knowledge graphs
- `conversation_to_knowledge` — YouTube podcasts → SurrealDB knowledge graph. *(walkthrough: podcast-to-knowledge-graph)*
- `meeting_notes_graph_neo4j` / `meeting_notes_graph_falkordb` — Google Drive meeting notes → Neo4j / FalkorDB graph.

### Custom sources / targets / streaming
- `pdf_to_markdown` — incremental PDF → Markdown with docling (local, no services). *(walkthrough: pdf-to-markdown)*
- `files_transform` — watch Markdown files → HTML, live mode (local, no services).
- `csv_to_kafka` — watch CSVs → publish rows to Kafka.
- `kafka_to_lancedb` — consume Kafka → route to LanceDB tables.

### Rust
- `rust/` — Rust ports of many of the above, using the CocoIndex Rust API.

## Conventions for edits

- Keep each example self-contained: its own `pyproject.toml`, `README.md`, and
  `.env.example`. When you add an example, add all three, and add a line to
  `EXAMPLE_CATALOG` in the docs repo (`docs/src/data/examples.ts`) so it appears
  in `/docs/llms.txt`.
- Match the surrounding code's low comment density.
- Don't commit generated artifacts (`cocoindex.db`, `__pycache__`, build output) —
  they're already git-ignored.
