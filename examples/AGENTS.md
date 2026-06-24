# AGENTS.md — CocoIndex examples

Guidance for AI coding agents (Claude Code, Cursor, etc.) working in this `examples/`
directory. Most top-level Python subfolders are self-contained, runnable
CocoIndex **v1** apps; `rust/` contains Rust ports with per-example READMEs.

## Before you write CocoIndex code: install the skill

CocoIndex v1 is a fundamental redesign from v0. Without context, LLMs tend to
hallucinate the v0 flow-builder DSL and deprecated decorators. Install the
official skill first — it teaches the correct v1 API. Quickest path is the
hosted single file:

```sh
mkdir -p .claude/skills/cocoindex/references
curl -fsSL https://cocoindex.io/docs/skill.md -o .claude/skills/cocoindex/SKILL.md
for f in api_reference connectors patterns setup_database setup_project; do
  curl -fsSL https://cocoindex.io/docs/references/$f.md -o .claude/skills/cocoindex/references/$f.md
done
```

Or clone the repo and copy the folder:

```sh
git clone --depth=1 https://github.com/cocoindex-io/cocoindex.git /tmp/cocoindex-skill
mkdir -p .claude/skills && cp -r /tmp/cocoindex-skill/skills/cocoindex .claude/skills/
```

The skill itself lives at [`skills/cocoindex/`](../skills/cocoindex) (SKILL.md +
`references/`). For Cursor, copy `SKILL.md` into `.cursor/rules/`. Full machine-readable
docs: <https://cocoindex.io/docs/llms.txt> (index) and
<https://cocoindex.io/docs/llms-full.txt> (everything, including example
walkthroughs). Regular docs pages also have raw Markdown twins by replacing the
trailing slash with `.md` (e.g.
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

When an example needs credentials or service configuration, required env vars
are templated in that example's **`.env.example`** — `cp` it to `.env` and fill
in the blanks; both `python main.py` and the `cocoindex` CLI load `.env`
automatically. Common ones:

- `POSTGRES_URL` — for Postgres/pgvector targets. Local instance:
  `docker compose -f ../../dev/postgres.yaml up -d` from inside an example
  directory.
- `OPENAI_API_KEY` / `GEMINI_API_KEY` — for examples that call an LLM.
- Service-specific (`QDRANT_URL`, `LANCEDB_URI`, `NEO4J_*`, `KAFKA_*`,
  `GOOGLE_SERVICE_ACCOUNT_CREDENTIAL`, …) — see that example's `.env.example`.

Examples with no `.env.example` (e.g. `files_transform`, `pdf_to_markdown`) run
fully locally with no credentials.

**Never commit secrets.** The `.env` files tracked in this repo hold only
non-secret defaults (`COCOINDEX_DB`, local service URLs); keep API keys and
credentials in your local `.env` edits and out of commits.

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
- `sec_edgar_analytics` — multi-format SEC filings → Apache Doris with a vector **and** a full-text index; hybrid (semantic + keyword) RRF search. *(walkthrough: sec-edgar-analytics)*

### Multimodal
- `image_search` — CLIP embeddings + Qdrant, queried via FastAPI + React.
- `image_search_colpali` — ColPali multi-vector model + Qdrant MaxSim.
- `multi_format_indexing` — PDFs + images as page screenshots → ColPali → Qdrant; no OCR, no chunking. *(walkthrough: multi-format-indexing)*
- `face_recognition` — detect faces (dlib) → 128-d embeddings → Qdrant face search. *(walkthrough: face-recognition)*
- `audio_to_text` — transcribe audio with LiteLLM → Postgres.
- `slides_to_speech` — slides → vision-LLM notes → Piper TTS narration → LanceDB semantic search. *(walkthrough: slides-to-speech)*

### Structured extraction (LLM / BAML / DSPy)
- `multi_codebase_summarization` — LLM per-file summaries across many repos. *(walkthrough: multi-codebase-summarization)*
- `hn_trending_topics` — scrape HackerNews → LLM topic extraction → Postgres.
- `manuals_llm_extraction` — PDF manuals → Markdown (docling) → typed module records → Postgres. *(walkthrough: manuals-llm-extraction)*
- `patient_intake_extraction_baml` / `patient_intake_extraction_dspy` — structured PDF extraction with BAML / DSPy (Gemini vision).

### Knowledge graphs
- `conversation_to_knowledge` — YouTube podcasts → SurrealDB knowledge graph. *(walkthrough: podcast-to-knowledge-graph)*
- `docs_to_knowledge_graph` — Markdown docs → Neo4j concept graph of LLM-extracted triples. *(walkthrough: docs-to-knowledge-graph)*
- `product_recommendation` — product catalog → LLM taxonomy extraction → Neo4j recommendation graph. *(walkthrough: product-recommendation)*
- `meeting_notes_graph_neo4j` / `meeting_notes_graph_falkordb` — Google Drive meeting notes → Neo4j / FalkorDB graph.

### Custom sources / targets / streaming
- `pdf_to_markdown` — incremental PDF → Markdown with docling (local, no services). *(walkthrough: pdf-to-markdown)*
- `files_transform` — watch Markdown files → HTML, live mode (local, no services).
- `csv_to_kafka` — watch CSVs → publish rows to Kafka.
- `kafka_to_lancedb` — consume Kafka → route to LanceDB tables.

### Rust
- `rust/` — Rust ports of many of the above, using the CocoIndex Rust API.

## Conventions for edits

- Keep each Python example self-contained: its own `pyproject.toml` and
  `README.md`; add `.env.example` when credentials or configurable services are
  required. When you add an example, also add a line to `EXAMPLE_CATALOG` in the
  docs repo (`docs/src/data/examples.ts`) so it appears in `/docs/llms.txt`.
- Match the surrounding code's low comment density.
- Don't commit generated artifacts (`cocoindex.db`, `__pycache__`, build output) —
  they're already git-ignored.
