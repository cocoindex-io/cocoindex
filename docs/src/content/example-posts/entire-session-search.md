---
title: Search Your *AI Coding Sessions*
description: 'Build a semantic search index over AI coding sessions captured by Entire with CocoIndex V1 — read transcripts, prompts, and context summaries, embed them with sentence-transformers, and store the vectors in Postgres with pgvector, then search every session in natural language. Plain async Python.'
slug: entire-session-search
image: https://cocoindex.io/blobs/docs-v1/img/examples/entire-session-search/cover.png
tags: [vector-index, ai-sessions]
---

![Search your AI coding sessions with CocoIndex V1](https://cocoindex.io/blobs/docs-v1/img/examples/entire-session-search/cover.png)

[Entire](https://entire.io) captures every AI coding session you run — the full conversation transcript, the prompt you started from, an AI-written context summary, and metadata like token counts and files touched — as checkpoints on disk. We'll take that folder of checkpoints and turn it into a [vector index](https://github.com/pgvector/pgvector) you can search in plain English: "how did I fix the auth bug" finds the right session even when it shares no keywords with what you typed.

The whole pipeline is ordinary `async` Python and your own types. The heavy lifting — [incremental processing](https://cocoindex.io/docs/programming_guide/core_concepts/), change tracking, managed targets — runs in a Rust engine underneath, so each new session you capture only embeds what changed, and every kind of checkpoint file is parsed by the same `process_file` component.

[→ View on GitHub](https://github.com/cocoindex-io/cocoindex/tree/main/examples/entire_session_search)

## Flow overview

![CocoIndex Entire session search flow: walk a folder of checkpoints, route each file by name through process_file, embed transcripts, prompts, and context summaries, and store the vectors in Postgres with pgvector alongside a metadata table](https://cocoindex.io/blobs/docs-v1/img/examples/entire-session-search/flow-v1.png)

From a high level, these are the steps:

1. Read Entire checkpoint files from a local directory (live).
2. Route each file by name: parse `full.jsonl` into per-turn transcript chunks, take `prompt.txt` whole, [split](https://cocoindex.io/docs/ops/text/) `context.md` into overlapping chunks, then [embed](https://cocoindex.io/docs/ops/sentence_transformers/) the text — while `metadata.json` becomes a structured row.
3. Store the embeddings and metadata in two Postgres tables (as [target states](https://cocoindex.io/docs/programming_guide/target_state/)).

You [declare the transformation logic](https://cocoindex.io/docs/programming_guide/core_concepts/) with native Python, without worrying about how updates propagate. Think: **target_state = transformation(source_state)**.

> **New to embeddings?** An [*embedding*](https://cocoindex.io/docs/ops/sentence_transformers/) is a list of numbers (a vector) that captures the *meaning* of a piece of text, so passages with similar meaning land close together in vector space. A [*vector index*](https://cocoindex.io/docs/common_resources/vector_schema/) stores those vectors and finds the nearest ones to your query fast. That's what lets search match by meaning instead of exact words.

## Setup

- A running Postgres with the [pgvector](https://github.com/pgvector/pgvector) extension. The repo ships a compose file:

  ```sh
  docker compose -f dev/postgres.yaml up -d
  export POSTGRES_URL="postgres://cocoindex:cocoindex@localhost/cocoindex"
  ```

- Install CocoIndex and the dependencies this example uses:

  ```sh
  pip install -U "cocoindex[postgres,sentence_transformers]" asyncpg pgvector numpy python-dotenv
  ```

- Some Entire checkpoints to index. From any repo where [Entire](https://entire.io) is capturing sessions, check the checkpoint data out next to the example:

  ```sh
  git worktree add entire_checkpoints entire/checkpoints/v1
  ```

  Each session is laid out as `<checkpoint_id[:2]>/<checkpoint_id[2:]>/<session_idx>/` with `full.jsonl` (transcript), `prompt.txt` (initial prompt), `context.md` (AI-written summary), and `metadata.json` (token counts, files touched).

## Define the data and shared resources

Each row of the embeddings table is one searchable piece of text — a transcript turn, a prompt, or a context chunk — tagged with its `content_type`, `role`, and the session it came from. The metadata table keeps one row per session for the structured fields. `coco_lifespan` provides the [shared resources](https://cocoindex.io/docs/programming_guide/context/) every step needs — the Postgres connection pool and the embedding model — once at startup.

```python title="main.py"
EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
PG_DB = coco.ContextKey[asyncpg.Pool]("entire_session_db")
EMBEDDER = coco.ContextKey[SentenceTransformerEmbedder]("embedder", detect_change=True)

_splitter = RecursiveSplitter()


@dataclass
class SessionEmbeddingRow:
    id: int
    checkpoint_id: str
    session_index: str
    content_type: str  # "transcript", "prompt", or "context"
    role: str  # "user", "assistant", or "" for non-transcript
    text: str
    embedding: Annotated[NDArray, EMBEDDER]


@dataclass
class SessionMetadataRow:
    checkpoint_id: str
    session_index: str
    prompt_summary: str
    total_tokens: int
    files_touched: str  # JSON array
    agent_percentage: float | None


@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    async with asyncpg.create_pool(DATABASE_URL) as pool:
        builder.provide(PG_DB, pool)
        builder.provide(EMBEDDER, SentenceTransformerEmbedder(EMBED_MODEL))
        yield
```

`embedding: Annotated[NDArray, EMBEDDER]` ties the vector column to the embedder, so its dimensions are inferred automatically — and if you swap the model later, CocoIndex notices (`detect_change=True`) and re-embeds.

## Process a file

![One processing component per checkpoint file: process_file routes by filename, embeds transcript, prompt, and context text into the embeddings table, and writes one metadata row](https://cocoindex.io/blobs/docs-v1/img/examples/entire-session-search/stage-file-process.png)

`process_file` runs once per checkpoint file and routes on its name. The checkpoint id and session index come straight from the file's path, and a fresh `IdGenerator` numbers the rows this file produces.

```python title="main.py"
@coco.fn(memo=True)
async def process_file(
    file: FileLike,
    emb_table: postgres.TableTarget[SessionEmbeddingRow],
    meta_table: postgres.TableTarget[SessionMetadataRow],
) -> None:
    info = extract_session_info(file)
    filename = file.file_path.path.name
    id_gen = IdGenerator()

    if filename == "full.jsonl":
        content = await file.read_text()
        chunks = parse_transcript(content)
        await coco.map(
            process_chunk,
            [
                ChunkInput(text=c.text, content_type="transcript", role=c.role)
                for c in chunks
            ],
            info, id_gen, emb_table,
        )

    elif filename == "prompt.txt":
        text = (await file.read_text()).strip()
        if text:
            emb_table.declare_row(
                row=SessionEmbeddingRow(
                    id=await id_gen.next_id(text),
                    checkpoint_id=info.checkpoint_id,
                    session_index=info.session_index,
                    content_type="prompt",
                    role="user",
                    text=text,
                    embedding=await coco.use_context(EMBEDDER).embed(text),
                ),
            )

    elif filename == "context.md":
        text = (await file.read_text()).strip()
        if text:
            chunks = _splitter.split(
                text, chunk_size=2000, chunk_overlap=500, language="markdown"
            )
            await coco.map(
                process_chunk,
                [
                    ChunkInput(text=c.text, content_type="context", role="")
                    for c in chunks
                ],
                info, id_gen, emb_table,
            )

    elif filename == "metadata.json":
        meta = json.loads(await file.read_text())
        usage = meta.get("token_usage", {})
        agent_pct = meta.get("initial_attribution", {}).get("agent_percentage")
        meta_table.declare_row(
            row=SessionMetadataRow(
                checkpoint_id=info.checkpoint_id,
                session_index=info.session_index,
                prompt_summary=meta.get("summary", {}).get("intent", ""),
                total_tokens=(usage.get("input_tokens") or 0) + (usage.get("output_tokens") or 0),
                files_touched=json.dumps(meta.get("files_touched", [])),
                agent_percentage=float(agent_pct) if agent_pct is not None else None,
            ),
        )
```

The transcript and the context summary each fan out to many rows, so they map to `process_chunk`; the prompt is a single short string, so it's embedded inline; and the metadata file declares one row directly into the *other* table — three content types and a structured record, all from one component.

[`@coco.fn`](https://cocoindex.io/docs/programming_guide/function/) with [`memo=True`](https://cocoindex.io/docs/advanced_topics/memoization_keys/) is what makes this incremental: if a file's content and this function's code are both unchanged, it's skipped on the next run, so finished sessions are never re-embedded. `coco.map` fans out to one `process_chunk` call per chunk.

## Process a chunk

`process_chunk` embeds one piece of text with the shared embedder and declares the target row. Both the transcript and the context paths funnel through it, carrying their own `content_type` and `role`.

```python title="main.py"
@coco.fn
async def process_chunk(
    chunk: ChunkInput,
    info: SessionInfo,
    id_gen: IdGenerator,
    emb_table: postgres.TableTarget[SessionEmbeddingRow],
) -> None:
    emb_table.declare_row(
        row=SessionEmbeddingRow(
            id=await id_gen.next_id(chunk.text),
            checkpoint_id=info.checkpoint_id,
            session_index=info.session_index,
            content_type=chunk.content_type,
            role=chunk.role,
            text=chunk.text,
            embedding=await coco.use_context(EMBEDDER).embed(chunk.text),
        ),
    )
```

We use [`SentenceTransformerEmbedder`](https://cocoindex.io/docs/ops/sentence_transformers/) with `all-MiniLM-L6-v2` — a small, fast model that runs locally with no API key. There are 12k+ sentence-transformer models on [Hugging Face](https://huggingface.co/models?other=sentence-transformers), so swap in whichever you prefer. `emb_table.declare_row` declares the row as a target state; CocoIndex handles inserting, updating, or deleting it to match. Each row's `id` is derived from the chunk text, so a turn that survives a re-parse keeps its row.

## Define the main function

![mount_each fans out one process_file component per checkpoint file, from the Entire filesystem source to the two Postgres tables](https://cocoindex.io/blobs/docs-v1/img/examples/entire-session-search/stage-main-function.png)

`app_main` wires the source to the targets. It mounts both Postgres tables, walks the checkpoint directory for the four file types, and mounts one [processing component](https://cocoindex.io/docs/programming_guide/processing_component/) per file.

```python title="main.py"
@coco.fn
async def app_main(checkpoints_dir: pathlib.Path) -> None:
    emb_table = await postgres.mount_table_target(
        PG_DB,
        table_name=TABLE_EMBEDDINGS,
        table_schema=await postgres.TableSchema.from_class(
            SessionEmbeddingRow, primary_key=["id"],
        ),
        pg_schema_name=PG_SCHEMA_NAME,   # "entire"
    )

    meta_table = await postgres.mount_table_target(
        PG_DB,
        table_name=TABLE_METADATA,
        table_schema=await postgres.TableSchema.from_class(
            SessionMetadataRow, primary_key=["checkpoint_id", "session_index"],
        ),
        pg_schema_name=PG_SCHEMA_NAME,
    )

    files = localfs.walk_dir(
        checkpoints_dir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(
            included_patterns=[
                "**/full.jsonl", "**/prompt.txt",
                "**/context.md", "**/metadata.json",
            ],
        ),
        live=True,  # watch for changes; pass -L to `cocoindex update` to run live
    )
    await coco.mount_each(process_file, files.items(), emb_table, meta_table)


app = coco.App(
    coco.AppConfig(name="EntireSessionSearch"),
    app_main,
    checkpoints_dir=pathlib.Path("./entire_checkpoints"),
)
```

`mount_table_target` creates and manages each Postgres table for you — schema, idempotent upserts, and orphan cleanup when a session disappears. The `included_patterns` are what makes one component handle four different files: every match flows through the same `process_file`, which routes on the name. `live=True` makes the [filesystem source](https://cocoindex.io/docs/connectors/localfs/) [watch for changes](https://cocoindex.io/docs/programming_guide/live_mode/), and `mount_each` runs one component per file so the engine can track and update them independently.

> **No vector index here.** To keep the example minimal, this flow doesn't declare a vector index, so queries do a sequential scan — fine for a personal session history. For a larger corpus, add one line — `emb_table.declare_vector_index(column="embedding")` — exactly as the [Semantic Search 101](https://cocoindex.io/docs/examples/text-embedding/) example does, and pgvector serves approximate-nearest-neighbor queries instead.

## Run the pipeline

Run the [`cocoindex` CLI](https://cocoindex.io/docs/cli/) to build and update the index. Choose catch-up (scan, sync, exit) or live (catch up, then keep watching):

```sh
# Catch-up run
cocoindex update main

# Live run: keep watching for new sessions
cocoindex update -L main
```

## Query the index

Match user text against the index with a plain SQL query, reusing the *same* embedder from the indexing flow so indexing and querying stay consistent.

```python title="main.py"
async def query_once(pool, embedder, query: str, *, top_k: int = 5) -> None:
    query_vec = await embedder.embed(query)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT checkpoint_id, session_index, content_type, role, text,
                   embedding <=> $1 AS distance
            FROM "{PG_SCHEMA_NAME}"."{TABLE_EMBEDDINGS}"
            ORDER BY distance ASC
            LIMIT $2
            """,
            query_vec, top_k,
        )
    for r in rows:
        score = 1.0 - float(r["distance"])
        tag = r["content_type"] + (f"/{r['role']}" if r["role"] else "")
        print(f"[{score:.3f}] {r['checkpoint_id']}/{r['session_index']} ({tag})")
        print(f"    {r['text'][:200]}")
        print("---")
```

The `<=>` operator is pgvector's cosine distance. We turn it into a similarity score and print which session and content type matched, so a transcript turn, a prompt, and a context chunk are all distinguishable in the results. Run a search straight from the command line:

```bash
python main.py "how did I fix the auth bug"
```

The most semantically similar sessions come back ranked — even when they share none of the words in your query. That's the whole point of a vector index.

## Incremental updates

CocoIndex keeps the index in sync with your sessions and does the **minimum work** to get there. You never compute a diff or write update logic. Two pieces make this work. `@coco.fn(memo=True)` decides what to *recompute* — a file is skipped when its content and the function's code are both unchanged, so a finished session is never re-embedded. `mount_table_target` decides what to *write* — each embedding row's [`id`](https://cocoindex.io/docs/common_resources/id_generation/) is derived from its text, so it upserts only the rows that actually changed and deletes rows whose source is gone.

- **A new session is captured** — only its files are parsed, chunked, and embedded; their rows are inserted. Everything already indexed is untouched.
- **A session is updated** — its files are re-routed and re-chunked; turns whose text is unchanged keep their `id` and embedding, genuinely new turns are embedded and inserted, and turns that no longer exist are deleted.
- **A session is removed** — its embedding and metadata rows are removed from both tables automatically.

The same machinery covers **logic** changes too: tune the chunk size or swap the embedding model, and CocoIndex compares the new output against what's already in Postgres and applies only the difference. A catch-up run (`cocoindex update main`) does this once and exits; live mode (`cocoindex update -L main`) keeps watching and applies each new session with low latency.

## Run it

The full, runnable example is in the CocoIndex repo: [examples/entire_session_search](https://github.com/cocoindex-io/cocoindex/tree/main/examples/entire_session_search). If your inputs are plain text or Markdown rather than session checkpoints, [Semantic Search 101](https://cocoindex.io/docs/examples/text-embedding/) is the same flow without the per-file routing; to search a folder of PDFs, see [Semantic Search over PDFs](https://cocoindex.io/docs/examples/pdf-embedding/).

Want to search your own AI coding history by meaning? Come tell us on [Discord](https://discord.com/invite/zpA9S2DR7s) — and if this was useful, [star CocoIndex on GitHub](https://github.com/cocoindex-io/cocoindex).
