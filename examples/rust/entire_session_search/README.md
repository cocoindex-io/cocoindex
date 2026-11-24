# Entire Session Search (Rust)

Rust port of the Python [`entire_session_search`](../../entire_session_search)
example.

[Entire](https://entire.io) captures every AI coding session you run — the full
transcript, the prompt you started from, an AI-written context summary, and
metadata like token counts and files touched — as checkpoints on disk. This
pipeline turns that folder into a [pgvector](https://github.com/pgvector/pgvector)
index you can search in plain English.

## How it works

A checkpoint folder holds four file types, and `process_file` routes on the
name: `full.jsonl` is parsed into per-turn transcript chunks, `prompt.txt` is
embedded whole, `context.md` is split into overlapping chunks, and
`metadata.json` becomes a structured row in a *second* table. The transcript and
context paths fan out to many rows concurrently (`ctx.map`); each embedding
row's `id` is derived from its text, so a turn that survives a re-parse keeps
its row.

Each checkpoint file is its own child component (`mount_each_live`), so a
finished session is never re-embedded; the embedder memoizes each text and
batches concurrent misses.

## Parallel to the Python example

| Concern          | Python                                            | Rust (this example)                                          |
| ---------------- | ------------------------------------------------- | ------------------------------------------------------------ |
| Source           | `localfs.walk_dir(..., live=True)` + `coco.mount_each` | `walk_dir(..).live()` + `ctx.mount_each_live`           |
| Per-file compute | `@coco.fn(memo=True) process_file`                | `#[cocoindex::function] process_file`, routing on the file name |
| Chunk fan-out    | `coco.map(process_chunk, ...)`                    | `ctx.map(...)`                                               |
| Chunking         | `RecursiveSplitter` (Markdown)                    | `ops::text::RecursiveSplitter` (Markdown)                    |
| Embeddings       | `SentenceTransformerEmbedder.embed`               | `SentenceTransformerEmbedder::embed(&ctx, text)`             |
| Targets          | two `postgres.mount_table_target`s                | two `postgres::mount_table_target`s                          |
| Row schema       | Dataclass + `Annotated[NDArray, EMBEDDER]`        | `#[derive(SchemaFields)]` + `#[coco(vector)]` + `with_vector_schema` |
| Query            | `python main.py "query"` (asyncpg)                | `cargo run -- query "query"` (sqlx)                          |

## Run

**1. Start Postgres + pgvector** (the repo ships a compose file):

```sh
docker compose -f ../../../dev/postgres.yaml up -d
export POSTGRES_URL=postgres://cocoindex:cocoindex@localhost/cocoindex
# optional: PG_SCHEMA_NAME (default entire), TABLE_EMBEDDINGS (session_embeddings), TABLE_METADATA (session_metadata)
```

**2. Check out some checkpoints** — from any repo where Entire is capturing
sessions:

```sh
git worktree add entire_checkpoints entire/checkpoints/v1
```

Each session is laid out as `<checkpoint_id[:2]>/<checkpoint_id[2:]>/<session_idx>/`
with `full.jsonl`, `prompt.txt`, `context.md`, and `metadata.json`.

**3. Build the index** — catch-up (scan, sync, exit) or live (catch up, then
keep watching for new sessions):

```sh
cargo run                                 # catch-up over ./entire_checkpoints
cargo run -- --live                       # live (Ctrl-C to stop)
cargo run -- index /path/to/checkpoints   # another checkpoints folder
```

**4. Search from the command line:**

```sh
cargo run -- query "how did I fix the auth bug"
```

Results print which session and content type matched, so a transcript turn, a
prompt, and a context chunk are all distinguishable. This example keeps it
minimal and doesn't declare a vector index, so queries do a sequential scan —
fine for a personal history. For a larger corpus, add
`emb_table.declare_vector_index(&ctx, "embedding", ..)` exactly as the
[`text_embedding`](../text_embedding) example does.
