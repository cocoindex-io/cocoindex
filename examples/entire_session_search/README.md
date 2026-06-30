<p align="center">
  <a href="https://cocoindex.io/docs/examples/entire-session-search/" title="Semantic search over AI coding sessions captured by Entire with CocoIndex — embed transcripts, prompts, and context into Postgres pgvector, incrementally, in plain async Python">
    <img src="https://cocoindex.io/blobs/docs-v1/img/examples/entire-session-search/cover.svg" alt="Search your AI coding sessions with CocoIndex — walk a folder of Entire checkpoints, route each file by name, embed transcripts, prompts, and context summaries with sentence-transformers, and store the vectors in Postgres pgvector alongside a metadata table" width="100%" draggable="false"/>
  </a>
</p>

<h1 align="center">Search your <em>AI coding sessions</em> in plain English.</h1>

<p align="center">
  <b>Walk a folder of <a href="https://entire.io">Entire</a> checkpoints, <em>route</em> each file by name, and <em>embed</em> transcripts, prompts, and context into Postgres pgvector.</b><br/>
  "How did I fix the auth bug" finds the right session even with zero shared keywords — in plain async Python.
</p>

<p align="center">
  <strong>Star us&nbsp;❤️&nbsp;→</strong>&nbsp;<a href="https://github.com/cocoindex-io/cocoindex" title="Star CocoIndex on GitHub"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg" alt="Star CocoIndex on GitHub" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://cocoindex.io/docs/examples/entire-session-search/" title="Read the full walkthrough"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg" alt="CocoIndex documentation" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://discord.com/invite/zpA9S2DR7s" title="Join the CocoIndex Discord"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg" alt="Join the CocoIndex Discord" height="36" align="absmiddle"/></picture></a>
</p>

<div align="center">

[![stars](https://img.shields.io/github/stars/cocoindex-io/cocoindex?style=flat-square&label=stars&color=FB6A76)](https://github.com/cocoindex-io/cocoindex)
[![pypi](https://img.shields.io/pypi/v/cocoindex?style=flat-square&label=pypi&color=E59A63)](https://pypi.org/project/cocoindex/)
[![discord](https://img.shields.io/discord/1314801574169673738?style=flat-square&logo=discord&logoColor=white&label=discord&color=5865F2)](https://discord.com/invite/zpA9S2DR7s)
[![license](https://img.shields.io/badge/license-Apache--2.0-5B5BD6?style=flat-square)](https://opensource.org/licenses/Apache-2.0)

</div>

<br/>

[Entire](https://entire.io) captures every AI coding session you run — the full transcript, the prompt you started from, an AI-written context summary, and metadata like token counts and files touched — as checkpoints on disk. This pipeline turns that folder into a [vector index](https://github.com/pgvector/pgvector) you can search in plain English. You declare the transformation in native Python and your own types — `target_state = transformation(source_state)` — and the heavy lifting (incremental processing, change tracking, managed targets) runs in a Rust engine underneath, so each new session you capture only embeds what changed.

## How it works

A checkpoint folder holds four file types, and `process_file` routes on the name: `full.jsonl` is parsed into per-turn transcript chunks, `prompt.txt` is embedded whole, `context.md` is split into overlapping chunks, and `metadata.json` becomes a structured row in a *second* table. The transcript and context paths fan out to many rows via `coco.map(process_chunk, ...)`; the prompt is a single short string embedded inline. Read it in [`main.py`](main.py):

```python
@coco.fn(memo=True)
async def process_file(file, emb_table, meta_table) -> None:
    info = extract_session_info(file)
    filename = file.file_path.path.name
    id_gen = IdGenerator()

    if filename == "full.jsonl":
        chunks = parse_transcript(await file.read_text())
        await coco.map(process_chunk,
            [ChunkInput(text=c.text, content_type="transcript", role=c.role) for c in chunks],
            info, id_gen, emb_table)

    elif filename == "prompt.txt":
        text = (await file.read_text()).strip()
        if text:
            emb_table.declare_row(row=SessionEmbeddingRow(
                id=await id_gen.next_id(text), ..., content_type="prompt", role="user",
                text=text, embedding=await coco.use_context(EMBEDDER).embed(text)))

    elif filename == "context.md":
        ...   # split into chunks, then coco.map(process_chunk, ..., content_type="context")

    elif filename == "metadata.json":
        meta = json.loads(await file.read_text())
        meta_table.declare_row(row=SessionMetadataRow(..., total_tokens=..., files_touched=...))
```

Three content types and a structured record, all from one component. Each embedding row's `id` is derived from its text, so a turn that survives a re-parse keeps its row.

<p align="center">
  📘 <b><a href="https://cocoindex.io/docs/examples/entire-session-search/">Full Tutorial →</a></b><br/>
  Step-by-step walkthrough with the two row shapes, the per-filename routing, the chunk fan-out, and the query.
</p>

## Why it's worth a star ⭐

- **One component, four file types.** A single `included_patterns` list pulls `full.jsonl`, `prompt.txt`, `context.md`, and `metadata.json` into the same `process_file`, which routes on the name — no four separate pipelines.
- **Two tables, one pass.** Searchable text lands in the embeddings table; structured fields (tokens, files touched, agent percentage) land in a metadata table — declared side by side.
- **Incremental by default.** `@coco.fn(memo=True)` skips a file whose content and code are unchanged, so a finished session is never re-embedded; `id` derived from text means only genuinely new turns are inserted and vanished turns are deleted.
- **Live without re-scanning.** The filesystem source declares `live=True` — pass `-L` and new sessions are picked up and embedded as they're written.
- **Plain Python, your stack.** Local `all-MiniLM-L6-v2` embedder, no API key; swap `EMBED_MODEL` for any of the 12k+ sentence-transformer models on Hugging Face.

## Run it

**1. Start Postgres + pgvector** (the repo ships a compose file):

```sh
docker compose -f ../../dev/postgres.yaml up -d
```

**2. Configure & install:**

```sh
cp .env.example .env     # set POSTGRES_URL (schema/table names are optional overrides)
pip install -e .
```

**3. Check out some checkpoints** — from any repo where [Entire](https://entire.io) is capturing sessions:

```sh
git worktree add entire_checkpoints entire/checkpoints/v1
```

Each session is laid out as `<checkpoint_id[:2]>/<checkpoint_id[2:]>/<session_idx>/` with `full.jsonl`, `prompt.txt`, `context.md`, and `metadata.json`.

**4. Build the index** — catch-up (scan, sync, exit) or live (catch up, then keep watching for new sessions):

```sh
cocoindex update main        # catch-up
cocoindex update -L main     # live
```

**5. Search from the command line:**

```sh
python main.py "how did I fix the auth bug"
```

Results print which session and content type matched, so a transcript turn, a prompt, and a context chunk are all distinguishable. This example keeps it minimal and doesn't declare a vector index, so queries do a sequential scan — fine for a personal history. For a larger corpus, add `emb_table.declare_vector_index(column="embedding")` exactly as [Semantic Search 101](https://cocoindex.io/docs/examples/text-embedding/) does.

---

<p align="center">
  If this made your coding history searchable, <a href="https://github.com/cocoindex-io/cocoindex"><b>give CocoIndex a star ⭐</b></a> — it helps a lot.<br/>
  <a href="https://cocoindex.io/docs">Docs</a> · <a href="https://cocoindex.io/docs/examples/entire-session-search/">Walkthrough</a> · <a href="https://discord.com/invite/zpA9S2DR7s">Discord</a> · <a href="https://github.com/cocoindex-io/cocoindex/tree/main/examples"><b>See all examples →</b></a>
</p>

<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=7f27e85b-be3a-411a-b612-0b9d53711814&page=examples/entire_session_search" alt="" width="1" height="1" />
