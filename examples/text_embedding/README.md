<p align="center">
  <a href="https://cocoindex.io/docs/examples/text-embedding/" title="Build a semantic search index over Markdown with CocoIndex — chunk, embed, and store vectors in Postgres + pgvector, in plain async Python">
    <img src="https://cocoindex.io/blobs/docs-v1/img/examples/text-embedding/cover.svg" alt="Semantic Search 101 with CocoIndex — chunk a folder of Markdown, embed each chunk with a local sentence-transformer, store the vectors in Postgres with pgvector, and search them in plain English" width="100%" draggable="false"/>
  </a>
</p>

<h1 align="center">Turn a folder of Markdown into a <em>searchable</em> vector index.</h1>

<p align="center">
  <b>Chunk, embed, and store every passage in Postgres + pgvector, then search it in <em>plain English</em> — the foundation under every RAG and semantic-search system.</b><br/>
  "How does incremental processing work?" finds the right passage even when it shares no keywords — in plain async Python.
</p>

<p align="center">
  <strong>Star us&nbsp;❤️&nbsp;→</strong>&nbsp;<a href="https://github.com/cocoindex-io/cocoindex" title="Star CocoIndex on GitHub"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg" alt="Star CocoIndex on GitHub" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://cocoindex.io/docs/examples/text-embedding/" title="Read the full walkthrough"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg" alt="CocoIndex documentation" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://discord.com/invite/zpA9S2DR7s" title="Join the CocoIndex Discord"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg" alt="Join the CocoIndex Discord" height="36" align="absmiddle"/></picture></a>
</p>

<div align="center">

[![stars](https://img.shields.io/github/stars/cocoindex-io/cocoindex?style=flat-square&label=stars&color=FB6A76)](https://github.com/cocoindex-io/cocoindex)
[![pypi](https://img.shields.io/pypi/v/cocoindex?style=flat-square&label=pypi&color=E59A63)](https://pypi.org/project/cocoindex/)
[![discord](https://img.shields.io/discord/1314801574169673738?style=flat-square&logo=discord&logoColor=white&label=discord&color=5865F2)](https://discord.com/invite/zpA9S2DR7s)
[![license](https://img.shields.io/badge/license-Apache--2.0-5B5BD6?style=flat-square)](https://opensource.org/licenses/Apache-2.0)

</div>

<br/>

A pile of Markdown has the answers hiding in plain sight — but locked behind exact-keyword search. This pipeline reads each file, splits it into overlapping chunks, embeds every chunk with a local sentence-transformer, and stores the vectors in [Postgres + pgvector](https://github.com/pgvector/pgvector) so you can search by *meaning*. You declare the transformation in native Python and your own types — `target_state = transformation(source_state)` — and the heavy lifting (incremental processing, change tracking, managed targets) runs in a Rust engine underneath, so editing one file re-embeds one file, not the whole folder.

## How it works

The whole pipeline is ordinary `async` Python and the row type is your own dataclass:

1. **Walk** Markdown files from a local directory (`live=True`, so it can watch for changes).
2. **Chunk** each file into overlapping pieces with `RecursiveSplitter` — small, focused units, with overlap so an idea straddling a boundary still lands whole.
3. **Embed** every chunk with `all-MiniLM-L6-v2`, a small, fast model that runs locally with no API key.
4. **Store** one row per chunk in Postgres, with a pgvector index over the embedding.

`process_file` runs once per file; `memo=True` makes it incremental — if a file's content and the function's code are unchanged, the whole file is skipped on the next run. Read it top-to-bottom in [`main.py`](main.py):

```python
@dataclass
class DocEmbedding:
    id: int
    filename: str
    chunk_start: int
    chunk_end: int
    text: str
    embedding: Annotated[NDArray, EMBEDDER]   # dimension inferred from the embedder

@coco.fn(memo=True)
async def process_file(file: FileLike, table: postgres.TableTarget[DocEmbedding]) -> None:
    text = await file.read_text()
    chunks = _splitter.split(text, chunk_size=2000, chunk_overlap=500, language="markdown")
    id_gen = IdGenerator()
    await coco.map(process_chunk, chunks, file.file_path.path, id_gen, table)

@coco.fn
async def app_main(sourcedir: pathlib.Path) -> None:
    target_table = await postgres.mount_table_target(
        PG_DB, table_name=TABLE_NAME,
        table_schema=await postgres.TableSchema.from_class(DocEmbedding, primary_key=["id"]),
        pg_schema_name=PG_SCHEMA_NAME,
    )
    target_table.declare_vector_index(column="embedding")
    files = localfs.walk_dir(sourcedir, recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md"]), live=True)
    await coco.mount_each(process_file, files.items(), target_table)
```

Each row's `id` is derived from its chunk text, so re-running upserts only the rows that actually changed and deletes the ones whose source is gone — you never write update logic.

<p align="center">
  📘 <b><a href="https://cocoindex.io/docs/examples/text-embedding/">Full Tutorial →</a></b><br/>
  Step-by-step walkthrough with the row schema, the chunk-and-embed flow, the vector index, the SQL query, and exactly what happens on each kind of change.
</p>

## Why it's worth a star ⭐

- **The simplest end-to-end vector index.** Walk → chunk → embed → store, in one short `main.py` — the canonical foundation under RAG and semantic search.
- **Incremental by default.** `@coco.fn(memo=True)` caches per file; edit one file and only its changed chunks re-embed, then `mount_table_target` upserts the diff and cleans up orphans — no diff logic to write.
- **Managed Postgres target.** A single `mount_table_target` owns the table schema, the pgvector index, idempotent upserts, and deletion when a file disappears.
- **Local, no API key.** Embeddings come from `all-MiniLM-L6-v2` via [sentence-transformers](https://huggingface.co/models?other=sentence-transformers) — swap in any of 12k+ models. The same embedder is reused at query time so indexing and search stay consistent.
- **Honest cache busting.** `EMBEDDER` is declared with `detect_change=True`, so swapping the model re-embeds everything against it with no cache to clear by hand.

## Run it

**1. Start Postgres + pgvector:**

```sh
docker compose -f ../../dev/postgres.yaml up -d
```

**2. Configure & install:**

```sh
cp .env.example .env     # set POSTGRES_URL (defaults to the local docker one)
pip install -e .
```

**3. Build the index** — the example ships a `markdown_files/` folder of sample docs:

```sh
cocoindex update main          # catch-up: scan, sync, exit
cocoindex update -L main       # live: keep watching for file changes
```

**4. Search** — embeds your query with the *same* model and returns the nearest chunks by pgvector cosine distance:

```sh
python main.py "what is self-attention?"
```

The most semantically similar chunks come back ranked — even when they share none of the words in your query. That's the whole point of a vector index.

---

<p align="center">
  If this made your docs searchable by meaning, <a href="https://github.com/cocoindex-io/cocoindex"><b>give CocoIndex a star ⭐</b></a> — it helps a lot.<br/>
  <a href="https://cocoindex.io/docs">Docs</a> · <a href="https://cocoindex.io/docs/examples/text-embedding/">Walkthrough</a> · <a href="https://discord.com/invite/zpA9S2DR7s">Discord</a> · <a href="https://github.com/cocoindex-io/cocoindex/tree/main/examples"><b>See all examples →</b></a>
</p>

<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=7f27e85b-be3a-411a-b612-0b9d53711814&page=examples/text_embedding" alt="" width="1" height="1" />
