<p align="center">
  <a href="https://cocoindex.io/docs/examples/text-embedding-lancedb/" title="Semantic search over Markdown with CocoIndex and LanceDB — chunk, embed, and store vectors in an embedded, file-based store with zero server to run, in plain async Python">
    <img src="https://cocoindex.io/blobs/docs-v1/img/examples/text-embedding-lancedb/cover.svg" alt="Semantic Search with LanceDB and CocoIndex — chunk a folder of Markdown, embed each chunk locally, and store the vectors in an embedded LanceDB table on disk, with no server to run" width="100%" draggable="false"/>
  </a>
</p>

<h1 align="center">Semantic search over Markdown, stored in <em>LanceDB</em>.</h1>

<p align="center">
  <b>The Semantic Search 101 pipeline pointed at <em>LanceDB</em> — an embedded, file-based vector store with no server to stand up, no <code>POSTGRES_URL</code>, just a directory on disk you can copy to move.</b><br/>
  Walk, chunk, embed locally, store — incrementally — in plain async Python.
</p>

<p align="center">
  <strong>Star us&nbsp;❤️&nbsp;→</strong>&nbsp;<a href="https://github.com/cocoindex-io/cocoindex" title="Star CocoIndex on GitHub"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg" alt="Star CocoIndex on GitHub" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://cocoindex.io/docs/examples/text-embedding-lancedb/" title="Read the full walkthrough"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg" alt="CocoIndex documentation" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://discord.com/invite/zpA9S2DR7s" title="Join the CocoIndex Discord"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg" alt="Join the CocoIndex Discord" height="36" align="absmiddle"/></picture></a>
</p>

<div align="center">

[![stars](https://img.shields.io/github/stars/cocoindex-io/cocoindex?style=flat-square&label=stars&color=FB6A76)](https://github.com/cocoindex-io/cocoindex)
[![pypi](https://img.shields.io/pypi/v/cocoindex?style=flat-square&label=pypi&color=E59A63)](https://pypi.org/project/cocoindex/)
[![discord](https://img.shields.io/discord/1314801574169673738?style=flat-square&logo=discord&logoColor=white&label=discord&color=5865F2)](https://discord.com/invite/zpA9S2DR7s)
[![license](https://img.shields.io/badge/license-Apache--2.0-5B5BD6?style=flat-square)](https://opensource.org/licenses/Apache-2.0)

</div>

<br/>

This is [Semantic Search 101](https://cocoindex.io/docs/examples/text-embedding/) with one thing changed: the vectors land in [LanceDB](https://lancedb.github.io/lancedb/) instead of Postgres. LanceDB is an embedded, file-based vector store — no server to stand up, just a `./lancedb_data/` directory created on first run. You declare the transformation in native Python and your own types — `target_state = transformation(source_state)` — and the heavy lifting (incremental processing, change tracking, managed targets) runs in a Rust engine underneath, so editing one file re-embeds one file, not the whole folder.

## How it works

The chunk-and-embed half is byte-for-byte the base example — `RecursiveSplitter` cuts each file into overlapping Markdown chunks, and a local `SentenceTransformerEmbedder` (`all-MiniLM-L6-v2`, no API key) turns each into a vector. What changes is the resource and the target: a `LanceAsyncConnection` instead of an `asyncpg` pool, and `lancedb.mount_table_target` instead of the Postgres one — same call shape, same `table.declare_row(...)`. Read it in [`main.py`](main.py):

```python
@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    conn = await lancedb.connect_async(LANCEDB_URI)   # the "connection" is just a path on disk
    builder.provide(LANCE_DB, conn)
    builder.provide(EMBEDDER, SentenceTransformerEmbedder(EMBED_MODEL))
    yield

@coco.fn
async def app_main(sourcedir: pathlib.Path) -> None:
    target_table = await lancedb.mount_table_target(
        LANCE_DB, table_name=TABLE_NAME,
        table_schema=await lancedb.TableSchema.from_class(DocEmbedding, primary_key=["id"]),
    )
    files = localfs.walk_dir(sourcedir, recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md"]), live=True)
    await coco.mount_each(process_file, files.items(), target_table)
```

`lancedb.mount_table_target` is the LanceDB counterpart to the Postgres `mount_table_target`: it creates and manages the table, handles idempotent upserts keyed on the primary key, and cleans up orphan rows when a file disappears. Only the import changed.

<p align="center">
  📘 <b><a href="https://cocoindex.io/docs/examples/text-embedding-lancedb/">Full Tutorial →</a></b><br/>
  Step-by-step walkthrough with the LanceDB connection, the managed table target, the row schema, and the async search query.
</p>

## Why it's worth a star ⭐

- **Zero infrastructure.** No database to install, no `POSTGRES_URL` — LanceDB writes to `./lancedb_data/`, created on first run. To start fresh, delete the directory and re-run.
- **Portable by design.** Data lives in one directory on disk; copy it to move the whole index.
- **Managed table target.** `lancedb.mount_table_target` owns the schema, idempotent upserts, and orphan cleanup — the same guarantees the Postgres target gives, against a local store.
- **Incremental by default.** `@coco.fn(memo=True)` skips files whose content and code are unchanged; each row's `id` is derived from its chunk text, so only changed rows are upserted and vanished ones are deleted.
- **Same flow, different store.** The chunk-and-embed code is identical to the Postgres version — proof the target is a swappable detail. The same local embedder is reused at query time so indexing and search stay consistent.

## Run it

> No database to install — LanceDB is embedded and writes to `./lancedb_data/`, created on first run.

**1. Configure & install:**

```sh
cp .env.example .env     # no required secrets; optional LANCEDB_URI override
pip install -e .
```

**2. Build the index** — the example ships a `markdown_files/` folder of sample docs:

```sh
cocoindex update main          # catch-up: scan, sync, exit
cocoindex update -L main       # live: keep watching for file changes
```

**3. Search** — embeds your query with the *same* model and returns the nearest vectors via LanceDB's async search:

```sh
python main.py "what is self-attention?"
```

The most semantically similar chunks come back ranked — even when they share none of the words in your query.

---

<p align="center">
  If this gave you a portable, server-free vector index, <a href="https://github.com/cocoindex-io/cocoindex"><b>give CocoIndex a star ⭐</b></a> — it helps a lot.<br/>
  <a href="https://cocoindex.io/docs">Docs</a> · <a href="https://cocoindex.io/docs/examples/text-embedding-lancedb/">Walkthrough</a> · <a href="https://discord.com/invite/zpA9S2DR7s">Discord</a> · <a href="https://github.com/cocoindex-io/cocoindex/tree/main/examples"><b>See all examples →</b></a>
</p>

<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=7f27e85b-be3a-411a-b612-0b9d53711814&page=examples/text_embedding_lancedb" alt="" width="1" height="1" />
