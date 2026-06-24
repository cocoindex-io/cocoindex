<p align="center">
  <a href="https://cocoindex.io/docs/examples/text-embedding-turbopuffer/" title="Semantic search over Markdown with CocoIndex and Turbopuffer — chunk, embed, and upsert vectors into a managed, serverless namespace, incrementally, in plain async Python">
    <img src="https://cocoindex.io/blobs/docs-v1/img/examples/text-embedding-turbopuffer/cover.svg" alt="Semantic Search with Turbopuffer using CocoIndex — chunk a folder of Markdown, embed each chunk locally, and upsert the vectors into a managed, serverless Turbopuffer namespace with no database to run yourself" width="100%" draggable="false"/>
  </a>
</p>

<h1 align="center">Semantic search over Markdown, stored in <em>Turbopuffer</em>.</h1>

<p align="center">
  <b>The Semantic Search 101 pipeline pointed at <em>Turbopuffer</em> — a managed, serverless vector store, so there's no database to run yourself.</b><br/>
  Walk, chunk, embed locally, upsert — incrementally — in plain async Python.
</p>

<p align="center">
  <strong>Star us&nbsp;❤️&nbsp;→</strong>&nbsp;<a href="https://github.com/cocoindex-io/cocoindex" title="Star CocoIndex on GitHub"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg" alt="Star CocoIndex on GitHub" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://cocoindex.io/docs/examples/text-embedding-turbopuffer/" title="Read the full walkthrough"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg" alt="CocoIndex documentation" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://discord.com/invite/zpA9S2DR7s" title="Join the CocoIndex Discord"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg" alt="Join the CocoIndex Discord" height="36" align="absmiddle"/></picture></a>
</p>

<div align="center">

[![stars](https://img.shields.io/github/stars/cocoindex-io/cocoindex?style=flat-square&label=stars&color=FB6A76)](https://github.com/cocoindex-io/cocoindex)
[![pypi](https://img.shields.io/pypi/v/cocoindex?style=flat-square&label=pypi&color=E59A63)](https://pypi.org/project/cocoindex/)
[![discord](https://img.shields.io/discord/1314801574169673738?style=flat-square&logo=discord&logoColor=white&label=discord&color=5865F2)](https://discord.com/invite/zpA9S2DR7s)
[![license](https://img.shields.io/badge/license-Apache--2.0-5B5BD6?style=flat-square)](https://opensource.org/licenses/Apache-2.0)

</div>

<br/>

This is [Semantic Search 101](https://cocoindex.io/docs/examples/text-embedding/) with one thing swapped: instead of storing the vectors in Postgres with pgvector, we write them to a [Turbopuffer](https://turbopuffer.com/) namespace — a managed, serverless vector store, so there's no database to run yourself. The chunking and embedding are identical; only the target changes. You declare the transformation in native Python and your own types — `target_state = transformation(source_state)` — and the heavy lifting (incremental processing, change tracking, managed targets) runs in a Rust engine underneath, so editing one file re-embeds one file, not the whole folder.

## How it works

Turbopuffer is a cloud service, so the shared resource is an `AsyncTurbopuffer` client (keyed off `TURBOPUFFER_API_KEY`) rather than a database pool. A Turbopuffer row is an `id`, a `vector`, and an open bag of `attributes` — the filename, text, and offsets ride along as attributes while the embedding is the indexed vector. Read it in [`main.py`](main.py):

```python
@coco.fn
async def process_chunk(chunk, filename, id_gen, target: turbopuffer.NamespaceTarget) -> None:
    embedding_vec = await coco.use_context(EMBEDDER).embed(chunk.text)
    target.declare_row(
        turbopuffer.Row(
            id=str(await id_gen.next_id(chunk.text)),   # stable id derived from chunk text
            vector=embedding_vec,
            attributes={"filename": str(filename), "chunk_start": chunk.start.char_offset,
                        "chunk_end": chunk.end.char_offset, "text": chunk.text},
        )
    )

@coco.fn
async def app_main(sourcedir: pathlib.Path) -> None:
    target_namespace = await turbopuffer.mount_namespace_target(
        TPUF_DB, namespace_name=TPUF_NAMESPACE,
        schema=await turbopuffer.NamespaceSchema.create(vectors=turbopuffer.VectorDef(schema=EMBEDDER)),
    )
    files = localfs.walk_dir(sourcedir, recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md"]), live=True)
    await coco.mount_each(process_file, files.items(), target_namespace)
```

`target.declare_row` declares the row as a target state; CocoIndex handles upserting and deleting it to match. The namespace's dimension comes straight from the embedder, so it always matches what you write.

<p align="center">
  📘 <b><a href="https://cocoindex.io/docs/examples/text-embedding-turbopuffer/">Full Tutorial →</a></b><br/>
  Step-by-step walkthrough with the Turbopuffer client, the namespace target, the row schema, and the incremental story.
</p>

## Why it's worth a star ⭐

- **No database to run.** Turbopuffer is managed and serverless — bring an API key and the namespace is created and managed for you.
- **Managed namespace target.** A single `mount_namespace_target` handles schema, idempotent upserts, and orphan cleanup when a file disappears.
- **No hardcoded dimensions.** The namespace's vector size comes from `VectorDef(schema=EMBEDDER)`, so swapping the model carries the schema along.
- **Incremental by default.** `@coco.fn(memo=True)` skips files whose content and code are unchanged; each row's `id` is derived from its chunk text, so only changed rows are upserted and vanished ones are deleted.
- **Same flow, different store.** The chunk-and-embed half is identical to the Postgres version; the query reuses the *same* local `all-MiniLM-L6-v2` embedder and asks Turbopuffer for the nearest vectors with `rank_by=("vector", "ANN", ...)`.

## Run it

**1. Get a Turbopuffer API key** — a free key from [turbopuffer.com](https://turbopuffer.com/).

**2. Configure & install:**

```sh
cp .env.example .env     # set TURBOPUFFER_API_KEY=tpuf_... (TURBOPUFFER_REGION defaults to gcp-us-central1)
pip install -e .
```

**3. Build the index** — the example ships a `markdown_files/` folder of sample docs:

```sh
cocoindex update main          # catch-up: scan, sync, exit
cocoindex update -L main       # live: keep watching for file changes
```

**4. Search** — embeds your query with the *same* model and asks Turbopuffer for the nearest vectors:

```sh
python main.py "what is self-attention?"
```

The most semantically similar chunks come back ranked — even when they share none of the words in your query.

---

<p align="center">
  If this gave you a serverless vector index, <a href="https://github.com/cocoindex-io/cocoindex"><b>give CocoIndex a star ⭐</b></a> — it helps a lot.<br/>
  <a href="https://cocoindex.io/docs">Docs</a> · <a href="https://cocoindex.io/docs/examples/text-embedding-turbopuffer/">Walkthrough</a> · <a href="https://discord.com/invite/zpA9S2DR7s">Discord</a> · <a href="https://github.com/cocoindex-io/cocoindex/tree/main/examples"><b>See all examples →</b></a>
</p>

<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=7f27e85b-be3a-411a-b612-0b9d53711814&page=examples/text_embedding_turbopuffer" alt="" width="1" height="1" />
