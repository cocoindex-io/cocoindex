<p align="center">
  <a href="https://cocoindex.io/docs/examples/amazon-s3-embedding/" title="Embed Markdown from an Amazon S3 bucket with CocoIndex — list objects, chunk, embed, and store vectors in Postgres + pgvector, in plain async Python">
    <img src="https://cocoindex.io/blobs/docs-v1/img/examples/amazon-s3-embedding/cover.svg" alt="Embed Markdown from Amazon S3 with CocoIndex — list Markdown objects from a bucket, chunk and embed each one locally, and store the vectors in Postgres with pgvector for natural-language search" width="100%" draggable="false"/>
  </a>
</p>

<h1 align="center">Semantic search over Markdown in an <em>S3 bucket</em>.</h1>

<p align="center">
  <b>The Semantic Search 101 pipeline with one thing swapped: the source is an <em>Amazon S3</em> bucket instead of a local folder.</b><br/>
  List objects, chunk, embed locally, store in Postgres + pgvector — incrementally — in plain async Python.
</p>

<p align="center">
  <strong>Star us&nbsp;❤️&nbsp;→</strong>&nbsp;<a href="https://github.com/cocoindex-io/cocoindex" title="Star CocoIndex on GitHub"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg" alt="Star CocoIndex on GitHub" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://cocoindex.io/docs/examples/amazon-s3-embedding/" title="Read the full walkthrough"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg" alt="CocoIndex documentation" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://discord.com/invite/zpA9S2DR7s" title="Join the CocoIndex Discord"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg" alt="Join the CocoIndex Discord" height="36" align="absmiddle"/></picture></a>
</p>

<div align="center">

[![stars](https://img.shields.io/github/stars/cocoindex-io/cocoindex?style=flat-square&label=stars&color=FB6A76)](https://github.com/cocoindex-io/cocoindex)
[![pypi](https://img.shields.io/pypi/v/cocoindex?style=flat-square&label=pypi&color=E59A63)](https://pypi.org/project/cocoindex/)
[![discord](https://img.shields.io/discord/1314801574169673738?style=flat-square&logo=discord&logoColor=white&label=discord&color=5865F2)](https://discord.com/invite/zpA9S2DR7s)
[![license](https://img.shields.io/badge/license-Apache--2.0-5B5BD6?style=flat-square)](https://opensource.org/licenses/Apache-2.0)

</div>

<br/>

This is [Semantic Search 101](https://cocoindex.io/docs/examples/text-embedding/) with one thing swapped: the source is an Amazon S3 bucket instead of a local directory. Everything downstream — chunking, embedding, the Postgres/pgvector target, and the query — is identical. You declare the transformation in native Python and your own types — `target_state = transformation(source_state)` — and the heavy lifting (incremental processing, change tracking, managed targets) runs in a Rust engine underneath, so adding one object embeds one object, not the whole bucket.

## How it works

The S3 connector needs an [aiobotocore](https://github.com/aio-libs/aiobotocore) client, opened once in the lifespan alongside the Postgres pool and embedder. `app_main` mounts the Postgres table exactly as in the base example, then swaps `localfs.walk_dir` for `amazon_s3.list_objects` — same `path_matcher` glob, same `mount_each` fan-out. Read it in [`main.py`](main.py):

```python
@coco.fn
async def app_main() -> None:
    target_table = await postgres.mount_table_target(
        PG_DB, table_name=TABLE_NAME,
        table_schema=await postgres.TableSchema.from_class(DocEmbedding, primary_key=["id"]),
        pg_schema_name=PG_SCHEMA_NAME,
    )
    client = coco.use_context(S3_CLIENT)
    files = amazon_s3.list_objects(
        client, S3_BUCKET, prefix=S3_PREFIX,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md"]),
    )
    await coco.mount_each(process_file, files.items(), target_table)
```

`list_objects` yields one `S3File` per matching object; `prefix` scopes the listing server-side, and the glob filters the rest. `process_file` then reads, chunks, and embeds each one — identical to the base example. `create_client("s3")` picks up standard AWS credentials (env vars, `~/.aws/credentials`, or an IAM role); set `AWS_ENDPOINT_URL` to point at an S3-compatible service like MinIO.

<p align="center">
  📘 <b><a href="https://cocoindex.io/docs/examples/amazon-s3-embedding/">Full Tutorial →</a></b><br/>
  Step-by-step walkthrough with the S3 client, the prefix/glob listing, the Postgres target, and the catch-up run.
</p>

## Why it's worth a star ⭐

- **S3 as a first-class source.** `amazon_s3.list_objects` drops into the same `mount_each` fan-out as a local folder — the source is a swappable detail, not a rewrite.
- **Scoped listing.** `prefix` filters server-side and the `**/*.md` glob filters the rest, so you index only what you mean to.
- **S3-compatible too.** Point `AWS_ENDPOINT_URL` at MinIO or any S3-compatible service; credentials come from the standard AWS chain.
- **Incremental by default.** `@coco.fn(memo=True)` skips objects whose content and code are unchanged; each row's `id` is derived from its chunk text, so re-running upserts only changed rows and deletes rows whose source object is gone.
- **Managed Postgres target.** A single `mount_table_target` owns the schema, idempotent upserts, and orphan cleanup; the same local `all-MiniLM-L6-v2` embedder is reused at query time so indexing and search stay consistent.

## Run it

**1. Start Postgres + pgvector:**

```sh
docker compose -f ../../dev/postgres.yaml up -d
```

**2. Configure & install** — set the bucket, optional prefix, and your AWS credentials:

```sh
cp .env.example .env     # set S3_BUCKET, optional S3_PREFIX; AWS creds from env / ~/.aws / IAM role
pip install -e .
```

**3. Build the index** — the `amazon_s3` source does not support live mode, so this is a one-shot catch-up run (scan the bucket, sync, exit):

```sh
cocoindex update main
```

**4. Search** — embeds your query with the *same* model and returns the nearest chunks by pgvector cosine distance:

```sh
python main.py "what is self-attention?"
```

The most semantically similar chunks come back ranked — even when they share none of the words in your query. Re-run `cocoindex update main` to pick up bucket changes; the engine still applies just the difference.

---

<p align="center">
  If this made your S3 archive searchable by meaning, <a href="https://github.com/cocoindex-io/cocoindex"><b>give CocoIndex a star ⭐</b></a> — it helps a lot.<br/>
  <a href="https://cocoindex.io/docs">Docs</a> · <a href="https://cocoindex.io/docs/examples/amazon-s3-embedding/">Walkthrough</a> · <a href="https://discord.com/invite/zpA9S2DR7s">Discord</a> · <a href="https://github.com/cocoindex-io/cocoindex/tree/main/examples"><b>See all examples →</b></a>
</p>

<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=7f27e85b-be3a-411a-b612-0b9d53711814&page=examples/amazon_s3_embedding" alt="" width="1" height="1" />
