<p align="center">
  <a href="https://cocoindex.io/docs/examples/oci-object-storage-embedding/" title="Embed Markdown objects from an OCI Object Storage bucket into Postgres pgvector with CocoIndex — incremental, live via OCI Streaming, in plain async Python">
    <img src="https://cocoindex.io/blobs/docs-v1/img/examples/oci-object-storage-embedding/cover.svg" alt="Embed an OCI Object Storage bucket with CocoIndex — list Markdown objects, chunk and embed each one with sentence-transformers, store the vectors in Postgres pgvector, and keep them live off OCI Streaming events" width="100%" draggable="false"/>
  </a>
</p>

<h1 align="center">Semantic search over an <em>OCI</em> Object Storage bucket.</h1>

<p align="center">
  <b>List Markdown objects from an Oracle Cloud bucket, <em>chunk</em> and <em>embed</em> each one, and store the vectors in Postgres pgvector — kept <em>live</em> off OCI Streaming.</b><br/>
  It's Semantic Search 101 with the source swapped for a bucket — in plain async Python.
</p>

<p align="center">
  <strong>Star us&nbsp;❤️&nbsp;→</strong>&nbsp;<a href="https://github.com/cocoindex-io/cocoindex" title="Star CocoIndex on GitHub"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg" alt="Star CocoIndex on GitHub" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://cocoindex.io/docs/examples/oci-object-storage-embedding/" title="Read the full walkthrough"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg" alt="CocoIndex documentation" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://discord.com/invite/zpA9S2DR7s" title="Join the CocoIndex Discord"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg" alt="Join the CocoIndex Discord" height="36" align="absmiddle"/></picture></a>
</p>

<div align="center">

[![stars](https://img.shields.io/github/stars/cocoindex-io/cocoindex?style=flat-square&label=stars&color=FB6A76)](https://github.com/cocoindex-io/cocoindex)
[![pypi](https://img.shields.io/pypi/v/cocoindex?style=flat-square&label=pypi&color=E59A63)](https://pypi.org/project/cocoindex/)
[![discord](https://img.shields.io/discord/1314801574169673738?style=flat-square&logo=discord&logoColor=white&label=discord&color=5865F2)](https://discord.com/invite/zpA9S2DR7s)
[![license](https://img.shields.io/badge/license-Apache--2.0-5B5BD6?style=flat-square)](https://opensource.org/licenses/Apache-2.0)

</div>

<br/>

Most documents already live in object storage, not on your laptop. This pipeline lists Markdown objects from an [OCI Object Storage](https://cocoindex.io/docs/connectors/oci_object_storage/) bucket, splits each into overlapping chunks, embeds them with sentence-transformers, and stores the vectors in Postgres with pgvector. You declare the transformation in native Python and your own types — `target_state = transformation(source_state)` — and the heavy lifting (incremental processing, change tracking, managed targets) runs in a Rust engine underneath, so adding one object embeds one object, not the whole bucket.

## How it works

The chunk → embed → store half is identical to [Semantic Search 101](https://cocoindex.io/docs/examples/text-embedding/); the part that differs is the source. The OCI SDK is synchronous and you create the client yourself, so the example builds one from a config-file profile, hands it to the [context](https://cocoindex.io/docs/programming_guide/context/), and lists objects with `oci_object_storage.list_objects` — the OCI analogue of `localfs.walk_dir`. Live mode is opt-in: when the four `OCI_STREAMING_*` env vars are set, it builds a Kafka-protocol `AIOConsumer` against OCI Streaming and passes it through as a `LiveStream[bytes]`. Read it in [`main.py`](main.py):

```python
@coco.fn
async def app_main() -> None:
    target_table = await postgres.mount_table_target(
        PG_DB, table_name=TABLE_NAME,
        table_schema=await postgres.TableSchema.from_class(DocEmbedding, primary_key=["id"]),
        pg_schema_name=PG_SCHEMA_NAME,
    )
    client = coco.use_context(OCI_CLIENT)

    # Live mode is opt-in: build a LiveStream[bytes] from OCI Streaming if configured.
    consumer = _build_streaming_consumer()
    live_stream = None
    if consumer is not None and OCI_STREAMING_TOPIC is not None:
        live_stream = kafka.topic_as_stream(consumer, [OCI_STREAMING_TOPIC]).payloads()

    files = oci_object_storage.list_objects(
        client, OCI_NAMESPACE, OCI_BUCKET, prefix=OCI_PREFIX,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md"]),
        live_stream=live_stream,
    )
    await coco.mount_each(process_file, files.items(), target_table)
```

With `live_stream=None` (the default), `list_objects` does a one-shot catch-up scan. Pass a stream and the connector keeps watching, re-reading each post-cutoff object to apply an authoritative update or delete. `mount_each` runs one [processing component](https://cocoindex.io/docs/programming_guide/processing_component/) per object so the engine tracks each independently.

<p align="center">
  📘 <b><a href="https://cocoindex.io/docs/examples/oci-object-storage-embedding/">Full Tutorial →</a></b><br/>
  Step-by-step walkthrough with the OCI client, the source call, the row schema, and exactly how live mode rides OCI Streaming.
</p>

## Why it's worth a star ⭐

- **Swap the source, keep the flow.** Only the source line changes from the local-folder example — `process_file` takes an `oci_object_storage.OCIFile` and reads it with `await file.read_text()`, just like a local `FileLike`.
- **Live without re-scanning.** OCI Streaming is Kafka-compatible, so object create/update/delete events ride the [Kafka connector](https://cocoindex.io/docs/connectors/kafka/) and drive incremental updates with no full bucket re-scan.
- **Authoritative, not event-trusting.** For each accepted event the connector re-reads the object (`head_object`) to determine current state, then issues an update (present) or delete (404) — the event type is never trusted as the dispatch signal.
- **Incremental by default.** `@coco.fn(memo=True)` skips an object whose content and code are unchanged; `mount_table_target` upserts only changed rows and deletes rows whose source is gone.
- **Plain Python, your stack.** Local sentence-transformer embedder, no API key; swap `EMBED_MODEL` for any of the 12k+ models on Hugging Face.

## Run it

**1. Start Postgres + pgvector** (the repo ships a compose file):

```sh
docker compose -f ../../dev/postgres.yaml up -d
```

**2. Configure & install** — point at a bucket with Markdown objects and an [OCI config file](https://docs.oracle.com/en-us/iaas/Content/API/Concepts/sdkconfig.htm) (default `~/.oci/config`):

```sh
cp .env.example .env     # set POSTGRES_URL, OCI_NAMESPACE, OCI_BUCKET (optional OCI_PREFIX)
pip install -e .
```

For live mode, also set `OCI_STREAMING_BOOTSTRAP_SERVERS`, `OCI_STREAMING_TOPIC`, `OCI_STREAMING_USERNAME`, and `OCI_STREAMING_AUTH_TOKEN` in `.env` (the `.env.example` documents each format). With those unset, the connector skips the subscription and just does the catch-up scan.

**3. Build the index** — catch-up (scan, sync, exit) or live (catch up, then keep watching the topic):

```sh
cocoindex update main        # catch-up
cocoindex update -L main     # live
```

**4. Search from the command line:**

```sh
python main.py "what is self-attention?"
```

This example keeps it minimal and doesn't declare a vector index, so queries do a sequential scan — fine for a few objects. For a larger corpus, add `target_table.declare_vector_index(column="embedding")` exactly as [Semantic Search 101](https://cocoindex.io/docs/examples/text-embedding/) does.

---

<p align="center">
  If this made your bucket searchable, <a href="https://github.com/cocoindex-io/cocoindex"><b>give CocoIndex a star ⭐</b></a> — it helps a lot.<br/>
  <a href="https://cocoindex.io/docs">Docs</a> · <a href="https://cocoindex.io/docs/examples/oci-object-storage-embedding/">Walkthrough</a> · <a href="https://discord.com/invite/zpA9S2DR7s">Discord</a> · <a href="https://github.com/cocoindex-io/cocoindex/tree/main/examples"><b>See all examples →</b></a>
</p>

<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=7f27e85b-be3a-411a-b612-0b9d53711814&page=examples/oci_object_storage_embedding" alt="" width="1" height="1" />
