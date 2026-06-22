---
title: Embed *OCI Object Storage*
description: 'The Semantic Search 101 flow with the source swapped for an Oracle Cloud (OCI) Object Storage bucket — chunk and embed every Markdown object, store the vectors in Postgres with pgvector, and keep them live with OCI Streaming.'
slug: oci-object-storage-embedding
image: https://cocoindex.io/blobs/docs-v1/img/examples/oci-object-storage-embedding/cover.png
tags: [vector-index, oci]
---

![Embed OCI Object Storage with CocoIndex V1](https://cocoindex.io/blobs/docs-v1/img/examples/oci-object-storage-embedding/cover.png)

This is the [Semantic Search 101](https://cocoindex.io/docs/examples/text-embedding/) example with exactly one thing changed: instead of reading Markdown from a local folder, it lists Markdown objects from an [Oracle Cloud (OCI) Object Storage](https://cocoindex.io/docs/connectors/oci_object_storage/) bucket. The chunk → embed → store-in-pgvector half is identical, so the [base walkthrough](https://cocoindex.io/docs/examples/text-embedding/) covers the embedding model, the `DocEmbedding` row, and the query. Here we focus on the part that differs: the OCI client, the source call, and how the same flow goes **live** off OCI Streaming.

[→ View on GitHub](https://github.com/cocoindex-io/cocoindex/tree/main/examples/oci_object_storage_embedding)

## Flow overview

![CocoIndex OCI Object Storage flow: list Markdown objects from a bucket, split into chunks, embed each chunk, and store the vectors in Postgres with pgvector](https://cocoindex.io/blobs/docs-v1/img/examples/oci-object-storage-embedding/flow-v1.png)

From a high level, these are the steps:

1. List Markdown objects from an OCI Object Storage bucket (optionally under a prefix).
2. [Split each object into overlapping chunks](https://cocoindex.io/docs/ops/text/), then [embed](https://cocoindex.io/docs/ops/sentence_transformers/) every chunk.
3. Store the chunks and their embeddings in Postgres (as [target states](https://cocoindex.io/docs/programming_guide/target_state/)).

You [declare the transformation logic](https://cocoindex.io/docs/programming_guide/core_concepts/) with native Python, without worrying about how updates propagate. Think: **target_state = transformation(source_state)**.

## Provide the OCI client

The OCI SDK is synchronous and you create the client yourself, so we build one from a config-file profile and hand it to the [context](https://cocoindex.io/docs/programming_guide/context/) alongside the Postgres pool and the embedder. The connector wraps every SDK call in `asyncio.to_thread`, so passing the sync client through is fine.

```python title="main.py"
def _build_oci_client() -> ObjectStorageClient:
    config = oci.config.from_file(
        file_location=os.path.expanduser(OCI_CONFIG_FILE),
        profile_name=OCI_PROFILE,
    )
    return ObjectStorageClient(config)


OCI_CLIENT = coco.ContextKey[ObjectStorageClient]("oci_object_storage_client")


@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    async with asyncpg.create_pool(DATABASE_URL) as pool:
        builder.provide(PG_DB, pool)
        builder.provide(EMBEDDER, SentenceTransformerEmbedder(EMBED_MODEL))
        builder.provide(OCI_CLIENT, _build_oci_client())
        yield
```

Everything downstream of this — `DocEmbedding`, `process_file`, `process_chunk` — is the same chunk-and-embed code as the [base example](https://cocoindex.io/docs/examples/text-embedding/), so we won't repeat it here. The one small difference is the source type: `process_file` takes an `oci_object_storage.OCIFile` and reads its text with `await file.read_text()`, just like the local `FileLike`.

## List objects from the bucket

`app_main` mounts the Postgres table, then walks the bucket with [`list_objects`](https://cocoindex.io/docs/connectors/oci_object_storage/). It is the OCI analogue of `localfs.walk_dir` — give it the client, the namespace, the bucket, an optional prefix, and a path matcher, and it yields one `OCIFile` per matching object.

```python title="main.py"
@coco.fn
async def app_main() -> None:
    target_table = await postgres.mount_table_target(
        PG_DB,
        table_name=TABLE_NAME,
        table_schema=await postgres.TableSchema.from_class(
            DocEmbedding, primary_key=["id"],
        ),
        pg_schema_name=PG_SCHEMA_NAME,
    )

    client = coco.use_context(OCI_CLIENT)

    # Live mode is opt-in: build a LiveStream[bytes] from OCI Streaming if configured.
    consumer = _build_streaming_consumer()
    live_stream = None
    if consumer is not None and OCI_STREAMING_TOPIC is not None:
        live_stream = kafka.topic_as_stream(consumer, [OCI_STREAMING_TOPIC]).payloads()

    files = oci_object_storage.list_objects(
        client,
        OCI_NAMESPACE,
        OCI_BUCKET,
        prefix=OCI_PREFIX,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md"]),
        live_stream=live_stream,
    )
    await coco.mount_each(process_file, files.items(), target_table)
```

`mount_each` runs one [processing component](https://cocoindex.io/docs/programming_guide/processing_component/) per object so the engine tracks and updates each independently. With `live_stream=None` (the default), `list_objects` does a one-shot catch-up scan. Pass a stream and it keeps watching — that's the next section.

## Live mode via OCI Streaming

OCI Streaming is [Kafka-protocol-compatible](https://cocoindex.io/docs/connectors/kafka/), so live updates ride the [Kafka connector](https://cocoindex.io/docs/connectors/kafka/). When the four `OCI_STREAMING_*` env vars are set, we build a `confluent_kafka.aio.AIOConsumer` with `SASL_SSL` + `PLAIN` auth, wrap it with `kafka.topic_as_stream(...).payloads()` to get a `LiveStream[bytes]`, and pass it to `list_objects`. The connector snapshots a cutoff before the scan, runs the scan and stream concurrently, and for each post-cutoff event re-reads the object to apply an authoritative update or delete — see the [OCI Object Storage connector docs](https://cocoindex.io/docs/connectors/oci_object_storage/) for the details.

```python title="main.py"
def _build_streaming_consumer() -> AIOConsumer | None:
    if not (
        OCI_STREAMING_BOOTSTRAP_SERVERS and OCI_STREAMING_TOPIC
        and OCI_STREAMING_USERNAME and OCI_STREAMING_AUTH_TOKEN
    ):
        return None
    return AIOConsumer({
        "bootstrap.servers": OCI_STREAMING_BOOTSTRAP_SERVERS,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": OCI_STREAMING_USERNAME,
        "sasl.password": OCI_STREAMING_AUTH_TOKEN,
        "group.id": OCI_STREAMING_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
```

## Setup and run

- A running Postgres with the [pgvector](https://github.com/pgvector/pgvector) extension:

  ```sh
  export POSTGRES_URL="postgres://cocoindex:cocoindex@localhost/cocoindex"
  ```

- An OCI Object Storage bucket with Markdown objects, and an [OCI config file](https://docs.oracle.com/en-us/iaas/Content/API/Concepts/sdkconfig.htm) (default `~/.oci/config`) with API-key auth. Copy `.env.example` to `.env` and fill in `OCI_NAMESPACE`, `OCI_BUCKET`, and an optional `OCI_PREFIX`.

- Install CocoIndex with the OCI, Kafka, Postgres, and embedding extras:

  ```sh
  pip install -U "cocoindex[oci,kafka,postgres,sentence_transformers]" asyncpg pgvector numpy python-dotenv
  ```

Build the index — catch-up (scan, sync, exit) or live (catch up, then keep watching the OCI Streaming topic):

```sh
# Catch-up run
cocoindex update main

# Live run: keep watching OCI Streaming for change events
cocoindex update -L main
```

Then search straight from the command line:

```sh
python main.py "what is self-attention?"
```

This example keeps it minimal and doesn't declare a vector index, so queries do a sequential scan — fine for a few objects. For a larger corpus, add `target_table.declare_vector_index(column="embedding")` exactly as [Semantic Search 101](https://cocoindex.io/docs/examples/text-embedding/) does.

## Incremental updates

Incrementality works the same as the [base example](https://cocoindex.io/docs/examples/text-embedding/): `@coco.fn(memo=True)` skips an object whose content and code are unchanged, and `mount_table_target` upserts only the rows that actually changed and deletes rows whose source is gone.

- **An object is added** — only that object is chunked and embedded; its rows are inserted.
- **An object is updated** — it is re-chunked; unchanged chunks keep their `id` and embedding, new chunks are embedded and inserted, and vanished chunks are deleted.
- **An object is deleted** — its rows are removed from the target automatically.

In catch-up mode CocoIndex discovers these by re-scanning the bucket; in live mode the OCI Streaming events drive the same updates with low latency, no full re-scan needed.

## Run it

The full, runnable example is in the CocoIndex repo: [examples/oci_object_storage_embedding](https://github.com/cocoindex-io/cocoindex/tree/main/examples/oci_object_storage_embedding). If you're starting from a local folder instead of a bucket, [Semantic Search 101](https://cocoindex.io/docs/examples/text-embedding/) is the same flow on the local filesystem.

If this helped, [give CocoIndex a star on GitHub](https://github.com/cocoindex-io/cocoindex) and come say hi on [Discord](https://discord.com/invite/zpA9S2DR7s).
