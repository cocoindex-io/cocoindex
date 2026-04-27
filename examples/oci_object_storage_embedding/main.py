"""
OCI Object Storage Text Embedding (v1) — CocoIndex pipeline example.

- List markdown files from an OCI Object Storage bucket
- Optionally subscribe to OCI Streaming (Kafka-compatible) for change events,
  enabling live-mode updates via the connector's ``LiveMapView``
- Chunk text (RecursiveSplitter)
- Embed chunks (SentenceTransformers)
- Store into Postgres with pgvector column (no vector index)
- Query demo using pgvector cosine distance (<=>)

Live mode is opt-in via the ``OCI_STREAMING_BOOTSTRAP_SERVERS`` env var
(plus ``OCI_STREAMING_TOPIC`` and credentials). When set, the example
constructs a Kafka consumer pointed at OCI Streaming and feeds its byte
payloads into ``oci_object_storage.list_objects(..., live_stream=...)``.
"""

from __future__ import annotations

import asyncio
import os
import sys
from dataclasses import dataclass
from typing import Annotated, AsyncIterator

import asyncpg
import oci  # type: ignore[import-not-found]
from confluent_kafka.aio import AIOConsumer  # type: ignore[import-not-found]
from numpy.typing import NDArray
from oci.object_storage import ObjectStorageClient  # type: ignore[import-not-found]

import cocoindex as coco
from cocoindex.connectors import kafka, oci_object_storage, postgres
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder
from cocoindex.ops.text import RecursiveSplitter
from cocoindex.resources.chunk import Chunk
from cocoindex.resources.file import PatternFilePathMatcher
from cocoindex.resources.id import IdGenerator


DATABASE_URL = os.getenv(
    "POSTGRES_URL", "postgres://cocoindex:cocoindex@localhost/cocoindex"
)
TABLE_NAME = "oci_object_storage_doc_embeddings"
PG_SCHEMA_NAME = "coco_examples"
TOP_K = 5

# OCI Object Storage configuration
OCI_NAMESPACE = os.environ["OCI_NAMESPACE"]
OCI_BUCKET = os.environ["OCI_BUCKET"]
OCI_PREFIX = os.getenv("OCI_PREFIX", "")
OCI_CONFIG_FILE = os.getenv("OCI_CONFIG_FILE", "~/.oci/config")
OCI_PROFILE = os.getenv("OCI_PROFILE", "DEFAULT")

# OCI Streaming (Kafka-compatible) configuration — optional, enables live mode
OCI_STREAMING_BOOTSTRAP_SERVERS = os.getenv("OCI_STREAMING_BOOTSTRAP_SERVERS")
OCI_STREAMING_TOPIC = os.getenv("OCI_STREAMING_TOPIC")
OCI_STREAMING_USERNAME = os.getenv("OCI_STREAMING_USERNAME")  # tenancy/user/streampool
OCI_STREAMING_AUTH_TOKEN = os.getenv("OCI_STREAMING_AUTH_TOKEN")
OCI_STREAMING_GROUP_ID = os.getenv(
    "OCI_STREAMING_GROUP_ID", "cocoindex-oci-object-storage-example"
)

EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
PG_DB = coco.ContextKey[asyncpg.Pool]("oci_embedding_db")
OCI_CLIENT = coco.ContextKey[ObjectStorageClient]("oci_object_storage_client")
EMBEDDER = coco.ContextKey[SentenceTransformerEmbedder]("embedder", detect_change=True)

_splitter = RecursiveSplitter()


def _build_oci_client() -> ObjectStorageClient:
    """Construct an OCI ObjectStorageClient from a config file profile.

    The native ``oci`` SDK is sync-only; the cocoindex connector wraps calls
    with ``asyncio.to_thread``, so passing the sync client through is fine.
    """
    config = oci.config.from_file(
        file_location=os.path.expanduser(OCI_CONFIG_FILE),
        profile_name=OCI_PROFILE,
    )
    return ObjectStorageClient(config)


def _build_streaming_consumer() -> AIOConsumer | None:
    """If OCI Streaming env vars are set, build an unsubscribed AIOConsumer
    pointed at the OCI Streaming endpoint. Returns None for catch-up-only mode.
    """
    if not (
        OCI_STREAMING_BOOTSTRAP_SERVERS
        and OCI_STREAMING_TOPIC
        and OCI_STREAMING_USERNAME
        and OCI_STREAMING_AUTH_TOKEN
    ):
        return None

    return AIOConsumer(
        {
            "bootstrap.servers": OCI_STREAMING_BOOTSTRAP_SERVERS,
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": OCI_STREAMING_USERNAME,
            "sasl.password": OCI_STREAMING_AUTH_TOKEN,
            "group.id": OCI_STREAMING_GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )


@coco.lifespan
async def coco_lifespan(
    builder: coco.EnvironmentBuilder,
) -> AsyncIterator[None]:
    async with await asyncpg.create_pool(DATABASE_URL) as pool:
        builder.provide(PG_DB, pool)
        builder.provide(EMBEDDER, SentenceTransformerEmbedder(EMBED_MODEL))
        builder.provide(OCI_CLIENT, _build_oci_client())
        yield


@dataclass
class DocEmbedding:
    id: int
    filename: str
    chunk_start: int
    chunk_end: int
    text: str
    embedding: Annotated[NDArray, EMBEDDER]


@coco.fn
async def process_chunk(
    chunk: Chunk,
    filename: str,
    id_gen: IdGenerator,
    table: postgres.TableTarget[DocEmbedding],
) -> None:
    table.declare_row(
        row=DocEmbedding(
            id=await id_gen.next_id(chunk.text),
            filename=filename,
            chunk_start=chunk.start.char_offset,
            chunk_end=chunk.end.char_offset,
            text=chunk.text,
            embedding=await coco.use_context(EMBEDDER).embed(chunk.text),
        ),
    )


@coco.fn(memo=True)
async def process_file(
    file: oci_object_storage.OCIFile,
    table: postgres.TableTarget[DocEmbedding],
) -> None:
    text = await file.read_text()
    chunks = _splitter.split(
        text, chunk_size=2000, chunk_overlap=500, language="markdown"
    )
    id_gen = IdGenerator()
    await coco.map(process_chunk, chunks, file.file_path.path.as_posix(), id_gen, table)


@coco.fn
async def app_main() -> None:
    target_table = await postgres.mount_table_target(
        PG_DB,
        table_name=TABLE_NAME,
        table_schema=await postgres.TableSchema.from_class(
            DocEmbedding,
            primary_key=["id"],
        ),
        pg_schema_name=PG_SCHEMA_NAME,
    )

    client = coco.use_context(OCI_CLIENT)

    # If OCI Streaming is configured, build a LiveStream[bytes] from the topic
    # and pass it to list_objects to enable live updates. Otherwise the
    # walker yields a plain async iterable for catch-up scans only.
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


app = coco.App(
    coco.AppConfig(name="OCIObjectStorageEmbeddingV1"),
    app_main,
)


# ============================================================================
# Query demo (no vector index)
# ============================================================================


async def query_once(
    pool: asyncpg.Pool,
    embedder: SentenceTransformerEmbedder,
    query: str,
    *,
    top_k: int = TOP_K,
) -> None:
    query_vec = await embedder.embed(query)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT
                filename,
                text,
                embedding <=> $1 AS distance
            FROM "{PG_SCHEMA_NAME}"."{TABLE_NAME}"
            ORDER BY distance ASC
            LIMIT $2
            """,
            query_vec,
            top_k,
        )

    for r in rows:
        score = 1.0 - float(r["distance"])
        print(f"[{score:.3f}] {r['filename']}")
        print(f"    {r['text']}")
        print("---")


async def query() -> None:
    embedder = SentenceTransformerEmbedder(EMBED_MODEL)
    async with await asyncpg.create_pool(DATABASE_URL) as pool:
        if len(sys.argv) > 2:
            q = " ".join(sys.argv[2:])
            await query_once(pool, embedder, q)
            return

        while True:
            q = input("Enter search query (or Enter to quit): ").strip()
            if not q:
                break
            await query_once(pool, embedder, q)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "query":
        asyncio.run(query())
