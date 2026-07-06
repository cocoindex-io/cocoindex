"""
Azure Blob Storage text embedding example.

Index Markdown blobs:
    cocoindex update main

Query the index:
    python main.py "your query"
"""

from __future__ import annotations

import asyncio
import os
import sys
from dataclasses import dataclass
from typing import Annotated, AsyncIterator

import asyncpg
from azure.identity.aio import DefaultAzureCredential
from azure.storage.blob.aio import ContainerClient
from dotenv import load_dotenv
from numpy.typing import NDArray
from pgvector.asyncpg import register_vector

import cocoindex as coco
from cocoindex.connectors import azure_blob, postgres
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder
from cocoindex.ops.text import RecursiveSplitter
from cocoindex.resources.chunk import Chunk
from cocoindex.resources.file import PatternFilePathMatcher
from cocoindex.resources.id import IdGenerator


load_dotenv()

DATABASE_URL = os.getenv(
    "POSTGRES_URL", "postgres://cocoindex:cocoindex@localhost/cocoindex"
)
TABLE_NAME = "azure_blob_doc_embeddings"
PG_SCHEMA_NAME = "coco_examples"
TOP_K = 5

AZURE_STORAGE_ACCOUNT = os.environ["AZURE_STORAGE_ACCOUNT"]
AZURE_STORAGE_CONTAINER = os.environ["AZURE_STORAGE_CONTAINER"]
AZURE_BLOB_PREFIX = os.getenv("AZURE_BLOB_PREFIX", "")

EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
PG_DB = coco.ContextKey[asyncpg.Pool]("azure_blob_embedding_db")
AZURE_CONTAINER_CLIENT = coco.ContextKey[ContainerClient]("azure_container_client")
EMBEDDER = coco.ContextKey[SentenceTransformerEmbedder]("embedder", detect_change=True)

_splitter = RecursiveSplitter()


@coco.lifespan
async def coco_lifespan(
    builder: coco.EnvironmentBuilder,
) -> AsyncIterator[None]:
    credential = DefaultAzureCredential()
    try:
        account_url = f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net"
        async with asyncpg.create_pool(DATABASE_URL) as pool:
            builder.provide(PG_DB, pool)
            builder.provide(EMBEDDER, SentenceTransformerEmbedder(EMBED_MODEL))

            async with ContainerClient(
                account_url=account_url,
                container_name=AZURE_STORAGE_CONTAINER,
                credential=credential,
            ) as container_client:
                builder.provide(AZURE_CONTAINER_CLIENT, container_client)
                yield
    finally:
        await credential.close()


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
    file: azure_blob.AzureBlobFile,
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

    client = coco.use_context(AZURE_CONTAINER_CLIENT)
    files = azure_blob.list_blobs(
        client,
        prefix=AZURE_BLOB_PREFIX,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md"]),
    )
    await coco.mount_each(process_file, files.items(), target_table)


app = coco.App(
    coco.AppConfig(name="AzureBlobEmbeddingV1"),
    app_main,
)


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


async def query(initial_query: str | None = None) -> None:
    embedder = SentenceTransformerEmbedder(EMBED_MODEL)
    async with asyncpg.create_pool(DATABASE_URL, init=register_vector) as pool:
        if initial_query is not None:
            await query_once(pool, embedder, initial_query)
            return

        while True:
            q = input("Enter search query (or Enter to quit): ").strip()
            if not q:
                break
            await query_once(pool, embedder, q)


if __name__ == "__main__":
    initial = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else None
    asyncio.run(query(initial))
