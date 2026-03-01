"""
Google Drive Text Embedding (v1) - CocoIndex pipeline example.

- Read text files from Google Drive
- Chunk text (RecursiveSplitter)
- Embed chunks (SentenceTransformers)
- Store into Postgres with pgvector column (no vector index)
- Query demo using pgvector cosine distance (<=>)
"""

from __future__ import annotations

import asyncio
import os
import sys
from dataclasses import dataclass
from typing import AsyncIterator, Annotated

import asyncpg
from numpy.typing import NDArray

import cocoindex as coco
from cocoindex.connectors import google_drive, postgres
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder
from cocoindex.ops.text import RecursiveSplitter
from cocoindex.resources.chunk import Chunk
from cocoindex.resources.id import IdGenerator


DATABASE_URL = os.getenv(
    "POSTGRES_URL", "postgres://cocoindex:cocoindex@localhost/cocoindex"
)
TABLE_NAME = "doc_embeddings"
PG_SCHEMA_NAME = "coco_examples_v1"
TOP_K = 5


@dataclass
class _GlobalState:
    pool: asyncpg.Pool | None = None
    db: postgres.PgDatabase | None = None


_state = _GlobalState()
_embedder = SentenceTransformerEmbedder("sentence-transformers/all-MiniLM-L6-v2")
_splitter = RecursiveSplitter()


@coco.lifespan
async def coco_lifespan(
    builder: coco.EnvironmentBuilder,
) -> AsyncIterator[None]:
    async with await postgres.create_pool(DATABASE_URL) as pool:
        _state.pool = pool
        _state.db = postgres.register_db("gdrive_text_embedding_db", pool)
        yield


@dataclass
class DocEmbedding:
    id: int
    filename: str
    text: str
    embedding: Annotated[NDArray, _embedder]


@coco.fn(memo=True)
async def process_file(
    file: google_drive.DriveFile,
    table: postgres.TableTarget[DocEmbedding],
) -> None:
    text = await file.read_text()
    chunks = _splitter.split(
        text, chunk_size=2000, chunk_overlap=500, language="markdown"
    )
    id_gen = IdGenerator()
    await coco.map(_emit_chunk, chunks, file.file_path.path.as_posix(), id_gen, table)


@coco.fn
async def _emit_chunk(
    chunk: Chunk,
    filename: str,
    id_gen: IdGenerator,
    table: postgres.TableTarget[DocEmbedding],
) -> None:
    table.declare_row(
        row=DocEmbedding(
            id=await id_gen.next_id(chunk.text),
            filename=filename,
            text=chunk.text,
            embedding=await _embedder.embed(chunk.text),
        ),
    )


@coco.fn
async def app_main() -> None:
    assert _state.db is not None

    table = await _state.db.mount_table_target(
        table_name=TABLE_NAME,
        table_schema=await postgres.TableSchema.from_class(
            DocEmbedding,
            primary_key=["id"],
        ),
        pg_schema_name=PG_SCHEMA_NAME,
    )

    credential_path = os.environ["GOOGLE_SERVICE_ACCOUNT_CREDENTIAL"]
    root_folder_ids = [
        folder.strip()
        for folder in os.environ["GOOGLE_DRIVE_ROOT_FOLDER_IDS"].split(",")
        if folder.strip()
    ]

    source = google_drive.GoogleDriveSource(
        service_account_credential_path=credential_path,
        root_folder_ids=root_folder_ids,
    )

    await coco.mount_each(process_file, source.items(), table)


app = coco.App(
    coco.AppConfig(name="GoogleDriveTextEmbeddingV1"),
    app_main,
)


async def query_once(query: str, *, top_k: int = TOP_K) -> None:
    query_vec = await _embedder.embed(query)
    pool = _state.pool
    assert pool is not None

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
    async with coco.runtime():
        if len(sys.argv) > 2:
            q = " ".join(sys.argv[2:])
            await query_once(q)
            return

        while True:
            q = input("Enter search query (or Enter to quit): ").strip()
            if not q:
                break
            await query_once(q)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "query":
        asyncio.run(query())
