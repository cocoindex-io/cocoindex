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
import cocoindex.asyncio as coco_aio
from cocoindex.connectors import google_drive, postgres
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder
from cocoindex.ops.text import RecursiveSplitter
from cocoindex.resources.chunk import Chunk


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


@coco_aio.lifespan
async def coco_lifespan(
    builder: coco_aio.EnvironmentBuilder,
) -> AsyncIterator[None]:
    async with await postgres.create_pool(DATABASE_URL) as pool:
        _state.pool = pool
        _state.db = postgres.register_db("gdrive_text_embedding_db", pool)
        yield


@dataclass
class DocEmbedding:
    filename: str
    location: str
    text: str
    embedding: Annotated[NDArray, _embedder]


@coco.function(memo=True)
async def process_file(
    scope: coco.Scope,
    file: google_drive.DriveFile,
    table: postgres.TableTarget[DocEmbedding],
) -> None:
    text = file.read_text()
    chunks = _splitter.split(
        text, chunk_size=2000, chunk_overlap=500, language="markdown"
    )
    await asyncio.gather(
        *(
            _emit_chunk(scope, file.relative_path.as_posix(), chunk, table)
            for chunk in chunks
        )
    )


@coco.function(memo=True)
async def _emit_chunk(
    scope: coco.Scope,
    filename: str,
    chunk: Chunk,
    table: postgres.TableTarget[DocEmbedding],
) -> None:
    location = f"{chunk.start.char_offset}:{chunk.end.char_offset}"
    table.declare_row(
        scope,
        row=DocEmbedding(
            filename=filename,
            location=location,
            text=chunk.text,
            embedding=await _embedder.embed_async(chunk.text),
        ),
    )


@coco.function
def app_main(scope: coco.Scope) -> None:
    assert _state.db is not None
    table = coco.mount_run(
        lambda inner_scope: _state.db.declare_table_target(
            inner_scope,
            table_name=TABLE_NAME,
            table_schema=postgres.TableSchema(
                DocEmbedding,
                primary_key=["filename", "location"],
            ),
            pg_schema_name=PG_SCHEMA_NAME,
        ),
        scope / "setup",
    ).result()

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

    for file in source.files():
        coco.mount(
            process_file,
            scope / "file" / file.relative_path.as_posix(),
            file,
            table,
        )


app = coco_aio.App(
    app_main,
    coco_aio.AppConfig(name="GoogleDriveTextEmbeddingV1"),
)


async def query_once(query: str, *, top_k: int = TOP_K) -> None:
    query_vec = await _embedder.embed_async(query)
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
    async with coco_aio.runtime():
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
