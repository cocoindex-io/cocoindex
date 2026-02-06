"""
Text Embedding (v1) - CocoIndex pipeline example.

- Walk local markdown files
- Chunk text (RecursiveSplitter)
- Embed chunks (SentenceTransformers)
- Store into Postgres with pgvector column (no vector index)
- Query demo using pgvector cosine distance (<=>)
"""

from __future__ import annotations

import asyncio
import os
import pathlib
import sys
from dataclasses import dataclass
from typing import AsyncIterator, Annotated

import asyncpg
from numpy.typing import NDArray

import cocoindex as coco
import cocoindex.asyncio as coco_aio
from cocoindex.connectors import localfs, postgres
from cocoindex.ops.text import RecursiveSplitter
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder
from cocoindex.resources.chunk import Chunk
from cocoindex.resources.file import FileLike, PatternFilePathMatcher
from cocoindex.resources.id import IdGenerator


DATABASE_URL = os.getenv(
    "POSTGRES_URL", "postgres://cocoindex:cocoindex@localhost/cocoindex"
)
TABLE_NAME = "doc_embeddings"
PG_SCHEMA_NAME = "coco_examples"
TOP_K = 5


PG_DB = coco.ContextKey[postgres.PgDatabase]("pg_db")

_embedder = SentenceTransformerEmbedder("sentence-transformers/all-MiniLM-L6-v2")
_splitter = RecursiveSplitter()


@coco_aio.lifespan
async def coco_lifespan(
    builder: coco_aio.EnvironmentBuilder,
) -> AsyncIterator[None]:
    # Provide resources needed across the CocoIndex environment
    async with await postgres.create_pool(DATABASE_URL) as pool:
        builder.provide(PG_DB, postgres.register_db("text_embedding_db", pool))
        yield


@dataclass
class DocEmbedding:
    id: int
    filename: str
    chunk_start: int
    chunk_end: int
    text: str
    embedding: Annotated[NDArray, _embedder]


@coco.function(memo=True)
async def process_chunk(
    id: int,
    filename: pathlib.PurePath,
    chunk: Chunk,
    table: postgres.TableTarget[DocEmbedding],
) -> None:
    table.declare_row(
        row=DocEmbedding(
            id=id,
            filename=str(filename),
            chunk_start=chunk.start.char_offset,
            chunk_end=chunk.end.char_offset,
            text=chunk.text,
            embedding=await _embedder.embed(chunk.text),
        ),
    )


@coco.function(memo=True)
async def process_file(
    file: FileLike,
    table: postgres.TableTarget[DocEmbedding],
) -> None:
    text = file.read_text()
    chunks = _splitter.split(
        text, chunk_size=2000, chunk_overlap=500, language="markdown"
    )
    id_gen = IdGenerator()
    await asyncio.gather(
        *(
            process_chunk(id_gen.next_id(chunk.text), file.file_path.path, chunk, table)
            for chunk in chunks
        )
    )


@coco.function
def app_main(sourcedir: pathlib.Path) -> None:
    target_db = coco.use_context(PG_DB)
    target_table = coco.mount_run(
        coco.component_subpath("setup", "table"),
        target_db.declare_table_target,
        table_name=TABLE_NAME,
        table_schema=postgres.TableSchema(
            DocEmbedding,
            primary_key=["id"],
        ),
        pg_schema_name=PG_SCHEMA_NAME,
    ).result()

    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["*.md"]),
    )
    with coco.component_subpath("file"):
        for f in files:
            coco.mount(
                coco.component_subpath(str(f.file_path.path)),
                process_file,
                f,
                target_table,
            )


app = coco_aio.App(
    coco_aio.AppConfig(name="TextEmbeddingV1"),
    app_main,
    sourcedir=pathlib.Path("./markdown_files"),
)


# ============================================================================
# Query demo (no vector index)
# ============================================================================


async def query_once(pool: asyncpg.Pool, query: str, *, top_k: int = TOP_K) -> None:
    query_vec = await _embedder.embed(query)
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
    async with await postgres.create_pool(DATABASE_URL) as pool:
        if len(sys.argv) > 2:
            q = " ".join(sys.argv[2:])
            await query_once(pool, q)
            return

        while True:
            q = input("Enter search query (or Enter to quit): ").strip()
            if not q:
                break
            await query_once(pool, q)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "query":
        asyncio.run(query())
