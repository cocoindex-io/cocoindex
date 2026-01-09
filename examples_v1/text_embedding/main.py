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
import functools
import pathlib
import sys
import threading
from dataclasses import dataclass
from typing import AsyncIterator

import asyncpg
import numpy as np
from numpy.typing import NDArray
from sentence_transformers import SentenceTransformer

import cocoindex as coco
import cocoindex.asyncio as coco_aio
from cocoindex.connectors import localfs, postgres
from cocoindex.extras.text import RecursiveSplitter
from cocoindex.resources.file import FileLike, PatternFilePathMatcher
from cocoindex.resources.chunk import Chunk
from cocoindex.resources.schema import VectorSpec


DATABASE_URL = "postgres://cocoindex:cocoindex@localhost/cocoindex"
TABLE_NAME = "doc_embeddings"
PG_SCHEMA_NAME = "coco_examples"
EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
TOP_K = 5


@dataclass
class _GlobalState:
    pool: asyncpg.Pool | None = None
    db: postgres.PgDatabase | None = None


_state = _GlobalState()
_splitter = RecursiveSplitter()


@functools.cache
def embedder() -> SentenceTransformer:
    # The model is cached in-process so repeated runs are fast.
    return SentenceTransformer(EMBED_MODEL)


_gpu_lock = threading.Lock()


def embed_text(text: str) -> NDArray[np.float32]:
    # TODO: convert to a cocoindex function with GPU and batching support
    with _gpu_lock:
        return embedder().encode(
            [text], convert_to_numpy=True, normalize_embeddings=True
        )[0]


# ============================================================================
# Table schema
# ============================================================================


@dataclass
class DocEmbedding:
    filename: str
    chunk_start: int
    chunk_end: int
    text: str
    embedding: NDArray[np.float32]


@coco.function
def setup_table(
    scope: coco.Scope,
) -> postgres.TableTarget[DocEmbedding, coco.PendingS]:
    assert _state.db is not None

    dim = embedder().get_sentence_embedding_dimension()
    if dim is None:
        raise RuntimeError(f"Embedding dimension is unknown for model {EMBED_MODEL}.")
    return _state.db.declare_table_target(
        scope,
        table_name=TABLE_NAME,
        table_schema=postgres.TableSchema(
            DocEmbedding,
            primary_key=["filename", "chunk_start"],
            column_specs={"embedding": VectorSpec(dim)},
        ),
        pg_schema_name=PG_SCHEMA_NAME,
    )


# ============================================================================
# CocoIndex environment + app
# ============================================================================


@coco_aio.lifespan
async def coco_lifespan(
    builder: coco_aio.EnvironmentBuilder,
) -> AsyncIterator[None]:
    builder.settings.db_path = pathlib.Path("./cocoindex.db")

    # register_vector is needed for query
    async with await postgres.create_pool(DATABASE_URL) as pool:
        _state.pool = pool
        _state.db = postgres.register_db("text_embedding_db", pool)
        yield


@coco.function
def process_chunk(
    scope: coco.Scope,
    filename: pathlib.PurePath,
    chunk: Chunk,
    table: postgres.TableTarget[DocEmbedding],
) -> None:
    table.declare_row(
        scope,
        row=DocEmbedding(
            filename=str(filename),
            chunk_start=chunk.start.char_offset,
            chunk_end=chunk.end.char_offset,
            text=chunk.text,
            embedding=embed_text(chunk.text),
        ),
    )


@coco.function(memo=True)
def process_file(
    scope: coco.Scope,
    file: FileLike,
    table: postgres.TableTarget[DocEmbedding],
) -> None:
    text = file.read_text()
    chunks = _splitter.split(
        text, chunk_size=2000, chunk_overlap=500, language="markdown"
    )
    # TODO: Process chunks in parallel
    for chunk in chunks:
        process_chunk(scope, file.relative_path, chunk, table)


@coco.function
async def app_main(scope: coco.Scope, sourcedir: pathlib.Path) -> None:
    table = await coco_aio.mount_run(setup_table, scope / "setup").result()

    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["*.md"]),
    )
    for f in files:
        coco_aio.mount(process_file, scope / "file" / str(f.relative_path), f, table)


app = coco_aio.App(
    app_main,
    coco_aio.AppConfig(name="TextEmbeddingV1"),
    sourcedir=pathlib.Path("./markdown_files"),
)


# ============================================================================
# Query demo (no vector index)
# ============================================================================


async def query_once(query: str, *, top_k: int = TOP_K) -> None:
    query_vec = await asyncio.to_thread(embed_text, query)
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


async def main() -> None:
    async with coco_aio.runtime():
        if len(sys.argv) > 1 and sys.argv[1] == "query":
            if len(sys.argv) > 2:
                q = " ".join(sys.argv[2:])
                await query_once(q)
                return

            while True:
                q = input("Enter search query (or Enter to quit): ").strip()
                if not q:
                    break
                await query_once(q)
            return

        await app.run()


if __name__ == "__main__":
    asyncio.run(main())
