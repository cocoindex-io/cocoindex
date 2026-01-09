"""
Text Embedding with LanceDB (v1) - CocoIndex pipeline example.

- Walk local markdown files
- Chunk text (RecursiveSplitter)
- Embed chunks (SentenceTransformers)
- Store into LanceDB with vector column and FTS
- Query demo using LanceDB native vector search
"""

from __future__ import annotations

import asyncio
import functools
import pathlib
import sys
import threading
from dataclasses import dataclass
from typing import AsyncIterator

import numpy as np
from numpy.typing import NDArray
from sentence_transformers import SentenceTransformer

import cocoindex as coco
import cocoindex.asyncio as coco_aio
from cocoindex.connectors import lancedb, localfs
from cocoindex.extras.text import RecursiveSplitter
from cocoindex.resources.chunk import Chunk
from cocoindex.resources.file import FileLike, PatternFilePathMatcher
from cocoindex.resources.schema import FtsSpec, VectorSpec


LANCEDB_URI = "./lancedb_data"
TABLE_NAME = "doc_embeddings"
EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
TOP_K = 5


@dataclass
class _GlobalState:
    db: lancedb.LanceDatabase | None = None


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
) -> lancedb.TableTarget[DocEmbedding, coco.PendingS]:
    assert _state.db is not None

    dim = embedder().get_sentence_embedding_dimension()
    if dim is None:
        raise RuntimeError(f"Embedding dimension is unknown for model {EMBED_MODEL}.")
    return _state.db.declare_table_target(
        scope,
        table_name=TABLE_NAME,
        table_schema=lancedb.TableSchema(
            DocEmbedding,
            primary_key=["filename", "chunk_start"],
            column_specs={
                "embedding": VectorSpec(dim),
                "text": FtsSpec(tokenizer="simple"),
            },
        ),
    )


# ============================================================================
# CocoIndex environment + app
# ============================================================================


@coco_aio.lifespan
async def coco_lifespan(
    builder: coco_aio.EnvironmentBuilder,
) -> AsyncIterator[None]:
    builder.settings.db_path = pathlib.Path("./cocoindex.db")

    # Register LanceDB connection
    await lancedb._register_db_async("text_embedding_db", LANCEDB_URI)
    _state.db = lancedb.LanceDatabase("text_embedding_db")
    yield


@coco.function
def process_chunk(
    scope: coco.Scope,
    filename: pathlib.PurePath,
    chunk: Chunk,
    table: lancedb.TableTarget[DocEmbedding],
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
    table: lancedb.TableTarget[DocEmbedding],
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
    coco_aio.AppConfig(name="TextEmbeddingLanceDBV1"),
    sourcedir=pathlib.Path("./markdown_files"),
)


# ============================================================================
# Query demo
# ============================================================================


async def query_once(query: str, *, top_k: int = TOP_K) -> None:
    query_vec = await asyncio.to_thread(embed_text, query)

    assert _state.db is not None
    conn = lancedb._get_connection(_state.db.key)
    table = await conn.open_table(TABLE_NAME)

    search = await table.search(query_vec, vector_column_name="embedding")
    results = await search.limit(top_k).to_list()

    for r in results:
        score = 1.0 - r["_distance"]
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
