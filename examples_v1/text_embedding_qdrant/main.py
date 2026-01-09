"""
Text Embedding with Qdrant (v1) - CocoIndex pipeline example.

- Walk local markdown files
- Chunk text (RecursiveSplitter)
- Embed chunks (SentenceTransformers)
- Store into Qdrant collection
- Query demo using Qdrant vector search
"""

from __future__ import annotations

import asyncio
import functools
import pathlib
import sys
import threading
import uuid
from dataclasses import dataclass
from typing import AsyncIterator

import numpy as np
from numpy.typing import NDArray
from qdrant_client import QdrantClient
from qdrant_client.http import models as qdrant_models
from sentence_transformers import SentenceTransformer

import cocoindex as coco
import cocoindex.asyncio as coco_aio
from cocoindex.connectors import localfs, qdrant
from cocoindex.extras.text import RecursiveSplitter
from cocoindex.resources.chunk import Chunk
from cocoindex.resources.file import FileLike, PatternFilePathMatcher


QDRANT_URL = "http://localhost:6334"
QDRANT_COLLECTION = "TextEmbedding"
EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
TOP_K = 5


@dataclass
class _GlobalState:
    db: qdrant.QdrantDatabase | None = None
    client: QdrantClient | None = None


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
    id: str
    filename: str
    chunk_start: int
    chunk_end: int
    text: str
    embedding: NDArray[np.float32]


@coco.function
def setup_collection(
    scope: coco.Scope,
) -> qdrant.TableTarget[DocEmbedding, coco.PendingS]:
    assert _state.db is not None

    dim = embedder().get_sentence_embedding_dimension()
    if dim is None:
        raise RuntimeError(f"Embedding dimension is unknown for model {EMBED_MODEL}.")
    return _state.db.declare_collection_target(
        scope,
        collection_name=QDRANT_COLLECTION,
        table_schema=qdrant.TableSchema(
            DocEmbedding,
            primary_key=["id"],
            column_specs={"embedding": qdrant.QdrantVectorSpec(dim, distance="cosine")},
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

    client = qdrant.create_client(QDRANT_URL, prefer_grpc=True)
    _state.client = client
    _state.db = qdrant.register_db("text_embedding_qdrant", client)
    yield


@coco.function
def process_chunk(
    scope: coco.Scope,
    filename: pathlib.PurePath,
    chunk: Chunk,
    table: qdrant.TableTarget[DocEmbedding],
) -> None:
    chunk_id = _chunk_id(filename, chunk)
    table.declare_row(
        scope,
        row=DocEmbedding(
            id=chunk_id,
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
    table: qdrant.TableTarget[DocEmbedding],
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
    table = await coco_aio.mount_run(setup_collection, scope / "setup").result()

    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["*.md"]),
    )
    for f in files:
        coco_aio.mount(process_file, scope / "file" / str(f.relative_path), f, table)


app = coco_aio.App(
    app_main,
    coco_aio.AppConfig(name="TextEmbeddingQdrantV1"),
    sourcedir=pathlib.Path("./markdown_files"),
)


# ============================================================================
# Query demo
# ============================================================================


async def query_once(query: str, *, top_k: int = TOP_K) -> None:
    query_vec = await asyncio.to_thread(embed_text, query)
    client = _state.client
    assert client is not None

    results = await asyncio.to_thread(
        _qdrant_search,
        client,
        QDRANT_COLLECTION,
        query_vec.tolist(),
        top_k,
    )

    for r in results:
        payload = r.payload or {}
        print(f"[{r.score:.3f}] {payload.get('filename', '<unknown>')}")
        print(f"    {payload.get('text', '')}")
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


def _chunk_id(filename: pathlib.PurePath, chunk: Chunk) -> str:
    raw = f"{filename}:{chunk.start.char_offset}-{chunk.end.char_offset}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, raw))


def _qdrant_search(
    client: QdrantClient,
    collection_name: str,
    query_vector: list[float],
    limit: int,
) -> list[qdrant_models.ScoredPoint]:
    # qdrant-client has different search APIs across versions; pick what's available.
    if hasattr(client, "search"):
        return client.search(
            collection_name=collection_name,
            query_vector=("embedding", query_vector),
            limit=limit,
        )
    if hasattr(client, "query_points"):
        response = client.query_points(
            collection_name=collection_name,
            query=query_vector,
            using="embedding",
            limit=limit,
        )
        return response.points
    if hasattr(client, "search_points"):
        response = client.search_points(
            collection_name=collection_name,
            vector=query_vector,
            limit=limit,
            with_payload=True,
        )
        return response.result
    raise RuntimeError("Unsupported qdrant-client version: no search method found.")


if __name__ == "__main__":
    asyncio.run(main())
