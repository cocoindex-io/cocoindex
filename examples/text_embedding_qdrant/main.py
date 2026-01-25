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
import pathlib
import sys
import uuid
from typing import AsyncIterator

from qdrant_client import QdrantClient
from qdrant_client.http import models as qdrant_models

import cocoindex as coco
import cocoindex.asyncio as coco_aio
from cocoindex.connectors import localfs, qdrant
from cocoindex.extras.text import RecursiveSplitter
from cocoindex.extras.sentence_transformers import SentenceTransformerEmbedder
from cocoindex.resources.chunk import Chunk
from cocoindex.resources.file import FileLike, PatternFilePathMatcher


QDRANT_URL = "http://localhost:6334"
QDRANT_COLLECTION = "TextEmbedding"
TOP_K = 5


QDRANT_DB = coco.ContextKey[qdrant.QdrantDatabase]("qdrant_db")

_embedder = SentenceTransformerEmbedder("sentence-transformers/all-MiniLM-L6-v2")
_splitter = RecursiveSplitter()


@coco_aio.lifespan
async def coco_lifespan(
    builder: coco_aio.EnvironmentBuilder,
) -> AsyncIterator[None]:
    # For CocoIndex internal states
    builder.settings.db_path = pathlib.Path("./cocoindex.db")
    # Provide resources needed across the CocoIndex environment
    client = qdrant.create_client(QDRANT_URL, prefer_grpc=True)
    builder.provide(QDRANT_DB, qdrant.register_db("text_embedding_qdrant", client))
    yield


@coco.function(memo=True)
async def process_chunk(
    scope: coco.Scope,
    filename: pathlib.PurePath,
    chunk: Chunk,
    target: qdrant.CollectionTarget,
) -> None:
    chunk_id = _chunk_id(filename, chunk)
    embedding_vec = await _embedder.embed_async(chunk.text)

    point = qdrant.PointStruct(
        id=chunk_id,
        vector=embedding_vec.tolist(),
        payload={
            "filename": str(filename),
            "chunk_start": chunk.start.char_offset,
            "chunk_end": chunk.end.char_offset,
            "text": chunk.text,
        },
    )
    target.declare_point(scope, point)


@coco.function(memo=True)
async def process_file(
    scope: coco.Scope,
    file: FileLike,
    target: qdrant.CollectionTarget,
) -> None:
    text = file.read_text()
    chunks = _splitter.split(
        text, chunk_size=2000, chunk_overlap=500, language="markdown"
    )
    await asyncio.gather(
        *(process_chunk(scope, file.relative_path, chunk, target) for chunk in chunks)
    )


@coco.function
def app_main(scope: coco.Scope, sourcedir: pathlib.Path) -> None:
    target_db = scope.use(QDRANT_DB)
    target_collection_handle = coco.mount_run(
        target_db.declare_collection_target,
        scope / "setup" / "collection",
        collection_name=QDRANT_COLLECTION,
        schema=qdrant.CollectionSchema(
            vectors=qdrant.QdrantVectorDef(schema=_embedder)
        ),
    )
    target_collection = target_collection_handle.result()
    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["*.md"]),
    )
    for f in files:
        coco.mount(
            process_file, scope / "file" / str(f.relative_path), f, target_collection
        )


app = coco_aio.App(
    app_main,
    coco_aio.AppConfig(name="TextEmbeddingQdrantV1"),
    sourcedir=pathlib.Path("./markdown_files"),
)


# ============================================================================
# Query demo
# ============================================================================


def query_once(client: QdrantClient, query: str, *, top_k: int = TOP_K) -> None:
    query_vec = _embedder.embed(query)
    results = _qdrant_search(client, QDRANT_COLLECTION, query_vec.tolist(), top_k)

    for r in results:
        payload = r.payload or {}
        print(f"[{r.score:.3f}] {payload.get('filename', '<unknown>')}")
        print(f"    {payload.get('text', '')}")
        print("---")


def query() -> None:
    client = qdrant.create_client(QDRANT_URL, prefer_grpc=True)
    if len(sys.argv) > 1:
        q = " ".join(sys.argv[1:])
        query_once(client, q)
        return

    while True:
        q = input("Enter search query (or Enter to quit): ").strip()
        if not q:
            break
        query_once(client, q)


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
            query_vector=query_vector,
            limit=limit,
        )
    if hasattr(client, "query_points"):
        response = client.query_points(
            collection_name=collection_name,
            query=query_vector,
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
    query()
