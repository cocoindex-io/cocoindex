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
from typing import AsyncIterator

from cocoindex.resources.id import IdGenerator
from qdrant_client import QdrantClient
from qdrant_client.http import models as qdrant_models

import cocoindex as coco
from cocoindex.connectors import localfs, qdrant
from cocoindex.ops.text import RecursiveSplitter
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder
from cocoindex.resources.chunk import Chunk
from cocoindex.resources.file import AsyncFileLike, PatternFilePathMatcher


QDRANT_URL = "http://localhost:6334"
QDRANT_COLLECTION = "TextEmbedding"
TOP_K = 5


QDRANT_DB = coco.ContextKey[qdrant.QdrantDatabase]("qdrant_db")

_embedder = SentenceTransformerEmbedder("sentence-transformers/all-MiniLM-L6-v2")
_splitter = RecursiveSplitter()


@coco.lifespan
async def coco_lifespan(
    builder: coco.EnvironmentBuilder,
) -> AsyncIterator[None]:
    # Provide resources needed across the CocoIndex environment
    client = qdrant.create_client(QDRANT_URL, prefer_grpc=True)
    builder.provide(QDRANT_DB, qdrant.register_db("text_embedding_qdrant", client))
    yield


@coco.function
async def process_chunk(
    chunk: Chunk,
    filename: pathlib.PurePath,
    id_gen: IdGenerator,
    target: qdrant.CollectionTarget,
) -> None:
    embedding_vec = await _embedder.embed(chunk.text)

    point = qdrant.PointStruct(
        id=await id_gen.next_id(chunk.text),
        vector=embedding_vec.tolist(),
        payload={
            "filename": str(filename),
            "chunk_start": chunk.start.char_offset,
            "chunk_end": chunk.end.char_offset,
            "text": chunk.text,
        },
    )
    target.declare_point(point)


@coco.function(memo=True)
async def process_file(
    file: AsyncFileLike,
    target: qdrant.CollectionTarget,
) -> None:
    text = await file.read_text()
    chunks = _splitter.split(
        text, chunk_size=2000, chunk_overlap=500, language="markdown"
    )
    id_gen = IdGenerator()
    await coco.map(process_chunk, chunks, file.file_path.path, id_gen, target)


@coco.function
async def app_main(sourcedir: pathlib.Path) -> None:
    target_db = coco.use_context(QDRANT_DB)
    target_collection = await target_db.mount_collection_target(
        collection_name=QDRANT_COLLECTION,
        schema=await qdrant.CollectionSchema.create(
            vectors=qdrant.QdrantVectorDef(schema=_embedder)
        ),
    )
    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md"]),
    )
    await coco.mount_each(process_file, files.items(), target_collection)


app = coco.App(
    coco.AppConfig(name="TextEmbeddingQdrantV1"),
    app_main,
    sourcedir=pathlib.Path("./markdown_files"),
)


# ============================================================================
# Query demo
# ============================================================================


async def query_once(client: QdrantClient, query: str, *, top_k: int = TOP_K) -> None:
    query_vec = await _embedder.embed(query)
    results = _qdrant_search(client, QDRANT_COLLECTION, query_vec.tolist(), top_k)

    for r in results:
        payload = r.payload or {}
        print(f"[{r.score:.3f}] {payload.get('filename', '<unknown>')}")
        print(f"    {payload.get('text', '')}")
        print("---")


async def query() -> None:
    client = qdrant.create_client(QDRANT_URL, prefer_grpc=True)
    if len(sys.argv) > 1:
        q = " ".join(sys.argv[1:])
        await query_once(client, q)
        return

    while True:
        q = input("Enter search query (or Enter to quit): ").strip()
        if not q:
            break
        await query_once(client, q)


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
    asyncio.run(query())
