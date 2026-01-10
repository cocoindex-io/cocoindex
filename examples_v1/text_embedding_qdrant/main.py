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
from dataclasses import dataclass
from typing import AsyncIterator, Annotated

from numpy.typing import NDArray
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
QDRANT_CLIENT = coco.ContextKey[QdrantClient]("qdrant_client")

_embedder = SentenceTransformerEmbedder("sentence-transformers/all-MiniLM-L6-v2")
_splitter = RecursiveSplitter()


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
    embedding: Annotated[NDArray, _embedder]


@coco.function
def setup_collection(
    scope: coco.Scope,
) -> qdrant.TableTarget[DocEmbedding, coco.PendingS]:
    return scope.use(QDRANT_DB).declare_collection_target(
        scope,
        collection_name=QDRANT_COLLECTION,
        table_schema=qdrant.TableSchema(
            DocEmbedding,
            primary_key=["id"],
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
    builder.provide(QDRANT_DB, qdrant.register_db("text_embedding_qdrant", client))
    builder.provide(QDRANT_CLIENT, client)
    yield


@coco.function(memo=True)
async def process_chunk(
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
            embedding=await _embedder.embed_async(chunk.text),
        ),
    )


@coco.function(memo=True)
async def process_file(
    scope: coco.Scope,
    file: FileLike,
    table: qdrant.TableTarget[DocEmbedding],
) -> None:
    text = file.read_text()
    chunks = _splitter.split(
        text, chunk_size=2000, chunk_overlap=500, language="markdown"
    )
    await asyncio.gather(
        *(process_chunk(scope, file.relative_path, chunk, table) for chunk in chunks)
    )


@coco.function
def app_main(scope: coco.Scope, sourcedir: pathlib.Path) -> None:
    table = coco.mount_run(setup_collection, scope / "setup").result()

    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["*.md"]),
    )
    for f in files:
        coco.mount(process_file, scope / "file" / str(f.relative_path), f, table)


app = coco_aio.App(
    app_main,
    coco_aio.AppConfig(name="TextEmbeddingQdrantV1"),
    sourcedir=pathlib.Path("./markdown_files"),
)


# ============================================================================
# Query demo
# ============================================================================


async def query_once(client: QdrantClient, query: str, *, top_k: int = TOP_K) -> None:
    query_vec = await _embedder.embed_async(query)
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
    if len(sys.argv) > 1 and sys.argv[1] == "query":
        client = qdrant.create_client(QDRANT_URL, prefer_grpc=True)
        if len(sys.argv) > 2:
            q = " ".join(sys.argv[2:])
            await query_once(client, q)
            return

        while True:
            q = input("Enter search query (or Enter to quit): ").strip()
            if not q:
                break
            await query_once(client, q)
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
