"""Shared helpers for image search examples."""

from __future__ import annotations

import pathlib
import sys
import uuid
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Callable

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from qdrant_client import QdrantClient
from qdrant_client.http import models as qdrant_models

import cocoindex as coco
from cocoindex.connectors import localfs, qdrant
from cocoindex.resources.file import PatternFilePathMatcher

IMAGE_PATTERNS = ["**/*.jpg", "**/*.jpeg", "**/*.png"]


def image_id(path: pathlib.PurePath) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, str(path)))


def walk_image_files(sourcedir: pathlib.Path) -> Any:
    return localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=IMAGE_PATTERNS),
    )


def provide_qdrant_client(
    builder: coco.EnvironmentBuilder,
    qdrant_url: str,
    *keys: coco.ContextKey[QdrantClient],
) -> None:
    client = qdrant.create_client(qdrant_url, prefer_grpc=True)
    for key in keys:
        builder.provide(key, client)


def qdrant_search(
    client: QdrantClient,
    collection_name: str,
    query_vector: Any,
    limit: int,
    *,
    require_query_points: bool = False,
) -> list[qdrant_models.ScoredPoint]:
    if require_query_points:
        if not hasattr(client, "query_points"):
            raise RuntimeError("qdrant-client must support query_points for ColPali.")
        response = client.query_points(
            collection_name=collection_name,
            query=query_vector,
            limit=limit,
            with_payload=True,
        )
        return response.points

    if hasattr(client, "search"):
        return client.search(
            collection_name=collection_name,
            query_vector=query_vector,
            limit=limit,
            with_payload=True,
        )
    if hasattr(client, "query_points"):
        response = client.query_points(
            collection_name=collection_name,
            query=query_vector,
            limit=limit,
            with_payload=True,
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


def print_search_results(results: list[qdrant_models.ScoredPoint]) -> None:
    for result in results:
        payload = result.payload or {}
        print(f"[{result.score:.3f}] {payload.get('filename', '<unknown>')}")
        print("---")


def query_loop(
    qdrant_url: str,
    query_once: Callable[[QdrantClient, str], None],
) -> None:
    client = qdrant.create_client(qdrant_url, prefer_grpc=True)
    if len(sys.argv) > 1:
        query_once(client, " ".join(sys.argv[1:]))
        return

    while True:
        query = input("Enter search query (or Enter to quit): ").strip()
        if not query:
            break
        query_once(client, query)


def create_search_api(image_search: Any) -> FastAPI:
    client: QdrantClient | None = None

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:  # type: ignore[override]
        nonlocal client
        async with coco.runtime():
            client = qdrant.create_client(image_search.QDRANT_URL, prefer_grpc=True)
            await coco.show_progress(image_search.app.update())
            yield
            client = None

    app = FastAPI(lifespan=lifespan)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.mount("/img", StaticFiles(directory="img"), name="img")

    @app.get("/search")
    async def search(
        q: str = Query(..., description="Search query"),
        limit: int = Query(5, description="Number of results"),
    ) -> dict[str, Any]:
        if client is None:
            raise RuntimeError("Qdrant client is not initialized.")

        results = image_search.search_qdrant(client, image_search.embed_query(q), limit)
        return {
            "results": [
                {
                    "filename": (result.payload or {}).get("filename"),
                    "score": result.score,
                }
                for result in results
            ]
        }

    return app
