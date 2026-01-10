"""
FastAPI wrapper for the v1 image search pipeline.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from qdrant_client import QdrantClient

import cocoindex.asyncio as coco_aio
from cocoindex.connectors import qdrant

try:
    from . import main as image_search
except ImportError:
    import importlib

    image_search = importlib.import_module("main")


# Module-level client reference for API endpoints
_client: QdrantClient | None = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:  # type: ignore[override]
    global _client
    async with coco_aio.runtime():
        # Initialize client for API endpoints
        _client = qdrant.create_client(image_search.QDRANT_URL, prefer_grpc=True)
        # Build/update the index once on startup so the collection exists.
        await image_search.app.run()
        yield
        _client = None


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve images from the local img folder
app.mount("/img", StaticFiles(directory="img"), name="img")


@app.get("/search")
async def search(
    q: str = Query(..., description="Search query"),
    limit: int = Query(5, description="Number of results"),
) -> dict[str, Any]:
    query_embedding = image_search.embed_query(q)

    if _client is None:
        raise RuntimeError("Qdrant client is not initialized.")

    results = image_search._qdrant_search(
        _client,
        image_search.QDRANT_COLLECTION,
        query_embedding,
        limit,
    )

    return {
        "results": [
            {
                "filename": (r.payload or {}).get("filename"),
                "score": r.score,
            }
            for r in results
        ]
    }
