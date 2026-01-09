"""
FastAPI wrapper for the v1 image search pipeline.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

try:
    from . import main as image_search
except ImportError:
    import importlib

    image_search = importlib.import_module("main")


@asynccontextmanager
async def lifespan(app: FastAPI):  # type: ignore[override]
    async with image_search.coco_aio.runtime():
        # Build/update the index once on startup so the collection exists.
        await image_search.app.run()
        yield


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
    client = image_search._state.client
    if client is None:
        raise RuntimeError("Qdrant client is not initialized.")

    results = image_search._qdrant_search(
        client,
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
