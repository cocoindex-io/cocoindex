"""
FastAPI wrapper for the v1 ColPali image search pipeline.

Runs the index in live mode in the background, so the FastAPI server keeps
the Qdrant collection in sync with `img/` while serving search requests.
"""

from __future__ import annotations

import asyncio
import contextlib
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from dotenv import load_dotenv
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from qdrant_client import QdrantClient

import cocoindex as coco
from cocoindex.connectors import qdrant

import pipeline

load_dotenv()


_client: QdrantClient | None = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:  # type: ignore[override]
    global _client
    async with coco.runtime():
        _client = qdrant.create_client(pipeline.qdrant_url(), prefer_grpc=True)

        # Start a live update; block startup until the initial sweep finishes so
        # the collection is queryable, then keep it running in the background.
        update_handle = pipeline.app.update(live=True)
        async for snap in update_handle.watch():
            if snap.status is coco.UpdateStatus.READY:
                break
        update_task = asyncio.create_task(update_handle.result())

        try:
            yield
        finally:
            update_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await update_task
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
    query_embedding = pipeline.embed_query(q)

    if _client is None:
        raise RuntimeError("Qdrant client is not initialized.")

    results = pipeline._qdrant_search(
        _client,
        pipeline.QDRANT_COLLECTION,
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
