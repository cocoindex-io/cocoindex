"""
Image Search with Qdrant (v1) - CocoIndex pipeline example.

- Walk local image files
- Embed images with CLIP
- Store embeddings in Qdrant
- Query by text using CLIP text embeddings
"""

from __future__ import annotations

import functools
import io
import os
import pathlib
import sys
import uuid
from typing import AsyncIterator

import torch
from PIL import Image
from qdrant_client import QdrantClient
from qdrant_client.http import models as qdrant_models
from transformers import CLIPModel, CLIPProcessor
import numpy as np

import cocoindex as coco
import cocoindex.asyncio as coco_aio
from cocoindex.connectors import localfs, qdrant
from cocoindex.resources.file import FileLike, PatternFilePathMatcher
from cocoindex.resources.schema import VectorSchema

QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6334/")
QDRANT_COLLECTION = "ImageSearch"
CLIP_MODEL_NAME = "openai/clip-vit-large-patch14"
TOP_K = 5


QDRANT_DB = coco.ContextKey[qdrant.QdrantDatabase]("qdrant_db")
QDRANT_CLIENT = coco.ContextKey[QdrantClient]("qdrant_client")


@functools.cache
def get_clip_model() -> tuple[CLIPModel, CLIPProcessor]:
    model = CLIPModel.from_pretrained(CLIP_MODEL_NAME)
    processor = CLIPProcessor.from_pretrained(CLIP_MODEL_NAME)
    return model, processor


def embed_query(text: str) -> list[float]:
    model, processor = get_clip_model()
    inputs = processor(text=[text], return_tensors="pt", padding=True)
    with torch.no_grad():
        features = model.get_text_features(**inputs)
    return features[0].tolist()


def embed_image_bytes(img_bytes: bytes) -> list[float]:
    model, processor = get_clip_model()
    image = Image.open(io.BytesIO(img_bytes)).convert("RGB")
    inputs = processor(images=image, return_tensors="pt")
    with torch.no_grad():
        features = model.get_image_features(**inputs)
    return features[0].tolist()


@coco_aio.lifespan
async def coco_lifespan(
    builder: coco_aio.EnvironmentBuilder,
) -> AsyncIterator[None]:
    # Provide resources needed across the CocoIndex environment
    client = qdrant.create_client(QDRANT_URL, prefer_grpc=True)
    builder.provide(QDRANT_DB, qdrant.register_db("image_search_qdrant", client))
    builder.provide(QDRANT_CLIENT, client)
    yield


@coco.function(memo=True)
def process_file(
    file: FileLike,
    target: qdrant.CollectionTarget,
) -> None:
    content = file.read()
    embedding = embed_image_bytes(content)
    row_id = _image_id(file.relative_path)
    point = qdrant.PointStruct(
        id=row_id,
        vector=embedding,
        payload={"filename": str(file.relative_path)},
    )
    target.declare_point(point)


@coco.function
def app_main(sourcedir: pathlib.Path) -> None:
    model, _ = get_clip_model()
    dim = int(model.config.projection_dim)

    target_db = coco.use_context(QDRANT_DB)
    with coco.component_subpath("setup"):
        target_collection = coco.mount_run(
            coco.component_subpath("table"),
            target_db.declare_collection_target,
            collection_name=QDRANT_COLLECTION,
            schema=qdrant.CollectionSchema(
                vectors=qdrant.QdrantVectorDef(
                    schema=VectorSchema(dtype=np.dtype(np.float32), size=dim),
                    distance="cosine",
                )
            ),
        ).result()

    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(
            included_patterns=["*.jpg", "*.jpeg", "*.png"]
        ),
    )
    for f in files:
        coco.mount(
            coco.component_subpath("file", str(f.relative_path)),
            process_file,
            f,
            target_collection,
        )


app = coco_aio.App(
    coco_aio.AppConfig(name="ImageSearchQdrantV1"),
    app_main,
    sourcedir=pathlib.Path("./img"),
)


# ============================================================================
# Query demo
# ============================================================================


def query_once(client: QdrantClient, query_text: str, *, top_k: int = TOP_K) -> None:
    query_vec = embed_query(query_text)
    results = _qdrant_search(client, QDRANT_COLLECTION, query_vec, top_k)

    for r in results:
        payload = r.payload or {}
        print(f"[{r.score:.3f}] {payload.get('filename', '<unknown>')}")
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


def _image_id(path: pathlib.PurePath) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, str(path)))


def _qdrant_search(
    client: QdrantClient,
    collection_name: str,
    query_vector: list[float],
    limit: int,
) -> list[qdrant_models.ScoredPoint]:
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


if __name__ == "__main__":
    query()
