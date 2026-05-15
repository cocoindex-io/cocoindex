"""
Image Search with Qdrant (v1) - CocoIndex pipeline definition.

Defines the CocoIndex `app` (walk local images -> embed with CLIP -> store in Qdrant)
and the helper functions (`embed_query`, `_qdrant_search`) used by `api.py`.

This module is not an entry point. To run the example, start the FastAPI server:

    python -m uvicorn api:app --reload --host 0.0.0.0 --port 8000

The server runs the index in live mode in the background and serves search requests.
"""

from __future__ import annotations

import functools
import io
import os
import pathlib
import uuid
from typing import Any, AsyncIterator

import torch
from PIL import Image
from qdrant_client import QdrantClient
from qdrant_client.http import models as qdrant_models
from transformers import CLIPModel, CLIPProcessor
import numpy as np

import cocoindex as coco
from cocoindex.connectors import localfs, qdrant
from cocoindex.resources.file import FileLike, PatternFilePathMatcher
from cocoindex.resources.schema import VectorSchema

QDRANT_COLLECTION = "ImageSearch"
CLIP_MODEL_NAME = "openai/clip-vit-large-patch14"
TOP_K = 5


def qdrant_url() -> str:
    return os.getenv("QDRANT_URL", "http://localhost:6334/")


QDRANT_DB = coco.ContextKey[QdrantClient]("image_search_qdrant")
QDRANT_CLIENT = coco.ContextKey[QdrantClient]("qdrant_client")


@functools.cache
def get_clip_model() -> tuple[CLIPModel, CLIPProcessor]:
    model = CLIPModel.from_pretrained(CLIP_MODEL_NAME)
    processor = CLIPProcessor.from_pretrained(CLIP_MODEL_NAME)
    return model, processor


def _projected_features(out: Any) -> torch.Tensor:
    # transformers >=5 returns BaseModelOutputWithPooling with the projected features in pooler_output;
    # transformers <5 returns the projected features tensor directly.
    return out.pooler_output if hasattr(out, "pooler_output") else out


def embed_query(text: str) -> list[float]:
    model, processor = get_clip_model()
    inputs = processor(text=[text], return_tensors="pt", padding=True)
    with torch.no_grad():
        out = model.get_text_features(**inputs)
    return _projected_features(out)[0].tolist()


def embed_image_bytes(img_bytes: bytes) -> list[float]:
    model, processor = get_clip_model()
    image = Image.open(io.BytesIO(img_bytes)).convert("RGB")
    inputs = processor(images=image, return_tensors="pt")
    with torch.no_grad():
        out = model.get_image_features(**inputs)
    return _projected_features(out)[0].tolist()


@coco.lifespan
async def coco_lifespan(
    builder: coco.EnvironmentBuilder,
) -> AsyncIterator[None]:
    client = qdrant.create_client(qdrant_url(), prefer_grpc=True)
    builder.provide(QDRANT_DB, client)
    builder.provide(QDRANT_CLIENT, client)
    yield


@coco.fn(memo=True)
async def process_file(
    file: FileLike,
    target: qdrant.CollectionTarget,
) -> None:
    content = await file.read()
    embedding = embed_image_bytes(content)
    row_id = _image_id(file.file_path.path)
    point = qdrant.PointStruct(
        id=row_id,
        vector=embedding,
        payload={"filename": str(file.file_path.path)},
    )
    target.declare_point(point)


@coco.fn
async def app_main(sourcedir: pathlib.Path) -> None:
    model, _ = get_clip_model()
    dim: int = model.config.projection_dim  # type: ignore[assignment]

    target_collection = await qdrant.mount_collection_target(
        QDRANT_DB,
        collection_name=QDRANT_COLLECTION,
        schema=await qdrant.CollectionSchema.create(
            vectors=qdrant.QdrantVectorDef(
                schema=VectorSchema(dtype=np.dtype(np.float32), size=dim),
                distance="cosine",
            )
        ),
    )

    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(
            included_patterns=["**/*.jpg", "**/*.jpeg", "**/*.png"]
        ),
        live=True,  # source supports live watch; api.py runs the app with live=True
    )
    await coco.mount_each(process_file, files.items(), target_collection)


app = coco.App(
    coco.AppConfig(name="ImageSearchQdrantV1"),
    app_main,
    sourcedir=pathlib.Path("./img"),
)


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
