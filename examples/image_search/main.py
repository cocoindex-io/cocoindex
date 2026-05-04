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
from typing import AsyncIterator

import torch
from PIL import Image
from qdrant_client import QdrantClient
from qdrant_client.http import models as qdrant_models
from transformers import CLIPModel, CLIPProcessor
import numpy as np

_EXAMPLES_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(_EXAMPLES_DIR) not in sys.path:
    sys.path.append(str(_EXAMPLES_DIR))

import cocoindex as coco
from _image_search_shared import (
    image_id,
    print_search_results,
    provide_qdrant_client,
    qdrant_search,
    query_loop,
    walk_image_files,
)
from cocoindex.connectors import qdrant
from cocoindex.resources.file import FileLike
from cocoindex.resources.schema import VectorSchema

QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6334/")
QDRANT_COLLECTION = "ImageSearch"
CLIP_MODEL_NAME = "openai/clip-vit-large-patch14"
TOP_K = 5


QDRANT_DB = coco.ContextKey[QdrantClient]("image_search_qdrant")
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


@coco.lifespan
async def coco_lifespan(
    builder: coco.EnvironmentBuilder,
) -> AsyncIterator[None]:
    provide_qdrant_client(builder, QDRANT_URL, QDRANT_DB, QDRANT_CLIENT)
    yield


@coco.fn(memo=True)
async def process_file(
    file: FileLike,
    target: qdrant.CollectionTarget,
) -> None:
    content = await file.read()
    embedding = embed_image_bytes(content)
    row_id = image_id(file.file_path.path)
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

    files = walk_image_files(sourcedir)
    await coco.mount_each(process_file, files.items(), target_collection)


app = coco.App(
    coco.AppConfig(name="ImageSearchQdrantV1"),
    app_main,
    sourcedir=pathlib.Path("./img"),
)


# ============================================================================
# Query demo
# ============================================================================


def query_once(client: QdrantClient, query_text: str, *, top_k: int = TOP_K) -> None:
    query_vec = embed_query(query_text)
    print_search_results(search_qdrant(client, query_vec, top_k))


def query() -> None:
    query_loop(QDRANT_URL, query_once)


def search_qdrant(
    client: QdrantClient,
    query_vector: list[float],
    limit: int,
) -> list[qdrant_models.ScoredPoint]:
    return qdrant_search(client, QDRANT_COLLECTION, query_vector, limit)


if __name__ == "__main__":
    query()
