"""
Image Search with ColPali (v1) - CocoIndex pipeline definition.

Defines the CocoIndex `app` (walk local images -> embed with ColPali multi-vector ->
store in Qdrant with MaxSim multivector config) and the helper functions
(`embed_query`, `_qdrant_search`) used by `api.py`.

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
from typing import AsyncIterator

import torch
from PIL import Image
from qdrant_client import QdrantClient
from qdrant_client.http import models as qdrant_models
import numpy as np

from colpali_engine import ColPali, ColPaliProcessor
from colpali_engine.utils.torch_utils import (
    get_torch_device,
    unbind_padded_multivector_embeddings,
)

import cocoindex as coco
from cocoindex.connectors import localfs, qdrant
from cocoindex.resources.file import FileLike, PatternFilePathMatcher
from cocoindex.resources.schema import MultiVectorSchema, VectorSchema

QDRANT_COLLECTION = "ImageSearchColpali"
COLPALI_MODEL_NAME = os.getenv("COLPALI_MODEL", "vidore/colpali-v1.2")
TOP_K = 5


def qdrant_url() -> str:
    return os.getenv("QDRANT_URL", "http://localhost:6334/")


QDRANT_DB = coco.ContextKey[QdrantClient]("image_search_colpali")
QDRANT_CLIENT = coco.ContextKey[QdrantClient]("qdrant_client")


@functools.cache
def get_colpali() -> tuple[ColPali, ColPaliProcessor, str]:
    model = ColPali.from_pretrained(COLPALI_MODEL_NAME)
    processor = ColPaliProcessor.from_pretrained(COLPALI_MODEL_NAME)
    device = get_torch_device("auto")
    model = model.to(device)
    model.eval()
    return model, processor, device


def _postprocess_embeddings(
    embeddings: torch.Tensor, processor: ColPaliProcessor
) -> list[list[float]]:
    padding_side = getattr(processor.tokenizer, "padding_side", "right")
    unpadded = unbind_padded_multivector_embeddings(
        embeddings, padding_side=padding_side
    )
    return unpadded[0].cpu().tolist()


def embed_query(text: str) -> list[list[float]]:
    model, processor, device = get_colpali()
    batch = processor.process_queries(texts=[text])
    batch = batch.to(device)
    with torch.no_grad():
        embeddings = model(**batch)
    return _postprocess_embeddings(embeddings, processor)


def embed_image_bytes(img_bytes: bytes) -> list[list[float]]:
    model, processor, device = get_colpali()
    image = Image.open(io.BytesIO(img_bytes)).convert("RGB")
    batch = processor.process_images([image])
    batch = batch.to(device)
    with torch.no_grad():
        embeddings = model(**batch)
    return _postprocess_embeddings(embeddings, processor)


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
    model, _, _ = get_colpali()
    dim = int(getattr(model, "dim", 128))

    target_collection = await qdrant.mount_collection_target(
        QDRANT_DB,
        collection_name=QDRANT_COLLECTION,
        schema=await qdrant.CollectionSchema.create(
            vectors=qdrant.QdrantVectorDef(
                schema=MultiVectorSchema(
                    vector_schema=VectorSchema(dtype=np.dtype(np.float32), size=dim)
                ),
                distance="cosine",
                multivector_comparator="max_sim",
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
    coco.AppConfig(name="ImageSearchColpaliV1"),
    app_main,
    sourcedir=pathlib.Path("./img"),
)


def _image_id(path: pathlib.PurePath) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, str(path)))


def _qdrant_search(
    client: QdrantClient,
    collection_name: str,
    query_vector: list[list[float]],
    limit: int,
) -> list[qdrant_models.ScoredPoint]:
    if not hasattr(client, "query_points"):
        raise RuntimeError("qdrant-client must support query_points for ColPali.")
    response = client.query_points(
        collection_name=collection_name,
        query=query_vector,
        limit=limit,
        with_payload=True,
    )
    return response.points
