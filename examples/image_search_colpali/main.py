"""
Image Search with ColPali (v1) - CocoIndex pipeline example.

- Walk local image files
- Embed images with ColPali (multi-vector)
- Store embeddings in Qdrant with MaxSim multivector config
- Query by text using ColPali query embeddings
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
import numpy as np

from colpali_engine import ColPali, ColPaliProcessor
from colpali_engine.utils.torch_utils import (
    get_torch_device,
    unbind_padded_multivector_embeddings,
)

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
from cocoindex.resources.schema import MultiVectorSchema, VectorSchema

QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6334/")
QDRANT_COLLECTION = "ImageSearchColpali"
COLPALI_MODEL_NAME = os.getenv("COLPALI_MODEL", "vidore/colpali-v1.2")
TOP_K = 5


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

    files = walk_image_files(sourcedir)
    await coco.mount_each(process_file, files.items(), target_collection)


app = coco.App(
    coco.AppConfig(name="ImageSearchColpaliV1"),
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
    query_vector: list[list[float]],
    limit: int,
) -> list[qdrant_models.ScoredPoint]:
    return qdrant_search(
        client,
        QDRANT_COLLECTION,
        query_vector,
        limit,
        require_query_points=True,
    )


if __name__ == "__main__":
    query()
