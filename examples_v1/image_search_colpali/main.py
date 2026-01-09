"""
Image Search with ColPali (v1) - CocoIndex pipeline example.

- Walk local image files
- Embed images with ColPali (multi-vector)
- Store embeddings in Qdrant with MaxSim multivector config
- Query by text using ColPali query embeddings
"""

from __future__ import annotations

import asyncio
import functools
import io
import os
import pathlib
import sys
import uuid
from dataclasses import dataclass
from typing import AsyncIterator

import torch
from PIL import Image
from qdrant_client import QdrantClient
from qdrant_client.http import models as qdrant_models

from colpali_engine import ColPali, ColPaliProcessor
from colpali_engine.utils.torch_utils import (
    get_torch_device,
    unbind_padded_multivector_embeddings,
)

import cocoindex as coco
import cocoindex.asyncio as coco_aio
from cocoindex.connectors import localfs, qdrant
from cocoindex.resources.file import FileLike, PatternFilePathMatcher

QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6334/")
QDRANT_COLLECTION = "ImageSearchColpali"
COLPALI_MODEL_NAME = os.getenv("COLPALI_MODEL", "vidore/colpali-v1.2")
TOP_K = 5


@dataclass
class _GlobalState:
    db: qdrant.QdrantDatabase | None = None
    client: QdrantClient | None = None


_state = _GlobalState()


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


# ============================================================================
# Table schema
# ============================================================================


@dataclass
class ImageEmbedding:
    id: str
    filename: str
    embedding: list[list[float]]


@coco.function
def setup_collection(
    scope: coco.Scope,
) -> qdrant.TableTarget[ImageEmbedding, coco.PendingS]:
    assert _state.db is not None

    model, _, _ = get_colpali()
    dim = int(getattr(model, "dim", 128))
    return _state.db.declare_collection_target(
        scope,
        collection_name=QDRANT_COLLECTION,
        table_schema=qdrant.TableSchema(
            ImageEmbedding,
            primary_key=["id"],
            column_specs={
                "embedding": qdrant.QdrantVectorSpec(
                    dim,
                    distance="cosine",
                    multivector=True,
                    multivector_comparator="max_sim",
                )
            },
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
    _state.client = client
    _state.db = qdrant.register_db("image_search_colpali", client)
    yield


@coco.function(memo=True)
def process_file(
    scope: coco.Scope,
    file: FileLike,
    table: qdrant.TableTarget[ImageEmbedding],
) -> None:
    content = file.read()
    embedding = embed_image_bytes(content)
    row_id = _image_id(file.relative_path)
    table.declare_row(
        scope,
        row=ImageEmbedding(
            id=row_id,
            filename=str(file.relative_path),
            embedding=embedding,
        ),
    )


@coco.function
async def app_main(scope: coco.Scope, sourcedir: pathlib.Path) -> None:
    table = await coco_aio.mount_run(setup_collection, scope / "setup").result()

    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(
            included_patterns=["*.jpg", "*.jpeg", "*.png"]
        ),
    )
    for f in files:
        coco_aio.mount(process_file, scope / "file" / str(f.relative_path), f, table)


app = coco_aio.App(
    app_main,
    coco_aio.AppConfig(name="ImageSearchColpaliV1"),
    sourcedir=pathlib.Path("./img"),
)


# ============================================================================
# Query demo
# ============================================================================


async def query_once(query: str, *, top_k: int = TOP_K) -> None:
    query_vec = await asyncio.to_thread(embed_query, query)
    client = _state.client
    assert client is not None

    results = await asyncio.to_thread(
        _qdrant_search,
        client,
        QDRANT_COLLECTION,
        query_vec,
        top_k,
    )

    for r in results:
        payload = r.payload or {}
        print(f"[{r.score:.3f}] {payload.get('filename', '<unknown>')}")
        print("---")


async def main() -> None:
    async with coco_aio.runtime():
        if len(sys.argv) > 1 and sys.argv[1] == "query":
            if len(sys.argv) > 2:
                q = " ".join(sys.argv[2:])
                await query_once(q)
                return

            while True:
                q = input("Enter search query (or Enter to quit): ").strip()
                if not q:
                    break
                await query_once(q)
            return

        await app.run()


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
        using="embedding",
        limit=limit,
        with_payload=True,
    )
    return response.points


if __name__ == "__main__":
    asyncio.run(main())
