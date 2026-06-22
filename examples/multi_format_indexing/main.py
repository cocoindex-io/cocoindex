"""
Multi-format Indexing (v1) — CocoIndex pipeline example, ColPali + Qdrant.

Index a folder of mixed documents — PDFs *and* images — into one searchable
Qdrant collection. Each PDF page is rendered to an image, every page/image is
embedded with the multi-vector ColPali model, and the embeddings are stored in
Qdrant with MaxSim so a text query retrieves the most relevant page, whatever
format it came from.

Index (one-shot catch-up, or `-L` for live):
    cocoindex update main
    cocoindex update -L main

Query the index:
    python main.py "revenue growth"
"""

from __future__ import annotations

import functools
import io
import mimetypes
import os
import pathlib
import sys
import uuid
from dataclasses import dataclass
from typing import AsyncIterator

import numpy as np
import torch
from PIL import Image
from pdf2image import convert_from_bytes
from qdrant_client import QdrantClient

from colpali_engine import ColPali, ColPaliProcessor
from colpali_engine.utils.torch_utils import (
    get_torch_device,
    unbind_padded_multivector_embeddings,
)

import cocoindex as coco
from cocoindex.connectors import localfs, qdrant
from cocoindex.resources.file import FileLike, PatternFilePathMatcher
from cocoindex.resources.schema import MultiVectorSchema, VectorSchema

QDRANT_COLLECTION = "MultiFormatIndexing"
COLPALI_MODEL_NAME = os.getenv("COLPALI_MODEL", "vidore/colpali-v1.2")
PDF_RENDER_DPI = 200
TOP_K = 5


def qdrant_url() -> str:
    return os.getenv("QDRANT_URL", "http://localhost:6334/")


QDRANT_DB = coco.ContextKey[QdrantClient]("multi_format_qdrant")


# ---------------------------------------------------------------------------
# ColPali multi-vector embedding (shared by index + query)
# ---------------------------------------------------------------------------


@functools.cache
def get_colpali() -> tuple[ColPali, ColPaliProcessor, str]:
    model = ColPali.from_pretrained(COLPALI_MODEL_NAME)
    processor = ColPaliProcessor.from_pretrained(COLPALI_MODEL_NAME)
    device = get_torch_device("auto")
    model = model.to(device).eval()
    return model, processor, device


def _postprocess(
    embeddings: torch.Tensor, processor: ColPaliProcessor
) -> list[list[float]]:
    padding_side = getattr(processor.tokenizer, "padding_side", "right")
    return (
        unbind_padded_multivector_embeddings(embeddings, padding_side=padding_side)[0]
        .cpu()
        .tolist()
    )


def embed_image(img: Image.Image) -> list[list[float]]:
    model, processor, device = get_colpali()
    batch = processor.process_images([img]).to(device)
    with torch.no_grad():
        return _postprocess(model(**batch), processor)


def embed_query(text: str) -> list[list[float]]:
    model, processor, device = get_colpali()
    batch = processor.process_queries(texts=[text]).to(device)
    with torch.no_grad():
        return _postprocess(model(**batch), processor)


# ---------------------------------------------------------------------------
# Pages — split any file into per-page images
# ---------------------------------------------------------------------------


@dataclass
class Page:
    page_number: int | None  # 1-based for PDFs, None for standalone images
    image: bytes


@coco.fn.as_async(runner=coco.GPU)
def file_to_pages(filename: str, content: bytes) -> list[Page]:
    """PDF -> one image per page; image -> a single page; anything else -> []."""
    mime_type, _ = mimetypes.guess_type(filename)
    if mime_type == "application/pdf":
        pages = []
        for i, image in enumerate(convert_from_bytes(content, dpi=PDF_RENDER_DPI)):
            buf = io.BytesIO()
            image.save(buf, format="PNG")
            pages.append(Page(page_number=i + 1, image=buf.getvalue()))
        return pages
    if mime_type and mime_type.startswith("image/"):
        return [Page(page_number=None, image=content)]
    return []


@coco.fn.as_async(runner=coco.GPU)
def embed_page(page_png: bytes) -> list[list[float]]:
    return embed_image(Image.open(io.BytesIO(page_png)).convert("RGB"))


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


def _page_id(filename: str, page_number: int | None) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{filename}|{page_number}"))


@coco.fn
async def process_page(
    page: Page, filename: str, target: qdrant.CollectionTarget
) -> None:
    embedding = await embed_page(page.image)
    target.declare_point(
        qdrant.PointStruct(
            id=_page_id(filename, page.page_number),
            vector=embedding,
            payload={"filename": filename, "page": page.page_number},
        )
    )


@coco.fn(memo=True)
async def process_file(file: FileLike, target: qdrant.CollectionTarget) -> None:
    filename = str(file.file_path.path)
    pages = await file_to_pages(filename, await file.read())
    await coco.map(process_page, pages, filename, target)


@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    builder.provide(QDRANT_DB, qdrant.create_client(qdrant_url(), prefer_grpc=True))
    yield


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
            included_patterns=["**/*.pdf", "**/*.jpg", "**/*.jpeg", "**/*.png"]
        ),
        live=True,
    )
    await coco.mount_each(process_file, files.items(), target_collection)


app = coco.App(
    coco.AppConfig(name="MultiFormatIndexing"),
    app_main,
    sourcedir=pathlib.Path("./source_files"),
)


# ---------------------------------------------------------------------------
# Query demo
# ---------------------------------------------------------------------------


def query(text: str, *, top_k: int = TOP_K) -> None:
    client = qdrant.create_client(qdrant_url(), prefer_grpc=True)
    for r in client.query_points(
        collection_name=QDRANT_COLLECTION,
        query=embed_query(text),
        limit=top_k,
        with_payload=True,
    ).points:
        payload = r.payload or {}
        page = f" page {payload['page']}" if payload.get("page") else ""
        print(f"[{r.score:.3f}] {payload.get('filename')}{page}")


if __name__ == "__main__":
    if len(sys.argv) >= 2:
        query(" ".join(sys.argv[1:]))
    else:
        print(__doc__)
