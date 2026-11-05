"""
PDF Elements Embedding (v1): a CocoIndex pipeline example.

Index (use `-L` for live mode, omit for one-shot catch-up):
    cocoindex update main
    cocoindex update -L main

Query the index:
    python main.py "how do players trade goods?"

Pipeline: walk local PDFs -> split each page into text and images ->
embed text with SentenceTransformers and images with CLIP ->
store both in Qdrant, each point tagged with its file and page.
"""

from __future__ import annotations

import asyncio
import base64
import functools
import io
import os
import pathlib
import sys
import uuid
from dataclasses import dataclass
from typing import Any, AsyncIterator

import numpy as np
import torch
from dotenv import load_dotenv
from PIL import Image
from pypdf import PdfReader
from qdrant_client import QdrantClient
from transformers import CLIPModel, CLIPProcessor

import cocoindex as coco
from cocoindex.connectors import localfs, qdrant
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder
from cocoindex.ops.text import CustomLanguageConfig, RecursiveSplitter
from cocoindex.resources.chunk import Chunk
from cocoindex.resources.file import FileLike, PatternFilePathMatcher
from cocoindex.resources.id import IdGenerator
from cocoindex.resources.schema import VectorSchema

load_dotenv()  # the `cocoindex` CLI loads .env too; this covers `python main.py`

QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6334/")
TEXT_COLLECTION = "PdfElementsText"
IMAGE_COLLECTION = "PdfElementsImage"
TEXT_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
CLIP_MODEL = "openai/clip-vit-large-patch14"
TOP_K = 5

QDRANT_DB = coco.ContextKey[QdrantClient]("pdf_elements_qdrant")
EMBEDDER = coco.ContextKey[SentenceTransformerEmbedder]("embedder", detect_change=True)


# ---------------------------------------------------------------------------
# 1. Split a PDF into elements: page text and page images
# ---------------------------------------------------------------------------


@dataclass
class PdfImage:
    name: str
    data: bytes  # PNG/JPEG thumbnail, at most 512x512


@dataclass
class PdfPage:
    page_number: int  # 1-based
    text: str
    images: list[PdfImage]


@coco.fn.as_async
def extract_pdf_elements(content: bytes) -> list[PdfPage]:
    """Read a PDF and return its text and images, page by page."""
    reader = PdfReader(io.BytesIO(content))
    pages = []
    for i, page in enumerate(reader.pages):
        images = []
        for image in page.images:
            img = image.image
            if img is None:
                continue
            # Skip icons, bullets and rules: anything under 16 px on a side.
            if img.width < 16 or img.height < 16:
                continue
            img.thumbnail((512, 512))  # in place, keeps the aspect ratio
            buf = io.BytesIO()
            img.save(buf, img.format or "PNG")
            images.append(PdfImage(name=image.name, data=buf.getvalue()))
        pages.append(
            PdfPage(page_number=i + 1, text=page.extract_text(), images=images)
        )
    return pages


# ---------------------------------------------------------------------------
# 2. One model per element type
# ---------------------------------------------------------------------------

# Plain prose: break at paragraphs, then sentences, then lines, then spaces.
_splitter = RecursiveSplitter(
    custom_languages=[
        CustomLanguageConfig(
            language_name="text",
            separators_regex=[r"\n(\s*\n)+", r"[\.!\?]\s+", r"\n", r"\s+"],
        )
    ]
)


@functools.cache
def clip() -> tuple[CLIPModel, CLIPProcessor]:
    return (
        CLIPModel.from_pretrained(CLIP_MODEL),
        CLIPProcessor.from_pretrained(CLIP_MODEL),
    )


def _features(out: Any) -> torch.Tensor:
    # transformers >= 5 wraps the projected features; < 5 returns them directly.
    return out.pooler_output if hasattr(out, "pooler_output") else out


@coco.fn.as_async(runner=coco.GPU, deps=CLIP_MODEL)  # re-embed if the model changes
def clip_embed_image(data: bytes) -> list[float]:
    model, processor = clip()
    inputs = processor(
        images=Image.open(io.BytesIO(data)).convert("RGB"), return_tensors="pt"
    )
    with torch.no_grad():
        return _features(model.get_image_features(**inputs))[0].tolist()


def clip_embed_text(text: str) -> list[float]:
    """Embed a text query into CLIP's space, to search images with words."""
    model, processor = clip()
    inputs = processor(text=[text], return_tensors="pt", padding=True)
    with torch.no_grad():
        return _features(model.get_text_features(**inputs))[0].tolist()


# ---------------------------------------------------------------------------
# 3. Per-element processing: embed, then declare one Qdrant point
# ---------------------------------------------------------------------------


@coco.fn
async def process_chunk(
    chunk: Chunk,
    filename: str,
    page: int,
    id_gen: IdGenerator,
    text_target: qdrant.CollectionTarget,
) -> None:
    embedding = await coco.use_context(EMBEDDER).embed(chunk.text)
    text_target.declare_point(
        qdrant.PointStruct(
            id=await id_gen.next_id(chunk.text),
            vector=embedding.tolist(),
            payload={"filename": filename, "page": page, "text": chunk.text},
        )
    )


@coco.fn(memo=True)  # an image already embedded is not sent to CLIP again
async def process_image(
    image: PdfImage,
    filename: str,
    page: int,
    image_target: qdrant.CollectionTarget,
) -> None:
    image_target.declare_point(
        qdrant.PointStruct(
            id=str(uuid.uuid5(uuid.NAMESPACE_URL, f"{filename}#{page}/{image.name}")),
            vector=await clip_embed_image(image.data),
            payload={
                "filename": filename,
                "page": page,
                "image": image.name,
                # The thumbnail itself, so a UI can show the hit.
                "image_data": base64.b64encode(image.data).decode(),
            },
        )
    )


@coco.fn(memo=True)
async def process_file(
    file: FileLike,
    text_target: qdrant.CollectionTarget,
    image_target: qdrant.CollectionTarget,
) -> None:
    filename = str(file.file_path.path)
    id_gen = IdGenerator()
    for page in await extract_pdf_elements(await file.read()):
        chunks = _splitter.split(
            page.text, chunk_size=600, chunk_overlap=100, language="text"
        )
        await coco.map(
            process_chunk, chunks, filename, page.page_number, id_gen, text_target
        )
        await coco.map(
            process_image, page.images, filename, page.page_number, image_target
        )


# ---------------------------------------------------------------------------
# 4. The app: two Qdrant collections, one pass over the PDF folder
# ---------------------------------------------------------------------------


@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    builder.provide(QDRANT_DB, qdrant.create_client(QDRANT_URL, prefer_grpc=True))
    builder.provide(EMBEDDER, SentenceTransformerEmbedder(TEXT_MODEL))
    yield


@coco.fn
async def app_main(sourcedir: pathlib.Path) -> None:
    text_target = await qdrant.mount_collection_target(
        QDRANT_DB,
        collection_name=TEXT_COLLECTION,
        schema=await qdrant.CollectionSchema.create(
            vectors=qdrant.QdrantVectorDef(schema=EMBEDDER)
        ),
    )
    clip_dim: int = clip()[0].config.projection_dim  # 768 for ViT-L/14
    image_target = await qdrant.mount_collection_target(
        QDRANT_DB,
        collection_name=IMAGE_COLLECTION,
        schema=await qdrant.CollectionSchema.create(
            vectors=qdrant.QdrantVectorDef(
                schema=VectorSchema(dtype=np.dtype(np.float32), size=clip_dim),
                distance="cosine",
            )
        ),
    )
    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.pdf"]),
        live=True,  # pass -L to `cocoindex update` to keep watching
    )
    await coco.mount_each(process_file, files.items(), text_target, image_target)


app = coco.App(
    coco.AppConfig(name="PdfElementsEmbeddingV1"),
    app_main,
    sourcedir=pathlib.Path("./source_files"),
)


# ---------------------------------------------------------------------------
# Query: one question, two collections
# ---------------------------------------------------------------------------


def search(client: QdrantClient, collection: str, vector: list[float]) -> list[Any]:
    return client.query_points(
        collection_name=collection, query=vector, limit=TOP_K, with_payload=True
    ).points


async def query(q: str) -> None:
    client = qdrant.create_client(QDRANT_URL, prefer_grpc=True)
    text_vec = (await SentenceTransformerEmbedder(TEXT_MODEL).embed(q)).tolist()

    print("Text passages:")
    for p in search(client, TEXT_COLLECTION, text_vec):
        print(f"  [{p.score:.3f}] {p.payload['filename']} p.{p.payload['page']}")
        print(f"      {p.payload['text'][:120]!r}")

    print("Images:")
    for p in search(client, IMAGE_COLLECTION, clip_embed_text(q)):
        print(
            f"  [{p.score:.3f}] {p.payload['filename']} p.{p.payload['page']} {p.payload['image']}"
        )


if __name__ == "__main__":
    asyncio.run(query(" ".join(sys.argv[1:]) or "how do players trade goods?"))
