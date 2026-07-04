"""
Face Recognition (v1) — CocoIndex pipeline example.

Walk local images, detect every face with `face_recognition` (dlib), embed each
face into a 128-d vector, and index the faces in Qdrant — one point per face,
keyed by a stable id and carrying the source filename and bounding box.

Index (use `-L` for live mode, omit for one-shot catch-up):
    cocoindex update main
    cocoindex update -L main

Query the index with a face image (find the most similar indexed faces):
    python main.py query path/to/face.jpg
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
from typing import Any, AsyncIterator

import face_recognition
import numpy as np
from PIL import Image
from qdrant_client import QdrantClient

import cocoindex as coco
from cocoindex.connectors import localfs, qdrant
from cocoindex.resources.file import FileLike, PatternFilePathMatcher
from cocoindex.resources.schema import VectorSchema

QDRANT_COLLECTION = "face_embeddings"
FACE_EMBED_DIM = 128  # face_recognition (dlib) face encodings are 128-d
MAX_IMAGE_WIDTH = 1280  # the CNN detector is slow on large images; downscale first
TOP_K = 5


def qdrant_url() -> str:
    return os.getenv("QDRANT_URL", "http://localhost:6334/")


QDRANT_DB = coco.ContextKey[QdrantClient]("face_qdrant")


# ---------------------------------------------------------------------------
# Face detection + embedding (sync, dlib — run on a GPU runner)
# ---------------------------------------------------------------------------


@dataclass
class ImageRect:
    min_x: int
    min_y: int
    max_x: int
    max_y: int


@dataclass
class Face:
    """One detected face: its bounding box and the cropped PNG."""

    rect: ImageRect
    image: bytes


@coco.fn.as_async(runner=coco.GPU)
def extract_faces(content: bytes) -> list[Face]:
    """Detect faces in an image and return each as a (bounding box, cropped PNG)."""
    orig = Image.open(io.BytesIO(content)).convert("RGB")

    # Downscale large images for the detector, then map boxes back to full size.
    if orig.width > MAX_IMAGE_WIDTH:
        ratio = orig.width / MAX_IMAGE_WIDTH
        small = orig.resize(
            (MAX_IMAGE_WIDTH, int(orig.height / ratio)),
            resample=Image.Resampling.BICUBIC,
        )
    else:
        ratio = 1.0
        small = orig

    faces: list[Face] = []
    for top, right, bottom, left in face_recognition.face_locations(
        np.array(small), model="cnn"
    ):
        rect = ImageRect(
            min_x=int(left * ratio),
            min_y=int(top * ratio),
            max_x=int(right * ratio),
            max_y=int(bottom * ratio),
        )
        buf = io.BytesIO()
        orig.crop((rect.min_x, rect.min_y, rect.max_x, rect.max_y)).save(
            buf, format="PNG"
        )
        faces.append(Face(rect=rect, image=buf.getvalue()))

    return faces


@coco.fn.as_async(runner=coco.GPU)
def embed_face(face_png: bytes) -> list[float]:
    """Embed a single cropped face into a 128-d vector."""
    img = Image.open(io.BytesIO(face_png)).convert("RGB")
    arr = np.array(img)
    encoding = face_recognition.face_encodings(
        arr, known_face_locations=[(0, img.width - 1, img.height - 1, 0)]
    )[0]
    return encoding.tolist()


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


def _face_id(filename: str, rect: ImageRect) -> str:
    key = f"{filename}|{rect.min_x},{rect.min_y},{rect.max_x},{rect.max_y}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, key))


@coco.fn
async def process_face(
    face: Face,
    filename: str,
    target: qdrant.CollectionTarget,
) -> None:
    """Embed one face and declare it as a Qdrant point."""
    embedding = await embed_face(face.image)
    target.declare_point(
        qdrant.PointStruct(
            id=_face_id(filename, face.rect),
            vector=embedding,
            payload={
                "filename": filename,
                "min_x": face.rect.min_x,
                "min_y": face.rect.min_y,
                "max_x": face.rect.max_x,
                "max_y": face.rect.max_y,
            },
        )
    )


@coco.fn(memo=True)
async def process_file(file: FileLike, target: qdrant.CollectionTarget) -> None:
    """Detect every face in one image and index each one."""
    faces = await extract_faces(await file.read())
    filename = str(file.file_path.path)
    await coco.map(process_face, faces, filename, target)


@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    builder.provide(QDRANT_DB, qdrant.create_client(qdrant_url(), prefer_grpc=True))
    yield


@coco.fn
async def app_main(sourcedir: pathlib.Path) -> None:
    target_collection = await qdrant.mount_collection_target(
        QDRANT_DB,
        collection_name=QDRANT_COLLECTION,
        schema=await qdrant.CollectionSchema.create(
            vectors=qdrant.QdrantVectorDef(
                schema=VectorSchema(dtype=np.dtype(np.float32), size=FACE_EMBED_DIM),
                distance="euclid",  # dlib face encodings compare by Euclidean distance
            )
        ),
    )

    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(
            included_patterns=["**/*.jpg", "**/*.jpeg", "**/*.png"]
        ),
        live=True,  # api supports live watch; pass -L to `cocoindex update` to run live
    )
    await coco.mount_each(process_file, files.items(), target_collection)


app = coco.App(
    coco.AppConfig(name="FaceRecognitionV1"),
    app_main,
    sourcedir=pathlib.Path("./images"),
)


# ---------------------------------------------------------------------------
# Query demo — find the indexed faces most similar to a query face image
# ---------------------------------------------------------------------------


def _qdrant_search(
    client: QdrantClient, query_vector: list[float], limit: int
) -> list[Any]:
    return client.query_points(
        collection_name=QDRANT_COLLECTION,
        query=query_vector,
        limit=limit,
        with_payload=True,
    ).points


def query(image_path: str, *, top_k: int = TOP_K) -> None:
    content = pathlib.Path(image_path).read_bytes()
    img = Image.open(io.BytesIO(content)).convert("RGB")
    arr = np.array(img)
    locs = face_recognition.face_locations(arr, model="cnn")
    if not locs:
        print("No face found in the query image.")
        return
    query_vec = face_recognition.face_encodings(arr, known_face_locations=locs[:1])[
        0
    ].tolist()

    client = qdrant.create_client(qdrant_url(), prefer_grpc=True)
    for r in _qdrant_search(client, query_vec, top_k):
        payload = r.payload or {}
        # Euclidean distance: smaller is more similar (same person typically < 0.6).
        print(f"[{r.score:.3f}] {payload.get('filename')}  {payload}")


if __name__ == "__main__":
    if len(sys.argv) >= 3 and sys.argv[1] == "query":
        query(sys.argv[2])
    else:
        print(__doc__)
