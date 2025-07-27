import cocoindex
import dataclasses
import datetime
import io
import os

import face_recognition
import numpy as np
from PIL import Image

QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6334/")
QDRANT_COLLECTION = "face_embeddings"


@dataclasses.dataclass
class ImageRect:
    min_x: int
    min_y: int
    max_x: int
    max_y: int


@dataclasses.dataclass
class FaceBase:
    """A face in an image."""

    rect: ImageRect
    image: bytes


MAX_IMAGE_WIDTH = 1280


@cocoindex.op.function(
    cache=True,
    behavior_version=1,
    gpu=True,
    arg_relationship=(cocoindex.op.ArgRelationship.RECTS_BASE_IMAGE, "content"),
)
def extract_faces(content: bytes) -> list[FaceBase]:
    """Extract the faces from a image."""
    orig_img = Image.open(io.BytesIO(content)).convert("RGB")

    # The model is too slow on large images, so we resize them if too large.
    if orig_img.width > MAX_IMAGE_WIDTH:
        ratio = orig_img.width * 1.0 / MAX_IMAGE_WIDTH
        img = orig_img.resize(
            (MAX_IMAGE_WIDTH, int(orig_img.height / ratio)),
            resample=Image.Resampling.BICUBIC,
        )
    else:
        ratio = 1.0
        img = orig_img

    # Extract face locations.
    locs = face_recognition.face_locations(np.array(img), model="cnn")

    faces: list[FaceBase] = []
    for min_y, max_x, max_y, min_x in locs:
        rect = ImageRect(
            min_x=int(min_x * ratio),
            min_y=int(min_y * ratio),
            max_x=int(max_x * ratio),
            max_y=int(max_y * ratio),
        )

        # Crop the face and save it as a PNG.
        buf = io.BytesIO()
        orig_img.crop((rect.min_x, rect.min_y, rect.max_x, rect.max_y)).save(
            buf, format="PNG"
        )
        face = buf.getvalue()
        faces.append(FaceBase(rect, face))

    return faces


@cocoindex.op.function(cache=True, behavior_version=1, gpu=True)
def extract_face_embedding(
    face: bytes,
) -> cocoindex.Vector[cocoindex.Float32]:
    """Extract the embedding of a face."""
    img = Image.open(io.BytesIO(face)).convert("RGB")
    embedding = face_recognition.face_encodings(
        np.array(img),
        known_face_locations=[(0, img.width - 1, img.height - 1, 0)],
    )[0]
    return embedding


@cocoindex.flow_def(name="FaceRecognition")
def face_recognition_flow(
    flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope
) -> None:
    """
    Define an example flow that embeds files into Qdrant vector database.
    """
    data_scope["images"] = flow_builder.add_source(
        cocoindex.sources.LocalFile(path="images", binary=True),
        refresh_interval=datetime.timedelta(seconds=10),
    )

    face_embeddings = data_scope.add_collector()

    with data_scope["images"].row() as image:
        # Extract faces
        image["faces"] = image["content"].transform(extract_faces)

        with image["faces"].row() as face:
            face["embedding"] = face["image"].transform(extract_face_embedding)

            # Collect embeddings
            face_embeddings.collect(
                id=cocoindex.GeneratedField.UUID,
                filename=image["filename"],
                rect=face["rect"],
                embedding=face["embedding"],
            )

    face_embeddings.export(
        QDRANT_COLLECTION,
        cocoindex.targets.Qdrant(collection_name=QDRANT_COLLECTION),
        primary_key_fields=["id"],
    )
