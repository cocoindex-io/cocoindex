import cocoindex
import io
import dataclasses
import datetime
import typing

import face_recognition
from PIL import Image
import numpy as np


@dataclasses.dataclass
class ImageRect:
    top: int
    left: int
    bottom: int
    right: int


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
    related_arg_attr=(cocoindex.op.RelatedFieldAttribute.RECTS_BASE_IMAGE, "content"),
)
def extract_faces(content: bytes) -> list[FaceBase]:
    """Extract the first pages of a PDF."""
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
    for top, right, bottom, left in locs:
        rect = ImageRect(
            left=int(left * ratio),
            top=int(top * ratio),
            right=int(right * ratio),
            bottom=int(bottom * ratio),
        )

        # Crop the face and save it as a PNG.
        buf = io.BytesIO()
        orig_img.crop((rect.left, rect.top, rect.right, rect.bottom)).save(
            buf, format="PNG"
        )
        face = buf.getvalue()
        faces.append(FaceBase(rect, face))

    return faces


@cocoindex.op.function(cache=True, behavior_version=1, gpu=True)
def extract_face_embedding(
    face: bytes,
) -> cocoindex.Vector[cocoindex.Float32, typing.Literal[128]]:
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
    Define an example flow that embeds files into a vector database.
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
                filename=image["filename"],
                rect=face["rect"],
                embedding=face["embedding"],
            )

    face_embeddings.export(
        "face_embeddings",
        cocoindex.targets.Postgres(),
        primary_key_fields=["filename", "rect"],
    )
