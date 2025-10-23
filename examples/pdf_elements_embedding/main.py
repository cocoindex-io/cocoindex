import cocoindex
import io
import torch
import functools
import PIL

from dataclasses import dataclass
from pypdf import PdfReader
from transformers import CLIPModel, CLIPProcessor
from typing import Literal


QDRANT_GRPC_URL = "http://localhost:6334"
QDRANT_COLLECTION_IMAGE = "PdfElementsEmbeddingImage"
QDRANT_COLLECTION_TEXT = "PdfElementsEmbeddingText"

CLIP_MODEL_NAME = "openai/clip-vit-large-patch14"
CLIP_MODEL_DIMENSION = 768
ClipVectorType = cocoindex.Vector[cocoindex.Float32, Literal[CLIP_MODEL_DIMENSION]]

IMG_THUMBNAIL_SIZE = (512, 512)


@functools.cache
def get_clip_model() -> tuple[CLIPModel, CLIPProcessor]:
    model = CLIPModel.from_pretrained(CLIP_MODEL_NAME)
    processor = CLIPProcessor.from_pretrained(CLIP_MODEL_NAME)
    return model, processor


@cocoindex.op.function(cache=True, behavior_version=1, gpu=True)
def clip_embed_image(img_bytes: bytes) -> ClipVectorType:
    """
    Convert image to embedding using CLIP model.
    """
    model, processor = get_clip_model()
    image = PIL.Image.open(io.BytesIO(img_bytes)).convert("RGB")
    inputs = processor(images=image, return_tensors="pt")
    with torch.no_grad():
        features = model.get_image_features(**inputs)
    return features[0].tolist()


def clip_embed_query(text: str) -> ClipVectorType:
    """
    Embed the caption using CLIP model.
    """
    model, processor = get_clip_model()
    inputs = processor(text=[text], return_tensors="pt", padding=True)
    with torch.no_grad():
        features = model.get_text_features(**inputs)
    return features[0].tolist()


@cocoindex.transform_flow()
def embed_text(
    text: cocoindex.DataSlice[str],
) -> cocoindex.DataSlice[cocoindex.Vector[cocoindex.Float32]]:
    """
    Embed the text using a SentenceTransformer model.
    This is a shared logic between indexing and querying, so extract it as a function."""
    return text.transform(
        cocoindex.functions.SentenceTransformerEmbed(
            model="sentence-transformers/all-MiniLM-L6-v2"
        )
    )


@dataclass
class PdfImage:
    name: str
    data: bytes


@dataclass
class PdfPage:
    page_number: int
    text: str
    images: list[PdfImage]


@cocoindex.op.function()
def extract_pdf_elements(content: bytes) -> list[PdfPage]:
    """
    Extract texts and images from a PDF file.
    """
    reader = PdfReader(io.BytesIO(content))
    result = []
    for i, page in enumerate(reader.pages):
        text = page.extract_text()
        images = []
        for image in page.images:
            img = image.image
            if img is None:
                continue
            # Skip very small images.
            if img.width < 16 or img.height < 16:
                continue
            thumbnail = io.BytesIO()
            img.thumbnail(IMG_THUMBNAIL_SIZE)
            img.save(thumbnail, img.format or "PNG")
            images.append(PdfImage(name=image.name, data=thumbnail.getvalue()))
        result.append(PdfPage(page_number=i + 1, text=text, images=images))
    return result


qdrant_connection = cocoindex.add_auth_entry(
    "qdrant_connection",
    cocoindex.targets.QdrantConnection(grpc_url=QDRANT_GRPC_URL),
)


@cocoindex.flow_def(name="PdfElementsEmbedding")
def multi_format_indexing_flow(
    flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope
) -> None:
    """
    Define an example flow that embeds files into a vector database.
    """
    data_scope["documents"] = flow_builder.add_source(
        cocoindex.sources.LocalFile(
            path="source_files", included_patterns=["*.pdf"], binary=True
        )
    )

    text_output = data_scope.add_collector()
    image_output = data_scope.add_collector()
    with data_scope["documents"].row() as doc:
        doc["pages"] = doc["content"].transform(extract_pdf_elements)
        with doc["pages"].row() as page:
            page["chunks"] = page["text"].transform(
                cocoindex.functions.SplitRecursively(
                    custom_languages=[
                        cocoindex.functions.CustomLanguageSpec(
                            language_name="text",
                            separators_regex=[
                                r"\n(\s*\n)+",
                                r"[\.!\?]\s+",
                                r"\n",
                                r"\s+",
                            ],
                        )
                    ]
                ),
                language="text",
                chunk_size=600,
                chunk_overlap=100,
            )
            with page["chunks"].row() as chunk:
                chunk["embedding"] = chunk["text"].call(embed_text)
                text_output.collect(
                    id=cocoindex.GeneratedField.UUID,
                    filename=doc["filename"],
                    page=page["page_number"],
                    text=chunk["text"],
                    embedding=chunk["embedding"],
                )
            with page["images"].row() as image:
                image["embedding"] = image["data"].transform(clip_embed_image)
                image_output.collect(
                    id=cocoindex.GeneratedField.UUID,
                    filename=doc["filename"],
                    page=page["page_number"],
                    image_data=image["data"],
                    embedding=image["embedding"],
                )

    text_output.export(
        "text_embeddings",
        cocoindex.targets.Qdrant(
            connection=qdrant_connection,
            collection_name=QDRANT_COLLECTION_TEXT,
        ),
        primary_key_fields=["id"],
    )
    image_output.export(
        "image_embeddings",
        cocoindex.targets.Qdrant(
            connection=qdrant_connection,
            collection_name=QDRANT_COLLECTION_IMAGE,
        ),
        primary_key_fields=["id"],
    )
