import datetime
import functools
import io
import os
from contextlib import asynccontextmanager
from typing import Any, Literal

import cocoindex
import torch
from dotenv import load_dotenv
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from PIL import Image
from qdrant_client import QdrantClient
from transformers import CLIPModel, CLIPProcessor

QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6334/")
QDRANT_COLLECTION = "ImageSearch"
CLIP_MODEL_NAME = "openai/clip-vit-large-patch14"
CLIP_MODEL_DIMENSION = 768


@functools.cache
def get_clip_model() -> tuple[CLIPModel, CLIPProcessor]:
    model = CLIPModel.from_pretrained(CLIP_MODEL_NAME)
    processor = CLIPProcessor.from_pretrained(CLIP_MODEL_NAME)
    return model, processor


def embed_query(text: str) -> list[float]:
    """
    Embed the caption using CLIP model.
    """
    model, processor = get_clip_model()
    inputs = processor(text=[text], return_tensors="pt", padding=True)
    with torch.no_grad():
        features = model.get_text_features(**inputs)
    return features[0].tolist()


@cocoindex.op.function(cache=True, behavior_version=1, gpu=True)
def embed_image(
    img_bytes: bytes,
) -> cocoindex.Vector[cocoindex.Float32, Literal[CLIP_MODEL_DIMENSION]]:
    """
    Convert image to embedding using CLIP model.
    """
    model, processor = get_clip_model()
    image = Image.open(io.BytesIO(img_bytes)).convert("RGB")
    inputs = processor(images=image, return_tensors="pt")
    with torch.no_grad():
        features = model.get_image_features(**inputs)
    return features[0].tolist()


# CocoIndex flow: Ingest images, extract captions, embed, export to Qdrant
@cocoindex.flow_def(name="ImageObjectEmbedding")
def image_object_embedding_flow(
    flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope
) -> None:
    data_scope["images"] = flow_builder.add_source(
        cocoindex.sources.LocalFile(
            path="img", included_patterns=["*.jpg", "*.jpeg", "*.png"], binary=True
        ),
        refresh_interval=datetime.timedelta(
            minutes=1
        ),  # Poll for changes every 1 minute
    )
    img_embeddings = data_scope.add_collector()
    with data_scope["images"].row() as img:
        img["embedding"] = img["content"].transform(embed_image)
        img_embeddings.collect(
            id=cocoindex.GeneratedField.UUID,
            filename=img["filename"],
            embedding=img["embedding"],
        )

    img_embeddings.export(
        "img_embeddings",
        cocoindex.targets.Qdrant(collection_name=QDRANT_COLLECTION),
        primary_key_fields=["id"],
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    load_dotenv()
    cocoindex.init()
    app.state.qdrant_client = QdrantClient(url=QDRANT_URL, prefer_grpc=True)

    # Start updater
    app.state.live_updater = cocoindex.FlowLiveUpdater(image_object_embedding_flow)
    app.state.live_updater.start()

    yield


# --- FastAPI app for web API ---
app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# Serve images from the 'img' directory at /img
app.mount("/img", StaticFiles(directory="img"), name="img")


# --- Search API ---
@app.get("/search")
def search(
    q: str = Query(..., description="Search query"),
    limit: int = Query(5, description="Number of results"),
) -> Any:
    # Get the embedding for the query
    query_embedding = embed_query(q)

    # Search in Qdrant
    search_results = app.state.qdrant_client.search(
        collection_name=QDRANT_COLLECTION,
        query_vector=("embedding", query_embedding),
        limit=limit,
    )

    return {
        "results": [
            {"filename": result.payload["filename"], "score": result.score}
            for result in search_results
        ]
    }
