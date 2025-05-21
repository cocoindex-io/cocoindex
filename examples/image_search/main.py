from dotenv import load_dotenv

import cocoindex
import datetime
import functools
import io
import os
import torch

from typing import Literal
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from qdrant_client import QdrantClient

from PIL import Image
from transformers import CLIPModel, CLIPProcessor


QDRANT_GRPC_URL = os.getenv("QDRANT_GRPC_URL", "http://localhost:6334/")

@functools.cache
def get_clip_model() -> tuple[CLIPModel, CLIPProcessor]:
    model = CLIPModel.from_pretrained("openai/clip-vit-large-patch14")
    processor = CLIPProcessor.from_pretrained("openai/clip-vit-large-patch14")
    return model, processor


# Convert text to embedding using CLIP model.
def query_to_embedding(text: str) -> list[float]:
    """
    Embed the caption using CLIP model.
    """
    model, processor = get_clip_model()
    inputs = processor(text=[text], images=None, return_tensors="pt", padding=True)
    with torch.no_grad():
        features = model.get_text_features(**inputs)
    return features[0].tolist()


# Convert image bytes to embedding using CLIP model.
@cocoindex.op.function(cache=True, behavior_version=1, gpu=True)
def image_to_embedding(img_bytes: bytes) -> cocoindex.Vector[cocoindex.Float32, Literal[384]]:
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
def image_object_embedding_flow(flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope):
    data_scope["images"] = flow_builder.add_source(
        cocoindex.sources.LocalFile(path="img", included_patterns=["*.jpg", "*.jpeg", "*.png"], binary=True),
        refresh_interval=datetime.timedelta(minutes=1)  # Poll for changes every 1 minute
    )
    img_embeddings = data_scope.add_collector()
    with data_scope["images"].row() as img:
        img["embedding"] = img["content"].transform(image_to_embedding)
        img_embeddings.collect(
            id=cocoindex.GeneratedField.UUID,
            filename=img["filename"],
            embedding=img["embedding"],
        )
    img_embeddings.export(
        "img_embeddings",
        cocoindex.storages.Qdrant(
            collection_name="image_search",
            grpc_url=QDRANT_GRPC_URL,
        ),
        primary_key_fields=["id"],
        setup_by_user=True,
    )

# --- FastAPI app for web API ---
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# Serve images from the 'img' directory at /img
app.mount("/img", StaticFiles(directory="img"), name="img")

# --- CocoIndex initialization on startup ---
@app.on_event("startup")
def startup_event():
    load_dotenv()
    cocoindex.init()
    # Initialize Qdrant client
    app.state.qdrant_client = QdrantClient(
        url=QDRANT_GRPC_URL,
        prefer_grpc=True
    )
    app.state.live_updater = cocoindex.FlowLiveUpdater(image_object_embedding_flow)
    app.state.live_updater.start()

@app.get("/search")
def search(q: str = Query(..., description="Search query"), limit: int = Query(5, description="Number of results")):
    # Get the embedding for the query
    query_embedding = query_to_embedding(q)
    
    # Search in Qdrant
    search_results = app.state.qdrant_client.search(
        collection_name="image_search",
        query_vector=("embedding", query_embedding),
        limit=limit
    )
    
    # Format results
    out = []
    for result in search_results:
        out.append({
            "filename": result.payload["filename"],
            "score": result.score
        })
    return {"results": out}
