import datetime
import functools
import io
import os
from contextlib import asynccontextmanager
from typing import Any, Literal

import cocoindex
import numpy as np
from dotenv import load_dotenv
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from PIL import Image
from qdrant_client import QdrantClient
from colpali_engine.models import ColPali, ColPaliProcessor


# --- Config ---

# Use GRPC
QDRANT_URL = os.getenv("QDRANT_URL", "localhost:6334")
PREFER_GRPC = os.getenv("QDRANT_PREFER_GRPC", "true").lower() == "true"

# Use HTTP
# QDRANT_URL = os.getenv("QDRANT_URL", "localhost:6333")
# PREFER_GRPC = os.getenv("QDRANT_PREFER_GRPC", "false").lower() == "true"

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434/")
QDRANT_COLLECTION = "ImageSearchColpali"
COLPALI_MODEL_NAME = os.getenv("COLPALI_MODEL", "vidore/colpali-v1.2")
COLPALI_MODEL_DIMENSION = 1031  # Set to match ColPali's output

# --- ColPali model cache and embedding functions ---
_colpali_model_cache = {}


def get_colpali_model(model: str = COLPALI_MODEL_NAME):
    global _colpali_model_cache
    if model not in _colpali_model_cache:
        print(f"Loading ColPali model: {model}")
        _colpali_model_cache[model] = {
            "model": ColPali.from_pretrained(model),
            "processor": ColPaliProcessor.from_pretrained(model),
        }
    return _colpali_model_cache[model]["model"], _colpali_model_cache[model][
        "processor"
    ]


def colpali_embed_image(
    img_bytes: bytes, model: str = COLPALI_MODEL_NAME
) -> list[float]:
    from PIL import Image
    import torch
    import io

    colpali_model, processor = get_colpali_model(model)
    pil_image = Image.open(io.BytesIO(img_bytes)).convert("RGB")
    inputs = processor.process_images([pil_image])
    with torch.no_grad():
        embeddings = colpali_model(**inputs)
    pooled_embedding = embeddings.mean(dim=-1)
    result = pooled_embedding[0].cpu().numpy()  # [1031]
    return result.tolist()


def colpali_embed_query(query: str, model: str = COLPALI_MODEL_NAME) -> list[float]:
    import torch
    import numpy as np

    colpali_model, processor = get_colpali_model(model)
    inputs = processor.process_queries([query])
    with torch.no_grad():
        embeddings = colpali_model(**inputs)
    pooled_embedding = embeddings.mean(dim=-1)
    query_tokens = pooled_embedding[0].cpu().numpy()  # [15]
    target_length = COLPALI_MODEL_DIMENSION
    result = np.zeros(target_length, dtype=np.float32)
    result[: min(len(query_tokens), target_length)] = query_tokens[:target_length]
    return result.tolist()


# --- End ColPali embedding functions ---


def embed_query(text: str) -> list[float]:
    """
    Embed the caption using ColPali model.
    """
    return colpali_embed_query(text, model=COLPALI_MODEL_NAME)


@cocoindex.op.function(cache=True, behavior_version=1, gpu=True)
def embed_image(
    img_bytes: bytes,
) -> cocoindex.Vector[cocoindex.Float32, Literal[COLPALI_MODEL_DIMENSION]]:
    """
    Convert image to embedding using ColPali model.
    """
    return colpali_embed_image(img_bytes, model=COLPALI_MODEL_NAME)


@cocoindex.flow_def(name="ImageObjectEmbeddingColpali")
def image_object_embedding_flow(
    flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope
) -> None:
    data_scope["images"] = flow_builder.add_source(
        cocoindex.sources.LocalFile(
            path="img", included_patterns=["*.jpg", "*.jpeg", "*.png"], binary=True
        ),
        refresh_interval=datetime.timedelta(minutes=1),
    )
    img_embeddings = data_scope.add_collector()
    with data_scope["images"].row() as img:
        ollama_model_name = os.getenv("OLLAMA_MODEL")
        if ollama_model_name is not None:
            # If an Ollama model is specified, generate an image caption
            img["caption"] = flow_builder.transform(
                cocoindex.functions.ExtractByLlm(
                    llm_spec=cocoindex.llm.LlmSpec(
                        api_type=cocoindex.LlmApiType.OLLAMA, model=ollama_model_name
                    ),
                    instruction=(
                        "Describe the image in one detailed sentence. "
                        "Name all visible animal species, objects, and the main scene. "
                        "Be specific about type, color, and notable features. "
                        "Mention what each animal is doing."
                    ),
                    output_type=str,
                ),
                image=img["content"],
            )
        img["embedding"] = img["content"].transform(embed_image)

        collect_fields = {
            "id": cocoindex.GeneratedField.UUID,
            "filename": img["filename"],
            "embedding": img["embedding"],
        }

        if ollama_model_name is not None:
            print(f"Using Ollama model '{ollama_model_name}' for captioning.")
            collect_fields["caption"] = img["caption"]
        else:
            print(f"No Ollama model '{ollama_model_name}' found — skipping captioning.")

        img_embeddings.collect(**collect_fields)

    img_embeddings.export(
        "img_embeddings",
        cocoindex.targets.Qdrant(collection_name=QDRANT_COLLECTION),
        primary_key_fields=["id"],
    )


@asynccontextmanager
async def lifespan(app: FastAPI) -> None:
    load_dotenv()
    cocoindex.init()
    image_object_embedding_flow.setup(report_to_stdout=True)

    app.state.qdrant_client = QdrantClient(url=QDRANT_URL, prefer_grpc=PREFER_GRPC)

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
        with_payload=True,
    )

    return {
        "results": [
            {
                "filename": result.payload["filename"],
                "score": result.score,
                "caption": result.payload.get("caption"),
            }
            for result in search_results
        ]
    }
