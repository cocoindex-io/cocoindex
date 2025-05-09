from dotenv import load_dotenv
import cocoindex
import os
import requests
import base64
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from cocoindex.lib import main_fn

load_dotenv(override=True)

OLLAMA_URL = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "gemma3"

# 1. Extract caption from image using Ollama vision model
@cocoindex.op.function()
def get_image_caption(img_bytes: bytes) -> str:
    """
    Use Ollama's gemma3 model to extract a detailed caption from an image.
    Returns a full-sentence natural language description of the image.
    """
    img_b64 = base64.b64encode(img_bytes).decode("utf-8")
    prompt = (
        "Describe this image in one detailed, natural language sentence. "
        "Always explicitly name every visible animal species, object, and the main scene. "
        "Be specific about the type, color, and any distinguishing features. "
        "Avoid generic words like 'animal' or 'creature'—always use the most precise name (e.g., 'elephant', 'cat', 'lion', 'zebra'). "
        "If an animal is present, mention its species and what it is doing. "
        "For example: 'A large grey elephant standing in a grassy savanna, with trees in the background.'"
    )
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "images": [img_b64],
        "stream": False,
    }
    resp = requests.post(OLLAMA_URL, json=payload)
    resp.raise_for_status()
    result = resp.json()
    text = result.get("response", "")
    text = text.strip().replace("\n", "").rstrip(".")
    return text


# 2. Embed the caption string
def caption_to_embedding(caption: cocoindex.DataSlice) -> cocoindex.DataSlice:
    return caption.transform(
        cocoindex.functions.SentenceTransformerEmbed(
            model="clip-ViT-L-14",
        )
    )

# 3. CocoIndex flow: Ingest images, extract captions, embed, export to Qdrant
@cocoindex.flow_def(name="ImageObjectEmbedding")
def image_object_embedding_flow(flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope):
    data_scope["images"] = flow_builder.add_source(
        cocoindex.sources.LocalFile(path="img", included_patterns=["*.jpg", "*.jpeg", "*.png"], binary=True)
    )
    img_embeddings = data_scope.add_collector()
    with data_scope["images"].row() as img:
        img["caption"] = img["content"].transform(get_image_caption)
        img["embedding"] = caption_to_embedding(img["caption"])
        img_embeddings.collect(
            id=cocoindex.GeneratedField.UUID,
            filename=img["filename"],
            caption=img["caption"],
            embedding=img["embedding"],
        )
    img_embeddings.export(
        "img_embeddings",
        cocoindex.storages.Qdrant(
            collection_name="image_search",
            grpc_url=os.getenv("QDRANT_GRPC_URL", "http://localhost:6334/"),
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
    settings = cocoindex.setting.Settings.from_env()
    cocoindex.init(settings)
    app.state.query_handler = cocoindex.query.SimpleSemanticsQueryHandler(
        name="ImageObjectSearch",
        flow=image_object_embedding_flow,
        target_name="img_embeddings",
        query_transform_flow=caption_to_embedding,
        default_similarity_metric=cocoindex.VectorSimilarityMetric.COSINE_SIMILARITY,
    )

@app.get("/search")
def search(q: str = Query(..., description="Search query"), limit: int = Query(5, description="Number of results")):
    query_handler = app.state.query_handler
    results, _ = query_handler.search(q, limit, "embedding")
    out = []
    for result in results:
        row = dict(result.data)
        # Only include filename and score
        out.append({
            "filename": row["filename"],
            "score": result.score
        })
    return {"results": out}

# --- CLI entrypoint ---
@main_fn()
def _run():
    pass
    
if __name__ == "__main__":
    _run()