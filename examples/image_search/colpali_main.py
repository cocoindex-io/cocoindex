import datetime
import os
from contextlib import asynccontextmanager
from typing import Any

import cocoindex
from dotenv import load_dotenv
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from qdrant_client import QdrantClient


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
print(f"📐 Using ColPali model {COLPALI_MODEL_NAME}")


@cocoindex.transform_flow()
def text_to_colpali_embedding(
    text: cocoindex.DataSlice[str],
) -> cocoindex.DataSlice[list[list[float]]]:
    """
    Embed text using a ColPali model, returning multi-vector format.
    This is shared logic between indexing and querying, ensuring consistent embeddings.
    """
    return text.transform(
        cocoindex.functions.ColPaliEmbedQuery(model=COLPALI_MODEL_NAME)
    )


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
        img["embedding"] = img["content"].transform(
            cocoindex.functions.ColPaliEmbedImage(model=COLPALI_MODEL_NAME)
        )

        collect_fields = {
            "id": cocoindex.GeneratedField.UUID,
            "filename": img["filename"],
            "embedding": img["embedding"],
        }
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
    # Get the multi-vector embedding for the query
    query_embedding = text_to_colpali_embedding.eval(q)
    print(
        f"🔍 Query multi-vector shape: {len(query_embedding)} tokens x {len(query_embedding[0]) if query_embedding else 0} dims"
    )

    # Search in Qdrant with multi-vector MaxSim scoring using query_points API
    search_results = app.state.qdrant_client.query_points(
        collection_name=QDRANT_COLLECTION,
        query=query_embedding,  # Multi-vector format: list[list[float]]
        using="embedding",  # Specify the vector field name
        limit=limit,
        with_payload=True,
    )

    print(f"📈 Found {len(search_results.points)} results with MaxSim scoring")

    return {
        "results": [
            {
                "filename": result.payload["filename"],
                "score": result.score,
                "caption": result.payload.get("caption"),
            }
            for result in search_results.points
        ]
    }
