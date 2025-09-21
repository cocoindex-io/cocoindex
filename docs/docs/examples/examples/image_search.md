---
title: Image Search App with ColPali and FastAPI
description: Build image search index with ColPali and FastAPI
sidebar_class_name: hidden
slug: /examples/image_search
canonicalUrl: '/examples/image_search'
sidebar_custom_props:
  image: /img/examples/image_search/cover.png
  tags: [vector-index, multi-modal]
image: /img/examples/image_search/cover.png
tags: [vector-index, multi-modal]
---

import { GitHubButton, YouTubeButton, DocumentationButton } from '../../../src/components/GitHubButton';

<GitHubButton url="https://github.com/cocoindex-io/cocoindex/tree/main/examples/image_search" margin="0 0 24px 0" />

![Image Search](/img/examples/image_search/cover.png)

## Overview
CocoIndex supports native integration with ColPali - with just a few lines of code, you embed and index images with ColPali’s late-interaction architecture. We also build a light weight image search application with FastAPI.


## ColPali

**ColPali (Contextual Late-interaction over Patches)** is a powerful model for multimodal retrieval.

It fundamentally rethinks how documents—especially visually complex or image-rich ones—are represented and searched. Instead of reducing each image or page to a single dense vector (as in traditional bi-encoders), ColPali breaks an image into many smaller patches, preserving local spatial and semantic structure. Each patch receives its own embedding, which together form a multi-vector representation of the complete document.

![ColPali Architecture](/img/examples/image_search/multi_modal_architecture.png)


## Flow Overview
![Flow](/img/examples/image_search/flow.png)

1. Ingest image files from the local filesystem
2. Use **ColPali** to embed each image into patch-level multi-vectors
3. Optionally extract image captions using an LLM
4. Export the embeddings (and optional captions) to a Qdrant collection

## Setup
- [Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres) if you don't have one.

- Make sure Qdrant is running
  ```
  docker run -d -p 6334:6334 -p 6333:6333 qdrant/qdrant
  ```


## Add Source

We start by defining a flow to read `.jpg`, `.jpeg`, and `.png` files from a local directory using `LocalFile`.

```python
@cocoindex.flow_def(name="ImageObjectEmbeddingColpali")
def image_object_embedding_flow(flow_builder, data_scope):
    data_scope["images"] = flow_builder.add_source(
        cocoindex.sources.LocalFile(
            path="img",
            included_patterns=["*.jpg", "*.jpeg", "*.png"],
            binary=True
        ),
        refresh_interval=datetime.timedelta(minutes=1),
    )
```

The `add_source` function sets up a table with fields like `filename` and `content`. Images are automatically re-scanned every minute.

<DocumentationButton url="https://cocoindex.io/docs/ops/sources#localfile" text="LocalFile" />


## Process Each Image and Collect the Embedding

We use CocoIndex's built-in `ColPaliEmbedImage` function, which returns a **multi-vector representation** for each image. Each patch receives its own vector, preserving spatial and semantic information.

<DocumentationButton url="https://cocoindex.io/docs/ops/functions#colpaliembedimage" text="ColPaliEmbedImage" margin="0 0 16px 0" />

```python
img_embeddings = data_scope.add_collector()
with data_scope["images"].row() as img:
    img["embedding"] = img["content"].transform(cocoindex.functions.ColPaliEmbedImage(model="vidore/colpali-v1.2"))
    collect_fields = {
        "id": cocoindex.GeneratedField.UUID,
        "filename": img["filename"],
        "embedding": img["embedding"],
    }
    img_embeddings.collect(**collect_fields)
```

This transformation turns the raw image bytes into a list of vectors — one per patch — that can later be used for **late interaction search**. And then we collect the embeddings.

![ColPali Embedding](/img/examples/image_search/embedding.png)

## Export the Embeddings

```python
img_embeddings.export(
    "img_embeddings",
    cocoindex.targets.Qdrant(collection_name="ImageSearchColpali"),
    primary_key_fields=["id"],
)
```

This creates a vector collection in Qdrant that supports **multi-vector fields** — required for ColPali-style late interaction search.


## Enable Real-Time Indexing

To keep the image index up to date automatically, we wrap the flow in a `FlowLiveUpdater`:

```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    load_dotenv()
    cocoindex.init()
    image_object_embedding_flow.setup(report_to_stdout=True)
    app.state.live_updater = cocoindex.FlowLiveUpdater(image_object_embedding_flow)
    app.state.live_updater.start()
    yield
```

This keeps your vector index fresh as new images arrive.

## Fast API Application

We build a simple FastAPI application to query the index.

```python
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
```

## Search API & Query the index

We use `ColPaliEmbedQuery` to embed the query text into a multi-vector format.

<DocumentationButton url="https://cocoindex.io/docs/ops/functions#colpaliembedquery" text="ColPaliEmbedQuery" margin="0 0 16px 0" />

```python
@cocoindex.transform_flow()
def text_to_colpali_embedding(
    text: cocoindex.DataSlice[str],
) -> cocoindex.DataSlice[list[list[float]]]:
    return text.transform(
        cocoindex.functions.ColPaliEmbedQuery(model=COLPALI_MODEL_NAME)
    )
```
Then we build a search API to query the index.

```python
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
```

## Run the application

- Install dependencies:
  ```
  pip install -e .
  pip install 'cocoindex[colpali]'  # Adds ColPali support
  ```

- Configure model (optional):
  ```sh
  # All ColVision models supported by colpali-engine are available
  # See https://github.com/illuin-tech/colpali#list-of-colvision-models for the complete list

  # ColPali models (colpali-*) - PaliGemma-based, best for general document retrieval
  export COLPALI_MODEL="vidore/colpali-v1.2"  # Default model
  export COLPALI_MODEL="vidore/colpali-v1.3"  # Latest version

  # ColQwen2 models (colqwen-*) - Qwen2-VL-based, excellent for multilingual text (29+ languages) and general vision
  export COLPALI_MODEL="vidore/colqwen2-v1.0"
  export COLPALI_MODEL="vidore/colqwen2.5-v0.2"  # Latest Qwen2.5 model

  # ColSmol models (colsmol-*) - Lightweight, good for resource-constrained environments
  export COLPALI_MODEL="vidore/colSmol-256M"

  # Any other ColVision models from https://github.com/illuin-tech/colpali are supported
  ```

- Run ColPali Backend:
  ```
  uvicorn colpali_main:app --reload --host 0.0.0.0 --port 8000
  ```
    :::warning
    Note that recent Nvidia GPUs (such as the RTX 5090) are not supported by the stable PyTorch version up to 2.7.1.
    :::

    If you get this error:

    ```
    The current PyTorch install supports CUDA capabilities sm_37 sm_50 sm_60 sm_61 sm_70 sm_75 sm_80 sm_86 sm_90 compute_37.
    ```

    You can install the nightly pytorch build here: https://pytorch.org/get-started/locally/

    ```sh
    pip install --pre torch torchvision torchaudio --index-url https://download.pytorch.org/whl/nightly/cu129
    ```
- Run Frontend:
  ```
  cd frontend
  npm install
  npm run dev
  ```

 Go to `http://localhost:5173` to search. The frontend works with both backends identically.

 ![Result](/img/examples/image_search/result.png)

## CLIP Model & Comparison with ColPali
We've also had a similar application built with CLIP model.

<DocumentationButton url="https://cocoindex.io/blogs/live-image-search" text="Image Search App with CLIP" margin="0 0 16px 0" />

In general,
- CLIP: Faster, good for general image-text matching
- ColPali: More accurate for document images and text-heavy content, supports multi-vector late interaction for better precision

## Connect to Any Data Source

One of CocoIndex’s core strengths is its ability to connect to your existing data sources and automatically keep your index fresh. Beyond local files, CocoIndex natively supports source connectors including:

- Google Drive
- Amazon S3 / SQS
- Azure Blob Storage

<DocumentationButton url="https://cocoindex.io/docs/ops/sources" text="Sources" margin="0 0 16px 0" />

Once connected, CocoIndex continuously watches for changes — new uploads, updates, or deletions — and applies them to your index in real time.
