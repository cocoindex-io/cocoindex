---
title: Index Images with ColPali
description: Build image search index with ColPali and FastAPI
sidebar_class_name: hidden
slug: /examples/image_search
canonicalUrl: '/examples/image_search'
sidebar_custom_props:
  image: /img/examples/image_search/cover.png
  tags: [vector-index, multi-modal]
tags: [vector-index, multi-modal]
---

import { GitHubButton, YouTubeButton } from '../../../src/components/GitHubButton';

<GitHubButton url="https://github.com/cocoindex-io/cocoindex/tree/main/examples/image_search"/>

## Overview

CocoIndex now supports native integration with ColPali — enabling multi-vector, patch-level image indexing using cutting-edge multimodal models. With just a few lines of code, you can now embed and index images with ColPali’s late-interaction architecture, fully integrated into CocoIndex’s composable flow system.


## Why ColPali for Indexing?

**ColPali (Contextual Late-interaction over Patches)** is a powerful model for multimodal retrieval.

It fundamentally rethinks how documents—especially visually complex or image-rich ones—are represented and searched. Instead of reducing each image or page to a single dense vector (as in traditional bi-encoders), ColPali breaks an image into many smaller patches, preserving local spatial and semantic structure. Each patch receives its own embedding, which together form a multi-vector representation of the complete document.


## Declare an Image Indexing Flow with CocoIndex


In this example, we will use CocoIndex to index images with ColPali, and Qdrant to store and retrieve the embeddings.


This flow illustrates how we’ll process and index images using ColPali:

1. Ingest image files from the local filesystem
2. Use **ColPali** to embed each image into patch-level multi-vectors
3. Optionally extract image captions using an LLM
4. Export the embeddings (and optional captions) to a Qdrant collection

Check out the full working code [here](https://github.com/cocoindex-io/cocoindex/blob/main/examples/image_search/colpali_main.py).

:star: Star [CocoIndex on GitHub](https://github.com/cocoindex-io/cocoindex) if you like it!


### 1. Ingest the Images

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


### 2. Process Each Image and Collect the Embedding

### 2.1 Embed the Image with ColPali

We use CocoIndex's built-in `ColPaliEmbedImage` function, which returns a **multi-vector representation** for each image. Each patch receives its own vector, preserving spatial and semantic information.

```python
img_embeddings = data_scope.add_collector()
with data_scope["images"].row() as img:
    img["embedding"] = img["content"].transform(cocoindex.functions.ColPaliEmbedImage(model="vidore/colpali-v1.2"))
```

This transformation turns the raw image bytes into a list of vectors — one per patch — that can later be used for **late interaction search**.


### 3. Collect and Export the Embeddings

Once we’ve processed each image, we collect its metadata and embedding and send it to Qdrant.

```python
collect_fields = {
    "id": cocoindex.GeneratedField.UUID,
    "filename": img["filename"],
    "embedding": img["embedding"],
}
img_embeddings.collect(**collect_fields)
```

Then we export to Qdrant using the `Qdrant` target:

```python
img_embeddings.export(
    "img_embeddings",
    cocoindex.targets.Qdrant(collection_name="ImageSearchColpali"),
    primary_key_fields=["id"],
)
```

This creates a vector collection in Qdrant that supports **multi-vector fields** — required for ColPali-style late interaction search.


### 4. Enable Real-Time Indexing

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


## What’s Actually Stored?

Unlike typical image search pipelines that store one global vector per image, ColPali stores:

```python
Vector[Vector[Float32, N]]
```

Where:

- The outer dimension is the **number of patches**
- The inner dimension is the **model’s hidden size**

This makes the index **multi-vector ready**, and compatible with late-interaction query strategies — like MaxSim or learned fusion.


## Real-Time Indexing with Live Updater

You can also attach CocoIndex’s `FlowLiveUpdater` to your FastAPI or any Python app to keep your ColPali index synced in real time:

```python
from fastapi import FastAPI
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    load_dotenv()
    cocoindex.init()
    image_object_embedding_flow.setup(report_to_stdout=True)
    app.state.live_updater = cocoindex.FlowLiveUpdater(image_object_embedding_flow)
    app.state.live_updater.start()
    yield

```

## Retrivel and application

Refer to this example on Query and application building:
https://cocoindex.io/blogs/live-image-search#3-query-the-index

Make sure we use ColPali to embed the query

```python
@app.get("/search")
def search(
    q: str = Query(..., description="Search query"),
    limit: int = Query(5, description="Number of results"),
) -> Any:
    # Get the multi-vector embedding for the query
    query_embedding = text_to_colpali_embedding.eval(q)

```

Full working code is available [here](https://github.com/cocoindex-io/cocoindex/blob/main/examples/image_search/colpali_main.py).

Check it out for yourself! It is fun :) In this image search example, the results look better compared to [using CLIP](http://localhost:3000/blogs/live-image-search) with a single dense vector (1D embedding).
ColPali produces richer and more fine-grained retrieval.


## Built with Flexibility in Mind

Whether you’re working on:

- Visual RAG
- Multimodal retrieval systems
- Fine-grained visual search tools
- Or want to bring image understanding to your AI agent workflows

[CocoIndex](https://github.com/cocoindex-io/cocoindex) + ColPali gives you a modular, modern foundation to build from.

## Connect to Any Data Source — and Keep It in Sync

One of CocoIndex’s core strengths is its ability to connect to your existing data sources and automatically keep your index fresh.
Beyond local files, CocoIndex natively supports source connectors including:

- Google Drive
- Amazon S3 / SQS
- Azure Blob Storage

See documentation [here](https://cocoindex.io/docs/ops/sources).

Once connected, CocoIndex continuously watches for changes — new uploads, updates, or deletions — and applies them to your index in real time.

## Support us

We’re constantly adding more examples and improving our runtime.
If you found this helpful, please ⭐ star [CocoIndex on GitHub](https://github.com/cocoindex-io/cocoindex) and share it with others.