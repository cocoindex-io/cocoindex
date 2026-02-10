---
title: LiteLLM Embedding
description: CocoIndex integration with LiteLLM for text embeddings, supporting 100+ providers through a unified API.
---

# LiteLLM Embedding

The `cocoindex.ops.litellm` module provides integration with the [LiteLLM](https://docs.litellm.ai/) library for text embeddings.

## Overview

The `LiteLLMEmbedder` class is a wrapper around LiteLLM's embedding API that:

- Implements `VectorSchemaProvider` for seamless integration with CocoIndex connectors
- Supports 100+ embedding providers (OpenAI, Azure, Vertex AI, Cohere, Bedrock, etc.) through a unified API
- Provides a simple async `embed()` method
- Passes through all additional arguments to the LiteLLM embedding API
- Returns properly typed numpy arrays

## Installation

To use LiteLLM with CocoIndex, install with the `litellm` extra:

```bash
pip install cocoindex[litellm]
```

Or with uv:

```bash
uv pip install cocoindex[litellm]
```

## Basic usage

### Creating an embedder

All extra keyword arguments are passed through to every `litellm.aembedding` call. See [Supported providers](#supported-providers) for provider-specific model strings and configuration.

```python
from cocoindex.ops.litellm import LiteLLMEmbedder

embedder = LiteLLMEmbedder("text-embedding-3-small")

# With explicit API key and base URL
embedder = LiteLLMEmbedder("text-embedding-3-small", api_key="sk-...", api_base="https://my-proxy.example.com")

# With custom dimensions (OpenAI text-embedding-3 models)
embedder = LiteLLMEmbedder("text-embedding-3-small", dimensions=512)

# With a timeout (seconds)
embedder = LiteLLMEmbedder("text-embedding-3-small", timeout=30)
```

### Embedding text

The `embed()` method converts text into a `numpy.ndarray` of `float32`. It's an async method — use `await` when calling it:

```python
# In a CocoIndex function
embedding = await embedder.embed("Hello, world!")

# Use the embedding in a dataclass row, store in a vector database, etc.
table.declare_row(row=DocEmbedding(text="Hello, world!", embedding=embedding))
```

### Using as a type annotation

The `LiteLLMEmbedder` implements [`VectorSchemaProvider`](../resource_types.md#vectorschemaprovider), which means it can be used directly as metadata in `Annotated` type annotations. This is the recommended way to declare vector columns — CocoIndex connectors automatically extract the vector dimension and dtype from the annotation when creating tables.

```python
from dataclasses import dataclass
from typing import Annotated
from numpy.typing import NDArray

embedder = LiteLLMEmbedder("text-embedding-3-small")

@dataclass
class DocEmbedding:
    id: int
    filename: str
    text: str
    embedding: Annotated[NDArray, embedder]
```

When you pass this dataclass to a connector's `TableSchema.from_class()`, the connector automatically reads the embedder annotation to determine the vector column's dimension and dtype. For example, with Postgres:

```python
from cocoindex.connectors import postgres

table_schema = await postgres.TableSchema.from_class(
    DocEmbedding,
    primary_key=["id"],
)
target_table = await coco_aio.mount_run(
    coco.component_subpath("setup", "table"),
    target_db.declare_table_target,
    table_name="doc_embeddings",
    table_schema=table_schema,
    pg_schema_name="my_schema",
).result()
```

The connector automatically creates the appropriate `vector(N)` column. See the [Connectors](../connectors/postgres.md) docs for other supported backends (LanceDB, Qdrant, SQLite).

## Supported providers

Below are common providers with their model strings and configuration. The `litellm` module is re-exported from `cocoindex.ops.litellm` for setting provider-specific variables. See the [LiteLLM embedding docs](https://docs.litellm.ai/docs/embedding/supported_embedding) for the full list.

### OpenAI

| Model | Model string |
|-------|-------------|
| Text Embedding 3 Small | `text-embedding-3-small` |
| Text Embedding 3 Large | `text-embedding-3-large` |
| Text Embedding Ada 002 | `text-embedding-ada-002` |

**Environment variables:** `OPENAI_API_KEY`

```python
embedder = LiteLLMEmbedder("text-embedding-3-small")
```

### Azure OpenAI

| Model | Model string |
|-------|-------------|
| Text Embedding 3 Small | `azure/<your-deployment-name>` |
| Text Embedding Ada 002 | `azure/<your-deployment-name>` |

The model string uses your Azure deployment name, not the OpenAI model name.

**Environment variables:** `AZURE_API_KEY`, `AZURE_API_BASE`, `AZURE_API_VERSION`

```python
embedder = LiteLLMEmbedder(
    "azure/my-deployment-name",
    api_key="your-azure-api-key",
    api_base="https://my-resource.openai.azure.com",
    api_version="2024-02-01",
)
```

### Gemini (Google AI Studio)

| Model | Model string |
|-------|-------------|
| Text Embedding 004 | `gemini/text-embedding-004` |

**Environment variables:** `GEMINI_API_KEY`

```python
embedder = LiteLLMEmbedder("gemini/text-embedding-004")
```

### Vertex AI

| Model | Model string |
|-------|-------------|
| Text Embedding 004 | `vertex_ai/text-embedding-004` |
| Text Multilingual Embedding 002 | `vertex_ai/text-multilingual-embedding-002` |
| Textembedding Gecko | `vertex_ai/textembedding-gecko` |

**Environment variables:** `GOOGLE_APPLICATION_CREDENTIALS` (path to service account JSON)

**Additional configuration:** Set project and location via the `litellm` module or environment variables `VERTEXAI_PROJECT` and `VERTEXAI_LOCATION`:

```python
from cocoindex.ops.litellm import LiteLLMEmbedder, litellm

litellm.vertex_project = "my-gcp-project"
litellm.vertex_location = "us-central1"

embedder = LiteLLMEmbedder("vertex_ai/text-embedding-004")
```

### AWS Bedrock

| Model | Model string |
|-------|-------------|
| Titan Text Embeddings V2 | `bedrock/amazon.titan-embed-text-v2:0` |
| Titan Text Embeddings V1 | `bedrock/amazon.titan-embed-text-v1` |
| Cohere Embed English | `bedrock/cohere.embed-english-v3` |
| Cohere Embed Multilingual | `bedrock/cohere.embed-multilingual-v3` |

**Environment variables:** `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION_NAME`

```python
embedder = LiteLLMEmbedder("bedrock/amazon.titan-embed-text-v2:0")
```

### Mistral AI

| Model | Model string |
|-------|-------------|
| Mistral Embed | `mistral/mistral-embed` |

**Environment variables:** `MISTRAL_API_KEY`

```python
embedder = LiteLLMEmbedder("mistral/mistral-embed")
```

### Voyage AI

| Model | Model string |
|-------|-------------|
| Voyage 3.5 | `voyage/voyage-3.5` |
| Voyage 3.5 Lite | `voyage/voyage-3.5-lite` |
| Voyage Code 3 | `voyage/voyage-code-3` |

**Environment variables:** `VOYAGE_API_KEY`

```python
embedder = LiteLLMEmbedder("voyage/voyage-3.5")
```

### Cohere

| Model | Model string |
|-------|-------------|
| Embed English V3 | `cohere/embed-english-v3.0` |
| Embed English Light V3 | `cohere/embed-english-light-v3.0` |
| Embed Multilingual V3 | `cohere/embed-multilingual-v3.0` |

**Environment variables:** `COHERE_API_KEY`

**Additional configuration:** V3 models require an `input_type` parameter (defaults to `"search_document"`; use `"search_query"` for queries):

```python
embedder = LiteLLMEmbedder("cohere/embed-english-v3.0", input_type="search_document")
```

### Nebius AI

| Model | Model string |
|-------|-------------|
| BGE EN ICL | `nebius/BAAI/bge-en-icl` |

**Environment variables:** `NEBIUS_API_KEY`

```python
embedder = LiteLLMEmbedder("nebius/BAAI/bge-en-icl")
```
