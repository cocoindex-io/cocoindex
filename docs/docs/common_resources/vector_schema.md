---
title: Vector Schema
description: Types for describing vector columns — VectorSchema, VectorSchemaProvider, and how to annotate dataclass fields for automatic vector column configuration.
---

# Vector Schema

The schema module (`cocoindex.resources.schema`) defines types that describe vector columns. CocoIndex connectors use these to automatically configure the correct column type (e.g., `vector(384)` in Postgres, `fixed_size_list<float32>(384)` in LanceDB).

## VectorSchema

A frozen dataclass that describes a vector column's dtype and dimension.

```python
from cocoindex.resources.schema import VectorSchema
import numpy as np

schema = VectorSchema(dtype=np.dtype(np.float32), size=768)
```

**Fields:**

- `dtype` — NumPy dtype of each element (e.g., `np.float32`)
- `size` — Number of dimensions in the vector (e.g., `384`)

You can construct `VectorSchema` directly when using a custom embedding model that doesn't implement `VectorSchemaProvider`:

```python
from cocoindex.resources.schema import VectorSchema

# For a custom CLIP model with known dimension
schema = VectorSchema(dtype=np.dtype(np.float32), size=768)

# Use it in a Qdrant vector definition
QDRANT_DB = coco.ContextKey[QdrantClient]("my_qdrant_db")
target_collection = await qdrant.mount_collection_target(
    QDRANT_DB,
    collection_name="image_search",
    schema=await qdrant.CollectionSchema.create(
        vectors=qdrant.QdrantVectorDef(schema=schema, distance="cosine")
    ),
)
```

## VectorSchemaProvider

A protocol for objects that can provide vector schema information. The primary use case is as metadata in `Annotated` type annotations — connectors extract vector column configuration from the annotation automatically.

Any object that implements the `__coco_vector_schema__()` method satisfies this protocol. The built-in [`SentenceTransformerEmbedder`](../ops/sentence_transformers.md) implements it.

There are three ways to specify vector schema in annotations:

### Using a `ContextKey` (recommended)

Define a [`ContextKey`](../programming_guide/context.md) for the embedder and use it as the annotation. The connector resolves the key at schema creation time. This is the recommended approach because the embedder is configured once in the lifespan and shared across all functions via context.

```python
import cocoindex as coco
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder

EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
EMBEDDER = coco.ContextKey[SentenceTransformerEmbedder]("embedder")

@dataclass
class DocEmbedding:
    id: int
    text: str
    embedding: Annotated[NDArray, EMBEDDER]  # dimension resolved from context

# In lifespan, provide the embedder:
@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    builder.provide(EMBEDDER, SentenceTransformerEmbedder(EMBED_MODEL))
    yield

# In coco functions, access the embedder:
embedding = await coco.use_context(EMBEDDER).embed(text)
```

### Using a `VectorSchemaProvider` instance

Pass an embedder instance directly as the annotation. Simpler for scripts where the embedder is a module-level constant.

```python
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder

embedder = SentenceTransformerEmbedder("sentence-transformers/all-MiniLM-L6-v2")

@dataclass
class DocEmbedding:
    id: int
    text: str
    embedding: Annotated[NDArray, embedder]  # dimension inferred from model (384)
```

### Using a `VectorSchema`

Specify dimension and dtype explicitly. Useful when using a custom embedding model that doesn't implement `VectorSchemaProvider`.

```python
from cocoindex.resources.schema import VectorSchema

@dataclass
class ImageEmbedding:
    id: int
    embedding: Annotated[NDArray, VectorSchema(dtype=np.dtype(np.float32), size=768)]
```

When a connector's `TableSchema.from_class()` encounters an `Annotated[NDArray, annotation]` field, it resolves the annotation — unwrapping `ContextKey` if needed — and calls `__coco_vector_schema__()` to determine the column's dimension and dtype.

## MultiVectorSchema / MultiVectorSchemaProvider

Analogous types for multi-vector columns (e.g., ColBERT-style token-level embeddings). `MultiVectorSchema` wraps a `VectorSchema` describing the individual vectors. Used by connectors like [Qdrant](../connectors/qdrant.md) that support multi-vector storage.

```python
from cocoindex.resources.schema import MultiVectorSchema, VectorSchema

multi_schema = MultiVectorSchema(
    vector_schema=VectorSchema(dtype=np.dtype(np.float32), size=128)
)
```
