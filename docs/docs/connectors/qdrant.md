---
title: Qdrant
toc_max_heading_level: 4
description: CocoIndex connector for reading from and writing to Qdrant vector databases.
---

The `qdrant` connector provides utilities for writing points to Qdrant vector databases, with support for both single and named vectors, as well as multi-vector configurations.

```python
from cocoindex.connectors import qdrant
```

:::note Dependencies
This connector requires additional dependencies. Install with:

```bash
pip install cocoindex[qdrant]
```

:::

## Connection setup

`create_client()` creates a Qdrant client connection with optional gRPC support.

```python
def create_client(
    url: str,
    *,
    prefer_grpc: bool = True,
    **kwargs: Any,
) -> QdrantClient
```

**Parameters:**

- `url` — Qdrant server URL (e.g., `"http://localhost:6333"`).
- `prefer_grpc` — Whether to prefer gRPC over HTTP (default: `True`).
- `**kwargs` — Additional arguments passed directly to `QdrantClient`.

**Returns:** A Qdrant client instance.

**Example:**

```python
client = qdrant.create_client("http://localhost:6333")
```

## As target

The `qdrant` connector provides target state APIs for writing points to collections. CocoIndex tracks what points should exist and automatically handles upserts and deletions.

### Declaring target states

#### Database registration

Before declaring target states, register the Qdrant client with a stable key that identifies the logical database. This key allows CocoIndex to recognize the same database even when connection details change.

```python
def register_db(key: str, client: QdrantClient) -> QdrantDatabase
```

**Parameters:**

- `key` — A stable identifier for this database (e.g., `"vector_db"`). Must be unique.
- `client` — A Qdrant client instance.

**Returns:** A `QdrantDatabase` handle for declaring target states.

**Example:**

```python
client = qdrant.create_client("http://localhost:6333")
db = qdrant.register_db("my_vectors", client)
```

#### Collections (parent state)

Declares a collection as a target state. Returns a `CollectionTarget` for declaring points.

```python
def QdrantDatabase.declare_collection_target(
    self,
    collection_name: str,
    schema: CollectionSchema,
    *,
    managed_by: Literal["system", "user"] = "system",
) -> CollectionTarget[coco.PendingS]
```

**Parameters:**

- `collection_name` — Name of the collection.
- `schema` — Schema definition specifying vector configurations (see [Collection Schema](#collection-schema)).
- `managed_by` — Whether CocoIndex manages the collection lifecycle (`"system"`) or assumes it exists (`"user"`).

**Returns:** A pending `CollectionTarget`. Use the convenience wrapper `await db.mount_collection_target(collection_name=..., schema=...)` to resolve.

#### Points (child states)

Once a `CollectionTarget` is resolved, declare points to be upserted using `qdrant.PointStruct`, which is an alias of `qdrant_client.http.models.PointStruct`:

```python
def CollectionTarget.declare_point(
    self,
    point: qdrant.PointStruct,
) -> None
```

**Parameters:**

- `point` — A `qdrant.PointStruct` (alias of `qdrant_client.http.models.PointStruct`) containing:
  - `id` — Point ID (str, int, or UUID)
  - `vector` — Vector data (single vector or dict of named vectors)
  - `payload` — Optional metadata as a JSON-serializable dict

### Collection schema

Define vector configurations for a collection using `CollectionSchema`. Unlike row-oriented databases, Qdrant uses a point-oriented model where each point has schemaless payload and one or more vectors with predefined dimensions.

```python
class CollectionSchema:
    @classmethod
    async def create(
        cls,
        vectors: QdrantVectorDef | dict[str, QdrantVectorDef],
    ) -> CollectionSchema
```

**Parameters:**

- `vectors` — Either:
  - A single `QdrantVectorDef` for an unnamed vector
  - A dict mapping vector names to `QdrantVectorDef` for named vectors

#### QdrantVectorDef

Specifies vector configuration including dimension, distance metric, and multi-vector settings:

```python
class QdrantVectorDef(NamedTuple):
    schema: VectorSchemaProvider | MultiVectorSchemaProvider
    distance: Literal["cosine", "dot", "euclid"] = "cosine"
    multivector_comparator: Literal["max_sim"] = "max_sim"
```

**Parameters:**

- `schema` — A `VectorSchemaProvider` or `MultiVectorSchemaProvider` that defines vector dimensions
- `distance` — Distance metric for similarity search (default: `"cosine"`)
- `multivector_comparator` — Comparator for multi-vector fields (only applies to `MultiVectorSchemaProvider`)

#### Single (unnamed) vector

For collections with a single unnamed vector:

```python
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder

embedder = SentenceTransformerEmbedder("sentence-transformers/all-MiniLM-L6-v2")

schema = await qdrant.CollectionSchema.create(
    vectors=qdrant.QdrantVectorDef(schema=embedder)
)
```

Points use the vector directly:

```python
point = qdrant.PointStruct(
    id="doc-123",
    vector=embedding.tolist(),  # Single vector
    payload={"text": "...", "metadata": {...}},
)
```

#### Named vectors

For collections with multiple named vectors:

```python
from cocoindex.resources.schema import VectorSchema
import numpy as np

schema = await qdrant.CollectionSchema.create(
    vectors={
        "text_embedding": qdrant.QdrantVectorDef(
            schema=VectorSchema(dtype=np.float32, size=384),
            distance="cosine",
        ),
        "image_embedding": qdrant.QdrantVectorDef(
            schema=VectorSchema(dtype=np.float32, size=512),
            distance="dot",
        ),
    }
)
```

Points use a dict of vectors:

```python
point = qdrant.PointStruct(
    id="doc-123",
    vector={
        "text_embedding": text_vec.tolist(),
        "image_embedding": image_vec.tolist(),
    },
    payload={"text": "...", "metadata": {...}},
)
```

#### VectorSchemaProvider

Vector dimensions are typically determined by the embedding model. By using a `VectorSchemaProvider`, the dimension is derived automatically from the source configuration.

A `VectorSchemaProvider` can be:

- **An embedding model** (e.g., [`SentenceTransformerEmbedder`](../ops/sentence_transformers.md)) — dimension is inferred from the model
- **A `VectorSchema`** — for explicit size and dtype when not using an embedder

```python
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder

embedder = SentenceTransformerEmbedder("sentence-transformers/all-MiniLM-L6-v2")

schema = await qdrant.CollectionSchema.create(
    vectors=qdrant.QdrantVectorDef(schema=embedder)  # dimension inferred (384)
)
```

Or with explicit configuration:

```python
from cocoindex.resources.schema import VectorSchema
import numpy as np

schema = await qdrant.CollectionSchema.create(
    vectors=qdrant.QdrantVectorDef(
        schema=VectorSchema(dtype=np.float32, size=384)
    )
)
```

#### Multi-vector support

For multi-vector configurations (multiple vectors per point stored together):

```python
from cocoindex.resources.schema import MultiVectorSchema, VectorSchema
import numpy as np

schema = await qdrant.CollectionSchema.create(
    vectors=qdrant.QdrantVectorDef(
        schema=MultiVectorSchema(
            vector_schema=VectorSchema(dtype=np.float32, size=384)
        ),
        multivector_comparator="max_sim",
    )
)
```

### Distance metrics

The `distance` parameter in `QdrantVectorDef` specifies the similarity metric:

- `"cosine"` — Cosine similarity (default, normalized dot product)
- `"dot"` — Dot product similarity
- `"euclid"` — Euclidean distance (L2)

### Example: single vector

```python
import cocoindex as coco
from cocoindex.connectors import qdrant
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder
from typing import AsyncIterator

QDRANT_URL = "http://localhost:6333"
QDRANT_DB = coco.ContextKey[qdrant.QdrantDatabase]("qdrant_db")

embedder = SentenceTransformerEmbedder("sentence-transformers/all-MiniLM-L6-v2")

@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    client = qdrant.create_client(QDRANT_URL)
    builder.provide(QDRANT_DB, qdrant.register_db("main_vectors", client))
    yield

@coco.function
async def process_document(
    doc_id: str,
    text: str,
    target: qdrant.CollectionTarget,
) -> None:
    embedding = await embedder.embed(text)

    point = qdrant.PointStruct(
        id=doc_id,
        vector=embedding.tolist(),
        payload={"text": text},
    )
    target.declare_point(point)

@coco.function
async def app_main() -> None:
    db = coco.use_context(QDRANT_DB)

    # Declare collection target state
    collection = await db.mount_collection_target(
        collection_name="documents",
        schema=await qdrant.CollectionSchema.create(
            vectors=qdrant.QdrantVectorDef(schema=embedder)
        ),
    )

    # Declare points
    for doc_id, text in documents:
        await coco.mount(
            coco.component_subpath("doc", doc_id),
            process_document,
            doc_id,
            text,
            collection,
        )
```

### Example: named vectors

```python
from cocoindex.resources.schema import VectorSchema
import numpy as np

@coco.function
async def app_main() -> None:
    db = coco.use_context(QDRANT_DB)

    collection = await db.mount_collection_target(
        collection_name="multimodal_docs",
        schema=await qdrant.CollectionSchema.create(
            vectors={
                "text": qdrant.QdrantVectorDef(
                    schema=text_embedder,
                    distance="cosine",
                ),
                "image": qdrant.QdrantVectorDef(
                    schema=VectorSchema(dtype=np.float32, size=512),
                    distance="dot",
                ),
            }
        ),
    )

    # Declare points with named vectors
    for doc in documents:
        point = qdrant.PointStruct(
            id=doc.id,
            vector={
                "text": doc.text_embedding.tolist(),
                "image": doc.image_embedding.tolist(),
            },
            payload={"title": doc.title, "url": doc.url},
        )
        collection.declare_point(point)
```

## Point IDs

Qdrant supports the following point ID types:

- `str` — String identifiers
- `int` — Integer identifiers (unsigned 64-bit)
- `uuid.UUID` — UUID identifiers (converted to string)

All other types are converted to strings automatically.

## Payloads

Point payloads are schemaless JSON objects. Any JSON-serializable Python data structure can be used:

```python
payload = {
    "text": "Document content",
    "metadata": {
        "author": "Alice",
        "tags": ["machine-learning", "nlp"],
        "published": "2024-01-15",
    },
    "stats": {
        "views": 1500,
        "likes": 42,
    },
}
```

## Vector search

The connector focuses on writing points to Qdrant. For vector search, use the Qdrant client directly:

```python
from qdrant_client.http import models as qdrant_models

# Get the registered client
client = qdrant.create_client("http://localhost:6333")

# Perform search
results = client.search(
    collection_name="documents",
    query_vector=query_embedding.tolist(),
    limit=10,
)

for result in results:
    print(f"Score: {result.score}, ID: {result.id}")
    print(f"Payload: {result.payload}")
```

For named vectors:

```python
results = client.search(
    collection_name="documents",
    query_vector=("text", query_embedding.tolist()),  # Search using "text" vector
    limit=10,
)
```
