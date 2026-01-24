# Sentence Transformers Integration

The `cocoindex.extras.sentence_transformers` module provides integration with the [sentence-transformers](https://www.sbert.net/) library for text embeddings.

## Overview

The `SentenceTransformerEmbedder` class is a wrapper around SentenceTransformer models that:

- Implements `VectorSchemaProvider` for seamless integration with CocoIndex connectors
- Handles model caching and thread-safe GPU access automatically
- Provides simple `embed()` and `embed_async()` methods
- Returns properly typed numpy arrays

## Installation

To use sentence transformers with CocoIndex, install with the `sentence_transformers` extra:

```bash
pip install cocoindex[sentence_transformers]
```

Or with uv:

```bash
uv pip install cocoindex[sentence_transformers]
```

## Basic Usage

### Creating an Embedder

```python
from cocoindex.extras.sentence_transformers import SentenceTransformerEmbedder

# Initialize embedder with a pre-trained model
embedder = SentenceTransformerEmbedder("sentence-transformers/all-MiniLM-L6-v2")
```

### Embedding Text

```python
# Embed a single text (returns 1D array)
embedding = embedder.embed("Hello, world!")
print(f"Shape: {embedding.shape}")  # Shape: (384,)
print(f"Dtype: {embedding.dtype}")  # Dtype: float32

# Async embedding
import asyncio

async def embed_async_example():
    embedding = await embedder.embed_async("Hello, world!")
    return embedding

embedding = asyncio.run(embed_async_example())
```

### Getting Vector Schema

The embedder automatically provides vector schema information:

```python
schema = embedder.__coco_vector_schema__()
print(f"Dimension: {schema.size}")  # 384
print(f"Dtype: {schema.dtype}")     # float32
```

## Using with CocoIndex Connectors

The `SentenceTransformerEmbedder` implements `VectorSchemaProvider`, which allows it to be used directly in type annotations with CocoIndex connectors.

### With Postgres

```python
from dataclasses import dataclass
from typing import Annotated
from numpy.typing import NDArray

import cocoindex as coco
from cocoindex.connectors import postgres
from cocoindex.extras.sentence_transformers import SentenceTransformerEmbedder

# Create a global embedder instance
embedder = SentenceTransformerEmbedder("sentence-transformers/all-MiniLM-L6-v2")

@dataclass
class DocEmbedding:
    filename: str
    text: str
    # Use embedder as a VectorSchemaProvider in type annotations
    embedding: Annotated[NDArray, embedder]

@coco.function
def setup_table(scope: coco.Scope, db: postgres.PgDatabase):
    return db.declare_table_target(
        scope,
        table_name="doc_embeddings",
        table_schema=postgres.TableSchema(
            DocEmbedding,
            primary_key=["filename"],
        ),
        pg_schema_name="public",
    )
```

The connector will automatically:
- Extract the vector dimension from `embedder.__coco_vector_schema__()`
- Create the appropriate `vector(384)` column in Postgres
- Handle type conversions properly

### With LanceDB

```python
from cocoindex.connectors import lancedb

@dataclass
class CodeEmbedding:
    filename: str
    code: str
    embedding: Annotated[NDArray, embedder]

@coco.function
def setup_table(scope: coco.Scope, db: lancedb.LanceDatabase):
    return db.declare_table_target(
        scope,
        table_name="code_embeddings",
        table_schema=lancedb.TableSchema(
            CodeEmbedding,
            primary_key=["filename"],
        ),
    )
```

### With Qdrant

```python
from cocoindex.connectors import qdrant

@dataclass
class DocEmbedding:
    id: str
    text: str
    embedding: Annotated[NDArray, embedder]

@coco.function
def setup_collection(scope: coco.Scope, db: qdrant.QdrantDatabase):
    return db.declare_collection_target(
        scope,
        collection_name="doc_embeddings",
        table_schema=qdrant.TableSchema(
            DocEmbedding,
            primary_key=["id"],
        ),
    )
```

## Example: Text Embedding Pipeline

Here's a complete example of a text embedding pipeline:

```python
import asyncio
import pathlib
from dataclasses import dataclass
from typing import Annotated, AsyncIterator

import asyncpg
from numpy.typing import NDArray

import cocoindex as coco
import cocoindex.asyncio as coco_aio
from cocoindex.connectors import localfs, postgres
from cocoindex.extras.text import RecursiveSplitter
from cocoindex.extras.sentence_transformers import SentenceTransformerEmbedder
from cocoindex.resources.file import FileLike, PatternFilePathMatcher
from cocoindex.resources.chunk import Chunk

# Global state and utilities
embedder = SentenceTransformerEmbedder("sentence-transformers/all-MiniLM-L6-v2")
splitter = RecursiveSplitter()

@dataclass
class DocEmbedding:
    filename: str
    chunk_start: int
    chunk_end: int
    text: str
    embedding: Annotated[NDArray, embedder]

@coco.function
def setup_table(scope: coco.Scope, db: postgres.PgDatabase):
    return db.declare_table_target(
        scope,
        table_name="doc_embeddings",
        table_schema=postgres.TableSchema(
            DocEmbedding,
            primary_key=["filename", "chunk_start"],
        ),
        pg_schema_name="public",
    )

@coco.function(memo=True)
async def process_chunk(
    scope: coco.Scope,
    filename: pathlib.PurePath,
    chunk: Chunk,
    table: postgres.TableTarget[DocEmbedding],
):
    table.declare_row(
        scope,
        row=DocEmbedding(
            filename=str(filename),
            chunk_start=chunk.start.char_offset,
            chunk_end=chunk.end.char_offset,
            text=chunk.text,
            embedding=await embedder.embed_async(chunk.text),
        ),
    )

@coco.function(memo=True)
async def process_file(
    scope: coco.Scope,
    file: FileLike,
    table: postgres.TableTarget[DocEmbedding],
):
    text = file.read_text()
    chunks = splitter.split(text, chunk_size=2000, chunk_overlap=500)

    # Process chunks in parallel
    await asyncio.gather(
        *(process_chunk(scope, file.relative_path, chunk, table) for chunk in chunks)
    )

@coco.function
def app_main(scope: coco.Scope, sourcedir: pathlib.Path, db: postgres.PgDatabase):
    table = coco.mount_run(setup_table, scope / "setup", db).result()

    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["*.md"]),
    )

    for f in files:
        coco.mount(process_file, scope / "file" / str(f.relative_path), f, table)
```

## API Reference

### `SentenceTransformerEmbedder`

::: cocoindex.extras.sentence_transformers.SentenceTransformerEmbedder
    options:
      show_root_heading: true
      show_source: false

## Configuration Options

### Model Selection

You can use any model from the [sentence-transformers library](https://www.sbert.net/docs/sentence_transformer/pretrained_models.html):

```python
# Small, fast model (384 dimensions)
embedder = SentenceTransformerEmbedder("sentence-transformers/all-MiniLM-L6-v2")

# Larger, more accurate model (768 dimensions)
embedder = SentenceTransformerEmbedder("sentence-transformers/all-mpnet-base-v2")

# Multilingual model
embedder = SentenceTransformerEmbedder("sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")

# Local model
embedder = SentenceTransformerEmbedder("/path/to/local/model")
```

### Normalization

By default, embeddings are normalized to unit length (suitable for cosine similarity):

```python
# Default: normalized embeddings
embedder = SentenceTransformerEmbedder(
    "sentence-transformers/all-MiniLM-L6-v2",
    normalize_embeddings=True  # Default
)

# Disable normalization if needed
embedder = SentenceTransformerEmbedder(
    "sentence-transformers/all-MiniLM-L6-v2",
    normalize_embeddings=False
)
```

## Thread Safety

The `SentenceTransformerEmbedder` is thread-safe:

- Model loading is lazy and uses double-checked locking
- GPU access is protected by a lock to prevent concurrent operations
- Safe to use in async contexts with `asyncio.to_thread()` (which `embed_async()` uses internally)

## Performance Considerations

### Model Caching

The model is loaded only once per embedder instance and cached in memory:

```python
# Good: Reuse the same embedder instance
embedder = SentenceTransformerEmbedder("sentence-transformers/all-MiniLM-L6-v2")

for text in texts:
    embedding = embedder.embed(text)  # Model loaded only once

# Avoid: Creating new embedder instances repeatedly
for text in texts:
    embedder = SentenceTransformerEmbedder("sentence-transformers/all-MiniLM-L6-v2")
    embedding = embedder.embed(text)  # Model loaded every time!
```

### Batch Processing

For better performance when embedding many texts, use async processing with `asyncio.gather()`:

```python
# Process many texts in parallel (with thread pool)
async def embed_all(texts: list[str]):
    return await asyncio.gather(
        *(embedder.embed_async(text) for text in texts)
    )

embeddings = asyncio.run(embed_all(texts))
```

### GPU Usage

The embedder automatically uses GPU if available. To specify a device:

```python
# Use specific GPU
embedder = SentenceTransformerEmbedder(
    "sentence-transformers/all-MiniLM-L6-v2",
    device="cuda:0"
)

# Force CPU
embedder = SentenceTransformerEmbedder(
    "sentence-transformers/all-MiniLM-L6-v2",
    device="cpu"
)
```

Note: Device selection is not currently exposed in the public API but can be added if needed.
