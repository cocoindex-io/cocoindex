---
title: Sentence Transformers Embedding
description: CocoIndex integration with sentence-transformers for local text embeddings with automatic model caching and GPU access.
---

# Sentence Transformers Embedding

The `cocoindex.ops.sentence_transformers` module provides integration with the [sentence-transformers](https://www.sbert.net/) library for text embeddings.

## Overview

The `SentenceTransformerEmbedder` class is a wrapper around SentenceTransformer models that:

- Implements `VectorSchemaProvider` for seamless integration with CocoIndex connectors
- Handles model caching and thread-safe GPU access automatically
- Provides a simple `embed()` method
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

## Basic usage

### Creating an embedder

```python
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder

# Initialize embedder with a pre-trained model
embedder = SentenceTransformerEmbedder("sentence-transformers/all-MiniLM-L6-v2")
```

### Embedding text

The `embed()` method converts text into a `numpy.ndarray` of `float32`. It supports both sync and async usage:

```python
# In a CocoIndex function
embedding = await embedder.embed("Hello, world!")

# Use the embedding in a dataclass row, store in a vector database, etc.
table.declare_row(row=CodeEmbedding(code="Hello, world!", embedding=embedding))
```

### Using as a type annotation

The `SentenceTransformerEmbedder` implements [`VectorSchemaProvider`](../resource_types.md#vectorschemaprovider), which means it can be used directly as metadata in `Annotated` type annotations. This is the recommended way to declare vector columns — CocoIndex connectors automatically extract the vector dimension and dtype from the annotation when creating tables.

```python
from dataclasses import dataclass
from typing import Annotated
from numpy.typing import NDArray

embedder = SentenceTransformerEmbedder("sentence-transformers/all-MiniLM-L6-v2")

@dataclass
class CodeEmbedding:
    id: int
    filename: str
    code: str
    embedding: Annotated[NDArray, embedder]  # vector(384) with float32
    start_line: int
    end_line: int
```

When you pass this dataclass to a connector's `TableSchema.from_class()`, the connector automatically reads the embedder annotation to determine the vector column's dimension and dtype. For example, with Postgres:

```python
from cocoindex.connectors import postgres

table_schema = await postgres.TableSchema.from_class(
    CodeEmbedding,
    primary_key=["id"],
)
target_table = await coco_aio.mount_run(
    coco.component_subpath("setup", "table"),
    target_db.declare_table_target,
    table_name="code_embeddings",
    table_schema=table_schema,
    pg_schema_name="my_schema",
).result()
```

The connector automatically creates the appropriate `vector(384)` column. See the [Connectors](../connectors/postgres.md) docs for other supported backends (LanceDB, Qdrant, SQLite).

## Example: text embedding pipeline

Here's a complete example of a text embedding pipeline (based on the [text_embedding example](https://github.com/cocoindex-io/cocoindex/tree/main/examples/text_embedding)):

```python
import asyncio
import pathlib
from dataclasses import dataclass
from typing import Annotated, AsyncIterator

from numpy.typing import NDArray

import cocoindex as coco
import cocoindex.asyncio as coco_aio
from cocoindex.connectors import localfs, postgres
from cocoindex.ops.text import RecursiveSplitter
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder
from cocoindex.resources.chunk import Chunk
from cocoindex.resources.file import FileLike, PatternFilePathMatcher
from cocoindex.resources.id import IdGenerator

PG_DB = coco.ContextKey[postgres.PgDatabase]("pg_db")

_embedder = SentenceTransformerEmbedder("sentence-transformers/all-MiniLM-L6-v2")
_splitter = RecursiveSplitter()

@dataclass
class DocEmbedding:
    id: int
    filename: str
    chunk_start: int
    chunk_end: int
    text: str
    embedding: Annotated[NDArray, _embedder]

@coco.function
async def process_chunk(
    filename: pathlib.PurePath,
    chunk: Chunk,
    id_gen: IdGenerator,
    table: postgres.TableTarget[DocEmbedding],
) -> None:
    table.declare_row(
        row=DocEmbedding(
            id=await id_gen.next_id(chunk.text),
            filename=str(filename),
            chunk_start=chunk.start.char_offset,
            chunk_end=chunk.end.char_offset,
            text=chunk.text,
            embedding=await _embedder.embed(chunk.text),
        ),
    )

@coco.function(memo=True)
async def process_file(
    file: FileLike,
    table: postgres.TableTarget[DocEmbedding],
) -> None:
    text = file.read_text()
    chunks = _splitter.split(
        text, chunk_size=2000, chunk_overlap=500, language="markdown"
    )
    id_gen = IdGenerator()
    await asyncio.gather(
        *(process_chunk(file.file_path.path, chunk, id_gen, table) for chunk in chunks)
    )

@coco.function
async def app_main(sourcedir: pathlib.Path) -> None:
    target_db = coco.use_context(PG_DB)
    target_table = await coco_aio.mount_run(
        coco.component_subpath("setup", "table"),
        target_db.declare_table_target,
        table_name="doc_embeddings",
        table_schema=await postgres.TableSchema.from_class(
            DocEmbedding,
            primary_key=["id"],
        ),
        pg_schema_name="public",
    ).result()

    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["*.md"]),
    )
    with coco.component_subpath("file"):
        for f in files:
            coco.mount(
                coco.component_subpath(str(f.file_path.path)),
                process_file,
                f,
                target_table,
            )
```

## Configuration options

### Model selection

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
