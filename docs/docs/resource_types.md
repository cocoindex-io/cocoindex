---
title: Resource Types
description: Common data types for files, vector schemas, and IDs shared across CocoIndex connectors and built-in operations.
---

The `cocoindex.resources` package provides common data models and abstractions shared across connectors and built-in operation modules, ensuring a consistent interface for working with data.

## File

The file module (`cocoindex.resources.file`) defines protocols and utilities for working with file-like objects.

### FileLike / AsyncFileLike

`FileLike` is a protocol for file objects with synchronous read access. `AsyncFileLike` is its async counterpart with the same properties but async read methods.

```python
from cocoindex.resources.file import FileLike

def process_file(file: FileLike) -> str:
    text = file.read_text()
    ...
    return text
```

```python
from cocoindex.resources.file import AsyncFileLike

async def process_file_async(file: AsyncFileLike) -> str:
    text = await file.read_text()
    ...
    return text
```

**Properties:**

- `file_path` — A `FilePath` object representing the file's path. Access the relative path via `file_path.path` (`PurePath`).
- `size` — File size in bytes
- `modified_time` — File modification time (`datetime`)

**Methods:**

- `read(size=-1)` — Read file content as bytes. Pass `size` to limit bytes read.
- `read_text(encoding=None, errors="replace")` — Read as text. Auto-detects encoding via BOM if not specified.

**Memoization:**

`FileLike` objects provide a memoization key based on `file_path` and `modified_time`. When used as arguments to a [memoized function](./programming_guide/function.md#memoization), CocoIndex can detect when a file has changed and skip recomputation for unchanged files.

### FilePath

`FilePath` is a base class that combines a **base directory** (with a stable key) and a **relative path**. This enables stable memoization even when the entire directory tree is moved to a different location.

```python
from cocoindex.resources.file import FilePath
```

Each connector provides its own `FilePath` subclass (e.g., `localfs.FilePath`). The base class defines the common interface.

**Properties:**

- `base_dir` — A `KeyedConnection` object that holds the base directory. The `base_dir.key` is used for stable memoization.
- `path` — The path relative to the base directory (`PurePath`).

**Methods:**

- `resolve()` — Resolve to the full path (type depends on the connector, e.g., `pathlib.Path` for local filesystem).

**Path Operations:**

`FilePath` supports most `pathlib.PurePath` operations:

```python
# Join paths with /
config_path = source_dir / "config" / "settings.json"

# Access path properties
config_path.name      # "settings.json"
config_path.stem      # "settings"
config_path.suffix    # ".json"
config_path.parts     # ("config", "settings.json")
config_path.parent    # FilePath pointing to "config/"

# Modify path components
config_path.with_name("other.json")
config_path.with_suffix(".yaml")
config_path.with_stem("config")

# Pattern matching
config_path.match("*.json")  # True

# Convert to POSIX string
config_path.as_posix()  # "config/settings.json"
```

**Memoization:**

`FilePath` provides a memoization key based on `(base_dir.key, path)`. This means:

- Two `FilePath` objects with the same base directory key and relative path have the same memo key
- Moving the entire project directory doesn't invalidate memoization, as long as you re-register with the same key

For connector-specific usage (e.g., `register_base_dir`), see the individual connector documentation like [Local File System](./connectors/localfs.md).

### FilePathMatcher

`FilePathMatcher` is a protocol for filtering files and directories during traversal.

```python
from cocoindex.resources.file import FilePathMatcher

class MyMatcher(FilePathMatcher):
    def is_dir_included(self, path: PurePath) -> bool:
        """Return True to traverse this directory."""
        return not path.name.startswith(".")

    def is_file_included(self, path: PurePath) -> bool:
        """Return True to include this file."""
        return path.suffix in (".py", ".md")
```

#### PatternFilePathMatcher

A built-in `FilePathMatcher` implementation using glob patterns:

```python
from cocoindex.resources.file import PatternFilePathMatcher

# Include only Python and Markdown files, exclude tests and hidden dirs
matcher = PatternFilePathMatcher(
    included_patterns=["*.py", "*.md"],
    excluded_patterns=["**/test_*", "**/.*"],
)
```

**Parameters:**

- `included_patterns` — Glob patterns for files to include. If `None`, all files are included.
- `excluded_patterns` — Glob patterns for files/directories to exclude. Excluded directories are not traversed.

## Vector Schema

The schema module (`cocoindex.resources.schema`) defines types that describe vector columns. CocoIndex connectors use these to automatically configure the correct column type (e.g., `vector(384)` in Postgres, `fixed_size_list<float32>(384)` in LanceDB).

### VectorSchema

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
target_collection = await coco_aio.mount_run(
    coco.component_subpath("setup", "collection"),
    target_db.declare_collection_target,
    collection_name="image_search",
    schema=await qdrant.CollectionSchema.create(
        vectors=qdrant.QdrantVectorDef(schema=schema, distance="cosine")
    ),
).result()
```

### VectorSchemaProvider

A protocol for objects that can provide vector schema information. The primary use case is as metadata in `Annotated` type annotations — connectors extract vector column configuration from the annotation automatically.

Any object that implements the `__coco_vector_schema__()` method satisfies this protocol. The built-in [`SentenceTransformerEmbedder`](./ops/sentence_transformers.md) implements it.

```python
from typing import Annotated
from numpy.typing import NDArray
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder

embedder = SentenceTransformerEmbedder("sentence-transformers/all-MiniLM-L6-v2")

@dataclass
class DocEmbedding:
    id: int
    text: str
    embedding: Annotated[NDArray, embedder]  # embedder provides vector schema
```

When a connector's `TableSchema.from_class()` encounters an `Annotated[NDArray, provider]` field where `provider` is a `VectorSchemaProvider`, it calls `__coco_vector_schema__()` to determine the column's dimension and dtype.

`VectorSchema` itself also implements `VectorSchemaProvider` (returning itself), so you can use a `VectorSchema` directly as an annotation:

```python
schema = VectorSchema(dtype=np.dtype(np.float32), size=768)

@dataclass
class ImageEmbedding:
    id: int
    embedding: Annotated[NDArray, schema]
```

### MultiVectorSchema / MultiVectorSchemaProvider

Analogous types for multi-vector columns (e.g., ColBERT-style token-level embeddings). `MultiVectorSchema` wraps a `VectorSchema` describing the individual vectors. Used by connectors like [Qdrant](./connectors/qdrant.md) that support multi-vector storage.

```python
from cocoindex.resources.schema import MultiVectorSchema, VectorSchema

multi_schema = MultiVectorSchema(
    vector_schema=VectorSchema(dtype=np.dtype(np.float32), size=128)
)
```

## ID Generation

The ID module (`cocoindex.resources.id`) provides utilities for generating stable unique IDs and UUIDs that persist across incremental updates.

### Choosing the Right API

| API | Same `dep` produces... | Use when... |
|-----|------------------------|-------------|
| `generate_id(dep)` | **Same** ID every time | Each unique input maps to exactly one ID |
| `IdGenerator.next_id(dep)` | **Distinct** ID each call | You need multiple IDs for potentially non-distinct inputs |

The same distinction applies to `generate_uuid` vs `UuidGenerator`.

### generate_id / generate_uuid

Async functions that return the **same** ID/UUID for the **same** `dep` value. These are idempotent: calling multiple times with identical `dep` yields identical results.

```python
from cocoindex.resources.id import generate_id, generate_uuid

async def process_item(item: Item) -> Row:
    # Same item.key always gets the same ID
    item_id = await generate_id(item.key)
    return Row(id=item_id, data=item.data)

async def process_document(doc: Document) -> Row:
    # Same doc.path always gets the same UUID
    doc_uuid = await generate_uuid(doc.path)
    return Row(id=doc_uuid, content=doc.content)
```

**Parameters:**

- `dep` — Dependency value that determines the ID/UUID. The same `dep` always produces the same result within a component. Defaults to `None`.

**Returns:**

- `generate_id` returns an `int` (IDs start from 1; 0 is reserved)
- `generate_uuid` returns a `uuid.UUID`

### IdGenerator / UuidGenerator

Classes that return a **distinct** ID/UUID on each call, even when called with the same `dep` value. The sequence is stable across runs.

Use these when you need multiple IDs for potentially non-distinct inputs, such as splitting text into chunks where chunks may have identical content but still need unique IDs.

```python
from cocoindex.resources.id import IdGenerator, UuidGenerator

async def process_document(doc: Document) -> list[Row]:
    # Use doc.path to distinguish generators within the same processing component
    id_gen = IdGenerator(deps=doc.path)
    rows = []
    for chunk in split_into_chunks(doc.content):
        # Each call returns a distinct ID, even if chunks are identical
        chunk_id = await id_gen.next_id(chunk.content)
        rows.append(Row(id=chunk_id, content=chunk.content))
    return rows

async def process_with_uuids(doc: Document) -> list[Row]:
    # Use doc.path to distinguish generators within the same processing component
    uuid_gen = UuidGenerator(deps=doc.path)
    rows = []
    for chunk in split_into_chunks(doc.content):
        # Each call returns a distinct UUID, even if chunks are identical
        chunk_uuid = await uuid_gen.next_uuid(chunk.content)
        rows.append(Row(id=chunk_uuid, content=chunk.content))
    return rows
```

**Constructor:**

- `IdGenerator(deps=None)` / `UuidGenerator(deps=None)` — Create a generator. The `deps` parameter distinguishes generators within the same processing component. Use distinct `deps` values for different generator instances.

**Methods:**

- `async IdGenerator.next_id(dep=None)` — Generate the next unique integer ID (distinct on each call)
- `async UuidGenerator.next_uuid(dep=None)` — Generate the next unique UUID (distinct on each call)
