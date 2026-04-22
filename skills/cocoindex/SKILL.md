---
name: cocoindex
description: This skill should be used when building data processing pipelines with CocoIndex, a Python library for incremental data transformation. Use when the task involves processing files/data into databases, creating vector embeddings, building knowledge graphs, ETL workflows, or any data pipeline requiring automatic change detection and incremental updates. CocoIndex is Python-native (supports any Python types), has no DSL, and is currently under pre-release (version 1.0.0a1 or later).
---

# CocoIndex

CocoIndex is a Python library for building incremental data processing pipelines with declarative target states. Think spreadsheets or React for data pipelines: declare what the output should look like based on current input, and CocoIndex automatically handles incremental updates, change detection, and syncing to external systems.

## Overview

CocoIndex enables building data pipelines that:

- **Automatically handle incremental updates**: Only reprocess changed data
- **Use declarative target states**: Declare what should exist, not how to update
- **Support any Python types**: No custom DSL -- use dataclasses, Pydantic, NamedTuple
- **Provide function memoization**: Skip expensive operations when inputs/code unchanged
- **Sync to multiple targets**: PostgreSQL, SQLite, LanceDB, Qdrant, SurrealDB, Apache Doris, file systems, Kafka

**Key principle**: `TargetState = Transform(SourceState)`

## When to Use This Skill

Use this skill when building pipelines that involve:

- **Document processing**: PDF/Markdown conversion, text extraction, chunking
- **Vector embeddings**: Embedding documents/code for semantic search
- **Database transformations**: ETL from source DB to target DB
- **Knowledge graphs**: Extract entities and relationships from data
- **LLM-based extraction**: Structured data extraction using LLMs
- **File-based pipelines**: Transform files from one format to another
- **Incremental indexing**: Keep search indexes up-to-date with source changes
- **Streaming pipelines**: Kafka-based real-time data processing

## Quick Start: Creating a New Project

### Initialize Project

```bash
cocoindex init my-project
cd my-project
```

This creates: `main.py`, `pyproject.toml`, `.env`, `README.md`.

### Add Dependencies

```toml
# For vector embeddings with PostgreSQL
dependencies = ["cocoindex>=1.0.0a1", "sentence-transformers", "asyncpg"]

# For LLM extraction
dependencies = ["cocoindex>=1.0.0a1", "litellm", "instructor", "pydantic>=2.0"]
```

See [references/setup_project.md](references/setup_project.md) for complete examples.

### Run the Pipeline

```bash
pip install -e .
cocoindex update main.py
```

## Core Concepts

### 1. Apps

An **App** is the top-level executable that binds a main function with parameters:

```python
import cocoindex as coco

@coco.fn
async def app_main(sourcedir: pathlib.Path) -> None:
    ...

app = coco.App(
    coco.AppConfig(name="MyApp"),
    app_main,
    sourcedir=pathlib.Path("./data"),
)
```

### 2. Functions (`@coco.fn`)

The `@coco.fn` decorator marks functions as CocoIndex processing functions. Add `memo=True` to skip re-execution when inputs/code are unchanged:

```python
@coco.fn(memo=True)
async def expensive_operation(data: str) -> Result:
    # LLM call, embedding generation, heavy computation
    return await expensive_transform(data)
```

**Key parameters:**
- `memo=True` -- Enable memoization (skip if inputs/code unchanged)
- `version=1` -- Explicit version bump to force re-execution
- `batching=True` -- Auto-batch concurrent calls (async only)
- `runner=coco.GPU` -- Serialize GPU-bound execution

### 3. Processing Components

A **processing component** groups an item's processing with its target states.

**Mount components** with `mount_each()` (preferred for lists) or `mount()`:

```python
# One component per item (preferred for lists)
await coco.mount_each(process_file, files.items(), target_table)

# Single component (subpath auto-derived from fn.__name__)
await coco.mount(setup_fn, arg1)

# Dependent component (blocks until result returned)
result = await coco.use_mount(init_fn)

# Explicit subpath (when you need a specific path, e.g. in loops)
await coco.mount(coco.component_subpath("item", item_id), process_item, item)
```

**Key points:**
- All mount APIs are `async`
- `mount()`, `use_mount()`, and `mount_each()` auto-derive subpath from `fn.__name__`; optional explicit subpath as first arg
- Use `use_mount()` when you need the return value
- Use stable component paths for proper memoization and cleanup

### 4. Target States

**Declare** what should exist -- CocoIndex handles creation/update/deletion:

```python
# Database row target
table.declare_row(row=MyRecord(id=1, name="example"))

# File target
localfs.declare_file(outdir / "output.txt", content, create_parent_dirs=True)

# Kafka message target
topic_target.declare_target_state(key="msg-1", value=json.dumps(data))
```

### 5. Context for Shared Resources

Use `ContextKey` to share expensive resources (DB connections, models) across components:

```python
EMBEDDER = coco.ContextKey[SentenceTransformerEmbedder]("embedder")

@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    builder.provide(EMBEDDER, SentenceTransformerEmbedder("all-MiniLM-L6-v2"))
    yield

# In processing functions:
embedder = coco.use_context(EMBEDDER)
```

The `@coco.lifespan` decorator registers the function to the default CocoIndex environment, which is shared among all apps by default. `ContextKey` also serves as the stable identity for sources/targets -- the `key` string must remain stable across runs.

### 6. ID Generation

Generate stable, unique identifiers that persist across incremental updates:

```python
from cocoindex.resources.id import generate_id, IdGenerator

# Deterministic: same dep -> same ID
chunk_id = await generate_id(chunk.content)

# Always distinct: each call -> new ID, even with same dep
id_gen = IdGenerator()
for chunk in chunks:
    chunk_id = await id_gen.next_id(chunk.content)
```

### 7. Catch-Up vs Live Mode

By default, `app.update()` runs in **catch-up mode**: it scans all sources, processes what changed since the last run (memoized components are skipped), syncs target states, and returns. Each call still has to scan sources to discover changes.

**Live mode** keeps the app running after catch-up and lets components stream changes continuously from their sources (e.g., file watcher, Kafka consumer), applying them with very low latency.

```python
# Enable live mode
app.update_blocking(live=True)
# Or: cocoindex update main.py -L
```

Two things are needed: (1) enable live mode on the app, and (2) use a source that supports live updates.

- **`LiveMapView`** sources (e.g., `localfs.walk_dir(..., live=True)`) scan current state first, then watch for changes. They also work in catch-up mode -- write the pipeline once, choose mode at run time.
- **`LiveMapFeed`** sources (e.g., `kafka.topic_as_map()`) only stream changes with no initial snapshot. `mount_each()` auto-detects these and creates a live component internally.

```python
# LocalFS with live watching
files = localfs.walk_dir(sourcedir, live=True, ...)
await coco.mount_each(process_file, files.items(), target)

# Kafka -- inherently live
items = kafka.topic_as_map(consumer, ["my-topic"])
await coco.mount_each(process_message, items, target)
```

## Common Pipeline Patterns

### Pattern 1: File Transformation

```python
import pathlib
import cocoindex as coco
from cocoindex.connectors import localfs
from cocoindex.resources.file import FileLike, PatternFilePathMatcher

@coco.fn(memo=True)
async def process_file(file: FileLike, outdir: pathlib.Path) -> None:
    content = await file.read_text()
    transformed = transform_content(content)
    outname = file.file_path.path.stem + ".out"
    localfs.declare_file(outdir / outname, transformed, create_parent_dirs=True)

@coco.fn
async def app_main(sourcedir: pathlib.Path, outdir: pathlib.Path) -> None:
    files = localfs.walk_dir(
        sourcedir,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md"]),
    )
    await coco.mount_each(process_file, files.items(), outdir)

app = coco.App(
    coco.AppConfig(name="Transform"),
    app_main,
    sourcedir=pathlib.Path("./data"),
    outdir=pathlib.Path("./out"),
)
```

### Pattern 2: Vector Embedding Pipeline

```python
import pathlib
from dataclasses import dataclass
from typing import AsyncIterator, Annotated

import asyncpg
from numpy.typing import NDArray

import cocoindex as coco
from cocoindex.connectors import localfs, postgres
from cocoindex.ops.text import RecursiveSplitter
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder
from cocoindex.resources.chunk import Chunk
from cocoindex.resources.file import FileLike, PatternFilePathMatcher
from cocoindex.resources.id import IdGenerator

DATABASE_URL = "postgres://cocoindex:cocoindex@localhost/cocoindex"
PG_DB = coco.ContextKey[asyncpg.Pool]("pg_db")
EMBEDDER = coco.ContextKey[SentenceTransformerEmbedder]("embedder")

_splitter = RecursiveSplitter()

@dataclass
class DocEmbedding:
    id: int
    filename: str
    text: str
    embedding: Annotated[NDArray, EMBEDDER]  # Dimensions inferred from ContextKey
    chunk_start: int
    chunk_end: int

@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    async with await asyncpg.create_pool(DATABASE_URL) as pool:
        builder.provide(PG_DB, pool)
        builder.provide(EMBEDDER, SentenceTransformerEmbedder("all-MiniLM-L6-v2"))
        yield

@coco.fn
async def process_chunk(
    chunk: Chunk, filename: pathlib.PurePath,
    id_gen: IdGenerator, table: postgres.TableTarget[DocEmbedding],
) -> None:
    table.declare_row(row=DocEmbedding(
        id=await id_gen.next_id(chunk.text),
        filename=str(filename),
        text=chunk.text,
        embedding=await coco.use_context(EMBEDDER).embed(chunk.text),
        chunk_start=chunk.start.char_offset,
        chunk_end=chunk.end.char_offset,
    ))

@coco.fn(memo=True)
async def process_file(file: FileLike, table: postgres.TableTarget[DocEmbedding]) -> None:
    text = await file.read_text()
    chunks = _splitter.split(text, chunk_size=2000, chunk_overlap=500)
    id_gen = IdGenerator()
    await coco.map(process_chunk, chunks, file.file_path.path, id_gen, table)

@coco.fn
async def app_main(sourcedir: pathlib.Path) -> None:
    target_table = await postgres.mount_table_target(
        PG_DB,
        table_name="embeddings",
        table_schema=await postgres.TableSchema.from_class(DocEmbedding, primary_key=["id"]),
    )
    target_table.declare_vector_index(column="embedding")

    files = localfs.walk_dir(sourcedir, recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md"]))
    await coco.mount_each(process_file, files.items(), target_table)

app = coco.App(coco.AppConfig(name="Embedding"), app_main, sourcedir=pathlib.Path("./data"))
```

### Pattern 3: LLM-Based Extraction

```python
import instructor
from pydantic import BaseModel
from litellm import acompletion

_instructor_client = instructor.from_litellm(acompletion, mode=instructor.Mode.JSON)

class ExtractionResult(BaseModel):
    title: str
    topics: list[str]

@coco.fn(memo=True)  # Memo avoids re-calling LLM
async def extract_and_store(content: str, message_id: int, table) -> None:
    result = await _instructor_client.chat.completions.create(
        model="gpt-4",
        response_model=ExtractionResult,
        messages=[{"role": "user", "content": f"Extract topics: {content}"}],
    )
    table.declare_row(row=Message(id=message_id, title=result.title, content=content))
```

## Connectors and Operations

CocoIndex provides connectors for reading from and writing to external systems:

| Connector | Source | Target | Vectors | Use Case |
|-----------|--------|--------|---------|----------|
| **PostgreSQL** | Y | Y | pgvector | Production SQL + vectors |
| **SQLite** | - | Y | sqlite-vec | Local SQL + vectors |
| **LanceDB** | - | Y | Y | Cloud-native vector DB |
| **Qdrant** | - | Y | Y | Specialized vector DB |
| **SurrealDB** | - | Y | Y | Graph + document DB |
| **Apache Doris** | - | Y | Y | Analytical DB + vectors |
| **LocalFS** | Y | Y | N/A | File-based pipelines |
| **Amazon S3** | Y | - | N/A | Cloud object storage |
| **Kafka** | Y | Y | N/A | Streaming pipelines |
| **Google Drive** | Y | - | N/A | Cloud file source |

For detailed connector documentation, see [references/connectors.md](references/connectors.md).

## Text and Embedding Operations

### Text Splitting

```python
from cocoindex.ops.text import RecursiveSplitter, detect_code_language

splitter = RecursiveSplitter()
language = detect_code_language(filename="example.py")
chunks = splitter.split(text, chunk_size=1000, chunk_overlap=200, language=language)
```

### Embeddings

```python
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder

embedder = SentenceTransformerEmbedder("sentence-transformers/all-MiniLM-L6-v2")
embedding = await embedder.embed(text)  # Returns NDArray
```

## CLI Commands

```bash
cocoindex init my-project              # Create new project
cocoindex update main.py               # Run app
cocoindex update main.py:my_app        # Run specific app
cocoindex update main.py -L            # Run in live mode (continuous)
cocoindex update main.py --full-reprocess  # Reprocess everything
cocoindex drop main.py [-f]            # Drop and reset all state
cocoindex ls [main.py]                 # List apps
cocoindex show main.py [--tree]        # Show component paths
```

## Best Practices

### 1. Use `@coco.fn` on All Processing Functions

Every function that participates in the pipeline (declares target states, calls mount APIs, etc.) must be decorated with `@coco.fn`.

### 2. Add Memoization for Expensive Operations

```python
@coco.fn(memo=True)  # Skip re-execution when inputs/code unchanged
async def process_chunk(chunk, table):
    embedding = await embedder.embed(chunk.text)  # Expensive!
    table.declare_row(...)
```

### 3. Use Stable Component Paths

```python
# Good: Stable identifiers
coco.component_subpath("file", str(file.file_path.path))
coco.component_subpath("record", record.id)

# Bad: Unstable identifiers
coco.component_subpath("file", file)      # Object reference
coco.component_subpath("idx", idx)        # Index changes
```

### 4. Use Context for Shared Resources

```python
@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    async with await asyncpg.create_pool(DATABASE_URL) as pool:
        builder.provide(PG_DB, pool)
        yield
```

### 5. Use `Annotated[NDArray, CONTEXT_KEY]` for Vectors

```python
EMBEDDER = coco.ContextKey[SentenceTransformerEmbedder]("embedder")

@dataclass
class Record:
    vector: Annotated[NDArray, EMBEDDER]  # Auto-infer dimensions from ContextKey
```

### 6. Use Convenience APIs for Targets

```python
# Mount table target -- subpath is automatic
table = await postgres.mount_table_target(
    PG_DB,
    table_name="my_table",
    table_schema=await postgres.TableSchema.from_class(MyRecord, primary_key=["id"]),
)
```

## Troubleshooting

### "Module not found" Error

Ensure pyproject.toml has pre-release config:

```toml
[tool.uv]
prerelease = "explicit"
```

### Everything Reprocessing

Add `memo=True` to expensive functions:

```python
@coco.fn(memo=True)  # Add this
async def process_item(item):
    ...
```

### Memoization Not Working

Check component paths are stable. Use stable IDs, not object references.

## Resources

### references/

- **[api_reference.md](references/api_reference.md)**: Quick API reference
- **[connectors.md](references/connectors.md)**: Complete connector reference
- **[patterns.md](references/patterns.md)**: Detailed pipeline patterns
- **[setup_project.md](references/setup_project.md)**: Project setup guide
- **[setup_database.md](references/setup_database.md)**: Database setup guide

### External

- [CocoIndex Documentation](https://docs.cocoindex.dev/docs/)
- [GitHub Examples](https://github.com/cocoindex-io/cocoindex/tree/v1/examples)

## Version Note

This skill is for CocoIndex `>=1.0.0a1` (v1). It uses a completely different API from v0.
