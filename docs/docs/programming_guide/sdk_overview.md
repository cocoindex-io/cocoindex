---
title: SDK Overview
description: Overview of the CocoIndex Python SDK package organization, common types like StableKey, sync vs async APIs, and how to mix sync/async across processing components.
---

This document provides an overview of the CocoIndex Python SDK organization and how to choose between the synchronous and asynchronous APIs.

## Package organization

The CocoIndex SDK is organized into several modules:

### Core package

| Package | Description |
|---------|-------------|
| `cocoindex` | All core APIs — async by default, sync variants have a `_blocking` suffix |

### Sub-packages

| Package | Description |
|---------|-------------|
| `cocoindex.connectors` | Connectors for data sources and targets |
| `cocoindex.resources` | Common data models and abstractions shared across connectors and built-in operations |
| `cocoindex.ops` | Built-in operations for common data processing tasks (e.g., text splitting, embedding with SentenceTransformers) |

Import connectors and extras by their specific sub-module:

```python
from cocoindex.connectors import localfs, postgres
from cocoindex.ops.text import RecursiveSplitter
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder
from cocoindex.resources.file import FileLike, PatternFilePathMatcher
from cocoindex.resources.chunk import Chunk
```

## Common types

### StableKey

`StableKey` is a type alias defining what values can be used when creating component paths via `coco.component_subpath()`:

```python
StableKey = None | bool | int | str | bytes | uuid.UUID | Symbol | tuple[StableKey, ...]
```

Common examples include strings (like `"setup"` or `"table"`), integers, and UUIDs. Tuples allow composite keys when needed.
`Symbol` provides predefined names that will never conflict with strings (which typically come from runtime data).

Each processing component must be mounted at a unique path. See [Processing Component](./processing_component.md) for how the component path tree affects target states and ownership.

## Async vs sync APIs

CocoIndex's API is **async-first**: most APIs are `async` and intended to be called with `await`.

For entry points that are typically called outside of async contexts (e.g., scripts or CLI usage), sync variants are provided with a `_blocking` suffix:

| Async | Sync (blocking) |
|-------|-----------------|
| `await app.update(...)` | `app.update_blocking(...)` |
| `await app.drop(...)` | `app.drop_blocking(...)` |
| `await coco.start()` | `coco.start_blocking()` |
| `await coco.stop()` | `coco.stop_blocking()` |
| `async with coco.runtime():` | `with coco.runtime():` |

Mount APIs (`mount`, `use_mount`, `mount_each`, `mount_target`, `map`) are async-only. The `@coco.fn` decorator preserves the sync/async nature of the underlying function.

### Mixing sync and async

You cannot directly call an async function from a sync function, and you should avoid calling a blocking sync function from an async function — just like any Python program. However, when you **mount a processing component**, the processing component's function is scheduled to run on CocoIndex's runtime (Rust core) — it is not a direct function call. This means your pipeline's main function can mount either sync or async processing component functions.

As a result, you need to make sure each processing component uses sync or async consistently internally, but there are no such constraints across processing components. This introduces extra flexibility and composability across your pipeline.

## Example: async vs sync usage

### Asynchronous APIs

```python
import asyncio
import cocoindex as coco

@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder):
    builder.settings.db_path = pathlib.Path("./cocoindex.db")
    # async resource setup can go here
    yield

@coco.fn
async def app_main(sourcedir: pathlib.Path):
    # ... processing logic (can call async functions internally) ...
    pass

app = coco.App(coco.AppConfig(name="MyApp"), app_main, sourcedir=pathlib.Path("./data"))

async def main():
    await app.update(report_to_stdout=True)

if __name__ == "__main__":
    asyncio.run(main())
```

### Synchronous entry point

```python
import cocoindex as coco

@coco.lifespan
def coco_lifespan(builder: coco.EnvironmentBuilder):
    builder.settings.db_path = pathlib.Path("./cocoindex.db")
    yield

@coco.fn
async def app_main(sourcedir: pathlib.Path):
    # ... processing logic ...
    pass

app = coco.App(coco.AppConfig(name="MyApp"), app_main, sourcedir=pathlib.Path("./data"))

def main():
    app.update_blocking(report_to_stdout=True)

if __name__ == "__main__":
    main()
```

### Mixing sync and async

An async main function can mount sync or async processing component functions:

```python
import cocoindex as coco

@coco.fn
async def fetch_and_process(url: str):
    # Async processing component — uses await internally
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            data = await response.text()
    # ... declare target states with data ...

@coco.fn
async def app_main(urls: list[str]):
    # Async function mounting async processing components
    for url in urls:
        await coco.mount(coco.component_subpath(url), fetch_and_process, url)
```

The reverse also works — an async main function can mount sync processing components.

## Common import pattern

A typical CocoIndex application imports from multiple modules:

```python
import cocoindex as coco

from cocoindex.connectors import localfs, postgres
from cocoindex.ops.text import RecursiveSplitter
from cocoindex.resources.file import FileLike
```
