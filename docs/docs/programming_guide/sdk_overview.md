---
title: SDK Overview
---

# SDK Overview

This document provides an overview of the CocoIndex Python SDK organization and how to choose between the synchronous and asynchronous APIs.

## Package Organization

The CocoIndex SDK is organized into several modules:

### Core Packages

| Package | Description |
|---------|-------------|
| `cocoindex` | Main package with synchronous core APIs |
| `cocoindex.asyncio` | Asynchronous core APIs for `async`/`await` workflows |

### Sub-packages

| Package | Description |
|---------|-------------|
| `cocoindex.connectors` | Connectors for data sources and targets (e.g., `localfs`, `postgres`, `lancedb`, `qdrant`, `google_drive`) |
| `cocoindex.extras` | Utility modules for common tasks (e.g., text splitting, embedding with SentenceTransformers) |
| `cocoindex.resources` | Resource types like `FileLike`, `Chunk`, and schema utilities |

Import connectors and extras by their specific sub-module:

```python
from cocoindex.connectors import localfs, postgres
from cocoindex.extras.text import RecursiveSplitter
from cocoindex.extras.sentence_transformers import SentenceTransformerEmbedder
from cocoindex.resources.file import FileLike, PatternFilePathMatcher
from cocoindex.resources.chunk import Chunk
```

## Sync vs Async APIs

CocoIndex provides both synchronous and asynchronous APIs to fit different application patterns.

- **`cocoindex`** — Use for synchronous applications or simpler scripts.
- **`cocoindex.asyncio`** — Use for applications that leverage `async`/`await`, or when integrating with async frameworks and I/O-bound workloads.

The two packages relate as follows:

- **APIs with sync/async variants** — Some core APIs have separate sync and async implementations. For example, the `App` class exists in both packages — `cocoindex.App` provides a blocking `run()` method, while `cocoindex.asyncio.App` provides an async `run()` method you call with `await`.

- **Shared APIs** — Many APIs are non-blocking and work identically in both contexts. For instance, `Scope` and effect declaration APIs are shared between both packages. Decorators like `@function` and `@lifespan` are also shared — they accept both sync and async functions. You can import these from either `cocoindex` or `cocoindex.asyncio`.

### Mixing Sync and Async

You cannot directly call an async function from a sync function, and you should avoid calling a blocking sync function from an async function — just like any Python program. However, when you **mount a component**, the component's function is scheduled to run on CocoIndex's runtime (Rust core) — it is not a direct function call. This means you can mount either sync or async component functions from either a sync or async context.

As a result, you need to make sure each component uses sync or async consistently internally, but there are no such constraints across components. This introduces extra flexibility and composability across your pipeline.

## Example: Sync vs Async Usage

### Synchronous APIs

```python
import cocoindex as coco

@coco.lifespan
def coco_lifespan(builder: coco.EnvironmentBuilder):
    builder.settings.db_path = pathlib.Path("./cocoindex.db")
    yield

@coco.function
def app_main(scope: coco.Scope, sourcedir: pathlib.Path):
    # ... processing logic ...
    pass

app = coco.App(app_main, coco.AppConfig(name="MyApp"), sourcedir=pathlib.Path("./data"))

def main():
    app.run(report_to_stdout=True)

if __name__ == "__main__":
    main()
```

### Asynchronous APIs

```python
import asyncio
import cocoindex.asyncio as coco_aio

@coco_aio.lifespan
async def coco_lifespan(builder: coco_aio.EnvironmentBuilder):
    builder.settings.db_path = pathlib.Path("./cocoindex.db")
    # async resource setup can go here
    yield

@coco_aio.function
def app_main(scope: coco_aio.Scope, sourcedir: pathlib.Path):
    # ... processing logic (can call async functions internally) ...
    pass

app = coco_aio.App(app_main, coco_aio.AppConfig(name="MyApp"), sourcedir=pathlib.Path("./data"))

async def main():
    await app.run(report_to_stdout=True)

if __name__ == "__main__":
    asyncio.run(main())
```

:::tip
Whether `app_main` (the root component's function) is sync or async is orthogonal to whether you use `coco.App` or `coco_aio.App`.
:::

## Common Import Pattern

A typical CocoIndex application imports from multiple modules:

```python
import cocoindex as coco  # or: import cocoindex.asyncio as coco_aio

from cocoindex.connectors import localfs, postgres
from cocoindex.extras.text import RecursiveSplitter
from cocoindex.resources.file import FileLike
```

The aliases `coco` and `coco_aio` are common conventions in examples.
