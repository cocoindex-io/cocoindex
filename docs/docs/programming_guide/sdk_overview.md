---
title: SDK Overview
description: Overview of the CocoIndex Python SDK package organization, common types like Scope and StableKey, sync vs async APIs, and how to mix sync/async across components.
---

# SDK Overview

This document provides an overview of the CocoIndex Python SDK organization and how to choose between the synchronous and asynchronous APIs.

## Package Organization

The CocoIndex SDK is organized into several modules:

### Core Packages

| Package | Description |
|---------|-------------|
| `cocoindex.asyncio` | Asynchronous core APIs for `async`/`await` workflows |
| `cocoindex` | Synchronous core APIs |

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

## Common Types

### Scope

`Scope` is a handle that many CocoIndex APIs require. It carries:

- A **stable path** that uniquely identifies the current position in the processing tree
- Context for accessing provided resources and declaring effects

You'll use `Scope` as the first argument for declaring effects, mounting components, accessing context values, etc. When your function requires `Scope`, pass it explicitly as the first argument.

You create child scopes using the `/` operator:

```python
scope / "setup" / "table"    # Creates path like /setup/table
scope / "file" / filename    # Creates path like /file/readme.md
```

The stable path should be consistent across runs — CocoIndex uses it to match effects from previous runs and determine what changed.

### StableKey

`StableKey` is a type alias defining what values can be used as path parts in stable paths:

```python
StableKey = None | bool | int | str | bytes | uuid.UUID | tuple[StableKey, ...]
```

Common examples include strings (like `"setup"` or `"table"`), integers, and UUIDs. Tuples allow composite keys when needed.

## Async vs Sync APIs

CocoIndex provides both asynchronous and synchronous APIs to fit different application patterns.

- **`cocoindex.asyncio`** — Use for applications that leverage `async`/`await`, or when integrating with async frameworks and I/O-bound workloads.
- **`cocoindex`** — Use for synchronous applications or simpler scripts.

The two packages relate as follows:

- **APIs with async/sync variants** — Some core APIs have separate async and sync implementations. For example, the `App` class exists in both packages — `cocoindex.asyncio.App` provides an async `run()` method you call with `await`, while `cocoindex.App` provides a blocking `run()` method.

- **Shared APIs** — Many APIs are non-blocking and work identically in both contexts. For instance, `Scope` and effect declaration APIs are shared between both packages. Decorators like `@function` and `@lifespan` are also shared — they accept both sync and async functions. You can import these from either `cocoindex.asyncio` or `cocoindex`.

### Mixing Sync and Async

You cannot directly call an async function from a sync function, and you should avoid calling a blocking sync function from an async function — just like any Python program. However, when you **mount a component**, the component's function is scheduled to run on CocoIndex's runtime (Rust core) — it is not a direct function call. This means you can mount either sync or async component functions from either a sync or async context.

As a result, you need to make sure each component uses sync or async consistently internally, but there are no such constraints across components. This introduces extra flexibility and composability across your pipeline.

## Example: Async vs Sync Usage

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

:::tip
Whether `app_main` (the root component's function) is sync or async is orthogonal to whether you use `coco_aio.App` or `coco.App`.
:::

## Common Import Pattern

A typical CocoIndex application imports from multiple modules:

```python
import cocoindex.asyncio as coco_aio  # or: import cocoindex as coco

from cocoindex.connectors import localfs, postgres
from cocoindex.extras.text import RecursiveSplitter
from cocoindex.resources.file import FileLike
```

The aliases `coco_aio` and `coco` are common conventions in examples.
