---
title: SDK Overview
description: Overview of the CocoIndex Python SDK package organization, common types like StableKey, sync vs async APIs, and how to mix sync/async across processing components.
---

This document provides an overview of the CocoIndex Python SDK organization and how to choose between the synchronous and asynchronous APIs.

## Package organization

The CocoIndex SDK is organized into several modules:

### Core packages

| Package | Description |
|---------|-------------|
| `cocoindex.asyncio` | Asynchronous core APIs for `async`/`await` workflows |
| `cocoindex` | Synchronous core APIs |

### Sub-packages

| Package | Description |
|---------|-------------|
| `cocoindex.connectors` | Connectors for data sources and targets |
| `cocoindex.resources` | Provide common data models and abstractions shared across connectors and extra utilities |
| `cocoindex.ops` | Extra utilities for performing common data processing tasks (e.g., text splitting, embedding with SentenceTransformers) |

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
StableKey = None | bool | int | str | bytes | uuid.UUID | tuple[StableKey, ...]
```

Common examples include strings (like `"setup"` or `"table"`), integers, and UUIDs. Tuples allow composite keys when needed.

Each processing component must be mounted at a unique path. See [Processing Component](./processing_component.md) for how the component path tree affects target states and ownership.

## Async vs sync APIs

CocoIndex provides both asynchronous and synchronous APIs to fit different application patterns.

- **`cocoindex.asyncio`** — Use for applications that leverage `async`/`await`, or when integrating with async frameworks and I/O-bound workloads.
- **`cocoindex`** — Use for synchronous applications or simpler scripts.

The two packages relate as follows:

- **APIs with async/sync variants** — Some core APIs have separate async and sync implementations. For example, the `App` class exists in both packages — `cocoindex.asyncio.App` provides an async `run()` method you call with `await`, while `cocoindex.App` provides a blocking `run()` method.

- **Shared APIs** — Many APIs are non-blocking and work identically in both contexts. For instance, `component_subpath()` and target state declaration APIs are shared between both packages. Decorators like `@function` and `@lifespan` are also shared — they accept both sync and async functions. You can import these from either `cocoindex.asyncio` or `cocoindex`.

### Mixing sync and async

You cannot directly call an async function from a sync function, and you should avoid calling a blocking sync function from an async function — just like any Python program. However, when you **mount a processing component**, the processing component's function is scheduled to run on CocoIndex's runtime (Rust core) — it is not a direct function call. This means you can mount either sync or async processing component functions from either a sync or async context.

As a result, you need to make sure each processing component uses sync or async consistently internally, but there are no such constraints across processing components. This introduces extra flexibility and composability across your pipeline.

## Example: async vs sync usage

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
def app_main(sourcedir: pathlib.Path):
    # ... processing logic (can call async functions internally) ...
    pass

app = coco_aio.App(coco_aio.AppConfig(name="MyApp"), app_main, sourcedir=pathlib.Path("./data"))

async def main():
    await app.update(report_to_stdout=True)

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
def app_main(sourcedir: pathlib.Path):
    # ... processing logic ...
    pass

app = coco.App(coco.AppConfig(name="MyApp"), app_main, sourcedir=pathlib.Path("./data"))

def main():
    app.update(report_to_stdout=True)

if __name__ == "__main__":
    main()
```

### Mixing sync and async

A sync function can mount an async processing component:

```python
import cocoindex as coco

@coco.function
async def fetch_and_process(url: str):
    # Async processing component — uses await internally
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            data = await response.text()
    # ... declare target states with data ...

@coco.function
def app_main(urls: list[str]):
    # Sync function mounting async processing components
    for url in urls:
        coco.mount(coco.component_subpath(url), fetch_and_process, url)
```

The reverse also works — an async function can mount sync processing components.

:::tip
Whether `app_main` (the root processing component's function) is sync or async is orthogonal to whether you use `coco_aio.App` or `coco.App`.
:::

## Common import pattern

A typical CocoIndex application imports from multiple modules:

```python
import cocoindex.asyncio as coco_aio  # or: import cocoindex as coco

from cocoindex.connectors import localfs, postgres
from cocoindex.ops.text import RecursiveSplitter
from cocoindex.resources.file import FileLike
```

The aliases `coco_aio` and `coco` are common conventions in examples.
