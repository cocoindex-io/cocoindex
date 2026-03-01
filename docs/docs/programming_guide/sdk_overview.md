---
title: SDK Overview
description: Overview of the CocoIndex Python SDK package organization, common types like StableKey, and how async and sync APIs work together.
---

This document provides an overview of the CocoIndex Python SDK organization and how async and sync APIs work together.

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

CocoIndex's API is **async-first**. The APIs fall into three categories:

### Orchestration APIs (async only)

The APIs that shape your pipeline are async:

`mount()`, `use_mount()`, `mount_each()`, `mount_target()`, `map()`

### Entry-point APIs (async + sync)

APIs for starting and running your pipeline have both async and sync variants. Sync variants use a `_blocking` suffix:

| Async | Sync (blocking) |
|-------|-----------------|
| `await app.update(...)` | `app.update_blocking(...)` |
| `await app.drop(...)` | `app.drop_blocking(...)` |
| `await coco.start()` | `coco.start_blocking()` |
| `await coco.stop()` | `coco.stop_blocking()` |
| `async with coco.runtime():` | `with coco.runtime():` |

Use the async variants when you're already in an async context. Use the `_blocking` variants for scripts and CLI usage. See [App](./app.md) for details.

### Processing functions (your choice)

The `@coco.fn` decorator preserves the sync/async nature of your function — your processing functions can be sync or async. See [Function](./function.md) for details.

## How sync and async work together

Like any async Python program, **async functions can call into sync code, but not the other way around**. In practice, this means higher-level functions (orchestration) tend to be async, while leaf functions (the actual computation) can be sync.

CocoIndex provides two ways for async code to call into sync functions:

- **Mounting** — When you mount a processing component, the function is scheduled on CocoIndex's runtime, not called directly. So an async function can mount a sync processing function.
- **`@coco.fn.as_async`** — Wraps a sync function with an async interface (runs on a thread pool). Useful for compute-intensive leaf functions. See [Function](./function.md) for details.

### Example: multi-level pipeline

A typical pipeline has an async main function orchestrating sync leaf functions:

```python
import cocoindex as coco
from cocoindex.connectors import localfs
from cocoindex.resources.file import FileLike

@coco.fn(memo=True)
async def process_file(file: FileLike, target: localfs.DirTarget) -> None:
    html = render(await file.read_text())
    target.declare_file(filename="out.html", content=html)

@coco.fn
async def app_main(sourcedir: pathlib.Path, outdir: pathlib.Path) -> None:
    # Async — orchestrates the pipeline
    target = await coco.use_mount(
        coco.component_subpath("setup"), localfs.declare_dir_target, outdir
    )
    files = localfs.walk_dir(sourcedir)
    await coco.mount_each(process_file, files.items(), target)

app = coco.App(coco.AppConfig(name="MyApp"), app_main,
               sourcedir=pathlib.Path("./data"), outdir=pathlib.Path("./out"))
```

### Example: simple leaf pipeline

When the main function is itself a leaf — no child components, no async calls — it can be sync:

```python
@coco.fn
async def app_main(sourcedir: pathlib.Path, outdir: pathlib.Path) -> None:
    async for f in localfs.walk_dir(sourcedir):
        html = render(await f.read_text())
        localfs.declare_file(outdir / f"{f.stem}.html", html)

app = coco.App("SimpleApp", app_main,
               sourcedir=pathlib.Path("./data"), outdir=pathlib.Path("./out"))
```

## Running an app

Run the app with either an async or sync entry point:

```python
# Async entry point
async def main():
    await app.update(report_to_stdout=True)

asyncio.run(main())
```

```python
# Sync entry point (scripts, CLI)
app.update_blocking(report_to_stdout=True)
```
