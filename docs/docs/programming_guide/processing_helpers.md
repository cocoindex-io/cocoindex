---
title: Processing Helpers
description: Utility APIs for common patterns within processing components, such as applying a function across a collection of items.
---

CocoIndex provides helper APIs for common patterns you use inside a [processing component](./processing_component.md). These are async-only.

## `map()` {#map}

`map()` applies an async function to each item in a collection, running all calls concurrently within the current processing component. Unlike [`mount()`](./processing_component.md#mount) and [`mount_each()`](./processing_component.md#mount-each), it does **not** create child processing components — it's purely concurrent execution (similar to `asyncio.gather()`).

```python
@coco.fn(memo=True)
async def process_file(file: FileLike, table: postgres.TableTarget[DocEmbedding]) -> None:
    chunks = splitter.split(file.read_text())
    id_gen = IdGenerator()
    await coco.map(process_chunk, chunks, file.file_path.path, id_gen, table)
```

The first argument to the function receives each item; additional arguments are passed through to every call. `map()` returns a `list` of the results, in the same order as the input items.

### When to use `map()` vs `mount_each()`

- Use **`mount_each()`** when each item should be its own processing component — with its own component path, target state ownership, and memoization boundary.
- Use **`map()`** when you want to process items concurrently *within* the current component, without creating new component boundaries. This is common for sub-item work like processing chunks within a file.
