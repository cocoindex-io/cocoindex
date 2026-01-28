---
title: Function
description: Understanding the @coco.function decorator, its capabilities like memoization and change tracking.
---

It's common to factor work into helper functions (for parsing, chunking, embedding, formatting, etc.). In CocoIndex, you can decorate any Python function with `@coco.function` when you want to add incremental capabilities to it. The decorated function is still a normal Python function: its signature stays the same, and you can call it normally.

```python
@coco.function
def process_file(file: FileLike) -> str:
    return file.read_text()

# Can be called like any normal function
result = process_file(file)
```

The function can be sync or async:

```python
@coco.function
async def process_file_async(file: FileLike) -> str:
    return await file.read_text_async()
```

## How to think about `@coco.function`

Decorating a function tells CocoIndex that calls to it are part of the incremental update engine. You still write normal Python, but CocoIndex can now:

- Skip work when it can safely reuse a previous result (memoization)
- Re-run work when the implementation changes (change tracking)

This is what lets CocoIndex avoid rerunning expensive steps on every `app.update()`. See [Processing Component](./processing_component.md) for how decorated functions are mounted at component paths.

If you don't need any of the above for a helper, keep it as a plain Python function.

## Capabilities

The `@coco.function` decorator provides the following additional capabilities.

### Memoization

With `memo=True`, the function is memoized. When input data and code haven't changed, CocoIndex skips recomputation of that function body entirely — it carries over target states declared during the function's previous invocation, and returns its previous return value.

```python
@coco.function(memo=True)
def process_chunk(chunk: Chunk) -> Embedding:
    # This computation is skipped if chunk and code are unchanged
    return embed(chunk.text)
```

See [Memoization Keys](../advanced_topics/memoization_keys.md) for details on how CocoIndex constructs keys for memoization.

:::tip When to memoize

**Cost:** Function return values must be stored for memoization. Larger return values mean higher storage costs.

**Benefit:** Memoization saves more when:

- The computation is expensive
- The function's caller is reprocessed frequently (due to data or code changes)

**Examples:**

- ✅ **Embedding functions** — good to memoize. Computation is heavy; return value is fixed-size and not too large.
- ❌ **Splitting text into fixed-size chunks** — usually not worth memoizing. Computation is light; return value can be large.
- ✅ **Processing component for files that mostly stable between runs** — very beneficial to memoize, since unchanged files are skipped entirely. We can save the cost of reading file content and processing them when they haven't changed.
- 🤔 **Chunk embedding when file-level memoization is already enabled** — still beneficial, but less so for stable files. The benefit increases for files that change frequently, or when your code evolves (e.g., adding more features per file triggers file-level reprocessing, but unchanged chunks can still skip embedding).

:::

### Change tracking

The logic of a function decorated with `@coco.function` is tracked based on the content of the function. When a function's implementation changes, CocoIndex detects this and re-executes the places where it's called.

You can also explicitly control the behavior version with a `version` option:

```python
@coco.function(memo=True, version=2)
def process_chunk(chunk: Chunk) -> Embedding:
    # Bumping version forces re-execution even if code looks the same
    return embed(chunk.text)
```

:::note
The change tracking capability is still under construction.
:::
