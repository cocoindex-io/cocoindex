---
title: Function
description: Understanding the @coco.fn decorator, its capabilities like memoization and change tracking.
---

It's common to factor work into helper functions (for parsing, chunking, embedding, formatting, etc.). In CocoIndex, you can decorate any Python function with `@coco.fn` when you want to add incremental capabilities to it. The decorated function is still a normal Python function: its signature stays the same, and you can call it normally.

```python
@coco.fn
def process_file(file: FileLike) -> str:
    return file.read_text()

# Can be called like any normal function
result = process_file(file)
```

The function can be sync or async:

```python
@coco.fn
async def process_file_async(file: FileLike) -> str:
    return await file.read_text_async()
```

`@coco.fn` preserves the sync/async nature of the underlying function. Decorating a sync function yields a sync function; decorating an async function yields an async function.

## `@coco.fn.as_async`

Use `@coco.fn.as_async` when you need an **async** interface for a function that has a sync underlying implementation. This is required for features like [batching](#batching) and [runner](#runner), which need an async entry point.

```python
@coco.fn.as_async
def embed(text: str) -> list[float]:
    return model.encode([text])[0]

# External usage: always async, even though the function body is sync
embedding = await embed("hello world")
```

`@coco.fn.as_async` is equivalent to wrapping the function in `asyncio.to_thread()` — the sync function runs on a thread pool and doesn't block the event loop.

You can also call any `@coco.fn`-decorated function asynchronously via the `.as_async()` method, without changing its primary signature:

```python
@coco.fn
def expensive_fn(data: bytes) -> bytes:
    return process(data)

# Primary call is sync:
result = expensive_fn(data)

# Async call via .as_async():
result = await expensive_fn.as_async(data)
```

## How to think about `@coco.fn`

Decorating a function tells CocoIndex that calls to it are part of the incremental update engine. You still write normal Python, but CocoIndex can now:

- Skip work when it can safely reuse a previous result (memoization)
- Re-run work when the implementation changes (change tracking)

This is what lets CocoIndex avoid rerunning expensive steps on every `app.update()`. See [Processing Component](./processing_component.md) for how decorated functions are mounted at component paths.

If you don't need any of the above for a helper, keep it as a plain Python function.

## Capabilities

The `@coco.fn` decorator provides the following additional capabilities.

### Memoization

With `memo=True`, the function is memoized. When input data and code haven't changed, CocoIndex skips recomputation of that function body entirely — it carries over target states declared during the function's previous invocation, and returns its previous return value.

```python
@coco.fn(memo=True)
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

The logic of a function decorated with `@coco.fn` is tracked based on the content of the function. When a function's implementation changes, CocoIndex detects this and re-executes the places where it's called.

You can also explicitly control the behavior version with a `version` option:

```python
@coco.fn(memo=True, version=2)
def process_chunk(chunk: Chunk) -> Embedding:
    # Bumping version forces re-execution even if code looks the same
    return embed(chunk.text)
```

### Batching

With `batching=True`, multiple concurrent calls to the function are automatically batched together. This is useful for operations that are more efficient when processing multiple inputs at once, such as embedding models.

Batching requires an async interface. If the underlying function is sync, use `@coco.fn.as_async(batching=True)` to make it async. If the underlying function is already `async def`, `@coco.fn(batching=True)` works directly.

When batching is enabled:

- The function implementation receives a `list[T]` and returns a `list[R]`
- The external signature becomes `async T -> R` (single input, single output)
- Concurrent calls are collected and processed together

```python
@coco.fn.as_async(batching=True, max_batch_size=32)
def embed(texts: list[str]) -> list[list[float]]:
    # Called with a batch of texts, returns a batch of embeddings
    return model.encode(texts)

# External usage: async, single input, single output
embedding = await embed("hello world")  # Returns list[float]

# Concurrent calls are automatically batched using asyncio.gather
embeddings = await asyncio.gather(
    embed("text1"),
    embed("text2"),
    embed("text3"),
)
```

The `max_batch_size` parameter limits how many inputs can be processed in a single batch.

:::tip When to use batching

Batching is beneficial when:

- The underlying operation has significant per-call overhead (e.g., GPU kernel launch)
- The operation can process multiple inputs more efficiently than one at a time
- You have concurrent calls from multiple coroutines

Common use cases:

- **Embedding models** — most embedding APIs and models are optimized for batch processing
- **LLM inference** — batch multiple prompts together for better GPU utilization
- **Database operations** — batch inserts or lookups

:::

### Runner

The `runner` parameter allows functions to execute in a specific context, such as a dedicated GPU runner that serializes GPU workloads.

Like batching, a runner requires an async interface. If the underlying function is sync, use `@coco.fn.as_async(runner=...)` to make it async. If the underlying function is already `async def`, `@coco.fn(runner=...)` works directly.

```python
@coco.fn.as_async(runner=coco.GPU)
def gpu_inference(data: bytes) -> bytes:
    # Runs with GPU serialization
    return model.predict(data)

# External usage: async
result = await gpu_inference(data)
```

The `coco.GPU` runner:

- By default, runs in-process with all functions sharing a queue for serial execution
- Sync functions run on a dedicated GPU thread to avoid blocking the event loop
- Set the environment variable `COCOINDEX_RUN_GPU_IN_SUBPROCESS=1` to run in a subprocess for GPU memory isolation

You can combine batching with a runner:

```python
@coco.fn.as_async(batching=True, max_batch_size=16, runner=coco.GPU)
def batch_gpu_embed(texts: list[str]) -> list[list[float]]:
    # Batched execution with GPU serialization
    return gpu_model.encode(texts)

# External usage: async
embedding = await batch_gpu_embed("hello world")

# Concurrent calls
embeddings = await asyncio.gather(
    batch_gpu_embed("text1"),
    batch_gpu_embed("text2"),
    batch_gpu_embed("text3"),
)
```

:::note
By default, `coco.GPU` runs functions in-process, so no pickling is required. When using subprocess mode (`COCOINDEX_RUN_GPU_IN_SUBPROCESS=1`), the function and all its arguments must be picklable since they are serialized for subprocess execution.
:::
