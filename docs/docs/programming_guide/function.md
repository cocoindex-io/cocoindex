---
title: Function
description: Understanding the @coco.function decorator, its capabilities like memoization and change tracking, and how functions access Scope.
---

It's common to factor work into helper functions (for parsing, chunking, embedding, formatting, etc.). In CocoIndex, you can decorate any Python function with `@coco.function` when you want to add incremental capabilities to it. The decorated function is still a normal Python function: its signature stays the same, and you can call it normally.

```python
@coco.function
def process_file(scope: coco.Scope, file: FileLike) -> str:
    return file.read_text()

# Can be called like any normal function
result = process_file(scope, file)
```

The function can be sync or async:

```python
@coco.function
async def process_file_async(scope: coco.Scope, file: FileLike) -> str:
    return await file.read_text_async()
```

## How to think about `@coco.function`

Decorating a function tells CocoIndex that calls to it are part of the incremental update engine. You still write normal Python, but CocoIndex can now:

- Skip work when it can safely reuse a previous result (memoization)
- Re-run work when the implementation changes (change tracking)

This is what lets CocoIndex avoid rerunning expensive steps on every `app.update()`. See [Processing Component](./processing_component.md) for how decorated functions are mounted at scopes.

If you don't need any of the above for a helper, keep it as a plain Python function.

## Capabilities

The `@coco.function` decorator provides the following additional capabilities.

### Memoization

With `memo=True`, the function is memoized. When input data and code haven't changed, CocoIndex skips recomputation of that function body entirely — it carries over target states declared during the function's previous invocation, and returns its previous return value.

```python
@coco.function(memo=True)
def process_chunk(scope: coco.Scope, chunk: Chunk) -> Embedding:
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
def process_chunk(scope: coco.Scope, chunk: Chunk) -> Embedding:
    # Bumping version forces re-execution even if code looks the same
    return embed(chunk.text)
```

:::note
The change tracking capability is still under construction.
:::

## Propagation of scope

`Scope` is CocoIndex's execution context: it identifies *where* a call happens in the scope tree and is required for declaring target states and mounting processing components (see [Scope](./processing_component.md#scope)). A function decorated with `@coco.function` needs access to a `Scope` to achieve its capabilities. There are two ways to provide it:

### Explicit: pass scope as first argument

```python
@coco.function(memo=True)
def process_file(scope: coco.Scope, file: FileLike) -> None:
    # scope is explicitly passed
    ...

# Called with scope as first argument

process_file(scope, file)

```

### Implicit: context variable propagation

If `Scope` is not passed as the first argument, CocoIndex propagates it through Python's `contextvars` automatically. (`contextvars` is a Python standard-library feature for "context-local" variables that flow through normal function calls and `await`s, so code can access a value without passing it through every signature.) This works for ordinary function calls — both sync and async:

```python
@coco.function(memo=True)
def helper_function(data: str) -> str:
    # No scope argument, but scope is available via contextvar
    return data.upper()

@coco.function
def process_file(scope: coco.Scope, file: FileLike) -> None:
    text = file.read_text()
    # helper_function receives scope implicitly
    result = helper_function(text)
```

However, implicit propagation **does not work** in situations where Python's context variables are not preserved (for example, thread pool dispatch via `concurrent.futures.ThreadPoolExecutor`).
In these cases, pass `Scope` explicitly as the first argument.

```python
from concurrent.futures import ThreadPoolExecutor

with ThreadPoolExecutor() as executor:
    # Pass scope explicitly across the thread boundary
    future = executor.submit(process_file, scope, file)
```

:::tip When to pass scope explicitly

- If your function declares target states or mounts child processing components, you need `Scope` anyway — pass it explicitly as the first argument.
- Otherwise, your function is likely a pure transformation. Prefer keeping the signature simple (which signals it's a pure transformation), and only pass `Scope` explicitly at the hop where context variables aren't preserved (e.g., `ThreadPoolExecutor`).

**Rule of thumb:** You only need to pay special attention when invoking functions in a way that context variables are not preserved — such as `ThreadPoolExecutor`.
:::
