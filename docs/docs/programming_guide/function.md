---
title: Function
description: Understanding the @function decorator, its capabilities like memoization and change tracking, and how functions access Scope.
---

# Function

A Python function can be decorated with `@function` to gain additional capabilities from CocoIndex. The decorator doesn't change the function's signature — it remains a regular Python function that you can call normally.

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

## Capabilities

The `@function` decorator provides additional capabilities:

### Memoization

With `memo=True`, the function is memoized — when input data and code haven't changed, CocoIndex skips recomputation, carries over the function's previous effects, and returns its previous return value.

```python
@coco.function(memo=True)
def process_chunk(scope: coco.Scope, chunk: Chunk) -> Embedding:
    # This computation is skipped if chunk and code are unchanged
    return embed(chunk.text)
```

See [Memoization Keys](../advanced_topics/memoization_keys.md) for details on how CocoIndex constructs keys for memoization.

### Change Tracking

The logic of a function decorated with `@function` is tracked based on the content of the function. When a function's implementation changes, CocoIndex detects this and re-executes affected call sites.

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

## Propagation of Scope

A function decorated with `@function` needs access to a `Scope` to achieve its capabilities. There are two ways to provide it:

### Explicit: Pass Scope as First Argument

```python
@coco.function(memo=True)
def process_file(scope: coco.Scope, file: FileLike) -> None:
    # scope is explicitly passed
    ...

# Called with scope as first argument

process_file(scope, file)

```

### Implicit: Context Variable Propagation

If `Scope` is not passed as the first argument, CocoIndex propagates it through Python's context variables automatically. This works for ordinary function calls — both sync and async:

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

However, implicit propagation **does not work** in situations where Python's context variables are not preserved for some situations, e.g. Thread pool dispatch (`concurrent.futures.ThreadPoolExecutor`).
In these cases, pass `Scope` explicitly as the first argument.

:::tip When to Pass Scope Explicitly

- If your function declares effects or mounts child processing components, you need `Scope` anyway — pass it explicitly as the first argument.
- Otherwise, your function is likely a pure transformation. Prefer keeping the signature simple (which signals it's a pure transformation), and only pass `Scope` explicitly at the hop where context variables aren't preserved (e.g., `ThreadPoolExecutor`).

**Rule of thumb:** You only need to pay special attention when invoking functions in a way that context variables are not preserved — such as `ThreadPoolExecutor`.
:::
