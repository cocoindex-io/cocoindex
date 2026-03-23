---
title: Exception Handlers
description: Monitor and handle exceptions from background-mounted components using global and scoped exception handlers.
---

# Exception Handlers

When `mount()` or `mount_each()` runs a component in the background, any exception it raises is caught and logged by default. This isolation property is intentional — a failure in one child component does not affect siblings.

Sometimes you want to observe or handle these exceptions yourself — for example, to send alerts, record metrics, or implement custom retry logic. CocoIndex lets you register **exception handlers** at two levels:

- **Global (environment-level)**: registered once in your lifespan function; applies to all background mounts in the environment.
- **Scoped**: an async context manager that applies to all `mount()` / `mount_each()` calls made within it.

:::note
This only applies to `mount()` and `mount_each()`. `use_mount()` propagates errors directly to the caller since the parent has an explicit dependency on the result.
:::

## Global exception handler

Register a handler inside your `@coco.lifespan` function using `builder.set_exception_handler()`:

```python
import cocoindex as coco

@coco.lifespan
def lifespan(builder: coco.EnvironmentBuilder):
    def on_error(exc: BaseException, ctx: coco.ExceptionContext) -> None:
        print(f"[{ctx.env_name}] {ctx.mount_kind} failed at {ctx.stable_path}: {exc}")

    builder.set_exception_handler(on_error)
    yield
```

This replaces the default "log error" behavior for all background mounts in the environment.

## Scoped exception handler

Use `coco.exception_handler()` as an async context manager to apply a handler to a specific dynamic scope:

```python
@coco.fn
async def process_all(files):
    def on_error(exc: BaseException, ctx: coco.ExceptionContext) -> None:
        print(f"Failed processing {ctx.stable_path}: {exc}")

    async with coco.exception_handler(on_error):
        for f in files:
            await coco.mount(coco.component_subpath(str(f.path)), process_file, f)
```

The handler applies to all `mount()` / `mount_each()` calls within the `async with` block, including those in nested functions called from within the block.

## Handler type

Both sync and async handlers are supported:

```python
from typing import Awaitable

# Sync handler
def sync_handler(exc: BaseException, ctx: coco.ExceptionContext) -> None:
    ...

# Async handler
async def async_handler(exc: BaseException, ctx: coco.ExceptionContext) -> None:
    await send_alert(exc, ctx)
```

The type alias is:

```python
ExceptionHandler = Callable[
    [BaseException, ExceptionContext],
    None | Awaitable[None],
]
```

## `ExceptionContext` fields

Your handler receives an `ExceptionContext` dataclass with information about the failure:

| Field | Type | Description |
|---|---|---|
| `env_name` | `str` | Name of the CocoIndex environment |
| `stable_path` | `str` | Full stable path of the failing component |
| `processor_name` | `str \| None` | Name of the processor (best-effort) |
| `mount_kind` | `"mount" \| "mount_each" \| "delete_background"` | How the component was mounted |
| `parent_stable_path` | `str \| None` | Stable path of the parent component |
| `is_background` | `bool` | Always `True` for exception handler invocations |
| `source` | `"component" \| "handler"` | `"component"` for the original failure; `"handler"` if a handler itself raised |
| `original_exception` | `BaseException \| None` | The original component exception, set only when `source == "handler"` |

## Handler stacking and fallback

Handlers are stacked: the most specific (innermost) handler runs first.

If the innermost handler raises an exception, the next outer handler is called with that new exception. In this case `ctx.source` is `"handler"` and `ctx.original_exception` holds the original component error.

This continues up the stack. If all handlers raise (or no handler is registered), CocoIndex falls back to the built-in behavior: logging the error at `ERROR` level, with no crash.

```python
@coco.lifespan
def lifespan(builder: coco.EnvironmentBuilder):
    builder.settings.db_path = "..."

    def global_handler(exc: BaseException, ctx: coco.ExceptionContext) -> None:
        if ctx.source == "handler":
            # A handler itself failed — exc is the handler's exception,
            # ctx.original_exception is the original component error.
            print(f"Handler error: {exc}; original: {ctx.original_exception}")
        else:
            print(f"Component error: {exc}")

    builder.set_exception_handler(global_handler)
    yield

@coco.fn
async def _root() -> None:
    def inner_handler(exc: BaseException, ctx: coco.ExceptionContext) -> None:
        print(f"inner: {exc}")
        raise RuntimeError("inner handler failed")  # falls through to global_handler

    async with coco.exception_handler(inner_handler):
        await coco.mount(coco.component_subpath("child"), _child)
```

## No breaking changes

Users who never register handlers see identical behavior to before — exceptions from background mounts are logged at `ERROR` level and siblings continue unaffected.
