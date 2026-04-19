---
title: Error Handling
description: How CocoIndex handles failures, recovery from interrupted updates, and the exception handler API for observing background component errors.
---

# Error Handling

This page covers the full picture of failure behavior in CocoIndex — from how components fail in isolation, through what happens during interrupted updates, to the APIs for observing and reacting to errors in production.

For a quick overview of failure isolation and the two-phase model, see [What happens when a component fails](../programming_guide/processing_component.md#what-happens-when-a-component-fails) in the Processing Component guide.

## Failure isolation recap

- **`use_mount()`** — data dependency: the child's exception propagates to the parent.
- **`mount()` and `mount_each()`** — background: a failure in one child does **not** affect the parent or siblings. By default the exception is logged at `ERROR` level.

## Processing and submit phases

CocoIndex processes each component in two phases:

1. **Processing** — runs your function, declares target states in memory. This phase is side-effect-free. If it fails (e.g., a parsing error, an API timeout), no writes were attempted.
2. **Submit** — writes changes to target backends. This phase only runs after processing completes successfully.

This separation means a processing failure never leaves partial data in your targets.

## Interrupted updates and recovery

An update can be interrupted by various events: a process kill (SIGKILL), Ctrl+C (SIGINT), an unhandled exception, or a target backend failure during submit.

**What state is left behind?**

CocoIndex's internal database (LMDB) uses transactions, so its own state is always consistent even after a crash. CocoIndex tracks all possible states a target could be in — if an update is interrupted partway through a commit, both the old and new states are retained as possibilities. This ensures no state is ever lost.

**Recovery is automatic.** On the next `app.update()`, CocoIndex computes the current desired state and reconciles against all possible previous states. The target connector converges the target to the correct state regardless of whether the previous commit partially succeeded or never ran.

For details on how target handlers deal with multiple possible previous states after an interruption, see [Custom Target Connector — Handle multiple previous states](./custom_target_connector.md#handle-multiple-previous-states).

## Monitoring errors

`app.update()` returns an `UpdateHandle` that exposes processing stats, including error counts:

```python
handle = app.update()

# Poll stats at any time
stats = handle.stats()
if stats is not None:
    print(f"Errors: {stats.total.num_errored}")

# Stream progress
async for snapshot in handle.watch():
    print(f"{snapshot.stats.total.num_errored} errors so far")
```

See [Monitoring progress](../programming_guide/app.md#monitoring-progress) for the full `UpdateHandle` API.

## Exception handlers

For background-mounted components (`mount()` and `mount_each()`), you can register **exception handlers** to observe or react to failures — for example, to send alerts, record metrics, or implement custom logic.

CocoIndex supports two levels of exception handlers:

- **Global (environment-level)**: registered once in your lifespan function; applies to all background mounts in the environment.
- **Scoped**: an async context manager that applies to all `mount()` / `mount_each()` calls made within it.

:::note
Exception handlers only apply to `mount()` and `mount_each()`. `use_mount()` propagates errors directly to the caller since the parent has an explicit dependency on the result.
:::

### Global exception handler

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

### Scoped exception handler

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

### Handler type

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

### `ExceptionContext` fields

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

### Handler stacking and fallback

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

Users who never register handlers see identical behavior to the default — exceptions from background mounts are logged at `ERROR` level and siblings continue unaffected.
