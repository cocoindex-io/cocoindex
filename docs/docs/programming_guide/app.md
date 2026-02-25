---
title: App
description: Understanding Apps as the top-level runnable unit, including creation, running, and environment lifecycle.
---

# App

An **App** is the top-level runnable unit in CocoIndex.
It names your pipeline and binds a main function with its parameters. When you call `app.update()`, CocoIndex runs that main function as the root [processing component](./processing_component.md) which can mount child processing components to do work and declare target states.

## Creating an app

To create an App, provide:

1. **An `AppConfig`** (or just a name string) — identifies the pipeline
2. **A main function** — the entry point for your pipeline
3. **Arguments** — any additional arguments to pass to the main function

```python
import cocoindex.asyncio as coco_aio

@coco_aio.function
def app_main(sourcedir: pathlib.Path) -> None:
    # ... your pipeline logic ...

app = coco_aio.App(
    coco_aio.AppConfig(name="MyPipeline"),
    app_main,
    sourcedir=pathlib.Path("./data"),
)
```

The corresponding sync API:

```python
import cocoindex as coco

@coco.function
def app_main(sourcedir: pathlib.Path) -> None:
    # ... your pipeline logic ...

app = coco.App(
    coco.AppConfig(name="MyPipeline"),
    app_main,
    sourcedir=pathlib.Path("./data"),
)
```

You can also pass just a name string instead of `AppConfig`:

```python
app = coco.App("MyPipeline", app_main, sourcedir=pathlib.Path("./data"))
```

:::tip
The main function can be sync or async regardless of whether you use `coco.App` or `coco_aio.App`. See [Mixing Sync and Async](./sdk_overview.md#mixing-sync-and-async) for details.
:::

## Updating an app

Call `update()` to execute the pipeline:

```python
# Async API
await app.update(report_to_stdout=True, full_reprocess=False)
```

```python
# Sync API
app.update(report_to_stdout=True, full_reprocess=False)
```

**Parameters:**

- `report_to_stdout` option prints periodic progress updates during execution.
- `full_reprocess` option reprocesses everything and invalidates existing caches. This forces all components to re-execute and all target states to be re-applied, even if they haven't changed.

When you update an App, CocoIndex:

1. Runs the lifespan setup (if not already done)
2. Executes the main function (the root processing component), which mounts child processing components
3. Syncs all declared target states to external systems
4. Compares with the previous run and applies only necessary changes

Given the same code and inputs, updates are repeatable. When data or code changes, only the affected parts re-execute.

## How an app runs

An App is the top-level runner and entry point. A **processing component** is the unit of incremental execution *within* an app.

- Your app's main function runs as the **root processing component** at the root path.
- Each call to `mount()` or `use_mount()` declares a **child processing component** at a child path. Sugar APIs like `mount_each()` and `mount_target()` also create child components.
- Each processing component declares a set of target states, and CocoIndex syncs them atomically when that component finishes.

This is why `app.update()` does not "run everything from scratch": CocoIndex uses the component path tree to decide what can be reused and what must re-run.

For example, an app that processes files might mount one component per file:

```text
(root)                         ← app_main component
├── "setup"                    ← declare_dir_target component
└── "process"
    ├── "hello.md"             ← process_file component
    └── "world.md"             ← process_file component
```

See [Processing Component](./processing_component.md) for how mounting and component paths define these boundaries.

## Concurrency control

By default, CocoIndex limits the number of concurrently executing processing components to **1024** per app. When components perform resource-intensive work (e.g., calling external APIs, running ML models), you may want to lower this limit.

Set `max_inflight_components` in `AppConfig` to control the limit:

```python
app = coco.App(
    coco.AppConfig(name="MyPipeline", max_inflight_components=4),
    app_main,
    sourcedir=pathlib.Path("./data"),
)
```

With `max_inflight_components=4`, at most 4 processing components execute at the same time. When a component finishes, the next pending one starts.

Setting `max_inflight_components=1` serializes all components — only one runs at a time.

You can also set the limit via the `COCOINDEX_MAX_INFLIGHT_COMPONENTS` environment variable:

```bash
export COCOINDEX_MAX_INFLIGHT_COMPONENTS=4
```

**Precedence:** `AppConfig` value > environment variable > default (1024).

:::info[Deadlock Prevention]
When a parent component mounts a child, the parent releases its concurrency slot so the child can make progress. This prevents deadlocks in nested mount scenarios — even with `max_inflight_components=1`, a parent mounting a child will not block forever.
:::

## Database path

CocoIndex needs a database path (`db_path`) to store its internal state. This database tracks target states and memoized results from previous runs, enabling CocoIndex to compute what changed and apply only the necessary updates.

The simplest way to configure the database path is via the `COCOINDEX_DB` environment variable:

```bash
export COCOINDEX_DB=./cocoindex.db
```

With `COCOINDEX_DB` set, you can create and run apps without any additional configuration:

```python
import cocoindex as coco

@coco.function
def app_main() -> None:
    # ... your pipeline logic ...

app = coco.App("MyPipeline", app_main)
app.update()  # Uses COCOINDEX_DB for storage
```

## Lifespan (optional)

A **lifespan function** defines the CocoIndex runtime lifecycle: its setup runs when the runtime starts (automatically before the first `app.update()`), and its cleanup runs when the runtime stops. Use it to configure CocoIndex settings programmatically or to initialize shared resources that processing components can reuse.

:::tip
If you only need to set the database path, using the `COCOINDEX_DB` environment variable is simpler than defining a lifespan function.
:::

### Defining a lifespan

Use the `@lifespan` decorator to register a lifespan function. By default, all apps share the same lifespan (unless you explicitly specify an app in a different [*Environment*](../advanced_topics/multiple_environments.md)). The function receives an `EnvironmentBuilder` for configuration and uses `yield` to separate setup from cleanup:

```python
import cocoindex.asyncio as coco_aio

@coco_aio.lifespan
async def coco_lifespan(builder: coco_aio.EnvironmentBuilder) -> AsyncIterator[None]:
    # Configure CocoIndex's internal database location (overrides COCOINDEX_DB if set)
    builder.settings.db_path = pathlib.Path("./cocoindex.db")
    # Setup: initialize resources here
    yield
    # Cleanup happens automatically when the context exits
```

Setting `db_path` in the lifespan takes precedence over the `COCOINDEX_DB` environment variable. If neither is provided, CocoIndex will raise an error.

The lifespan function can be sync or async:

```python
import cocoindex as coco

@coco.lifespan
def coco_lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
    builder.settings.db_path = pathlib.Path("./cocoindex.db")
    yield
```

You can also use the lifespan to provide resources (like database connections) that processing components can access. See [Context](./context.md) for details on sharing resources across your pipeline.

### Explicit lifecycle control (optional)

The lifespan runs automatically the first time any App updates — most users don't need to do anything beyond defining the lifespan and calling `app.update()`.

If you need more explicit control — for example, to know when startup completes for health checks, or to explicitly trigger shutdown — you can manage the lifecycle directly:

```python
# Async API
await coco_aio.start()   # Run lifespan setup
# ... run apps or other operations ...
await coco_aio.stop()    # Run lifespan cleanup
```

```python
# Sync API
coco.start()   # Run lifespan setup
# ... run apps or other operations ...
coco.stop()    # Run lifespan cleanup
```

Or use the `runtime()` context manager:

```python
# Async API
async with coco_aio.runtime():
    await app.update()
```

```python
# Sync API
with coco.runtime():
    app.update()
```

## Managing apps with CLI

CocoIndex provides a CLI for managing your apps without writing additional code.

### Update an app

Run your app once to sync all target states:

```bash
cocoindex update main.py
```

This executes your pipeline and applies all declared target states to external systems.

### Drop an app

Remove an app and revert all its target states:

```bash
cocoindex drop main.py
```

This will delete all target states created by the app (e.g., drop tables, delete rows) and clear its internal state.

See [CLI Reference](../cli) for more commands and options.
