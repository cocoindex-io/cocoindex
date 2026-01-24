---
title: App
description: Understanding Apps as the top-level runnable unit, including creation, running, and environment lifecycle.
---

# App

An **App** is the top-level runnable unit in CocoIndex. 
It names your pipeline and binds a main function with its parameters. When you call `app.update()`, CocoIndex runs that main function as the root [processing component](./processing_component.md) which can mount child processing components to do work and declare target states.

## Creating an app

To create an App, provide:

1. **A main function** — the entry point that receives a `Scope` as its first argument
2. **An `AppConfig`** — at minimum, a name for the pipeline
3. **Arguments** — any additional arguments to pass to the main function

```python
import cocoindex.asyncio as coco_aio

@coco_aio.function
def app_main(scope: coco_aio.Scope, sourcedir: pathlib.Path) -> None:
    # ... your pipeline logic ...

app = coco_aio.App(
    app_main,
    coco_aio.AppConfig(name="MyPipeline"),
    sourcedir=pathlib.Path("./data"),
)
```

The corresponding sync API:

```python
import cocoindex as coco

@coco.function
def app_main(scope: coco.Scope, sourcedir: pathlib.Path) -> None:
    # ... your pipeline logic ...

app = coco.App(
    app_main,
    coco.AppConfig(name="MyPipeline"),
    sourcedir=pathlib.Path("./data"),
)
```

:::tip
The main function can be sync or async regardless of whether you use `coco.App` or `coco_aio.App`. See [Mixing Sync and Async](./sdk_overview.md#mixing-sync-and-async) for details.
:::

## Updating an app

Call `update()` to execute the pipeline:

```python
# Async API
await app.update(report_to_stdout=True)
```

```python
# Sync API
app.update(report_to_stdout=True)
```

The `report_to_stdout` option prints periodic progress updates during execution.

When you update an App, CocoIndex:

1. Runs the lifespan setup (if not already done)
2. Executes the main function (the root processing component), which mounts child processing components
3. Syncs all declared target states to external systems
4. Compares with the previous run and applies only necessary changes

Given the same code and inputs, updates are repeatable. When data or code changes, only the affected parts re-execute.

## How an app runs

An App is the top-level runner and entry point. A **processing component** is the unit of incremental execution *within* an app.

- Your app's main function runs as the **root processing component** at the root scope.
- Each call to `mount()` or `mount_run()` declares a **child processing component** at a child scope.
- Each processing component declares a set of target states, and CocoIndex syncs them atomically when that component finishes.

This is why `app.update()` does not "run everything from scratch": CocoIndex uses the scope tree to decide what can be reused and what must re-run.

For example, an app that processes files might mount one component per file:

```text
(root)                         ← app_main component
├── "setup"                    ← declare_dir_target component
└── "process"
    ├── "hello.md"             ← process_file component
    └── "world.md"             ← process_file component
```

See [Processing Component](./processing_component.md) for how mounting and scopes define these boundaries.

## Lifespan

Apps are typically updated many times. A **lifespan function** defines the CocoIndex runtime lifecycle: its setup runs when the runtime starts (automatically before the first `app.update()`), and its cleanup runs when the runtime stops. Use it to configure CocoIndex and initialize shared resources that processing components can reuse.

### Defining a lifespan

Use the `@lifespan` decorator to define the function. It receives an `EnvironmentBuilder` for configuration and uses `yield` to separate setup from cleanup:

```python
import cocoindex.asyncio as coco_aio

@coco_aio.lifespan
async def coco_lifespan(builder: coco_aio.EnvironmentBuilder) -> AsyncIterator[None]:
    # Configure CocoIndex's internal database location
    builder.settings.db_path = pathlib.Path("./cocoindex.db")
    # Setup: initialize resources here
    yield
    # Cleanup happens automatically when the context exits
```

The `db_path` setting specifies where CocoIndex stores its internal database. CocoIndex uses this database to track target states and memoized results from previous runs, enabling it to compute what changed and apply only the necessary updates. You should provide a path on your local filesystem.

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
