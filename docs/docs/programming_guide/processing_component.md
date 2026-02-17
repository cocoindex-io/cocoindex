---
title: Processing Component
description: Understanding processing components as the sync boundaries for target states, including mounting APIs.
---

Most apps process many independent source items — files, rows, or entities. A **Processing Component** is the unit of execution for one: it runs that item's transformation logic and declares the set of **target states** produced for it.

## Component path

A **component path** is the stable identifier for a processing component across runs (think of it like a path in a tree). CocoIndex uses it to match a component to its previous run, detect what changed for that item, and sync that component's target states atomically when it finishes. This sync happens per component; CocoIndex does not wait for other components in the same app to complete.

Component paths are hierarchical and form a tree structure. You specify child paths using `coco.component_subpath()` with stable identifiers like string literals, file names, row keys, or entity IDs:

```python
coco.component_subpath(filename)           # e.g., coco.component_subpath("hello.pdf")
coco.component_subpath("user", user_id)    # e.g., coco.component_subpath("user", 12345)
```

Choose paths that are stable for the "same" item (e.g., file path, primary key). If an item disappears and its path is no longer present, CocoIndex cleans up the target states owned by that path (and its sub-paths).

Here's an example component path tree (from the [Quickstart](../getting_started/quickstart.md)):

```text
(root)                         ← app_main component
├── "setup"                    ← declare_dir_target component
└── "process"
    ├── "hello.pdf"            ← process_file component
    └── "world.pdf"            ← process_file component
```

See [StableKey](./sdk_overview.md#stablekey) in the SDK Overview for details on what values can be used in component paths.

## Mount

Mounting is how you declare (instantiate) a processing component within an app at a specific path, so CocoIndex knows that component exists, should run, and owns a set of target states.

CocoIndex provides two core mounting APIs:

- **`mount()`** — sets up a processing component in a child path without depending on data from it. This allows the component to refresh independently in live mode.
- **`use_mount()`** — returns a value from the component's execution to the caller. The component at that path cannot refresh independently without re-executing the caller.

And two async-only sugar APIs that simplify common patterns:

- **`mount_each()`** — mounts one component per item in a keyed iterable
- **`mount_target()`** — mounts a target without an explicit subpath

See also [Processing Helpers](./processing_helpers.md) for utility APIs like `map()` that operate within a component without creating new ones.

### `mount()`

Use `mount()` when you don't need a return value from the processing component. It schedules the processing component to run and returns a handle:

```python
handle = await coco_aio.mount(
    coco.component_subpath("process", filename),
    process_file,
    file,
    target,
)
```

The handle provides a method you can call if you need to wait until the processing component is fully ***ready*** — meaning all its target states have been applied to external systems and all components in its sub-paths are ready:

```python
await handle.ready()  # Async API
```

The corresponding sync API:

```python
handle = coco.mount(
    coco.component_subpath("process", filename),
    process_file,
    file,
    target,
)
handle.wait_until_ready()  # Blocks until ready
```

You usually only need to call `ready()` (or `wait_until_ready()` in sync) when you have logic that depends on the processing component's target states being applied — for example, querying the latest data from a target table after syncing it.

### `use_mount()`

Use `use_mount()` when you need the processing component's return value. It mounts the component, waits until it's ready, and returns the value directly:

```python
table = await coco_aio.use_mount(
    coco.component_subpath("setup"),
    setup_table,
    table_name="docs",
)
```

The corresponding sync API:

```python
table = coco.use_mount(
    coco.component_subpath("setup"),
    setup_table,
    table_name="docs",
)
```

A common use of `use_mount()` is to obtain a [target](./target_state#where-do-targets-come-from) after its container target state is applied.

### `mount_each()` {#mount-each}

`mount_each()` mounts one processing component per item in a keyed iterable. It's async-only (available in `coco_aio`).

```python
files = localfs.walk_dir(sourcedir, path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md"]))
await coco_aio.mount_each(process_file, files.items(), target)
```

Each item in the iterable is a `(key, value)` tuple. The key becomes the component subpath, and the value is passed as the first argument to the function. Any additional arguments are passed through.

This is equivalent to:

```python
for key, file in files.items():
    coco.mount(coco.component_subpath(key), process_file, file, target)
```

Source connectors provide an `items()` method that returns `(StableKey, T)` pairs. For example, `localfs.walk_dir(...).items()` yields `(relative_path, File)` tuples.

### `mount_target()` {#mount-target}

`mount_target()` mounts a target without requiring an explicit subpath. It's async-only (available in `coco_aio`).

```python
from cocoindex.connectors import localfs

dir_target = await coco_aio.mount_target(localfs.dir_target(outdir))
```

The component path is derived automatically from the target's globally unique key — you don't need to create a `component_subpath` for it. This is sugar over calling `use_mount()` with a target declaration function.

Connectors also provide convenience methods that wrap `mount_target()`:

```python
# Equivalent to the above
dir_target = await localfs.mount_dir_target(outdir)

# PostgreSQL example
table = await target_db.mount_table_target(
    table_name="doc_embeddings",
    table_schema=await postgres.TableSchema.from_class(DocEmbedding, primary_key=["id"]),
)
```

### Using `component_subpath` as a context manager

You can use `component_subpath()` as a context manager to create nested paths without repeating common prefixes:

```python
with coco.component_subpath("process"):
    for f in files:
        coco.mount(
            coco.component_subpath(str(f.relative_path)),
            process_file,
            f,
            target,
        )
```

This is equivalent to:

```python
for f in files:
    coco.mount(
        coco.component_subpath("process", str(f.relative_path)),
        process_file,
        f,
        target,
    )
```

:::tip
When iterating over keyed items, prefer [`mount_each()`](#mount-each) — it handles the loop and subpath creation for you.
:::

## How target states sync

The component path tree determines ownership. When a component is no longer mounted at a path (e.g., a source file is deleted), CocoIndex automatically cleans up its target states — and recursively for all its sub-paths.

:::info[Sync Mechanism]

After a processing component finishes, CocoIndex syncs its target states:

1. **Compares** the target states declared in this run against those from the previous run at the same path
2. **Applies changes** as a unit — creating, updating, or deleting target states as needed
3. **Recursively cleans up** sub-paths where components are no longer mounted

This provides atomic updates per component. For example, if a source file changes, its component's target states are applied atomically.

:::

## How big should a processing component be?

When defining processing components, think about granularity — what one path represents — because it determines the sync boundary for target states.

For example, if you're processing files:

- **Coarse**: one component for "all files" (`coco.component_subpath("process")`)
- **Medium**: one component per file (`coco.component_subpath("process", file_path)`)
- **Fine**: one component per chunk (`coco.component_subpath("process", file_path, chunk_id)`)

In general:

- **Coarse-grained** (fewer, larger components): More target states sync together as a unit, but you only see updates after the larger component finishes.
- **Fine-grained** (more, smaller components): Each component syncs its target states as soon as it finishes, but target states owned by different components do not sync atomically together.

For small datasets, a single processing component that owns all target states is simple and ensures all target states sync atomically. As data grows, consider breaking it down into one component per source item (e.g., one per file) to reduce latency: you see each item's target states synced as soon as it's processed, without waiting for the full dataset to complete. This also helps isolate failures to that item.

## Explicit context management

CocoIndex automatically propagates component context through Python's `contextvars`, which works for ordinary function calls (both sync and async). However, in situations where context variables are not preserved (for example, when using `concurrent.futures.ThreadPoolExecutor`), you need to explicitly capture and attach the context.

Use `coco.get_component_context()` to capture the current context, and `context.attach()` to restore it:

```python
from concurrent.futures import ThreadPoolExecutor

@coco.function
def app_main() -> None:
    # Capture the current context
    ctx = coco.get_component_context()

    def worker(item):
        # Attach the context in the worker thread
        with ctx.attach():
            # Now CocoIndex APIs work correctly
            process_item(item)

    with ThreadPoolExecutor() as executor:
        executor.map(worker, items)
```

This pattern ensures that CocoIndex can track component relationships and target state ownership even across thread boundaries.
