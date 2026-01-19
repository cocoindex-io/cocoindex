---
title: Processing Unit
description: Understanding processing units as the sync boundaries for effects, including mounting APIs.
---

# Processing Unit

A **Processing Unit** is a long-lived instance that defines the boundary where CocoIndex syncs effects to external systems. Processing Units are identified by a stable path and own all effects declared within them.

See [Scope](./sdk_overview.md#scope) in the SDK Overview for details on how scopes and stable paths work.

## Effect Sync Boundaries

After a processing unit finishes execution, CocoIndex syncs its effects to external systems:

1. **Compares** the effects declared in this run against those from the previous run at the same path
2. **Applies changes** as a unit — creating new effects, updating changed ones, and deleting effects that are no longer declared
3. **Recursively handles** child processing units that are no longer mounted, cleaning up their effects as well

This boundary provides clear ownership and atomic updates. For example:

- If a source file changes, its processing unit's effects are applied as a unit — atomically and not blocked by other processing units
- If a source file is removed, after the parent's function executes, CocoIndex notices the child processing unit is no longer mounted and removes its effects

## Mounting Processing Units

CocoIndex provides two APIs to mount processing units: `mount()` and `mount_run()`.

### `mount()` — When You Don't Need the Return Value

Use `mount()` when you don't need a return value from the processing unit. It schedules the processing unit to run and returns a handle:

```python
handle = coco_aio.mount(process_file, scope / "file" / filename, file, target)
```

The handle provides a method you can call if you need to wait until the processing unit is fully ***ready*** — meaning all its effects have been applied to external systems and all its children are ready:

```python
await handle.ready()  # Async API
```

The corresponding sync API:

```python
handle = coco.mount(process_file, scope / "file" / filename, file, target)
handle.wait_until_ready()  # Blocks until ready
```

You usually only need to call `ready()` (or `wait_until_ready()` in sync) when you have logic that depends on the processing unit's effects being applied — for example, querying the latest data from a target table after syncing it.

### `mount_run()` — When You Need the Return Value

Use `mount_run()` when you need the processing unit's return value. It returns a handle with a `result()` method:

```python
handle = coco_aio.mount_run(setup_table, scope / "setup", table_name="docs")
table = await handle.result()  # Waits until ready, then returns the value
```

Calling `result()` waits until the processing unit is ready and then returns the value.

The corresponding sync API:

```python
handle = coco.mount_run(setup_table, scope / "setup", table_name="docs")
table = handle.result()  # Blocks until ready, then returns the value
```

A common use of `mount_run()` is to obtain an [effect provider](./effect#obtaining-effect-providers) after its parent effect is applied.

## Granularity Trade-offs

The granularity of processing units determines effect sync boundaries:

- **Coarse-grained** (fewer, larger processing units): More effects sync together as a unit, but you must wait until all items are processed before any effects are synced — higher latency to see outcomes.
- **Fine-grained** (more, smaller processing units): Each processing unit's effects sync independently as soon as it completes — lower latency, but effects from different processing units are not synced atomically together.

For small datasets, a single processing unit that owns all effects is simple and ensures all effects sync atomically. As data grows, breaking work into per-item processing units (e.g., one per file) reduces latency — you see each file's effects synced as soon as it's processed, without waiting for all files to complete.
