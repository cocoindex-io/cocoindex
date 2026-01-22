---
title: Processing Component
description: Understanding processing components as the sync boundaries for target states, including mounting APIs.
---

# Processing Component

Your pipeline often processes many items — files, rows, entities — where each can be handled independently.
A **Processing Component** groups an item's processing together with its output target states.
Each Processing Component runs on its own and applies its target states as soon as it completes, without waiting for the rest of the pipeline.

Processing Components are identified by **stable paths** (e.g., you can construct it using file names, row keys, entity IDs, etc.), which CocoIndex uses to track and reconcile target states across runs.

See [Scope](./sdk_overview.md#scope) in the SDK Overview for details on how scopes and stable paths work.

## Hierarchical Structure

Processing Components form a tree. An [App](./app.md) establishes a root Processing Component, which can **mount** child Processing Components. Each child can mount its own children, and so on.

This hierarchy is how CocoIndex tracks ownership: when a parent no longer mounts a child (e.g., a source file is deleted), CocoIndex automatically cleans up the child's target states.

## Target State Sync Boundaries

After a processing component finishes execution, CocoIndex syncs its target states to external systems:

1. **Compares** the target states declared in this run against those from the previous run at the same path
2. **Applies changes** as a unit — creating new target states, updating changed ones, and deleting target states that are no longer declared
3. **Recursively handles** child processing components that are no longer mounted, cleaning up their target states as well

This boundary provides clear ownership and atomic updates. For example:

- If a source file changes, its processing component's target states are applied as a unit — atomically and not blocked by other processing components
- If a source file is removed, after the parent's function executes, CocoIndex notices the child processing component is no longer mounted and removes its target states

## Mounting Processing Components

CocoIndex provides two APIs to mount processing components: `mount()` and `mount_run()`.

### `mount()` — When You Don't Need the Return Value

Use `mount()` when you don't need a return value from the processing component. It schedules the processing component to run and returns a handle:

```python
handle = coco_aio.mount(process_file, scope / "file" / filename, file, target)
```

The handle provides a method you can call if you need to wait until the processing component is fully ***ready*** — meaning all its target states have been applied to external systems and all its children are ready:

```python
await handle.ready()  # Async API
```

The corresponding sync API:

```python
handle = coco.mount(process_file, scope / "file" / filename, file, target)
handle.wait_until_ready()  # Blocks until ready
```

You usually only need to call `ready()` (or `wait_until_ready()` in sync) when you have logic that depends on the processing component's target states being applied — for example, querying the latest data from a target table after syncing it.

### `mount_run()` — When You Need the Return Value

Use `mount_run()` when you need the processing component's return value. It returns a handle with a `result()` method:

```python
handle = coco_aio.mount_run(setup_table, scope / "setup", table_name="docs")
table = await handle.result()  # Waits until ready, then returns the value
```

Calling `result()` waits until the processing component is ready and then returns the value.

The corresponding sync API:

```python
handle = coco.mount_run(setup_table, scope / "setup", table_name="docs")
table = handle.result()  # Blocks until ready, then returns the value
```

A common use of `mount_run()` is to obtain a [target states provider](./target_state#obtaining-target-state-providers) after its parent target state is applied.

## Granularity Trade-offs

The granularity of processing components determines target state sync boundaries:

- **Coarse-grained** (fewer, larger processing components): More target states sync together as a unit, but you must wait until all items are processed before any target states are synced — higher latency to see outcomes.
- **Fine-grained** (more, smaller processing components): Each processing component's target states sync independently as soon as it completes — lower latency, but target states from different processing components are not synced atomically together.

For small datasets, a single processing component that owns all target states is simple and ensures all target states sync atomically. As data grows, breaking work into per-item processing components (e.g., one per file) reduces latency — you see each file's target states synced as soon as it's processed, without waiting for all files to complete.
