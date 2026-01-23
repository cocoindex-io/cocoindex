---
title: Processing Component
description: Understanding processing components as the sync boundaries for target states, including mounting APIs.
---

Your pipeline often processes many items — files, rows, entities — where each can be handled independently.
A **Processing Component** groups an item's processing together with its output target states.
Each Processing Component runs on its own and applies its target states as soon as it completes, without waiting for the rest of the pipeline.

## The Scope Tree

Scopes form a tree structure. You create child scopes using the `/` operator with stable identifiers like literals, file names, row keys, or entity IDs:

```python
scope / filename           # e.g., scope / "hello.md"
scope / "user" / user_id   # e.g., scope / "user" / 12345
```

Each processing component must be mounted in a **unique scope**. The scope uniquely identifies the component across runs — CocoIndex uses this to match target states from previous runs, determine what changed, and apply updates atomically.

Here's an example scope tree (from the [Quickstart](../getting_started/quickstart.md)):

```text
(root)                         ← app_main component
├── "setup"                    ← declare_dir_target component
└── "process"
    ├── "hello.md"             ← process_file component
    └── "world.md"             ← process_file component
```

See [Scope](./sdk_overview.md#scope) in the SDK Overview for details on scopes and `StableKey`.

## Mounting APIs

CocoIndex provides two APIs to mount processing components: `mount()` and `mount_run()`.

- `mount()` sets up a processing component in a child scope without depending on data from it. This allows the component to refresh independently in live mode.
- `mount_run()` returns a value from the component's execution to the caller. The component at that scope cannot refresh independently without re-executing the caller.

Usually, only use `mount_run()` when you need the return value.

### `mount()`

Use `mount()` when you don't need a return value from the processing component. It schedules the processing component to run and returns a handle:

```python
handle = coco_aio.mount(process_file, scope / "process" / filename, file, target)
```

The handle provides a method you can call if you need to wait until the processing component is fully ***ready*** — meaning all its target states have been applied to external systems and all components in its sub-scopes are ready:

```python
await handle.ready()  # Async API
```

The corresponding sync API:

```python
handle = coco.mount(process_file, scope / "process" / filename, file, target)
handle.wait_until_ready()  # Blocks until ready
```

You usually only need to call `ready()` (or `wait_until_ready()` in sync) when you have logic that depends on the processing component's target states being applied — for example, querying the latest data from a target table after syncing it.

### `mount_run()`

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

A common use of `mount_run()` is to obtain a [target states provider](./target_state#obtaining-target-states-providers) after its target state is applied.

## How Target States Sync

The scope tree determines ownership. When a component is no longer mounted at a scope (e.g., a source file is deleted), CocoIndex automatically cleans up its target states — and recursively for all its sub-scopes.

:::info[Sync Mechanism]

After a processing component finishes, CocoIndex syncs its target states:

1. **Compares** the target states declared in this run against those from the previous run in the same scope
2. **Applies changes** as a unit — creating, updating, or deleting target states as needed
3. **Recursively cleans up** sub-scopes where components are no longer mounted

This provides atomic updates per component. For example, if a source file changes, its component's target states are applied atomically.

:::

## Granularity Trade-offs

The granularity of processing components determines target state sync boundaries:

- **Coarse-grained** (fewer, larger processing components): More target states sync together as a unit, but you must wait until all items are processed before any target states are synced — higher latency to see outcomes.
- **Fine-grained** (more, smaller processing components): Each processing component's target states sync independently as soon as it completes — lower latency, but target states from different processing components are not synced atomically together.

For small datasets, a single processing component that owns all target states is simple and ensures all target states sync atomically. As data grows, breaking work into per-item processing components (e.g., one per file) reduces latency — you see each file's target states synced as soon as it's processed, without waiting for all files to complete.
