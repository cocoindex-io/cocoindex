# API Upgrade Spec: v1 Syntax Sugar

This document specifies the new convenience APIs introduced in v1, built on top of the existing `mount()` and `mount_run()` primitives. All APIs below are async (`coco_aio`).

## Concepts

### StableKey

A value that can be used as a component path segment. Must be one of:

```python
StableKey = None | bool | int | str | bytes | uuid.UUID | Symbol | tuple[StableKey, ...]
```

#### Symbol

A namespaced key type that cannot collide with user-provided strings. Used internally by convenience APIs (e.g., `mount_target()` uses a predefined symbol like `Symbol("cocoindex/setup")` as a top-level path segment).

### StableKeyProvider

An object that provides a `StableKey`. An argument is a StableKeyProvider if it satisfies any of:

1. It is itself a `StableKey` value.
2. It implements `__coco_stable_key__() -> StableKey`.

### Target

A target object (e.g., from `target_db.table_target(...)`) that declares a container target state and provides a child target (e.g., `TableTarget`) for declaring child target states (rows, files, points). A target is also a **StableKeyProvider**.

## API Summary

| API | Purpose | Returns |
|-----|---------|---------|
| `mount()` | Mount an independent component | `MountHandle` |
| `use_mount()` | Mount a dependent component, caller depends on result | `T` |
| `mount_each()` | Mount one component per item in an iterable | `None` |
| `mount_target()` | Mount a target (sugar over `use_mount()`) | `T` |
| `map()` | Run a function concurrently on each item (no mounting) | `list[T]` |

## API Details

### `mount()`

Mount an independent processing component. The child component can refresh independently of its parent in live mode.

```python
await coco_aio.mount(subpath, fn, *args, **kwargs) -> MountHandle
await coco_aio.mount(fn, first_arg, *args, **kwargs) -> MountHandle
```

**Parameters:**
- `subpath` *(optional, ComponentSubpath)* — Explicit component subpath. If omitted, `first_arg` (i.e., `args[0]` for passthrough) must be a **StableKeyProvider**.
- `fn` *(callable)* — The function to run as the processing component.
- `*args, **kwargs` — Passthrough arguments to `fn`.

**Returns:** `MountHandle` with a `ready()` method.

**Subpath resolution:** When `subpath` is omitted, the component path is derived from the stable key of the first passthrough argument. This avoids redundancy when the key is already carried by the argument (e.g., a `FileLike` whose path is the key).

### `use_mount()`

Mount a dependent processing component and return its result. The child component **cannot** refresh independently — re-executing the child requires re-executing the parent.

The `use_` prefix (consistent with `use_context()`) signals that the caller creates a dependency on the child's result.

**Key difference from the old `mount_run()`:** `mount_run()` returned a handle; calling `.result()` on the handle was a separate step. `use_mount()` directly returns the value (after `await`).

```python
await coco_aio.use_mount(subpath, fn, *args, **kwargs) -> T
await coco_aio.use_mount(fn, first_arg, *args, **kwargs) -> T
```

**Parameters:** Same as `mount()`.

**Returns:** `T` — The return value of `fn`, available after the child component is ready.

### `mount_each()`

Mount one independent component per item in an iterable. Sugar over a loop of `mount()` calls.

```python
coco_aio.mount_each(fn, items, *args, **kwargs) -> None
```

**Parameters:**
- `fn` *(callable)* — The function to run for each item. The item is passed as the first argument.
- `items` *(Iterable[T])* — The items to iterate. Each element must be a **StableKeyProvider** (its stable key is used as the component subpath).
- `*args, **kwargs` — Additional passthrough arguments to `fn` (appended after the item).

**Returns:** `None`.

**Equivalent to:**
```python
for item in items:
    coco_aio.mount(fn, item, *args, **kwargs)
    # which is equivalent to:
    # coco_aio.mount(coco.component_subpath(item.__coco_stable_key__()), fn, item, *args, **kwargs)
```

### `mount_target()`

Mount a target, ensuring its container target state is applied before returning the child target. Sugar over `use_mount()` for targets.

Internally uses a predefined `Symbol("cocoindex/setup")` as a top-level path segment, so the target's component path won't collide with user-defined paths. Users do **not** need to wrap `mount_target()` in `with coco.component_subpath("setup")`.

```python
await coco_aio.mount_target(target) -> T
await coco_aio.mount_target(subpath, target) -> T
```

**Parameters:**
- `subpath` *(optional, ComponentSubpath)* — Explicit component subpath. If omitted, derived from the target's stable key, under the internal `Symbol("cocoindex/setup")` prefix.
- `target` — A target object (e.g., from `target_db.table_target(...)`). Must be a **StableKeyProvider**.

**Returns:** The child target (e.g., `TableTarget[T]`), ready to use.

### `map()`

Run a function concurrently on each item in an iterable. No processing components are created — this is pure concurrent execution (async tasks) within the current component.

```python
await coco_aio.map(fn, items, *args, **kwargs) -> list[T]
```

**Parameters:**
- `fn` *(callable)* — The function to apply to each item. The item is passed as the first argument.
- `items` *(Iterable[S])* — The items to iterate.
- `*args, **kwargs` — Additional passthrough arguments to `fn` (appended after the item).

**Returns:** `list[T]` — Results from each invocation (may be `list[None]` for side-effecting functions).

## Subpath Resolution Rules

For `mount()`, `use_mount()`, and `mount_target()`, the first positional argument is inspected to determine whether it is a `ComponentSubpath`:

1. If the first argument is a `ComponentSubpath` instance, it is consumed as the subpath. The remaining arguments are `fn, *args, **kwargs` (for `mount`/`use_mount`) or `target` (for `mount_target`).
2. Otherwise, no explicit subpath is provided. The stable key is extracted from:
   - `mount()` / `use_mount()`: The first passthrough argument (`args[0]`), which must be a **StableKeyProvider**.
   - `mount_target()`: The target, which must be a **StableKeyProvider**. Additionally, the path is prefixed with `Symbol("cocoindex/setup")`.

The `component_subpath()` context manager still composes with these APIs:

```python
with coco.component_subpath("process"):
    coco_aio.mount_each(process_file, files, target_table)
    # Each item's path: current_path / "process" / item.__coco_stable_key__()
```

## Migration from Previous API

| Before | After |
|--------|-------|
| `await mount_run(subpath, fn, *args).result()` | `await use_mount(subpath, fn, *args)` |
| `mount(subpath, fn, item, *args)` where subpath derived from item | `mount(fn, item, *args)` |
| `for item in items: mount(subpath(item), fn, item, *args)` | `mount_each(fn, items, *args)` |
| `with component_subpath("setup"): await mount_run(subpath, declare_target, ...).result()` | `await mount_target(target)` |
| `await asyncio.gather(*(fn(item) for item in items))` | `await map(fn, items)` |

## Example: Before and After

### Before

```python
@coco.function
async def app_main(sourcedir: pathlib.Path) -> None:
    target_db = coco.use_context(PG_DB)

    target_table = await coco_aio.mount_run(
        coco.component_subpath("setup", "table"),
        target_db.declare_table_target,
        table_name=TABLE_NAME,
        table_schema=await postgres.TableSchema.from_class(CodeEmbedding, primary_key=["id"]),
        pg_schema_name=PG_SCHEMA_NAME,
    ).result()

    files = localfs.walk_dir(sourcedir, ...)
    with coco.component_subpath("file"):
        async for file in files:
            coco_aio.mount(
                coco.component_subpath(str(file.file_path.path)),
                process_file,
                file,
                target_table,
            )
```

### After

```python
@coco.function
async def app_main(sourcedir: pathlib.Path) -> None:
    target_db = coco.use_context(PG_DB)

    target_table = await coco_aio.mount_target(
        target_db.table_target(
            table_name=TABLE_NAME,
            table_schema=await postgres.TableSchema.from_class(
                CodeEmbedding, primary_key=["id"]
            ),
            pg_schema_name=PG_SCHEMA_NAME,
        )
    )

    files = localfs.walk_dir(sourcedir, ...)
    coco_aio.mount_each(process_file, files, target_table)
```
