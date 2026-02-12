# API Upgrade Spec: v1 Syntax Sugar

This document specifies the new convenience APIs introduced in v1, built on top of the existing `mount()` and `mount_run()` primitives. All APIs below are async (`coco_aio`).

## Concepts

### StableKey

A value that can be used as a component path segment. Must be one of:

```python
StableKey = None | bool | int | str | bytes | uuid.UUID | Symbol | tuple[StableKey, ...]
```

#### Symbol

A namespaced key type that cannot collide with user-provided strings. Used internally by convenience APIs (e.g., `mount_target()` uses a predefined symbol as a top-level path segment).

### Target

A target object (e.g., from `target_db.table_target(...)`) that declares a container target state and provides a child target (e.g., `TableTarget`) for declaring child target states (rows, files, points).

Targets have globally unique keys by construction. For example, a Postgres table target's key is composed from `"cocoindex.io/postgres"`, the database key, and the table name — so it is inherently unique without relying on user-provided subpaths.

## API Summary

| API | Purpose | Returns |
|-----|---------|---------|
| `mount()` | Mount an independent component | `MountHandle` |
| `use_mount()` | Mount a dependent component, caller depends on result | `T` |
| `mount_each()` | Mount one component per item in a keyed iterable | `None` |
| `mount_target()` | Mount a target (sugar over `use_mount()`) | `T` |
| `map()` | Run a function concurrently on each item (no mounting) | `list[T]` |

## API Details

### `mount()`

Mount an independent processing component. The child component can refresh independently of its parent in live mode.

```python
coco_aio.mount(subpath, fn, *args, **kwargs) -> MountHandle
```

**Parameters:**
- `subpath` *(ComponentSubpath)* — The component subpath.
- `fn` *(callable)* — The function to run as the processing component.
- `*args, **kwargs` — Passthrough arguments to `fn`.

**Returns:** `MountHandle` with a `ready()` method.

### `use_mount()`

Mount a dependent processing component and return its result. The child component **cannot** refresh independently — re-executing the child requires re-executing the parent.

The `use_` prefix (consistent with `use_context()`) signals that the caller creates a dependency on the child's result.

**Key difference from the old `mount_run()`:** `mount_run()` returned a handle; calling `.result()` on the handle was a separate step. `use_mount()` directly returns the value (after `await`).

```python
await coco_aio.use_mount(subpath, fn, *args, **kwargs) -> T
```

**Parameters:** Same as `mount()`.

**Returns:** `T` — The return value of `fn`, available after the child component is ready.

### `mount_each()`

Mount one independent component per item in a keyed iterable. Sugar over a loop of `mount()` calls.

```python
coco_aio.mount_each(fn, items, *args, **kwargs) -> None
```

**Parameters:**
- `fn` *(callable)* — The function to run for each item. The item value is passed as the first argument.
- `items` *(Iterable[tuple[StableKey, T]])* — A keyed iterable. Each element is a `(key, value)` pair. The key is used as the component subpath; the value is passed to `fn`.
- `*args, **kwargs` — Additional passthrough arguments to `fn` (appended after the item value).

**Returns:** `None`.

**Note on keys:** Stable keys are a property of membership in a collection, not an intrinsic property of the element. For example, a file's relative path is only unique within its directory. Source connectors provide key-value pairs (e.g., via an `items()` method) where the connector determines the appropriate key.

**Equivalent to:**
```python
for key, item in items:
    coco_aio.mount(coco.component_subpath(key), fn, item, *args, **kwargs)
```

### `mount_target()`

Mount a target, ensuring its container target state is applied before returning the child target. Sugar over `use_mount()` combined with `declare_target_state_with_child()`.

The `target` argument is a `TargetState` — the same type created by `TargetStateProvider.target_state(key, value)` and normally passed to `coco.declare_target_state_with_child()`. For example, this is what `PgDatabase.declare_table_target()` does internally today:

```python
# Current manual workflow (inside declare_table_target):
provider = coco.declare_target_state_with_child(
    _table_provider.target_state(key, spec)   # <-- this is the TargetState
)
return TableTarget(provider, table_schema)
```

With `mount_target()`, the user passes the `TargetState` directly, and `mount_target()` handles declaring it in a separate component, waiting for it to be applied, and returning the child target:

```python
await coco_aio.mount_target(target_state) -> T
```

Targets have globally unique keys by construction (e.g., a Postgres table target's key includes `"cocoindex.io/postgres"`, the database key, and the table name). The component subpath is derived from this key automatically. Users do **not** need to provide an explicit subpath or wrap in `with coco.component_subpath(...)`.

**Parameters:**

- `target_state` *(TargetState)* — A `TargetState` with a child handler, as created by `TargetStateProvider.target_state(key, value)`. The key must be globally unique (target connectors ensure this by construction).

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

## Migration from Previous API

| Before | After |
|--------|-------|
| `await mount_run(subpath, fn, *args).result()` | `await use_mount(subpath, fn, *args)` |
| `for key, item in items: mount(subpath(key), fn, item, *args)` | `mount_each(fn, items, *args)` |
| `with component_subpath("setup"): await mount_run(...).result()` | `await mount_target(target)` |
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
    async for file in files:
        coco_aio.mount(
            coco.component_subpath("file", str(file.file_path.path)),
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
