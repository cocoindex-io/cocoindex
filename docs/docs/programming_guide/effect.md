---
title: Effect
description: Understanding effects as units of desired external state, effect hierarchies, and how to declare effects.
---

# Effect

An **Effect** is a unit of desired external state produced by your transformations. On each run, CocoIndex compares the newly declared effects with those from the previous run and applies the necessary changes — creating, updating, or deleting — so that external systems match your intent.

Effects can form hierarchies (e.g., a table contains rows). CocoIndex connectors provide specific APIs to declare effects at each level.

See [Core Concepts](./core_concepts.md#effects-desired-targets-in-external-systems) for examples of how effects map to external system operations.

## Declaring Effects with Target Connectors

CocoIndex connectors provide **effect providers** with specific `declare_*` methods for declaring effects. For example:

- `postgres.TableTarget` provides `declare_row()` to declare a row in a table
- `localfs.DirTarget` provides `declare_file()` to declare a file in a directory

```python
# Declare a row effect
table_target.declare_row(scope, row=DocEmbedding(...))

# Declare a file effect
dir_target.declare_file(scope, filename="output.html", content=html)
```

## Obtaining Effect Providers

Some effect providers are created once a parent effect is ready. For example, you can only declare rows after the table exists, or files after the directory exists.

The pattern is:

1. **Mount** a processing unit that declares the parent effect
2. **Call `.result()`** to wait until the parent effect is applied and get the provider
3. **Use the provider** to declare child effects

### Example: Writing Rows to PostgreSQL

```python
from cocoindex.connectors import postgres

@coco.function
def app_main(scope: coco.Scope, sourcedir: pathlib.Path) -> None:
    db = scope.use(PG_DB)

    # Declare the table effect, wait for it, get back a TableTarget provider
    table = coco.mount_run(
        db.declare_table_target,
        scope / "setup" / "table",
        table_name="doc_embeddings",
        table_schema=postgres.TableSchema(DocEmbedding, primary_key=["filename", "chunk_start"]),
    ).result()

    # Use the provider to declare row effects
    for file in localfs.walk_dir(sourcedir, ...):
        coco.mount(process_file, scope / "file" / str(file.relative_path), file, table)

@coco.function(memo=True)
def process_file(scope: coco.Scope, file: FileLike, table: postgres.TableTarget) -> None:
    # ... process file into chunks ...
    for chunk in chunks:
        table.declare_row(scope, row=DocEmbedding(...))
```

### Example: Writing Files to a Directory

```python
from cocoindex.connectors import localfs

@coco.function
def app_main(scope: coco.Scope, sourcedir: pathlib.Path, outdir: pathlib.Path) -> None:
    # Declare the directory effect, wait for it, get back a DirTarget provider
    target = coco.mount_run(
        localfs.declare_dir_target, scope / "setup", outdir
    ).result()

    for file in localfs.walk_dir(sourcedir, ...):
        coco.mount(process_file, scope / "file" / str(file.relative_path), file, target)

@coco.function(memo=True)
def process_file(scope: coco.Scope, file: FileLike, target: localfs.DirTarget) -> None:
    html = render_markdown(file.read_text())
    target.declare_file(scope, filename=file.name + ".html", content=html)
```

See [Processing Unit](./processing_unit.md) for more on `mount_run()`.

:::tip Type Safety for Effect Providers
Effect providers have two states: **pending** (just created) and **resolved** (after the parent effect is applied). The type system tracks this — if you try to use a pending provider in the same processing unit that declares the parent effect, type checkers like mypy will flag the error.
:::

## Effect Hierarchies

The pattern above reflects that effects often form **hierarchies** — a parent effect creates the container, and child effects populate it:

| Parent Effect | Child Effects |
|---------------|---------------|
| A directory on disk | Files in that directory |
| A relational database table (schema, columns) | Rows in that table |
| A graph database table | Nodes and relationships in that graph |

CocoIndex ensures the parent exists before children are added, and properly cleans up children when the parent changes.

## Generic Effect APIs

CocoIndex also provides generic effect APIs for cases where connector-specific APIs don't cover your needs:

- `declare_effect()` — declare a leaf effect
- `declare_effect_with_child()` — declare an effect that provides child effects

These are exported from `cocoindex` and used internally by connectors like `postgres` and `localfs`. For defining custom effect providers, see [Effect Provider](../advanced_topics/effect_provider.md).
