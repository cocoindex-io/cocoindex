---
title: Target State
description: Understanding target states as units of desired external state, target state hierarchies, and how to declare target states.
---

# Target State

A **Target State** is a unit of desired external state produced by your transformations. On each run, CocoIndex compares the newly declared target states with those from the previous run and applies the necessary changes — creating, updating, or deleting — so that external systems match your intent.

Target states can form hierarchies (e.g., a table contains rows). CocoIndex connectors provide specific APIs to declare target states at each level.

See [Core Concepts](./core_concepts.md#target-states-desired-targets-in-external-systems) for examples of how target states map to external system operations.

## Declaring Target States with Target Connectors

CocoIndex connectors provide **target states providers** with specific `declare_*` methods for declaring target states. For example:

- `postgres.TableTarget` provides `declare_row()` to declare a row in a table
- `localfs.DirTarget` provides `declare_file()` to declare a file in a directory

```python
# Declare a row target state
table_target.declare_row(scope, row=DocEmbedding(...))

# Declare a file target state
dir_target.declare_file(scope, filename="output.html", content=html)
```

## Obtaining Target States Providers

Some target states providers are created once a parent target state is ready. For example, you can only declare rows after the table exists, or files after the directory exists.

The pattern is:

1. **Mount** a processing component that declares the parent target state
2. **Call `.result()`** to wait until the parent target state is applied and get the provider
3. **Use the provider** to declare child target states

### Example: Writing Rows to PostgreSQL

```python
from cocoindex.connectors import postgres

@coco.function
def app_main(scope: coco.Scope, sourcedir: pathlib.Path) -> None:
    db = scope.use(PG_DB)

    # Declare the table target state, wait for it, get back a TableTarget provider
    table = coco.mount_run(
        db.declare_table_target,
        scope / "setup" / "table",
        table_name="doc_embeddings",
        table_schema=postgres.TableSchema(DocEmbedding, primary_key=["filename", "chunk_start"]),
    ).result()

    # Use the provider to declare row target states
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
    # Declare the directory target state, wait for it, get back a DirTarget provider
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

See [Processing Component](./processing_component.md) for more on `mount_run()`.

:::tip Type Safety for Target States Providers
Target state providers have two statuses: **pending** (just created) and **resolved** (after the parent target state is applied). The type system tracks this — if you try to use a pending provider in the same processing component that declares the parent target state, type checkers like mypy will flag the error.
:::

## Target State Hierarchies

The pattern above reflects that target states often form **hierarchies** — a parent target state creates the container, and child target states populate it:

| Parent Target State | Child Target States |
|---------------------|---------------------|
| A directory on disk | Files in that directory |
| A relational database table (schema, columns) | Rows in that table |
| A graph database table | Nodes and relationships in that graph |

CocoIndex ensures the parent exists before children are added, and properly cleans up children when the parent changes.

## Generic Target State APIs

CocoIndex also provides generic target state APIs for cases where connector-specific APIs don't cover your needs:

- `declare_target_state()` — declare a leaf target state
- `declare_target_state_with_child()` — declare a target state that provides child target states

These are exported from `cocoindex` and used internally by connectors like `postgres` and `localfs`. For defining custom target states providers, see [Target States Provider](../advanced_topics/target_states_provider.md).
