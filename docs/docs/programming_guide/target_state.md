---
title: Target State
description: Understanding target states as what you want to exist in external systems and how to declare them.
---


A **target state** represents what you want to exist in an external system. You *declare* target states in your code; CocoIndex keeps them in sync with your intent — creating, updating, or removing them as needed.

:::note Terminology
A **target state** is the external thing you want to exist (a file, a row, a table, etc.). A **target** is the object/API you use to declare those target states (like `DirTarget` or `TableTarget`).

CocoIndex treats your declarations as the source of truth: if you stop declaring a target state, CocoIndex will remove it from the external system.
:::

Examples of target states:

- A file in a directory
- A row in a database table
- An embedding vector in a vector store

When your source data changes, CocoIndex compares the newly declared target states with those from the previous run and applies only the necessary changes.

## Declaring target states

CocoIndex connectors provide **targets** with `declare_*` methods:

```python
# Declare a file target state
dir_target.declare_file(filename="output.html", content=html)

# Declare a row target state
table_target.declare_row(row=DocEmbedding(...))
```

### Where do targets come from?

Target states can be nested — a directory contains files, a table contains rows. The container itself is a target state you declare, and once it's ready, you get a target to declare child target states within it.

Container target states (like a directory or table) are typically top-level — you can declare them directly. Child target states (like files or rows) require the container to be ready first.

Connectors provide convenience methods that mount the container and return a ready-to-use target in one step:

### Example: writing a file to a directory

For simple cases where each processing component writes a single file, you can declare the file directly:

```python
from cocoindex.connectors import localfs

# Declare a single file target state directly
localfs.declare_file(outdir / "output.html", html, create_parent_dirs=True)
```

When you need a `DirTarget` to declare multiple files, use the connector's convenience method:

```python
# Mount a directory target, get a ready-to-use DirTarget
dir_target = await localfs.mount_dir_target(outdir)

# Declare child target states (files)
dir_target.declare_file(filename="output.html", content=html)
```

### Example: writing a row to PostgreSQL

```python
from cocoindex.connectors import postgres

# Mount a table target, get a ready-to-use TableTarget
table = await target_db.mount_table_target(
    table_name="doc_embeddings",
    table_schema=await postgres.TableSchema.from_class(
        DocEmbedding, primary_key=["id"]
    ),
)

# Declare a child target state (a row)
table.declare_row(row=DocEmbedding(...))
```

These convenience methods wrap [`mount_target()`](./processing_component.md#mount-target), which automatically derives the component path from the target's globally unique key. See [Processing Component](./processing_component.md) for more on mounting APIs.

:::tip Type safety
Targets like `DirTarget` and `TableTarget` have two statuses: **pending** (just created) and **resolved** (after the container target state is ready). The type system tracks this — if you try to use a pending target before it's resolved, type checkers like mypy will flag the error.
:::

## How CocoIndex syncs target states

Under the hood, CocoIndex compares your declared target states with the previous run and applies the minimal changes needed:

<table>
  <thead>
    <tr>
      <th rowspan="2">Target State</th>
      <th colspan="3" style={{textAlign: 'center'}}>CocoIndex's Action</th>
    </tr>
    <tr>
      <th>On first declaration</th>
      <th>When declared differently</th>
      <th>When no longer declared</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>A database table</td>
      <td>Create the table</td>
      <td>Alter the table</td>
      <td>Drop the table</td>
    </tr>
    <tr>
      <td>A row in a database table</td>
      <td>Insert the row</td>
      <td>Update the row</td>
      <td>Delete the row</td>
    </tr>
    <tr>
      <td>A file in a directory</td>
      <td>Create the file</td>
      <td>Update the file</td>
      <td>Delete the file</td>
    </tr>
  </tbody>
</table>

CocoIndex ensures containers exist before their contents are added, and properly cleans up contents when the container changes.

## Generic target state APIs

For cases where connector-specific APIs don't cover your needs, CocoIndex provides generic APIs:

- `declare_target_state()` — declare a leaf target state
- `declare_target_state_with_child()` — declare a target state that provides child target states

These are exported from `cocoindex` and used internally by connectors. For defining custom targets, see [Custom Target States Connector](../advanced_topics/custom_target_connector.md).
