---
title: Effect
description: Understanding effects as units of desired external state, and effect providers for declaring dependent effects.
---

# Effect

An **Effect** is a unit of desired external state produced by your transformations. On each run, CocoIndex compares the newly declared effects with those from the previous run and applies the necessary changes — creating, updating, or deleting — so that external systems match your intent.

TODO: Add more content about effects.

## Effect Providers

An **effect provider** is an object that allows you to declare effects that depend on a "parent" effect. This pattern is common when you have hierarchical relationships between effects — for example, a database table (parent) and its rows (children).

Effect providers:

- Are returned by functions that declare the parent effect
- Provide methods to declare child effects (e.g., `declare_row()`, `declare_file()`)
- Ensure the parent effect exists before child effects can be declared

### Common Pattern: Dependent Effects

A common pattern arises when one effect depends on another. For example, you might need to declare a database table before you can insert rows into it.

The pattern is:

1. Mount a child component that declares the parent effect and returns an effect provider
2. Call `result()` to wait until the parent effect is applied and get the provider
3. Pass the provider to other components, which use it to declare child effects

Here's an example from a text embedding pipeline:

```python
@coco.function
def app_main(scope: coco.Scope, sourcedir: pathlib.Path) -> None:
    target_db = scope.use(PG_DB)

    # Mount a component that declares the table effect and returns an effect provider
    # Call .result() to wait until the table exists and get the provider
    target_table = coco.mount_run(
        target_db.declare_table_target,
        scope / "setup" / "table",
        table_name="doc_embeddings",
        table_schema=postgres.TableSchema(DocEmbedding, primary_key=["filename", "chunk_start"]),
    ).result()

    # Pass the effect provider to child components to declare row effects
    files = localfs.walk_dir(sourcedir, ...)
    for f in files:
        coco.mount(process_file, scope / "file" / str(f.relative_path), f, target_table)
```

In this pattern:

- The `declare_table_target` component declares the table effect and returns a `TableTarget` effect provider
- Calling `.result()` ensures the table is created before proceeding
- The `target_table` effect provider is passed to child components, which call methods like `declare_row()` to declare row effects
- Each file gets its own component, so changes to individual files result in atomic updates to their rows

See [Component](./component.md) for more on mounting and `mount_run()`.
