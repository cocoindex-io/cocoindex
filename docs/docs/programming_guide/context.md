---
title: Context
description: Sharing resources across your pipeline using ContextKey, builder.provide(), and use_context().
---

# Context

CocoIndex provides a **context** mechanism for sharing resources across your pipeline. This is useful for database connections, API clients, configuration objects, or any resource that multiple processing components need to access.

## ContextKey

A `ContextKey[T]` is a typed key that identifies a resource. Define keys at module level:

```python
import cocoindex as coco
from cocoindex.connectors import postgres

# Define typed keys for resources you want to share
PG_DB = coco.ContextKey[postgres.PgDatabase]("pg_db")
CONFIG = coco.ContextKey[AppConfig]("config")
```

The type parameter (`postgres.PgDatabase`, `AppConfig`) enables type checking — when you retrieve the value, your editor knows its type.

## Providing Values

In your [lifespan function](./app.md#defining-a-lifespan), use `builder.provide()` to make resources available:

```python
import cocoindex.asyncio as coco_aio
from cocoindex.connectors import postgres

PG_DB = coco_aio.ContextKey[postgres.PgDatabase]("pg_db")

@coco_aio.lifespan
async def coco_lifespan(builder: coco_aio.EnvironmentBuilder) -> AsyncIterator[None]:
    builder.settings.db_path = pathlib.Path("./cocoindex.db")

    # Create and provide a database connection
    async with await postgres.create_pool(DATABASE_URL) as pool:
        builder.provide(PG_DB, postgres.register_db("my_db", pool))
        yield
```

The resource is available for the lifetime of the environment. When the lifespan exits (after `yield`), cleanup happens automatically if you use a context manager pattern.

## Retrieving Values

In processing components, use `coco.use_context()` to retrieve provided resources:

```python
@coco_aio.function
def app_main(sourcedir: pathlib.Path) -> None:
    db = coco.use_context(PG_DB)  # Returns postgres.PgDatabase

    table = coco_aio.mount_run(
        coco.component_subpath("setup", "table"),
        db.declare_table_target,
        table_name="docs",
        table_schema=postgres.TableSchema(Doc, primary_key=["id"]),
    ).result()

    # ... rest of pipeline ...
```
