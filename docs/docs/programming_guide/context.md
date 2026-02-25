---
title: Context
description: Sharing resources across your pipeline and beyond using ContextKey, builder.provide(), use_context(), and get_context().
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

## Providing values

In your [lifespan function](./app.md#defining-a-lifespan), use `builder.provide()` to make resources available:

```python
import cocoindex as coco
from cocoindex.connectors import postgres

PG_DB = coco.ContextKey[postgres.PgDatabase]("pg_db")

@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    builder.settings.db_path = pathlib.Path("./cocoindex.db")

    # Create and provide a database connection
    async with await postgres.create_pool(DATABASE_URL) as pool:
        builder.provide(PG_DB, postgres.register_db("my_db", pool))
        yield
```

The resource is available for the lifetime of the environment. When the lifespan exits (after `yield`), cleanup happens automatically if you use a context manager pattern.

## Retrieving values

In processing components, use `coco.use_context()` to retrieve provided resources:

```python
@coco.function
async def app_main(sourcedir: pathlib.Path) -> None:
    db = coco.use_context(PG_DB)  # Returns postgres.PgDatabase

    table = await db.mount_table_target(
        table_name="docs",
        table_schema=await postgres.TableSchema.from_class(
            Doc, primary_key=["id"]
        ),
    )

    # ... rest of pipeline ...
```

## Accessing context outside processing components

If you need to access context values outside of CocoIndex processing components — for example, in query/serving logic that shares resources with your indexing pipeline — use `env.get_context()`:

```python
# Sync API
db = coco.default_env().get_context(PG_DB)
```

```python
# Async API
db = (await coco.default_env()).get_context(PG_DB)
```

This is useful when your application runs both indexing and serving in the same process and you want to initialize shared resources (like database connection pools or configuration) once in the lifespan.

:::note
`default_env()` starts the environment if it hasn't been started yet, which runs the lifespan function. If you're using an explicit environment, call `get_context()` directly on that environment instance.
:::
