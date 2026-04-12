---
title: Context
description: Sharing resources across your pipeline and beyond using ContextKey, builder.provide(), use_context(), and get_context().
---

# Context

CocoIndex provides a **context** mechanism for sharing resources across your pipeline. This is useful for database connections, API clients, configuration objects, or any resource that multiple processing components need to access.

## ContextKey

A `ContextKey[T]` is a typed key that identifies a resource. Define keys at module level:

```python
import asyncpg
import cocoindex as coco

# Define typed keys for resources you want to share
PG_DB = coco.ContextKey[asyncpg.Pool]("pg_db", detect_change=False)
CONFIG = coco.ContextKey[AppConfig]("config")
```

The type parameter (`asyncpg.Pool`, `AppConfig`) enables type checking — when you retrieve the value, your editor knows its type.

### Change detection

By default, context keys have **change detection enabled** — if you change the provided value between runs, CocoIndex automatically invalidates memoized functions that consumed it via `use_context()`. This works the same way as [function change tracking](./function.md#change-tracking): the engine detects the change and re-executes affected functions.

```python
# Change detection enabled (default) — changing the model invalidates memos that used it
EMBEDDER = coco.ContextKey[SentenceTransformerEmbedder]("embedder")

# Change detection disabled — changing the logger won't invalidate memos
LOGGER = coco.ContextKey[logging.Logger]("logger", detect_change=False)
```

Use `detect_change=False` for resources that don't affect computation results — loggers, debug flags, monitoring clients, etc. This avoids unnecessary reprocessing when those values change.

:::tip
Change detection is transitive: if function `foo` (memoized) calls function `bar`, and `bar` calls `use_context(key)` on a change-detected key, then `foo`'s memo is also invalidated when the context value changes.
:::

## ContextKey as stable identity

Beyond sharing resources, a `ContextKey` also serves as the **stable identity** of the resource it points to. When you anchor sources or targets to a `ContextKey`, CocoIndex treats *the key itself* — not the underlying value — as the identifier across runs.

This has two consequences:

1. **The underlying value can change without losing tracked state.** Rotating credentials, moving a database, or relocating a directory won't invalidate memoization or managed state, as long as the same `ContextKey` is used.

2. **Renaming a `ContextKey` is a breaking change.** Two different keys are two different resources, even if they point to the same physical backend. Existing tracked state will be treated as orphaned. When migrating code, reuse the previous key name to preserve continuity.

:::tip
Pick a `ContextKey` name that reflects the *logical* role of the resource (e.g., `"text_embedding_db"`, `"docs_root"`), not its current address. The name is what CocoIndex persists.
:::

## Providing values

In your [lifespan function](./app.md#defining-a-lifespan), use `builder.provide()` to make resources available:

```python
import asyncpg
import cocoindex as coco
from cocoindex.connectors import postgres

PG_DB = coco.ContextKey[asyncpg.Pool]("my_db", detect_change=False)

@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    builder.settings.db_path = pathlib.Path("./cocoindex.db")

    # Create and provide a database connection pool
    async with await postgres.create_pool(DATABASE_URL) as pool:
        builder.provide(PG_DB, pool)
        yield
```

The resource is available for the lifetime of the environment. When the lifespan exits (after `yield`), cleanup happens automatically if you use a context manager pattern.

## Retrieving values

In processing components, use `coco.use_context()` to retrieve provided resources:

```python
@coco.fn
async def app_main(sourcedir: pathlib.Path) -> None:
    table = await postgres.mount_table_target(
        PG_DB,
        "docs",
        await postgres.TableSchema.from_class(Doc, primary_key=["id"]),
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
