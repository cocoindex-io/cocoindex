---
title: Internal Storage
description: How CocoIndex stores internal state and how to configure the LMDB database.
---

# Internal Storage

CocoIndex uses an [LMDB](http://www.lmdb.tech/doc/) database to persist its internal state. This database tracks target states and memoization results from previous runs, enabling CocoIndex to detect what changed and apply only the necessary updates.

## Database path

CocoIndex needs a database path (`db_path`) to know where to store this internal state. The simplest way to set it is via the `COCOINDEX_DB` environment variable:

```bash
export COCOINDEX_DB=./cocoindex.db
```

You can also set it programmatically in a [lifespan function](../programming_guide/app.md#lifespan-optional):

```python
@coco.lifespan
def coco_lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
    builder.settings.db_path = pathlib.Path("./cocoindex.db")
    yield
```

Or pass it directly when creating a [`Settings`](./multiple_environments.md) object:

```python
settings = coco.Settings(db_path=pathlib.Path("./cocoindex.db"))
```

Setting `db_path` in the lifespan or `Settings` takes precedence over the `COCOINDEX_DB` environment variable. If neither is provided, CocoIndex will raise an error.

## LMDB tuning

The LMDB database has two tunable settings. The defaults work well for most use cases — you only need to adjust them for large-scale deployments.

| Setting | Default | Env Variable | Description |
|---------|---------|-------------|-------------|
| `lmdb_max_dbs` | `1024` | `COCOINDEX_LMDB_MAX_DBS` | Maximum number of named LMDB databases. Must be &ge; 1. |
| `lmdb_map_size` | `4294967296` (4 GiB) | `COCOINDEX_LMDB_MAP_SIZE` | Maximum size of the LMDB memory map in bytes. Must be &gt; 0. |

### When to adjust

- **Increase `lmdb_map_size`** if you encounter LMDB "map full" errors. This happens when the accumulated internal state (target states + memoization cache) exceeds 4 GiB. On 64-bit systems, `map_size` is a virtual address space reservation — setting it larger than needed is safe and does not consume physical memory.
- **Increase `lmdb_max_dbs`** if you have an unusually large number of apps sharing a single database directory.

### Configuration

Via environment variables:

```bash
export COCOINDEX_LMDB_MAP_SIZE=8589934592   # 8 GiB
export COCOINDEX_LMDB_MAX_DBS=2048
```

Or programmatically in a lifespan function:

```python
@coco.lifespan
def coco_lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
    builder.settings.db_path = pathlib.Path("./cocoindex.db")
    builder.settings.lmdb_map_size = 8 * 1024 * 1024 * 1024  # 8 GiB
    builder.settings.lmdb_max_dbs = 2048
    yield
```

Or when creating a `Settings` object directly:

```python
settings = coco.Settings(
    db_path=pathlib.Path("./cocoindex.db"),
    lmdb_map_size=8 * 1024 * 1024 * 1024,  # 8 GiB
    lmdb_max_dbs=2048,
)
```

When using `Settings.from_env()`, the LMDB settings are automatically loaded from their environment variables if set; otherwise, the defaults apply.
