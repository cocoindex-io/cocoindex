---
title: Postgres
toc_max_heading_level: 4
description: CocoIndex connector for reading from and writing to PostgreSQL databases.
---

# Postgres

The `postgres` connector provides utilities for reading rows from and writing rows to PostgreSQL databases, with built-in support for pgvector.

```python
from cocoindex.connectors import postgres
```

:::note Dependencies
This connector requires additional dependencies. Install with:

```bash
pip install cocoindex[postgres]
```

:::

## Connection setup

`create_pool()` is a thin wrapper around [`asyncpg.create_pool()`](https://magicstack.github.io/asyncpg/current/api/index.html#asyncpg.pool.create_pool) that registers necessary extensions (e.g., pgvector) on each connection.

```python
async def create_pool(
    dsn: str | None = None,
    *,
    init: Callable[[asyncpg.Connection], Any] | None = None,
    **kwargs: Any,
) -> asyncpg.Pool
```

**Parameters:**

- `dsn` — PostgreSQL connection string (e.g., `"postgresql://user:pass@localhost/dbname"`).
- `init` — Optional callback to initialize each connection (called after extension registration).
- `**kwargs` — Additional arguments passed directly to `asyncpg.create_pool()`.

**Returns:** An asyncpg connection pool.

**Example:**

```python
async with await postgres.create_pool("postgresql://localhost/mydb") as pool:
    # Use pool for source or target operations
    ...
```

## As source

Use `PgTableSource` to read rows from a PostgreSQL table. It returns a `RowFetcher` that supports both synchronous and asynchronous iteration.

### PgTableSource

```python
class PgTableSource(Generic[RowT]):
    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        table_name: str,
        columns: Sequence[str] | None = None,
        pg_schema_name: str | None = None,
        row_type: type[RowT] | None = None,
        row_factory: Callable[[dict[str, Any]], RowT] | None = None,
    ) -> None

    def fetch_rows(self) -> RowFetcher[RowT]
```

**Parameters:**

- `pool` — An asyncpg connection pool.
- `table_name` — Name of the table to read from.
- `columns` — List of column names to select. If omitted with `row_type`, uses the record's field names. If omitted without `row_type`, uses `SELECT *`.
- `pg_schema_name` — Optional PostgreSQL schema name (defaults to `"public"`).
- `row_type` — Optional record type (dataclass, NamedTuple, or Pydantic model) for automatic row conversion. When provided, `columns` (if specified) must be a subset of the record's fields.
- `row_factory` — Optional callable to transform each row dict. Mutually exclusive with `row_type`.

### Row mapping

By default, rows are returned as `dict[str, Any]`, with PostgreSQL types converted to Python types using [asyncpg's type conversion](https://magicstack.github.io/asyncpg/current/usage.html#type-conversion). You can configure automatic conversion to custom types using `row_type` or `row_factory`.

#### Using `row_type`

Pass a record type (dataclass, NamedTuple, or Pydantic model) to automatically convert rows. When `columns` is omitted, the record's field names are used:

```python
from dataclasses import dataclass

@dataclass
class Product:
    id: int
    name: str
    price: float

source = postgres.PgTableSource(
    pool,
    table_name="products",
    row_type=Product,  # columns inferred as ["id", "name", "price"]
)
```

#### Using `row_factory`

For custom transformations, pass a callable:

```python
source = postgres.PgTableSource(
    pool,
    table_name="products",
    columns=["id", "name", "price"],
    row_factory=lambda row: (row["name"], row["price"] * 1.1),  # Add 10% markup
)
```

### Iterating rows

`fetch_rows()` returns a `RowFetcher` that supports both sync and async iteration:

```python
# Synchronous iteration
for row in source.fetch_rows():
    print(row.name, row.price)

# Asynchronous iteration (streams rows using a cursor)
async for row in source.fetch_rows():
    print(row.name, row.price)
```

### Example

```python
import cocoindex as coco
from cocoindex.connectors import postgres

@dataclass
class SourceProduct:
    product_id: str
    name: str
    description: str

@coco.fn
async def app_main(pool: asyncpg.Pool) -> None:
    source = postgres.PgTableSource(
        pool,
        table_name="products",
        row_type=SourceProduct,
    )

    async for product in source.fetch_rows():
        coco.mount(
            coco.component_subpath("product", product.product_id),
            process_product,
            product,
        )
```

## As target

The `postgres` connector provides target state APIs for writing rows to tables. With it, CocoIndex tracks what rows should exist and automatically handles upserts and deletions.

### Declaring target states

#### Setting up a connection

Create a `ContextKey[asyncpg.Pool]` to identify your connection pool, then provide the pool directly in your lifespan:

:::note
The key name is load-bearing across runs — it's the stable identity CocoIndex uses to track managed rows. See [ContextKey as stable identity](../programming_guide/context.md#contextkey-as-stable-identity) before renaming.
:::

```python
import asyncpg
import cocoindex as coco

PG_DB = coco.ContextKey[asyncpg.Pool]("my_db")

@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    async with await postgres.create_pool(DATABASE_URL) as pool:
        builder.provide(PG_DB, pool)
        yield
```

#### Tables (parent state)

Declares a table as a target state. Returns a `TableTarget` for declaring rows.

```python
def declare_table_target(
    db: ContextKey[asyncpg.Pool],
    table_name: str,
    table_schema: TableSchema[RowT],
    *,
    pg_schema_name: str | None = None,
    managed_by: Literal["system", "user"] = "system",
) -> TableTarget[RowT, coco.PendingS]
```

**Parameters:**

- `db` — A `ContextKey[asyncpg.Pool]` identifying the connection pool to use.
- `table_name` — Name of the table.
- `table_schema` — Schema definition including columns and primary key (see [Table Schema](#table-schema-from-python-class)).
- `pg_schema_name` — Optional PostgreSQL schema name (defaults to `"public"`).
- `managed_by` — Whether CocoIndex manages the table lifecycle (`"system"`) or assumes it exists (`"user"`).

**Returns:** A pending `TableTarget`. Use the convenience wrapper `await postgres.mount_table_target(PG_DB, table_name, table_schema)` to resolve.

#### Rows (child states)

Once a `TableTarget` is resolved, declare rows to be upserted:

```python
def TableTarget.declare_row(
    self,
    *,
    row: RowT,
) -> None
```

**Parameters:**

- `row` — A row object (dict, dataclass, NamedTuple, or Pydantic model). Must include all primary key columns.

#### Vector indexes (attachment)

Declare a pgvector index on a vector column of the table. CocoIndex tracks the index spec and automatically creates, recreates, or drops the index as needed.

```python
def TableTarget.declare_vector_index(
    self,
    *,
    name: str | None = None,
    column: str,
    metric: Literal["cosine", "l2", "ip"] = "cosine",
    method: Literal["ivfflat", "hnsw"] = "ivfflat",
    lists: int | None = None,
    m: int | None = None,
    ef_construction: int | None = None,
) -> None
```

The actual PostgreSQL index is named `{table_name}__vector__{name}`.

**Parameters:**

- `name` — Logical index name (defaults to `column`).
- `column` — Column to index (must be a vector column).
- `metric` — Distance metric: `"cosine"`, `"l2"`, or `"ip"` (inner product).
- `method` — Index method: `"ivfflat"` or `"hnsw"`.
- `lists` — Number of lists (ivfflat only).
- `m` — Maximum number of connections per layer (hnsw only).
- `ef_construction` — Size of the dynamic candidate list for construction (hnsw only).

**Example:**

```python
# Creates a PostgreSQL index named "products__vector__embedding"
table.declare_vector_index(
    column="embedding",
    metric="cosine",
    method="hnsw",
    m=16,
    ef_construction=64,
)
```

#### SQL command attachments

Declare an arbitrary SQL command that CocoIndex manages alongside the table. The setup SQL runs when the attachment is created or changed; the optional teardown SQL runs when the attachment is removed or before re-running setup on change.

```python
def TableTarget.declare_sql_command_attachment(
    self,
    *,
    name: str,
    setup_sql: str,
    teardown_sql: str | None = None,
) -> None
```

**Parameters:**

- `name` — Stable identifier for the attachment.
- `setup_sql` — SQL to execute on creation or change.
- `teardown_sql` — SQL to execute on removal or before re-running setup (optional). If omitted, no cleanup is performed when the attachment is removed.

**Example:**

```python
table.declare_sql_command_attachment(
    name="content_fts_idx",
    setup_sql='CREATE INDEX "content_fts" ON "products" USING gin (to_tsvector(\'english\', "description"))',
    teardown_sql='DROP INDEX IF EXISTS "content_fts"',
)
```

### Table schema: from Python class

Define the table structure using a Python class (dataclass, NamedTuple, or Pydantic model):

```python
@classmethod
async def TableSchema.from_class(
    cls,
    record_type: type[RowT],
    primary_key: list[str],
    *,
    column_overrides: dict[str, PgType | VectorSchemaProvider] | None = None,
) -> TableSchema[RowT]
```

**Parameters:**

- `record_type` — A record type whose fields define table columns.
- `primary_key` — List of column names forming the primary key.
- `column_overrides` — Optional per-column overrides for type mapping or vector configuration.

**Example:**

```python
@dataclass
class OutputProduct:
    category: str
    name: str
    price: float
    embedding: Annotated[NDArray, embedder]

schema = await postgres.TableSchema.from_class(
    OutputProduct,
    primary_key=["category", "name"],
)
```

Python types are automatically mapped to PostgreSQL types:

| Python Type | PostgreSQL Type |
|-------------|-----------------|
| `bool` | `boolean` |
| `int` | `bigint` |
| `float` | `double precision` |
| `decimal.Decimal` | `numeric` |
| `str` | `text` |
| `bytes` | `bytea` |
| `uuid.UUID` | `uuid` |
| `datetime.date` | `date` |
| `datetime.time` | `time with time zone` |
| `datetime.datetime` | `timestamp with time zone` |
| `datetime.timedelta` | `interval` |
| `list`, `dict`, nested structs | `jsonb` |
| `NDArray` (with vector schema) | `vector(n)` or `halfvec(n)` |

To override the default mapping, provide a `PgType` or `VectorSchemaProvider` via:

- **Type annotation** — using `typing.Annotated` on the field
- **`column_overrides`** — passing overrides when constructing `TableSchema`

#### PgType

Use `PgType` to specify a custom PostgreSQL type:

```python
from typing import Annotated
from cocoindex.connectors.postgres import PgType

@dataclass
class MyRow:
    id: Annotated[int, PgType("integer")]           # instead of bigint
    value: Annotated[float, PgType("real")]         # instead of double precision
    created_at: Annotated[datetime.datetime, PgType("timestamp")]  # without timezone
```

Or via `column_overrides`:

```python
schema = postgres.TableSchema(
    MyRow,
    primary_key=["id"],
    column_overrides={
        "created_at": postgres.PgType("timestamp"),
    },
)
```

#### VectorSchemaProvider

For `NDArray` fields, a [`VectorSchemaProvider`](../resource_types.md#vectorschemaprovider) annotation specifies the vector dimension and dtype. The connector has built-in pgvector support and automatically creates the extension when needed. See [Vector Schema](../resource_types.md#vectorschemaprovider) for the full list of annotation options (`ContextKey`, embedder instance, or explicit `VectorSchema`).

### Table schema: explicit column definitions

Define columns directly using `ColumnDef`:

```python
def TableSchema.__init__(
    self,
    columns: dict[str, ColumnDef],
    primary_key: list[str],
) -> None
```

**Example:**

```python
schema = postgres.TableSchema(
    {
        "category": postgres.ColumnDef(type="text", nullable=False),
        "name": postgres.ColumnDef(type="text", nullable=False),
        "price": postgres.ColumnDef(type="numeric"),
        "embedding": postgres.ColumnDef(type="vector(384)"),
    },
    primary_key=["category", "name"],
)
```

### Example

```python
import asyncpg
import cocoindex as coco
from cocoindex.connectors import postgres

DATABASE_URL = "postgresql://localhost/mydb"

PG_DB = coco.ContextKey[asyncpg.Pool]("main_db")

@dataclass
class OutputProduct:
    category: str
    name: str
    description: str
    embedding: Annotated[NDArray, embedder]

@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    async with await postgres.create_pool(DATABASE_URL) as pool:
        builder.provide(PG_DB, pool)
        yield

@coco.fn
async def app_main() -> None:
    # Declare table target state
    table = await postgres.mount_table_target(
        PG_DB,
        "products",
        await postgres.TableSchema.from_class(
            OutputProduct,
            primary_key=["category", "name"],
        ),
    )

    # Declare rows
    for product in products:
        table.declare_row(row=product)

    # Declare a vector index on the embedding column
    table.declare_vector_index(
        column="embedding",
        metric="cosine",
        method="hnsw",
    )
```
