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

## Connection Setup

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

## As Source

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

### Row Mapping

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

### Iterating Rows

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

@coco.function
async def app_main(scope: coco.Scope, pool: asyncpg.Pool) -> None:
    source = postgres.PgTableSource(
        pool,
        table_name="products",
        row_type=SourceProduct,
    )

    async for product in source.fetch_rows():
        coco.mount(process_product, scope / "product" / product.product_id, product)
```

## As Target

The `postgres` connector provides target state APIs for writing rows to tables. With it, CocoIndex tracks what rows should exist and automatically handles upserts and deletions.

### Declaring Target States

#### Database Registration

Before declaring target states, register the connection pool with a stable key that identifies the logical database. This key allows CocoIndex to recognize the same database even when connection details change (e.g., username, password, or host address).

```python
def register_db(key: str, pool: asyncpg.Pool) -> PgDatabase
```

**Parameters:**

- `key` — A stable identifier for this database (e.g., `"main_db"`). Must be unique.
- `pool` — An asyncpg connection pool.

**Returns:** A `PgDatabase` handle for declaring target states.

The `PgDatabase` can be used as a context manager to automatically unregister on exit:

```python
async with await postgres.create_pool(DATABASE_URL) as pool:
    with postgres.register_db("my_db", pool) as db:
        # Use db to declare target states
        ...
    # db is automatically unregistered here
```

#### Tables (Parent State)

Declares a table as a target state. Returns a `TableTarget` for declaring rows.

```python
def PgDatabase.declare_table_target(
    self,
    scope: coco.Scope,
    table_name: str,
    table_schema: TableSchema[RowT],
    *,
    pg_schema_name: str | None = None,
    managed_by: Literal["system", "user"] = "system",
) -> TableTarget[RowT, coco.PendingS]
```

**Parameters:**

- `table_name` — Name of the table.
- `table_schema` — Schema definition including columns and primary key (see [Table Schema](#table-schema-from-python-class)).
- `pg_schema_name` — Optional PostgreSQL schema name (defaults to `"public"`).
- `managed_by` — Whether CocoIndex manages the table lifecycle (`"system"`) or assumes it exists (`"user"`).

**Returns:** A pending `TableTarget`. Use `mount_run(...).result()` to wait for resolution.

#### Rows (Child States)

Once a `TableTarget` is resolved, declare rows to be upserted:

```python
def TableTarget.declare_row(
    self,
    scope: coco.Scope,
    *,
    row: RowT,
) -> None
```

**Parameters:**

- `row` — A row object (dict, dataclass, NamedTuple, or Pydantic model). Must include all primary key columns.

### Table Schema: From Python Class

Define the table structure using a Python class (dataclass, NamedTuple, or Pydantic model):

```python
def TableSchema.__init__(
    self,
    columns: type[RowT],
    primary_key: list[str],
    *,
    column_overrides: dict[str, PgType | VectorSchemaProvider] | None = None,
) -> None
```

**Parameters:**

- `columns` — A record type whose fields define table columns.
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

schema = postgres.TableSchema(
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

For `NDArray` fields, a `VectorSchemaProvider` specifies the vector dimension and dtype. Vector dimensions are typically determined by the embedding model—hardcoding them is error-prone and creates maintenance burden when switching models. By using a `VectorSchemaProvider`, the dimension is derived automatically from the source configuration.

The connector has built-in pgvector support and automatically creates the extension when needed.

A `VectorSchemaProvider` can be:

- **An embedding model** (e.g., [`SentenceTransformerEmbedder`](../utilities/sentence_transformers.md)) — dimension is inferred from the model
- **A `VectorSchema`** — for explicit size and dtype when not using an embedder

```python
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder

embedder = SentenceTransformerEmbedder("sentence-transformers/all-MiniLM-L6-v2")

@dataclass
class Document:
    id: str
    content: str
    embedding: Annotated[NDArray, embedder]  # dimension inferred from model (384)
```

```python
from cocoindex.resources.schema import VectorSchema

@dataclass
class Document:
    id: str
    content: str
    embedding: Annotated[NDArray, VectorSchema(dtype=np.float32, size=384)]
```

### Table Schema: Explicit Column Definitions

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
import cocoindex as coco
import cocoindex.asyncio as coco_aio
from cocoindex.connectors import postgres

DATABASE_URL = "postgresql://localhost/mydb"

PG_DB = coco.ContextKey[postgres.PgDatabase]("pg_db")

@dataclass
class OutputProduct:
    category: str
    name: str
    description: str
    embedding: Annotated[NDArray, embedder]

@coco_aio.lifespan
async def coco_lifespan(builder: coco_aio.EnvironmentBuilder) -> AsyncIterator[None]:
    async with await postgres.create_pool(DATABASE_URL) as pool:
        builder.provide(PG_DB, postgres.register_db("main_db", pool))
        yield

@coco.function
async def app_main(scope: coco.Scope) -> None:
    db = scope.use(PG_DB)

    # Declare table target state
    table = await coco_aio.mount_run(
        db.declare_table_target,
        scope / "setup" / "table",
        table_name="products",
        table_schema=postgres.TableSchema(
            OutputProduct,
            primary_key=["category", "name"],
        ),
    ).result()

    # Declare rows
    for product in products:
        table.declare_row(
            scope / "row" / product.category / product.name,
            row=product,
        )
```
