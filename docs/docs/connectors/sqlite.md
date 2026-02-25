---
title: SQLite
toc_max_heading_level: 4
description: CocoIndex connector for writing to SQLite databases with optional vector support via sqlite-vec.
---

# SQLite

The `sqlite` connector provides utilities for writing rows to SQLite databases, with optional vector support via the sqlite-vec extension.

```python
from cocoindex.connectors import sqlite
```

:::note Vector Support
For vector operations, install the sqlite-vec extension:

```bash
pip install cocoindex[sqlite]
```

Note: The default SQLite library bundled with macOS does not support extensions. Use Homebrew Python (`brew install python`) or build SQLite with extension support.
:::

## Connection Setup

### connect

`connect()` creates a managed SQLite connection with sensible defaults, including automatic sqlite-vec loading and thread-safe access.

```python
def connect(
    database: str | Path,
    *,
    timeout: float = 5.0,
    load_vec: bool | Literal["auto"] = "auto",
    **kwargs: Any,
) -> ManagedConnection
```

**Parameters:**

- `database` — Path to the SQLite database file, or `":memory:"` for an in-memory database.
- `timeout` — How long to wait for locks before raising an error.
- `load_vec` — Whether to load the sqlite-vec extension for vector support:
  - `"auto"` (default): Try to load, silently ignore if unavailable.
  - `True`: Load and raise an error if unavailable.
  - `False`: Don't attempt to load.
- `**kwargs` — Additional arguments passed directly to `sqlite3.connect()`.

**Returns:** A `ManagedConnection` with thread-safe access and extension tracking.

**Example:**

```python
managed_conn = sqlite.connect("mydb.sqlite")  # Auto-loads sqlite-vec if available
# Or for in-memory:
managed_conn = sqlite.connect(":memory:")
# Or explicitly require vector support:
managed_conn = sqlite.connect("mydb.sqlite", load_vec=True)
# Or disable auto-loading:
managed_conn = sqlite.connect("mydb.sqlite", load_vec=False)
```

### ManagedConnection

A wrapper around `sqlite3.Connection` that provides thread-safe access and tracks loaded extensions. The connection uses autocommit mode internally.

**Methods:**

- `transaction()` — Context manager that acquires a lock and executes within a transaction (`BEGIN`...`COMMIT`/`ROLLBACK`). Use for write operations that should be atomic.
- `readonly()` — Context manager that acquires a lock for read-only operations. No transaction is started since the connection uses autocommit mode.
- `close()` — Closes the underlying connection.

**Properties:**

- `loaded_extensions` — A read-only `Set[str]` of loaded extension names (e.g., `"sqlite-vec"`).

## As Target

The `sqlite` connector provides target state APIs for writing rows to tables. With it, CocoIndex tracks what rows should exist and automatically handles upserts and deletions.

### Declaring Target States

#### Database Registration

Before declaring target states, register the connection with a stable key that identifies the logical database. This key allows CocoIndex to recognize the same database even when the file path changes.

```python
def register_db(key: str, managed_conn: ManagedConnection) -> SqliteDatabase
```

**Parameters:**

- `key` — A stable identifier for this database (e.g., `"main_db"`). Must be unique.
- `managed_conn` — A `ManagedConnection` from `connect()`.

**Returns:** A `SqliteDatabase` handle for declaring target states.

The `SqliteDatabase` can be used as a context manager to automatically unregister on exit:

```python
managed_conn = sqlite.connect("mydb.sqlite")
with sqlite.register_db("my_db", managed_conn) as db:
    # Use db to declare target states
    ...
# db is automatically unregistered here
```

#### Tables (Parent State)

Declares a table as a target state. Returns a `TableTarget` for declaring rows.

```python
def SqliteDatabase.declare_table_target(
    self,
    table_name: str,
    table_schema: TableSchema[RowT],
    *,
    managed_by: Literal["system", "user"] = "system",
) -> TableTarget[RowT, coco.PendingS]
```

**Parameters:**

- `table_name` — Name of the table.
- `table_schema` — Schema definition including columns and primary key (see [Table Schema](#table-schema-from-python-class)).
- `managed_by` — Whether CocoIndex manages the table lifecycle (`"system"`) or assumes it exists (`"user"`).

**Returns:** A pending `TableTarget`. Use the convenience wrapper `await db.mount_table_target(table_name=..., table_schema=...)` to resolve.

#### Rows (Child States)

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

### Table Schema: From Python Class

Define the table structure using a Python class (dataclass, NamedTuple, or Pydantic model):

```python
@classmethod
async def TableSchema.from_class(
    cls,
    record_type: type[RowT],
    primary_key: list[str],
    *,
    column_overrides: dict[str, SqliteType | VectorSchemaProvider] | None = None,
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

schema = await sqlite.TableSchema.from_class(
    OutputProduct,
    primary_key=["category", "name"],
)
```

Python types are automatically mapped to SQLite type affinities:

| Python Type | SQLite Type |
|-------------|-------------|
| `bool` | `INTEGER` (0/1) |
| `int` | `INTEGER` |
| `float` | `REAL` |
| `decimal.Decimal` | `TEXT` |
| `str` | `TEXT` |
| `bytes` | `BLOB` |
| `uuid.UUID` | `TEXT` |
| `datetime.date` | `TEXT` (ISO format) |
| `datetime.time` | `TEXT` (ISO format) |
| `datetime.datetime` | `TEXT` (ISO format) |
| `datetime.timedelta` | `REAL` (total seconds) |
| `list`, `dict`, nested structs | `TEXT` (JSON) |
| `NDArray` (with vector schema) | `float[N]` (sqlite-vec type, e.g., `float[384]`) |

To override the default mapping, provide a `SqliteType` or `VectorSchemaProvider` via:

- **Type annotation** — using `typing.Annotated` on the field
- **`column_overrides`** — passing overrides when constructing `TableSchema`

#### SqliteType

Use `SqliteType` to specify a custom SQLite type and optional encoder:

```python
from typing import Annotated
from cocoindex.connectors.sqlite import SqliteType

@dataclass
class MyRow:
    id: int
    value: Annotated[float, SqliteType("REAL")]
    data: Annotated[dict, SqliteType("TEXT", encoder=lambda v: json.dumps(v))]
```

Or via `column_overrides`:

```python
schema = sqlite.TableSchema(
    MyRow,
    primary_key=["id"],
    column_overrides={
        "data": sqlite.SqliteType("TEXT", encoder=lambda v: json.dumps(v)),
    },
)
```

#### VectorSchemaProvider

For `NDArray` fields, a `VectorSchemaProvider` specifies the vector dimension and dtype. Vectors are stored as BLOBs in sqlite-vec compatible float32 format.

A `VectorSchemaProvider` can be:

- **An embedding model** (e.g., [`SentenceTransformerEmbedder`](../ops/sentence_transformers.md)) — dimension is inferred from the model
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
schema = sqlite.TableSchema(
    {
        "category": sqlite.ColumnDef(type="TEXT", nullable=False),
        "name": sqlite.ColumnDef(type="TEXT", nullable=False),
        "price": sqlite.ColumnDef(type="REAL"),
        "embedding": sqlite.ColumnDef(type="float[384]"),  # sqlite-vec vector type
    },
    primary_key=["category", "name"],
)
```

### Virtual Tables

SQLite virtual tables allow custom storage backends and specialized functionality. The `sqlite` connector supports creating virtual tables through the same `declare_table_target()` API used for regular tables.

#### Vec0 Virtual Tables

The `vec0` module from sqlite-vec provides optimized vector storage for similarity search. Use vec0 virtual tables when:

- You need efficient vector similarity search with built-in indexing
- You want to partition vectors by categories for faster queries
- You're working with large vector datasets

**Requirements:**

- Exactly one `INTEGER` primary key column
- At least one `float[N]` vector column
- The sqlite-vec extension must be loaded (`load_vec=True`)

#### Vec0TableDef

Configure vec0-specific features using `Vec0TableDef`:

```python
from cocoindex.connectors.sqlite import Vec0TableDef

virtual_table_def = Vec0TableDef(
    partition_key_columns=["category"],  # Optional: partition index by these columns
    auxiliary_columns=["metadata"],      # Optional: columns excluded from KNN filters
)
```

**Parameters:**

- `partition_key_columns` — List of column names used to partition the vector index. Queries can filter by partition keys efficiently. Multiple partition keys create a composite partition.
- `auxiliary_columns` — List of column names to mark as auxiliary (stored but not usable in KNN filters). Useful for metadata that doesn't need to participate in similarity search.

#### Creating Vec0 Virtual Tables

Pass `virtual_table_def` to `declare_table_target()`:

```python
@dataclass
class VectorDocument:
    id: int
    category: str
    content: str
    embedding: Annotated[NDArray, embedder]  # e.g., float[384]
    metadata: str

# Create vec0 virtual table with partition key and auxiliary column
table = await db.mount_table_target(
    table_name="documents",
    table_schema=await sqlite.TableSchema.from_class(
        VectorDocument,
        primary_key=["id"],
    ),
    virtual_table_def=sqlite.Vec0TableDef(
        partition_key_columns=["category"],
        auxiliary_columns=["metadata"],
    ),
)
```

:::warning Current Limitations
Vec0 virtual tables have the following limitations:

- **Schema changes are not supported incrementally**. When you modify the table schema (add/remove columns, change `virtual_table_def` settings), the table will be recreated and **existing data will be lost**.
- **Switching between regular and virtual tables** will also recreate the table and clear existing data.

A future schema versioning mechanism will allow preserving row data across table recreations.
:::

### Example

```python
import cocoindex as coco
from cocoindex.connectors import sqlite

DATABASE_PATH = "mydb.sqlite"

SQLITE_DB = coco.ContextKey[sqlite.SqliteDatabase]("sqlite_db")

@dataclass
class OutputProduct:
    category: str
    name: str
    description: str
    embedding: Annotated[NDArray, embedder]

@coco.lifespan
def coco_lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
    managed_conn = sqlite.connect(DATABASE_PATH, load_vec=True)  # Enable vector support
    with sqlite.register_db("main_db", managed_conn) as db:
        builder.provide(SQLITE_DB, db)
        yield
    managed_conn.close()

@coco.function
async def app_main() -> None:
    db = coco.use_context(SQLITE_DB)

    # Declare table target state
    table = await db.mount_table_target(
        table_name="products",
        table_schema=await sqlite.TableSchema.from_class(
            OutputProduct,
            primary_key=["category", "name"],
        ),
    )

    # Declare rows
    for product in products:
        table.declare_row(row=product)
```

### Example: Vec0 Virtual Table

```python
import cocoindex as coco
from cocoindex.connectors import sqlite
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder
from dataclasses import dataclass
from typing import Annotated
from numpy.typing import NDArray

DATABASE_PATH = "vectors.sqlite"
SQLITE_DB = coco.ContextKey[sqlite.SqliteDatabase]("sqlite_db")

embedder = SentenceTransformerEmbedder("sentence-transformers/all-MiniLM-L6-v2")

@dataclass
class VectorDocument:
    id: int
    category: str
    title: str
    content: str
    embedding: Annotated[NDArray, embedder]  # float[384]
    metadata: str  # Will be marked as auxiliary

@coco.lifespan
def coco_lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
    managed_conn = sqlite.connect(DATABASE_PATH, load_vec=True)
    with sqlite.register_db("vec_db", managed_conn) as db:
        builder.provide(SQLITE_DB, db)
        yield
    managed_conn.close()

@coco.function
async def app_main() -> None:
    db = coco.use_context(SQLITE_DB)

    # Create vec0 virtual table with partition key and auxiliary column
    table = await db.mount_table_target(
        table_name="documents",
        table_schema=await sqlite.TableSchema.from_class(
            VectorDocument,
            primary_key=["id"],
        ),
        virtual_table_def=sqlite.Vec0TableDef(
            partition_key_columns=["category"],  # Partition index by category
            auxiliary_columns=["metadata"],       # Store but don't index for KNN
        ),
    )

    # Declare document rows
    docs = [
        VectorDocument(
            id=1,
            category="tech",
            title="Introduction to AI",
            content="Artificial intelligence is...",
            embedding=await embedder.embed("Artificial intelligence is..."),
            metadata='{"source": "blog", "date": "2025-01-15"}',
        ),
        # ... more documents
    ]

    for doc in docs:
        table.declare_row(row=doc)
```
