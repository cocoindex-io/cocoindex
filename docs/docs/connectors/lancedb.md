---
title: LanceDB
toc_max_heading_level: 4
description: CocoIndex connector for writing to LanceDB tables.
---

# LanceDB

The `lancedb` connector provides target state APIs for writing rows to LanceDB tables.

```python
from cocoindex.connectors import lancedb
```

::::note Dependencies
This connector requires additional dependencies. Install with:

```bash
pip install cocoindex[lancedb]
```

::::

## Connection setup

LanceDB connections are created directly via the LanceDB library. CocoIndex exposes thin wrappers:

```python
async def connect_async(uri: str, **options: Any) -> LanceAsyncConnection
def connect(uri: str, **options: Any) -> lancedb.DBConnection
```

**Parameters:**

- `uri` — LanceDB URI (local path like `"./lancedb_data"` or cloud URI like `"s3://bucket/path"`).
- `**options` — Additional options passed directly to `lancedb.connect_async()` / `lancedb.connect()`.

**Returns:** A LanceDB connection.

**Example:**

```python
conn = await lancedb.connect_async("./lancedb_data")
```

## As target

The `lancedb` connector provides target state APIs for writing rows to tables. With it, CocoIndex tracks what rows should exist and automatically handles upserts and deletions.

### Declaring target states

#### Database registration

Before declaring target states, register the connection with a stable key that identifies the logical database. This key allows CocoIndex to recognize the same database even when connection details change.

```python
def register_db(key: str, conn: LanceAsyncConnection) -> LanceDatabase
```

**Parameters:**

- `key` — A stable identifier for this database (e.g., `"main_db"`). Must be unique.
- `conn` — An async LanceDB connection (from `connect_async()`).

**Returns:** A `LanceDatabase` handle for declaring target states.

The `LanceDatabase` can be used as a context manager to automatically unregister on exit:

```python
conn = await lancedb.connect_async("./lancedb_data")
with lancedb.register_db("my_db", conn) as db:
    # Use db to declare target states
    ...
# db is automatically unregistered here
```

#### Tables (parent state)

Declares a table as a target state. Returns a `TableTarget` for declaring rows.

```python
def LanceDatabase.declare_table_target(
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

### Table schema: from Python class

Define the table structure using a Python class (dataclass, NamedTuple, or Pydantic model):

```python
@classmethod
async def TableSchema.from_class(
    cls,
    record_type: type[RowT],
    primary_key: list[str],
    *,
    column_specs: dict[str, LanceType | VectorSchemaProvider] | None = None,
) -> TableSchema[RowT]
```

**Parameters:**

- `record_type` — A record type whose fields define table columns.
- `primary_key` — List of column names forming the primary key.
- `column_specs` — Optional per-column overrides for type mapping or vector configuration.

**Example:**

```python
@dataclass
class OutputDocument:
    doc_id: str
    title: str
    content: str
    embedding: Annotated[NDArray, embedder]

schema = await lancedb.TableSchema.from_class(
    OutputDocument,
    primary_key=["doc_id"],
)
```

Python types are automatically mapped to PyArrow types:

| Python Type | PyArrow Type |
|-------------|--------------|
| `bool` | `bool` |
| `int` | `int64` |
| `float` | `float64` |
| `str` | `string` |
| `bytes` | `binary` |
| `list`, `dict`, nested structs | `string` (JSON encoded) |
| `NDArray` (with vector schema) | `fixed_size_list<float>` |

To override the default mapping, provide a `LanceType` or `VectorSchemaProvider` via:

- **Type annotation** — using `typing.Annotated` on the field
- **`column_specs`** — passing overrides when constructing `TableSchema`

#### LanceType

Use `LanceType` to specify a custom PyArrow type or encoder:

```python
from typing import Annotated
from cocoindex.connectors.lancedb import LanceType
import pyarrow as pa

@dataclass
class MyRow:
    id: Annotated[int, LanceType(pa.int32())]
    value: Annotated[float, LanceType(pa.float32())]
```

#### VectorSchemaProvider

For `NDArray` fields, a `VectorSchemaProvider` specifies the vector dimension and dtype. Vector dimensions are typically determined by the embedding model—hardcoding them is error-prone and creates maintenance burden when switching models. By using a `VectorSchemaProvider`, the dimension is derived automatically from the source configuration.

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
schema = lancedb.TableSchema(
    {
        "doc_id": lancedb.ColumnDef(type=pa.string(), nullable=False),
        "title": lancedb.ColumnDef(type=pa.string()),
        "content": lancedb.ColumnDef(type=pa.string()),
        "embedding": lancedb.ColumnDef(type=pa.list_(pa.float32(), list_size=384)),
    },
    primary_key=["doc_id"],
)
```

### Example

```python
import cocoindex as coco
from cocoindex.connectors import lancedb

LANCEDB_URI = "./lancedb_data"

LANCE_DB = coco.ContextKey[lancedb.LanceDatabase]("lance_db")

@dataclass
class OutputDocument:
    doc_id: str
    title: str
    content: str
    embedding: Annotated[NDArray, embedder]

@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    conn = await lancedb.connect_async(LANCEDB_URI)
    builder.provide(LANCE_DB, lancedb.register_db("main_db", conn))
    yield

@coco.function
async def app_main() -> None:
    db = coco.use_context(LANCE_DB)

    # Declare table target state
    table = await db.mount_table_target(
        table_name="documents",
        table_schema=await lancedb.TableSchema.from_class(
            OutputDocument,
            primary_key=["doc_id"],
        ),
    )

    # Declare rows
    for doc in documents:
        table.declare_row(row=doc)
```
