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

#### Setting up a connection

Create a `ContextKey[lancedb.LanceAsyncConnection]` to identify your LanceDB connection, then provide it in your lifespan:

:::note
The key name is load-bearing across runs — it's the stable identity CocoIndex uses to track managed tables. See [ContextKey as stable identity](../programming_guide/context.md#contextkey-as-stable-identity) before renaming.
:::

```python
import cocoindex as coco

LANCE_DB = coco.ContextKey[lancedb.LanceAsyncConnection]("main_db")

@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    conn = await lancedb.connect_async(LANCEDB_URI)
    builder.provide(LANCE_DB, conn)
    yield
```

#### Tables (parent state)

Declares a table as a target state. Returns a `TableTarget` for declaring rows.

```python
def declare_table_target(
    db: ContextKey[LanceAsyncConnection],
    table_name: str,
    table_schema: TableSchema[RowT],
    *,
    managed_by: Literal["system", "user"] = "system",
) -> TableTarget[RowT, coco.PendingS]
```

**Parameters:**

- `db` — A `ContextKey[LanceAsyncConnection]` identifying the connection to use.
- `table_name` — Name of the table.
- `table_schema` — Schema definition including columns and primary key (see [Table Schema](#table-schema-from-python-class)).
- `managed_by` — Whether CocoIndex manages the table lifecycle (`"system"`) or assumes it exists (`"user"`).

**Returns:** A pending `TableTarget`. Use the convenience wrapper `await lancedb.mount_table_target(LANCE_DB, table_name, table_schema)` to resolve.

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

For `NDArray` fields, a [`VectorSchemaProvider`](../resource_types.md#vectorschemaprovider) annotation specifies the vector dimension and dtype. See [Vector Schema](../resource_types.md#vectorschemaprovider) for the full list of annotation options (`ContextKey`, embedder instance, or explicit `VectorSchema`).

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

LANCE_DB = coco.ContextKey[lancedb.LanceAsyncConnection]("main_db")

@dataclass
class OutputDocument:
    doc_id: str
    title: str
    content: str
    embedding: Annotated[NDArray, embedder]

@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    conn = await lancedb.connect_async(LANCEDB_URI)
    builder.provide(LANCE_DB, conn)
    yield

@coco.fn
async def app_main() -> None:
    # Declare table target state
    table = await lancedb.mount_table_target(
        LANCE_DB,
        "documents",
        await lancedb.TableSchema.from_class(
            OutputDocument,
            primary_key=["doc_id"],
        ),
    )

    # Declare rows
    for doc in documents:
        table.declare_row(row=doc)
```
