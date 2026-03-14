---
title: SurrealDB
toc_max_heading_level: 4
description: CocoIndex connector for writing to SurrealDB, with support for normal tables, relation (graph edge) tables, and vector indexes.
---

# SurrealDB

The `surrealdb` connector provides utilities for writing records to SurrealDB databases, with support for normal tables, relation (graph edge) tables, optional schema enforcement, and vector indexes.

```python
from cocoindex.connectors import surrealdb
```

:::note Dependencies
This connector requires additional dependencies. Install with:

```bash
pip install cocoindex[surrealdb]
```

:::

## Connection setup

### make_conn_params

Create a connection parameter object that identifies a SurrealDB database. Connections are created on-demand on the engine's event loop via WebSocket when target states are applied.

```python
def make_conn_params(
    *,
    url: str,
    namespace: str,
    database: str,
    credentials: dict[str, str] | None = None,
) -> ConnParams
```

**Parameters:**

- `url` — WebSocket URL (e.g., `"ws://localhost:8000/rpc"`).
- `namespace` — SurrealDB namespace.
- `database` — SurrealDB database name.
- `credentials` — Optional dict with signin credentials (e.g., `{"username": "root", "password": "root"}`).

**Returns:** A `ConnParams` object to be provided via `ContextKey`.

**Example:**

```python
import cocoindex as coco

SURREAL_DB = coco.ContextKey("main_db", tracked=False)

@coco.lifespan
def coco_lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
    builder.provide(
        SURREAL_DB,
        surrealdb.make_conn_params(
            url="ws://localhost:8000/rpc",
            namespace="test",
            database="test",
            credentials={"username": "root", "password": "root"},
        ),
    )
    yield
```

### create_connection

Create and authenticate a standalone SurrealDB connection. This is a convenience helper for use outside the target state system (e.g., for querying).

```python
async def create_connection(
    url: str,
    *,
    namespace: str,
    database: str,
    credentials: dict[str, str] | None = None,
) -> AsyncSurreal
```

## As target

The `surrealdb` connector provides target state APIs for writing records to normal tables and relation tables. CocoIndex tracks what records should exist and automatically handles upserts and deletions.

All tables within the same database share a single transaction sink, so changes across related tables and relations are applied atomically.

### Declaring target states

#### Setting up a connection

See [make_conn_params](#make_conn_params) above.

#### Normal tables (parent state)

Declares a table as a target state. Returns a `TableTarget` for declaring records.

```python
def declare_table_target(
    db: ContextKey,
    table_name: str,
    table_schema: TableSchema[RowT] | None = None,
    *,
    managed_by: Literal["system", "user"] = "system",
) -> TableTarget[RowT, coco.PendingS]
```

**Parameters:**

- `db` — A `ContextKey` holding the `ConnParams` for the SurrealDB connection.
- `table_name` — Name of the table.
- `table_schema` — Optional schema definition (see [Table Schema](#table-schema-from-python-class)). When provided, the table is `SCHEMAFULL`; when omitted, the table is `SCHEMALESS`.
- `managed_by` — Whether CocoIndex manages the table lifecycle (`"system"`) or assumes it exists (`"user"`).

**Returns:** A pending `TableTarget`. Use `await surrealdb.mount_table_target(SURREAL_DB, ...)` to get a resolved target.

#### Records (child states)

Once a `TableTarget` is resolved, declare records to be upserted:

```python
def TableTarget.declare_record(
    self,
    *,
    row: RowT,
) -> None
```

**Parameters:**

- `row` — A row object (dict, dataclass, NamedTuple, or Pydantic model). Must include an `id` field.

`declare_row` is an alias for `declare_record`, for compatibility with Postgres and other RDBMS targets.

#### Relation tables (parent state)

Declares a relation (graph edge) table. Returns a `RelationTarget` for declaring relation records.

```python
def declare_relation_target(
    db: ContextKey,
    table_name: str,
    from_table: TableTarget | Collection[TableTarget],
    to_table: TableTarget | Collection[TableTarget],
    table_schema: TableSchema[RowT] | None = None,
    *,
    managed_by: Literal["system", "user"] = "system",
) -> RelationTarget[RowT, coco.PendingS]
```

**Parameters:**

- `db` — A `ContextKey` holding the `ConnParams` for the SurrealDB connection.
- `table_name` — Name of the relation table.
- `from_table` — Source table(s). Pass a single `TableTarget` or a collection for polymorphic relations.
- `to_table` — Target table(s). Same rules as `from_table`.
- `table_schema` — Optional schema. The schema does **not** require an `id` field (unlike normal tables).
- `managed_by` — Whether CocoIndex manages the table lifecycle.

**Returns:** A pending `RelationTarget`. Use `await surrealdb.mount_relation_target(SURREAL_DB, ...)` to get a resolved target.

#### Relations (child states)

Once a `RelationTarget` is resolved, declare relation records:

```python
def RelationTarget.declare_relation(
    self,
    *,
    from_id: Any,
    to_id: Any,
    record: RowT | None = None,
    from_table: TableTarget | None = None,
    to_table: TableTarget | None = None,
) -> None
```

**Parameters:**

- `from_id` — ID of the source record.
- `to_id` — ID of the target record.
- `record` — Optional data fields for the relation. The `id` field is optional: when absent, the record id is auto-derived from the endpoints as `"{from_table}_{from_id}_{to_table}_{to_id}"`.
- `from_table` / `to_table` — Required when the relation was declared with multiple (polymorphic) source/target tables.

#### Vector indexes (attachment)

Declare a vector index on a field of the table. CocoIndex tracks the index spec and automatically creates, recreates, or drops the index as needed.

```python
def TableTarget.declare_vector_index(
    self,
    *,
    name: str | None = None,
    field: str,
    metric: Literal["cosine", "euclidean", "manhattan"] = "cosine",
    method: Literal["mtree", "hnsw"] = "mtree",
    dimension: int | None = None,
    vector_type: Literal["f32", "f64", "i16", "i32", "i64"] = "f32",
) -> None
```

**Parameters:**

- `name` — Index name (defaults to `idx_{table}__{field}`).
- `field` — Field to index (must be a vector/array field).
- `metric` — Distance metric: `"cosine"`, `"euclidean"`, or `"manhattan"`.
- `method` — Index method: `"mtree"` or `"hnsw"`.
- `dimension` — Vector dimension (required).
- `vector_type` — Vector element type: `"f32"`, `"f64"`, `"i16"`, `"i32"`, or `"i64"`.

### Table schema: from Python class

Define the table structure using a Python class (dataclass, NamedTuple, or Pydantic model):

```python
@classmethod
async def TableSchema.from_class(
    cls,
    record_type: type[RowT],
    *,
    column_overrides: dict[str, SurrealType | VectorSchemaProvider] | None = None,
) -> TableSchema[RowT]
```

**Parameters:**

- `record_type` — A record type whose fields define table columns. For normal tables, must include an `id` field. For relation tables, `id` is optional.
- `column_overrides` — Optional per-column overrides for type mapping or vector configuration.

**Example:**

```python
@dataclass
class Product:
    id: str
    name: str
    price: float
    embedding: Annotated[NDArray, embedder]

schema = await surrealdb.TableSchema.from_class(Product)
```

Python types are automatically mapped to SurrealDB types:

| Python Type | SurrealDB Type |
|-------------|----------------|
| `bool` | `bool` |
| `int` | `int` |
| `float` | `float` |
| `decimal.Decimal` | `decimal` |
| `str` | `string` |
| `bytes` | `bytes` |
| `uuid.UUID` | `uuid` |
| `datetime.datetime` | `datetime` |
| `datetime.date` | `datetime` |
| `datetime.time` | `datetime` |
| `datetime.timedelta` | `duration` |
| `list`, `dict`, nested structs | `object` |
| `NDArray` (with vector schema) | `array<float, N>` |

#### SurrealType

Use `SurrealType` to override the default type mapping:

```python
from typing import Annotated
from cocoindex.connectors.surrealdb import SurrealType

@dataclass
class MyRow:
    id: str
    value: Annotated[float, SurrealType("decimal")]
```

Or via `column_overrides`:

```python
schema = await surrealdb.TableSchema.from_class(
    MyRow,
    column_overrides={"value": surrealdb.SurrealType("decimal")},
)
```

#### VectorSchemaProvider

For `NDArray` fields, a [`VectorSchemaProvider`](../resource_types.md#vectorschemaprovider) annotation specifies the vector dimension and dtype. See [Vector Schema](../resource_types.md#vectorschemaprovider) for the full list of annotation options.

### Table schema: explicit column definitions

Define columns directly using `ColumnDef`:

```python
def TableSchema.__init__(
    self,
    columns: dict[str, ColumnDef],
    *,
    row_type: type[RowT] | None = None,
) -> None
```

**Example:**

```python
schema = surrealdb.TableSchema(
    {
        "id": surrealdb.ColumnDef(type="string", nullable=False),
        "name": surrealdb.ColumnDef(type="string", nullable=False),
        "price": surrealdb.ColumnDef(type="float"),
    },
)
```

### Example: Normal tables

```python
import cocoindex as coco
from cocoindex.connectors import surrealdb

SURREAL_DB = coco.ContextKey("main_db", tracked=False)

@dataclass
class Product:
    id: str
    name: str
    price: float
    embedding: Annotated[NDArray, embedder]

@coco.lifespan
def coco_lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
    builder.provide(
        SURREAL_DB,
        surrealdb.make_conn_params(
            url="ws://localhost:8000/rpc",
            namespace="test",
            database="test",
            credentials={"username": "root", "password": "root"},
        ),
    )
    yield

@coco.fn
async def app_main() -> None:
    # Declare table target state
    table = await surrealdb.mount_table_target(
        SURREAL_DB,
        "products",
        await surrealdb.TableSchema.from_class(Product),
    )

    # Declare records
    for product in products:
        table.declare_record(row=product)

    # Declare a vector index
    table.declare_vector_index(
        field="embedding",
        metric="cosine",
        method="hnsw",
        dimension=384,
    )
```

### Example: Relation tables

```python
@dataclass
class Person:
    id: str
    name: str

@dataclass
class Post:
    id: str
    title: str

@coco.fn
async def app_main() -> None:
    person_schema = await surrealdb.TableSchema.from_class(Person)
    person_target = await surrealdb.mount_table_target(SURREAL_DB, "person", person_schema)
    for p in persons:
        person_target.declare_record(row=p)

    post_schema = await surrealdb.TableSchema.from_class(Post)
    post_target = await surrealdb.mount_table_target(SURREAL_DB, "post", post_schema)
    for p in posts:
        post_target.declare_record(row=p)

    # Declare a relation table (schemaless, no id needed)
    likes_target = await surrealdb.mount_relation_target(
        SURREAL_DB,
        "likes",
        from_table=person_target,
        to_table=post_target,
    )

    # Declare relations — id is auto-derived from endpoints
    for like in likes:
        likes_target.declare_relation(
            from_id=like["person_id"],
            to_id=like["post_id"],
        )
```
