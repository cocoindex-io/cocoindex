# CocoIndex Connectors Reference

Comprehensive reference for all CocoIndex connectors.

## Common Patterns

### Target Setup

All target connectors follow this pattern:

1. **Provide** connection via `ContextKey` in lifespan
2. **Mount** target using `mount_*_target(ContextKey, ...)` convenience method
3. **Declare** rows/points/files on the returned target object

Target connectors take a `ContextKey` (not the connection object directly) as their first argument. This enables CocoIndex to track the identity of the target across runs.

### Vector Support

Most connectors support vector embeddings via `VectorSchemaProvider`:

```python
EMBEDDER = coco.ContextKey[SentenceTransformerEmbedder]("embedder")

@dataclass
class Record:
    vector: Annotated[NDArray, EMBEDDER]  # Auto-infer dimensions from ContextKey
```

---

## PostgreSQL

**Import**: `from cocoindex.connectors import postgres`

**Capabilities**: Source + Target | **Vector**: pgvector

### Connection Setup

```python
import asyncpg
import cocoindex as coco
from cocoindex.connectors import postgres

PG_DB = coco.ContextKey[asyncpg.Pool]("pg_db", detect_change=False)

@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    async with await postgres.create_pool(DATABASE_URL) as pool:
        builder.provide(PG_DB, pool)
        yield
```

### As Target

```python
from dataclasses import dataclass
from typing import Annotated
from numpy.typing import NDArray

@dataclass
class Embedding:
    id: int
    text: str
    vector: Annotated[NDArray, EMBEDDER]

# Mount table target
target_table = await postgres.mount_table_target(
    PG_DB,
    table_name="embeddings",
    table_schema=await postgres.TableSchema.from_class(Embedding, primary_key=["id"]),
    pg_schema_name="my_schema",  # Optional, default "public"
)

# Optional: declare vector index
target_table.declare_vector_index(column="vector")

# Declare rows
target_table.declare_row(row=Embedding(id=1, text="hello", vector=vec))
```

### As Source

```python
@dataclass
class Record:
    id: int
    name: str

source = postgres.PgTableSource(
    coco.use_context(PG_DB),
    table_name="my_table",
    row_type=Record,
)

# Iterate (async)
fetcher = source.fetch_rows()
async for record in fetcher:
    ...

# Keyed iteration for mount_each
await coco.mount_each(process_record, source.fetch_rows().items(key=lambda r: r.id), table)
```

### Type Mapping

| Python | PostgreSQL |
|--------|-----------|
| `bool` | `boolean` |
| `int` | `bigint` |
| `float` | `double precision` |
| `str` | `text` |
| `bytes` | `bytea` |
| `UUID` | `uuid` |
| `datetime` | `timestamp with time zone` |
| `list`, `dict` | `jsonb` |
| `NDArray` + vector schema | `vector(n)` or `halfvec(n)` |

### SQL Command Attachments

```python
target_table.declare_sql_command_attachment(
    name="my_index",
    setup_sql="CREATE INDEX ...",
    teardown_sql="DROP INDEX ...",  # Optional
)
```

---

## SQLite

**Import**: `from cocoindex.connectors import sqlite`

**Capabilities**: Target only | **Vector**: sqlite-vec

### Connection Setup

```python
from cocoindex.connectors import sqlite

SQLITE_DB = coco.ContextKey[sqlite.ManagedConnection]("sqlite_db", detect_change=False)

@coco.lifespan
def coco_lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
    conn = sqlite.connect("./data.db", load_vec="auto")
    builder.provide(SQLITE_DB, conn)
    yield
    conn.close()
```

### As Target

```python
target_table = await sqlite.mount_table_target(
    SQLITE_DB,
    table_name="embeddings",
    table_schema=await sqlite.TableSchema.from_class(Embedding, primary_key=["id"]),
)

target_table.declare_row(row=Embedding(id=1, text="hello", vector=vec))
```

### Type Mapping

| Python | SQLite |
|--------|--------|
| `bool` | `INTEGER` (0/1) |
| `int` | `INTEGER` |
| `float` | `REAL` |
| `str` | `TEXT` |
| `bytes` | `BLOB` |
| `datetime` | `TEXT` (ISO format) |
| `list`, `dict` | `TEXT` (JSON) |
| `NDArray` + vector schema | `BLOB` (sqlite-vec) |

---

## LanceDB

**Import**: `from cocoindex.connectors import lancedb`

**Capabilities**: Target only | **Vector**: Native | **Storage**: Local or cloud (S3, GCS)

### Connection Setup

```python
from cocoindex.connectors import lancedb

LANCE_DB = coco.ContextKey[lancedb.LanceAsyncConnection]("lance_db", detect_change=False)

@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    conn = await lancedb.connect_async("./lancedb_data")  # or "s3://bucket/path"
    builder.provide(LANCE_DB, conn)
    yield
```

### As Target

```python
target_table = await lancedb.mount_table_target(
    LANCE_DB,
    table_name="embeddings",
    table_schema=await lancedb.TableSchema.from_class(Embedding, primary_key=["id"]),
)

target_table.declare_row(row=Embedding(id=1, text="hello", vector=vec))
```

### Type Mapping

| Python | PyArrow |
|--------|---------|
| `bool` | `bool` |
| `int` | `int64` |
| `float` | `float64` |
| `str` | `string` |
| `bytes` | `binary` |
| `list`, `dict` | `string` (JSON) |
| `NDArray` + vector schema | `fixed_size_list<float>` |

---

## Qdrant

**Import**: `from cocoindex.connectors import qdrant`

**Capabilities**: Target only | **Vector**: Native | **Model**: Point-oriented

### Connection Setup

```python
from cocoindex.connectors import qdrant

from qdrant_client import QdrantClient
QDRANT_DB = coco.ContextKey[QdrantClient]("qdrant_db", detect_change=False)

@coco.lifespan
def coco_lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
    client = qdrant.create_client("http://localhost:6333")
    builder.provide(QDRANT_DB, client)
    yield
```

### As Target (Single Vector)

```python
collection = await qdrant.mount_collection_target(
    QDRANT_DB,
    collection_name="embeddings",
    schema=await qdrant.CollectionSchema.create(
        vectors=qdrant.QdrantVectorDef(schema=EMBEDDER, distance="cosine"),
    ),
)

collection.declare_point(point=qdrant.PointStruct(
    id="point-1",
    vector=embedding_array.tolist(),
    payload={"text": "hello"},
))
```

### As Target (Named Vectors)

```python
schema = await qdrant.CollectionSchema.create(
    vectors={
        "text": qdrant.QdrantVectorDef(schema=text_embedder_key, distance="cosine"),
        "image": qdrant.QdrantVectorDef(schema=image_embedder_key, distance="cosine"),
    },
)
collection = await qdrant.mount_collection_target(QDRANT_DB, "multimodal", schema=schema)

collection.declare_point(point=qdrant.PointStruct(
    id="point-1",
    vector={"text": text_vec.tolist(), "image": image_vec.tolist()},
    payload={"title": "example"},
))
```

### Distance Metrics

- `"cosine"` -- Cosine similarity (default)
- `"dot"` -- Dot product
- `"euclid"` -- Euclidean distance (L2)

---

## SurrealDB

**Import**: `from cocoindex.connectors import surrealdb`

**Capabilities**: Target only | **Vector**: Native | **Model**: Graph + document

### Connection Setup

```python
from cocoindex.connectors import surrealdb

SURREAL_DB = coco.ContextKey[surrealdb.ConnectionFactory]("surreal_db", detect_change=False)

@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    factory = surrealdb.ConnectionFactory(
        url="ws://localhost:8000/rpc",
        namespace="my_ns",
        database="my_db",
        credentials={"username": "root", "password": "root"},
    )
    builder.provide(SURREAL_DB, factory)
    yield
```

### As Target (Table)

```python
target_table = await surrealdb.mount_table_target(
    SURREAL_DB,
    table_name="records",
    table_schema=await surrealdb.TableSchema.from_class(MyRecord),
)

target_table.declare_row(row=MyRecord(id="rec1", name="example"))
```

### As Target (Relation)

```python
relation_target = await surrealdb.mount_relation_target(
    SURREAL_DB,
    table_name="likes",
    from_table=users_table,
    to_table=items_table,
    table_schema=await surrealdb.TableSchema.from_class(MyRelation),
)
relation_target.declare_relation(from_id="user:1", to_id="item:1", record=MyRelation(...))
```

---

## LocalFS

**Import**: `from cocoindex.connectors import localfs`

**Capabilities**: Source + Target

### As Source

```python
from cocoindex.connectors import localfs
from cocoindex.resources.file import PatternFilePathMatcher

files = localfs.walk_dir(
    pathlib.Path("./data"),
    recursive=True,
    path_matcher=PatternFilePathMatcher(
        included_patterns=["**/*.py", "**/*.md"],
        excluded_patterns=[".*/**", "__pycache__/**"],
    ),
    live=True,  # Enable file watching for live mode
)

# Process each file
await coco.mount_each(process_file, files.items(), target_table)
```

### As Target (Single File)

```python
localfs.declare_file(
    outdir / "output.txt",
    "file content",
    create_parent_dirs=True,
)
```

### As Target (Directory)

```python
dir_target = await localfs.mount_dir_target(pathlib.Path("./output"))
dir_target.declare_file("file1.txt", "content 1")
dir_target.declare_file("subdir/file2.txt", "content 2")
```

### Stable File Paths

Use `localfs.FilePath` for stable memoization when the base directory might move:

```python
files = localfs.walk_dir(localfs.FilePath(path="./data"), ...)
```

---

## Amazon S3

**Import**: `from cocoindex.connectors import amazon_s3`

**Capabilities**: Source only

### Connection Setup

```python
import aiobotocore.session
from aiobotocore.client import AioBaseClient

S3_CLIENT = coco.ContextKey[AioBaseClient]("s3_client", detect_change=False)

@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    session = aiobotocore.session.get_session()
    async with session.create_client("s3") as s3_client:
        builder.provide(S3_CLIENT, s3_client)
        yield
```

### Reading Objects

```python
from cocoindex.connectors import amazon_s3

client = coco.use_context(S3_CLIENT)

# List and iterate objects
walker = amazon_s3.list_objects(
    client, "my-bucket",
    prefix="data/",
    path_matcher=PatternFilePathMatcher(included_patterns=["**/*.json"]),
    max_file_size=10_000_000,
)

await coco.mount_each(process_file, walker.items(), target)

# Single object
file = await amazon_s3.get_object(client, "s3://my-bucket/path/to/file.json")
content = await amazon_s3.read(client, "s3://my-bucket/path/to/file.json")
```

---

## Kafka

**Import**: `from cocoindex.connectors import kafka`

**Capabilities**: Source + Target

### As Source (Live)

```python
from confluent_kafka.aio import AIOConsumer
from cocoindex.connectors import kafka

consumer = AIOConsumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "my-group",
    "enable.auto.commit": "false",
    "auto.offset.reset": "earliest",
})

items = kafka.topic_as_map(consumer, ["my-topic"])
await coco.mount_each(process_message, items, target_table)
```

`topic_as_map()` returns a `LiveMapFeed`, which `mount_each()` auto-detects for live mode.

### As Target

```python
from confluent_kafka.aio import AIOProducer

KAFKA_PRODUCER = coco.ContextKey[AIOProducer]("kafka_producer", detect_change=False)

topic_target = await kafka.mount_kafka_topic_target(KAFKA_PRODUCER, "my-topic")
topic_target.declare_target_state(key="msg-key", value=json.dumps(data))
```

---

## Apache Doris

**Import**: `from cocoindex.connectors import doris`

**Capabilities**: Target only | **Vector**: Native | **Features**: Vector indexes, inverted indexes, Stream Load

### Connection Setup

```python
from cocoindex.connectors import doris

DORIS_DB = coco.ContextKey[doris.ManagedConnection]("doris_db", detect_change=False)

@coco.lifespan
def coco_lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
    config = doris.DorisConnectionConfig(
        fe_host="localhost",
        database="my_db",
        fe_http_port=8080,
        query_port=9030,
        username="root",
        password="",
    )
    conn = doris.connect(config)
    builder.provide(DORIS_DB, conn)
    yield
```

### As Target

```python
target_table = await doris.mount_table_target(
    DORIS_DB,
    table_name="embeddings",
    table_schema=await doris.TableSchema.from_class(MyRecord, primary_key=["id"]),
    vector_indexes=[doris.VectorIndexDef(field_name="embedding", metric_type="cosine_distance")],
)

target_table.declare_row(row=MyRecord(id=1, text="hello", embedding=vec))
```

---

## Google Drive

**Import**: `from cocoindex.connectors import google_drive`

**Capabilities**: Source only

```python
from cocoindex.connectors import google_drive

source = google_drive.GoogleDriveSource(
    service_account_credential_path="credentials.json",
    root_folder_ids=["folder-id"],
)
await coco.mount_each(process_file, source.items(), target)
```

---

## Connector Comparison

| Connector | Source | Target | Vectors | Best For |
|-----------|--------|--------|---------|----------|
| **PostgreSQL** | Y | Y | pgvector | Production SQL + vectors |
| **SQLite** | - | Y | sqlite-vec | Local SQL + vectors |
| **LanceDB** | - | Y | Native | Cloud-native vector DB |
| **Qdrant** | - | Y | Native | Specialized vector search |
| **SurrealDB** | - | Y | Native | Graph + document DB |
| **Apache Doris** | - | Y | Native | Analytical DB + vectors |
| **LocalFS** | Y | Y | N/A | File-based pipelines |
| **Amazon S3** | Y | - | N/A | Cloud object storage |
| **Kafka** | Y | Y | N/A | Streaming pipelines |
| **Google Drive** | Y | - | N/A | Cloud file source |

---

## See Also

- [API Reference](./api_reference.md)
- [Patterns](./patterns.md)
- [Setup Database](./setup_database.md)
