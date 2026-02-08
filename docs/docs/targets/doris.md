---
title: Apache Doris
description: CocoIndex Apache Doris Target
toc_max_heading_level: 4
---

import { ExampleButton } from '../../src/components/GitHubButton';

# Apache Doris

Exports data to Apache Doris (or VeloDB Cloud) with support for vector indexes and full-text search.

## Data Mapping

Here's how CocoIndex data elements map to Doris elements during export:

| CocoIndex Element | Doris Element |
|-------------------|---------------|
| an export target | a unique table |
| a collected row | a row |
| a field | a column |

For example, if you have a data collector that collects rows with fields `id`, `title`, and `embedding`, it will be exported to a Doris table with corresponding columns.

:::info Vector Type Mapping

Vectors are mapped to `ARRAY<FLOAT>` columns in Doris. Vector columns are automatically created as `NOT NULL` since Doris vector indexes require non-nullable columns.

:::

:::info Table Model

Doris tables are created using the `DUPLICATE KEY` model, which is required for vector index support in Doris 4.0+.

:::

## Spec

The spec takes the following fields:

### Connection

* `fe_host` (`str`, required): The Doris Frontend (FE) host address.

* `database` (`str`, required): The database name.

* `table` (`str`, required): The table name.

* `fe_http_port` (`int`, default: `8080`): The HTTP port for Stream Load API.

* `query_port` (`int`, default: `9030`): The MySQL protocol port for DDL operations.

* `username` (`str`, default: `"root"`): The username for authentication.

* `password` (`str`, default: `""`): The password for authentication.

* `enable_https` (`bool`, default: `False`): Whether to use HTTPS for Stream Load.

* `be_load_host` (`str`, optional): If set, rewrite the host in Stream Load redirects to this value. Doris FE redirects Stream Load requests (HTTP 307) to a BE node, but the BE's advertised address may be unreachable from the client (e.g. a Docker-internal IP when running on macOS). Setting this to `"localhost"` makes the client connect to the mapped port on the host instead. Leave unset for production clusters where BE nodes are directly reachable.

### Behavior

* `batch_size` (`int`, default: `10000`): Maximum number of rows per Stream Load batch.

* `stream_load_timeout` (`int`, default: `600`): Timeout in seconds for Stream Load operations.

* `auto_create_table` (`bool`, default: `True`): Whether to automatically create the table if it doesn't exist.

* `schema_evolution` (`"extend"` | `"strict"`, default: `"extend"`): Schema evolution strategy.
  - `"extend"`: Allow extra columns in the database, only add missing columns, never drop columns. Indexes are created only if referenced columns exist and are compatible.
  - `"strict"`: Require exact schema match. If the database table has extra columns or type mismatches, an error is raised. Tables are only recreated when primary keys change.

### Retry Configuration

Retry logic covers transient connection errors for both HTTP (Stream Load API) and MySQL protocol (DDL operations). Retryable errors include connection timeouts, server disconnections, and MySQL errors like "server has gone away" or "too many connections".

* `max_retries` (`int`, default: `3`): Maximum number of retry attempts for transient errors.

* `retry_base_delay` (`float`, default: `1.0`): Base delay in seconds between retries (uses exponential backoff).

* `retry_max_delay` (`float`, default: `30.0`): Maximum delay in seconds between retries.

### Table Properties

* `replication_num` (`int`, default: `1`): Number of replicas for the table.

* `buckets` (`int` | `"auto"`, default: `"auto"`): Number of hash buckets, or `"auto"` to let Doris determine automatically.

## Index Support

### Vector Index

Doris supports ANN (Approximate Nearest Neighbor) vector indexes using HNSW or IVF algorithms.

#### HNSW Index

```python
flow_builder.export(
    "embeddings",
    cocoindex.targets.doris.DorisTarget(
        fe_host="your-doris-host",
        database="your_db",
        table="embeddings",
    ),
    primary_key=["doc_id"],
    index=cocoindex.VectorIndex(
        field="embedding",
        metric=cocoindex.VectorSimilarityMetric.L2_DISTANCE,
        index_method=cocoindex.HnswVectorIndexMethod(
            m=16,
            ef_construction=200,
        ),
    ),
)
```

#### IVF Index

```python
flow_builder.export(
    "embeddings",
    cocoindex.targets.doris.DorisTarget(
        fe_host="your-doris-host",
        database="your_db",
        table="embeddings",
    ),
    primary_key=["doc_id"],
    index=cocoindex.VectorIndex(
        field="embedding",
        metric=cocoindex.VectorSimilarityMetric.L2_DISTANCE,
        index_method=cocoindex.IvfFlatVectorIndexMethod(
            lists=128,  # Number of clusters (nlist)
        ),
    ),
)
```

Supported metrics:
- `L2_DISTANCE`: Euclidean distance (recommended)
- `INNER_PRODUCT`: Inner product similarity
- `COSINE_SIMILARITY`: Cosine distance (note: does not support index acceleration in Doris 4.0)

:::info IVF Index Requirements

IVF (Inverted File) indexes require training data. The `lists` parameter (number of clusters) must be less than or equal to the number of vectors in the table. For best results:
- Load data first, then create the IVF index
- Use `lists` value appropriate for your data size (e.g., `lists=128` for 10K+ vectors)

:::

:::tip Vector Search Functions

Doris 4.0 provides two ways to query vectors:

1. **Approximate functions** (use index acceleration):
   - `l2_distance_approximate(vector_col, query_vector)` - L2/Euclidean distance
   - `inner_product_approximate(vector_col, query_vector)` - Inner product similarity

2. **Exact functions** (also benefit from index via ORDER BY optimization):
   - `l2_distance(vector_col, query_vector)`
   - `inner_product(vector_col, query_vector)`

The `build_vector_search_query` helper automatically uses the `_approximate` functions.

:::

### Full-Text Search Index (Inverted Index)

Doris supports inverted indexes for full-text search on text columns.

```python
flow_builder.export(
    "documents",
    cocoindex.targets.doris.DorisTarget(
        fe_host="your-doris-host",
        database="your_db",
        table="documents",
    ),
    primary_key=["doc_id"],
    fts_index=cocoindex.FtsIndex(
        field="content",
        parameters={"parser": "unicode"},  # or "english", "chinese"
    ),
)
```

### Hybrid Search

You can combine both vector and full-text indexes for hybrid search:

```python
flow_builder.export(
    "documents",
    cocoindex.targets.doris.DorisTarget(...),
    primary_key=["doc_id"],
    index=cocoindex.VectorIndex(
        field="embedding",
        metric=cocoindex.VectorSimilarityMetric.L2_DISTANCE,
    ),
    fts_index=cocoindex.FtsIndex(
        field="content",
        parameters={"parser": "unicode"},
    ),
)
```

## Schema Evolution

The `schema_evolution` parameter controls how CocoIndex handles schema differences between your flow definition and the existing database table.

### Extend Mode (Default)

In `"extend"` mode:
- Extra columns in the database that aren't in your schema are kept untouched
- Missing columns are added via `ALTER TABLE ADD COLUMN`
- Indexes are created only if the referenced column exists and has a compatible type
- Tables are never dropped except for primary key changes

This mode is recommended for production use as it's safe and non-destructive.

### Strict Mode

In `"strict"` mode:
- Extra columns in the database that aren't in your schema will raise an error
- Type mismatches between schema and database will raise an error
- Missing columns are still added via `ALTER TABLE ADD COLUMN`
- Tables are only dropped and recreated when primary key columns change

This mode is useful for development environments where you want to catch schema drift early. Note that if you need to remove columns, you must manually drop and recreate the table or use database migration tools.

## Query Helpers

The connector provides helper functions for querying data:

### connect_async

```python
from cocoindex.targets.doris import connect_async

conn = await connect_async(
    fe_host="your-doris-host",
    query_port=9030,
    username="root",
    password="your-password",
    database="your_db",
)
try:
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT * FROM your_table LIMIT 10")
        rows = await cursor.fetchall()
finally:
    conn.close()
    await conn.ensure_closed()
```

### build_vector_search_query

```python
from cocoindex.targets.doris import build_vector_search_query

sql = build_vector_search_query(
    table="your_db.embeddings",
    vector_field="embedding",
    query_vector=[0.1, 0.2, ...],  # Your query vector
    metric="l2_distance",
    limit=10,
    select_columns=["doc_id", "title", "content"],
)
```

## Requirements

- Apache Doris 4.0+ (for vector index support) or VeloDB Cloud
- Python packages: `aiohttp`, `aiomysql`

Install dependencies:
```bash
pip install aiohttp aiomysql
```

## Example

<ExampleButton
  href="https://github.com/cocoindex-io/cocoindex/tree/main/examples/text_embedding_doris"
  text="Text Embedding Example with Doris"
  margin="16px 0 24px 0"
/>
