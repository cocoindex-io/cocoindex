---
title: LanceDB
description: CocoIndex LanceDB Target
toc_max_heading_level: 4
---

import { ExampleButton } from '../../src/components/GitHubButton';

# LanceDB

Exports data to a [LanceDB](https://lancedb.github.io/lancedb/) table.

## Data Mapping

Here's how CocoIndex data elements map to LanceDB elements during export:

| CocoIndex Element | LanceDB Element |
|-------------------|-----------------|
| an export target  | a unique table  |
| a collected row   | a row           |
| a field           | a column        |

::::info Installation and import

This target is provided via an optional dependency `[lancedb]`:

```sh
pip install "cocoindex[lancedb]"
```

To use it, you need to import the submodule `cocoindex.targets.lancedb`:

```python
import cocoindex.targets.lancedb as coco_lancedb
```

::::

## Spec

The spec `coco_lancedb.LanceDB` takes the following fields:

* `db_uri` (`str`, required): The LanceDB database location (e.g. `./lancedb_data`).
* `table_name` (`str`, required): The name of the table to export the data to.
* `db_options` (`coco_lancedb.DatabaseOptions`, optional): Advanced database options.
  * `storage_options` (`dict[str, Any]`, optional): Passed through to LanceDB when connecting.

Additional notes:

* Exactly one primary key field is required for LanceDB targets. We create B-Tree index on this key column.
* **Full-Text Search (FTS) indexes** are supported via the `fts_indexes` parameter. Note that FTS functionality requires [LanceDB Enterprise](https://lancedb.com/docs/indexing/fts-index/). You can pass any parameters supported by the target's FTS index creation API (e.g., `tokenizer_name` for LanceDB). See [LanceDB FTS documentation](https://lancedb.com/docs/indexing/fts-index/) for full parameter details.

:::info

LanceDB has a limitation that it cannot build a vector index on an empty table (see [LanceDB issue #4034](https://github.com/lancedb/lance/issues/4034)).
If you want to use vector indexes, you can run the flow once to populate the target table with data, and then create the vector indexes.

:::

You can find an end-to-end example here: [examples/text_embedding_lancedb](https://github.com/cocoindex-io/cocoindex/tree/main/examples/text_embedding_lancedb).

### FTS Index Example

```python
import cocoindex
import cocoindex.targets.lancedb as coco_lancedb

@cocoindex.flow_def(name="DocumentSearchFlow")
def document_search_flow(flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope):
    # ... source and transformations ...

    doc_collector = data_scope.add_collector()
    # ... collect document data ...

    doc_collector.export(
        "documents",
        coco_lancedb.LanceDB(
            db_uri="./lancedb_data",
            table_name="documents"
        ),
        primary_key_fields=["id"],
        # Add FTS indexes for full-text search
        fts_indexes=[
            # Basic FTS index with default tokenizer
            cocoindex.FtsIndexDef("content"),
            # FTS index with stemming for better search recall
            cocoindex.FtsIndexDef("description", parameters={"tokenizer_name": "en_stem"}),
            # FTS index with position tracking for phrase searches
            cocoindex.FtsIndexDef("title", parameters={"tokenizer_name": "default", "with_position": True})
        ]
    )
```

## `connect_async()` helper

We provide a helper to obtain a shared `AsyncConnection` that is reused across your process and shared with CocoIndex's writer for strong read-after-write consistency:

```python
from cocoindex.targets import lancedb as coco_lancedb

db = await coco_lancedb.connect_async("./lancedb_data")
table = await db.open_table("TextEmbedding")
```

Signature:

```python
def connect_async(
  db_uri: str,
  *,
  db_options: coco_lancedb.DatabaseOptions | None = None,
  read_consistency_interval: datetime.timedelta | None = None
) -> lancedb.AsyncConnection
```

Once `db_uri` matches, it automatically reuses the same connection instance without re-establishing a new connection.
This achieves strong consistency between your indexing and querying logic, if they run in the same process.

## Example

<ExampleButton
  href="https://github.com/cocoindex-io/cocoindex/tree/main/examples/text_embedding_lancedb"
  text="Text Embedding LanceDB Example"
  margin="16px 0 24px 0"
/>
