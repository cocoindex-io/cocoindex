---
title: ChromaDB
description: CocoIndex ChromaDB Target
toc_max_heading_level: 4
---

import { ExampleButton } from '../../src/components/GitHubButton';

# ChromaDB

Exports data to a [ChromaDB](https://www.trychroma.com/) collection with vector search support.

## Data Mapping

Here's how CocoIndex data elements map to ChromaDB elements during export:

| CocoIndex Element | ChromaDB Element |
|-------------------|------------------|
| an export target  | a unique collection |
| a collected row   | a document |
| a vector field    | the embedding |
| a field matching `document_field` | the document content |
| other fields      | metadata key-value pairs |

ChromaDB supports one embedding per document. Exactly one vector field must be present in the value schema — it becomes the embedding. Non-vector fields become metadata, except the field named by `document_field` (if set), which is stored as ChromaDB's document content and enables its built-in text search.

::::info Installation and import

This target is provided via an optional dependency `[chromadb]`:

```sh
pip install "cocoindex[chromadb]"
```

To use it, import the submodule `cocoindex.targets.chromadb`:

```python
import cocoindex.targets.chromadb as coco_chromadb
```

::::

## Spec

The spec `coco_chromadb.ChromaDB` takes the following fields:

### Collection

* `collection_name` (`str`, required): The name of the collection to export data to.

* `document_field` (`str`, optional): Name of the value field to pass as ChromaDB's `documents` parameter instead of metadata. Enables ChromaDB's built-in text search on that field.

### Client

* `client_type` (`coco_chromadb.ClientType`, optional, default: `PERSISTENT`): Which ChromaDB client to use:
  - `PERSISTENT` — local on-disk storage via `PersistentClient`.
  - `HTTP` — connects to a remote ChromaDB server via `HttpClient`.
  - `CLOUD` — connects to [Chroma Cloud](https://www.trychroma.com/) via `CloudClient`.

* `path` (`str`, optional, default: `"./chromadb_data"`): Data directory. Used with `PERSISTENT` client.

* `host` (`str`, optional, default: `"localhost"`): Server host. Used with `HTTP` client.

* `port` (`int`, optional, default: `8000`): Server port. Used with `HTTP` client.

* `ssl` (`bool`, optional, default: `False`): Whether to use SSL. Used with `HTTP` client.

* `api_key` (`str`, optional): API key for authentication. Required when using `CLOUD` client.

* `tenant` (`str`, optional): Chroma tenant (defaults to Chroma's default tenant).

* `database` (`str`, optional): Chroma database (defaults to Chroma's default database).

### HNSW Index

* `hnsw_config` (`coco_chromadb.HnswConfig`, optional): HNSW index tuning parameters.
  * `m` (`int`, optional): Number of bi-directional links per element.
  * `ef_construction` (`int`, optional): Size of the dynamic candidate list during index construction.
  * `ef_search` (`int`, optional): Size of the dynamic candidate list during search.

Additional notes:

* Exactly one primary key field is required.
* Exactly one vector field is required — ChromaDB stores a single embedding per document.
* Supported distance metrics: `COSINE_SIMILARITY`, `L2_DISTANCE`, `INNER_PRODUCT`.
* Complex metadata values (lists, dicts, etc.) are JSON-serialized automatically.

## Example

```python
import cocoindex
import cocoindex.targets.chromadb as coco_chromadb

@cocoindex.flow_def(name="TextEmbeddingWithChromaDB")
def text_embedding_flow(flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope):
    # ... source and transformations ...

    doc_embeddings = data_scope.add_collector()
    # ... collect fields: id, filename, text, text_embedding ...

    doc_embeddings.export(
        "doc_embeddings",
        coco_chromadb.ChromaDB(
            collection_name="text_embedding",
            path="./chromadb_data",
            document_field="text",
        ),
        primary_key_fields=["id"],
        vector_indexes=[
            cocoindex.VectorIndexDef(
                "text_embedding",
                cocoindex.VectorSimilarityMetric.COSINE_SIMILARITY,
            )
        ],
    )
```

<ExampleButton
  href="https://github.com/cocoindex-io/cocoindex/tree/main/examples/text_embedding_chromadb"
  text="Text Embedding ChromaDB Example"
  margin="16px 0 24px 0"
/>
