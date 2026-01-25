---
title: Pinecone
description: CocoIndex Pinecone Target
toc_max_heading_level: 4
---

import { ExampleButton } from '../../src/components/GitHubButton';

# Pinecone

Exports data to a [Pinecone](https://www.pinecone.io/) index.

## Data Mapping

Here's how CocoIndex data elements map to Pinecone elements during export:

| CocoIndex Element | Pinecone Element |
|-------------------|------------------|
| an export target  | a unique index   |
| a collected row   | a vector         |
| a field           | metadata field   |

::::info Installation and import

This target is provided via an optional dependency `[pinecone]`:
```sh
pip install "cocoindex[pinecone]"
```

To use it, you need to import the submodule `cocoindex.targets.pinecone`:
```python
import cocoindex.targets.pinecone as coco_pinecone
```

::::

## Spec

The spec `coco_pinecone.Pinecone` takes the following fields:

* `api_key` (`str`, required): Your Pinecone API key.
* `index_name` (`str`, required): The name of the index to export the data to.
* `environment` (`str`, optional): The Pinecone environment (e.g., `us-east-1-aws`). If not specified, uses the default environment from your API key.
* `namespace` (`str`, optional, default: `""`): The namespace within the index to use.
* `batch_size` (`int`, optional, default: 100): Number of vectors to upsert in a single batch operation.
* `index_options` (`coco_pinecone.IndexOptions`, optional): Advanced index configuration options.
  * `metric` (`str`, optional, default: `"cosine"`): Distance metric for the index (`"cosine"`, `"euclidean"`, or `"dotproduct"`).
  * `pod_type` (`str`, optional): Pod type for the index (e.g., `"p1.x1"`, `"s1.x1"`).
  * `replicas` (`int`, optional): Number of replicas for the index.

Additional notes:

* Exactly one primary key field is required for Pinecone targets. This maps to the vector `id`.
* Exactly one vector embedding field is required. This must be a list/array of floats.
* All other fields are stored as metadata and can be used for filtering.
* **Metadata filtering** is supported via Pinecone's metadata query syntax. You can filter on any metadata fields during queries.

:::info

Pinecone indexes must be created before use. If the index doesn't exist, CocoIndex will attempt to create it automatically using the dimension from your vector embeddings and the specified `index_options`.

:::

You can find an end-to-end example here: [examples/text_embedding_pinecone](https://github.com/cocoindex-io/cocoindex/tree/main/examples/text_embedding_pinecone).

### Metadata Filtering Example
```python
import cocoindex
import cocoindex.targets.pinecone as coco_pinecone

@cocoindex.flow_def(name="DocumentSearchFlow")
def document_search_flow(flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope):

    doc_collector = data_scope.add_collector()

    doc_collector.export(
        "documents",
        coco_pinecone.Pinecone(
            api_key="your-api-key",
            index_name="documents",
            namespace="production",
            index_options=coco_pinecone.IndexOptions(
                metric="cosine",
                pod_type="p1.x1"
            )
        ),
        primary_key_fields=["id"],
        vector_fields=["embedding"]
    )
```

## `get_index()` helper

We provide a helper to obtain a Pinecone index instance that is configured consistently with CocoIndex's writer:
```python
from cocoindex.targets import pinecone as coco_pinecone

index = coco_pinecone.get_index(
    api_key="your-api-key",
    index_name="documents",
    namespace="production"
)
```

Signature:
```python
def get_index(
  api_key: str,
  index_name: str,
  *,
  environment: str | None = None,
  namespace: str = ""
) -> pinecone.Index
```

This helper ensures you're using the same index configuration as your indexing pipeline, making it easier to query the data you've indexed.

## Example

<ExampleButton
  href="https://github.com/cocoindex-io/cocoindex/tree/main/examples/text_embedding_pinecone"
  text="Text Embedding Pinecone Example"
  margin="16px 0 24px 0"
/>
