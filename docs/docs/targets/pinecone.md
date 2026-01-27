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

:::info Installation and import

This target is provided via an optional dependency `[pinecone]`:
```sh
pip install "cocoindex[pinecone]"
```

To use it, import the spec from the engine specs module:
```python
from cocoindex.targets._engine_builtin_specs import Pinecone, PineconeConnection
from cocoindex.targets.pinecone import get_index
```

:::

## Spec

The spec `Pinecone` takes the following fields:

* `index_name` (`str`, required): The name of the index to export the data to.
* `connection` (`AuthEntryReference[PineconeConnection]`, required): Reference to a Pinecone connection spec.
  * `api_key` (`str`): Your Pinecone API key.
  * `environment` (`str | None`, optional): The Pinecone environment. If not specified, uses the default from your API key.
* `namespace` (`str`, optional, default: `""`): The namespace within the index to use.
* `cloud` (`str`, optional, default: `"aws"`): The cloud provider for the index (`"aws"`, `"gcp"`, or `"azure"`).
* `region` (`str`, optional, default: `"us-east-1"`): The region for the index.
* `batch_size` (`int`, optional, default: `100`): Number of vectors to upsert/delete in a single batch operation.

Additional notes:

* Exactly one primary key field is required for Pinecone targets. This maps to the vector `id`.
* Exactly one vector embedding field is required. This must be a list/array of floats.
* All other fields are stored as metadata and can be used for filtering.
* **Metadata filtering** is supported via Pinecone's metadata query syntax. You can filter on any metadata fields during queries.

:::info

Pinecone indexes must be created before use. If the index doesn't exist, CocoIndex will attempt to create it automatically using the dimension from your vector embeddings.

:::

You can find an end-to-end example here: [examples/text_embedding_pinecone](https://github.com/cocoindex-io/cocoindex/tree/main/examples/text_embedding_pinecone).

### Example Usage
```python
import cocoindex
from cocoindex.targets._engine_builtin_specs import Pinecone, PineconeConnection
from cocoindex.auth_registry import AuthEntryReference

@cocoindex.flow_def(name="DocumentSearchFlow")
def document_search_flow(flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope):

    doc_collector = data_scope.add_collector()

    # Create a Pinecone connection reference
    pinecone_conn = AuthEntryReference(
        name="pinecone_connection",
        value=PineconeConnection(
            api_key="your-api-key",
            environment=None  # Optional, inferred from API key if not provided
        )
    )

    doc_collector.export(
        "documents",
        Pinecone(
            index_name="documents",
            connection=pinecone_conn,
            namespace="production",
            cloud="aws",
            region="us-east-1"
        ),
        primary_key_fields=["id"],
        vector_fields=["embedding"]
    )
```

## `get_index()` helper

We provide a helper to obtain a Pinecone index instance for querying:
```python
from cocoindex.targets.pinecone import get_index

index = get_index(
    api_key="your-api-key",
    index_name="documents"
)

# Query the index
results = index.query(
    vector=[0.1, 0.2, ...],
    top_k=10,
    namespace="production"
)
```

Signature:
```python
def get_index(
    api_key: str,
    index_name: str,
) -> pinecone.Index
```

This helper creates a Pinecone index reference configured with your API key and index name, making it easy to query the data you've indexed with CocoIndex.

## Example

<ExampleButton
  href="https://github.com/cocoindex-io/cocoindex/tree/main/examples/text_embedding_pinecone"
  text="Text Embedding Pinecone Example"
  margin="16px 0 24px 0"
/>
