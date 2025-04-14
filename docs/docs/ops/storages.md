---
title: Storages
description: CocoIndex Built-in Storages
---

# CocoIndex Built-in Storages

## Postgres

Exports data to Postgres database (with pgvector extension).

The spec takes the following fields:

*   `database_url` (type: `str`, optional): The URL of the Postgres database to use as the internal storage, e.g. `postgres://cocoindex:cocoindex@localhost/cocoindex`. If unspecified, will use the same database as the [internal storage](/docs/core/basics#internal-storage).

*   `table_name` (type: `str`, optional): The name of the table to store to. If unspecified, will generate a new automatically. We recommend specifying a name explicitly if you want to directly query the table. It can be omitted if you want to use CocoIndex's query handlers to query the table.

## Qdrant

Exports data to a [Qdrant](https://qdrant.tech/) collection.

The spec takes the following fields:

*   `collection_name` (type: `str`, required): The name of the collection to export the data to.

*   `grpc_url` (type: `str`, optional): The [gRPC URL](https://qdrant.tech/documentation/interfaces/#grpc-interface) of the Qdrant instance. Defaults to `http://localhost:6334/`.

*   `api_key` (type: `str`, optional). API key to authenticate requests with.

Before exporting, you must create a collection with a [vector name](https://qdrant.tech/documentation/concepts/vectors/#named-vectors) that matches the vector field name in CocoIndex, and set `setup_by_user=True` during export.

Example:

```python
doc_embeddings.export(
    "doc_embeddings",
    cocoindex.storages.Qdrant(
        collection_name="cocoindex",
        grpc_url="https://xyz-example.cloud-region.cloud-provider.cloud.qdrant.io:6334/",
        api_key="<your-api-key-here>",
    ),
    primary_key_fields=["id_field"],
    setup_by_user=True,
)
```

You can find an end-to-end example [here](https://github.com/cocoindex-io/cocoindex/tree/main/examples/text_embedding_qdrant).
