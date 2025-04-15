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

## Neo4j

### Setup

If you don't have a Postgres database, you can start a Postgres SQL database for cocoindex using our docker compose config:

```bash
docker compose -f <(curl -L https://raw.githubusercontent.com/cocoindex-io/cocoindex/refs/heads/main/dev/neo4j.yaml) up -d
```

### Neo4jRelationship

The `Neo4jRelationship` storage exports each row as a relationship to Neo4j Knowledge Graph.
When you collect rows for `Neo4jRelationship`, fields will be mapped to a relationship and source/target nodes for the relationship:

*   You can explicitly specify fields mapped to source/target nodes.
*   All remaining fields will be mapped to relationship properties by default.


The spec takes the following fields:

*   `connection` (type: [auth reference](../core/flow_def#auth-registry) to `Neo4jConnectionSpec`): The connection to the Neo4j database. `Neo4jConnectionSpec` has the following fields:
    *   `uri` (type: `str`): The URI of the Neo4j database to use as the internal storage, e.g. `bolt://localhost:7687`.
    *   `user` (type: `str`): Username for the Neo4j database.
    *   `password` (type: `str`): Password for the Neo4j database.
    *   `db` (type: `str`, optional): The name of the Neo4j database to use as the internal storage, e.g. `neo4j`.
*   `rel_type` (type: `str`): The type of the relationship.
*   `source`/`target` (type: `Neo4jRelationshipEndSpec`): The source/target node of the relationship, with the following fields:
    *   `label` (type: `str`): The label of the node.
    *   `fields` (type: `list[Neo4jFieldMapping]`): Map fields from the collector to nodes in Neo4j, with the following fields:
        *   `field_name` (type: `str`): The name of the field in the collected row.
        *   `node_field_name` (type: `str`, optional): The name of the field to use as the node field. If unspecified, will use the same as `field_name`.
*   `nodes` (type: `dict[str, Neo4jRelationshipNodeSpec]`): This configures indexes for different node labels. Key is the node label. The value `Neo4jRelationshipNodeSpec` has the following fields to configure [storage indexes](../core/flow_def#storage-indexes) for the node.
        *   `primary_key_fields` is required.
        *   `vector_indexes` is also supported and optional.