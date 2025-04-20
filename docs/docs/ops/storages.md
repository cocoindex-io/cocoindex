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

If you don't have a Postgres database, you can start a Postgres SQL database for cocoindex using our docker compose config:

```bash
docker compose -f <(curl -L https://raw.githubusercontent.com/cocoindex-io/cocoindex/refs/heads/main/dev/neo4j.yaml) up -d
```

:::warning

The docker compose config above will start a Neo4j Enterprise instance under the [Evaluation License](https://neo4j.com/terms/enterprise_us/),
with 30 days trial period.
Please read and agree the license before starting the instance.

:::

The `Neo4j` storage exports each row as a relationship to Neo4j Knowledge Graph. The spec takes the following fields:

*   `connection` (type: [auth reference](../core/flow_def#auth-registry) to `Neo4jConnectionSpec`): The connection to the Neo4j database. `Neo4jConnectionSpec` has the following fields:
    *   `uri` (type: `str`): The URI of the Neo4j database to use as the internal storage, e.g. `bolt://localhost:7687`.
    *   `user` (type: `str`): Username for the Neo4j database.
    *   `password` (type: `str`): Password for the Neo4j database.
    *   `db` (type: `str`, optional): The name of the Neo4j database to use as the internal storage, e.g. `neo4j`.
*   `mapping`: The mapping from collected row to nodes or relationships of the graph. 2 variations are supported:
    *   `cocoindex.storages.NodeMapping`: Each collected row is mapped to a node in the graph. It has the following fields:
        *   `label`: The label of the node.
    *   `cocoindex.storages.RelationshipMapping`: Each collected row is mapped to a relationship in the graph,
        With the following fields:

        *   `rel_type` (type: `str`): The type of the relationship.
        *   `source`/`target` (type: `cocoindex.storages.NodeReferenceMapping`): The source/target node of the relationship, with the following fields:
            *   `label` (type: `str`): The label of the node.
            *   `fields` (type: `Sequence[cocoindex.storages.TargetFieldMapping]`): Map fields from the collector to nodes in Neo4j, with the following fields:
                *   `source` (type: `str`): The name of the field in the collected row.
                *   `target` (type: `str`, optional): The name of the field to use as the node field. If unspecified, will use the same as `source`.

            :::info

            All fields specified in `fields.source` will be mapped to properties of source/target nodes. All remaining fields will be mapped to relationship properties by default.

            :::

        *   `nodes_storage_spec` (type: `dict[str, cocoindex.storages.NodeStorageSpec]`): This configures indexes for different node labels. Key is the node label. The value type `NodeStorageSpec` has the following fields to configure [storage indexes](../core/flow_def#storage-indexes) for the node.
                *   `primary_key_fields` is required.
                *   `vector_indexes` is also supported and optional.