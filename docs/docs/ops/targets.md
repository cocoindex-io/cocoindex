---
title: Targets
description: CocoIndex Built-in Targets
toc_max_heading_level: 4
---

# CocoIndex Built-in Targets

For each target, data are exported from a data collector, containing data of multiple entries, each with multiple fields.
The way to map data from a data collector to a target depends on data model of the target.

## Entry-Oriented Targets

An entry-oriented target organizes data into independent entries, such as rows, key-value pairs, or documents.
Each entry is self-contained and does not explicitly link to others.
There is usually a straightforward mapping from data collector rows to entries.

### Postgres

Exports data to Postgres database (with pgvector extension).

#### Data Mapping

Here's how CocoIndex data elements map to Postgres elements during export:

| CocoIndex Element | Postgres Element |
|-------------------|------------------|
| an export target | a unique table |
| a collected row | a row |
| a field | a column |

For example, if you have a data collector that collects rows with fields `id`, `title`, and `embedding`, it will be exported to a Postgres table with corresponding columns.
It should be a unique table, meaning that no other export target should export to the same table.

#### Spec

The spec takes the following fields:

*   `database` (type: [auth reference](../core/flow_def#auth-registry) to `DatabaseConnectionSpec`, optional): The connection to the Postgres database.
    See [DatabaseConnectionSpec](../core/settings#databaseconnectionspec) for its specific fields.
    If not provided, will use the same database as the [internal storage](/docs/core/basics#internal-storage).

*   `table_name` (type: `str`, optional): The name of the table to store to. If unspecified, will use the table name `[${AppNamespace}__]${FlowName}__${TargetName}`, e.g. `DemoFlow__doc_embeddings` or `Staging__DemoFlow__doc_embeddings`.

### Qdrant

Exports data to a [Qdrant](https://qdrant.tech/) collection.

#### Data Mapping

Here's how CocoIndex data elements map to Qdrant elements during export:

| CocoIndex Element | Qdrant Element |
|-------------------|------------------|
| an export target  | a unique collection |
| a collected row   | a point |
| a field           | a named vector, if fits into Qdrant vector; or a field within payload otherwise |

A vector with `Float32`, `Float64` or `Int64` type, and with fixed dimension, fits into Qdrant vector.

#### Spec

The spec takes the following fields:

*   `connection` (type: [auth reference](../core/flow_def#auth-registry) to `QdrantConnection`, optional): The connection to the Qdrant instance. `QdrantConnection` has the following fields:
    *   `grpc_url` (type: `str`): The [gRPC URL](https://qdrant.tech/documentation/interfaces/#grpc-interface) of the Qdrant instance, e.g. `http://localhost:6334/`.
    *   `api_key` (type: `str`, optional). API key to authenticate requests with.

    If `connection` is not provided, will use local Qdrant instance at `http://localhost:6334/` by default.

*   `collection_name` (type: `str`, required): The name of the collection to export the data to.

You can find an end-to-end example [here](https://github.com/cocoindex-io/cocoindex/tree/main/examples/text_embedding_qdrant).

## Property Graph Targets

Property graph is a widely-adopted model for knowledge graphs, where both nodes and relationships can have properties.
[Graph database concepts](https://neo4j.com/docs/getting-started/appendix/graphdb-concepts/) has a good introduction to basic concepts of property graphs.

The following concepts will be used in the following sections:
* [Node](https://neo4j.com/docs/getting-started/appendix/graphdb-concepts/#graphdb-node)
    * [Node label](https://neo4j.com/docs/getting-started/appendix/graphdb-concepts/#graphdb-labels), which represents a type of nodes.
* [Relationship](https://neo4j.com/docs/getting-started/appendix/graphdb-concepts/#graphdb-relationship), which describes a connection between two nodes.
    * [Relationship type](https://neo4j.com/docs/getting-started/appendix/graphdb-concepts/#graphdb-relationship-type)
* [Properties](https://neo4j.com/docs/getting-started/appendix/graphdb-concepts/#graphdb-properties), which are key-value pairs associated with nodes and relationships.

### Data Mapping

Data from collectors are mapped to graph elements in various types:

1.  Rows from collectors → Nodes in the graph
2.  Rows from collectors → Relationships in the graph (including source and target nodes of the relationship)

This is what you need to provide to define these mappings:

*   Specify [nodes to export](#nodes-to-export).
*   [Declare extra node labels](#declare-extra-node-labels), for labels to appear as source/target nodes of relationships but not exported as nodes.
*   Specify [relationships to export](#relationships-to-export).

In addition, the same node may appear multiple times, from exported nodes and various relationships.
They should appear as the same node in the target graph database.
CocoIndex automatically [matches and deduplicates nodes](#nodes-matching-and-deduplicating) based on their primary key values.

#### Nodes to Export

Here's how CocoIndex data elements map to nodes in the graph:

| CocoIndex Element | Graph Element |
|-------------------|------------------|
| an export target  | nodes with a unique label |
| a collected row   | a node |
| a field           | a property of node |

Note that the label used in different `Nodes`s should be unique.

`cocoindex.targets.Nodes` is to describe mapping to nodes. It has the following fields:

*   `label` (type: `str`): The label of the node.

For example, consider we have collected the following rows:

<small>

| filename | summary |
|----------|---------|
| chapter1.md | At the beginning, ... |
| chapter2.md | In the second day, ... |

</small>

We can export them to nodes under label `Document` like this:

```python
document_collector.export(
    ...
    cocoindex.targets.Neo4j(
        ...
        mapping=cocoindex.targets.Nodes(label="Document"),
    ),
    primary_key_fields=["filename"],
)
```

The collected rows will be mapped to nodes in knowledge database like this:

```mermaid
graph TD
  Doc_Chapter1@{
    shape: rounded
    label: "**[Document]**
            **filename\\*: chapter1.md**
            summary: At the beginning, ..."
    classDef: node
  }

  Doc_Chapter2@{
    shape: rounded
    label: "**[Document]**
            **filename\\*: chapter2.md**
            summary: In the second day, ..."
    classDef: node
  }

  classDef node font-size:8pt,text-align:left,stroke-width:2;
```

#### Declare Extra Node Labels

If a node label needs to appear as source or target of a relationship, but not exported as a node, you need to [declare](../core/flow_def#target-declarations) the label with necessary configuration.

The dataclass to describe the declaration is specific to each target (e.g. `cocoindex.targets.Neo4jDeclarations`),
while they share the following common fields:

*   `nodes_label` (required): The label of the node.
*   Options for [storage indexes](../core/flow_def#storage-indexes).
    *   `primary_key_fields` (required)
    *   `vector_indexes` (optional)

Continuing the same example above.
Considering we want to extract relationships from `Document` to `Place` later (i.e. a document mentions a place), but the `Place` label isn't exported as a node, we need to declare it:

```python
flow_builder.declare(
    cocoindex.targets.Neo4jDeclarations(
        connection = ...,
        nodes_label="Place",
        primary_key_fields=["name"],
    ),
)
```

#### Relationships to Export

Here's how CocoIndex data elements map to relationships in the graph:

| CocoIndex Element | Graph Element |
|-------------------|------------------|
| an export target  | relationships with a unique type |
| a collected row   | a relationship |
| a field           | a property of relationship, or a property of source/target node, based on configuration |

Note that the type used in different `Relationships`s should be unique.

`cocoindex.targets.Relationships` is to describe mapping to relationships. It has the following fields:

*   `rel_type` (type: `str`): The type of the relationship.
*   `source`/`target` (type: `cocoindex.targets.NodeFromFields`): Specify how to extract source/target node information from specific fields in the collected row. It has the following fields:
    *   `label` (type: `str`): The label of the node.
    *   `fields` (type: `Sequence[cocoindex.targets.TargetFieldMapping]`): Specify field mappings from the collected rows to node properties, with the following fields:
        *   `source` (type: `str`): The name of the field in the collected row.
        *   `target` (type: `str`, optional): The name of the field to use as the node field. If unspecified, will use the same as `source`.

        :::note Map necessary fields for nodes of relationships

        You need to map the following fields for nodes of each relationship:

        *   Make sure all primary key fields for the label are mapped.
        *   Optionally, you can also map non-key fields. If you do so, please make sure all value fields are mapped.

        :::

All fields in the collector that are not used in mappings for source or target node fields will be mapped to relationship properties.

For example, consider we have collected the following rows, to describe places mentioned in each file, along with embeddings of the places:

<small>

| doc_filename | place_name | place_embedding | location |
|----------|-------|-----------------|-----------------|
| chapter1.md | Crystal Palace | [0.1, 0.5, ...] | 12 |
| chapter2.md | Magic Forest | [0.4, 0.2, ...] | 23 |
| chapter2.md | Crystal Palace | [0.1, 0.5, ...] | 56 |

</small>

We can export them to relationships under type `MENTION` like this:

```python
doc_place_collector.export(
    ...
    cocoindex.targets.Neo4j(
        ...
        mapping=cocoindex.targets.Relationships(
            rel_type="MENTION",
            source=cocoindex.targets.NodeFromFields(
                label="Document",
                fields=[cocoindex.targets.TargetFieldMapping(source="doc_filename", target="filename")],
            ),
            target=cocoindex.targets.NodeFromFields(
                label="Place",
                fields=[
                    cocoindex.targets.TargetFieldMapping(source="place_name", target="name"),
                    cocoindex.targets.TargetFieldMapping(source="place_embedding", target="embedding"),
                ],
            ),
        ),
    ),
    ...
)
```

The `doc_filename` field is mapped to `Document.filename` property for the source node, while `place_name` and `place_embedding` are mapped to `Place.name` and `Place.embedding` properties for the target node.
The remaining field `location` becomes a property of the relationship.
For the data above, we get a bunch of relationships like this:

```mermaid
graph TD
  Doc_Chapter1@{
    shape: rounded
    label: "**[Document]**
            **filename\\*: chapter1.md**"
    classDef: nodeRef
  }

  Doc_Chapter2_a@{
    shape: rounded
    label: "**[Document]**
            **filename\\*: chapter2.md**"
    classDef: nodeRef
  }

  Doc_Chapter2_b@{
    shape: rounded
    label: "**[Document]**
            **filename\\*: chapter2.md**"
    classDef: nodeRef
  }

  Place_CrystalPalace_a@{
    shape: rounded
    label: "**[Place]**
            **name\\*: Crystal Palace**
            embedding: [0.1, 0.5, ...]"
    classDef: node
  }

  Place_MagicForest@{
    shape: rounded
    label: "**[Place]**
            **name\\*: Magic Forest**
            embedding: [0.4, 0.2, ...]"
    classDef: node
  }

  Place_CrystalPalace_b@{
    shape: rounded
    label: "**[Place]**
            **name\\*: Crystal Palace**
            embedding: [0.1, 0.5, ...]"
    classDef: node
  }


  Doc_Chapter1:::nodeRef -- **:MENTION** (location:12) --> Place_CrystalPalace_a:::node
  Doc_Chapter2_a:::nodeRef -- **:MENTION** (location:23) --> Place_MagicForest:::node
  Doc_Chapter2_b:::nodeRef -- **:MENTION** (location:56) --> Place_CrystalPalace_b:::node

  classDef nodeRef font-size:8pt,text-align:left,fill:transparent,stroke-width:1,stroke-dasharray:5 5;
  classDef node font-size:8pt,text-align:left,stroke-width:2;

```

#### Nodes Matching and Deduplicating

The nodes and relationships we got above are discrete elements.
To fit them into a connected property graph, CocoIndex will match and deduplicate nodes automatically:

*   Match nodes based on their primary key values. Nodes with the same primary key values are considered as the same node.
*   For non-primary key fields (a.k.a. value fields), CocoIndex will pick the values from an arbitrary one.
    If multiple nodes (before deduplication) with the same primary key provide value fields, an arbitrary one will be picked.

:::note

The best practice is to make the value fields consistent across different appearances of the same node, to avoid non-determinism in the exported graph.

:::

After matching and deduplication, we get the final graph:

```mermaid
graph TD
  Doc_Chapter1@{
    shape: rounded
    label: "**[Document]**
            **filename\\*: chapter1.md**
            summary: At the beginning, ..."
    classDef: node
  }

  Doc_Chapter2@{
    shape: rounded
    label: "**[Document]**
            **filename\\*: chapter2.md**
            summary: In the second day, ..."
    classDef: node
  }

  Place_CrystalPalace@{
    shape: rounded
    label: "**[Place]**
            **name\\*: Crystal Palace**
            embedding: [0.1, 0.5, ...]"
    classDef: node
  }

  Place_MagicForest@{
    shape: rounded
    label: "**[Place]**
            **name\\*: Magic Forest**
            embedding: [0.4, 0.2, ...]"
    classDef: node
  }

  Doc_Chapter1:::node -- **:MENTION** (location:12) --> Place_CrystalPalace:::node
  Doc_Chapter2:::node -- **:MENTION** (location:23) --> Place_MagicForest:::node
  Doc_Chapter2:::node -- **:MENTION** (location:56) --> Place_CrystalPalace:::node

  classDef node font-size:8pt,text-align:left,stroke-width:2;
```

#### Examples

You can find end-to-end examples fitting into any of supported property graphs in the following directories:
*   [examples/docs_to_knowledge_graph](https://github.com/cocoindex-io/cocoindex/tree/main/examples/docs_to_knowledge_graph)
*   [examples/product_recommendation](https://github.com/cocoindex-io/cocoindex/tree/main/examples/product_recommendation)

### Neo4j

#### Spec

The `Neo4j` target spec takes the following fields:

*   `connection` (type: [auth reference](../core/flow_def#auth-registry) to `Neo4jConnectionSpec`): The connection to the Neo4j database. `Neo4jConnectionSpec` has the following fields:
    *   `url` (type: `str`): The URI of the Neo4j database to use as the internal storage, e.g. `bolt://localhost:7687`.
    *   `user` (type: `str`): Username for the Neo4j database.
    *   `password` (type: `str`): Password for the Neo4j database.
    *   `db` (type: `str`, optional): The name of the Neo4j database to use as the internal storage, e.g. `neo4j`.
*   `mapping` (type: `Nodes | Relationships`): The mapping from collected row to nodes or relationships of the graph. For either [nodes to export](#nodes-to-export) or [relationships to export](#relationships-to-export).

Neo4j also provides a declaration spec `Neo4jDeclaration`, to configure indexing options for nodes only referenced by relationships. It has the following fields:

*   `connection` (type: auth reference to `Neo4jConnectionSpec`)
*   Fields for [nodes to declare](#declare-extra-node-labels), including
    *   `nodes_label` (required)
    *   `primary_key_fields` (required)
    *   `vector_indexes` (optional)

#### Neo4j dev instance

If you don't have a Neo4j database, you can start a Neo4j database using our docker compose config:

```bash
docker compose -f <(curl -L https://raw.githubusercontent.com/cocoindex-io/cocoindex/refs/heads/main/dev/neo4j.yaml) up -d
```

If will bring up a Neo4j instance, which can be accessed by username `neo4j` and password `cocoindex`.
You can access the Neo4j browser at [http://localhost:7474](http://localhost:7474).

:::warning

The docker compose config above will start a Neo4j Enterprise instance under the [Evaluation License](https://neo4j.com/terms/enterprise_us/),
with 30 days trial period.
Please read and agree the license before starting the instance.

:::


### Kuzu

#### Spec

CocoIndex supports talking to Kuzu through its [API server](https://github.com/kuzudb/api-server).

The `Kuzu` target spec takes the following fields:

*   `connection` (type: [auth reference](../core/flow_def#auth-registry) to `KuzuConnectionSpec`): The connection to the Kuzu database. `KuzuConnectionSpec` has the following fields:
    *   `api_server_url` (type: `str`): The URL of the Kuzu API server, e.g. `http://localhost:8123`.
*   `mapping` (type: `Nodes | Relationships`): The mapping from collected row to nodes or relationships of the graph. For either [nodes to export](#nodes-to-export) or [relationships to export](#relationships-to-export).

Kuzu also provides a declaration spec `KuzuDeclaration`, to configure indexing options for nodes only referenced by relationships. It has the following fields:

*   `connection` (type: auth reference to `KuzuConnectionSpec`)
*   Fields for [nodes to declare](#declare-extra-node-labels), including
    *   `nodes_label` (required)
    *   `primary_key_fields` (required)

#### Kuzu dev instance

If you don't have a Kuzu instance yet, you can bring up a Kuzu API server locally by running:

```bash
KUZU_DB_DIR=$HOME/.kuzudb
KUZU_PORT=8123
docker run -d --name kuzu -p ${KUZU_PORT}:8000 -v ${KUZU_DB_DIR}:/database kuzudb/api-server:latest
```

To explore the graph you built with Kuzu, you can use the [Kuzu Explorer](https://github.com/kuzudb/explorer).
Currently Kuzu API server and the explorer cannot be up at the same time. So you need to stop the API server before running the explorer.

To start the instance of the explorer, run:

```bash
KUZU_EXPLORER_PORT=8124
docker run -d --name kuzu-explorer -p ${KUZU_EXPLORER_PORT}:8000  -v ${KUZU_DB_DIR}:/database -e MODE=READ_ONLY  kuzudb/explorer:latest
```

You can then access the explorer at [http://localhost:8124](http://localhost:8124).
