---
title: Storages
description: CocoIndex Built-in Storages
toc_max_heading_level: 4
---

# CocoIndex Built-in Storages

For each target storage, data are exported from a data collector, containing data of multiple entries, each with multiple fields.
The way to map data from a data collector to a target storage depends on data model of the target storage.

## Entry-Oriented Targets

Entry-Oriented Storage organizes data into independent entries, such as rows, key-value pairs, or documents.
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

*   `database_url` (type: `str`, optional): The URL of the Postgres database to use as the internal storage, e.g. `postgres://cocoindex:cocoindex@localhost/cocoindex`. If unspecified, will use the same database as the [internal storage](/docs/core/basics#internal-storage).

*   `table_name` (type: `str`, optional): The name of the table to store to. If unspecified, will generate a new automatically. We recommend specifying a name explicitly if you want to directly query the table. It can be omitted if you want to use CocoIndex's query handlers to query the table.

### Qdrant

Exports data to a [Qdrant](https://qdrant.tech/) collection.

#### Data Mapping

Here's how CocoIndex data elements map to Qdrant elements during export:

| CocoIndex Element | Qdrant Element |
|-------------------|------------------|
| an export target  | a unique collection |  
| a collected row   | a point |
| a field           | a named vector (for fields with vector type); a field within payload (otherwise) |

#### Spec

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

## Property Graph Targets

Property graph is a graph data model where both nodes and relationships can have properties.

### Data Mapping

In CocoIndex, you can export data to property graph databases.
This usually involves more than one collectors, and you export them to different types of graph elements (nodes and relationships).
In particular,

1.  You can export rows from some collectors to nodes in the graph.
2.  You can export rows from some other collectors to relationships in the graph.
3.  Some nodes referenced by relationships exported in 2 may not exist as nodes exported in 1.
    CocoIndex will automatically create and keep these nodes, as long as they're still referenced by at least one relationship.
    This guarantees that all relationships exported in 2 are valid.

We provide common types `NodeMapping`, `RelationshipMapping`, and `ReferencedNode`, to configure for each situation.
They're agnostic to specific graph databases.

#### Nodes

Here's how CocoIndex data elements map to nodes in the graph:

| CocoIndex Element | Graph Element |
|-------------------|------------------|
| an export target  | nodes with a unique label |
| a collected row   | a node |
| a field           | a property of node |

Note that the label used in different `NodeMapping`s should be unique.

`cocoindex.storages.NodeMapping` is to describe mapping to nodes. It has the following fields:

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
    cocoindex.storages.Neo4j(
        ...
        mapping=cocoindex.storages.NodeMapping(label="Document"),
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

#### Relationships

Here's how CocoIndex data elements map to relationships in the graph:

| CocoIndex Element | Graph Element |
|-------------------|------------------|
| an export target  | relationships with a unique type |
| a collected row   | a relationship |
| a field           | a property of relationship, or a property of source/target node, based on configuration |

Note that the type used in different `RelationshipMapping`s should be unique.

`cocoindex.storages.RelationshipMapping` is to describe mapping to relationships. It has the following fields:

*   `rel_type` (type: `str`): The type of the relationship.
*   `source`/`target` (type: `cocoindex.storages.NodeReferenceMapping`): Specify how to extract source/target node information from the collected row. It has the following fields:
    *   `label` (type: `str`): The label of the node.
    *   `fields` (type: `Sequence[cocoindex.storages.TargetFieldMapping]`): Specify field mappings from the collected rows to node properties, with the following fields:
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
    cocoindex.storages.Neo4j(
        ...
        mapping=cocoindex.storages.RelationshipMapping(
            rel_type="MENTION",
            source=cocoindex.storages.NodeReferenceMapping(
                label="Document",
                fields=[cocoindex.storages.TargetFieldMapping(source="doc_filename", target="filename")],
            ),
            target=cocoindex.storages.NodeReferenceMapping(
                label="Place",
                fields=[
                    cocoindex.storages.TargetFieldMapping(source="place_name", target="name"),
                    cocoindex.storages.TargetFieldMapping(source="place_embedding", target="embedding"),
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

  Doc_Chapter2@{
    shape: rounded
    label: "**[Document]**
            **filename\\*: chapter2.md**"
    classDef: nodeRef
  }

  Place_CrystalPalace@{
    shape: rounded
    label: "**[Place]**
            **name\\*: Crystal Palace**
            embedding: [0.1, 0.5, ...]"
    classDef: nodeRef
  }

  Place_MagicForest@{
    shape: rounded
    label: "**[Place]**
            **name\\*: Magic Forest**
            embedding: [0.4, 0.2, ...]"
    classDef: nodeRef
  }

  Doc_Chapter1:::nodeRef -- **[MENTION]**{location:12} --> Place_CrystalPalace:::nodeRef
  Doc_Chapter2:::nodeRef -- **[MENTION]**{location:23} --> Place_MagicForest:::nodeRef
  Doc_Chapter2:::nodeRef -- **[MENTION]**{location:56} --> Place_CrystalPalace:::nodeRef

  classDef nodeRef font-size:8pt,text-align:left,fill:transparent,stroke-width:1,stroke-dasharray:5 5;

```


#### Nodes only referenced by relationships

If a node appears as source or target of a relationship, but not exported using `NodeMapping`, CocoIndex will automatically create and keep these nodes until they're no longer referenced by any relationships.

:::note Merge of node values

If the same node (as identified by primary key values) appears multiple times (e.g. they're referenced by different relationships),
CocoIndex uses value fields provided by an arbitrary one of them.
The best practice is to make the value fields consistent across different appearances of the same node, to avoid non-determinism in the exported graph.

:::

If a node's label specified in `NodeReferenceMapping` doesn't exist in any `NodeMapping`, you need to [declare](../core/flow_def#target-declarations) a `ReferencedNode` to configure [storage indexes](../core/flow_def#storage-indexes) for nodes with this label.
The following options are supported:

*   `primary_key_fields` (required)
*   `vector_indexes` (optional)

Using the same example above.
After combining exported nodes and relationships, we get the knowledge graph with all information:

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
    classDef: nodeRef
  }

  Place_MagicForest@{
    shape: rounded
    label: "**[Place]**
            **name\\*: Magic Forest**
            embedding: [0.4, 0.2, ...]"
    classDef: nodeRef
  }

  Doc_Chapter1:::node -- **[MENTION]**{location:12} --> Place_CrystalPalace:::nodeRef
  Doc_Chapter2:::node -- **[MENTION]**{location:23} --> Place_MagicForest:::nodeRef
  Doc_Chapter2:::node -- **[MENTION]**{location:56} --> Place_CrystalPalace:::nodeRef

  classDef node font-size:8pt,text-align:left,stroke-width:2;
  classDef nodeRef font-size:8pt,text-align:left,fill:transparent,stroke-width:1,stroke-dasharray:5 5;

```

Nodes with `Place` label in the example aren't exported explicitly using `NodeMapping`, so CocoIndex will automatically create them as long as they're still referenced by any relationship.
You need to declare a `ReferencedNode`:

```python
flow_builder.declare(
    cocoindex.storages.Neo4jDeclarations(
        ...
        referenced_nodes=[
            cocoindex.storages.ReferencedNode(label="Place", primary_key_fields=["name"]),
        ],
    ),
)
```

### Neo4j

If you don't have a Neo4j database, you can start a Neo4j database using our docker compose config:

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
*   `mapping` (type: `NodeMapping | RelationshipMapping`): The mapping from collected row to nodes or relationships of the graph. 2 variations are supported:

Neo4j also provides a declaration spec `Neo4jDeclaration`, to configure indexing options for nodes only referenced by relationships. It has the following fields:

*   `connection` (type: auth reference to `Neo4jConnectionSpec`)
*   `relationships` (type: `Sequence[ReferencedNode]`)

You can find an end-to-end example [here](https://github.com/cocoindex-io/cocoindex/tree/main/examples/docs_to_knowledge_graph).
