---
title: Neo4j
description: CocoIndex Neo4j Target
toc_max_heading_level: 4
---
import { ExampleButton } from '../../src/components/GitHubButton';

# Neo4j

**Exports data to a [Neo4j](https://neo4j.com/) graph database.**


## Get Started
Read [Property Graph Targets](./index.md#property-graph-targets) for more information to get started on how it works in CocoIndex.


## Spec

The `Neo4j` target spec takes the following fields:

*   `connection` ([auth reference](/docs/core/flow_def#auth-registry) to `Neo4jConnectionSpec`): The connection to the Neo4j database. `Neo4jConnectionSpec` has the following fields:
    *   `url` (`str`): The URI of the Neo4j database to use as the internal storage, e.g. `bolt://localhost:7687`.
    *   `user` (`str`): Username for the Neo4j database.
    *   `password` (`str`): Password for the Neo4j database.
    *   `db` (`str`, optional): The name of the Neo4j database to use as the internal storage, e.g. `neo4j`.
*   `mapping` (`Nodes | Relationships`): The mapping from collected row to nodes or relationships of the graph. For either [nodes to export](./index.md#nodes-to-export) or [relationships to export](./index.md#relationships-to-export).

Neo4j also provides a declaration spec `Neo4jDeclaration`, to configure indexing options for nodes only referenced by relationships. It has the following fields:

*   `connection` (auth reference to `Neo4jConnectionSpec`)
*   Fields for [nodes to declare](./index.md#declare-extra-node-labels), including
    *   `nodes_label` (required)
    *   `primary_key_fields` (required)
    *   `vector_indexes` (optional)

## Neo4j dev instance

If you don't have a Neo4j database, you can start a Neo4j database using our docker compose config:

```sh
docker compose -f <(curl -L https://raw.githubusercontent.com/cocoindex-io/cocoindex/refs/heads/main/dev/neo4j.yaml) up -d
```

This will bring up a Neo4j instance, which can be accessed by username `neo4j` and password `cocoindex`.
You can access the Neo4j browser at [http://localhost:7474](http://localhost:7474).

## Example
<ExampleButton
  href="https://github.com/cocoindex-io/cocoindex/tree/main/examples/docs_to_knowledge_graph"
  text="Docs to Knowledge Graph"
  margin="16px 0 24px 0"
/>

## Data Clean up between different projects
If you are building multiple CocoIndex flows from different projects to neo4j, we recommend you to

- bring up separate container for each flow if you are on community edition, or
- setup different databases within one container if you are on enterprise edition.

This way, you can clean up the data for each flow independently.

In case you need to clean up the data in the same database, you can do it manually by running `cocoindex drop <APP_TARGET>` from the project you want to clean up.
