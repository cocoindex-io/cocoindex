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

| Target   | Link |
|----------|------|
| Postgres | [Postgres](./targets/entry-oriented/postgres) |
| Qdrant   | [Qdrant](./targets/entry-oriented/qdrant)     |
| LanceDB  | [LanceDB](./targets/entry-oriented/lancedb)   |


## Property Graph Targets

Property graph is a widely-adopted model for knowledge graphs, where both nodes and relationships can have properties.
[Graph database concepts](https://neo4j.com/docs/getting-started/appendix/graphdb-concepts/) has a good introduction to basic concepts of property graphs.


| Target   | Link |
|----------|------|
| Neo4j | [Neo4j](./targets/property-graph/neo4j) |
| Kuzu   | [Kuzu](./targets/property-graph/kuzu)     |

