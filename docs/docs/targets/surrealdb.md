---
title: SurrealDB
description: CocoIndex SurrealDB Target
toc_max_heading_level: 4
---

# SurrealDB

Exports data to a [SurrealDB](https://surrealdb.com/) database.

This target supports:
- **Vector search** via SurrealDB **HNSW** indexes
- **Property graph** exports (nodes + relationships) via SurrealDB **relation tables**

## Get Started

Read [Property Graph Targets](./index.md#property-graph-targets) for how `Nodes`, `Relationships`, and declarations work in CocoIndex.

## Spec

The `SurrealDB` target spec takes the following fields:

* `connection` ([auth reference](/docs/core/flow_def#auth-registry) to `SurrealDBConnection`, required): Connection to SurrealDB. `SurrealDBConnection` has:
  * `url` (`str`): WebSocket RPC url, e.g. `ws://localhost:8000/rpc`
  * `namespace` (`str`): SurrealDB namespace
  * `database` (`str`): SurrealDB database
  * `username` (`str`): Root username
  * `password` (`str`): Root password
* `mapping` (`Nodes | Relationships`): Map collector rows to graph nodes or relationships.

SurrealDB also provides a declaration spec `SurrealDBDeclaration`, to configure indexing options for nodes only referenced by relationships. It has:

* `connection` (auth reference to `SurrealDBConnection`)
* Fields for [nodes to declare](./index.md#declare-extra-node-labels), including:
  * `nodes_label` (required)
  * `primary_key_fields` (required)
  * `vector_indexes` (optional)

## Index support

- **Vector indexes**: HNSW only (`VectorIndexMethod.Hnsw`). Supported metrics: **CosineSimilarity** and **L2Distance**.
- **FTS indexes**: not supported yet.

Note: SurrealDB identifiers are generated from labels/field names by replacing non-alphanumeric characters with `_`. Use simple labels like `Document` / `MENTION` to avoid surprises.

## Run a local SurrealDB

In-memory:

```sh
surreal start -u root -p root
```

Persistent (RocksDB):

```sh
surreal start -u root -p root rocksdb:database
```
