---
title: Kuzu
description: CocoIndex Kuzu Target
toc_max_heading_level: 4
---
import { ExampleButton } from '../../src/components/GitHubButton';

# Kuzu (Archived)

Note:[Kuzu](https://github.com/kuzudb/kuzu) - embedded graph database is no longer maintained.

Exports data to a [Kuzu](https://kuzu.com/) graph database.

## Get Started

Read [Property Graph Targets](./index.md#property-graph-targets) for more information to get started on how it works in CocoIndex.

## Spec

CocoIndex supports talking to Kuzu through its [API server](https://github.com/kuzudb/api-server).

The `Kuzu` target spec takes the following fields:

*   `connection` ([auth reference](/docs/core/flow_def#auth-registry) to `KuzuConnectionSpec`): The connection to the Kuzu database. `KuzuConnectionSpec` has the following fields:
    *   `api_server_url` (`str`): The URL of the Kuzu API server, e.g. `http://localhost:8123`.
*   `mapping` (`Nodes | Relationships`): The mapping from collected row to nodes or relationships of the graph. For either [nodes to export](./index.md#nodes-to-export) or [relationships to export](./index.md#relationships-to-export).

Kuzu also provides a declaration spec `KuzuDeclaration`, to configure indexing options for nodes only referenced by relationships. It has the following fields:

*   `connection` (auth reference to `KuzuConnectionSpec`)
*   Fields for [nodes to declare](./index.md#declare-extra-node-labels), including
    *   `nodes_label` (required)
    *   `primary_key_fields` (required)

## Kuzu dev instance

If you don't have a Kuzu instance yet, you can bring up a Kuzu API server locally by running:

```sh
KUZU_DB_DIR=$HOME/.kuzudb
KUZU_PORT=8123
docker run -d --name kuzu -p ${KUZU_PORT}:8000 -v ${KUZU_DB_DIR}:/database kuzudb/api-server:latest
```

To explore the graph you built with Kuzu, you can use the [Kuzu Explorer](https://github.com/kuzudb/explorer).
Currently Kuzu API server and the explorer cannot be up at the same time. So you need to stop the API server before running the explorer.

To start the instance of the explorer, run:

```sh
KUZU_EXPLORER_PORT=8124
docker run -d --name kuzu-explorer -p ${KUZU_EXPLORER_PORT}:8000  -v ${KUZU_DB_DIR}:/database -e MODE=READ_ONLY  kuzudb/explorer:latest
```

You can then access the explorer at [http://localhost:8124](http://localhost:8124).

## Example
<ExampleButton
  href="https://github.com/cocoindex-io/cocoindex/tree/main/examples/docs_to_knowledge_graph"
  text="Docs to Knowledge Graph"
  margin="16px 0 24px 0"
/>
