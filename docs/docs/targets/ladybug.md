---
title: Ladybug
description: CocoIndex Ladybug Target
toc_max_heading_level: 4
---
import { ExampleButton } from '../../src/components/GitHubButton';

# Ladybug

Exports data to a [Ladybug](https://github.com/LadybugDB/ladybug) graph database. Ladybug is a maintained fork of Kuzu that carries forth the original vision of Kuzu, with added functionality for the lakehouse ecosystem. Just like Kuzu, Ladybug follows the structurd property graph model and functions as a fast, embedded database with a permissive (MIT) license.

## Get Started

Read [Property Graph Targets](./index.md#property-graph-targets) for more information to get started on how it works in CocoIndex.

## Spec

CocoIndex supports talking to Ladybug through its API server.

The `Ladybug` target spec takes the following fields:

* `connection` ([auth reference](/docs/core/flow_def#auth-registry) to `LadybugConnectionSpec`): The connection to the Ladybug database. `LadybugConnectionSpec` has the following fields:
  * `api_server_url` (`str`): The URL of the Ladybug API server, e.g. `http://localhost:8123`.
* `mapping` (`Nodes | Relationships`): The mapping from collected row to nodes or relationships of the graph. For either [nodes to export](./index.md#nodes-to-export) or [relationships to export](./index.md#relationships-to-export).

Ladybug also provides a declaration spec `LadybugDeclaration`, to configure indexing options for nodes only referenced by relationships. It has the following fields:

* `connection` (auth reference to `LadybugConnectionSpec`)
* Fields for [nodes to declare](./index.md#declare-extra-node-labels), including
  * `nodes_label` (required)
  * `primary_key_fields` (required)

## Ladybug API server

For running the API server locally or in Docker, follow the instructions in the Ladybug documentation.

## Python client

If you want the Ladybug Python client, install it with:

```sh
pip install real_ladybug
```

## Example

<ExampleButton
  href="https://github.com/cocoindex-io/cocoindex/tree/main/examples/docs_to_knowledge_graph"
  text="Docs to Knowledge Graph"
  margin="16px 0 24px 0"
/>
