---
title: Qdrant
description: CocoIndex Qdrant Target
toc_max_heading_level: 4
---

import { ExampleButton } from '../../src/components/GitHubButton';

# Qdrant

Exports data to a [Qdrant](https://qdrant.tech/) collection.

## Data Mapping

Here's how CocoIndex data elements map to Qdrant elements during export:

| CocoIndex Element | Qdrant Element |
|-------------------|------------------|
| an export target  | a unique collection |
| a collected row   | a point |
| a field           | a named vector, if fits into Qdrant vector; or a field within payload otherwise |

The following vector types fit into Qdrant vector:
*   One-dimensional vectors with fixed dimension, e.g. *Vector[Float32, N]*, *Vector[Float64, N]* and *Vector[Int64, N]*.
    We map them to [dense vectors](https://qdrant.tech/documentation/concepts/vectors/#dense-vectors).
*   Two-dimensional vectors whose inner layer is a one-dimensional vector with fixed dimension, e.g. *Vector[Vector[Float32, N]]*, *Vector[Vector[Int64, N]]*, *Vector[Vector[Float64, N]]*. The outer layer may or may not have a fixed dimension.
    We map them to [multivectors](https://qdrant.tech/documentation/concepts/vectors/#multivectors).


:::warning vector type mapping to Qdrant

Since vectors in Qdrant must have fixed dimension, we only map vectors of number types with fixed dimension to Qdrant vectors.
For all other vector types, we map to Qdrant payload as JSON arrays.

:::

## Spec

The spec takes the following fields:

*   `connection` ([auth reference](/docs/core/flow_def#auth-registry) to `QdrantConnection`, optional): The connection to the Qdrant instance. `QdrantConnection` has the following fields:
    *   `grpc_url` (`str`): The [gRPC URL](https://qdrant.tech/documentation/interfaces/#grpc-interface) of the Qdrant instance, e.g. `http://localhost:6334/`.
    *   `api_key` (`str`, optional). API key to authenticate requests with.

    If `connection` is not provided, will use local Qdrant instance at `http://localhost:6334/` by default.

*   `collection_name` (`str`, required): The name of the collection to export the data to.

You can find an end-to-end example [here](https://github.com/cocoindex-io/cocoindex/tree/main/examples/text_embedding_qdrant).

## Example
<ExampleButton
  href="https://github.com/cocoindex-io/cocoindex/tree/main/examples/text_embedding_qdrant"
  text="Text Embedding Qdrant Example"
  margin="16px 0 24px 0"
/>
