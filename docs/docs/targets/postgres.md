---
title: Postgres
description: CocoIndex Postgres Target
toc_max_heading_level: 4
---

import { ExampleButton } from '../../src/components/GitHubButton';

# Postgres

Exports data to Postgres database (with pgvector extension).

## Data Mapping

Here's how CocoIndex data elements map to Postgres elements during export:

| CocoIndex Element | Postgres Element |
|-------------------|------------------|
| an export target | a unique table |
| a collected row | a row |
| a field | a column |

For example, if you have a data collector that collects rows with fields `id`, `title`, and `embedding`, it will be exported to a Postgres table with corresponding columns.
It should be a unique table, meaning that no other export target should export to the same table.

:::warning vector type mapping to Postgres

Since vectors in pgvector must have fixed dimension, we only map vectors of number types with fixed dimension (i.e. *Vector[cocoindex.Float32, N]*, *Vector[cocoindex.Float64, N]*, and *Vector[cocoindex.Int64, N]*) to `vector(N)` columns.
For all other vector types, we map them to `jsonb` columns.

:::

:::info U+0000 (NUL) characters in strings

U+0000 (NUL) is a valid character in Unicode, but Postgres has a limitation that strings (including `text`-like types and strings in `jsonb`) cannot contain them.
CocoIndex automatically strips U+0000 (NUL) characters from strings before exporting to Postgres. For example, if you have a string `"Hello\0World"`, it will be exported as `"HelloWorld"`.

:::

## Spec

The spec takes the following fields:

*   `database` ([auth reference](/docs/core/flow_def#auth-registry) to `DatabaseConnectionSpec`, optional): The connection to the Postgres database.
    See [DatabaseConnectionSpec](/docs/core/settings#databaseconnectionspec) for its specific fields.
    If not provided, will use the same database as the [internal storage](/docs/core/basics#internal-storage).

*   `table_name` (`str`, optional): The name of the table to store to. If unspecified, will use the table name `[${AppNamespace}__]${FlowName}__${TargetName}`, e.g. `DemoFlow__doc_embeddings` or `Staging__DemoFlow__doc_embeddings`.

*   `schema` (`str`, optional): The PostgreSQL schema to create the table in. If unspecified, the table will be created in the default schema (usually `public`). When specified, `table_name` must also be explicitly specified. CocoIndex will automatically create the schema if it doesn't exist.

## Example
<ExampleButton
  href="https://github.com/cocoindex-io/cocoindex/tree/main/examples/text_embedding"
  text="Text Embedding Example with Postgres"
  margin="16px 0 24px 0"
/>
