---
title: Basics
description: CocoIndex Basics
---

# CocoIndex Basics

An **index** is a collection of data stored in a way that is easy for retrieval.

CocoIndex is an ETL framework for building indexes from specified data sources, a.k.a. indexing. It also offers utilities for users to retrieve data from the indexes.

## Indexing Flow

An indexing flow extracts data from speicfied data sources, upon specified transformations, and puts the transformed data into specified storage for later retrieval.

An indexing flow has two aspects: data and operations on data.

### Data

An indexing flow involves source data and transformed data (either as an intermediate result or the final result to be put into storage). All data within the indexing flow has **schema** determined at flow definition time.

Each piece of data has a **data type**, falling into one of the following categories:

*   Basic type.
*   Struct type: a collection of **fields**, each with a name and a type.
*   Collection type: a collection of **rows**, each of which is a struct with specified schema. A collection type can be a table (which has a key field) or a list (ordered but without key field).

An indexing flow always has a top-level struct, containing all data within and managed by the flow.

See [Data Types](data_types) for more details about data types.

### Operations

An **operation** in an indexing flow defines a step in the flow. An operation is defined by:

*   **Action**, which defines the behavior of the operation, e.g. *import*, *transform*, *for each*, *collect* and *export*.
    See [Flow Definition](flow_def) for more details for each action.

*   Some actions (i.e. "import", "transform" and "export") require an **Operation Spec**, which describes the specific behavior of the operation, e.g. a source to import from, a function describing the transformation behavior, a storage to export to as an index.
    *   Each operation spec has a **operation type**, e.g. `LocalFile` (data source), `SplitRecursively` (function), `SentenceTransformerEmbed` (function), `Postgres` (storage).
    *   CocoIndex framework maintains a set of supported operation types. Users can also implement their own.

"import" and "transform" operations produce output data, whose data type is determined based on the operation spec and data types of input data (for "transform" operation only).

### Example

For the example shown in the [Quickstart](../getting_started/quickstart) section, the indexing flow is as follows:

![Flow Example](flow_example.svg)

This creates the following data for the indexing flow:

*   The `Localfile` source creates a `documents` field at the top level, with `filename` (key) and `content` sub fields.
*   A "for each" action works on each document, with the following transformations:
    *   The `SplitRecursively` function splits content into chunks, adds a `chunks` field into the current scope (each document), with `location` (key) and `text` sub fields.
    *   A "collect" action works on each chunk, with the following transformations:
        *   The `SentenceTransformerEmbed` function embeds the chunk into a vector space, adding a `embedding` field into the current scope (each chunk).

This shows schema and example data for the indexing flow:

![Data Example](data_example.svg)

### Life Cycle of an Indexing Flow

An indexing flow, once set up, maintains a long-lived relationship between source data and indexes. This means:

1. The indexes created by the flow remain available for querying at any time
2. When source data changes, the indexes are automatically updated to reflect those changes
3. CocoIndex intelligently manages these updates by:
   - Determining which parts of the index need to be recomputed
   - Reusing existing computations where possible
   - Only reprocessing the minimum necessary data

You can think of an indexing flow similar to formulas in a spreadsheet:

- In a spreadsheet, you define formulas that transform input cells into output cells
- When input values change, the spreadsheet automatically recalculates affected outputs
- You focus on defining the transformation logic, not managing updates

CocoIndex works the same way, but with more powerful capabilities:

- Instead of flat tables, CocoIndex models data in nested data structures, making it more natural to model complex data
- Instead of simple cell-level formulas, you have operations like "for each" to apply the same formula across rows without repeating yourself

This means when writing your flow operations, you can treat source data as if it were static - focusing purely on defining the transformation logic. CocoIndex takes care of maintaining the dynamic relationship between sources and indexes behind the scenes.

### Internal Storage

As an indexing flow is long-lived, it needs to store intermediate data to keep track of the states.
CocoIndex uses internal storage for this purpose.

Currently, CocoIndex uses Postgres database as the internal storage.
See [Initialization](initialization) for configuring its location, and `cocoindex setup` CLI command (see [CocoIndex CLI](cli)) creates tables for the internal storage.

## Retrieval

There are two ways to retrieve data from indexes built by an indexing flow:

*   Query the underlying index storage directly for maximum flexibility.
*   Use CocoIndex *query handlers* for a more convenient experience with built-in tooling support (e.g. CocoInsight) to understand query performance against the index.

Query handlers are tied to specific indexing flows. They accept query inputs, transform them by defined operations, and retrieve matching data from the index storage that was created by the flow.