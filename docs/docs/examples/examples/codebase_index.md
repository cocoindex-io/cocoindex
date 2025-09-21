---
title: Real-time Codebase Indexing
description: Build a real-time codebase index for retrieval-augmented generation (RAG) using CocoIndex and Tree-sitter. Chunk, embed, and search code with semantic understanding.
sidebar_class_name: hidden
slug: /examples/code_index
canonicalUrl: '/examples/code_index'
sidebar_custom_props:
  image: /img/examples/codebase_index/cover.png
  tags: [vector-index, codebase]
image: /img/examples/codebase_index/cover.png
tags: [vector-index, codebase]
---

import { GitHubButton, YouTubeButton, DocumentationButton } from '../../../src/components/GitHubButton';

<GitHubButton url="https://github.com/cocoindex-io/cocoindex/tree/main/examples/code_embedding" margin="0 0 24px 0" />
<YouTubeButton url="https://youtu.be/G3WstvhHO24?si=ndYfM0XRs03_hVPR" margin="0 0 24px 0" />

![Codebase Index](/img/examples/codebase_index/cover.png)

## Overview
In this tutorial, we will build codebase index. [CocoIndex](https://github.com/cocoindex-io/cocoindex) provides built-in support for codebase chunking, with native Tree-sitter support. It works with large codebases, and can be updated in near real-time with incremental processing - only reprocess what's changed.

## Use Cases
A wide range of applications can be built with an effective codebase index that is always up-to-date.

- Semantic code context for AI coding agents like Claude, Codex, Gemini CLI.
- MCP for code editors such as Cursor, Windsurf, and VSCode.
- Context-aware code search applications—semantic code search, natural language code retrieval.
- Context for code review agents—AI code review, automated code analysis, code quality checks, pull request summarization.
- Automated code refactoring, large-scale code migration.
- SRE workflows: enable rapid root cause analysis, incident response, and change impact assessment by indexing infrastructure-as-code, deployment scripts, and config files for semantic search and lineage tracking.
- Automatically generate design documentation from code—keep design docs up-to-date.

## Flow Overview

![Flow Overview](/img/examples/codebase_index/flow.png)

The flow is composed of the following steps:

- Read code files from the local filesystem
- Extract file extensions, to get the language of the code for Tree-sitter to parse
- Split code into semantic chunks using Tree-sitter
- Generate embeddings for each chunk
- Store in a vector database for retrieval

## Setup
- Install Postgres, follow [installation guide](https://cocoindex.io/docs/getting_started/installation#-install-postgres).
- Install CocoIndex
  ```bash
  pip install -U cocoindex
  ```

## Add the codebase as a source.
We will index the CocoIndex codebase. Here we use the `LocalFile` source to ingest files from the CocoIndex codebase root directory.

```python
@cocoindex.flow_def(name="CodeEmbedding")
def code_embedding_flow(flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope):
    data_scope["files"] = flow_builder.add_source(
        cocoindex.sources.LocalFile(path="../..",
                                    included_patterns=["*.py", "*.rs", "*.toml", "*.md", "*.mdx"],
                                    excluded_patterns=[".*", "target", "**/node_modules"]))
    code_embeddings = data_scope.add_collector()
```

- Include files with the extensions of `.py`, `.rs`, `.toml`, `.md`, `.mdx`
- Exclude files and directories starting `.`,  `target` in the root and `node_modules` under any directory.

`flow_builder.add_source` will create a table with sub fields (`filename`, `content`).
<DocumentationButton url="https://cocoindex.io/docs/ops/sources" text="Sources" />


## Process each file and collect the information

### Extract the extension of a filename

We need to pass the language (or extension) to Tree-sitter to parse the code.
Let's define a function to extract the extension of a filename while processing each file.

```python
@cocoindex.op.function()
def extract_extension(filename: str) -> str:
    """Extract the extension of a filename."""
    return os.path.splitext(filename)[1]
```

<DocumentationButton url="https://cocoindex.io/docs/custom_ops/custom_functions" text="Custom Function" margin="0 0 16px 0" />

### Split the file into chunks
We use the `SplitRecursively` function to split the file into chunks.  `SplitRecursively` is CocoIndex building block, with native integration with Tree-sitter. You need to pass in the language to the `language` parameter if you are processing code.

```python
with data_scope["files"].row() as file:
    # Extract the extension of the filename.
    file["extension"] = file["filename"].transform(extract_extension)
    file["chunks"] = file["content"].transform(
          cocoindex.functions.SplitRecursively(),
          language=file["extension"], chunk_size=1000, chunk_overlap=300)
```
<DocumentationButton url="https://cocoindex.io/docs/ops/functions#splitrecursively" text="SplitRecursively" margin="0 0 16px 0" />

![SplitRecursively](/img/examples/codebase_index/chunk.png)

### Embed the chunks
We use `SentenceTransformerEmbed` to embed the chunks.

```python
@cocoindex.transform_flow()
def code_to_embedding(text: cocoindex.DataSlice[str]) -> cocoindex.DataSlice[list[float]]:
    return text.transform(
        cocoindex.functions.SentenceTransformerEmbed(
            model="sentence-transformers/all-MiniLM-L6-v2"))
```

<DocumentationButton url="https://cocoindex.io/docs/ops/functions#sentencetransformerembed" text="SentenceTransformerEmbed" margin="0 0 16px 0" />

:::tip
`@cocoindex.transform_flow()` is needed to share the transformation across indexing and query. When building a vector index and querying against it, the embedding computation must remain consistent between indexing and querying.
:::

<DocumentationButton url="https://cocoindex.io/docs/query#transform-flow" text="Transform Flow" margin="0 0 16px 0" />

Then for each chunk, we will embed it using the `code_to_embedding` function, and collect the embeddings to the `code_embeddings` collector.

```python
with data_scope["files"].row() as file:
    with file["chunks"].row() as chunk:
        chunk["embedding"] = chunk["text"].call(code_to_embedding)
        code_embeddings.collect(filename=file["filename"], location=chunk["location"],
                                code=chunk["text"], embedding=chunk["embedding"])
```

### Export the embeddings

```python
code_embeddings.export(
    "code_embeddings",
    cocoindex.storages.Postgres(),
    primary_key_fields=["filename", "location"],
    vector_indexes=[cocoindex.VectorIndex("embedding", cocoindex.VectorSimilarityMetric.COSINE_SIMILARITY)])
```

We use [Cosine Similarity](https://en.wikipedia.org/wiki/Cosine_similarity) to measure the similarity between the query and the indexed data.

## Query the index
We match against user-provided text by a SQL query, reusing the embedding operation in the indexing flow.

```python
def search(pool: ConnectionPool, query: str, top_k: int = 5):
    # Get the table name, for the export target in the code_embedding_flow above.
    table_name = cocoindex.utils.get_target_storage_default_name(code_embedding_flow, "code_embeddings")
    # Evaluate the transform flow defined above with the input query, to get the embedding.
    query_vector = code_to_embedding.eval(query)
    # Run the query and get the results.
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT filename, code, embedding <=> %s::vector AS distance
                FROM {table_name} ORDER BY distance LIMIT %s
            """, (query_vector, top_k))
            return [
                {"filename": row[0], "code": row[1], "score": 1.0 - row[2]}
                for row in cur.fetchall()
            ]
```

Define a main function to run the query in terminal.

```python
def main():
    # Initialize the database connection pool.
    pool = ConnectionPool(os.getenv("COCOINDEX_DATABASE_URL"))
    # Run queries in a loop to demonstrate the query capabilities.
    while True:
        try:
            query = input("Enter search query (or Enter to quit): ")
            if query == '':
                break
            # Run the query function with the database connection pool and the query.
            results = search(pool, query)
            print("\nSearch results:")
            for result in results:
                print(f"[{result['score']:.3f}] {result['filename']}")
                print(f"    {result['code']}")
                print("---")
            print()
        except KeyboardInterrupt:
            break

if __name__ == "__main__":
    main()
```

## Run the index setup & update

- Install dependencies
    ```bash
    pip install -e .
    ```

- Setup and update the index
    ```sh
    cocoindex update --setup main.py
    ```
    You'll see the index updates state in the terminal


## Test the query
At this point, you can start the CocoIndex server and develop your RAG runtime against the data. To test your index, you could

``` bash
python main.py
```

When you see the prompt, you can enter your search query. for example: spec.
The returned results - each entry contains score (Cosine Similarity), filename, and the code snippet that get matched.

## CocoInsight
To get a better understanding of the indexing flow, you can use CocoInsight to help the development step by step.
To spin up, it is super easy.

```
cocoindex server main.py -ci
```
Follow the url from the terminal - `https://cocoindex.io/cocoinsight` to access the CocoInsight.


## Supported Languages

SplitRecursively has native support for all major programming languages.

<DocumentationButton url="https://cocoindex.io/docs/ops/functions#supported-languages" text="Supported Languages" margin="0 0 16px 0" />
