---
title: Real-time Codebase Indexing
description: Build a real-time codebase index for retrieval-augmented generation (RAG) using CocoIndex and Tree-sitter. Chunk, embed, and search code with semantic understanding.
sidebar_class_name: hidden
slug: /examples/code_index
canonicalUrl: '/examples/code_index'
---

import { GitHubButton, YouTubeButton } from '../../../src/components/GitHubButton';

<GitHubButton url="https://github.com/cocoindex-io/cocoindex/tree/main/examples/code_embedding"/>
<YouTubeButton url="https://youtu.be/G3WstvhHO24?si=ndYfM0XRs03_hVPR" />

## Setup 

If you don't have Postgres installed, please follow [installation guide](https://cocoindex.io/docs/getting_started/installation).

## Add the codebase as a source. 

Ingest files from the CocoIndex codebase root directory.

```python
@cocoindex.flow_def(name="CodeEmbedding")
def code_embedding_flow(flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope):
    """
    Define an example flow that embeds files into a vector database.
    """
    data_scope["files"] = flow_builder.add_source(
        cocoindex.sources.LocalFile(path="../..",
                                    included_patterns=["*.py", "*.rs", "*.toml", "*.md", "*.mdx"],
                                    excluded_patterns=[".*", "target", "**/node_modules"]))
    code_embeddings = data_scope.add_collector()
```

- Include files with the extensions of `.py`, `.rs`, `.toml`, `.md`, `.mdx`
- Exclude files and directories starting `.`,  `target` in the root and `node_modules` under any directory.

`flow_builder.add_source` will create a table with sub fields (`filename`, `content`). 
See [documentation](https://cocoindex.io/docs/ops/sources) for more details.


## Process each file and collect the information.

###  Extract the extension of a filename

We need to pass the language (or extension) to Tree-sitter to parse the code.
Let's define a function to extract the extension of a filename while processing each file.
You can find the documentation for custom function [here](https://cocoindex.io/docs/core/custom_function).

```python
@cocoindex.op.function()
def extract_extension(filename: str) -> str:
    """Extract the extension of a filename."""
    return os.path.splitext(filename)[1]
```

Then we are going to process each file and collect the information.

```python
with data_scope["files"].row() as file:
    file["extension"] = file["filename"].transform(extract_extension)
```

Here we extract the extension of the filename and store it in the `extension` field.


### Split the file into chunks

We will chunk the code with Tree-sitter. 
We use the `SplitRecursively` function to split the file into chunks. 
It is integrated with Tree-sitter, so you can pass in the language to the `language` parameter.
To see all supported language names and extensions, see the documentation [here](https://cocoindex.io/docs/ops/functions#splitrecursively). All the major languages are supported, e.g., Python, Rust, JavaScript, TypeScript, Java, C++, etc. If it's unspecified or the specified language is not supported, it will be treated as plain text.

```python
with data_scope["files"].row() as file:
    file["chunks"] = file["content"].transform(
          cocoindex.functions.SplitRecursively(),
          language=file["extension"], chunk_size=1000, chunk_overlap=300) 
```


### Embed the chunks

We use `SentenceTransformerEmbed` to embed the chunks. 
You can refer to the documentation [here](https://cocoindex.io/docs/ops/functions#sentencetransformerembed). 

```python
@cocoindex.transform_flow()
def code_to_embedding(text: cocoindex.DataSlice[str]) -> cocoindex.DataSlice[list[float]]:
    """
    Embed the text using a SentenceTransformer model.
    """
    return text.transform(
        cocoindex.functions.SentenceTransformerEmbed(
            model="sentence-transformers/all-MiniLM-L6-v2"))
```

Then for each chunk, we will embed it using the `code_to_embedding` function. and collect the embeddings to the `code_embeddings` collector.

`@cocoindex.transform_flow()` is needed to share the transformation across indexing and query. We build a vector index and query against it, 
the embedding computation needs to be consistent between indexing and querying. See [documentation](https://cocoindex.io/docs/query#transform-flow) for more details.


```python
with data_scope["files"].row() as file:
    with file["chunks"].row() as chunk:
        chunk["embedding"] = chunk["text"].call(code_to_embedding)
        code_embeddings.collect(filename=file["filename"], location=chunk["location"],
                                code=chunk["text"], embedding=chunk["embedding"])
```


### 2.4 Collect the embeddings

Export the embeddings to a table.

```python
code_embeddings.export(
    "code_embeddings",
    cocoindex.storages.Postgres(),
    primary_key_fields=["filename", "location"],
    vector_indexes=[cocoindex.VectorIndex("embedding", cocoindex.VectorSimilarityMetric.COSINE_SIMILARITY)])
```

We use Consine Similarity to measure the similarity between the query and the indexed data. 
To learn more about Consine Similarity, see [Wiki](https://en.wikipedia.org/wiki/Cosine_similarity).

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

🎉 Now you are all set!

Run following command to setup and update the index.
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

You can find the search results in the terminal

The returned results - each entry contains score (Cosine Similarity), filename, and the code snippet that get matched.
