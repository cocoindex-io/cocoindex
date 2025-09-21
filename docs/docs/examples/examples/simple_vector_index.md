---
title: Simple Vector Index with Text Embedding
description: Indexing text with CocoIndex and text embeddings, and query it with natural language.
sidebar_class_name: hidden
slug: /examples/simple_vector_index
canonicalUrl: '/examples/simple_vector_index'
sidebar_custom_props:
  image: /img/examples/simple_vector_index/cover.png
  tags: [vector-index]
image: /img/examples/simple_vector_index/cover.png
tags: [vector-index]
---

import { GitHubButton, YouTubeButton, DocumentationButton } from '../../../src/components/GitHubButton';

<GitHubButton url="https://github.com/cocoindex-io/cocoindex/tree/main/examples/text_embedding" margin="0 0 24px 0" />

![Simple Vector Index](/img/examples/simple_vector_index/cover.png)

## Overview
In this tutorial, we will build index with text embeddings and query it with natural language.
We try to keep it minimalistic and focus on the gist of the indexing flow.


## Flow Overview
![Flow](/img/examples/simple_vector_index/flow.png)

1. Read text files from the local filesystem
2. Chunk each document
3. For each chunk, embed it with a text embedding model
4. Store the embeddings in a vector database for retrieval

## Prerequisites

- [Install Postgres](https://cocoindex.io/docs/getting_started/installation).
CocoIndex uses Postgres to keep track of data lineage for incremental processing.


## Add Source

```python
@cocoindex.flow_def(name="TextEmbedding")
def text_embedding_flow(flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope):
    """
    Define an example flow that embeds text into a vector database.
    """
    data_scope["documents"] = flow_builder.add_source(
        cocoindex.sources.LocalFile(path="markdown_files"))

    doc_embeddings = data_scope.add_collector()
```

`flow_builder.add_source` will create a table with sub fields (`filename`, `content`)
<DocumentationButton url="https://cocoindex.io/docs/ops/sources" text="Source" />


## Process each file and collect the embeddings

### Chunk the file

```python
with data_scope["documents"].row() as doc:
    doc["chunks"] = doc["content"].transform(
        cocoindex.functions.SplitRecursively(),
        language="markdown", chunk_size=2000, chunk_overlap=500)
```

![Chunking](/img/examples/simple_vector_index/chunk.png)

<DocumentationButton url="https://cocoindex.io/docs/ops/functions#splitrecursively" text="SplitRecursively" />

### Embed each chunk

```python
with doc["chunks"].row() as chunk:
    chunk["embedding"] = chunk["text"].transform(
        cocoindex.functions.SentenceTransformerEmbed(
            model="sentence-transformers/all-MiniLM-L6-v2"
        )
    )
    doc_embeddings.collect(filename=doc["filename"], location=chunk["location"],
                            text=chunk["text"], embedding=chunk["embedding"])
```

The `MiniLM-L6-v2` model is a good balance of speed and quality for text embeddings, though you can swap in other SentenceTransformer models as needed.

<DocumentationButton url="https://cocoindex.io/docs/ops/functions#sentencetransformerembed" text="SentenceTransformerEmbed" margin="0 0 16px 0" />

![Embedding](/img/examples/simple_vector_index/embed.png)

## Export the embeddings

Export the embeddings to a table in Postgres.

```python
doc_embeddings.export(
    "doc_embeddings",
    cocoindex.storages.Postgres(),
    primary_key_fields=["filename", "location"],
    vector_indexes=[
        cocoindex.VectorIndexDef(
            field_name="embedding",
            metric=cocoindex.VectorSimilarityMetric.COSINE_SIMILARITY)])
```
CocoIndex supports other vector databases as well, with 1-line switch.
<DocumentationButton url="https://cocoindex.io/docs/ops/targets" text="Targets" />

## Query the index

### Define a shared flow for both indexing and querying

```python
@cocoindex.transform_flow()
def text_to_embedding(text: cocoindex.DataSlice[str]) -> cocoindex.DataSlice[list[float]]:
    """
    Embed the text using a SentenceTransformer model.
    This is a shared logic between indexing and querying, so extract it as a function.
    """
    return text.transform(
        cocoindex.functions.SentenceTransformerEmbed(
            model="sentence-transformers/all-MiniLM-L6-v2"))
```

This code defines a transformation function that converts text into vector embeddings using the SentenceTransformer model.
`@cocoindex.transform_flow()` is needed to share the transformation across indexing and query.

This decorator marks this as a reusable transformation flow that can be called on specific input data from user code using `eval()`, as shown in the search function below.

### Write query

CocoIndex doesn't provide additional query interface at the moment. We can write SQL or rely on the query engine by the target storage, if any.

<DocumentationButton url="https://cocoindex.io/docs/ops/targets#postgres" text="Postgres" margin="0 0 16px 0" />


```python
def search(pool: ConnectionPool, query: str, top_k: int = 5):
    table_name = cocoindex.utils.get_target_storage_default_name(text_embedding_flow, "doc_embeddings")
    query_vector = text_to_embedding.eval(query)

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT filename, text, embedding <=> %s::vector AS distance
                FROM {table_name} ORDER BY distance LIMIT %s
            """, (query_vector, top_k))
            return [
                {"filename": row[0], "text": row[1], "score": 1.0 - row[2]}
                for row in cur.fetchall()
            ]
```

Setup `main()` for interactive query in terminal.

```python
def _main():
    # Initialize the database connection pool.
    pool = ConnectionPool(os.getenv("COCOINDEX_DATABASE_URL"))
    # Run queries in a loop to demonstrate the query capabilities.
    while True:
        query = input("Enter search query (or Enter to quit): ")
        if query == '':
            break
        # Run the query function with the database connection pool and the query.
        results = search(pool, query)
        print("\nSearch results:")
        for result in results:
            print(f"[{result['score']:.3f}] {result['filename']}")
            print(f"    {result['text']}")
            print("---")
        print()

if __name__ == "__main__":
    load_dotenv()
    cocoindex.init()
    _main()
```

In the function above, most parts are standard query logic - you can use any libraries you like.
There're two CocoIndex-specific logic:

1.  Get the table name from the export target in the `text_embedding_flow` above.
    Since the table name for the `Postgres` target is not explicitly specified in the `export()` call,
    CocoIndex uses a default name.
    `cocoindex.utils.get_target_default_name()` is a utility function to get the default table name for this case.

2.  Evaluate the transform flow defined above with the input query, to get the embedding.
    It's done by the `eval()` method of the transform flow `text_to_embedding`.
    The return type of this method is `NDArray[np.float32]` as declared in the `text_to_embedding()` function (`cocoindex.DataSlice[NDArray[np.float32]]`).



## Time to have fun!
- Run the following command to setup and update the index.

    ```sh
    cocoindex update --setup main.py
    ```

- Start the interactive query in terminal.
    ```sh
    python main.py
    ```


## CocoInsight

You can walk through the project step by step in [CocoInsight](https://www.youtube.com/watch?v=MMrpUfUcZPk) to see exactly how each field is constructed and what happens behind the scenes.


```sh
cocoindex server -ci main
```

Follow the url `https://cocoindex.io/cocoinsight`.  It connects to your local CocoIndex server, with zero pipeline data retention.
