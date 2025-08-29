---
title: Quickstart
description: Get started with CocoIndex in 10 minutes
---

import { GitHubButton, YouTubeButton, DocumentationButton } from '../../src/components/GitHubButton';

<GitHubButton url="https://github.com/cocoindex-io/cocoindex-quickstart" margin="0 0 16px 0"/>
<YouTubeButton url="https://www.youtube.com/watch?v=gv5R8nOXsWU" margin="0 0 16px 0"/>

In this tutorial, we will build index with text embeddings and query it with natural language. 
We try to keep it minimalistic and focus on the gist of the indexing flow.


## Flow Overview
![Flow](/img/examples/simple_vector_index/flow.png)

1. Read text files from the local filesystem
2. Chunk each document
3. For each chunk, embed it with a text embedding model
4. Store the embeddings in a vector database for retrieval


## Setup

1.  Install CocoIndex:

    ```bash
    pip install -U 'cocoindex[embeddings]'
    ```

2.  [Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres).

3.  Open the terminal and create a new directory for your project:

    ```bash
    mkdir cocoindex-quickstart
    cd cocoindex-quickstart
    ```
4.  Place input files in a directory `markdown_files`. You may download from [markdown_files.zip](markdown_files.zip).

5.  Create a new file `quickstart.py` and import the `cocoindex` library:

    ```python title="quickstart.py"
    import cocoindex
    ```

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

We extend a new field `"chunks"` to each row by *transforming* the `"content"` field using `SplitRecursively`. The output of the `SplitRecursively` is also a KTable representing each chunk of the document.

![Chunking](/img/examples/simple_vector_index/chunk.png)

<DocumentationButton url="https://cocoindex.io/docs/ops/functions#splitrecursively" text="SplitRecursively" />


### Embed each chunk and collect the embeddings

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

<DocumentationButton url="https://cocoindex.io/docs/ops/functions#sentencetransformerembed" text="SentenceTransformerEmbed" margin="0 0 16px 0" />
 
Plug in the `text_to_embedding` function and collect the embeddings.

```python
with doc["chunks"].row() as chunk:
    chunk["embedding"] = text_to_embedding(chunk["text"])
    doc_embeddings.collect(filename=doc["filename"], location=chunk["location"],
                            text=chunk["text"], embedding=chunk["embedding"])
```

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

## Run the indexing pipeline

Specify the database URL by environment variable:

```bash
export COCOINDEX_DATABASE_URL="postgresql://cocoindex:cocoindex@localhost:5432/cocoindex"
```

Now we're ready to build the index:

```bash
cocoindex update --setup quickstart.py
```

CocoIndex will run for a few seconds and populate the target table with data as declared by the flow. It will output the following statistics:

```
documents: 3 added, 0 removed, 0 updated
```

## End to end: Query the index (Optional)

### Define the query function

We'll use the [`psycopg` library](https://www.psycopg.org/) along with pgvector to connect to the database and run queries on vector data.

```bash
pip install numpy "psycopg[binary,pool]" pgvector
```

Now we can create a function to query the index upon a given input query:

```python title="quickstart.py"
from psycopg_pool import ConnectionPool
from pgvector.psycopg import register_vector

def search(pool: ConnectionPool, query: str, top_k: int = 5):
    # Get the table name, for the export target in the text_embedding_flow above.
    table_name = cocoindex.utils.get_target_default_name(text_embedding_flow, "doc_embeddings")
    # Evaluate the transform flow defined above with the input query, to get the embedding.
    query_vector = text_to_embedding.eval(query)
    # Run the query and get the results.
    with pool.connection() as conn:
        register_vector(conn)
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT filename, text, embedding <=> %s AS distance
                FROM {table_name} ORDER BY distance LIMIT %s
            """, (query_vector, top_k))
            return [
                {"filename": row[0], "text": row[1], "score": 1.0 - row[2]}
                for row in cur.fetchall()
            ]
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

### Add the main 

Now we can add the main logic to the program. It uses the query function we just defined:

```python title="quickstart.py"
if __name__ == "__main__":
    # Initialize CocoIndex library states
    cocoindex.init()

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
                print(f"    {result['text']}")
                print("---")
            print()
        except KeyboardInterrupt:
            break
```

It interacts with users and search the database by calling the `search()` method created in Step 4.2.

### Query the index

```bash
python quickstart.py
```


## Next Steps

Next, you may want to:

*   Learn about [CocoIndex Basics](../core/basics.md).
*   Learn about other examples in the [examples](https://cocoindex.io/docs/examples) directory.
