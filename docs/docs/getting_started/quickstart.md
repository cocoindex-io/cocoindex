---
title: Quickstart
description: Get started with CocoIndex in 10 minutes
---

# Build your first CocoIndex project

This guide will help you get up and running with CocoIndex in just a few minutes, that does:
*   Read files from a directory
*   Perform basic chunking and embedding
*   loads the data into a vector store (PG Vector)


## Prerequisite: Install CocoIndex environment

We'll need to install a bunch of dependencies for this project.

1.  Install CocoIndex:
 
    ```bash
    pip install cocoindex
    ```

2.  You can skip this step if you already have a Postgres database with pgvector extension installed.
    If not, the easiest way is to bring up a Postgres database using docker compose:

    - Make sure Docker Compose is installed: [docs](https://docs.docker.com/compose/install/)
    - Start a Postgres SQL database for cocoindex using our docker compose config:

    ```bash
    docker compose -f <(curl -L https://raw.githubusercontent.com/cocoindex-io/cocoindex/refs/heads/main/dev/postgres.yaml) up -d
    ```

## Step 1: Prepare directory for your project

1.  Open the terminal and create a new directory for your project:

    ```bash
    mkdir cocoindex-quickstart
    cd cocoindex-quickstart
    ```

2.  Prepare input files for the index. Put them in a directory, e.g. `markdown_files`.
    If you don't have any files at hand, you may download the example [markdown_files.zip](markdown_files.zip) and unzip it in the current directory.

## Step 2: Create the Python file `quickstart.py`

Create a new file `quickstart.py` and import the `cocoindex` library:

```python title="quickstart.py"
import cocoindex
```

Then we'll put the following pieces into the file:

*   Define an indexing flow, which specifies a data flow to transform data from specified data source into a vector index.
*   Define a query handler, which can be used to query the vector index.
*   A main function, to interact with users and run queries using the query handler above.

### Step 2.1: Define the indexing flow

Starting from the indexing flow:

```python title="quickstart.py"
@cocoindex.flow_def(name="TextEmbedding")
def text_embedding_flow(flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope):
    # Add a data source to read files from a directory
    data_scope["documents"] = flow_builder.add_source(
        cocoindex.sources.LocalFile(path="markdown_files"))

    # Add a collector for data to be exported to the vector index
    doc_embeddings = data_scope.add_collector()

    # Transform data of each document
    with data_scope["documents"].row() as doc:
        # Split the document into chunks, put into `chunks` field
        doc["chunks"] = doc["content"].transform(
            cocoindex.functions.SplitRecursively(
                language="markdown", chunk_size=300, chunk_overlap=100))

        # Transform data of each chunk
        with doc["chunks"].row() as chunk:
            # Embed the chunk, put into `embedding` field
            chunk["embedding"] = chunk["text"].transform(
                cocoindex.functions.SentenceTransformerEmbed(
                    model="sentence-transformers/all-MiniLM-L6-v2"))

            # Collect the chunk into the collector.
            doc_embeddings.collect(filename=doc["filename"], location=chunk["location"],
                                   text=chunk["text"], embedding=chunk["embedding"])

    # Export collected data to a vector index.
    doc_embeddings.export(
        "doc_embeddings",
        cocoindex.storages.Postgres(),
        primary_key_fields=["filename", "location"],
        vector_index=[("embedding", cocoindex.VectorSimilarityMetric.COSINE_SIMILARITY)])
```

Notes:

1.  The `@cocoindex.flow_def` declares a function to be a CocoIndex flow.

2.  In CocoIndex, data is organized in different *data scopes*.
    *   `data_scope`, representing all data.
    *   `doc`, representing each row of `documents`.
    *   `chunk`, representing each row of `chunks`.

3.  A *data source* extracts data from an external source. In this example, the `LocalFile` data source defines a table, each row has `"filename"` and `"content"` fields.

4. After defining the table, we extended a new field `"chunks"` to each row by *transforming* the `"content"` field using `SplitRecursively`. The output of the `SplitRecursively` is also a table representing each chunk of the document, with `"location"` and `"text"` fields.

5. After defining the table, we extended a new field `"embedding"` to each row by *transforming* the `"text"` field using `SentenceTransformerEmbed`.

6. In CocoIndex, a *collector* collects multiple entries of data together. In this example, the `doc_embeddings` collector collects data from all `chunk`s across all `doc`s, and using the collected data to build a vector index `"doc_embeddings"`, using `Postgres`.

### Step 2.2: Define the query handler

Starting from the query handler:

```python title="quickstart.py"
query_handler = cocoindex.query.SimpleSemanticsQueryHandler(
    name="SemanticsSearch",
    flow=text_embedding_flow,
    target_name="doc_embeddings",
    query_transform_flow=lambda text: text.transform(
        cocoindex.functions.SentenceTransformerEmbed(
            model="sentence-transformers/all-MiniLM-L6-v2")),
    default_similarity_metric=cocoindex.VectorSimilarityMetric.COSINE_SIMILARITY)
```

This handler queries the vector index `"doc_embeddings"`, and uses the same embedding model `"sentence-transformers/all-MiniLM-L6-v2"` to transform query text into vectors for similarity matching.


### Step 2.3: Define the main function

The main function is used to interact with users and run queries using the query handler above.

```python title="quickstart.py"
@cocoindex.main_fn()
def _main():
    # Run queries to demonstrate the query capabilities.
    while True:
        try:
            query = input("Enter search query (or Enter to quit): ")
            if query == '':
                break
            results, _ = query_handler.search(query, 10)
            print("\nSearch results:")
            for result in results:
                print(f"[{result.score:.3f}] {result.data['filename']}")
                print(f"    {result.data['text']}")
                print("---")
            print()
        except KeyboardInterrupt:
            break

if __name__ == "__main__":
    _main()
```

The `@cocoindex.main_fn` declares a function as the main function for an indexing application. This achieves the following effects:

*   Initialize the CocoIndex librart states. Settings (e.g. database URL) are loaded from environment variables by default.
*   When the CLI is invoked with `cocoindex` subcommand, `cocoindex CLI` takes over the control, which provides convenient ways to manage the index. See the next step for more details.


## Step 3: Run the indexing pipeline and queries

Specify the database URL by environment variable:

```bash
export COCOINDEX_DATABASE_URL="postgresql://cocoindex:cocoindex@localhost:5432/cocoindex"
```

### Step 3.1: Setup the index pipeline

We need to setup the index:

```bash
python quickstart.py cocoindex setup
```

Enter `yes` and it will automatically create a few tables in the database.

Now we have tables needed by this CocoIndex flow.

### Step 3.2: Build the index

Now we're ready to build the index:

```bash
python quickstart.py cocoindex update
```

It will run for a few seconds and output the following statistics:

```
documents: 3 added, 0 removed, 0 updated
```

### Step 3.3: Run queries against the index

Now we have the index built. We can run the same Python file without additional arguments, which will run the main function defined in Step 2.3:

```bash
python quickstart.py
```

It will ask you to enter a query and it will return the top 10 results.

## Next Steps

Next, you may want to:

*   Learn about [CocoIndex Basics](../core/basics.md).
*   Learn about other examples in the [examples](https://github.com/cocoIndex/cocoindex/tree/main/examples) directory.
    *    The `text_embedding` example is this quickstart with some polishing (loading environment variables from `.env` file, extract pieces shared by the indexing flow and query handler into a function).
    *    Pick other examples to learn upon your interest.