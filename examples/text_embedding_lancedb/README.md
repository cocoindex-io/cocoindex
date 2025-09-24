# Build text embedding and semantic search 🔍 with LanceDB

[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)

CocoIndex supports LanceDB natively. In this example, we will build index flow from text embedding from local markdown files, and query the index. We will use **LanceDB** as the vector database.

We appreciate a star ⭐ at [CocoIndex Github](https://github.com/cocoindex-io/cocoindex) if this is helpful.


## Steps
### Indexing Flow

1.  We will ingest a list of local files.
2.  For each file, perform chunking (recursively split) and then embedding.
3.  We will save the embeddings and the metadata in LanceDB.

### Query

1.  We have `search()` as a [query handler](https://cocoindex.io/docs/query#query-handler), to query the LanceDB table with LanceDB client.
2.  We share the embedding operation `text_to_embedding()` between indexing and querying,
  by wrapping it as a [transform flow](https://cocoindex.io/docs/query#transform-flow).

## Pre-requisites

1.  [Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres) if you don't have one. Although the target store is LanceDB, CocoIndex uses Postgres to track the data lineage for incremental processing.

2.  Install dependencies:

    ```sh
    pip install -e .
    ```

LanceDB will automatically create a local database directory when you run the example (no additional setup required).

## Run

Update index, which will also setup LanceDB tables at the first time:

```bash
cocoindex update --setup main
```

You can also run the command with `-L`, which will watch for file changes and update the index automatically.

```bash
cocoindex update --setup -L main
```

## CocoInsight
I used CocoInsight (Free beta now) to troubleshoot the index generation and understand the data lineage of the pipeline.
It just connects to your local CocoIndex server, with Zero pipeline data retention. Run following command to start CocoInsight:

```bash
cocoindex server -ci -L main
```

Open the CocoInsight UI at [https://cocoindex.io/cocoinsight](https://cocoindex.io/cocoinsight).
You can run queries in the CocoInsight UI.
