# Build text embedding and semantic search with Apache Doris

[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)

CocoIndex supports Apache Doris natively. In this example, we will build index flow from text embedding from local markdown files, and query the index. We will use **Apache Doris** (or **VeloDB Cloud**) as the vector database.

We appreciate a star at [CocoIndex Github](https://github.com/cocoindex-io/cocoindex) if this is helpful.

## Steps

### Indexing Flow

1. We will ingest a list of local files.
2. For each file, perform chunking (recursively split) and then embedding.
3. We will save the embeddings and the metadata in Apache Doris with vector index support.

### Query

1. We have `search()` as a [query handler](https://cocoindex.io/docs/query#query-handler), to query the Doris table with vector similarity search.
2. We share the embedding operation `text_to_embedding()` between indexing and querying,
  by wrapping it as a [transform flow](https://cocoindex.io/docs/query#transform-flow).

## Pre-requisites

1. [Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres) if you don't have one. Although the target store is Apache Doris, CocoIndex uses Postgres to track the data lineage for incremental processing.

2. Install dependencies:

    ```sh
    pip install -e .
    ```

3. Set up Apache Doris or VeloDB Cloud:
   - **Option A: Local Doris** - [Install Apache Doris 4.x](https://doris.apache.org/docs/4.x/gettingStarted/quick-start/)
   - **Option B: VeloDB Cloud** - Sign up at [VeloDB Cloud](https://cloud.velodb.io/) for a managed service, please use [VeloDB Cloud 5.0 beta](https://docs.velodb.io/cloud/5.x-preview/getting-started/quick-start)

4. Configure environment variables in `.env`:
    ```sh
    DORIS_FE_HOST=your-doris-host.example.com
    DORIS_PASSWORD=your-password
    # Optional:
    DORIS_USERNAME=root
    DORIS_HTTP_PORT=8080
    DORIS_QUERY_PORT=9030
    DORIS_DATABASE=cocoindex_demo
    ```

## Run

Update index, which will also setup Doris tables at the first time:

```sh
cocoindex update main
```

You can also run the command with `-L`, which will watch for file changes and update the index automatically.

```sh
cocoindex update -L main
```

## CocoInsight

I used CocoInsight (Free beta now) to troubleshoot the index generation and understand the data lineage of the pipeline.
It just connects to your local CocoIndex server, with Zero pipeline data retention. Run following command to start CocoInsight:

```sh
cocoindex server -ci main
```

Open the CocoInsight UI at [https://cocoindex.io/cocoinsight](https://cocoindex.io/cocoinsight).
You can run queries in the CocoInsight UI.
