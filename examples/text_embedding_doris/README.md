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
   - **Option A: Local Doris** - Run with a single command (defaults to Doris 4.x):
     ```sh
     curl -sSL https://doris.apache.org/files/start-doris.sh | bash
     ```
     Verify it's running: `mysql -uroot -P9030 -h127.0.0.1`

   - **Option B: VeloDB Cloud** - Sign up at [VeloDB Cloud](https://www.velodb.cloud/passport/login) for a managed service, please use [VeloDB Cloud 5.0 beta](https://docs.velodb.io/cloud/5.x-preview/getting-started/quick-start)

4. Configure environment variables in `.env`:

    For **local Doris**:
    ```sh
    DORIS_FE_HOST=127.0.0.1
    DORIS_PASSWORD=
    ```

    For **VeloDB Cloud**:
    ```sh
    DORIS_FE_HOST=your-cluster.velodb.cloud
    DORIS_PASSWORD=your-password
    DORIS_USERNAME=admin
    DORIS_HTTP_PORT=443
    DORIS_QUERY_PORT=9030
    DORIS_DATABASE=cocoindex_demo
    ```

## Run

1. Update index (this will also setup Doris tables the first time):

    ```sh
    cocoindex update main
    ```

    You can also run with `-L` to watch for file changes and update automatically:

    ```sh
    cocoindex update -L main
    ```

2. Run queries:

    ```sh
    python main.py
    ```

## CocoInsight

I used CocoInsight (Free beta now) to troubleshoot the index generation and understand the data lineage of the pipeline.
It just connects to your local CocoIndex server, with Zero pipeline data retention. Run following command to start CocoInsight:

```sh
cocoindex server -ci main
```

Open the CocoInsight UI at [https://cocoindex.io/cocoinsight](https://cocoindex.io/cocoinsight).
You can run queries in the CocoInsight UI.
