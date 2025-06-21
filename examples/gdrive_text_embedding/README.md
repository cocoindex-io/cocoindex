# Build Google Drive text embedding and semantic search 🔍
[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)

In this example, we will build an embedding index based on Google Drive files and perform semantic search.

It continuously updates the index as files are added / updated / deleted in the source folders. It keeps the index in sync with the source folders in real-time.

We appreciate a star ⭐ at [CocoIndex Github](https://github.com/cocoindex-io/cocoindex) if this is helpful.

## Steps

### Indexing Flow
<img width="801" alt="Google Drive File Ingestion" src="https://github.com/user-attachments/assets/bc772e1e-d7a0-46de-b57c-290a78c128ac" />

1. We will ingest files from Google Drive folders.
2. For each file, perform chunking (recursively split) and then embedding.
3. We will save the embeddings and the metadata in Postgres with PGVector.

### Query
We will match against user-provided text by a SQL query, and reuse the embedding operation in the indexing flow.

## Prerequisite

Before running the example, you need to:

1.  [Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres) if you don't have one.

2.  Prepare for Google Drive:

    -   Setup a service account in Google Cloud, and download the credential file.
    -   Share folders containing files you want to import with the service account's email address.

    See [Setup for Google Drive](https://cocoindex.io/docs/ops/sources#setup-for-google-drive) for more details.

3.  Create `.env` file with your credential file and folder IDs.
    Starting from copying the `.env.example`, and then edit it to fill in your credential file path and folder IDs.

    ```bash
    cp .env.exmaple .env
    $EDITOR .env
    ```

## Run

- Install dependencies:

    ```sh
    pip install -e .
    ```

- Setup:

    ```sh
    cocoindex setup main.py
    ```

- Run:

    ```sh
    python main.py
    ```

During running, it will keep observing changes in the source folders and update the index automatically.
At the same time, it accepts queries from the terminal, and performs search on top of the up-to-date index.


## CocoInsight
I used CocoInsight (Free beta now) to troubleshoot the index generation and understand the data lineage of the pipeline.
It just connects to your local CocoIndex server, with Zero pipeline data retention. Run following command to start CocoInsight:

```sh
cocoindex server -ci main.py
```

You can also add a `-L` flag to make the server keep updating the index to reflect source changes at the same time:

```sh
cocoindex server -ci -L main.py
```

Then open the CocoInsight UI at [https://cocoindex.io/cocoinsight](https://cocoindex.io/cocoinsight).

<img width="1316" alt="Use CocoInsight to understand the data of the pipeline" src="https://github.com/user-attachments/assets/0ed848db-3cc3-43d3-8cb8-35069f503288" />
