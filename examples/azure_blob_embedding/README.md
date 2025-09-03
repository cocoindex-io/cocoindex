This example builds an embedding index based on files stored in an Azure Blob Storage container.
It continuously updates the index as files are added / updated / deleted in the source container:
it keeps the index in sync with the Azure Blob Storage container effortlessly.

## Prerequisite

Before running the example, you need to:

1.  [Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres) if you don't have one.

2.  Prepare for Azure Blob Storage.
    See [Setup for Azure Blob Storage](https://cocoindex.io/docs/ops/sources#setup-for-azure-blob-storage) for more details.

3.  Create a `.env` file with your Azure Blob Storage container name and (optionally) prefix.
    Start from copying the `.env.example`, and then edit it to fill in your bucket name and prefix.

    ```bash
    cp .env.example .env
    $EDITOR .env
    ```

    Example `.env` file:
    ```
    # Database Configuration
    DATABASE_URL=postgresql://localhost:5432/cocoindex

    # Azure Blob Storage Configuration
    AZURE_BLOB_STORAGE_ACCOUNT_NAME=your-account-name
    AZURE_BLOB_STORAGE_CONTAINER_NAME=your-container-name
    ```

## Run

Install dependencies:

```sh
pip install -e .
```

Run:

```sh
python main.py
```

During running, it will keep observing changes in the Amazon S3 bucket and update the index automatically.
At the same time, it accepts queries from the terminal, and performs search on top of the up-to-date index.


## CocoInsight
CocoInsight is in Early Access now (Free) 😊 You found us! A quick 3 minute video tutorial about CocoInsight: [Watch on YouTube](https://youtu.be/ZnmyoHslBSc?si=pPLXWALztkA710r9).

Run CocoInsight to understand your RAG data pipeline:

```sh
cocoindex server -ci main
```

You can also add a `-L` flag to make the server keep updating the index to reflect source changes at the same time:

```sh
cocoindex server -ci -L main
```

Then open the CocoInsight UI at [https://cocoindex.io/cocoinsight](https://cocoindex.io/cocoinsight).
