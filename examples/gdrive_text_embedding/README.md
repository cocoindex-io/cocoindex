# Google Drive Text Embedding (v1) 📄

This example embeds text files from Google Drive, stores chunk embeddings in Postgres (pgvector), and includes a simple query demo.

## Prerequisites

- A running Postgres with the pgvector extension available
- A Google Cloud service account with Drive access
- Environment variables:

```sh
export COCOINDEX_DATABASE_URL="postgres://cocoindex:cocoindex@localhost/cocoindex"
export GOOGLE_SERVICE_ACCOUNT_CREDENTIAL="/path/to/service-account.json"
export GOOGLE_DRIVE_ROOT_FOLDER_IDS="folder_id_1,folder_id_2"
```

## Run

Install deps:

```sh
pip install -e .
```

Build/update the index:

```sh
cocoindex update main.py
```

Query:

```sh
python main.py query "what is self-attention?"
```

Note: this example does not create a vector index; queries do a sequential scan.
