# Google Drive Text Embedding (v1) 📄

This example embeds text files from Google Drive, stores chunk embeddings in Postgres (pgvector), and includes a simple query demo.

## Prerequisites

- A running Postgres with the pgvector extension. If you don't have one, start a local instance with the compose file in this repo:

  ```sh
  docker compose -f ../../dev/postgres.yaml up -d
  ```

- A Google Cloud service account with Drive access
- Environment variables:

  ```sh
  export POSTGRES_URL="postgres://cocoindex:cocoindex@localhost/cocoindex"
  export GOOGLE_SERVICE_ACCOUNT_CREDENTIAL="/path/to/service-account.json"
  export GOOGLE_DRIVE_ROOT_FOLDER_IDS="folder_id_1,folder_id_2"
  ```

## Run

Install deps:

```sh
pip install -e .
```

Build/update the index (one-shot catch-up; the google_drive source does not support live mode):

```sh
cocoindex update main
```

Query:

```sh
python main.py "what is self-attention?"
```

Note: this example does not create a vector index; queries do a sequential scan.
