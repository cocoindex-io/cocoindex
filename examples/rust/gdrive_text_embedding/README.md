# Google Drive Text Embedding (Rust)

Rust port of `examples/gdrive_text_embedding`.

Pipeline:

```text
Google Drive files -> markdown chunks -> all-MiniLM-L6-v2 embeddings -> Postgres/pgvector
```

## Prerequisites

- Postgres with `pgvector`
- A Google Cloud service account that can read the target Drive folders
- Environment variables:

```sh
export POSTGRES_URL="postgres://cocoindex:cocoindex@localhost/cocoindex"
export GOOGLE_SERVICE_ACCOUNT_CREDENTIAL="/path/to/service-account.json"
export GOOGLE_DRIVE_ROOT_FOLDER_IDS="folder_id_1,folder_id_2"
```

## Run

Index:

```sh
cargo run -- index
```

Query:

```sh
cargo run -- query "what is self-attention?"
```

The Rust source currently uses Drive file id as the `mount_each` key to avoid
duplicate-name collisions. The stored `filename` is the path relative to the
configured Drive root folder, matching the Python example's path-oriented
metadata.
