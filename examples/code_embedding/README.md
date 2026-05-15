# Code Embedding (v1) 🧑‍💻

This example extracts code chunks from local Python files, stores the code and their vector embeddings in Postgres (pgvector), and provides a simple semantic search demo for code.

## Prerequisites

- A running Postgres with the pgvector extension. If you don't have one, start a local instance with the compose file in this repo:

  ```sh
  docker compose -f ../../dev/postgres.yaml up -d
  ```

- `POSTGRES_URL` set, e.g.

  ```sh
  export POSTGRES_URL="postgres://cocoindex:cocoindex@localhost/cocoindex"
  ```

## Run

Install deps:

```sh
pip install -e .
```

Build/update the index (writes rows into Postgres). Either of the following works:

```sh
cocoindex update main
```

or

```sh
python main.py
```

Query:

```sh
python main.py query "embedding"
```
