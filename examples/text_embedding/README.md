# Text Embedding (v1) 🔍

This example embeds local markdown files, stores the chunks + embeddings in Postgres (pgvector), and provides a simple semantic-search query demo.

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

Build/update the index (writes rows into Postgres). Pick one of the two modes:

- **Catch-up run** — scan sources, sync changes, exit:

  ```sh
  cocoindex update main
  ```

- **Live run** — catch up, then keep watching for file changes (the source declares `live=True` in `main.py`):

  ```sh
  cocoindex update -L main
  ```

Query:

```sh
python main.py "what is self-attention?"
```

Note: this example **does not create a vector index**; queries will do a sequential scan.
