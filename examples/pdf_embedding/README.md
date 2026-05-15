# PDF Embedding (v1)

[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)

This example builds an embedding index from local PDF files. It converts PDFs to markdown, chunks the text, embeds each chunk, and stores the results in Postgres (pgvector). It also provides a simple query demo.

We appreciate a star ⭐ at [CocoIndex Github](https://github.com/cocoindex-io/cocoindex) if this is helpful.

## Prerequisite

A running Postgres with the pgvector extension. If you don't have one, start a local instance with the compose file in this repo:

```sh
docker compose -f ../../dev/postgres.yaml up -d
```

## Run

Install dependencies:

```sh
pip install -e .
```

Set a database URL (or use `.env`):

```sh
export POSTGRES_URL="postgres://cocoindex:cocoindex@localhost/cocoindex"
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
python main.py "what is attention?"
```

Note: this example **does not create a vector index**; queries will do a sequential scan.
