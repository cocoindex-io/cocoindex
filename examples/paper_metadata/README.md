# Paper Metadata (v1)

[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)

This example extracts metadata (title, authors, abstract) from PDF papers, stores it in Postgres, and builds embeddings for semantic search.

We appreciate a star ⭐ at [CocoIndex Github](https://github.com/cocoindex-io/cocoindex) if this is helpful.

## Prerequisites

- A running Postgres with the pgvector extension. If you don't have one, start a local instance with the compose file in this repo:

  ```sh
  docker compose -f ../../dev/postgres.yaml up -d
  ```

- Set `OPENAI_API_KEY` for metadata extraction
- Set `POSTGRES_URL` for Postgres access

## Run

Install dependencies:

```sh
pip install -e .
```

Set environment variables:

```sh
export OPENAI_API_KEY="your_key"
export POSTGRES_URL="postgres://cocoindex:cocoindex@localhost/cocoindex"
```

This example uses the `coco_examples_v1` schema by default to avoid clashing with the legacy example tables.

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
python main.py "graph neural networks"
```

Note: this example **does not create a vector index**; queries will do a sequential scan.
