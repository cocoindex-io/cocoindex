# Text Embedding (v1) 🔍

This example embeds local markdown files, stores the chunks + embeddings in Postgres (pgvector), and provides a simple semantic-search query demo.

## Prerequisites

- A running Postgres with the pgvector extension available
- `POSTGRES_URL` set, e.g.

```sh
export POSTGRES_URL="postgres://cocoindex:cocoindex@localhost/cocoindex"
```

## Run

Install deps:

```sh
pip install -e .
```

Build/update the index (writes rows into Postgres):

```sh
cocoindex update main.py
```

Query:

```sh
python main.py query "what is self-attention?"
```

Note: this example **does not create a vector index**; queries will do a sequential scan.
