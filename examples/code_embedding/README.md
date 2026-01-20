# Code Embedding (v1) 🧑‍💻

This example extracts code chunks from local Python files, stores the code and their vector embeddings in Postgres (pgvector), and provides a simple semantic search demo for code.

## Prerequisites

- A running Postgres with the pgvector extension available
- `DATABASE_URL` (or `COCOINDEX_DATABASE_URL`) set, e.g.

```sh
export DATABASE_URL="postgres://cocoindex:cocoindex@localhost/cocoindex"
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
python main.py query "embedding"
```
