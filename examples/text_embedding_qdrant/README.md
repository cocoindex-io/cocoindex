# Text Embedding with Qdrant (v1) 🔍

This example embeds local markdown files, stores the chunks + embeddings in Qdrant, and provides a simple semantic-search query demo.

## Prerequisites

- Run Qdrant locally (HTTP 6333, gRPC 6334)

```sh
docker run -d -p 6334:6334 -p 6333:6333 qdrant/qdrant
```

## Run

Install deps:

```sh
pip install -e .
```

Build/update the index (writes points into Qdrant). Pick one of the two modes:

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

You can also open the Qdrant dashboard at <http://localhost:6333/dashboard>.

## Environment

Copy `.env.example` to `.env` and fill in the blanks — it is loaded automatically when you run the example:

```sh
cp .env.example .env
```

Stores vectors in Qdrant (defaults to a local container at `http://localhost:6334/`), so no required secrets — the file documents the optional `QDRANT_URL` override.
