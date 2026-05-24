# Image Search with ColPali (v1)
[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)

This example builds an image search index using the ColPali multi-vector embedding model and stores vectors in Qdrant (MaxSim multivector config), then queries it with natural language via a small FastAPI server and React frontend.

We appreciate a star ⭐ at [CocoIndex Github](https://github.com/cocoindex-io/cocoindex) if this is helpful.

## Setup
- A running Postgres. If you don't have one, start a local instance with the compose file in this repo:

  ```sh
  docker compose -f ../../dev/postgres.yaml up -d
  ```

- Make sure Qdrant is running

  ```sh
  docker run -d -p 6334:6334 -p 6333:6333 qdrant/qdrant
  ```

## Run

Install dependencies:

```sh
pip install -e .
```

Start the FastAPI server:

```sh
python -m uvicorn api:app --reload --host 0.0.0.0 --port 8000
```

The server runs the index in **live mode** in the background — startup blocks until the initial sweep over `img/` finishes (so the collection is queryable), then file changes keep flowing into Qdrant while requests are served. There is no separate "build the index" step.

Then in another terminal, start the frontend:

```sh
cd frontend
npm install
npm run dev
```

Go to `http://localhost:5173` to search.

## Code layout

- `pipeline.py` — defines the CocoIndex `app`, the ColPali embedder helpers, and a small `_qdrant_search` shim. Library only — not an entry point.
- `api.py` — FastAPI server. Imports `pipeline`, runs `pipeline.app.update(live=True)` in the background, and exposes `/search`.
