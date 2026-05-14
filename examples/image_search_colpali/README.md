# Image Search with ColPali (v1)
[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)

This example builds an image search index using the ColPali embedding model and stores vectors in Qdrant. It supports both CLI queries and a FastAPI backend for the included frontend.

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

## Run (CLI)

Install dependencies:

```
pip install -e .
```

Build/update the index:

```
cocoindex update main.py
```

Query:

```
python main.py query "a red car"
```

## Run (API)

```
python -m uvicorn api:app --reload --host 0.0.0.0 --port 8000
```

## Frontend

```
cd frontend
npm install
npm run dev
```

Go to `http://localhost:5173` to search.
