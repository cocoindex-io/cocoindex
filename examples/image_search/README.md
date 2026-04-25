# Image Search with CocoIndex (v1)
[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)

This example builds an image search index with CLIP embeddings and Qdrant, then queries it with natural language.

We appreciate a star ⭐ at [CocoIndex Github](https://github.com/cocoindex-io/cocoindex) if this is helpful.

<img width="1105" alt="cover" src="https://github.com/user-attachments/assets/544fb80d-c085-4150-84b6-b6e62c4a12b9" />

## Technologies
- CocoIndex v1 app pipeline
- CLIP ViT-L/14 for embeddings
- Qdrant for vector storage

## Setup
- [Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres) if you don't have one.

- Make sure Qdrant is running
  ```
  docker run -d -p 6334:6334 -p 6333:6333 qdrant/qdrant
  ```

## Run (CLI)

Install dependencies:

```sh
pip install -e .
```

Build/update the index:

```sh
cocoindex update main.py
```

Query:

```sh
python main.py query "a red car"
```

## Frontend (optional)

If you want a UI, start the FastAPI wrapper and the frontend:

```sh
python -m uvicorn api:app --reload --host 0.0.0.0 --port 8000
```

Then in another terminal:

```sh
cd frontend
npm install
npm run dev
```

Then open `http://localhost:5173`.
