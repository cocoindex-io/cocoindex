# Image Search with CocoIndex
[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)

We will build live image search and query it with natural language, using multimodal embedding model. We are going use CocoIndex to build real-time indexing flow. During running, you can add new files to the folder and it only process changed files and will be indexed within a minute.

We appreciate a star ⭐ at [CocoIndex Github](https://github.com/cocoindex-io/cocoindex) if this is helpful.

<img width="1105" alt="cover" src="https://github.com/user-attachments/assets/544fb80d-c085-4150-84b6-b6e62c4a12b9" />


## Technologies
- CocoIndex for ETL and live update
- CLIP ViT-L/14 - Embeddings Model for images and query
- Qdrant for Vector Storage
- FastApi for backend

## Setup
- Make sure Postgres and Qdrant are running
  ```
  docker run -d -p 6334:6334 -p 6333:6333 qdrant/qdrant
  export COCOINDEX_DATABASE_URL="postgres://cocoindex:cocoindex@localhost/cocoindex"
  ```

## Run
- Install dependencies:
  ```
  pip install -e .
  ```

- Run Backend
  ```
  cocoindex setup main.py
  uvicorn main:app --reload --host 0.0.0.0 --port 8000
  ```

- Run Frontend
  ```
  cd frontend
  npm install
  npm run dev
  ```

Go to `http://localhost:5174` to search.
