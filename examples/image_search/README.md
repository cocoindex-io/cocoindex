# Image Search with CocoIndex

![image](https://github.com/user-attachments/assets/3a696344-c9b4-46e8-9413-6229dbb8672a)

- Qdrant for Vector Storage
- CLIP ViT-L/14 - Embeddings Model for both images and text
- Live Update

## Make sure Postgres and Qdrant are running
```
docker run -d -p 6334:6334 -p 6333:6333 qdrant/qdrant
export COCOINDEX_DATABASE_URL="postgres://cocoindex:cocoindex@localhost/cocoindex"
```

## Create Qdrant Collection
```
curl -X PUT 'http://localhost:6333/collections/image_search' \
  -H 'Content-Type: application/json' \
  -d '{
    "vectors": {
      "embedding": {
        "size": 768,
        "distance": "Cosine"
      }
    }
  }'
```

## Run Backend
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

