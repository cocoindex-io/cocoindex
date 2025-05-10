# Image Search with CocoIndex

![image](https://github.com/user-attachments/assets/3a696344-c9b4-46e8-9413-6229dbb8672a)

- QDrant for Vector Storage
- Ollama Gemma3 (Image to Text)
- CLIP ViT-L/14 - Embeddings Model

## Make sure Postgres and Qdrant are running
```
docker run -d --name qdrant -p 6334:6334 qdrant/qdrant:latest
export COCOINDEX_DATABASE_URL="postgres://cocoindex:cocoindex@localhost/cocoindex"
```

## Create QDrant Collection
```
curl -X PUT
  'http://localhost:6333/collections/image_search' \
  --header 'Content-Type: application/json' \
  --data-raw '{
    "vectors": {
      "embedding": {
        "size": 768,
        "distance": "Cosine"
      }
    }
  }'

```

## Run Ollama
```
ollama pull gemma3
ollama serve
```

## Create virtual environment and install dependencies
```
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Place your images in the `img` directory.


## Run Backend
```
python main.py cocoindex setup
python main.py cocoindex update
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

## Run Frontend
```
cd frontend
npm install
npm run dev
```

Go to `http://localhost:5174` to search.
