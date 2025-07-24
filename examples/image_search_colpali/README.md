# Image Search with CocoIndex (ColPali Edition)
[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)

We will build live image search and query it with natural language, using a multimodal embedding model (ColPali). We use CocoIndex to build a real-time indexing flow. During running, you can add new files to the folder and it only processes changed files, indexing them within a minute.

We appreciate a star ⭐ at [CocoIndex Github](https://github.com/cocoindex-io/cocoindex) if this is helpful.

<img width="1105" alt="cover" src="https://github.com/user-attachments/assets/544fb80d-c085-4150-84b6-b6e62c4a12b9" />


## Technologies
- CocoIndex for ETL and live update
- **ColPali** - Multimodal Embeddings Model for images and query
- Qdrant for Vector Storage (supports both gRPC and HTTP)
- FastAPI for backend
- Ollama (Optional) for generating image captions using `gemma3` or other models

## Setup
- Make sure Postgres and Qdrant are running
  ```
  docker run -d -p 6334:6334 -p 6333:6333 qdrant/qdrant
  export COCOINDEX_DATABASE_URL="postgres://cocoindex:cocoindex@localhost/cocoindex"
  ```

## Qdrant Protocol Configuration
- By default, the app uses **gRPC** (port 6334) to connect to Qdrant for best performance.
- To use HTTP (port 6333) instead, change the config at the top of `main.py`:
  ```python
  # Use GRPC (default)
  QDRANT_URL = os.getenv("QDRANT_URL", "localhost:6334")
  PREFER_GRPC = os.getenv("QDRANT_PREFER_GRPC", "true").lower() == "true"
  # Use HTTP (uncomment below to use HTTP)
  #QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333/")
  #PREFER_GRPC = os.getenv("QDRANT_PREFER_GRPC", "false").lower() == "true"
  ```
- You can also override these with environment variables:
  ```sh
  export QDRANT_URL="localhost:6334"           # for gRPC (default)
  export QDRANT_PREFER_GRPC=true                # for gRPC (default)
  # or for HTTP:
  # export QDRANT_URL="http://localhost:6333/"
  # export QDRANT_PREFER_GRPC=false
  ```

## (Optional) Run Ollama
- This enables automatic image captioning
```
ollama pull gemma3
ollama serve
export OLLAMA_MODEL="gemma3"  # Optional, for caption generation
```

## Run the App
- Install dependencies:
  ```
  pip install -e .
  ```

- Run Backend
  ```
  uvicorn main:app --reload --host 0.0.0.0 --port 8000
  ```

- Run Frontend
  ```
  cd frontend
  npm install
  npm run dev
  ```

Go to `http://localhost:5174` to search.
