# Image Search with CocoIndex
[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)

We will build live image search and query it with natural language, using multimodal embedding models. We use CocoIndex to build real-time indexing flow. During running, you can add new files to the folder and it only processes changed files, indexing them within a minute.

We appreciate a star ⭐ at [CocoIndex Github](https://github.com/cocoindex-io/cocoindex) if this is helpful.

<img width="1105" alt="cover" src="https://github.com/user-attachments/assets/544fb80d-c085-4150-84b6-b6e62c4a12b9" />

## Two Implementation Options

This example provides two different image search implementations:

### 1. CLIP-based Search (`main.py`)
- **Model**: CLIP ViT-L/14 (OpenAI)
- **Embedding**: Single-vector embeddings (768 dimensions)
- **Search**: Standard cosine similarity

### 2. ColPali-based Search (`colpali_main.py`)
- **Model**: ColPali (Contextual Late-interaction over Patches)
- **Embedding**: Multi-vector embeddings with late interaction
- **Search**: MaxSim scoring for optimal patch-level matching
- **Performance**: Better for document/text-in-image search

## Technologies
- CocoIndex for ETL and live update
- **CLIP ViT-L/14** OR **ColPali** - Multimodal embedding models
- Qdrant for Vector Storage (with multi-vector support for ColPali)
- FastAPI for backend
- Ollama (Optional) for generating image captions

## Setup
- [Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres) if you don't have one.

- Make sure Qdrant is running
  ```
  docker run -d -p 6334:6334 -p 6333:6333 qdrant/qdrant
  ```

## (Optional) Run Ollama

- This enables automatic image captioning
```
ollama pull gemma3
ollama serve
export OLLAMA_MODEL="gemma3"  # Optional, for caption generation
```

## Run the App

### Option 1: CLIP-based Search
- Install dependencies:
  ```
  pip install -e .
  ```

- Run CLIP Backend:
  ```
  uvicorn main:app --reload --host 0.0.0.0 --port 8000
  ```

### Option 2: ColPali-based Search
- Install dependencies:
  ```
  pip install -e .
  pip install 'cocoindex[colpali]'  # Adds ColPali support
  ```

- Configure model (optional):
  ```sh
  # All ColVision models supported by colpali-engine are available
  # See https://github.com/illuin-tech/colpali#list-of-colvision-models for the complete list

  # ColPali models (colpali-*) - PaliGemma-based, best for general document retrieval
  export COLPALI_MODEL="vidore/colpali-v1.2"  # Default model
  export COLPALI_MODEL="vidore/colpali-v1.3"  # Latest version

  # ColQwen2 models (colqwen-*) - Qwen2-VL-based, excellent for multilingual text (29+ languages) and general vision
  export COLPALI_MODEL="vidore/colqwen2-v1.0"
  export COLPALI_MODEL="vidore/colqwen2.5-v0.2"  # Latest Qwen2.5 model

  # ColSmol models (colsmol-*) - Lightweight, good for resource-constrained environments
  export COLPALI_MODEL="vidore/colSmol-256M"

  # Any other ColVision models from https://github.com/illuin-tech/colpali are supported
  ```

- Run ColPali Backend:
  ```
  uvicorn colpali_main:app --reload --host 0.0.0.0 --port 8000
  ```

Note that recent Nvidia GPUs (RTX 5090) will not work with the Stable pytorch version up to 2.7.1

If you get this error:

```
The current PyTorch install supports CUDA capabilities sm_37 sm_50 sm_60 sm_61 sm_70 sm_75 sm_80 sm_86 sm_90 compute_37.
```

You can install the nightly pytorch build here: https://pytorch.org/get-started/locally/

```sh
pip install --pre torch torchvision torchaudio --index-url https://download.pytorch.org/whl/nightly/cu129
```

### Frontend (same for both)
- Run Frontend:
  ```
  cd frontend
  npm install
  npm run dev
  ```

Go to `http://localhost:5173` to search. The frontend works with both backends identically.

## Performance Notes
- **CLIP**: Faster, good for general image-text matching
- **ColPali**: More accurate for document images and text-heavy content, supports multi-vector late interaction for better precision
