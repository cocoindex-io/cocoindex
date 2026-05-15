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

Build/update the index. Either of the following works:

```sh
cocoindex update main
```

or

```sh
python main.py
```

Query:

```sh
python main.py query "what is self-attention?"
```

You can also open the Qdrant dashboard at <http://localhost:6333/dashboard>.
