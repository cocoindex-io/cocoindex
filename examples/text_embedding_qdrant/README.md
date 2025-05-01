## Description

Example to build a vector index in Qdrant based on local files.

## Pre-requisites

- [Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres) if you don't have one.

- Run Qdrant.

```bash
docker run -d -p 6334:6334 -p 6333:6333 qdrant/qdrant
```

- [Create a collection](https://qdrant.tech/documentation/concepts/vectors/#named-vectors) to export the embeddings to.

```bash
curl  -X PUT \
  'http://localhost:6333/collections/cocoindex' \
  --header 'Content-Type: application/json' \
  --data-raw '{
  "vectors": {
    "text_embedding": {
      "size": 384,
      "distance": "Cosine"
    }
  }
}'
```

You can view the collections and data with the Qdrant dashboard at <http://localhost:6333/dashboard>.

## Run

Install dependencies:

```bash
pip install -e .
```

Setup:

```bash
python main.py cocoindex setup
```

Update index:

```bash
python main.py cocoindex update
```

Run:

```bash
python main.py
```

## CocoInsight

CocoInsight is in Early Access now (Free) 😊 You found us! A quick 3 minute video tutorial about CocoInsight: [Watch on YouTube](https://youtu.be/ZnmyoHslBSc?si=pPLXWALztkA710r9).

Run CocoInsight to understand your RAG data pipeline:

```bash
python main.py cocoindex server -ci
```

Then open the CocoInsight UI at [https://cocoindex.io/cocoinsight](https://cocoindex.io/cocoinsight).
