## Description
# Build text embedding and semantic search 🔍 with Qdrant

[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)

In this example, we will build index flow from text embedding from local markdown files, and query the index.
We will use **Qdrant** as the vector database.

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
I used CocoInsight (Free beta now) to troubleshoot the index generation and understand the data lineage of the pipeline. 
It just connects to your local CocoIndex server, with Zero pipeline data retention. Run following command to start CocoInsight:

```bash
python main.py cocoindex server -ci
```

Open the CocoInsight UI at [https://cocoindex.io/cocoinsight](https://cocoindex.io/cocoinsight).


