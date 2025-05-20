# Build text embedding and semantic search 
[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/cocoindex-io/cocoindex/blob/main/examples/text_embedding/Text_Embedding.ipynb)


In this example, we will build a text embedding index and a semantic search flow based on local markdown files.


<img width="461" alt="Screenshot 2025-05-19 at 5 48 28 PM" src="https://github.com/user-attachments/assets/b6825302-a0c7-4b86-9a2d-52da8286b4bd" />

- We will ingest from a list of local files.
- For each file, perform chunking (Recursive Split) and then embeddings. 
- We will save the embeddings and the metadata in Postgres with PGVector.
- And then add a simpler query handler for semantic search.

## Prerequisite

[Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres) if you don't have one.

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

```
python main.py cocoindex server -ci
```

Then open the CocoInsight UI at [https://cocoindex.io/cocoinsight](https://cocoindex.io/cocoinsight).

