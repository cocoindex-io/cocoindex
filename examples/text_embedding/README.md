# Build text embedding and semantic search 🔍
[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/cocoindex-io/cocoindex/blob/main/examples/text_embedding/Text_Embedding.ipynb)
[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)

In this example, we will build index flow from text embedding from local markdown files. And build semantic search with simple query handler.

We appreicate a star ⭐ at [CocoIndex Github](https://github.com/cocoindex-io/cocoindex) if this is helpful.

<img width="461" alt="Screenshot 2025-05-19 at 5 48 28 PM" src="https://github.com/user-attachments/assets/b6825302-a0c7-4b86-9a2d-52da8286b4bd" />

Steps:
1. We will ingest from a list of local files.
2. For each file, perform chunking (Recursive Split) and then embeddings. 
3. We will save the embeddings and the metadata in Postgres with PGVector.
4. And then add a simpler query handler for semantic search.

🌱 A detailed step by step tutorial can be found here: [Get Started Documentation](https://cocoindex.io/docs/getting_started/quickstart)


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

I used CocoInsight (Free beta now) to troubleshoot the index generation and understand the data lineage of the pipeline. 
It just connects to your local CocoIndex server, with Zero pipeline data retention. Run following command to start CocoInsight:

```
python main.py cocoindex server -ci
```

Then open the CocoInsight UI at [https://cocoindex.io/cocoinsight](https://cocoindex.io/cocoinsight).

