# ChromaDB text embedding demo, built around a local store

[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)

This example is a compact playground for ChromaDB; it builds a small embedding index from local markdown files and then queries it, using a local persistent ChromaDB directory.

If this helps, a star at [CocoIndex Github](https://github.com/cocoindex-io/cocoindex) is appreciated.

## What it does

A tiny file set is ingested; each file is chunked, embedded with a SentenceTransformer model, and written into a ChromaDB collection. The query handler reuses the same embedding flow so the index and query paths stay consistent.

## Prerequisites

You will need Postgres for CocoIndex metadata; ChromaDB stores the vectors and documents. Follow [the installation guide](https://cocoindex.io/docs/getting_started/installation#-install-postgres) if Postgres is not set up.

Install dependencies from this directory:

```sh
pip install -e .
```

## Environment

Copy the example env file and adjust the local path or collection name if you want.

```sh
cp .env.example .env
```

This example uses a local persistent store by default; the folder will be created the first time you run the flow.

## Run

Build or update the index:

```sh
cocoindex update main
```

Live mode will watch the folder and keep the collection fresh:

```sh
cocoindex update -L main
```

## Query

Use the query handler from the CLI:

```sh
cocoindex query main.py search --query "vector databases"
```

## CocoInsight

CocoInsight can show the data lineage and recent updates. Start it with:

```sh
cocoindex server -ci main
```

Then open [https://cocoindex.io/cocoinsight](https://cocoindex.io/cocoinsight).
