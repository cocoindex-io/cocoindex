# PostgreSQL Source Example (v1) 🗄️

[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)

This example demonstrates how to use a PostgreSQL table as a source for CocoIndex v1. It reads structured product data from an existing table, computes derived fields, generates embeddings, and stores results in another PostgreSQL table.

We appreciate a star ⭐ at [CocoIndex Github](https://github.com/cocoindex-io/cocoindex) if this is helpful.

## Prerequisites

1. Install dependencies:

```sh
pip install -e .
```

2. Follow the [CocoIndex PostgreSQL setup guide](https://cocoindex.io/docs/getting_started/quickstart) to install and configure PostgreSQL with pgvector extension.

3. Create source table `source_products` with sample data:

```sh
psql "postgres://cocoindex:cocoindex@localhost/cocoindex" -f ./prepare_source_data.sql
```

For simplicity, we use the same database for source and target. You can also set `SOURCE_DATABASE_URL` to use a separate database.

## Run

Build/update the index:

```sh
python main.py
```

Query:

```sh
python main.py query "wireless headphones"
```
