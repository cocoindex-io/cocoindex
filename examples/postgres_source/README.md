# PostgreSQL Source Example 🗄️

[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)

This example demonstrates how to use Postgres tables as the source for CocoIndex.
It reads structured product data from existing PostgreSQL tables, performs calculations, generates embeddings, and stores them in a separate CocoIndex table.

We appreciate a star ⭐ at [CocoIndex Github](https://github.com/cocoindex-io/cocoindex) if this is helpful.

This example contains one flow:

`postgres_product_indexing_flow`: Read from a table `source_products` (composite primary key), compute additional fields like total value and full description, then generate embeddings for semantic search.


## Prerequisites

Before running the example, you need to:

1.  Install dependencies:

    ```bash
    pip install -e .
    ```

2.  Follow the [CocoIndex PostgreSQL setup guide](https://cocoindex.io/docs/getting_started/quickstart) to install and configure PostgreSQL with pgvector extension.

3.  Create source table `source_products` with sample data:

    ```bash
    $ psql "postgres://cocoindex:cocoindex@localhost/cocoindex" -f ./prepare_source_data.sql
    ```

    For simplicity, we use the same database for source and target. You can also setup a separate Postgres database to use as the source database.
    Remember to update the `SOURCE_DATABASE_URL` in `.env` file if you use a separate database.

## Run

Update index, which will also setup the tables at the first time:

```bash
cocoindex update --setup main.py
```

## CocoInsight
CocoInsight is in Early Access now (Free) 😊 You found us! A quick 3 minute video tutorial about CocoInsight: [Watch on YouTube](https://youtu.be/ZnmyoHslBSc?si=pPLXWALztkA710r9).

Run CocoInsight to understand your RAG data pipeline:

```sh
cocoindex server -ci main
```

You can also add a `-L` flag to make the server keep updating the index to reflect source changes at the same time:

```sh
cocoindex server -ci -L main
```

Then open the CocoInsight UI at [https://cocoindex.io/cocoinsight](https://cocoindex.io/cocoinsight).
