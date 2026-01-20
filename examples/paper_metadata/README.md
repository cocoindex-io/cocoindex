# Paper Metadata (v1)

[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)

This example extracts metadata (title, authors, abstract) from PDF papers, stores it in Postgres, and builds embeddings for semantic search.

We appreciate a star ⭐ at [CocoIndex Github](https://github.com/cocoindex-io/cocoindex) if this is helpful.

## Prerequisites

- [Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres)
- Set `OPENAI_API_KEY` for metadata extraction
- Set `COCOINDEX_DATABASE_URL` for Postgres access

## Run

Install dependencies:

```sh
pip install -e .
```

Set environment variables:

```sh
export OPENAI_API_KEY="your_key"
export COCOINDEX_DATABASE_URL="postgres://cocoindex:cocoindex@localhost/cocoindex"
```

This example uses the `coco_examples_v1` schema by default to avoid clashing with the legacy example tables.

Build/update the index:

```sh
cocoindex update main.py
```

Query:

```sh
python main.py query "graph neural networks"
```

Note: this example **does not create a vector index**; queries will do a sequential scan.
