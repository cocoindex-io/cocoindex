# PDF Embedding (v1)

[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)

This example builds an embedding index from local PDF files. It converts PDFs to markdown, chunks the text, embeds each chunk, and stores the results in Postgres (pgvector). It also provides a simple query demo.

We appreciate a star ⭐ at [CocoIndex Github](https://github.com/cocoindex-io/cocoindex) if this is helpful.

## Prerequisite

[Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres) if you don't have one.

## Run

Install dependencies:

```sh
pip install -e .
```

Set a database URL (or use `.env`):

```sh
export COCOINDEX_DATABASE_URL="postgres://cocoindex:cocoindex@localhost/cocoindex"
```

Build/update the index:

```sh
python main.py
```

Query:

```sh
python main.py query "what is attention?"
```

Note: this example **does not create a vector index**; queries will do a sequential scan.
