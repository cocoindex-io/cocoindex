# Text Embedding with Turbopuffer (v1)

This example embeds local markdown files, stores the chunks + embeddings in a [Turbopuffer](https://turbopuffer.com/) namespace, and provides a simple semantic-search query demo.

## Prerequisites

Copy `.env.example` to `.env` and fill in your Turbopuffer API key:

```sh
cp .env.example .env
# then edit .env and set TURBOPUFFER_API_KEY=tpuf_...
```

The example loads variables from `.env` automatically via `python-dotenv`. `TURBOPUFFER_REGION` defaults to `gcp-us-central1` if you don't change it.

## Run

Install deps:

```sh
pip install -e .
```

Build/update the index:

```sh
cocoindex update main.py
```

Query:

```sh
python main.py "what is self-attention?"
```

Or run interactively:

```sh
python main.py
```
