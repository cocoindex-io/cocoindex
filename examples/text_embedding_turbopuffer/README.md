# Text Embedding with Turbopuffer (v1)

This example embeds local markdown files, stores the chunks + embeddings in a [Turbopuffer](https://turbopuffer.com/) namespace, and provides a simple semantic-search query demo.

## Prerequisites

Set your Turbopuffer credentials in the environment (e.g. via a `.env` file):

```sh
export TURBOPUFFER_API_KEY=tpuf_...
export TURBOPUFFER_REGION=gcp-us-central1   # optional; this is the default
```

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
