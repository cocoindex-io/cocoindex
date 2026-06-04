# Amazon S3 Embedding (v1) 🪣

This example embeds markdown files from an S3 bucket, stores the chunks + embeddings in Postgres (pgvector), and provides a simple semantic-search query demo.

## Prerequisites

- A running Postgres with the pgvector extension. If you don't have one, start a local instance with the compose file in this repo:

  ```sh
  docker compose -f ../../dev/postgres.yaml up -d
  ```

- An S3 bucket (or S3-compatible service like MinIO) with markdown files
- AWS credentials configured (e.g. via `aws configure`, env vars, or IAM role)

Copy `.env.example` to `.env` and fill in your values:

```sh
cp .env.example .env
```

## Run

Install deps:

```sh
pip install -e .
```

Build/update the index (one-shot catch-up; the amazon_s3 source does not support live mode):

```sh
cocoindex update main
```

Query:

```sh
python main.py "what is self-attention?"
```

Note: this example **does not create a vector index**; queries will do a sequential scan.
