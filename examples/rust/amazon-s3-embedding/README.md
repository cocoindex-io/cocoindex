# Amazon S3 Text Embedding (Rust)

Rust port of the Python [`amazon_s3_embedding`](../../amazon_s3_embedding) example.

Lists markdown files from an S3 bucket (or an S3-compatible service like MinIO),
chunks each file, embeds the chunks, and stores them in Postgres/pgvector — then
serves similarity search.

## Parallel to the Python example

| Concern          | Python                                   | Rust (this example)                                  |
| ---------------- | ---------------------------------------- | ---------------------------------------------------- |
| Source           | `amazon_s3.list_objects` (aiobotocore)   | `cocoindex::amazon_s3::list_objects` (aws-sdk-s3)    |
| Per-file compute | `@coco.fn(memo=True) process_file`       | `#[cocoindex::function(memo)] process_file`          |
| Chunking         | `RecursiveSplitter` (markdown, 2000/500) | `cocoindex_ops_text` `RecursiveChunker` (markdown)   |
| Embeddings       | `sentence-transformers/all-MiniLM-L6-v2` | `fastembed` `AllMiniLML6V2` (same model, 384-dim)    |
| Target           | `postgres.mount_table_target` + pgvector | `postgres::mount_table_target` + `declare_vector_index` |

The S3 source is one-shot (no live mode), matching the Python example. Reads go
through `S3Client` (the listed `S3File` is a serializable metadata item, so
per-file memoization handles edits and the managed `TableTarget` reconciles away
rows for deleted objects).

## Run

Against real S3, or a local MinIO:

```bash
# Local MinIO:
docker run -d --name minio -p 9000:9000 \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data

export AWS_ENDPOINT_URL=http://localhost:9000     # omit for real AWS S3
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export S3_BUCKET=my-bucket
export S3_PREFIX=docs/                            # optional
export POSTGRES_URL=postgres://cocoindex:cocoindex@localhost/cocoindex   # pgvector-enabled

cargo run -- index                  # list s3://$S3_BUCKET/$S3_PREFIX/**.md -> embed -> Postgres
cargo run -- query "your query"     # cosine similarity search
```
