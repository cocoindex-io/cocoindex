````markdown
# Build text embedding and semantic search 🔍 with Pinecone

[![GitHub](https://img.shields.io/github/stars/cocoindex-io/cocoindex?color=5B5BD6)](https://github.com/cocoindex-io/cocoindex)

CocoIndex supports Pinecone as a target. In this example, we will build an index by embedding local markdown files and export vectors and metadata to a Pinecone index.

We appreciate a star ⭐ at [CocoIndex Github](https://github.com/cocoindex-io/cocoindex) if this is helpful.

## Steps

### Indexing Flow

1. Ingest local files from `markdown_files`.
2. For each file, perform chunking (recursively split) and then embedding.
3. Save the embeddings and metadata to a Pinecone index.

### Query

1. The `search()` query handler computes an embedding for the query and queries Pinecone using the `get_index()` helper.
2. We reuse the `text_to_embedding()` transform for both indexing and querying.

## Pre-requisites

1. CocoIndex requires a Postgres database for tracking pipeline state. Ensure Postgres is available and set `COCOINDEX_DATABASE_URL`.

2. Install dependencies:

	```sh
	pip install -e .
	```

3. Set your Pinecone API key in environment. See `.env.example`.

## Run

Update index (this will create the Pinecone index if needed):

```sh
cocoindex update main
```

Run with file-watching:

```sh
cocoindex update -L main
```

## Notes

- The example uses a transient auth entry created from `PINECONE_API_KEY`. The example will raise if `PINECONE_API_KEY` is not set.
- Pinecone index creation may be attempted automatically; ensure your API key has permissions.
- Pinecone has two types of indexes: Dense and Sparse. This example builds a Dense Index which is needed for Semantic Search

````
