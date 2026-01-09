# Text Embedding with LanceDB (v1)

This example embeds local markdown files, stores the chunks + embeddings in LanceDB, and provides a simple semantic-search query demo.

## Key Features

- **No database setup needed**: LanceDB is embedded - no external database required
- **Vector search**: Semantic text search using sentence embeddings
- **Full-text search**: FTS index on text content for keyword search
- **Portable**: Data stored in `./lancedb_data/` directory - just copy to move it

## Data Storage

All data is stored in the `./lancedb_data/` directory in your project folder. This directory is created automatically on first run.

To start fresh, simply delete the `./lancedb_data/` directory and re-run the indexing.

## Run

Install deps:

```sh
pip install -e .
```

Build/update the index (stores data in `./lancedb_data/`):

```sh
python main.py
```

Query:

```sh
python main.py query "what is self-attention?"
```
