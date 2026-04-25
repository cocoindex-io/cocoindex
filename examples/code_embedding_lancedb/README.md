# Code Embedding with LanceDB (v1)

This example extracts code chunks from local Python, Rust, TOML, and Markdown files, stores the code and their vector embeddings in LanceDB, and provides a simple semantic search demo for code.

## Key Features

- **No database setup needed**: LanceDB is embedded - no external database required
- **Vector search**: Semantic code search using sentence embeddings
- **Full-text search**: FTS index on code content for keyword search
- **Portable**: Data stored in `./lancedb_data/` directory - just copy to move it
- **Syntax-aware chunking**: Uses RecursiveSplitter with language detection

## Data Storage

All data is stored in the `./lancedb_data/` directory in your project folder. This directory is created automatically on first run.

To start fresh, simply delete the `./lancedb_data/` directory and re-run the indexing.

## Run

Install dependencies:

```sh
pip install -e .
```

Build/update the index (stores data in `./lancedb_data/`):

```sh
cocoindex update main.py
```

Query interactively:

```sh
python main.py query
```

Or query with a specific search term:

```sh
python main.py query "embedding"
```
