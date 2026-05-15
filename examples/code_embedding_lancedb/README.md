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

Build/update the index (writes rows into LanceDB). Pick one of the two modes:

- **Catch-up run** — scan sources, sync changes, exit:

  ```sh
  cocoindex update main
  ```

- **Live run** — catch up, then keep watching for file changes (the source declares `live=True` in `main.py`):

  ```sh
  cocoindex update -L main
  ```

Query:

```sh
python main.py "embedding"
```
