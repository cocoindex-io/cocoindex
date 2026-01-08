# Code Embedding with LanceDB (v1)

This example extracts code chunks from local Python, Rust, TOML, and Markdown files, stores the code and their vector embeddings in LanceDB, and provides a simple semantic search demo for code.

## Key Features

- **No database setup needed**: LanceDB is embedded - no external database required
- **Vector search**: Semantic code search using sentence embeddings
- **Full-text search**: FTS index on code content for keyword search
- **Portable**: Data stored in `./lancedb_data/` directory - just copy to move it
- **Syntax-aware chunking**: Uses RecursiveSplitter with language detection

## Prerequisites

None! LanceDB is an embedded database, so no external setup is required.

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
python main.py
```

Query interactively:

```sh
python main.py query
```

Or query with a specific search term:

```sh
python main.py query "embedding"
```

## How It Works

1. **Walk repository**: Finds all `.py`, `.rs`, `.toml`, `.md`, and `.mdx` files
2. **Detect language**: Identifies programming language from file extension
3. **Chunk code**: Splits code into semantic chunks with syntax awareness
4. **Embed chunks**: Generates vector embeddings using SentenceTransformers
5. **Store in LanceDB**: Saves chunks with embeddings and FTS index
6. **Vector search**: Queries using semantic similarity

## Comparison with Postgres Version

| Feature | Postgres Version | LanceDB Version |
|---------|------------------|-----------------|
| Setup | Requires Postgres + pgvector | No setup needed |
| Storage | External database | Local `./lancedb_data/` directory |
| Connection | Network connection string | Local file path |
| Portability | Database dump/restore | Copy directory |
| Vector search | SQL with `<=>` operator | Native `.search()` API |
| Full-text search | Not included | Built-in FTS support |

## Advantages of LanceDB

1. **Zero setup**: No database installation, no connection configuration
2. **Embedded**: All data in one directory (`./lancedb_data/`)
3. **Portable**: Just copy the directory to move your data
4. **Fast development**: No network latency, no connection management
5. **Cloud ready**: Can use S3/GCS URIs for production (e.g., `s3://bucket/path`)
6. **Built-in FTS**: Native full-text search without additional extensions

## Example Queries

Try these example searches:

- `"embedding"` - Find code related to embeddings
- `"table schema"` - Find table schema definitions
- `"async function"` - Find async function examples
- `"error handling"` - Find error handling patterns

## Notes

- The first run downloads the embedding model (~80MB) and caches it locally
- Subsequent runs reuse the cached model for faster execution
- The index is incremental - re-running updates only changed files
- LanceDB requires at least 256 rows before creating vector indexes
