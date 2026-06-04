# Project Setup Guide

Setting up CocoIndex projects for different use cases.

## Creating a New Project

```bash
cocoindex init my-project
cd my-project
```

This creates: `main.py`, `pyproject.toml`, `.env`, `README.md`.

```bash
pip install -e .
```

## Dependencies by Use Case

### Vector Embedding Pipeline

```toml
[project]
dependencies = [
    "cocoindex>=1.0.0",
    "sentence-transformers",
    "asyncpg",
]
```

### PostgreSQL Integration

```toml
[project]
dependencies = [
    "cocoindex>=1.0.0",
    "asyncpg",
]
```

### SQLite Integration

```toml
[project]
dependencies = [
    "cocoindex>=1.0.0",
    "sqlite-vec",
]
```

### LanceDB Integration

```toml
[project]
dependencies = [
    "cocoindex>=1.0.0",
    "lancedb",
]
```

### Qdrant Integration

```toml
[project]
dependencies = [
    "cocoindex>=1.0.0",
    "qdrant-client",
]
```

### Kafka Integration

```toml
[project]
dependencies = [
    "cocoindex>=1.0.0",
    "confluent-kafka",
]
```

### LLM-Based Extraction

```toml
[project]
dependencies = [
    "cocoindex>=1.0.0",
    "litellm",
    "instructor",
    "pydantic>=2.0",
    "asyncpg",
]
```

---

## Environment Configuration

### `.env` File

CocoIndex automatically loads `.env` from the current directory.

```bash
# CocoIndex internal database (required)
COCOINDEX_DB=./cocoindex.db

# PostgreSQL (if using)
POSTGRES_URL=postgres://user:pass@localhost/db

# Qdrant (if using)
QDRANT_URL=http://localhost:6333

# API keys (if using LLM extraction)
OPENAI_API_KEY=sk-...
ANTHROPIC_API_KEY=sk-ant-...
```

### Manual Settings (in lifespan)

```python
@coco.lifespan
def coco_lifespan(builder: coco.EnvironmentBuilder) -> Iterator[None]:
    builder.settings.db_path = pathlib.Path("./custom.db")
    yield
```

---

## Running Your Pipeline

```bash
pip install -e .                    # Install dependencies
cocoindex update main.py            # Run pipeline
cocoindex update main.py -L         # Run in live mode
cocoindex show main.py              # Show component paths
cocoindex drop main.py -f           # Reset everything
```

---

## Common Issues

### Import Errors

```bash
pip install -e .
```

### Database Connection Errors

Verify database is running and `.env` has correct URLs. See [setup_database.md](./setup_database.md).

---

## See Also

- [Database Setup](./setup_database.md)
- [Patterns](./patterns.md)
- [API Reference](./api_reference.md)
