# Database Setup Guide

Setup instructions for databases used with CocoIndex v1.

## PostgreSQL with pgvector

### Using Docker (Recommended)

```bash
cat > docker-compose.yml <<EOF
version: '3.8'

services:
  postgres:
    image: pgvector/pgvector:pg16
    container_name: cocoindex-postgres
    environment:
      POSTGRES_USER: cocoindex
      POSTGRES_PASSWORD: cocoindex
      POSTGRES_DB: cocoindex
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U cocoindex"]
      interval: 5s
      timeout: 5s
      retries: 5

volumes:
  postgres_data:
EOF

docker-compose up -d
```

Connection URL: `postgres://cocoindex:cocoindex@localhost:5432/cocoindex`

### Using Existing PostgreSQL

```bash
psql "postgres://user:password@localhost/dbname" \
  -c "CREATE EXTENSION IF NOT EXISTS vector;"
```

### Environment Configuration

```bash
# .env
POSTGRES_URL=postgres://cocoindex:cocoindex@localhost:5432/cocoindex
```

---

## SQLite with sqlite-vec

### Installation

```bash
pip install sqlite-vec
```

**Note for macOS**: Use Homebrew Python for extension support:

```bash
brew install python
```

### Usage in Code

```python
from cocoindex.connectors import sqlite

conn = sqlite.connect("./data.db", load_vec="auto")  # Auto-load sqlite-vec
conn = sqlite.connect("./data.db", load_vec=True)    # Require sqlite-vec
conn = sqlite.connect("./data.db", load_vec=False)   # No vector support
```

---

## LanceDB

### Installation

```bash
pip install lancedb
```

### Local Storage

No setup needed. Just specify a directory:

```python
conn = await lancedb.connect_async("./lancedb_data")
```

### Cloud Storage (S3, GCS)

```python
# AWS S3
conn = await lancedb.connect_async("s3://your-bucket/lancedb_data")

# Google Cloud Storage
conn = await lancedb.connect_async("gs://your-bucket/lancedb_data")
```

Configure credentials via environment variables (`AWS_ACCESS_KEY_ID`, `GOOGLE_APPLICATION_CREDENTIALS`, etc.).

---

## Qdrant

### Using Docker

```bash
cat > docker-compose.yml <<EOF
version: '3.8'

services:
  qdrant:
    image: qdrant/qdrant:latest
    container_name: cocoindex-qdrant
    ports:
      - "6333:6333"
      - "6334:6334"
    volumes:
      - qdrant_data:/qdrant/storage

volumes:
  qdrant_data:
EOF

docker-compose up -d
```

### Using Qdrant Cloud

```python
client = qdrant.create_client(
    url="https://your-cluster.qdrant.io",
    api_key="your-api-key",
    prefer_grpc=True,
)
```

### Environment Configuration

```bash
# .env
QDRANT_URL=http://localhost:6333
# QDRANT_API_KEY=your-key  # For Qdrant Cloud
```

---

## SurrealDB

### Using Docker

```bash
docker run --rm -p 8000:8000 surrealdb/surrealdb:latest \
  start --user root --pass root
```

### Usage in Code

```python
factory = surrealdb.ConnectionFactory(
    url="ws://localhost:8000/rpc",
    namespace="my_ns",
    database="my_db",
    credentials={"username": "root", "password": "root"},
)
```

---

## Multi-Database Setup

```yaml
# docker-compose.yml
version: '3.8'

services:
  postgres:
    image: pgvector/pgvector:pg16
    environment:
      POSTGRES_USER: cocoindex
      POSTGRES_PASSWORD: cocoindex
      POSTGRES_DB: cocoindex
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

  qdrant:
    image: qdrant/qdrant:latest
    ports:
      - "6333:6333"
      - "6334:6334"
    volumes:
      - qdrant_data:/qdrant/storage

volumes:
  postgres_data:
  qdrant_data:
```

```bash
# .env
POSTGRES_URL=postgres://cocoindex:cocoindex@localhost:5432/cocoindex
QDRANT_URL=http://localhost:6333
```

---

## Common Issues

### PostgreSQL Connection Refused

```bash
docker-compose ps          # Check if running
nc -zv localhost 5432      # Check port
```

### pgvector Extension Not Found

```bash
psql -U postgres -c "SELECT * FROM pg_available_extensions WHERE name = 'vector';"
```

Use the `pgvector/pgvector:pg16` Docker image which includes it.

### SQLite Extensions Not Loading

```python
import sqlite3
conn = sqlite3.connect(":memory:")
conn.enable_load_extension(True)  # Should not raise
```

If it fails, use Homebrew Python on macOS or ensure Python was built with `--enable-loadable-sqlite-extensions` on Linux.

### Qdrant Connection Issues

```bash
curl http://localhost:6333/       # Check health
docker logs cocoindex-qdrant      # Check logs
```

---

## See Also

- [Connectors Reference](./connectors.md)
- [Patterns Reference](./patterns.md)
