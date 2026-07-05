# Azure Blob Storage Embedding

This example indexes Markdown files from an Azure Blob Storage container into Postgres with pgvector.

It is the same basic flow as the local text embedding example:

1. List matching Markdown blobs from a container.
2. Split each file into chunks.
3. Embed each chunk with `sentence-transformers/all-MiniLM-L6-v2`.
4. Store the chunks and vectors in Postgres.

## Requirements

- Azure CLI login or another `DefaultAzureCredential` source.
- An Azure Storage account and container.
- Read/list permission on the container. `Storage Blob Data Reader` is enough.
- Postgres with pgvector.

## Configure Azure

```bash
az login
az account set --subscription "<subscription-name-or-id>"
```

Upload a few `.md` files to your container. If you only want to index a folder inside the container, set `AZURE_BLOB_PREFIX`.

## Run it

Start Postgres:

```bash
docker compose -f ../../dev/postgres.yaml up -d
```

Configure the example:

```bash
cp .env.example .env
```

Edit `.env`:

```bash
AZURE_STORAGE_ACCOUNT=myaccount
AZURE_STORAGE_CONTAINER=docs
AZURE_BLOB_PREFIX=
```

Install dependencies:

```bash
pip install -e .
```

Build the index:

```bash
cocoindex update main
```

Query the index:

```bash
python main.py "what is in these docs?"
```

Re-run `cocoindex update main` to pick up changed, added, or deleted blobs.
