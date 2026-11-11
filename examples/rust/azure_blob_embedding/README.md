# Azure Blob Storage Text Embedding (Rust)

Rust port of the Python [`azure_blob_embedding`](../../azure_blob_embedding) example.

Lists Markdown blobs from an Azure Blob Storage container, chunks each file,
embeds the chunks, and stores them in Postgres/pgvector — then serves
similarity search.

## Parallel to the Python example

| Concern          | Python                                   | Rust (this example)                                           |
| ---------------- | ---------------------------------------- | ------------------------------------------------------------- |
| Source           | `azure_blob.list_blobs` (Azure SDK)      | `cocoindex::connectors::azure_blob::list_blobs` (REST API)    |
| Authentication   | `DefaultAzureCredential`                 | account key, SAS token, Entra ID access token, or `TokenCredential::Default` (the `DefaultAzureCredential` sources) |
| Per-file compute | `@coco.fn(memo=True) process_file`       | `#[cocoindex::function(memo)] process_file`                   |
| Chunking         | `RecursiveSplitter` (markdown, 2000/500) | `RecursiveSplitter` (markdown, 2000/500)                      |
| Embeddings       | `sentence-transformers/all-MiniLM-L6-v2` | `SentenceTransformerEmbedder` (same model, 384-dim)           |
| Target           | `postgres.mount_table_target` + pgvector | `postgres::mount_table_target` + `declare_vector_index`       |

The source is one-shot (no live mode), matching the Python example. Each listed
`AzureBlobFile` reads itself, per-file memoization handles edits, and the managed
`TableTarget` reconciles away the rows of deleted blobs.

## Run

Start Postgres with pgvector (`docker compose -f ../../../dev/postgres.yaml up -d`),
then configure the container and one credential:

```bash
export AZURE_STORAGE_ACCOUNT=myaccount
export AZURE_STORAGE_CONTAINER=docs
export AZURE_BLOB_PREFIX=                          # optional folder inside the container

# One of, or none to get an Entra ID token as DefaultAzureCredential does
# (AZURE_TENANT_ID / AZURE_CLIENT_ID / AZURE_CLIENT_SECRET, a managed
# identity, or the `az login` account):
export AZURE_STORAGE_KEY=<account-key>
export AZURE_STORAGE_SAS_TOKEN='<sas-token>'       # needs the read and list permissions
export AZURE_STORAGE_ACCESS_TOKEN=$(az account get-access-token \
  --resource https://storage.azure.com/ --query accessToken -o tsv)

export POSTGRES_URL=postgres://cocoindex:cocoindex@localhost/cocoindex

cargo run -- index                  # list **/*.md -> embed -> Postgres
cargo run -- query "your query"     # cosine similarity search
```

An access token needs the `Storage Blob Data Reader` role on the container. The
connector does not renew it, so it suits a one-shot run.

To run against the [Azurite](https://github.com/Azure/Azurite) emulator, point
the example at it with its development account:

```bash
export AZURE_STORAGE_ACCOUNT=devstoreaccount1
export AZURE_STORAGE_ACCOUNT_URL=http://127.0.0.1:10000/devstoreaccount1
export AZURE_STORAGE_KEY='Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=='
```

Re-run `cargo run -- index` to pick up changed, added, or deleted blobs.
