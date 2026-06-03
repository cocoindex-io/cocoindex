# OCI Object Storage Text Embedding (Rust)

Rust port of the Python [`oci_object_storage_embedding`](../../oci_object_storage_embedding)
example.

Lists markdown files from an Oracle Cloud Infrastructure (OCI) Object Storage
bucket, chunks each file, embeds the chunks, and stores them in
Postgres/pgvector — then serves similarity search.

## Parallel to the Python example

| Concern          | Python                                          | Rust (this example)                                       |
| ---------------- | ----------------------------------------------- | --------------------------------------------------------- |
| Source           | `oci_object_storage.list_objects` (`oci` SDK)   | `cocoindex::oci_object_storage::list_objects` (native REST)|
| Auth             | `oci.config.from_file` + `ObjectStorageClient`  | `OciClient::connect` reads `~/.oci/config`, signs requests |
| Per-file compute | `@coco.fn(memo=True) process_file`              | `#[cocoindex::function(memo)] process_file`               |
| Chunking         | `RecursiveSplitter` (markdown, 2000/500)        | `RecursiveSplitter` (markdown, 2000/500)                  |
| Embeddings       | `sentence-transformers/all-MiniLM-L6-v2`        | `fastembed` `AllMiniLML6V2` (same model, 384-dim)         |
| Target           | `postgres.mount_table_target` + pgvector        | `postgres::mount_table_target` + `declare_vector_index`   |

There is no official Oracle SDK for Rust, so the connector talks to the Object
Storage REST API directly and implements OCI's RSA-SHA256 HTTP Signature signing
(the same `~/.oci/config` API-key auth the Python SDK uses). The source is
one-shot (no live mode); the listed `OciFile` is a serializable metadata item, so
per-file memoization handles edits and the managed `TableTarget` reconciles away
rows for deleted objects.

## Prerequisites

- An `~/.oci/config` profile with an **unencrypted** API key. Generate one in the
  OCI console (Profile → API Keys → Add API Key) and download the config snippet
  and private key. It looks like:

  ```ini
  [DEFAULT]
  user=ocid1.user.oc1..aaaa
  fingerprint=aa:bb:cc:...
  tenancy=ocid1.tenancy.oc1..bbbb
  region=us-ashburn-1
  key_file=~/.oci/oci_api_key.pem
  ```

- A pgvector-enabled Postgres.

## Run

```bash
export OCI_NAMESPACE=my-namespace        # your Object Storage namespace
export OCI_BUCKET=my-bucket
export OCI_PREFIX=docs/                   # optional
# Optional overrides (defaults: ~/.oci/config and the DEFAULT profile):
# export OCI_CONFIG_FILE=~/.oci/config
# export OCI_PROFILE=DEFAULT
export POSTGRES_URL=postgres://cocoindex:cocoindex@localhost/cocoindex   # pgvector-enabled

cargo run -- index                  # list oci://$OCI_NAMESPACE/$OCI_BUCKET/$OCI_PREFIX/**.md -> embed -> Postgres
cargo run -- query "your query"     # cosine similarity search
```

## Not yet supported

- Live bucket watching (OCI Events / Streaming) — deferred until the Rust live
  source API is generalized, matching the Amazon S3 example.
- Pass-phrase-encrypted private keys — use an unencrypted API key.
