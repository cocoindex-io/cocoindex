# OCI Object Storage Embedding (v1) ☁️

This example embeds markdown files from an Oracle Cloud Infrastructure (OCI) Object Storage bucket, stores the chunks + embeddings in Postgres (pgvector), and provides a simple semantic-search query demo.

It optionally subscribes to **OCI Streaming** (Kafka-protocol-compatible) for live updates: when configured, object create/update/delete events flowing through the stream trigger incremental re-processing without re-scanning the whole bucket.

## Prerequisites

- A running Postgres with the pgvector extension available
- An OCI Object Storage bucket with markdown files
- An OCI config file (default `~/.oci/config`) with API-key auth set up — see [OCI SDK config docs](https://docs.oracle.com/en-us/iaas/Content/API/Concepts/sdkconfig.htm)
- *(Optional, for live mode)* An OCI Streaming stream pool with object events from the bucket published to a topic, plus a streaming auth token

Copy `.env.example` to `.env` and fill in your values:

```sh
cp .env.example .env
```

## Run

Install deps:

```sh
pip install -e .
```

Build/update the index in catch-up mode (writes rows into Postgres and exits):

```sh
cocoindex update main.py
```

Run in **live mode** — performs an initial scan, then keeps watching the OCI Streaming topic and applies incremental updates:

```sh
cocoindex update -L main.py
```

Live mode requires `OCI_STREAMING_BOOTSTRAP_SERVERS`, `OCI_STREAMING_TOPIC`, `OCI_STREAMING_USERNAME`, and `OCI_STREAMING_AUTH_TOKEN` to be set in `.env`. With those unset, the connector skips live-stream subscription and just performs the catch-up scan.

### `OCI_STREAMING_USERNAME` format

`<tenancy-name>/<username>/<stream-pool-ocid>` — note the first two segments are **plain names, not OCIDs**:

- `tenancy-name`: e.g. `acme-corp` (NOT `ocid1.tenancy.oc1..…`). If your tenancy uses Identity Domains, the format is `<tenancy-name>/<domain-name>/<username>/<stream-pool-ocid>`.
- `username`: e.g. `alice@example.com` (NOT `ocid1.user.oc1..…`). For federated users, prefix with the IDP, e.g. `oracleidentitycloudservice/alice@example.com`.
- `stream-pool-ocid`: this one *is* an OCID, e.g. `ocid1.streampool.oc1.iad.aaaa…`.

The OCI Console pre-formats this exact string for you under **Streaming → Stream Pools → \<your pool\> → Kafka Connection Settings → "SASL Connection String"**. Just copy that value verbatim.

### `OCI_STREAMING_AUTH_TOKEN`

This is a per-user "Auth Token" — a one-shot credential, distinct from your Console password and from API signing keys. Generate via Console → **Profile → My profile → Auth tokens → Generate token**. Copy it immediately; it cannot be viewed again.

Query:

```sh
python main.py query "what is self-attention?"
```

Note: this example **does not create a vector index**; queries will do a sequential scan.

## How live mode works

OCI Streaming exposes a Kafka-compatible interface. The example builds a `confluent_kafka.aio.AIOConsumer` configured with `SASL_SSL` + `PLAIN` auth pointing at the streaming endpoint, wraps it via `cocoindex.connectors.kafka.topic_as_stream(...).payloads()` to get a `LiveStream[bytes]`, and passes that to `oci_object_storage.list_objects(..., live_stream=...)`. The connector then:

1. Snapshots a wall-clock cutoff (`now - 5s`) before the scan starts.
2. Runs the initial scan concurrently with stream consumption.
3. Drops streamed events whose `eventTime` predates the cutoff (the scan covers them).
4. For accepted post-cutoff events: re-reads the object via `head_object` to determine current state, then issues an authoritative `update` (object present) or `delete` (404) — event type is not trusted as a dispatch signal.

See `python/cocoindex/connectors/oci_object_storage/_source.py` for details.
