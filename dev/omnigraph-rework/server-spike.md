# Omnigraph server spike — probe findings

Answers to the Epic A probes in `SPEC.md` §4. Every answer records the exact request and the
exact response. An answer sourced from reading Omnigraph source is not an answer until the
binary confirms it.

## Environment

- `omnigraph-server` / `omnigraph` 0.10.0 from `test/bin/` (macOS arm64).
- rustfs `1.0.0-rc.5` (`rustfs-macos-aarch64-v1.0.0-rc.5.zip`, sha256 verified against the
  release `SHA256SUMS`), at `test/bin/rustfs`. No Homebrew formula exists; all rustfs releases
  are prereleases, so `releases/latest` returns 404 — fetch a specific tag.
- rustfs: `rustfs server --address 127.0.0.1:9100 --access-key cocotest --secret-key cocotestsecret ./scratch/rustfs-data`
- Bucket `omnigraph` created with boto3, path-style addressing.
- omnigraph reads standard AWS SDK / `object_store` env vars. Working set:
  `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL`, `AWS_REGION`, `AWS_ALLOW_HTTP=true`.
- Both servers must run with the Claude Code sandbox disabled — neither can bind a port inside it.

## P1 — Does a storage-root-booted server accept HTTP schema apply?

**Answer: no. 409, identical refusal to the cluster-dir boot. There is no server mode that accepts it.**

First, a finding that reframes the question: **"config-free serving" is not config-free.** Booting
against a bare storage root with no prior cluster state fails outright:

```
$ omnigraph-server --cluster s3://omnigraph/probe --unauthenticated --bind 127.0.0.1:9110
Error:
   0: the cluster at 's3://omnigraph/probe' is not ready to serve:
        [cluster_state_missing] __cluster/state.json: no cluster state ledger;
        run `cluster import` and `cluster apply` first
```

The storage-root boot is not an alternative to cluster configuration — it is the *same* cluster,
with the state ledger living in the object store instead of a local directory. `cluster.yaml`
accepts a `storage:` key naming the root:

```yaml
version: 1
storage: s3://omnigraph/probe
graphs:
  probe:
    schema: graphs/probe.pg
```

`omnigraph cluster import --config <dir>` then `cluster apply --config <dir>` write
`s3://omnigraph/probe/__cluster/state.json` and create the graph (`applied_count: 2`,
`converged: true`). The server then boots and serves it:

```
$ curl http://127.0.0.1:9110/healthz
{"status":"ok","version":"0.10.0","internal_schema_version":6}
$ curl http://127.0.0.1:9110/graphs/probe/schema
{"schema_source":"node Person {\n  coco_key: String @key\n  name: String\n}\n"}
```

The probe itself, adding a nullable property to the served schema:

```
$ curl -X POST http://127.0.0.1:9110/graphs/probe/schema/apply \
    -H 'Content-Type: application/json' \
    -d '{"schema_source":"node Person {\n  coco_key: String @key\n  name: String\n  nickname: String?\n}\n"}'

{"error":"server-side schema apply is disabled for cluster-backed serving; update the cluster config, run `omnigraph cluster apply`, and restart the server.","code":"conflict"}
HTTP 409
```

**Why this generalises.** The refusal is scoped to "cluster-backed serving", and `--cluster` is the
server's *only* boot source (`omnigraph-server --help`: "The server's only boot source (RFC-011
cluster-only)"). There is no `--store` flag on the server. Every served graph is therefore
cluster-backed, so `POST /schema/apply` is disabled unconditionally. The endpoint exists in the
OpenAPI document, but no server configuration accepts it.

**Consequence:** `managed_by="system"` cannot be implemented over HTTP by any means available to
the connector. Since every other cocoindex connector supports `SYSTEM` and defaults to it, dropping
the feature was rejected (decision 32); Epic D keeps it through the `cluster apply` control plane
instead, behind an `apply_schema()` seam so a future server-side apply (D3) replaces it in one
place.

## P2 — Is there any HTTP path that creates a graph?

**Answer: no.**

```
$ curl -X POST http://127.0.0.1:9110/graphs -d '{"graph_id":"newgraph"}'
HTTP 405            (empty body)

$ curl http://127.0.0.1:9110/graphs/nosuchgraph/schema
{"error":"graph 'nosuchgraph' not found","code":"not_found"}
HTTP 404
```

The full served path list contains no graph-creation route — `/graphs` is `GET` only:

```
GET      /graphs
GET,HEAD /graphs/{graph_id}/blob
GET,POST /graphs/{graph_id}/branches
POST     /graphs/{graph_id}/branches/merge
DELETE   /graphs/{graph_id}/branches/{branch}
POST     /graphs/{graph_id}/change
GET      /graphs/{graph_id}/changes
POST     /graphs/{graph_id}/changes/baseline
GET      /graphs/{graph_id}/commits
GET      /graphs/{graph_id}/commits/{commit_id}
GET      /graphs/{graph_id}/commits/{commit_id}/changes
POST     /graphs/{graph_id}/export
POST     /graphs/{graph_id}/ingest
POST     /graphs/{graph_id}/load
POST     /graphs/{graph_id}/load/ndjson
POST     /graphs/{graph_id}/mutate
POST     /graphs/{graph_id}/mutate/if-graph-commit
GET      /graphs/{graph_id}/queries
POST     /graphs/{graph_id}/queries/{name}
POST     /graphs/{graph_id}/queries/{name}/if-graph-commit
POST     /graphs/{graph_id}/query
POST     /graphs/{graph_id}/read
GET      /graphs/{graph_id}/schema
POST     /graphs/{graph_id}/schema/apply    (409 — see P1)
GET      /graphs/{graph_id}/snapshot
GET      /healthz
```

**Consequence:** graph provisioning is permanently an operator task (`cluster.yaml` →
`cluster apply` → restart). The connector's uninitialized-graph branch becomes a clear failure
naming the graph, the base URL, and the provisioning procedure — never an attempt to create one.
The 404 body above is the signal to key on.

`GET /graphs` is served here because the server runs `--unauthenticated` with no policy bundle;
decision 15 (never call it) still stands, since it is forbidden by default under a policy.

## P12 — Does a storage-root server pick up an applied revision without a restart? (added)

**Answer: no. `cluster apply` succeeds and the store is updated, but a running server keeps serving
the old schema indefinitely. A restart picks it up.**

Served schema before:

```
{"schema_source":"node Person {\n  coco_key: String @key\n  name: String\n}\n"}
```

Added a nullable property to `graphs/probe.pg`, then:

```
$ omnigraph cluster apply --config ../scratch/p1probe --json
ok: True | converged: True | applied: 1
   graph.probe   update  derived
   schema.probe  update  applied
```

Polled `GET /graphs/probe/schema` every 3s for 60s — the served source never changed. After
`pkill omnigraph-server` and an identical re-boot, the same request returns:

```
{"schema_source":"node Person {\n  coco_key: String @key\n  name: String\n  nickname: String?\n}\n"}
```

**Two consequences.**

1. A restart is unavoidable for any connector-driven schema change, on either boot mode. There is
   no polling, no signal, no in-place reload.
2. **`cluster apply` never contacted the server.** It ran from a plain config directory on a
   different host from the one serving, wrote `__cluster/state.json` into the bucket, and the
   server only learned about it at boot. So the control plane needs the CLI binary, the config
   directory, and object-store credentials — but *not* colocation with the server. That is a
   materially weaker requirement than the original spec assumed, and it means a cocoindex worker
   can drive schema itself as long as something else can restart the server.

## Probes not yet run

P3 (write/merge serialization), P4 (merge conflict shape), P5 (endpoint-not-found wording over
HTTP), P6 (entity cap status), P7 (mixed upsert/delete), P8 (branch delete consent), P9
(`delete_branch: true`), P10 (Cedar actions), P11 (`if-graph-commit` 412 body).
