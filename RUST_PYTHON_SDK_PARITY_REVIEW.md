# Rust SDK vs Python SDK Parity Review

Branch: `rust-sync`
Last reviewed: 2026-06-03 against the local worktree and `origin/main`.

Reference model:

- Python SDK source: `python/cocoindex`
- Rust SDK source: `rust/sdk/cocoindex` and `rust/sdk/cocoindex_macros`
- Public docs: `https://cocoindex.io/docs/programming_guide/sdk_overview/`
- Python remains the behavior reference; Rust does not need identical syntax when
  Rust types make a different surface clearer.

Current scale marker:

- Python tests: 839 `test_*` functions under `python/tests`.
- Rust SDK tests: 376 Rust test annotations: 204 integration tests and 172
  SDK source-unit tests. Macro behavior is covered by the SDK integration
  suite.
- Many connector e2e tests are gated by service credentials. That is fine, but
  each connector still needs unit coverage for planning/reconcile behavior.

## Summary

Rust SDK parity is much better than the old review implied, but it is not done.
The core SDK shape is now in place: `App`, `Ctx`, `#[function]`, memoization,
target state, live components, shared file resources, ID/schema/chunk resources,
and several native target/source connectors.

The remaining gaps are concentrated in connector breadth and connector-specific
feature depth:

1. Doris target is implemented (table target, Stream Load ingestion, SQL
   deletes, inverted index, retry). `USING ANN` vector indexes need Doris 3.x
   for a live test; the DDL is unit-covered.
2. Iggy source has keyed-map parity and single-partition keyless payload-stream
   parity; multi-partition readiness remains.
3. Kafka source has all-partition keyed-map parity and all-partition keyless
   payload-stream parity; consumer-group rebalance handling remains.
4. OCI source has the live bucket-event view (`list_objects_live`); deeper
   cancellation/scan-failure edge tests remain.
5. Several database/graph connectors still use explicit Rust schemas where
   Python has `from_class`/annotation-driven schema construction.
6. CLI/default environment/settings/runner APIs are still product decisions, not
   connector blockers.

## Aligned SDK Concepts

These areas should not get more parity churn unless tests fail or Python
behavior changes.

| Area | Rust status |
| --- | --- |
| App/context/memo | `App`, async/blocking update/drop handles, stats, `ContextKey`, `Ctx::{memo,batch,map,scope,mount_each}`, exception handling, and max-inflight controls exist. |
| Target state | Public `TargetState`, `TargetStateProvider`, `TargetHandler`, `TargetActionSink`, `TargetReconcileOutput`, `ChildTargetDef`, `declare_target_state`, `declare_target_state_with_child`, `mount_target`, root provider registration, attachments, provider generations, and ownership transfer tests exist. |
| Managed target diffing | `ManagedBy`, `ManagedTargetOptions`, `MutualTrackingRecord`, `CompositeTrackingRecord`, `diff`, `diff_composite`, and system/user-managed transitions exist. |
| Live SDK | `LiveComponent`, `LiveComponentOperator`, `LiveMapFeed`, `LiveMapView`, `LiveMapSubscriber`, `mount_live`, `mount_each_live`, and handler propagation/swallow behavior exist. |
| Shared file resources | Rust has `file::{FilePath, FileMetadata, FileContentCache, FileLike, FileSourceItem, FilePathMatcher, MatchAllFilePathMatcher, PatternFilePathMatcher}` with async lazy metadata/read/fingerprint caching and stable memo keys. |
| LocalFS | `fs::walk_dir`, `DirWalker::items()`, path matchers, async `FileEntry`, directory target helpers, and feature-gated live watching via `DirWalker::live()` exist. |
| IDs/chunks/schema resources | `IdGenerator`, `UuidGenerator`, `Chunk`, `TextPosition`, `VectorSchema`, `MultiVectorSchema`, and providers exist. |
| Ops facades | Text splitting, sentence-transformer embedding, OpenAI-compatible embed/transcribe APIs, image embedding, and entity-resolution primitives exist behind features. |

## Priority Work

### P0: Keep Implemented Targets On The Generic Target-State Pattern

This is the main design rule from Python: target connectors should declare a
container target, expose child providers for rows/messages/records, and rely on
child invalidation for destructive schema changes.

Current status:

- Postgres, SQLite, SurrealDB, Neo4j, FalkorDB, Kafka, Iggy, Qdrant,
  Turbopuffer, LocalFS directory target, and the LanceDB `mount_table_target`
  path follow the pattern.
- LanceDB `declare_table_target` remains sync for same-component ergonomics. It
  uses a schema-keyed row provider because Rust cannot currently declare child
  rows through a pending provider the way Python can. Prefer async
  `mount_table_target` when parity semantics matter.

Action:

- Decide whether to make LanceDB declaration fully async or add first-class Rust
  pending child-provider support.

### P1: Missing Or Incomplete Sources

| Source | Python behavior | Rust status | Action |
| --- | --- | --- | --- |
| Iggy | `topic_as_stream`, payload stream, keyed map, offset readiness, multi-partition safeguards | **Keyed map + keyless payload stream done** — `IggyConsumer` + `topic_as_map`/`_with_options` give a `LiveMapView<String, Vec<u8>>` over one partition (`scan` compacts to latest-payload-per-key via the required `key_fn`; `watch` tails), with `IggySourceOptions::is_deletion`. `topic_as_stream` gives an append-only offset-keyed payload view. 3 live e2e (`tests/iggy_source.rs`: compaction, live tail, keyless stream). | Remaining: multi-partition readiness. |
| Kafka | stream, payload stream, keyed map, partition assignment/rebalance, safe offset readiness | `topic_as_map` over **all partitions** (per-partition offset tracking; `scan` compacts across partitions, `watch` round-robins) with tombstone/custom delete semantics. `topic_as_stream` reads keyless payloads over all partitions using stable `partition:offset` child keys. 4 live e2e (`tests/kafka_source.rs`: compaction, live tail, all-partitions read, keyless stream). | Remaining: consumer-group rebalance (the SDK consumes the topic directly, no group assignment). |
| OCI Object Storage | shared file source plus live bucket events fed by a stream | **Done** — static list/read/range source plus `list_objects_live(client, ns, bucket, options, events)` returning a `LiveMapView<String, OciFile>`: `scan` lists matching objects (snapshotting an `eventTime` cutoff), `watch` turns an event stream (`OciEventStream`) into per-object updates/deletes, re-reading each via `HEAD` (live state wins over event type), filtered by envelope/namespace/bucket/cutoff/prefix/matcher/max-size. `OciClient::with_base_url` adds a mock seam. 1 hermetic e2e (`tests/oci_live.rs`, wiremock: scan + create/delete/old-cutoff/cross-bucket/malformed). | Remaining: cancellation-while-blocked and scan-failure-propagation edge tests. |
| Google Drive | static file listing/read/export | Static source exists | No live work unless Python adds live Google Drive support. |
| S3 | static file listing/read/range | Static source exists with MinIO e2e | No live work unless Python adds S3 live behavior. |

### P1: Missing Connector

| Connector | Python status | Rust status | Action |
| --- | --- | --- | --- |
| Doris | Native target with stream load, retries, vector indexes, inverted indexes, and schema/type mapping | **Done** — `doris::{DorisConnection, DorisConfig, TableSchema, ColumnDef, VectorIndexDef, InvertedIndexDef}` with the `table_target`/`declare_*`/`mount_*` split and composite (PK + per-column) tracking. DDL/DELETE over the MySQL protocol (`sqlx`, unprepared text protocol — Doris only prepares point-query SELECT/INSERT), row ingestion via Stream Load (HTTP `PUT`, manual FE→BE `307` redirect with optional `be_load_host`, retry/backoff), DUPLICATE KEY model with delete-before-insert upserts. 8 unit + 6 live e2e (`tests/doris_target.rs`: create/insert, update, delete, map rows, no-change-no-dup, inverted index — verified vs a Dockerized `apache/doris:doris-all-in-one-2.1.0`). | Remaining: live `USING ANN` vector-index test (needs Doris 3.x — 2.1 rejects the syntax; DDL generation is unit-tested). |
| Notion | Empty Python connector | Missing | No Rust work until Python has a real connector. |

### P2: Vector Store Parity

| Connector | Rust status | Python delta | Action |
| --- | --- | --- | --- |
| Qdrant | Collection target, provider-based schema constructors, unnamed/named dense vectors, unnamed/named multivectors, **`f16` datatype** (`Datatype::Float16` from `VectorSchema::f16`), search helpers | Rust coverage is unit-level (incl. f16 datatype mapping) plus gated live unnamed-vector e2e | Add hosted e2e coverage for named vectors/multivectors when a Qdrant service is available. |
| Turbopuffer | Namespace target, provider-based schema constructors, unnamed/named vector fields, `f32`/`f16` schema rendering, named-field search helper | Rust coverage is unit-level plus gated live unnamed-vector e2e | Add hosted e2e coverage for named-vector writes/search when service credentials are available. |
| LanceDB | Table target, vector columns, search, additive scalar evolution, destructive replacement with row replay on async mount | Python has optimize/retry knobs and richer schema construction | Add optimize/retry/error-path coverage only when Rust exposes those knobs. |

### P2: Database And Graph Schema Ergonomics

Python often builds connector schemas from dataclasses and annotations:
`TableSchema.from_class`, column overrides, `PgType`, vector schema providers,
and graph/SQL type mapping. Rust mostly asks users to spell out `ColumnDef`
values explicitly.

This is acceptable as a Rust-first API, but examples should stay concise and
the behavior must be equivalent once a schema is declared.

Action:

- Add helper builders where they remove real repetition, for example
  `*_schema_from_row::<T>()` only if it can be type-safe and unsurprising.
- Keep explicit schemas as the stable low-level API.
- Add tests that compare generated DDL/reconcile behavior to Python for:
  Postgres type changes/drop retries, graph PK constraints/indexes, SurrealDB
  vector indexes, SQLite vec0, and LanceDB destructive replacement.

### P2: Graph Target Public Surface

Neo4j and FalkorDB now have table/relation targets, automatic PK
constraints/indexes, relation PK indexes, vector-index attachments, and node
property index attachments.

Record writes are now **parameter-bound**: `apply_record` emits `$param`
placeholders + a params map via `CypherExecutor::execute_with_params` (neo4j
binds Bolt params through `json_to_bolt`; falkordb uses the `CYPHER name=value`
header), so user values are never interpolated into the query body. Validated by
unit tests plus live neo4j/falkordb record-write e2e.

Remaining decision:

- Python exposes pure Cypher builder helpers for unit-testable DDL strings.
  Rust currently keeps most builders private inside `cypher_graph.rs`.
- Expose a small public builder module only if users need to inspect DDL or
  tests need to assert exact Cypher outside connector internals.

### P3: Product-Level Python APIs

These are not connector blockers. Decide explicitly before implementing.

| Python surface | Rust status | Recommendation |
| --- | --- | --- |
| CLI/user app loader | Python has CLI discovery/loading | Defer unless Rust apps become first-class deployed apps. |
| Default environment/lifespan | Python has implicit/default env and lifespan hooks | Keep Rust explicit by default; add only with a strong use case. |
| Settings/LmdbSettings env loading | Python has settings objects | Defer unless CLI/default env is accepted. |
| Inspect/stable path wrappers | Python exposes public inspection helpers | Add lightweight Rust wrappers if users need debugging/state inspection. |
| Runner/GPU/pickle/dynamic typing | Python-specific runtime support | Treat as non-goals unless an SDK feature requires them. |

## Test Backlog

Use this order:

1. Iggy source tests: offset readiness, duplicate offsets, delete predicate,
   and multi-partition guard. Keyed-map compaction, live tail, and keyless
   payload stream are covered.
2. Kafka source tests: consumer-group rebalance behavior. Keyed-map compaction,
   live tail, tombstone deletes, all-partition catch-up, and keyless payload
   stream are covered.
3. OCI live source tests: event cutoff, malformed events, cross-bucket filters,
   and the create/delete re-read are covered (`tests/oci_live.rs`, hermetic).
   Remaining: max-size/path-matcher live filters, blocked-ready cancellation,
   scan failure propagation.
4. Doris target tests: create/update/delete, map rows, inverted index, and
   no-change behavior are covered live. Remaining: `USING ANN` vector index
   (needs Doris 3.x).
5. Qdrant/Turbopuffer tests: add hosted named-vector e2e coverage when service
   credentials are available.
6. Database/graph schema tests: Postgres destructive schema retries, graph
   constraints/indexes, SurrealDB vector-index lifecycle, SQLite vec0 e2e when
   CI has `sqlite-vec`.
7. Product API tests only after deciding to build CLI/default-env/settings.

## Example Notes

The LanceDB examples use the async `mount_table_target` API. Do not add back
`examples/rust/entire-session-search`; it remains de-scoped for Rust because it
overlaps code embedding and depends on source shapes that are still evolving.

## Definition Of Done

Rust SDK parity means:

1. Every non-empty Python connector has a Rust equivalent or an explicit
   non-goal.
2. Implemented Rust connectors use the same declarative source/target semantics
   as Python.
3. Shared resources prevent examples from hand-rolling file, chunk, ID, vector,
   text, or embedder behavior.
4. Live source behavior exists where Python has live sources.
5. Every Python connector test family has a Rust counterpart, with e2e tests
   skipped only for explicit external-service requirements.
