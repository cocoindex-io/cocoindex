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
- Rust SDK tests: 334 Rust test annotations: 179 integration tests, 136 SDK
  source-unit tests, and 19 macro tests.
- Many connector e2e tests are gated by service credentials. That is fine, but
  each connector still needs unit coverage for planning/reconcile behavior.

## Summary

Rust SDK parity is much better than the old review implied, but it is not done.
The core SDK shape is now in place: `App`, `Ctx`, `#[function]`, memoization,
target state, live components, shared file resources, ID/schema/chunk resources,
and several native target/source connectors.

The remaining gaps are concentrated in connector breadth and connector-specific
feature depth:

1. Doris target is missing.
2. Iggy source has keyed-map parity; keyless stream/payload and multi-partition
   readiness remain.
3. Kafka source has all-partition keyed-map parity; keyless stream/payload and
   consumer-group rebalance handling remain.
4. OCI source lacks Python's live bucket-event view.
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
| Iggy | `topic_as_stream`, payload stream, keyed map, offset readiness, multi-partition safeguards | **Keyed map done** — `IggyConsumer` + `topic_as_map`/`_with_options` give a `LiveMapView<String, Vec<u8>>` over a partition (`scan` compacts to latest-payload-per-key via the required `key_fn`; `watch` tails), with `IggySourceOptions::is_deletion`. 2 live e2e (`tests/iggy_source.rs`: compaction + live tail, verified vs a Dockerized Apache Iggy server). | Remaining: keyless `topic_as_stream`/payload view and multi-partition readiness. |
| Kafka | stream, payload stream, keyed map, partition assignment/rebalance, safe offset readiness | `topic_as_map` over **all partitions** (per-partition offset tracking; `scan` compacts across partitions, `watch` round-robins) with tombstone/custom delete semantics. 3 live e2e (`tests/kafka_source.rs`: compaction, live tail, all-partitions read). | Remaining: keyless `topic_as_stream`/payload view; consumer-group rebalance (the SDK consumes the topic directly, no group assignment). |
| OCI Object Storage | shared file source plus live bucket events fed by a stream | Static list/read/range source exists; no live event view | Port Python's live event adapter/filtering tests from `test_oci_object_storage.py`. |
| Google Drive | static file listing/read/export | Static source exists | No live work unless Python adds live Google Drive support. |
| S3 | static file listing/read/range | Static source exists with MinIO e2e | No live work unless Python adds S3 live behavior. |

### P1: Missing Connector

| Connector | Python status | Rust status | Action |
| --- | --- | --- | --- |
| Doris | Native target with stream load, retries, vector indexes, inverted indexes, and schema/type mapping | Missing | Implement `doris` target after vector-schema-provider parity is settled; port `test_doris_target.py`. |
| Notion | Empty Python connector | Missing | No Rust work until Python has a real connector. |

### P2: Vector Store Parity

| Connector | Rust status | Python delta | Action |
| --- | --- | --- | --- |
| Qdrant | Collection target, provider-based schema constructors, unnamed/named dense vectors, unnamed/named multivectors, search helpers | Rust coverage is unit-level plus gated live unnamed-vector e2e | Add hosted e2e coverage for named vectors/multivectors when a Qdrant service is available. |
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

1. Iggy source tests: keyless stream/payload APIs, offset readiness, duplicate
   offsets, delete predicate, and multi-partition guard.
2. Kafka source tests: keyless stream/payload APIs and consumer-group rebalance
   behavior. Keyed-map compaction, live tail, tombstone deletes, and
   all-partition catch-up are covered.
3. OCI live source tests: event cutoff, malformed events, cross-bucket filters,
   max-size/path filters, blocked-ready cancellation, scan failure propagation.
4. Doris target tests: create/update/delete, dict rows, vector index, inverted
   index, retry/no-change behavior.
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
