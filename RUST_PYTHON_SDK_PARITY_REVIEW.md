# Rust SDK vs Python SDK Parity Review

Branch: `rust-sync`
Last reviewed: 2026-06-02 against the local worktree and `origin/main`.

Reference model:

- Python SDK source: `python/cocoindex`
- Rust SDK source: `rust/sdk/cocoindex` and `rust/sdk/cocoindex_macros`
- Public docs: `https://cocoindex.io/docs/programming_guide/sdk_overview/`
- Python remains the behavior reference; Rust does not need identical syntax when
  Rust types make a different surface clearer.

Scale marker:

- Python: 839 `test_*` functions under `python/tests`.
- Rust SDK: 213 integration + 170 source-unit + 19 macro tests. Many connector
  e2e tests are gated by service credentials (run when the service is up).

## Summary

Connector and source parity is **functionally complete**: every non-empty Python
connector has a Rust equivalent (the only Python connector without one is
`notion`, an empty stub in Python too), and the implemented connectors use the
same declarative source/target semantics. The core SDK shape — `App`, `Ctx`,
`#[function]`, memoization, target state, live components, shared file
resources, ID/schema/chunk resources, `#[derive(SchemaFields)]` schema
derivation — is in place.

What remains is **not** missing behavior; it is external-dependency-gated
coverage, a few explicit product decisions, and optional polish:

1. **Service/version-gated tests** — Doris `USING ANN` vector index (needs Doris
   3.x; DDL unit-covered) and hosted named-vector e2e for Qdrant/Turbopuffer
   (need credentials). Python gates these the same way.
2. **One framework edge test** — OCI live blocked-ready cancellation.
3. **Optional extensions** — apply `#[derive(SchemaFields)]` to graph/vector
   connectors; example symmetry (see [Example Parity](#python--rust-example-parity)).
4. **Product decisions** — CLI / default-environment / settings / runner APIs
   (deliberately deferred; see [P3](#p3-product-level-python-apis)).

Items that were investigated and are **N/A by design** (do not re-open): Kafka
consumer-group rebalance (the Rust source consumes all partitions directly, no
group membership), and the Iggy keyed `topic_as_map` being per-partition
(matches Python's per-partition keyed consumption; the keyless `topic_as_stream`
reads all partitions).

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

Decision (2026-06-02): **async `mount_table_target` is the canonical, Python-
consistent path** — Python's SDK is async-first, so the async mount matches its
semantics. The sync `declare_table_target` stays as a same-component convenience
with its documented schema-keyed-provider caveat; first-class pending
child-provider support is deferred until a concrete need appears (it is an
internal-ergonomics nicety, not a parity gap).

### P1: Missing Or Incomplete Sources

| Source | Rust status | Remaining |
| --- | --- | --- |
| Iggy | Keyed map (`topic_as_map`, per-partition `LiveMapView<String, Vec<u8>>`, `scan` compaction + `watch` tail, `is_deletion`) and keyless `topic_as_stream` over **all partitions** (`partition:offset` child keys, enumerated via `get_topic`). 4 live e2e incl. 3-partition read. | Keyed map is per-partition by design (matches Python). |
| Kafka | Keyed map over **all partitions** (per-partition offsets, tombstone/custom deletes) and keyless `topic_as_stream` with `partition:offset` child keys. 4 live e2e. | Consumer-group rebalance is N/A — the SDK consumes all partitions directly, no group membership to rebalance. |
| OCI Object Storage | Static list/read/range plus live `list_objects_live` → `LiveMapView<String, OciFile>` (scan with `eventTime` cutoff, event-stream watch, `HEAD` re-read, full filter chain). 7 hermetic e2e (`tests/oci_live.rs`: scan/create/delete/cutoff/cross-bucket/malformed, max-size, path-matcher, scan-failure). | Blocked-ready cancellation edge test. |
| Google Drive | Static file listing/read/export. | No live work unless Python adds live Google Drive support. |
| S3 | Static file listing/read/range, MinIO e2e. | No live work unless Python adds S3 live behavior. |

### P1: Missing Connector

| Connector | Rust status | Remaining |
| --- | --- | --- |
| Doris | **Done** — `doris::{DorisConnection, DorisConfig, TableSchema, ColumnDef, VectorIndexDef, InvertedIndexDef}` with `table_target`/`declare_*`/`mount_*`, composite (PK + per-column) tracking, DDL/DELETE over the MySQL protocol, Stream Load ingestion (FE→BE `307` redirect, retry/backoff), DUPLICATE KEY delete-before-insert upserts. 8 unit + 6 live e2e vs Dockerized `apache/doris:doris-all-in-one-2.1.0`. | Live `USING ANN` vector-index test (needs Doris 3.x — 2.1 rejects the syntax; DDL is unit-tested). |
| Notion | Absent — Python connector is an empty stub (`connectors/notion/` has no source on this branch). | No Rust work until Python ships a real connector. |

### P2: Vector Store Parity

| Connector | Rust status | Python delta | Action |
| --- | --- | --- | --- |
| Qdrant | Collection target, provider-based schema constructors, unnamed/named dense vectors, unnamed/named multivectors, **`f16` datatype** (`Datatype::Float16` from `VectorSchema::f16`), search helpers | Rust coverage is unit-level (incl. f16 datatype mapping) plus gated live unnamed-vector e2e | Add hosted e2e coverage for named vectors/multivectors when a Qdrant service is available. |
| Turbopuffer | Namespace target, provider-based schema constructors, unnamed/named vector fields, `f32`/`f16` schema rendering, named-field search helper | Rust coverage is unit-level plus gated live unnamed-vector e2e | Add hosted e2e coverage for named-vector writes/search when service credentials are available. |
| LanceDB | Table target, vector columns, search, additive scalar evolution, destructive replacement with row replay on async mount | Python has optimize/retry knobs and richer schema construction | Add optimize/retry/error-path coverage only when Rust exposes those knobs. |

### P2: Database And Graph Schema Ergonomics

Python builds connector schemas from dataclasses and annotations
(`TableSchema.from_class`, column overrides, vector schema providers).

**Done for the table connectors:** `#[derive(SchemaFields)]`
([`cocoindex_macros`]) + `TableSchema::from_row::<T>(primary_key)` on Postgres,
SQLite, and Doris derive a schema from a Rust row struct, mapping each field via
the same leaf-type table as Python's per-connector `from_class`
(`row_schema::LogicalType` is the connector-agnostic intermediate). `Option<T>`
→ nullable; `#[coco(vector = N[, half])]`, `#[coco(type = "…")]`,
`#[coco(json)]`, and `#[coco(rename = "…")]` are the field attributes (the Rust
analogues of `VectorSchema` / `PgType`/`SqliteType`/`DorisType`). The explicit
`ColumnDef` API stays the stable low-level path. Tests:
`tests/schema_from_row.rs` (per-connector schema equality + SQLite/Doris
round-trip).

Remaining:

- Extend the same derive to graph (Neo4j/FalkorDB) and vector-store (Qdrant /
  Turbopuffer / LanceDB) connectors if their schema shapes warrant it.
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

## Test Backlog (remaining only)

Kafka, Iggy, OCI, Doris, Postgres, SQLite, SurrealDB, Neo4j, and FalkorDB all
have live/hermetic e2e plus unit coverage; the items below are the only gaps.

1. OCI live source: blocked-ready cancellation edge test.
2. Doris target: `USING ANN` vector index (needs Doris 3.x).
3. Qdrant/Turbopuffer: hosted named-vector / multivector e2e (needs credentials).
4. SQLite vec0 e2e gated on the `sqlite-vec` extension being present in CI.
5. Product API tests only after deciding to build CLI/default-env/settings.

## Per-Connector Behavioral Audit (2026-06-02)

Each connector's generated DDL / reconcile / value-encoding was diffed against its
Python reference. Most categories match (CREATE TABLE/upsert/vector-index DDL,
metric mappings, fingerprint reconcile, managed_by gating, delete semantics).

### Fixed in this pass (contained correctness bugs, tested)

| Connector | Bug | Fix |
| --- | --- | --- |
| Doris | `RowHandler::reconcile` used `prev.iter().any(== fp)` — skipped a row whose previous fingerprints disagree | now `!prev.is_empty() && prev.iter().all(== fp)`, matching Kafka/Iggy/Python (`doris.rs`) |
| SQLite | `CREATE TABLE` did not force `NOT NULL` on PK columns (Python does) — non-rowid PKs could hold NULL | PK columns now `NOT NULL` (`sqlite.rs` `create_table_sql`) |
| Neo4j/FalkorDB | multi-field node-index name joined fields with `_` vs Python's `__` → divergent index names | `node_index_name` now joins with `__` (`cypher_graph.rs`) |
| Postgres | `bytea` column with a `Vec<u8>` produced invalid `'[104,105]'::bytea` | hex `'\x..'::bytea` encoder (`postgres.rs` `bytea_literal`); round-trip e2e added |

### Open findings (real divergences, not yet fixed — larger refactors / Python-side)

Priority order. Each is a genuine behavioral divergence; none is a missing connector.

1. **Postgres reconcile (high):** `TableHandler` tracks the whole `TableSpec` + plain
   `diff` (`postgres.rs` ~`reconcile`), so it never emits `Destructive` child
   invalidation and a **primary-key change is silently ignored** (`CREATE TABLE IF
   NOT EXISTS` never alters the PK). `reconcile_columns` (information_schema) adds/drops
   columns but **never retypes** (no `ALTER COLUMN TYPE` / drop-retry). Python uses
   composite tracking (`diff_composite`) for exact retype + PK-change→drop+recreate.
   Fix: adopt the composite pattern already used by `sqlite.rs`/`doris.rs`, or extend
   `reconcile_columns` with type comparison (note: information_schema type
   normalization is fragile — composite tracking is the cleaner route).
2. **SurrealDB reconcile (high):** same whole-`TableSpec` + `DEFINE TABLE/FIELD IF
   NOT EXISTS` shape (`surrealdb.rs`) → column drop/retype silently not applied; no
   incremental `DEFINE/REMOVE FIELD`. Also: `child_invalidation` is `Lossy` on a
   table replace where Python uses `destructive` (and Rust never issues `REMOVE
   TABLE` on replace); the normal-table UPSERT `CONTENT` does not strip the `id`
   field (the relation path does); relation auto-id uses a length-prefixed scheme
   vs Python's underscore-join (cross-SDK id mismatch — Rust's is collision-safe).
   Fix: composite tracking + per-field DEFINE/REMOVE; align `child_invalidation`;
   strip `id` from CONTENT; pick one canonical relation-id scheme.
3. **LanceDB additive evolution (high, data-loss):** adding a **vector** column
   triggers a destructive table recreate (`sql_null_type()` is `None` for vectors,
   `lancedb.rs`) — existing rows are wiped — whereas Python adds a nullable vector
   column additively (`add_columns`). Fix: evolve vector columns via the Arrow
   field `add_columns` API instead of a SQL `NULL` cast. Also: sync
   `declare_table_target` uses a schema-fingerprint-keyed *root* row provider
   instead of a child provider (diverges from Python and from Rust SQLite); and
   there is no background `optimize` / `num_transactions_before_optimize` option.
4. **Qdrant (medium):** f16 — Rust sets `Datatype::Float16` but Python ignores the
   schema `dtype` (a latent Python bug; the Rust comment falsely claims parity).
   Point ids are `u64`-only in Rust while Python accepts `str`/UUID (capability gap
   + cross-SDK key incompatibility). Fix: make Python honor f16; widen the Rust
   point id to int-or-UUID.

### Acceptable by design (not bugs — do not "fix")

- **Literal value encoding vs Python's bound parameters** (postgres/sqlite/doris build
  SQL literals; Python binds). Equivalent results; the one real gap (postgres `bytea`)
  is fixed above. SQLite `quote_string` strips `\0` from text (an inline-literal
  limitation) where Python's bind preserves it — documented, low impact.
- **No per-connector `encoder` layer** (Doris/SurrealDB/Postgres): the Rust row's
  `Serialize` impl is the single source of truth, replacing Python's `ColumnDef.encoder`.
- **Kafka/Iggy `is_deletion`/`key_fn` receive raw payload bytes** in Rust vs the full
  message object in Python; live-map values are `Vec<u8>` not the message. Idiomatic.
- **Container-delete `child_invalidation`** is `Destructive` in Rust vs `None` in
  Python (Qdrant/Turbopuffer) — cosmetic (the container is dropped regardless).
- **Rust-only search helpers** (Qdrant/Turbopuffer/LanceDB) and **stricter quoting**
  (LanceDB/Doris delete predicates escape quotes; Python has a latent quoting bug).

## Python ↔ Rust Example Parity

Verified 2026-06-02. 23 examples have matched Python (`examples/<name>`) and Rust
(`examples/rust/<name>`) pairs:

`amazon_s3_embedding`, `audio_to_text`, `code_embedding`, `code_embedding_lancedb`,
`conversation_to_knowledge`, `csv_to_kafka`, `files_transform`,
`gdrive_text_embedding`, `hn_trending_topics`, `image_search`,
`image_search_colpali`, `meeting_notes_graph_falkordb`,
`meeting_notes_graph_neo4j`, `multi_codebase_summarization`,
`oci_object_storage_embedding`, `paper_metadata`, `pdf_embedding`,
`pdf_to_markdown`, `postgres_source`, `text_embedding`, `text_embedding_lancedb`,
`text_embedding_qdrant`, `text_embedding_turbopuffer`.

### Python-only examples (no Rust counterpart)

| Example | Reason | Action |
| --- | --- | --- |
| `entire_session_search` | De-scoped for Rust — overlaps code embedding and depends on source shapes that are still evolving. | Non-goal; do not re-add `examples/rust/entire-session-search`. |
| `patient_intake_extraction_baml` | Depends on BAML (Python-only library). | Non-goal unless a Rust equivalent appears. |
| `patient_intake_extraction_dspy` | Depends on DSPy (Python-only framework). | Non-goal unless a Rust equivalent appears. |
| `kafka_to_lancedb` | Rust covers Kafka consumption via `kafka-consume` (live-map), not a LanceDB-sink variant. | Optional: add a Rust Kafka→LanceDB example, or leave `kafka-consume` as the canonical Kafka demo. |

`notion_target_basics` was removed 2026-06-02 — it had no committed source, only
stale `.venv`/`__pycache__`/`cocoindex.db` artifacts (the Notion connector is an
empty stub). Restore it if/when a real Notion connector lands.

### Rust-only examples (no Python counterpart)

| Example | What it is | Action |
| --- | --- | --- |
| `csv-to-iggy` | CSV → Iggy producer (Python only has `csv_to_kafka`). | Optional: add a Python `csv_to_iggy` for symmetry. |
| `files-to-sqlite` | SQLite target demo (Python has the `sqlite` connector but no example). | Optional: add a Python `files_to_sqlite` to exercise the connector. |
| `kafka-consume` | Kafka live-map consume demo (Python's analogue is `kafka_to_lancedb`). | Keep; pairs conceptually with the Python Kafka example. |

The LanceDB examples use the async `mount_table_target` API.

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
