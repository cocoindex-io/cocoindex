# Rust SDK vs Python SDK Parity Review

Current branch reviewed: `rust-sync`

This document is a living audit of the current Rust SDK against the Python SDK.
It focuses on public interfaces, connector design, and test coverage. The source
of truth is the current worktree, especially:

- Python SDK public APIs under `python/cocoindex`
- Python connector tests under `python/tests/connectors`
- Python core/runtime tests under `python/tests/core` and `python/tests/internal`
- Rust SDK public APIs under `rust/sdk/cocoindex/src`
- Rust SDK tests under `rust/sdk/cocoindex/tests`
- Rust examples under `examples/rust`

## Executive Summary

Rust SDK is not yet Python SDK parity. The Rust core contains much of the
underlying engine machinery, but the Rust SDK public surface is still much
narrower than Python's public surface.

The most important architectural gap was the lack of a public Rust equivalent
to Python's generic target-state API. Rust now exposes a first typed facade for:

- `TargetState`
- `TargetStateProvider`
- `TargetHandler`
- `TargetActionSink`
- `declare_target_state`
- `declare_target_state_with_child`
- `register_root_target_states_provider`
- `mount_target`

Rust has core-level equivalents and now exposes these names at the SDK layer.
**Phase 1 is now implemented**: in addition to the typed no-child facade, the
Rust SDK now also exposes child-handler generation (`ChildTargetDef` +
`TargetActionSink::from_async_fn_with_children`), public attachment handler
definitions (`TargetHandler::attachments`), and a foreground `mount_target` that
runs the parent target as a sub-component (via `use_mount`) so the returned child
provider is *ready* to declare child rows on immediately — matching Python's
`mount_target`/`use_mount` readiness boundary. A generic SDK-level test suite
(`tests/target_state.rs`) covers flat insert/update/no-change/delete, target
states declared inside components, `mount_target` child insert/delete, multiple
mounted child targets from one provider, provider generation
(destructive/lossy/none child invalidation), attachment create/cleanup, and
ownership transfer between component scopes. The remaining parity work is Phase
2: migrating existing connector implementations away from private `Ctx` helpers
onto this public shape.

The top-level public API difference is visible immediately:

- Python `cocoindex.__all__` re-exports the 76-name
  `python/cocoindex/_internal/api.py::__all__` surface.
- Rust `rust/sdk/cocoindex/src/lib.rs` flat-reexports only the app/context,
  entity resolution, error, fs, id, stats, and `#[function]` groups. Feature
  connectors are public modules, but most Python runtime/resource/connectorkit
  APIs have no top-level Rust counterpart.

Test coverage is similarly uneven:

- Python currently has 839 test functions under `python/tests`.
- Rust SDK currently has 117 integration test functions under
  `rust/sdk/cocoindex/tests`, and 168 SDK crate tests if unit tests under
  `rust/sdk/cocoindex/src` are included.
- Python connector tests alone account for 284 tests; Rust connector/source
  tests account for 9 tests in `gdrive_source.rs`, `kafka_target.rs`,
  `lancedb_target.rs`, `postgres_source.rs`, `postgres_target.rs`, and
  `surrealdb_target.rs`.

## Parity Status Legend

- `Good`: Rust has a comparable user-facing interface and meaningful tests.
- `Partial`: Rust has some interface or examples, but important behavior or API
  shape is missing.
- `Missing`: No native Rust SDK counterpart.
- `Different`: Rust intentionally uses an idiomatic Rust shape; still needs a
  documented decision and test coverage.

## Public Runtime API Matrix

| Python area | Python evidence | Rust evidence | Status | Notes |
| --- | --- | --- | --- | --- |
| App lifecycle | `App`, `AppConfig`, `UpdateHandle`, `DropHandle`, `show_progress` in `python/cocoindex/_internal/app.py` | `App`, `AppBuilder`, `UpdateHandle`, `DropHandle`, `Progress` in `rust/sdk/cocoindex/src/app.rs` | Partial | Rust has explicit app builder/open/update/drop. Missing Python default environment/lifespan shape. |
| Default environment | `Environment`, `EnvironmentBuilder`, `default_env`, `start`, `stop`, `lifespan` | No public Rust default environment | Missing/Different | Rust examples use explicit `App`. If intentional, document as Rust-specific. |
| Component mount | `use_mount`, `mount`, `mount_each`, `ComponentMountHandle.ready()` | `Ctx::scope`, `Ctx::mount_each`; lower-level core `Component::mount/use_mount` not SDK-public | Partial | Missing public background `mount` and foreground `use_mount` parity. |
| Target state | `TargetState`, providers, handlers, sinks, root provider registration, child provider declaration | `target_state.rs` exposes typed providers, states, handlers, sinks, root registration, declarations, child handler definitions, and foreground `mount_target` readiness | Partial/Good | Generic SDK-level lifecycle is now covered for CRUD/no-change, component declarations, mount-target children, provider generation, attachments, and ownership transfer. Remaining work is connector adoption plus broader runtime preview/live semantics. |
| Attachments | Python target providers expose `.attachment(...)` and handlers expose attachments | Rust providers expose `attachment(...)`; handlers expose public attachment definitions through `TargetHandler::attachments` and `ChildTargetDef` | Good | Generic attachment create/orphan-cleanup is covered in `tests/target_state.rs`; connector-specific attachment options still need broader tests. |
| Live components | `LiveComponent`, `LiveComponentOperator`, `LiveMapFeed`, `LiveMapView`, `LiveStream` | Core has live machinery; SDK exposes only `Ctx::auto_refresh` | Missing at SDK level | Blocks live localfs, Kafka/Iggy sources, OCI live object watching. |
| Exception handlers | `exception_handler`, `ExceptionContext`, global/scoped background error routing | No comparable Rust SDK public API | Missing | Needed for parity with background/live component semantics. |
| Context keys | `ContextKey`, `ContextProvider`, memo state hooks | `ContextKey::new`, `new_detect_change`, `new_with_state` | Good | Rust has idiomatic support and tests. |
| Memo functions | `@coco.fn`, memo key controls, logic tracking modes | `#[cocoindex::function]`, `memo::cached`, context deps | Partial | Rust has memo basics and macro. Missing Python's full logic tracking mode surface and many dynamic method cases. |
| Batching/Runner | `Runner`, `GPU`, batching queues, subprocess execution | `memo::batch`, `Ctx::batch` | Partial/Missing | No Runner/GPU/subprocess equivalent. |
| Stats groups | `stats_group`, `StatsGroupHandle`, progress output | `Ctx::stats_group`, `StatsGroupHandle`, `StatsGroupOptions` | Partial/Good | Rust has useful equivalent; less tested than Python live/progress cases. |
| Settings | `Settings`, `LmdbSettings`, env loading | AppBuilder setters | Partial/Different | Rust lacks settings object and env loading tests. |
| Data type reflection | Python `RecordType`, `TypeChecker`, numpy/pydantic/dataclass support | Rust serde and compile-time types | Different | Idiomatic, but connector schema APIs need explicit Rust tests. |
| Inspect/stable-path APIs | `iter_stable_paths`, `iter_stable_paths_by_name`, `list_stable_paths*` | No Rust SDK inspect module | Missing | Python can inspect persisted app trees and stable paths; Rust exposes no equivalent user-facing API. |
| Engine-object dumping | `dump_engine_object` recursively serializes Python specs/configs | Rust uses serde directly | Different/Missing | Rust likely should use serde-native config structs, but the absence should be documented because Python connector specs rely on this normalization layer. |
| User app loader / CLI app discovery | `load_user_app`, `cocoindex.cli` | No Rust CLI/user-loader surface | Missing/Different | If Rust SDK is library-only, document as a non-goal; otherwise add CLI tests before parity claims. |

## Connector Matrix

| Connector | Python public API | Rust public API | Status | Main gaps |
| --- | --- | --- | --- | --- |
| `localfs` | `FilePath`, `File`, `DirWalker`, `walk_dir`, `DirTarget`, `dir_target`, `declare_dir_target`, `mount_dir_target`, `declare_file`, live mode | `fs::FilePath`, `FileLike`, `FileEntry`, path matchers, `DirWalker`, `walk_dir`, `DirTarget`, `dir_target`, `declare_dir_target`, `mount_dir_target` | Partial/Good static path | Static walking/target APIs now exist with tests. Remaining gaps are `walk_dir(..., live=True)`, live map/view semantics, async cached `FileLike`, and full shared resource reuse across S3/OCI/GDrive. |
| `postgres` | `PgTableSource`, `RowFetcher`, `PgSourceSpec`, `TableTarget`, `table_target`, `declare_table_target`, `mount_table_target`, vector index, SQL command attachment, `managed_by` | `postgres::Database`, `read_table`, `read_table_with_options`, `TableTarget`, `mount_table_target`, vector index | Partial | Missing generic target-state shape, `managed_by`, SQL command attachments, streaming/`items()` source, row factory ergonomics, snapshot cursor tests. |
| `surrealdb` | `ConnectionFactory`, `TableSchema`, `TableTarget`, `RelationTarget`, `table_target`, `declare_table_target`, `mount_table_target`, relation target, vector index, `managed_by` | `Graph`, `TableSchema`, `TableTarget`, `RelationTarget`, `mount_table_target`, relation helpers | Partial | Missing vector indexes, `managed_by`, generic target-state shape, schema evolution tests, table drop tests, type mapping tests. |
| `kafka` | `TopicStream`, `topic_as_stream`, `topic_as_map`, `KafkaTopicTarget`, `kafka_topic_target`, `declare_kafka_topic_target`, `mount_kafka_topic_target` | `KafkaProducer`, `KafkaTopicTarget`, `mount_kafka_topic_target` | Partial | Rust is target-only. Missing source stream/map APIs and public target-state style helpers. |
| `iggy` | `TopicStream`, `topic_as_stream`, `topic_as_map`, `IggyTopicTarget`, target helpers | None | Missing | Should mirror Kafka pattern once Rust Kafka source/target shape is settled. |
| `amazon_s3` | `S3FilePath`, `S3File`, `S3Walker`, `get_object`, `read`, `list_objects` | None | Missing | File source abstraction missing in Rust. |
| `oci_object_storage` | `OCIFilePath`, `OCIFile`, `OCIWalker`, `get_object`, `read`, `list_objects`, live stream support | None | Missing | Requires live map/source framework first. |
| `google_drive` | package `__all__`: `DriveFileInfo`, `DriveFile`, `GoogleDriveSourceSpec`, `GoogleDriveSource`, `list_files`; `_source.py` also defines `DriveFilePath` | `gdrive::DriveFile`, `GoogleDriveClient`, `GoogleDriveSource` | Partial/in progress | Rust has service-account/static-token client, recursive listing, MIME filtering, export/download reads, and mock tests. Missing Python-style `DriveFileInfo`, top-level `list_files(spec)`, async `items()`, and shared file source abstraction; clarify whether `DriveFilePath` should be publicly exported in Python. |
| `sqlite` | `ManagedConnection`, `Vec0TableDef`, `TableSchema`, `TableTarget`, target helpers, vec0 support, user/system managed | None | Missing | Large target connector. Python tests are extensive and should be ported if implemented. |
| `doris` | `DorisConnectionConfig`, `ManagedConnection`, `TableSchema`, `DorisTableTarget`, vector/inverted indexes, retry config | None | Missing | Needs table target architecture and stream-load retry behavior. |
| `lancedb` | `TableSchema`, `TableTarget`, target helpers, optimize behavior | `lancedb::LanceDatabase`, `TableSchema`, `ColumnDef`, `LanceTableTarget`, `mount_table_target`, `vector_search` | Partial | Native target exists with hermetic e2e coverage for create/upsert/search/delete and additive scalar-column schema evolution. Missing Python optimize behavior, mutation-preservation/retry tests, full add-column edge cases, and constructor/declaration split. |
| `qdrant` | `QdrantVectorDef`, `CollectionSchema`, `CollectionTarget`, target helpers | `qdrant::QdrantConnection`, `CollectionSchema`, `CollectionTarget`, `mount_collection_target`, `mount_collection_target_with_options`, `vector_search` | Partial | Native single-vector collection target exists with `ManagedTargetOptions`. Remaining gaps are named/multivector schema parity, Python-style constructor/declaration split, richer validation, and live/e2e coverage in CI. |
| `turbopuffer` | `VectorDef`, `NamespaceSchema`, `Row`, `NamespaceTarget`, target helpers | `turbopuffer::TurbopufferConnection`, `NamespaceSchema`, `NamespaceTarget`, `mount_namespace_target`, `mount_namespace_target_with_options`, `vector_search` | Partial | Native namespace target exists with `ManagedTargetOptions`. Remaining gaps are named-vector/f16 schema parity, Python-style constructor/declaration split, richer validation, and hosted-service e2e coverage. |
| `neo4j` | `ConnectionFactory`, `TableTarget`, `RelationTarget`, Cypher builders, indexes, constraints, vector index | `neo4j::Graph`, `TableSchema`, `TableTarget`, `RelationTarget`, `mount_table_target`, `mount_table_target_with_options`, `mount_relation_target`, `mount_relation_target_with_options` | Partial | Native graph target API now exists with `ManagedTargetOptions` and shares the generic target-state path. Remaining gaps are vector indexes, explicit index/constraint helpers, declaration split, richer type mapping, and Python's full validation surface. |
| `falkordb` | `ConnectionFactory`, `TableTarget`, `RelationTarget`, Cypher builders, indexes, vector index | `falkordb::Graph`, `TableSchema`, `TableTarget`, `RelationTarget`, `mount_table_target`, `mount_table_target_with_options`, `mount_relation_target`, `mount_relation_target_with_options` | Partial | Native graph target API now exists with `ManagedTargetOptions` and shares the generic target-state path. Remaining gaps are vector indexes, explicit index helpers, declaration split, richer type mapping, and Python's full validation surface. |

## Connector Test Matrix

| Connector | Python test evidence | Rust test evidence | Status |
| --- | --- | --- | --- |
| localfs | `test_file_path.py` 7 tests, `test_localfs_live.py` 1 test, `test_source_items.py` 3 tests | `pipeline.rs` has fs walk and dir target tests | Partial |
| Postgres source | 9 Python tests: dict, row type, row factory, columns, empty table, schema, items, snapshot, exclusivity | 1 Rust live test covering typed read, schema, columns, unsupported type, memo/reconcile | Partial |
| Postgres target | 8 Python tests: vector indexes, no-change, halfvec, SQL attachments, NUL handling, failed column-drop retry | 2 Rust live tests: row reconcile/delete-only, vector index create/delete | Partial |
| SurrealDB target | 37 Python tests: schemafull/schemaless, update/delete/no-op, drop table, schema evolution, relations, ordering, vectors, managed table, type mapping | 2 Rust live/smoke tests | Partial |
| Kafka source | 14 Python tests | none | Missing |
| Kafka target | 17 Python tests | 1 Rust live target test | Partial |
| Iggy source | 7 Python tests | none | Missing |
| OCI object storage | 23 Python tests | none | Missing |
| Amazon S3 | 24 Python tests | none | Missing |
| SQLite target | 21 Python tests | none | Missing |
| LanceDB target | 12 Python tests | 1 hermetic Rust e2e plus LanceDB unit tests | Partial |
| Doris target | 6 Python tests | none | Missing |
| Neo4j target | 45 Python tests | shared Rust graph target unit tests plus example compile/e2e path | Partial |
| FalkorDB target | 31 Python tests | shared Rust graph target unit tests plus example compile/e2e path | Partial |
| Turbopuffer target | 19 Python tests | Rust target unit tests plus example compile path | Partial |
| Qdrant | Python interface exists; no dedicated test file found | Rust target unit tests plus example compile path | Partial |
| Google Drive | Python interface exists; no dedicated Python test family found | Rust unit tests plus mock integration tests | Partial |

## Packaging and Feature Parity Matrix

Python connector availability is declared through `pyproject.toml` optional
dependencies and connector package `__all__` exports. Rust connector
availability is declared through Cargo features and `#[cfg(feature = "...")]`
modules in `rust/sdk/cocoindex`.

| Capability / extra | Python optional dependency | Rust Cargo feature | Status | Notes |
| --- | --- | --- | --- | --- |
| `postgres` | `asyncpg>=0.31.0` | `postgres` | Partial | Rust has source read helpers and table target, but fewer APIs/tests. |
| `surrealdb` | `surrealdb>=1.0.0` | `surrealdb` | Partial | Rust has graph/table/relation helpers, but no vector index or managed-by parity. |
| `kafka` | `confluent_kafka>=2.6` | `kafka` | Partial | Rust feature is target-only; Python extra includes source and target. |
| `google_drive` | `google-api-python-client`, `google-auth`, `google-auth-httplib2`, `httplib2` | `google_drive` | Partial | Rust uses `reqwest` plus pure-Rust RS256 JWT signing instead of Google's Python client stack. |
| `amazon_s3` | `aiobotocore>=2.0.0` | none | Missing | Requires shared Rust file/object source abstraction first. |
| `oci` | `oci>=2.0` | none | Missing | Requires object source and live source/map API. |
| `iggy` | `apache-iggy>=0.8.0` | none | Missing | Should mirror Kafka source/target shape after Kafka parity. |
| `sqlite` | `sqlite-vec>=0.1.6` | none | Missing | Good next target after generic target-state API. |
| `doris` | `aiohttp`, `pymysql`, `aiomysql` | none | Missing | Needs table target lifecycle, stream load, retry, vector/inverted index support. |
| `lancedb` | `lancedb`, `pyarrow` | Cargo feature `lancedb` with `lancedb`, `arrow-array`, `arrow-schema` | Partial | Native target exists; optimize and fuller schema-evolution parity remain. |
| `qdrant` | `qdrant-client>=1.6.0` | `qdrant` with `qdrant-client` | Partial | Initial collection target and text-embedding example exist. Remaining parity work is named/multivector schemas and fuller tests. |
| `turbopuffer` | `turbopuffer>=0.5.0` | `turbopuffer` with `reqwest` | Partial | Initial namespace target and text-embedding example exist. Remaining parity work is Python's richer vector/schema validation and fuller tests. |
| `neo4j` | `neo4j>=5.18.0` | `neo4j` with `neo4rs` | Partial | Initial graph table/relation target support exists. Remaining parity work is Python's vector index, index/constraint, managed lifecycle, and validation coverage. |
| `falkordb` | `falkordb>=1.1.0` | `falkordb` with `redis` | Partial | Initial graph table/relation target support exists. Remaining parity work is Python's vector index, index, managed lifecycle, and validation coverage. |
| `litellm` | `litellm>=1.81.0` | none | Missing | Rust examples hand-roll OpenAI-compatible HTTP clients. |
| `sentence_transformers` | `sentence-transformers>=3.3.1` | none | Missing/Different | Rust examples use `fastembed` directly; no SDK embedder wrapper feature. |
| `colpali` | `colpali-engine` | none | Missing | Blocks ColPali image-search parity. |
| `entity_resolution` | `faiss-cpu>=1.7` | built into base Rust SDK | Different/Partial | Rust has `entity_resolution` module without feature gate and uses a simple in-memory candidate index rather than FAISS. |
| `entity_resolution_llm` | `faiss-cpu`, `instructor`, `litellm` | none | Missing | Rust lacks `LlmPairResolver` and LiteLLM/instructor wrapper. |

Packaging recommendation: when adding Rust connectors, add a matching Cargo
feature even if the connector's public module is small. Keep feature names
aligned with Python extras where practical (`amazon_s3`, `google_drive`,
`surrealdb`, etc.) so examples and docs can talk about parity consistently.

## Connector Semantic Parity Matrix

Python connectors are not just wrappers around client libraries. Most target
connectors follow the same semantic contract:

- a spec constructor (`*_target`) returns a `TargetState`;
- `declare_*_target` declares a pending child target;
- `mount_*_target` mounts the target and returns a ready wrapper;
- `managed_by=system/user` controls lifecycle ownership;
- `statediff.diff` / `diff_composite` maps desired/previous records into
  insert/upsert/replace/delete actions;
- target-level handlers may expose attachments such as vector indexes;
- row/point/relation wrappers call `declare_target_state` for child records;
- source connectors expose stable `(key, item)` iteration for `mount_each`.

Rust now exposes the shared generic target-state contract publicly, but many
connectors still need to migrate from private `Ctx` helpers and connector-local
wrappers onto that common facade.

| Family | Python semantic contract | Rust state | Required Rust parity work |
| --- | --- | --- | --- |
| Generic target lifecycle | `TargetState`, `TargetStateProvider`, `TargetHandler`, `TargetActionSink`, `TargetReconcileOutput`, `mount_target`, attachments | SDK exposes typed target states, child handler definitions, foreground mount readiness, attachments, and generic lifecycle tests | Refactor connectors onto the public facade and add connector-specific parity tests. |
| Managed ownership | `target.ManagedBy`, `statediff.MutualTrackingRecord`, `resolve_system_transition` in table/vector/graph connectors | Rust exposes `ManagedBy`, `ManagedTargetOptions`, `MutualTrackingRecord`, `TrackingRecordTransition`, `resolve_system_transition`, and `diff`; Qdrant, Turbopuffer, Neo4j, and FalkorDB accept managed options | Refactor older Postgres/SurrealDB/LanceDB target implementations onto the shared helper so all target connectors consistently avoid user-managed DDL. |
| Diff semantics | `statediff.diff` and `diff_composite` distinguish insert/upsert/replace/delete plus incomplete previous state | Rust handlers mostly hand-roll reconciliation | Add shared Rust diff helpers and port Python test cases from `connectorkits/statediff.py` behavior. |
| Table targets | Postgres, SQLite, Doris, LanceDB, SurrealDB, Neo4j, FalkorDB expose table constructors/declaration/mount; many support schema evolution | Rust has Postgres and SurrealDB mount-only helpers | Add constructor/declaration split and common lifecycle shape; implement SQLite next as proof the API generalizes beyond Postgres. |
| Row/record writes | Python wrappers declare rows/records into child providers; handlers upsert/delete only changed desired state | Rust Postgres/SurrealDB/Kafka target wrappers declare rows/messages into providers | Existing Rust pattern is directionally right, but needs generic API and broader schema/change tests. |
| Vector/index attachments | Postgres, SurrealDB, Neo4j, FalkorDB, Doris expose vector/inverted/index attachments or target-level index lifecycle | Rust Postgres vector index exists as internal attachment; SurrealDB graph/vector indexes missing | Public attachment API plus per-connector vector index tests. |
| Graph relations | SurrealDB, Neo4j, FalkorDB expose `RelationTarget` with endpoint tables and relation rows | Rust SurrealDB has relation helpers; no Neo4j/FalkorDB | Generalize relation target concepts and add Cypher-specific escaping/index/constraint behavior. |
| Vector stores | Qdrant `CollectionTarget`, Turbopuffer `NamespaceTarget`, LanceDB `TableTarget` | LanceDB table target, Qdrant collection target, and Turbopuffer namespace target all exist; the Qdrant/Turbopuffer targets are built on the public target-state facade (collection/namespace → point/row via `mount_target`) | Vector targets are kept as collection/namespace lifecycles (not forced into the relational model). Remaining work is named/multivector schemas and f16 for Qdrant/Turbopuffer. |
| Stream targets/sources | Kafka/Iggy expose source stream/map and topic target with tombstones/delete values | Rust Kafka target only | Add stream/map source APIs, delete filters, offsets/readiness, and Iggy parity. |
| File/object sources | LocalFS/S3/OCI/GDrive share `FilePath`, `FileLike`, `FileMetadata`, `items()`; OCI and localfs can be live | LocalFS has `FilePath`/`FileLike`/matchers/`items()`; GDrive has separate `DriveFile`; no shared cross-connector file resource module | Promote or generalize the LocalFS file resource surface before adding S3/OCI parity; add live source support separately. |

## Implemented Connector Findings

### LocalFS / `fs`

Rust has a useful static walker and target:

- `fs::walk`
- `walk_dir`
- `FilePath`
- `FileEntry`
- `FileLike`
- `MatchAllFilePathMatcher`
- `PatternFilePathMatcher`
- `DirWalker`
- `DirTarget`
- `dir_target`
- `declare_dir_target`
- `mount_dir_target`
- `DirTarget::declare_file`

Gaps against Python:

- No `walk_dir(..., live=True)`.
- No live file watching / live map view.
- Rust `FileLike` is synchronous today; Python's `FileLike` is async and caches
  full reads/fingerprints.
- Rust `FilePath` supports stable base keys and resolution, but not the full
  Python `PurePath`-like method surface.

Recommended Rust tests:

- Path traversal and base-dir memo-key behavior now has coverage.
- Matcher include/exclude and excluded-directory pruning now has coverage.
- `DirWalker.items()` recursive/nonrecursive behavior now has coverage.
- Live add/edit/delete once live map support exists.
- Target removal, nested directory behavior, and `declare_dir_target` missing-dir
  behavior now have coverage.

### Postgres

Recent Rust changes improved parity:

- `read_table_with_options` supports schema and selected columns.
- Unsupported source column types now error instead of silently becoming `null`.
- Vector indexes are table attachments.
- Delete-only row reconciliation recreates missing tables.

Remaining gaps:

- No `managed_by` option.
- No public `table_target` / `declare_table_target`.
- No SQL command attachments.
- No `RowFetcher` / source `items()` API.
- `read_table` uses `fetch_all`, not streaming cursor iteration.
- No repeatable-read snapshot test.
- No row factory/row type exclusivity equivalent; Rust may intentionally use
  generic `DeserializeOwned`, but this should be documented.
- No NUL stripping parity test for text/json.
- No failed column-drop retry parity.
- No halfvec opclass live test.

Recommended Rust tests:

- SQL command attachment setup/teardown/no-teardown.
- Mixed rows and attachments in one target.
- `managed_by=USER` style behavior.
- Snapshot/isolation or explicitly document why `fetch_all` is sufficient.
- Source empty table.
- NUL handling in text/json.
- Halfvec opclass.
- Column schema change/retry behavior.

### SurrealDB

Rust has the basic table/relation model:

- `Graph`
- `TableSchema`
- `TableTarget`
- `RelationTarget`
- fixed and polymorphic-ish relation helpers

Remaining gaps:

- No vector index attachment.
- No `managed_by`.
- No generic `table_target` / `declare_table_target` split.
- No explicit table drop-on-removal test.
- Schema evolution behavior is much less tested.
- Type mapping is not tested at Python depth.
- Relation auto-id behavior was improved with length-prefixed keys, but the
  Python behavior and Rust behavior should be aligned/documented.
- No alias `declare_row` method in Rust, only `declare_record`.

Recommended Rust tests:

- Schemafull and schemaless table creation/readback.
- Update/delete/no-op with operation counters where possible.
- Drop table on removal once target-state public API supports it.
- Add/remove/change schema fields.
- Relation schema with/without id.
- Polymorphic relation errors and success paths.
- Vector index mtree/hnsw create/update/delete.
- User-managed table.
- Type mapping.

### Kafka

Rust target is useful and tested:

- Produces new messages.
- Skips unchanged messages.
- Produces updated messages.
- Produces tombstones for deletions.

Remaining gaps:

- No Kafka source `TopicStream`.
- No `topic_as_stream`.
- No `topic_as_map`.
- No payload filtering.
- No public `kafka_topic_target` / `declare_kafka_topic_target` split.
- Topic creation is an explicit Rust convenience, not part of reconciliation.

Recommended Rust tests:

- Source stream basic consumption.
- `payloads()` filters null values.
- Stream-to-map behavior with tombstones.
- Offset/partition readiness behavior.
- Custom deletion value target option.

### Google Drive

Rust now has a first native Google Drive source pass:

- `DriveFile`
- `GoogleDriveClient::from_service_account_file`
- `GoogleDriveClient::from_static_token`
- `GoogleDriveClient::with_base_url`
- `GoogleDriveClient::read`
- `GoogleDriveClient::read_text`
- `GoogleDriveSource::list_files`
- MIME export mapping for Docs/Sheets/Slides
- Hermetic mock tests for recursive listing, MIME filtering, binary download,
  Google-doc export, and auth header wiring

Remaining gaps against Python:

- No `DriveFilePath` equivalent with display path plus resolved Drive file id.
- No `DriveFileInfo` public metadata struct.
- No top-level `GoogleDriveSourceSpec` / `list_files(spec)` helper.
- No async `files()` or `items()` API.
- Rust currently exposes `DriveFile::key()` as file id for uniqueness, while
  Python `GoogleDriveSource.items()` yields the file name path. The Rust choice
  is safer for duplicate filenames, but it should be documented or replaced with
  a richer `DriveFilePath` model.
- No live change notification support.

Recommended Rust tests:

- Service-account live test gated on environment variables.
- Duplicate filename behavior once the item-key policy is finalized.
- Metadata/fingerprint behavior after `DriveFilePath` or `FileLike` parity exists.
- Example-level E2E against real Drive and Postgres once credentials are
  available.

## Missing Connector Implementation Notes

Before implementing any new target connector, verify these shared SDK pieces
exist or are explicitly part of that connector PR:

- public target-state API;
- `ManagedBy` and state transition helpers;
- constructor/declaration/mount API split;
- attachment API if the connector manages indexes;
- connector-specific live/hermetic tests plus skipped live tests for real
  service dependencies.

Before implementing any new object/file source, verify these shared SDK pieces
exist or are explicitly part of that connector PR:

- `FilePath`-like stable path abstraction;
- metadata and content fingerprint abstraction;
- `(key, item)` iteration API for `mount_each`;
- path matcher support;
- live map/source support where Python has live semantics.

### SQLite

Do not start by hand-rolling one example. Python SQLite has a mature target
surface with normal tables, user-managed tables, and `vec0` virtual tables.
Rust should first have public target-state APIs, then port:

- `ManagedConnection`
- `TableSchema`
- `TableTarget`
- `table_target`
- `declare_table_target`
- `mount_table_target`
- optional vec0 support

Minimum tests from Python:

- create/insert/update/delete
- no-change optimization
- multiple tables
- dict/struct rows
- user-managed table
- vec0 validation and schema-switch behavior if vec0 is implemented

### LanceDB

Important Python behavior is schema evolution and optimize scheduling. If Rust
implements this connector, tests must cover:

- add column preserves existing rows
- add non-nullable column materialized nullable
- optimize interval validation
- optimize failure retry/no overlap

### Doris

Python connector includes connection config, retry config, stream-load errors,
vector/inverted indexes, and table target. Rust implementation should not skip
retry and stream-load failure semantics.

### Qdrant / Turbopuffer

These are vector store targets rather than relational tables. Rust should avoid
forcing them into the Postgres table model. Required concepts:

- collection/namespace schema
- named vectors
- point/row upsert/delete
- no-change tracking

**Status: implemented (single unnamed vector).** Both now have native Rust
targets — `cocoindex::qdrant` (`qdrant-client` gRPC) and `cocoindex::turbopuffer`
(`reqwest` v2 HTTP). Each is a two-level managed target (collection/namespace →
point/row) built on the **public target-state facade** (`mount_target` child
readiness, `from_async_fn_with_children`, `ChildTargetDef`) — the first real
connectors to dogfood Phase 1. They cover collection/namespace lifecycle,
point/row upsert/delete, fingerprint no-change tracking, and destructive
schema-change invalidation, with live integration tests (Qdrant container,
Turbopuffer hosted) and `text-embedding-{qdrant,turbopuffer}` examples validated
end-to-end. Remaining: named/multivector schemas, f16 (Turbopuffer), and the
Python constructor/declaration (`*_target`/`declare_*_target`) split.

### Neo4j / FalkorDB

These should share a graph-target design with:

- node table targets
- relation targets
- endpoint merge/upsert behavior
- node/relationship indexes
- vector index attachment
- Cypher builder tests

SurrealDB relation APIs are the closest current Rust reference point, but Neo4j
and FalkorDB need Cypher-specific escaping, constraints, and index semantics.

### Object Sources: S3 / OCI / Google Drive

These need a Rust source abstraction before connector work is worthwhile:

- file path type with stable memo identity
- file metadata
- file content read APIs
- list/walk APIs yielding `(key, file)`
- optional live stream integration for OCI

OCI should wait for Rust live source/map support, because Python's OCI connector
is explicitly designed around live object events.

## Core Runtime Test Gap Matrix

| Python test family | Rust coverage | Status |
| --- | --- | --- |
| `test_trivial_app.py` | Rust `pipeline.rs` app/update tests | Good |
| `test_update_handle.py` | Rust update handle tests | Partial/Good |
| `test_settings.py` | Rust builder tests only | Partial |
| `test_default_env.py`, `test_default_env_async.py` | no default env | Missing/Different |
| `test_concurrency_control.py` | `AppBuilder::max_inflight_components`, `mount_each` order tests, quota peak enforcement, nested-scope no-deadlock test | Partial/Good | Rust now directly covers quota enforcement and nested scope behavior. Remaining Python-only behavior is default-limit/env fallback (`COCOINDEX_MAX_INFLIGHT_COMPONENTS`), because Rust has no default-env loader parity. |
| `test_context_tracked_key.py` | Rust context key tests | Good |
| `test_context_tracked_state_validation.py` | partial via `new_with_state` | Partial |
| `test_function_memo.py` | Rust memo tests and macro tests | Partial |
| `test_function_batching.py` | Rust `Ctx::batch`; no Runner/GPU | Partial/Missing |
| `test_component_memo.py` | Rust `scope` and `mount_each` tests | Partial |
| `test_component_target_states.py` | Rust connector-specific target tests | Missing generic public API coverage |
| `test_flat_target_states.py` | `pipeline.rs::public_target_state_api_reconciles_typed_actions_and_tracking_records`, `target_state::tests` | Partial |
| `test_attachment_target_states.py` | core/internal support, Postgres vector test | Missing public generic coverage |
| `test_provider_generation.py` | core support, limited public tests | Missing/Partial |
| `test_ownership_transfer.py` | core likely supports; no Rust SDK test family | Missing |
| `test_full_reprocess.py` | memo full reprocess tests | Partial |
| `test_app_drop.py` | drop state/memo tests | Partial |
| `test_exception_handlers.py` | no public exception handler API | Missing |
| `test_live_component.py` | core support only | Missing SDK public API |
| `test_auto_refresh.py` | Rust `Ctx::auto_refresh` tests | Partial |
| `test_stats_group.py` | Rust stats group tests | Partial |
| `test_datatype.py` | Rust serde-oriented, no dynamic type checker | Different |
| `test_typed_serde_memo.py` | Rust serde memo tests partial | Different/Partial |
| `test_target_key_types.py` | Rust stable key coverage is indirect | Partial |
| internal memo fingerprint tests | Rust fingerprint behavior likely in utils, not SDK parity-tested | Partial |

## Resources, Ops, and Connectorkits Matrix

These are not storage connectors, but they are part of the public Python SDK and
many connectors/examples depend on them.

| Python public area | Python evidence | Rust evidence | Status | Notes |
| --- | --- | --- | --- | --- |
| File resources | `FileMetadata`, `FileLike`, `FilePath`, `FilePathMatcher`, `MatchAllFilePathMatcher`, `PatternFilePathMatcher` in `python/cocoindex/resources/file.py` | LocalFS exposes `FilePath`, `FileMetadata`, `FileLike`, matchers, `DirWalker.items()`; no shared cross-connector resource module | Partial | LocalFS now has the static resource shape and BOM-aware text decode. Remaining gaps are async/lazy cached reads, Python memo-state fallback semantics, live source support, and reuse by S3/OCI/GDrive. |
| ID resources | `generate_id`, `generate_uuid`, `IdGenerator`, `UuidGenerator` in `python/cocoindex/resources/id.py` | `generate_id`, `generate_uuid`, `IdGenerator`, `UuidGenerator` in `rust/sdk/cocoindex/src/id.rs` | Good | Rust has tests for repeated identical deps, stability across runs, and constructor deps. |
| Chunk resources | `TextPosition`, `Chunk` in `python/cocoindex/resources/chunk.py` | `cocoindex_ops_text::split::Chunk`, `OutputPosition`, `TextRange` | Partial/Different | Rust exposes this through `rust/ops_text`, not the main SDK. Rust chunks carry ranges and positions; callers slice the original text themselves. Python chunks include `text`, `location`, `start`, and `end`. |
| Vector schema resources | `VectorSchema`, `MultiVectorSchema`, provider helpers in `python/cocoindex/resources/schema.py` | Per-connector vector column/index options only | Missing/Partial | Rust lacks a generic vector schema provider abstraction. |
| Embedder resource protocol | `Embedder` protocol in `python/cocoindex/resources/embedder.py` | `entity_resolution::EntityEmbedder`; examples use `fastembed` wrappers | Partial/Different | Rust has trait-based entity resolution embedding but no general SDK embedder protocol for annotated vectors. |
| Text ops | `detect_code_language`, `SeparatorSplitter`, `RecursiveSplitter`, `CustomLanguageConfig` in `python/cocoindex/ops/text.py` | `cocoindex_ops_text::{prog_langs, split}` | Partial/Good | Python wraps the same Rust text implementation, so core behavior is close. The public Rust SDK still lacks the Python-style `cocoindex::ops::text` facade and direct chunk-text shape. |
| Entity resolution ops | `CanonicalSide`, `PairDecision`, `ExistingCanonicalPolicy`, `PairResolver`, `ResolvedEntities`, `resolve_entities`; `LlmPairResolver` | Rust `entity_resolution` has core resolve API; no `LlmPairResolver` | Partial | Core Rust resolver is tested, but `resolve_entities` currently computes ordered `ResolutionEvent`s and drops them internally because there is no `on_resolution` callback/API. LLM convenience resolver is Python-only. |
| LiteLLM ops | `LiteLLMEmbedder`, `LiteLLMTranscriber`, `litellm` helper | None | Missing | Rust examples hand-roll HTTP clients or use local `fastembed`; no SDK op parity. |
| SentenceTransformer op | `SentenceTransformerEmbedder` | None | Missing/Different | Rust examples use `fastembed` directly. If accepted, document as idiomatic Rust replacement; otherwise add SDK wrapper. |
| Connectorkit target utilities | `ManagedBy` plus statediff helpers | Rust exposes `statediff::{ManagedBy, ManagedTargetOptions, MutualTrackingRecord, TrackingRecordTransition, resolve_system_transition, diff}` | Partial/Good | The shared transition semantics now exist and are used by Qdrant, Turbopuffer, Neo4j, and FalkorDB. Older Postgres/SurrealDB/LanceDB targets still need refactoring onto the helper. |
| Connectorkit fingerprint utilities | `Fingerprint`, `fingerprint_bytes`, `fingerprint_str`, `fingerprint_object` | Rust memo key/fingerprint helpers in `memo.rs` | Partial | Rust has lower-level memo fingerprinting but no connector-facing equivalent module. |
| Async adapters | `sync_to_async_iter`, `async_to_sync_iter` | None | Missing/Different | Rust async iterator ergonomics differ; source APIs should still define a consistent item-stream pattern. |

## Ops and Resource Semantic Findings

### Text Ops

The underlying splitter implementation is strong because Python calls into Rust
for most of the work. The mismatch is primarily API packaging:

- Python SDK users call `cocoindex.ops.text.RecursiveSplitter.split(...)` and
  receive `Chunk` objects with text included.
- Rust users call `cocoindex_ops_text::split::RecursiveChunker` directly and
  receive ranges into the original string.
- This is acceptable inside Rust examples, but it is not SDK parity until the
  main `cocoindex` crate exposes an ops facade and a first-class chunk shape.

Recommended fix: add `cocoindex::ops::text` wrappers that re-export or lightly
wrap `cocoindex_ops_text`, and decide whether Rust chunks should expose a helper
for `chunk.text(source)` instead of forcing every example to hand-slice ranges.

### ID Resources

Rust is close to Python here. Python `IdGenerator` / `UuidGenerator` combine:

- constructor dependencies,
- the dependency passed to `next_id` / `next_uuid`,
- an occurrence ordinal for repeated identical dependencies.

Rust mirrors this behavior in `rust/sdk/cocoindex/src/id.rs`, including the
important repeated-identical-dependency case. This is the right model for
chunk-row ids: repeated equal chunk text must still produce distinct stable ids.

### File Resources

This remains a large design gap. Python's `FileLike` is not just a file handle;
it is the shared resource abstraction that lets localfs, S3, OCI, and Google
Drive look alike:

- stable `FilePath` / metadata identity,
- lazy metadata and content loading,
- memo state based on metadata plus content fingerprint,
- `read()` / `read_text()` with UTF BOM handling and UTF-8 fallback,
- matcher semantics via `PatternFilePathMatcher`.

Rust currently has `fs::FileEntry` for local files and `gdrive::DriveFile` for
Drive metadata. They are useful, but not substitutable. This is why new Rust
file/object sources keep inventing local shapes instead of sharing the Python
connector paradigm.

Recommended fix: add a shared Rust file resource module before adding S3/OCI as
full parity connectors. It should define the Rust equivalents of `FilePath`,
`FileMetadata`, `FileLike`, and path matchers, with explicit memo/fingerprint
semantics.

### Embedder and Vector Schema

Python separates embedding execution from vector schema declaration:

- `Embedder` is a protocol with `embed(text)`.
- `VectorSchemaProvider` and `MultiVectorSchemaProvider` let targets discover
  vector dimensions and element type.

Rust has a narrower `EntityEmbedder` trait inside entity resolution and
connector-local vector options. That works for examples, but it does not create
the common SDK contract Python uses for vector targets and embedders.

Recommended fix: add a general Rust embedder trait and vector schema provider
types before standardizing additional vector targets such as LanceDB, Qdrant,
Turbopuffer, Neo4j, and FalkorDB.

### Entity Resolution

The Rust implementation has the important core concepts: canonical side,
existing-canonical policy, pair decisions, candidate selection, and tests for
the resolver path. The remaining gaps are API-level:

- no Python-style `on_resolution` callback, even though `ResolutionEvent` exists;
- no SDK `LlmPairResolver`;
- no shared `Embedder` protocol, so examples wrap local embedding models by hand;
- less Python test parity around concurrency, error cancellation, hallucinated
  LLM decisions, and event ordering.

Recommended fix: expose an optional event callback/collector in Rust
`ResolveOptions`, add an LLM pair resolver or a documented trait-based example,
and port the Python entity-resolution test families that cover events and error
behavior.

## Resource and Ops Test Gap Matrix

| Python test family | Rust coverage | Status |
| --- | --- | --- |
| `resources/test_id.py` 12 tests | Rust `pipeline.rs` ID/UUID generator tests | Good |
| `resources/test_file_path_matcher.py` 14 tests | LocalFS matcher unit tests | Partial |
| `connectors/test_file_path.py` 7 tests | LocalFS `FilePath` base-key test | Partial |
| `connectors/test_source_items.py` 3 tests | LocalFS `DirWalker.items()` tests | Partial |
| `ops/test_text.py` 24 tests | Rust ops_text has implementation tests outside SDK framing; examples compile | Partial |
| `ops/test_entity_resolution.py` 33 tests | Rust `entity_resolution.rs` has 7 tests covering core resolver paths | Partial/Good |
| `ops/test_llm_pair_resolver.py` 6 tests | no Rust LLM pair resolver | Missing |
| `ops/test_litellm_transcriber.py` 1 test | no Rust LiteLLM transcriber | Missing |
| `ops/test_embedder_refactor.py` 3 tests | no Rust SDK embedder abstraction | Missing/Different |

## Operational Utility Parity Matrix

These interfaces are not connectors, but they affect the SDK experience and
test surface. They also show where Python's dynamic-language support should not
be copied blindly into Rust, but still needs an explicit Rust design decision.

| Python area | Python evidence | Rust evidence | Status | Notes |
| --- | --- | --- | --- | --- |
| Inspect API | `python/cocoindex/_internal/inspect_api.py`, re-exported by `cocoindex.inspect` | None in `rust/sdk/cocoindex/src` | Missing | Python supports async and sync stable-path listing for an app or environment/app name. Rust has internal stable-path machinery through core, but no public SDK wrapper. |
| CLI | `python/cocoindex/cli.py` plus `python/tests/cli/test_cli.py` | None | Missing/Different | Python CLI covers `ls`, `show`, `update`, `drop`, `init`, default DB env vars, confirmation, preview, and full reprocess. Rust examples are run directly with `cargo run`. |
| User app loading | `python/cocoindex/user_app_loader.py` | None | Missing/Different | Python can load file paths, modules, and package-relative apps while preserving import/memo identity. Rust does not need Python import semantics, but a Rust CLI would need its own app discovery model. |
| Engine-object dump | `python/cocoindex/engine_object.py` | Rust serde/config structs | Different | Rust should usually prefer typed serde structures, but connector specs need explicit serialization tests when they replace Python's duck-typed dumper. |
| Dynamic datatype analysis | `python/cocoindex/_internal/datatype.py`, `test_datatype.py` | Rust compile-time serde plus connector schemas | Different | This is a legitimate language difference. The parity requirement is connector schema correctness, not reproducing Python's runtime type-hint analyzer. |
| Safe serde/pickle controls | `unpickle_safe`, `serialize_by_pickle`, `test_serde.py`, `test_safe_unpickle.py` | Rust serde and `rmp-serde` memo values | Different/Missing | Rust does not need pickle safety APIs, but memo/target persistence needs its own backward-compat and schema-evolution tests. |
| Memo fingerprint registry | `memo_fingerprint`, `register_memo_key_function`, `NotMemoKeyable`, `test_memo_fingerprint.py` | `memo::key_bytes*`, `key_fingerprint_result`, serde fingerprinting | Partial/Different | Rust has deterministic key helpers, but no registry/hook surface equivalent to Python. Use serde traits/explicit key functions as the Rust design and test collision/order behavior. |
| Settings/env | `Settings`, `LmdbSettings`, `Settings.from_env`, `test_settings.py` | `AppBuilder::db_path/lmdb_*` | Partial/Different | Rust has builder knobs but no settings object or env-loader parity. |

Recommended stance:

- Treat CLI/user-app loading as a product decision, not a connector blocker.
- Treat Python pickle/datatype APIs as Rust non-goals unless a Rust CLI/plugin
  host needs dynamic loading.
- Still add Rust tests for the replacement invariants: stable key listing if
  exposed, settings/env loading if exposed, deterministic serde memo keys,
  connector schema serialization, and persisted-state backward compatibility.

## Operational Utility Test Gap Matrix

| Python test family | Rust coverage | Status |
| --- | --- | --- |
| `cli/test_cli.py` 44 tests | none | Missing/Different |
| `core/test_settings.py` 12 tests | no settings object/env loader tests | Partial/Different |
| `core/test_datatype.py` 18 tests | no runtime type analyzer | Different |
| `core/test_serde.py` 30 tests plus safe-unpickle coverage | Rust serde is exercised indirectly by memo/target tests | Partial/Different |
| `core/test_typed_serde_memo.py` 8 tests | Rust memo tests cover typed serde basics, not Python's pydantic/dataclass/pickle matrix | Partial/Different |
| `internal/test_memo_fingerprint.py` 37 tests | Rust memo key helper tests are much smaller | Partial/Different |
| Inspect/stable-path CLI show tests | no Rust inspect module | Missing |

## Test Coverage Totals

| Test area | Python test functions | Rust SDK test functions | Notes |
| --- | ---: | ---: | --- |
| Connectors | 284 | 8 | Rust count includes Google Drive, Kafka target, Postgres source/target, and SurrealDB target. |
| Core/runtime | 374 | mostly in `pipeline.rs` 87 | Rust core tests are dense but do not cover many Python public APIs such as default env, exceptions, live components, full child target-state mount parity, serde/type checker. |
| Internal helpers | 44 | partly in `pipeline.rs` / `memo_batch_regressions.rs` | Python has dedicated memo fingerprint and type hint extraction tests. |
| Ops | 67 | 7 entity-resolution SDK tests plus ops_text crate tests | Rust lacks LiteLLM, SentenceTransformer wrapper, and LLM pair resolver parity. |
| Resources | 26 | ID plus LocalFS `FilePath`/matcher tests | Rust still lacks the full Python resource package surface. |
| CLI | 44 | none | Rust SDK has no matching CLI surface. |
| Total | 839 | 106 integration tests in `rust/sdk/cocoindex/tests`; 157 SDK crate tests including `src` unit tests | This is not a quality score, but it is a useful scale marker for parity work. |

Current Rust integration-test evidence:

| Rust test file | Count | Main coverage |
| --- | ---: | --- |
| `tests/pipeline.rs` | 87 | App/update/drop handles, context keys, memo, function macro, batching, `mount_each`, `max_inflight_components`, public target-state facade, stats groups, auto-refresh, localfs walk, dir target reconciliation. |
| `tests/entity_resolution.rs` | 7 | Empty input, candidate matching, top-N, existing-canonical policies, invalid resolver matches. |
| `tests/memo_batch_regressions.rs` | 3 | Pending-entry cleanup after batch serialization/fingerprint/count errors. |
| `tests/gdrive_source.rs` | 2 | Mocked recursive listing/MIME filtering and read/export behavior. |
| `tests/kafka_target.rs` | 1 | Live Kafka target insert/skip/update/tombstone behavior when Kafka is available. |
| `tests/postgres_source.rs` | 1 | Live Postgres source read/process/reconcile path when Postgres is available. |
| `tests/postgres_target.rs` | 2 | Live Postgres row target reconciliation and vector-index attachment reconciliation. |
| `tests/surrealdb_target.rs` | 2 | Live SurrealDB record/relation reconciliation plus conversation graph write smoke test. |

The Rust integration suite is dense, but much of Python's breadth is still only
represented indirectly. The highest-risk missing test shape is generic
target-state behavior because Python tests it once at the SDK layer while Rust
currently tests behavior through specific connectors.

## Recommended Implementation Roadmap

### Phase 1: Expose Rust Target-State Public API — DONE

SDK-level equivalents now exist (`rust/sdk/cocoindex/src/target_state.rs`,
re-exported from `lib.rs`):

- [x] `TargetState`
- [x] `TargetStateProvider`
- [x] `TargetHandler` (now with a default `attachments()` method)
- [x] `TargetActionSink` (+ `from_async_fn_with_children` for container targets)
- [x] `TargetReconcileOutput`
- [x] `ChildTargetDef` (child/attachment handler definition)
- [x] `declare_target_state`
- [x] `declare_target_state_with_child`
- [x] `mount_target` (foreground child-provider readiness via `use_mount`)
- [x] root provider registration (`register_root_target_states_provider`)
- [x] attachment provider access (`TargetStateProvider::attachment`)

Generic tests added in `tests/target_state.rs`:

- [x] flat target insert/update/delete/no-change
- [x] target state in components (declared inside `mount_each`)
- [x] mount target child insert/delete
- [x] attachments lifecycle (create + orphan cleanup)
- [x] provider generation destructive/lossy/no invalidation
- [x] ownership transfer between component scopes

### Phase 2: Refactor Existing Rust Connectors to the Public Shape

Refactor:

- Postgres
- SurrealDB
- Kafka target
- LocalFS target

Each should expose the same conceptual layers as Python:

- target spec constructor (`table_target`, `dir_target`, etc.)
- pending declaration (`declare_*_target`)
- mounted convenience (`mount_*_target`)
- target object methods (`declare_row`, `declare_record`, `declare_file`, etc.)

### Phase 3: Finish Existing Connector Parity

Postgres:

- `managed_by`
- SQL command attachment
- source iterator/items/snapshot
- remaining Python target tests

SurrealDB:

- vector index attachment
- `managed_by`
- schema evolution tests
- table removal/drop behavior

Kafka:

- source stream/map APIs
- custom delete value test

LocalFS:

- file path/matcher/filelike resources
- live watching

### Phase 4: Add Missing Connectors by Family

1. File/object source family: shared file resource abstraction, then S3, OCI,
   and full Google Drive parity.
2. Embedded/local table target: SQLite.
3. Vector stores: LanceDB, Qdrant, Turbopuffer.
4. Warehouse target: Doris.
5. Graph stores: Neo4j, FalkorDB.
6. Iggy source/target.
7. Ops/resource parity: SDK-facing text ops, embedder abstractions, and LLM
   resolver/transcriber helpers where Rust should match Python.

## Completion Criteria for True Parity

The Rust SDK should not be called Python-parity until:

- Every Python connector has a Rust connector or a documented intentional
  non-goal.
- Every Python public connector function/class has a Rust equivalent or a
  documented idiomatic Rust replacement.
- Every Python connector test family has a Rust test family or a documented
  environment-specific skip.
- The public Rust SDK can express target-state connectors without each connector
  using private `ctx` methods.
- Live source semantics are available to Rust connectors that need them.
- Existing Rust examples use SDK-native connectors rather than manual database
  writes where Python has native targets.

## Example Parity Matrix

This matrix is the practical "does the SDK feel the same?" layer. It compares
the checked-in examples, not just connector modules.

Current Rust example scan:

- Postgres-writing examples (`code-embedding`, `gdrive-text-embedding`,
  `hn-trending-topics`, `postgres-source`) declare rows through
  `postgres::mount_table_target`; their direct `sqlx::query` calls are query or
  readback paths, not manual target writes.
- SurrealDB-writing example (`conversation-to-knowledge`) declares records and
  relations through `cocoindex::surrealdb` targets.
- Kafka-writing example (`csv-to-kafka`) declares messages through
  `KafkaTopicTarget`.
- File-output examples (`files-transform`, `multi-codebase-summarization`) use
  `DirTarget::declare_file`.

So the remaining example gap is mostly missing SDK interface parity
(`FileLike`, live sources, target constructor/declaration split, vector stores,
graph stores), not a large amount of manual write code in existing Rust
examples.

| Python example | Python connector/API shape | Rust counterpart | Status | Action |
| --- | --- | --- | --- | --- |
| `examples/postgres_source` | `postgres.PgTableSource`, `postgres.mount_table_target` | `examples/rust/postgres-source` | Partial/close | Keep Rust typed read APIs, but add source-object/iterator parity or explicitly document Rust's typed `read_table` replacement. |
| `examples/hn_trending_topics` | Postgres table target, web/API source, table rows | `examples/rust/hn-trending-topics` | Partial/close | Rust writes through `postgres::mount_table_target`; remaining direct `sqlx::query` usage is read/query UI. Add target lifecycle tests that mirror Python table target behavior. |
| `examples/conversation_to_knowledge` | `localfs.walk_dir`, SurrealDB table/relation targets | `examples/rust/conversation-to-knowledge` | Partial | Rust has SurrealDB target/relation helpers, but still needs vector indexes, managed-by, richer schema tests, and closer target naming. |
| `examples/entire_session_search` | `localfs.walk_dir(live=True)`, `PatternFilePathMatcher`, `RecursiveSplitter`, `SentenceTransformerEmbedder`, two Postgres table targets/vector index | none | Missing/overlaps | Connector coverage overlaps with `code-embedding`, but the live localfs source, two-table Postgres target shape, and exact query helper path are not ported. |
| `examples/files_transform` | `localfs.walk_dir`, `localfs.dir_target`, `declare_file` | `examples/rust/files-transform` | Partial/close | Static LocalFS target/source shape exists; live mode remains missing. |
| `examples/multi_codebase_summarization` | LocalFS source plus generated file target | `examples/rust/multi-codebase-summarization` | Partial | Static LocalFS pieces exist; live mode and exact source-resource shape remain different. |
| `examples/csv_to_kafka` | Kafka topic target | `examples/rust/csv-to-kafka` | Partial | Rust has target only. Add Kafka source stream/map APIs and constructor/declaration split. |
| `examples/code_embedding` | LocalFS source, Postgres table target/vector index | `examples/rust/code-embedding` | Partial | Postgres target is the right native direction. Remaining gaps are localfs source shape and Postgres target attachments/options. |
| `examples/amazon_s3_embedding` | S3 object source, Postgres target | none | Missing | Needs Rust object-source abstraction and S3 connector. |
| `examples/gdrive_text_embedding` | Google Drive source, Postgres target | `examples/rust/gdrive-text-embedding` | Partial/close | Rust example compiles and uses native Google Drive source plus Postgres target. Remaining gap is shared Python-style file path/items abstraction and live source semantics. |
| `examples/oci_object_storage_embedding` | OCI object source, Postgres target, live object semantics | none | Missing | Wait for Rust live source/map public API, then port OCI. |
| `examples/image_search` | LocalFS image source, `FileLike`, `PatternFilePathMatcher`, `VectorSchema`, Qdrant collection target, CLIP embedding, FastAPI query path | none | Missing | Needs Rust Qdrant collection target plus shared file/image resource shape; frontend/API parity is example-level. |
| `examples/image_search_colpali` | LocalFS image source, `MultiVectorSchema`, Qdrant multivector collection target, ColPali embedding, FastAPI query path | none | Missing | Needs Rust Qdrant multivector support and generic vector/multivector schema resources before example parity. |
| `examples/code_embedding_lancedb` | LocalFS source, LanceDB target | none | Missing | LanceDB target exists; exact example still not ported. |
| `examples/text_embedding_lancedb` | LocalFS source, LanceDB target | `examples/rust/text-embedding-lancedb` | Partial/close | Rust example exists with native LanceDB target; remaining gaps are live LocalFS and fuller LanceDB optimize/schema behavior. |
| `examples/kafka_to_lancedb` | Kafka source, LanceDB target | none | Missing | Needs both Kafka source and LanceDB target. |
| `examples/text_embedding_qdrant` | LocalFS source, Qdrant collection target | `examples/rust/text-embedding-qdrant` | Partial/close | Native Qdrant collection target and matching MiniLM embedding path exist. Remaining gaps are live LocalFS, Python constructor/declaration split, and fuller Qdrant target parity. |
| `examples/text_embedding_turbopuffer` | LocalFS source, Turbopuffer namespace target | `examples/rust/text-embedding-turbopuffer` | Partial/close | Native Turbopuffer namespace target and matching MiniLM embedding path exist. Remaining gaps are live LocalFS, Python constructor/declaration split, and fuller Turbopuffer schema parity. |
| `examples/meeting_notes_graph_neo4j` | Google Drive source, Neo4j table/relation targets | `examples/rust/meeting-notes-graph-neo4j` | Partial | Rust example uses deterministic local Markdown notes plus native `neo4j` graph targets. Remaining example parity is Google Drive/LLM extraction shape and fuller Neo4j index/vector target coverage. |
| `examples/meeting_notes_graph_falkordb` | Google Drive source, FalkorDB table/relation targets | `examples/rust/meeting-notes-graph-falkordb` | Partial | Rust example uses deterministic local Markdown notes plus native `falkordb` graph targets. Remaining example parity is Google Drive/LLM extraction shape and fuller FalkorDB index/vector target coverage. |
| `examples/text_embedding` | LocalFS source, Postgres target | `examples/rust/text-embedding` | Partial/close | Rust standalone example exists; remaining gaps are live LocalFS and exact Python resource/target constructor shape. |
| `examples/pdf_embedding`, `examples/pdf_to_markdown` | LocalFS file resources plus PDF parsing targets | none | Missing | Connector parity depends on localfs file abstraction; transforms are example-level. |
| `examples/audio_to_text` | External audio/transcription transforms plus target writes | none | Out of connector parity scope | Port after connector/runtime parity, unless product wants Rust examples for these flows. |
| `examples/paper_metadata` | External paper metadata extraction plus target writes | none | Out of connector parity scope | Port after connector/runtime parity; likely depends more on transform/client parity than a new storage connector. |
| `examples/patient_intake_extraction_baml` | External BAML extraction plus target writes | none | Out of connector parity scope | Port only if Rust examples should cover BAML-style extraction; not a connector blocker. |
| `examples/patient_intake_extraction_dspy` | External DSPy extraction plus target writes | none | Out of connector parity scope | Port only if Rust examples should cover DSPy-style extraction; not a connector blocker. |

## Appendix A: Top-Level Python API Mapping

Python top-level `cocoindex.__all__` is the 76-name
`python/cocoindex/_internal/api.py::__all__` surface. Rust's flat top-level
surface is much smaller, so this table groups every Python export by current
Rust parity status.

| Status | Python top-level names | Rust evidence / note |
| --- | --- | --- |
| Good/close | `App`, `DropHandle`, `UpdateHandle`, `ContextKey`, `StatsGroupHandle`, `fn` | Rust exposes `App`, `DropHandle`, `UpdateHandle`, `ContextKey`, `StatsGroupHandle`, and `#[cocoindex::function]`. Shape is Rust-idiomatic rather than Python-identical. |
| Partial/different app surface | `AppConfig`, `show_progress`, `ComponentStats`, `UpdateSnapshot`, `UpdateStats`, `UpdateStatus` | Rust uses `AppBuilder`, `UpdateOptions`, `Progress`, `RunStats`, and handle snapshots. Needs mapping docs/tests if these are considered API parity requirements. |
| Partial context/component surface | `stats_group`, `use_context`, `get_component_context`, `ComponentContext`, `ComponentSubpath`, `component_subpath`, `mount_each`, `map`, `auto_refresh` | Rust exposes `Ctx`, `Ctx::stats_group`, `Ctx::get_key`, `Ctx::mount_each`, `Ctx::map`, `Ctx::auto_refresh`; missing Python global function shape and component context/subpath API. |
| Partial/Good generic target-state surface | `ChildTargetDef`, `TargetState`, `TargetStateProvider`, `TargetReconcileOutput`, `TargetHandler`, `TargetActionSink`, `PendingTargetStateProvider`, `declare_target_state`, `declare_target_state_with_child`, `register_root_target_states_provider`, `mount_target` | Rust exposes `ChildTargetDef`, `TargetState`, `TargetStateProvider`, `TargetReconcileOutput`, `TargetHandler`, `TargetActionSink`, declarations, root registration, handler attachments, and foreground `mount_target` readiness. Python's explicit `PendingTargetStateProvider` type is represented Rust-idiomatically by typed child providers returned from declaration/mount calls. |
| Missing/default environment surface | `Environment`, `EnvironmentBuilder`, `LifespanFn`, `lifespan`, `start`, `stop`, `start_blocking`, `stop_blocking`, `default_env`, `runtime` | Rust examples use explicit `App::builder`; no Python-style default env/lifespan API. |
| Missing live/error surface | `LiveComponent`, `LiveComponentOperator`, `LiveMapFeed`, `LiveMapView`, `LiveMapSubscriber`, `ComponentMountHandle`, `mount`, `use_mount`, `ExceptionContext`, `ExceptionHandler`, `exception_handler` | Rust exposes only limited `auto_refresh`; no public live component, mount/use_mount, or scoped exception handler equivalents. |
| Missing runner surface | `GPU`, `Runner` | Rust has batching helpers but no Runner/GPU/subprocess runner equivalent. |
| Missing serialization/fingerprint surface | `unpickle_safe`, `serialize_by_pickle`, `memo_fingerprint`, `register_memo_key_function`, `NotMemoKeyable` | Rust has serde-based memo key helpers in `memo.rs`, but no Python pickle/safe-unpickle/registry surface. |
| Missing/pending marker and resolve protocol | `MaybePendingS`, `PendingS`, `ResolvedS`, `ResolvesTo` | Rust uses static types and immediate values; no pending marker API exposed. |
| Missing/different settings/stable symbols | `Settings`, `LmdbSettings`, `ROOT_PATH`, `StablePath`, `StableKey`, `Symbol` | Rust has `AppBuilder` db path and core stable keys internally; no public settings object or top-level stable path/symbol API. |
| Missing/different typing sentinels | `NON_EXISTENCE`, `NonExistenceType`, `is_non_existence`, `MemoStateOutcome` | Rust represents state through typed results/core internals; no public Python-like sentinel surface. |

Implication: generic target-state connector structure can now be designed the
same way as Python, but many resource/connectorkit helper types are still absent
or connector-local in Rust.

## Appendix B: Python Connector Public Surface

This is the connector interface inventory Rust needs to match or replace with a
documented Rust-native equivalent.

| Connector | Public Python surface |
| --- | --- |
| `localfs` | `FilePath`, `to_file_path`, `File`, `DirWalker`, `walk_dir`, `DirTarget`, `dir_target`, `declare_dir_target`, `mount_dir_target`, `declare_file` |
| `postgres` | `PgSourceSpec`, `RowFetcher`, `PgTableSource`, `PgType`, `ColumnDef`, `TableSchema`, `TableTarget`, `table_target`, `declare_table_target`, `mount_table_target`, `create_pool`, `ValueEncoder` |
| `surrealdb` | `ConnectionFactory`, `SurrealType`, `ColumnDef`, `TableSchema`, `TableTarget`, `RelationTarget`, `table_target`, `declare_table_target`, `mount_table_target`, `relation_target`, `declare_relation_target`, `mount_relation_target`, `ValueEncoder` |
| `kafka` | `TopicStream`, `topic_as_stream`, `topic_as_map`, `IsDeleteFn`, `KafkaTopicTarget`, `kafka_topic_target`, `declare_kafka_topic_target`, `mount_kafka_topic_target`, `DeletionValueFn` |
| `iggy` | `TopicStream`, `topic_as_stream`, `topic_as_map`, `IsDeleteFn`, `KeyFn`, `IggyTopicTarget`, `iggy_topic_target`, `declare_iggy_topic_target`, `mount_iggy_topic_target`, `DeletionValueFn` |
| `amazon_s3` | `S3FilePath`, `S3File`, `get_object`, `read`, `S3Walker`, `list_objects` |
| `oci_object_storage` | `OCIFilePath`, `OCIFile`, `get_object`, `read`, `OCIWalker`, `list_objects` |
| `google_drive` | Package export: `DriveFileInfo`, `DriveFile`, `GoogleDriveSourceSpec`, `GoogleDriveSource`, `list_files`; `_source.py` also defines non-`__all__` `DriveFilePath` |
| `sqlite` | `ManagedConnection`, `Vec0TableDef`, `SqliteType`, `ColumnDef`, `TableSchema`, `TableTarget`, `table_target`, `declare_table_target`, `mount_table_target`, `connect`, `managed_connection`, `ValueEncoder` |
| `doris` | `DorisError`, `DorisConnectionError`, `DorisAuthError`, `DorisStreamLoadError`, `DorisSchemaError`, `RetryConfig`, `DorisType`, `ColumnDef`, `TableSchema`, `DorisConnectionConfig`, `ManagedConnection`, `connect`, `VectorIndexDef`, `InvertedIndexDef`, `DorisTableTarget`, `table_target`, `declare_table_target`, `mount_table_target`, `connect_async`, `build_vector_search_query` |
| `lancedb` | `LanceType`, `ColumnDef`, `TableSchema`, `TableTarget`, `table_target`, `declare_table_target`, `mount_table_target`, `connect_async`, `connect`, `LanceAsyncConnection` |
| `qdrant` | `QdrantVectorDef`, `CollectionSchema`, `create_client`, `CollectionTarget`, `collection_target`, `declare_collection_target`, `mount_collection_target`, `PointStruct` |
| `turbopuffer` | `VectorDef`, `NamespaceSchema`, `Row`, `NamespaceTarget`, `namespace_target`, `declare_namespace_target`, `mount_namespace_target`, `AsyncTurbopuffer`, `DistanceMetric` |
| `neo4j` | `ConnectionFactory`, `Neo4jType`, `ColumnDef`, `TableSchema`, `TableTarget`, `RelationTarget`, `table_target`, `declare_table_target`, `mount_table_target`, `relation_target`, `declare_relation_target`, `mount_relation_target`, `ValueEncoder`, Cypher builder helpers |
| `falkordb` | `ConnectionFactory`, `FalkorType`, `ColumnDef`, `TableSchema`, `TableTarget`, `RelationTarget`, `table_target`, `declare_table_target`, `mount_table_target`, `relation_target`, `declare_relation_target`, `mount_relation_target`, `ValueEncoder`, Cypher builder helpers |

### Lower-Level Python Helper Exports

These are public exports, but they are connector/framework helper APIs rather
than end-user connector entry points.

| Area | Exact Python exports | Rust status |
| --- | --- | --- |
| Connectorkit mount naming | `default_subpath_name` | Missing public helper; Rust uses closure/key functions directly. |
| Connectorkit fingerprint | `Fingerprint`, `Fingerprintable`, `fingerprint_object`, `fingerprint_bytes`, `fingerprint_str` | Partial/different; Rust exposes memo key/fingerprint helpers in `memo.rs`, but no connectorkit module. |
| Connectorkit statediff | `CompositeTrackingRecord`, `DiffAction`, `ManagedBy`, `MutualTrackingRecord`, `TrackingRecordTransition`, `diff`, `diff_composite`, `resolve_system_transition` | Missing public Rust connectorkit layer; this is a blocker for consistent target connector implementations. |
| Resource schema helpers | `get_vector_schema`, `get_multi_vector_schema` | Missing generic Rust vector-schema provider helpers. |
| Neo4j Cypher helpers | `IDENTIFIER_RE`, `build_constraint_create`, `build_constraint_drop`, `build_node_delete`, `build_node_index_create`, `build_node_index_drop`, `build_node_upsert`, `build_relationship_delete`, `build_relationship_index_create`, `build_relationship_index_drop`, `build_relationship_upsert`, `build_vector_index_create`, `build_vector_index_drop`, `constraint_name`, `index_name`, `validate_identifier`, `vector_index_name` | Missing until Rust Neo4j connector exists. |
| FalkorDB Cypher helpers | `IDENTIFIER_RE`, `build_node_upsert`, `build_node_delete`, `build_relationship_upsert`, `build_relationship_delete`, `build_node_index_create`, `build_node_index_drop`, `build_relationship_index_create`, `build_relationship_index_drop`, `build_vector_index_create`, `build_vector_index_drop`, `validate_identifier` | Missing until Rust FalkorDB connector exists. |

## Appendix C: Current Rust SDK Public Surface

The table below groups method-heavy Rust modules rather than listing every
method in the top-level matrix. Method names are included when they materially
define the public SDK shape.

| Rust module | Public surface |
| --- | --- |
| `app` | `AppBuilder` (`db_path`, `lmdb_max_dbs`, `lmdb_map_size`, `max_inflight_components`, `provide`, `provide_key`, `build`, `build_blocking`), `App` (`open`, `open_blocking`, `builder`, `run`, `update`, `update_with_options`, `start_update`, `start_update_with_options`, `update_blocking`, `update_blocking_with_options`, `start_drop_state`, `drop_state`, `drop_state_blocking`, `name`), `UpdateOptions`, `StatsGroupOptions`, `Progress::is_done`, `UpdateHandle`/`DropHandle`/`StatsGroupHandle` (`stats_snapshot`, `changed`, `result`) |
| `ctx` | `ContextKey` (`new`, `new_detect_change`, `new_with_state`, `name`, `detect_change`), `Ctx` (`get_or_err`, `try_get`, `get_key`, `has_pipeline_context`, `stats_group`, `stats_group_with_options`, `auto_refresh`, `scope`, `memo`, `batch`, `mount_each`, `map`, `write_file`); internal macro hook `__coco_tracked_fn` is public only for generated macro code |
| `entity_resolution` | `CanonicalSide`, `ExistingCanonicalPolicy`, `PairDecision` (`no_match`, `matched`, `matched_with`), `ResolutionEvent`, `ResolvedEntities` (`canonical_of`, `canonicals`, `groups`, `to_map`), `EntityEmbedder`, `PairResolver`, `ResolveOptions`, `resolve_entities` |
| `error` | `Error`, `Result` |
| `fs` | `FileEntry` (`path`, `relative_path`, `stem`, `fingerprint`, `content`, `content_str`, `key`), `walk`, `DirTarget` (`mount`, `dir`, `declare_file`), `mount_dir_target` |
| `gdrive` | `DriveFile` (`key`), `GoogleDriveClient` (`from_service_account_file`, `from_static_token`, `with_base_url`, `state_id`, `read`, `read_text`), `GoogleDriveSource` (`new`, `mime_types`, `client`, `list_files`) |
| `id` | `generate_id`, `generate_id_default`, `generate_uuid`, `generate_uuid_default`, `IdGenerator` (`new`, `with_deps`, `next_id`, `next_id_default`), `UuidGenerator` (`new`, `with_deps`, `next_uuid`, `next_uuid_default`) |
| `kafka` | `KafkaProducer` (`connect`, `state_id`, `ensure_topic`), `DeletionValueFn`, `KafkaTopicOptions`, `KafkaTopicTarget` (`topic`, `declare_message`), `mount_kafka_topic_target` |
| `memo` | `cached`, `cached_by_fingerprint`, `key_bytes`, `key_bytes_result`, `key_fingerprint_result`, `new_key_fingerprinter`, `write_key_fingerprint_part`, `finish_key_fingerprinter`, `batch`, `batch_by_fingerprint` |
| `neo4j` | `Graph` (`connect`, `state_id`), `ColumnDef`, `TableSchema` (`new`, `columns`, `primary_key`), `TableTarget` (`table_name`, `declare_record`), `RelationTarget` (`declare_relation`, `declare_relation_record`), `mount_table_target`, `mount_table_target_with_options`, `mount_relation_target`, `mount_relation_target_with_options` |
| `falkordb` | `Graph` (`connect`, `state_id`), `ColumnDef`, `TableSchema` (`new`, `columns`, `primary_key`), `TableTarget` (`table_name`, `declare_record`), `RelationTarget` (`declare_relation`, `declare_relation_record`), `mount_table_target`, `mount_table_target_with_options`, `mount_relation_target`, `mount_relation_target_with_options` |
| `postgres` | `Database` (`connect`, `from_pool`, `pool`, `state_id`), `ColumnDef`, `TableSchema` (`new`, `columns`, `primary_key`), `TableTarget` (`table_name`, `declare_row`, `declare_vector_index`), `VectorIndexOptions`, `mount_table_target`, `ReadTableOptions` (`new`, `pg_schema_name`, `columns`), `read_table`, `read_table_with_options` |
| `qdrant` | `QdrantConnection` (`connect`, `state_id`, `client`), `Distance`, `CollectionSchema` (`new`), `CollectionTarget` (`collection_name`, `declare_point`), `mount_collection_target`, `mount_collection_target_with_options`, `vector_search` |
| `statediff` | `ManagedBy`, `ManagedTargetOptions`, `MutualTrackingRecord`, `TrackingRecordTransition`, `DiffAction`, `resolve_system_transition`, `diff` |
| `surrealdb` | `ColumnDef`, `TableSchema`, `IntoRecordId`, `Graph` (`connect`, `state_id`, `count`), `TableTarget` (`table_name`, `declare_record`), `RelationTarget` (`table_name`, `declare_relation`, `declare_relation_record`, `declare_relation_between`, `declare_relation_record_between`), `mount_table_target`, `mount_table_target_with_schema`, `mount_relation_target`, `mount_relation_target_many`, `mount_relation_target_unconstrained`, `RecordIdValue` |
| `turbopuffer` | `TurbopufferConnection` (`new`, `with_base_url`, `state_id`, `delete_namespace`), `DistanceMetric`, `NamespaceSchema` (`new`), `NamespaceTarget` (`namespace`, `declare_row`), `mount_namespace_target`, `mount_namespace_target_with_options`, `vector_search` |
| `cocoindex_ops_text` | `PatternMatcher`, `detect_language`, `SeparatorSplitter`, `RecursiveChunker`, `Chunk`, `TextRange`, `OutputPosition` |

## Appendix D: Test Families Rust Should Port

These are the highest-value Python test families to mirror first because they
lock down SDK semantics, not just examples.

| Priority | Python tests to mirror | Why |
| --- | --- | --- |
| 1 | Generic target-state tests: flat targets, component target states, attachments, provider generation, ownership transfer | Unlocks every connector and prevents connector-specific one-offs. |
| 1 | Postgres target/source tests | Postgres is the most complete Rust connector and should become the reference implementation. |
| 1 | SurrealDB table/relation target tests | Rust already has this connector; parity tests will expose missing schema/vector/managed behavior quickly. |
| 2 | LocalFS path/live tests | Required by most examples and object-source connectors. |
| 2 | Kafka source/stream/map tests | Required before Kafka-to-target examples can be Rust-native. |
| 2 | SQLite target tests | Good next table-target connector after generic target-state API is public. |
| 3 | LanceDB/Qdrant/Turbopuffer tests | Vector target family; should share no-change/upsert/delete test patterns. |
| 3 | Neo4j/FalkorDB graph target tests | Graph family; should share relation endpoint/index test patterns with SurrealDB concepts. |
| 3 | S3/OCI/GDrive object source tests | Depends on source abstraction and live API decisions. |

## Appendix E: Connector Test Port Checklist

This checklist is derived from the current Python test names under
`python/tests/connectors` and current Rust SDK tests under
`rust/sdk/cocoindex/tests`.

| Connector/test file | Python behaviors to port | Current Rust coverage | Priority |
| --- | --- | --- | --- |
| `test_postgres_source.py` | `fetch_rows` as dict/row type/row factory, selected columns, empty table, schema-qualified table, `items()`, repeatable-read snapshot, row factory vs row type exclusivity | `postgres_source_reads_processes_and_reconciles_when_available` | High |
| `test_postgres_target.py` | vector index fingerprint/no-change, halfvec opclass, SQL command attachment with/without teardown, mixed rows plus attachments, NUL stripping in text/jsonb, failed column-drop retry | `postgres_table_target_reconciles_rows_when_available`, `postgres_vector_index_target_reconciles_when_available` | High |
| `test_surrealdb_target.py` | schemafull/schemaless create, update/delete/no-op, drop table, add/remove/change schema fields, relation variants, transaction/table ordering, multiple tables shared sink, mtree/hnsw vector indexes, user-managed table, `declare_row` alias, type mapping, identifier/string escaping | `surrealdb_targets_reconcile_records_and_relations_when_available`, `surrealdb_targets_e2e_conversation_graph_write_when_available` | High |
| `test_kafka_source.py` | stream consumption, payload filtering, in-order/out-of-order completion, partial drain, null key handling, tombstone deletion, custom deletion predicate, watermark readiness, partition rebalance | none | High |
| `test_kafka_target.py` | reconcile output, non-existence behavior, child handler creation, skip unchanged, prev-may-be-missing upsert, tombstone/delete callback, custom deletion value, multiple topics, memo key | `kafka_target_produces_skips_updates_and_tombstones_when_available` | High |
| `test_iggy_source.py` | offset tracking after contiguous completion, ready fast path, duplicate offset skip, payload view, multi-partition watermark, map key/deletion predicate | none | Medium |
| `test_file_path.py`, `test_source_items.py`, `test_localfs_live.py` | stable memo key with/without base dir, localfs no global base-dir registry, flat/recursive/empty `items()`, live add/edit/delete | partial `fs_walk_integration`, dir target tests in `pipeline.rs` | High |
| `test_amazon_s3.py` | basic/prefix listing, include/exclude matcher, max file size, empty bucket, skip directory markers, async `items()`, `read`/`read_text`/`size`, file path properties, full-key resolve, `get_object`, nonexistent object error, memo key and memo state, bucket-aware keys | none | Medium |
| `test_oci_object_storage.py` | prefix/matcher/max-size/pagination, `get_object`/`read`, `exists()` metadata caching and 404, live event cutoff/readiness/future/malformed/deleted/cross-bucket/transient-error/cancel behavior, path matcher, memo key, real `mount_each` scan | none | Medium/High after live API |
| `test_google_drive` | No dedicated Python test file exists yet | `gdrive_source_lists_recursively_and_filters_mime_types`, `gdrive_client_reads_binary_and_exports_google_docs`; unit tests for JWT/list parsing/export mapping | Add Python parity tests or document Rust-only coverage |
| `test_sqlite_target.py` | CRUD, schema types, drop table, no-change optimization, multiple tables, dict rows, user-managed table, vec0 basic/partition/aux columns/schema switch/validation/extension errors/column overrides, regular-vs-vec0 switch | none | High after target-state API |
| `test_lancedb_target.py` | add columns preserving existing rows, nullable materialization for new non-null columns, optimize interval, optimize no-overlap, mutation preservation/retry after optimize failure, existing/new table optimize behavior, async add-columns API | Rust e2e covers create/upsert/search/delete and additive scalar column preservation; unit tests cover schema validation, nullability, nullable vectors, predicates | Medium |
| `test_doris_target.py` | CRUD, dict rows, vector index creation, no-change optimization | none | Medium |
| `test_qdrant_target.py` | No dedicated Python test file currently found | `qdrant_target.rs` unit/live-when-available tests | Add Python parity tests or document Rust-only coverage; still needs named/multivector and schema-change coverage |
| `test_turbopuffer_target.py` | insert/update/delete, named vectors, f32/f16, unsupported dtype, single vector list/ndarray/dict rejection, missing/non-dict named vectors, reserved attribute collisions, empty named vectors, schema construction | `turbopuffer_target.rs` unit/live-when-available tests | Medium; Rust still lacks Python's full named-vector/schema validation matrix |
| `test_neo4j_target.py` | node CRUD/no-op, relationship endpoint merge/delete/no cascade, vector indexes, identifier/name builders, node/relationship index create/drop, uniqueness constraints, vector dimension validation, schema and target validation, connection factory validation, dataclass/custom PK | none | Medium/High for graph family |
| `test_falkordb_target.py` | node CRUD/no-op, relationships, vector indexes, identifier validation, single/compound PK relations, endpoint merge/no cascade, node/relationship indexes, vector index create/drop naming, schema/target validation, dataclass/custom PK | none | Medium/High for graph family |

## Appendix F: Runtime, Resource, Ops, and CLI Test Port Checklist

This checklist is derived from current Python tests outside
`python/tests/connectors`. It is intentionally grouped by behavior because the
Python runtime suite is broad: 374 core tests, 44 internal tests, 26 resource
tests, 67 ops tests, and 44 CLI tests.

| Python test family | Python behaviors to port or explicitly reject | Current Rust coverage | Priority |
| --- | --- | --- | --- |
| App lifecycle/drop/default env | `test_trivial_app.py`, `test_update_handle.py`, `test_app_drop.py`, `test_default_env.py`, `test_default_env_async.py`, `test_lazy_environment_lock.py` cover sync/async app forms, handles/watch/progress, drop cleanup/retry, default env startup and env vars | Rust `pipeline.rs` covers app run/open, update/drop handles, blocking/async updates, stats snapshots, drop state clears memoization | Medium/High; default environment is missing/different |
| Concurrency control | `test_concurrency_control.py` covers max-inflight quota enforcement, nested-mount deadlock prevention, default limit, and `COCOINDEX_MAX_INFLIGHT_COMPONENTS` env fallback | Rust `pipeline.rs` now covers explicit `max_inflight_components` quota enforcement and nested scope no-deadlock behavior | Medium; default/env fallback remains missing because Rust has no default-env loader parity |
| Generic target states | `test_flat_target_states.py`, `test_component_target_states.py`, `test_attachment_target_states.py`, `test_provider_generation.py`, `test_ownership_transfer.py`, `test_typed_serde_target.py` cover insert/upsert/delete/no-change, preview, components, mount target, attachments, destructive/lossy generation, ownership transfer, typed handler wrapping | Rust `tests/target_state.rs` covers public typed CRUD/no-change, component declarations, mount-target children, multiple mounted child targets from one provider, attachments, destructive/lossy/no invalidation, and ownership transfer; preview-specific runtime coverage is still missing | Medium |
| Live components and exception routing | `test_live_component.py`, `test_exception_handlers.py`, `test_auto_refresh.py`, `test_cancellation.py`, `test_component_submit_order.py` cover live catch-up/incremental delete/update, ready signaling, background errors, scoped/global handlers, cancellation, parent waits for child ready | Rust has `ctx_auto_refresh_*` tests only; no public live component or exception handler API | High |
| Function memoization and logic tracking | `test_function_memo.py`, `test_logic_change_detection.py`, `test_component_memo.py`, `test_function_class_methods.py`, `test_function_misc.py`, `test_full_reprocess.py` cover memo with targets/components, nested calls, decorator/code/deps invalidation, bound/static/class methods, full reprocess semantics | Rust `pipeline.rs` has strong memo/macro/function hash/context dependency tests, but less class/method/dynamic logic-mode surface because Rust shape differs | Medium; document idiomatic differences. Python exposes `LogicTracking`; Rust has macro/hash behavior but no named `LogicTracking` type alias. |
| Batching and runners | `test_function_batching.py` covers sync/async batching, max batch size, extra arg grouping, `Runner`, `GPU`, subprocess runner, memo with runner | Rust covers `Ctx::batch`, `memo::batch`, macro batch cache hits/errors, duplicate keys; no Runner/GPU/subprocess equivalent | Medium/High if Python Runner is product parity goal |
| Context keys and memo state validation | `test_context_tracked_key.py`, `test_context_tracked_state_validation.py`, `test_memo_state_validation.py`, `test_memo_state_use_context.py`, internal `test_context_keys.py` | Rust has context key state/memo invalidation tests and missing-resource errors | Medium; add validation edge cases and transitive invalidation parity |
| Serialization, datatype, type checking | `test_serde.py`, `test_safe_unpickle.py`, `test_datatype.py`, `test_type_checker.py`, `test_typed_serde_memo.py`, internal `test_type_hint_extraction.py` | Rust uses serde/compile-time types; has memo complex type tests but no dynamic Python type checker/unpickle equivalent | Different; document non-goals and add serde boundary tests for connector schemas |
| Memo fingerprint internals | internal `test_memo_fingerprint.py` covers order-independent dict/set fingerprints, NaN, hooks/registry, cycles, dataclass/pydantic, call fingerprint parameter transforms | Rust has memo key/fingerprint helper usage tests; no dedicated parity suite | Medium; add Rust fingerprint determinism/collision-shape tests |
| Settings and stats | `test_settings.py`, `test_stats_group.py` cover env loading, legacy settings compatibility, stats group nesting/stdout/live/reporting | Rust has `AppBuilder` setters and stats group tests | Medium; document settings object divergence and add env-loading tests if desired |
| Resources | `resources/test_id.py`, `resources/test_file_path_matcher.py` cover IDs/UUIDs, path matcher include/exclude/alternation/invalid glob/posix conversion | Rust ID/UUID tests are good; no file path matcher API | High for file/object source parity |
| Ops | `ops/test_text.py`, `ops/test_entity_resolution.py`, `ops/test_llm_pair_resolver.py`, `ops/test_litellm_transcriber.py`, `ops/test_embedder_refactor.py` cover text splitting/language configs, rich entity resolution policies/concurrency/errors/events, LLM pair resolver retry/hallucination behavior, LiteLLM embedder/transcriber | Rust has `entity_resolution.rs` core tests and `cocoindex_ops_text` crate APIs; no LLM pair resolver, LiteLLM, or general embedder wrapper | Medium |
| CLI | `cli/test_cli.py` covers app discovery, update/drop/show/init, env selection, default DB env vars, confirmation/force, preview actions and rejects, full reprocess CLI behavior | No Rust CLI parity surface | Low unless Rust SDK should ship CLI |

## Appendix G: Concrete Fix Order

1. Public Rust target-state API.
2. Refactor Postgres target to expose `table_target`, `declare_table_target`,
   and `mount_table_target` using that API.
3. Refactor SurrealDB and Kafka target to the same API shape.
4. Add public localfs source/target constructors and file path abstraction.
5. Add Kafka source stream/map.
6. Add SQLite as the next native table target.
7. Add shared file-resource abstractions (`FilePath`, metadata, content read,
   item keys), then complete Google Drive parity and add S3/OCI.
8. Add vector store targets, then graph store targets.
9. Add missing ops/resource wrappers where Rust should match Python:
   SDK-facing text ops, general embedder trait/wrapper, LLM pair resolver, and
   transcriber helpers.

This order avoids building more manual examples while the core Rust connector
shape is still narrower than Python's.
