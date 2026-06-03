# Rust SDK vs Python SDK Parity

Branch: `rust-sync`

Last reviewed: 2026-06-02 against the local worktree.

This is an action plan, not a changelog. It compares the current Rust SDK in
`rust/sdk/cocoindex/src` with the Python SDK in `python/cocoindex`, using
Python's public exports, connector implementations, tests, and examples as the
reference behavior.

Current scale marker:

- Python tests: 839 test functions across connectors, core, ops, resources,
  CLI, and internals.
- Rust SDK tests: 143 integration tests plus 100 source-unit test annotations.
- Rust connector e2e tests are often gated by external services; skipped live
  tests are expected when credentials or services are not available.

## CocoIndex Concepts To Keep Aligned

These are the SDK contracts that matter most for parity.

- **Components and memoization:** user code runs in scoped components. Stable
  paths, context-key fingerprints, logic hashes, and explicit memo keys decide
  what can be reused.
- **Sources:** Python sources generally expose iterable items, often as
  `(stable_key, item)` pairs. File-like sources also provide stable path keys,
  lazy metadata, cached full reads, and content fingerprints.
- **Targets:** targets are declarative. A run declares desired target states;
  reconciliation inserts/updates changed states, skips unchanged states, and
  deletes orphaned states. Container targets can expose child target providers
  and attachments.
- **Managed ownership:** `managed_by=system` lets CocoIndex create/drop backing
  resources. `managed_by=user` leaves DDL and destructive changes to the caller
  while still reconciling child rows/messages where appropriate.
- **Live components:** live connectors feed changes into the same component and
  target-state model, with exception handlers deciding whether background
  failures are swallowed or propagated.
- **Ops/resources:** Python provides reusable text chunking, embedders,
  vector-schema providers, file resources, and ID generators. Rust has some of
  these, but examples still hand-roll several common pieces.

## Done

Do not spend parity time here unless tests fail or Python behavior changes.

| Area | Rust status |
| --- | --- |
| Target-state API | Public `TargetState`, `TargetStateProvider`, `TargetHandler`, `TargetActionSink`, `ChildTargetDef`, `declare_target_state`, `declare_target_state_with_child`, `mount_target`, root-provider registration, attachments, provider-generation `memo_key`. Covered by `tests/target_state.rs`. |
| Managed target helpers | `ManagedBy`, `ManagedTargetOptions`, `MutualTrackingRecord`, `diff`, `resolve_system_transition`, `CompositeTrackingRecord`, `diff_composite`. |
| Live SDK API | `LiveComponent`, `LiveComponentOperator`, `LiveMapFeed`, `LiveMapView`, `LiveMapSubscriber`, `Ctx::mount_live`, `mount_live_with_handler`, `mount_each_live`, `ExceptionHandler`, `ExceptionContext`. Covered by `tests/live_component.rs`. |
| Core app/context API | `App`, run/update/drop handles, blocking and async update paths, stats groups, `ContextKey::{new,new_detect_change,new_with_state}`, `Ctx::memo`, `batch`, `map`, `scope`, `mount_each`, `auto_refresh`, `max_inflight_components`. |
| Stable IDs | `generate_id`, `generate_uuid`, `IdGenerator`, `UuidGenerator`; repeated identical deps get occurrence-distinct generator IDs. |
| Shared file source API | Public `file` module with `FilePath`, `FileMetadata`, `FileContentCache`, async `FileLike`, `FileSourceItem`, `FilePathMatcher`, `MatchAllFilePathMatcher`, and `PatternFilePathMatcher`. LocalFS, Google Drive, and S3 use this shared contract for lazy metadata, cached reads, content fingerprints, Python-style path memo keys, mtime/content-fingerprint memo states, path matchers, and stable `(key, item)` iteration. |
| Implemented target connectors | Postgres, SQLite, SurrealDB, Kafka target, Iggy target, LanceDB, Qdrant, Turbopuffer, Neo4j, FalkorDB, and LocalFS directory target use the public target-state API and expose constructor/declare/mount style helpers where applicable. |
| Implemented sources | LocalFS static walk, Google Drive listing/download/export, Postgres table reads, Amazon S3 (`amazon_s3`: `list_objects`/`get_object`/`read` over `aws-sdk-s3`, MinIO-compatible), OCI Object Storage (`oci_object_storage`: native REST + RSA-SHA256 HTTP Signature signing, `~/.oci/config` auth, `list_objects`/`get_object`/`read`/`read_range` on the shared `file` contract). |
| Entity resolution core | Rust has `resolve_entities`, embedder/resolver traits, policies, resolution events, and tests. |

## P1: Build These Next

These are the highest-value gaps because they block multiple Python connectors
or examples.

### 1. Remaining File Source Work

The connector-neutral Rust `file` module now matches Python's base file
resource contract for LocalFS, Google Drive, and S3. Remaining parity work is
connector breadth and live behavior:

1. Add `walk_dir(..., live=true)` / live map support for LocalFS.
2. **S3 source — ✅ done.** `amazon_s3` (`feature = "amazon_s3"`): `S3Client`
   (`connect` reads standard AWS env incl. `AWS_ENDPOINT_URL` for MinIO),
   `S3FilePath`/`S3File`, `list_objects` → `S3Walker` (`list`/`items`, prefix +
   shared `PatternFilePathMatcher` + `max_file_size`, dir-marker skip),
   `get_object`/`get_object_uri`/`read`/`read_text`/`read_range`. 7 unit + 3 live
   MinIO e2e tests (`tests/amazon_s3_source.rs`: list/prefix/matcher/max-size/
   dir-marker/items/read/range/URI/nonexistent, empty bucket, `mount_each`
   pipeline). `S3File` implements the shared async `FileLike` while skipping its
   clone-cheap client/cache from serde, like Google Drive.
3. **OCI Object Storage source — ✅ done.** `oci_object_storage`
   (`feature = "oci_object_storage"`): no official Oracle Rust SDK, so it talks
   to the Object Storage REST API directly and implements OCI's RSA-SHA256 HTTP
   Signature signing (reusing the `google_drive` `rsa`/`sha2`/`base64`/`reqwest`
   crates). `OciClient::connect` reads an `~/.oci/config` profile
   (`OCI_CONFIG_FILE`/`OCI_PROFILE` overrides); `OciFilePath`/`OciFile`,
   `list_objects` → `OciWalker` (`list`/`items`, `fields=name,size,md5,...`,
   `start`/`nextStartWith` pagination, prefix + shared `PatternFilePathMatcher` +
   `max_file_size`, dir-marker skip), `get_object`/`get_object_uri`/`read`/
   `read_text`/`read_range`. `OciFile` implements the shared async `FileLike` and
   skips its client/cache from serde, like S3/Google Drive. 15 inline unit tests
   (INI/config parse, signing-string canonical form, RSA sign+verify round-trip,
   percent-encoding, URI parse, date parse, relative-key strip, memo keys,
   matcher). Not yet: live bucket events, pass-phrase-encrypted keys.
4. Keep source items consistently exposed as stable `(key, item)` pairs.

Tests to port next:

- Python LocalFS live add/edit/delete tests.
- File memo-state regression coverage is in `tests/pipeline.rs`; expand with S3
  and Google Drive live/mock cases if their metadata behavior changes.
- `python/tests/connectors/test_source_items.py` for OCI once implemented.

### 2. Kafka And Iggy Sources

- **Kafka source — ✅ done.** `kafka::KafkaConsumer` + `topic_as_map` /
  `topic_as_map_with_options` produce a `LiveMapView<String, Vec<u8>>` over a
  topic, fed to `Ctx::mount_each_live`. `scan()` reads the log to the
  high-watermark (compacted to the latest value per key, tombstones removed);
  `watch()` tails new records from there. Custom delete filtering via
  `KafkaSourceOptions::is_deletion` (defaults to tombstone). 2 live e2e tests
  (`tests/kafka_source.rs`: catch-up compaction, live tailing). Current scope:
  single partition (partition 0); multi-partition + a keyless `topic_as_stream`
  remain.
- **Iggy source — remaining.** Mirror the Kafka source shape on the live-
  components API once it settles; keep the target deletion semantics distinct
  (Kafka tombstones are native; Iggy requires `deletion_value_fn`).

Tests to port:

- `python/tests/connectors/test_kafka_source.py` (partial: compaction + tail
  covered; multi-partition offset/rebalance not yet)
- `python/tests/connectors/test_iggy_source.py`

### 3. Ops And Resource Facades — DONE

Python examples use first-class SDK resources and ops; Rust examples used
`fastembed`, `cocoindex_ops_text`, and custom HTTP clients directly. The SDK now
ships the facades below (heavy deps gated behind features so the default build is
unaffected):

1. `cocoindex::ops::text` (feature `text`): `detect_code_language`,
   `SeparatorSplitter`, `RecursiveSplitter`, and `CustomLanguageConfig`,
   returning `resources::chunk::Chunk` with ergonomic `Chunk::text(source)`
   access (the chunk keeps only the byte range, slicing the source on demand).
2. `cocoindex::resources::schema`: `VectorSchema`, `VectorSchemaProvider`,
   `MultiVectorSchema`, `MultiVectorSchemaProvider` (element type carried by the
   `VectorElementType` enum — `f32`/`f16` — in place of NumPy's `dtype`).
3. SDK embedders/transcribers, each implementing `VectorSchemaProvider` where
   applicable:
   - `cocoindex::ops::sentence_transformers::SentenceTransformerEmbedder`
     (feature `fastembed`) — Rust-native equivalent of Python's
     `SentenceTransformerEmbedder`, backed by `fastembed`/ONNX.
   - `cocoindex::ops::api::{ApiEmbedder, ApiTranscriber}` (feature `embed_api`)
     — Rust-native equivalent of Python's `LiteLLMEmbedder`/`LiteLLMTranscriber`,
     talking to an OpenAI-compatible HTTP API via `reqwest` (there is no Rust
     `litellm` router; point them at any compatible base URL).
4. **All embedding/transcription examples now use the SDK facades** instead of
   hand-rolling `cocoindex_ops_text`/`fastembed`/HTTP: `text-embedding`,
   `text-embedding-{lancedb,qdrant,turbopuffer}`, `code-embedding`,
   `code-embedding-lancedb`, `pdf-embedding`, `gdrive-text-embedding`,
   `amazon-s3-embedding`, `postgres-source`, and `paper-metadata` use
   `ops::text::{RecursiveSplitter, detect_code_language}` +
   `ops::sentence_transformers::SentenceTransformerEmbedder`; `audio-to-text`
   uses `ops::api::ApiTranscriber`. All build; e2e-revalidated:
   `text-embedding`, `text-embedding-lancedb`, `code-embedding`, `pdf-embedding`,
   `paper-metadata`, and `audio-to-text`. **Exception:** `conversation-to-knowledge`
   keeps its hand-rolled clients — its default embedder is
   `Snowflake/snowflake-arctic-embed-xs` (not in fastembed's built-in registry, so
   it loads via hf-hub `UserDefinedEmbeddingModel`), and it uses a chat-completion
   LLM + AssemblyAI transcription, neither of which the current embedder/transcriber
   facades cover.

Tests ported (Rust integration tests under `rust/sdk/cocoindex/tests/`):

- `python/tests/ops/test_text.py` → `tests/ops_text.rs`
- `python/tests/ops/test_embedder_refactor.py` → `tests/ops_api.rs`
- `python/tests/ops/test_litellm_transcriber.py` → `tests/ops_api.rs`
  (mock HTTP server via wiremock)

## P2: Connector Feature Gaps

These are narrower gaps in connectors Rust already has.

| Connector | Rust status | Action |
| --- | --- | --- |
| Neo4j / FalkorDB | Table/relation targets, PK artifacts, **and node-table vector-index attachments** (`TableTarget::declare_vector_index(field, dimension, VectorMetric)`, per-dialect `CREATE/DROP VECTOR INDEX` via the `CypherExecutor` trait; `VectorMetric` = cosine/euclidean/inner-product, the last FalkorDB-only). Covered by cypher_graph unit tests (exact per-dialect Cypher) + `tests/graph_vector_index.rs` live e2e (create→drop on real Neo4j + FalkorDB). | Remaining: exported node/relationship **index + uniqueness-constraint** builder helpers (`build_*_index_*`, `build_constraint_*`). |
| Qdrant | Rust supports one unnamed vector per collection. Python supports `QdrantVectorDef`, named vectors, and multivectors. | Add vector schema resources first, then named and multivector collection schemas. |
| Turbopuffer | Rust supports one unnamed `f32` vector. Python supports `VectorDef`, named vectors, and `f16`/`f32`. | Add vector schema resources first, then named vectors and `f16`. |
| Google Drive | Rust lists files, reads/exports content, exposes `DriveFilePath`, `DriveFileInfo`, top-level `list_files(spec)`, `GoogleDriveSource::items()`, and shared async `FileLike`. It still lacks live notifications. | Add live notifications only after the live source API is generalized. |
| LocalFS | Rust has static walk, shared path matchers, async cached `FileLike`, `DirWalker.items()`, and directory target. Python also has live watching. | Implement live `walk_dir(..., live=true)`. |
| Postgres | Target/source basics, vector index, SQL command attachment, and repeatable-read source snapshot exist. | Add edge-case tests for NUL strings, halfvec, column-drop/retype retries, and broader source iterator behavior if a streaming source API lands. |
| SQLite | Regular table, `managed_by`, vec0 DDL, validation, embedded e2e, and example exist. | Add live vec0 e2e when `sqlite-vec` is available in CI. |
| LanceDB | Table target and hermetic e2e exist. | Add tests for optimize/retry/error paths if Rust exposes those features. |

## P3: Missing Connectors

| Python connector | Why it matters | Next step |
| --- | --- | --- |
| Doris | Native Python target supports stream-load, retry config, vector indexes, and inverted indexes. | Implement after vector-schema resources; port `test_doris_target.py` with live Doris gating. |
| OCI Object Storage | Python source uses shared file resources and live object events. | **✅ Source done** (`oci_object_storage`, native REST + signing — see P1 §1). Remaining: live object events after the live source API is generalized. |
| Notion | Python connector is currently empty. | No Rust work until Python ships a real connector. |

## P4: Product Decisions

These are not connector blockers. Decide whether Rust should match Python or
document them as intentional non-goals.

| Python surface | Rust status | Decision needed |
| --- | --- | --- |
| CLI and user app loader | Rust examples run via `cargo run`; no SDK CLI. | Build a Rust CLI only if Rust apps are meant to be first-class deployed CocoIndex apps. |
| Default environment and lifespan | Rust uses explicit `App::builder`; no `default_env`, `EnvironmentBuilder`, or lifespan hooks. | Decide whether implicit global environments fit Rust style. |
| Settings/env loader | Rust has builder knobs, not Python `Settings`/`LmdbSettings` env loading. | Add only if CLI/default-env work is accepted. |
| Inspect/stable-path API | Core machinery exists; no public Rust wrapper. | Add lightweight wrappers if Rust users need state introspection. |
| Runner/GPU/subprocess runner | Rust has memo/batch, not Python runner abstractions. | Likely defer unless ops wrappers need it. |
| Python dynamic typing internals | Rust uses serde and static types. | Treat pickle/safe-unpickle/type-checker APIs as non-goals; test Rust replacement invariants instead. |

## Test Backlog

Use this order. It moves from shared semantics to connector-specific behavior.

1. Remaining file-source tests:
   LocalFS live tests and `test_source_items.py` for S3/OCI once implemented.
2. Kafka/Iggy source tests:
   `test_kafka_source.py`, `test_iggy_source.py`.
3. Ops/resource tests:
   `test_text.py`, `test_embedder_refactor.py`, `test_litellm_transcriber.py`,
   `test_llm_pair_resolver.py`.
4. Graph feature tests:
   vector-index lifecycle from Neo4j/FalkorDB target tests.
5. Qdrant/Turbopuffer named-vector and multivector tests.
6. Doris connector tests once its Rust connector exists; S3/OCI live e2e tests
   gated on real credentials (S3 has MinIO e2e; OCI has inline unit tests).
7. Product-decision tests only after deciding to build CLI/default-env/settings.

## Example Parity

Rust examples currently cover most non-source-blocked Python examples:

- text/code embedding across Postgres, LanceDB, Qdrant, and Turbopuffer
- Postgres source
- HN trending topics
- Google Drive text embedding
- meeting-notes graph examples for Neo4j and FalkorDB
- conversation-to-knowledge
- CSV to Kafka and Iggy
- files-transform
- files-to-SQLite
- multi-codebase summarization
- audio-to-text
- paper metadata
- PDF embedding and PDF-to-Markdown
- Amazon S3 embedding (`amazon-s3-embedding`, validated against MinIO + Postgres)
- OCI Object Storage embedding (`oci-object-storage-embedding`; one-shot, builds
  against the native `oci_object_storage` source — live OCI run needs real
  `~/.oci/config` credentials)

Examples still blocked or intentionally deferred:

| Python example | Status |
| --- | --- |
| `kafka_to_lancedb` | blocked on Kafka source |
| `entire_session_search` | intentionally de-scoped for Rust; overlaps code embedding and needs live LocalFS/two-table source shape |
| `patient_intake_extraction_baml`, `patient_intake_extraction_dspy` | BAML/DSPy integration, outside current Rust SDK parity scope |
| `notion_target_basics` | Python connector is empty; no Rust parity target yet |

## Definition Of Parity

Rust SDK parity means:

1. Every non-empty Python connector has a Rust equivalent or a documented
   intentional non-goal.
2. Every implemented Rust connector follows the same declarative target/source
   semantics as Python.
3. Shared file, vector, text, and embedder resources exist so examples do not
   hand-roll common SDK behavior.
4. Live source semantics exist for connectors that need them.
5. Each Python connector test family has a Rust family, with live tests gated
   only by explicit external-service requirements.
