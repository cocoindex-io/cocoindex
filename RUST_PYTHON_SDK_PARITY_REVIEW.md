# Rust SDK vs Python SDK Parity — Action List

Branch: `rust-sync` · Last reviewed: 2026-06-02 (verified against the worktree).

This is an **action-oriented** audit. Areas that are already at parity are
collapsed into the "Done" section below; everything else is an open item with a
concrete next step. Source of truth:

- Python: `python/cocoindex` (`_internal/api.py::__all__` is the 76-name top-level
  surface; connectors under `connectors/`, ops under `ops/`, resources under
  `resources/`, helpers under `connectorkits/`).
- Rust: `rust/sdk/cocoindex/src` (flat re-exports in `lib.rs`); tests under
  `rust/sdk/cocoindex/tests` + `src` unit tests; examples under `examples/rust`.

Scale marker (not a quality score): **Python 839 test fns** (284 connector, 374
core, 67 ops, 44 internal, 44 CLI, 26 resources) vs **Rust 203** (127 integration
+ 76 `src` unit). Most Rust connector integration tests are live/gated (skip when
the service/credential is absent).

---

## ✅ Done — at parity, no action needed

These are intentionally removed from the action list below.

- **Generic target-state public API** (`target_state.rs`): `TargetState`,
  `TargetStateProvider`, `TargetHandler` (+ `attachments()`), `TargetActionSink`
  (+ `from_async_fn_with_children`), `ChildTargetDef`, `declare_target_state`,
  `declare_target_state_with_child`, `register_root_target_states_provider`,
  foreground `mount_target`, provider-generation `memo_key`. Covered by
  `tests/target_state.rs` (CRUD/no-change, in-component declarations, mount-target
  children, attachments create/cleanup, destructive/lossy generation, ownership
  transfer).
- **All 10 implemented connectors are on the facade** with the full
  constructor/declaration/mount split (`*_target` / `declare_*_target` /
  `mount_*_target` [+ `_with_options`]): postgres, surrealdb, kafka, lancedb,
  qdrant, turbopuffer, neo4j, falkordb, fs(localfs), and the gdrive source. None
  remain on the private `profile` helpers.
- **`managed_by` / `ManagedTargetOptions`** on every target connector except Kafka
  (stream target) and fs (always system-managed).
- **`statediff` core**: `ManagedBy`, `MutualTrackingRecord`,
  `TrackingRecordTransition`, `DiffAction`, `resolve_system_transition`, `diff`.
- **Postgres**: table target + **vector-index attachment** + **SQL-command
  attachment** (`declare_sql_command_attachment`) + transactional row apply.
  Source reads (`read_table` / `read_table_items`) use a **REPEATABLE READ,
  READ ONLY snapshot**; `read_table_items` keys rows for `Ctx::mount_each`.
- **SurrealDB**: table + relation (`relation_target_many` /
  `relation_target_unconstrained`) + **vector-index attachment**
  (`declare_vector_index`).
- **Context keys** (`new` / `new_detect_change` / `new_with_state`), **id/uuid
  generators**, **entity-resolution core** (`resolve_entities`, policies,
  candidate matching), **app lifecycle** (builder/open/run/update/drop, blocking
  + async, stats groups, `max_inflight_components` quota), **`Ctx::auto_refresh`**.
- **Examples**: text/code embedding (pg + lancedb + qdrant + turbopuffer),
  postgres-source, hn-trending, gdrive, meeting-notes (neo4j/falkordb),
  conversation-to-knowledge, csv-to-kafka, files-transform,
  multi-codebase-summarization, audio-to-text, paper-metadata, pdf-embedding,
  pdf-to-markdown, code-embedding-lancedb — all build + e2e-validated.

---

## 🔴 P1 — Missing connectors & the abstractions that gate them

### Connectors with no Rust equivalent

| Python connector | Python public surface | Note |
| --- | --- | --- |
| `sqlite` | `ManagedConnection`, `Vec0TableDef`, `TableSchema`, `TableTarget`, `table_target`/`declare`/`mount`, vec0 | Best next table target — proves the facade generalizes beyond Postgres. 21 Python tests. |
| `doris` | `DorisConnectionConfig`, `RetryConfig`, `DorisTableTarget`, `VectorIndexDef`, `InvertedIndexDef`, stream-load | Needs stream-load + retry + vector/inverted index. 6 Python tests. |
| `amazon_s3` | `S3FilePath`, `S3File`, `S3Walker`, `get_object`, `read`, `list_objects` | **Blocked on shared file/object source abstraction** (below). 24 Python tests. |
| `oci_object_storage` | `OCIFilePath`, `OCIFile`, `OCIWalker`, live object events | **Blocked on file source + live source** (below). 23 Python tests. |
| `iggy` | `TopicStream`, `topic_as_stream`/`map`, `IggyTopicTarget` + split | Should mirror Kafka shape once Kafka source lands. 7 Python tests. |
| `notion` | (empty in Python too) | No-op until Python ships it. |

### Shared abstractions that block the above

- **Shared file/object source resource** — Rust has `fs::FileEntry` (sync) and a
  separate `gdrive::DriveFile`; Python has one `FileLike`/`FilePath`/`FileMetadata`/
  `items()` shape reused by localfs/S3/OCI/GDrive. **Action:** promote a shared Rust
  file-resource module (stable `FilePath`, lazy metadata + content fingerprint,
  `(key, item)` iteration, matchers) before adding S3/OCI.
- **Kafka source** — target is done; `TopicStream` / `topic_as_stream` /
  `topic_as_map` / payload filtering are missing (14 Python source tests, 0 Rust).
- **Live components SDK API** — core has the machinery; the SDK exposes only
  `Ctx::auto_refresh`. Missing `LiveComponent`/`LiveMapFeed`/`LiveMapView`/
  `LiveMapSubscriber`. **Blocks** live localfs (`walk_dir(live=True)`), Kafka/Iggy
  sources, and OCI live object watching.
- **Exception handlers** — no `exception_handler`/`ExceptionContext` equivalent;
  needed for background/live error routing.

---

## 🟠 P2 — Existing connectors missing features

| Connector | Missing vs Python | Action |
| --- | --- | --- |
| Neo4j / FalkorDB | vector index; explicit node/relationship **index + constraint** builders (Python exposes `build_*_index_*`, `build_constraint_*`, `vector_index_name`, …) | Add index/constraint/vector-index attachments on the shared `cypher_graph` backend. |
| Qdrant / Turbopuffer | named / **multivector** schemas; f16 (Turbopuffer) | Currently single unnamed vector only. Blocks `image_search_colpali`. |
| `statediff` | **composite** layer: `CompositeTrackingRecord`, `diff_composite` | Needed for column-level / attachment-level diffs; Python has it. |
| Google Drive | `DriveFilePath` (display path + id), `DriveFileInfo`, top-level `list_files(spec)`, async `items()`; live change notifications | Fold into the shared file-resource work above. |
| LocalFS | live watching; async lazy-cached `FileLike` (Rust `FileLike` is sync) | Pairs with the live-components API. |
| SurrealDB | `declare_row` alias (only `declare_record` today) | Minor ergonomic alias. |

---

## 🟠 P2 — Ops & resource parity (Rust SDK has none of these)

The Rust SDK crate has **no `ops` module** and no shared embedder/vector-schema
abstractions; examples hand-roll HTTP clients and use `fastembed`/`cocoindex_ops_text`
directly. Python exposes these as first-class SDK surface.

- **`cocoindex::ops::text` facade** — wrap `cocoindex_ops_text` and give chunks a
  `chunk.text(source)` helper instead of forcing every example to slice ranges.
- **General `Embedder` trait + `VectorSchema`/`MultiVectorSchema` providers** —
  Python's common contract for vector targets/embedders; Rust only has the
  narrower `entity_resolution::EntityEmbedder`.
- **`SentenceTransformerEmbedder`, `LiteLLMEmbedder`, `LiteLLMTranscriber`** —
  no SDK wrappers (audio-to-text/paper-metadata hand-roll the OpenAI calls).
- **Entity resolution**: no `on_resolution` callback (the `ResolutionEvent`s are
  computed then dropped) and no `LlmPairResolver`.

---

## 🟡 P3 — Product decisions (build, or document as an intentional non-goal)

These are language/runtime-shape differences, not connector blockers. Each needs
an explicit decision; if kept as a non-goal, say so in the docs.

- **CLI** (`cocoindex` ls/show/update/drop/init) + **user-app loader** — 44 Python
  CLI tests; Rust runs examples via `cargo run`.
- **Inspect / stable-path API** (`iter_stable_paths`, `list_stable_paths*`) — core
  has the machinery; no public SDK wrapper.
- **Default environment / lifespan** (`Environment`, `EnvironmentBuilder`,
  `default_env`, `start`/`stop`, `COCOINDEX_MAX_INFLIGHT_COMPONENTS` fallback) —
  Rust uses explicit `App::builder`.
- **`Settings` / `LmdbSettings` + env loader** — Rust has `AppBuilder` knobs only.
- **`Runner` / `GPU` / subprocess runner** — Rust has `Ctx::batch`/`memo::batch`
  but no runner abstraction.
- **Dynamic-typing APIs** (`datatype`, `type_checker`, `unpickle_safe`,
  `serialize_by_pickle`, `memo_fingerprint` registry/`NotMemoKeyable`,
  pending/sentinel markers) — likely Rust non-goals; instead add Rust tests for the
  replacement invariants (deterministic serde memo keys, connector-schema
  serialization, persisted-state backward compat).

---

## 📋 Test-coverage gaps (highest-value Python families to port)

Even where the Rust API exists, behavior coverage lags. Port these first because
they lock down SDK semantics rather than examples:

| Area | Python tests | Rust today | Priority |
| --- | ---: | --- | --- |
| Neo4j target | 45 | shared graph unit tests + example e2e | High (graph family) |
| SurrealDB target | 37 | 5 live (incl. vector index, schema-evolution, table-drop) | Medium |
| FalkorDB target | 31 | shared graph unit tests + example e2e | High |
| Amazon S3 source | 24 | none | After file-source abstraction |
| OCI source | 23 | none | After live source API |
| SQLite target | 21 | none | High (next connector) |
| Turbopuffer target | 19 | 1 live + unit | Medium (named-vector/f16 gaps) |
| Kafka source | 14 | none | High (no Rust source) |
| LanceDB target | 12 | 2 hermetic + unit | Medium (optimize/retry untested) |
| Postgres source | 9 | 2 live (snapshot + `read_table_items` covered) | Low (streaming/iterator gated on live source) |
| Postgres target | 8 | 4 live (rows, vector index, SQL-command attachment) | Medium (NUL/halfvec/column-drop-retry) |
| Kafka target | 17 | 2 live (incl. custom delete value) | Mostly covered |
| CLI / settings / inspect | 44 / 12 / — | none | Gated on the P3 decisions above |

Recommended order: **(1)** SQLite (proves the facade generalizes) → **(2)** Kafka
source + live-components API → **(3)** shared file source → S3/OCI/GDrive parity →
**(4)** graph index/constraint + vector targets named/multivector → **(5)** ops &
embedder wrappers.

---

## 🧩 Examples still not ported

| Python example | Blocker |
| --- | --- |
| `image_search` | image/file resource shape + CLIP embedder (Qdrant target exists). |
| `image_search_colpali` | Qdrant multivector + `MultiVectorSchema` + ColPali. |
| `amazon_s3_embedding` | S3 source. |
| `oci_object_storage_embedding` | OCI source + live API. |
| `kafka_to_lancedb` | Kafka source. |
| `entire_session_search` | overlaps `code-embedding`; live localfs + two-table shape (de-scoped). |
| `patient_intake_extraction_baml` / `_dspy` | BAML/DSPy — out of connector-parity scope. |

---

## Completion criteria for "Rust SDK at parity"

- Every Python connector has a Rust connector or a documented intentional non-goal.
- Every Python connector test family has a Rust family or a documented gated skip.
- Live source semantics exist for connectors that need them.
- Ops/resource contracts (text ops facade, embedder trait, vector schema) exist so
  examples stop hand-rolling them.
