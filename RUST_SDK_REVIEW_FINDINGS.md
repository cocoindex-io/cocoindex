# Rust SDK — Ground-Up Review: Findings & Fix Status

Reviewed `rust/sdk/cocoindex/src` (≈18.8k LOC, 36 modules) vs `python/cocoindex`.
`file:line` are at review time (worktree is changing — re-grep before acting).

## ✅ Fixed + tested this pass

All hermetic unit tests pass; live Postgres (7) and SQLite (8) integration suites green;
`--all-features` compiles clean.

| Fix | What | Tests |
| --- | --- | --- |
| **Postgres vector-index `WITH` gating** | `vector_index_with_clause` gates `lists`→ivfflat, `m`/`ef_construction`→hnsw (emitting the wrong param made invalid DDL). | 4 unit (`postgres::review_fix_tests`) |
| **Postgres jsonb NUL** | `sanitize_json_nul` recursively strips NUL from nested strings/keys before serialize (Postgres rejects ` ` in jsonb). | 2 unit |
| **SQLite vec0 silent data loss** | `upsert_sql` → `Vec<String>`; `apply_rows` executes each. vec0 (no UPSERT) now runs DELETE + INSERT as **two** statements — a single `sqlx::query` ran only the first, dropping the INSERT. | 1 new + existing updated; live sqlite suite |
| **`RunStats` doc** | Clarified `processed` is the **total** (`= written + skipped + deleted`); numbers were already consistent, the doc implied they were disjoint. Behavior unchanged. | — |

## ❌ Verified false positives (reviewing agent was wrong — no change)

- **LanceDB `recreate`**: actually computed from `diff_composite` (Replace / incompatible
  column change) and the recreate path sets `child_invalidation: Destructive`, so rows
  are cleaned up. Works correctly.
- **entity-resolution `chain_walk` cycle fallback**: returning the lexicographically
  smallest visited name is a deterministic, stable cycle guard — not a bug.

## ↺ Already closed by ongoing work (subagent findings stale)

Postgres **incremental column evolution**, **column-drop retry**, and **text NUL
stripping** now exist (live tests `postgres_adds_and_drops_columns_*`,
`postgres_column_drop_retries_*`, `postgres_strips_nul_from_text_*`).

## ✅ Previously deferred semantics — now fixed

- ✅ **entity-resolution `on_resolution`** — resolved: added `resolve_entities_with_events`
  with an `on_resolution` callback (one event per entity, canonical delivery order); the
  `ResolutionEvent`/`deliver_events` machinery is now live and tested.
- **qdrant/turbopuffer fingerprint `.expect()`** — *intentionally kept.* Serialization of
  `(vec, json map)` is infallible; the `.expect("fingerprint …")` documents that invariant
  (same class as the adjacent `.expect("non-empty")`). Right home is a shared
  `reconcile_by_fingerprint` helper (see refactors), not a standalone churn of two files.
- ✅ **`auto_refresh` exception routing** — fixed: each cycle now passes the
  inherited handler chain into `update_full`; a swallowed post-ready failure is
  reported and the loop continues. Regression test:
  `auto_refresh_cycle_failure_uses_inherited_handler_and_continues`.
- ✅ **`get_key` outside memo/scope** — fixed: tracked context keys read at the
  component body level now record a component-level dependency when no
  function-call context is active. Regression test:
  `detect_change_context_key_read_outside_memo_invalidates_component`.

## 🔭 P1 parity gaps

All implemented + tested this pass (see per-item status):

1. ✅ **File memo-state** (mtime fast-path + content-fingerprint fallback) — verified already
   wired: the `#[function(memo)]` macro fingerprints `file_path().memo_key()` as the key while
   mtime+content live in the memo *state* via `cached_by_fingerprint_with_state` /
   `file_memo_state` (`memo.rs`). 2 tests pass.
2. ✅ **General `Embedder` trait** (`resources::embedder::Embedder`, `#[async_trait]`) with a
   default `embed_batch` fan-out and a blanket `impl<E: Embedder> EntityEmbedder for E`;
   implemented for `SentenceTransformerEmbedder` + `ApiEmbedder`. 2 tests + impls compile.
3. ✅ **Exception-handler chaining** (`Ctx.handler_chain`, nearest-first, swallow/re-raise) +
   richer `ExceptionContext` (env_name, stable_path, parent_stable_path, processor_name,
   mount_kind, is_background) using Rust's stable `MountKind` variants
   (`UpdateFull`, `Update`, `Delete`).
   ✅ **`preview` mode** (`UpdateOptions.preview`, `App::preview` → `Vec<PreviewAction>`) +
   `UpdateOptions.report_to_stdout` (wired to core `show_progress`). 8 + 2 tests pass.
4. ✅ **`LlmPairResolver`** + `ApiChatClient` (`ops::api`, OpenAI-compatible `/chat/completions`,
   retry-on-invalid-candidate) + concurrent per-component entity resolution (`try_join_all`) +
   `resolve_entities_with_events` `on_resolution` callback. 16 tests pass.
5. ✅ **Cypher value parameter binding** — `apply_record` now emits `$param` placeholders +
   a params map via `CypherExecutor::execute_with_params`; neo4j binds Bolt params
   (`json_to_bolt`), falkordb uses the `CYPHER name=value` header. 20 unit tests + live
   neo4j/falkordb record-write tests pass.
6. ✅ **Vector breadth**: named vectors already present (qdrant `CollectionSchema::named`,
   turbopuffer `NamespaceSchema::named`); added **f16** datatype mapping for qdrant
   (`Datatype::Float16`) — turbopuffer `[N]f16` already supported. `VectorSchema::f16`
   constructor added. 14 qdrant tests + turbopuffer `write_schema_named_and_f16`.
   (LanceDB `optimize()` / compound-PK breadth remain out of scope — separate items.)
7. ✅ **Kafka source** (keyed map across all partitions, plus keyless
   `topic_as_stream` payloads across all partitions) + **Iggy source** (keyed
   map, plus single-partition keyless `topic_as_stream` payloads). Both have
   live integration tests.
8. ✅ **`FilePath` method surface** (`parts`, `parent`, `suffixes`, `with_name`, `with_stem`,
   `with_suffix`, `relative_to`) — preserve base_key/base_dir. Tests pass.
9. ✅ **`decode_bytes` UTF-32** — BOM checked before UTF-16; manual `decode_utf32` (encoding_rs
   has no UTF-32 decoder). Test pass.

## 🧹 P2 dead code — cleaned up this pass

- ✅ **`FileEntry::fingerprint()`** deleted — zero callers, a redundant 3rd fingerprint
  scheme (connectors fingerprint via `FileLike`/content; the path+size+mtime variant had no
  users).
- ✅ **`profile.rs` blanket `#![allow(dead_code)]`** removed — every type is now reachable
  (preview wires up `Action`); the crate builds warning-free across all 18 features.
- ✅ **`lib.rs` ↔ `prelude.rs` drift** closed — the prelude now re-exports the always-available
  public surface lib.rs added (`Embedder`, `PreviewAction`/`PreviewValue`, the
  `entity_resolution` and `live_component` types), so `use cocoindex::prelude::*` matches.
- *Kept, with reason:* `FileEntry::{content,content_str,key}` are used across examples/tests
  (not dead); `FileEntry::key()` (relative) vs `FileSourceItem::key()` (full posix) serve
  different layers (component path vs source identity) — not a bug. SurrealDB
  `index_names`/`field_names` are used by the integration tests (table-introspection helpers).

## 🔁 P3 refactors (high value, large — deferred)

These consolidate **working, tested** connectors; doing them in a cleanup sweep risks
regressions, so they remain dedicated-PR work with their own test deltas:

- **neo4j.rs ↔ falkordb.rs ~75% copy-paste** → generic `cypher_graph` target / a
  `graph_connector!` macro. (The record-write path is already shared via
  `CypherExecutor::execute_with_params`; the remaining dup is the public wrapper surface.)
- **Relational SQL helpers duplicated 3×** (postgres/sqlite/lancedb) → shared `sql_target`
  util (would also fold the qdrant/turbopuffer fingerprint `.expect()` into one helper).
- **kafka.rs ↔ iggy.rs** stream-target scaffolding → parameterize by sink + delete-behavior.

## API consistency nits

- ✅ `lancedb::mount_table_target` — already **async** (stale finding; no longer a nit).
- *Kept, with reason:* kafka/iggy expose **two** paths — the composable `*_topic_target`
  (a `TopicHandler` container whose child is the message handler: the generic
  target-state-conformant path) and the `mount_*` shortcut (registers the message handler as
  a root directly). They register different provider keys by design; the composable path is
  currently unused but is the architecturally-correct one, so it is kept (deleting it would
  drop the generic-pattern path and break public API). Documented so the two are not mixed
  for one topic.
- *Kept, with reason:* `AppBuilder::provide`/`provide_key` **panic** on a duplicate — this is
  fail-fast on a builder-construction programming error (not a runtime condition), is
  covered by a test, and is consistent across both `provide` methods.
