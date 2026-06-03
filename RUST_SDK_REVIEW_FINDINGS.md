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

## ⏭ Real but deferred (need a design decision or are feature-sized)

- **qdrant/turbopuffer fingerprint `.expect()`** — serialization of `(vec, json map)` is
  effectively infallible; low value to churn two volatile files. Best folded into a shared
  `reconcile_by_fingerprint` helper (see refactors).
- **`auto_refresh` swallows post-ready errors** (`ctx.rs`); **`get_key` skips the
  context-change dep when called outside a memo/scope** (`ctx.rs`) — both real, need an
  exception/runtime decision.
- **entity-resolution `on_resolution`** — the `ResolutionEvent`/`deliver_events` machinery
  is genuinely dead (`let _events = …`); fix is an API addition *or* a deletion — maintainer's call.

## 🔭 P1 parity gaps (feature-sized; recommend dedicated PRs)

1. **File memo-state** (mtime fast-path + content-fingerprint fallback) — biggest gap: any
   mtime/LastModified bump reprocesses even with identical content; the correct stable keys
   (`FilePath::memo_key`, `S3FilePath::memo_key`) exist but are **dead** (not used as the key).
2. **General `Embedder` trait** + `get_vector_schema`/`get_multi_vector_schema` resolvers.
3. **Exception-handler chaining** + richer `ExceptionContext`; **`preview` mode** + root
   `report_to_stdout` (`UpdateOptions` discards `_preview_collector`).
4. **`LlmPairResolver`** + concurrent (per-component) entity resolution (currently sequential, O(n²) partition).
5. **Cypher value parameter binding** (neo4j/falkordb inline-escape values incl. big vectors).
6. **Vector breadth**: named vectors / f16 (qdrant, turbopuffer), UUID point ids (qdrant);
   LanceDB `optimize()` + f16/narrow types; neo4j/falkordb vector index + compound PK.
7. **Kafka multi-partition** + keyless `topic_as_stream`; **Iggy source**.
8. **`FilePath` method surface** (`parts`, `parent`, `suffixes`, `with_*`, …).
9. **`decode_bytes` UTF-32** mis-detected as UTF-16 (`encoding_rs` has no UTF-32 decoder; rare).

## 🧹 P2 dead code

- Dead "stable memo key" methods unless wired into memo-state (P1.1).
- `FileEntry` duplicate **sync** API (`content`/`content_str`, uncached) parallel to async
  `FileLike`; `FileEntry::fingerprint()` (3rd scheme, no callers); `FileEntry::key()`
  (relative) disagrees with `FileSourceItem::key()` (full posix).
- `profile.rs` blanket `#![allow(dead_code)]`; SurrealDB unused `pub index_names`/`field_names`;
  `lib.rs` vs `prelude.rs` export drift.

## 🔁 P3 refactors (high value, large)

- **neo4j.rs ↔ falkordb.rs ~75% copy-paste** (~250 lines/file) → make `cypher_graph`
  target generic or a `graph_connector!` macro.
- **Relational SQL helpers duplicated 3×** (postgres/sqlite/lancedb): `pk_stable_key`,
  `row_state`, `quote_ident`, `quote_string`, `validate_ident`, `vector_literal`, and the
  no-change reconcile body → shared `sql_target` util (would also kill the
  qdrant/turbopuffer `.expect()` divergence).
- **kafka.rs ↔ iggy.rs** stream-target scaffolding (~150 lines) → parameterize by sink +
  delete-behavior. 3 copies of `validate_ident`; `declare_X == mount_X` aliases.

## API consistency nits

- `lancedb::mount_table_target` is **sync** while other mounts are async.
- kafka/iggy: the composable `*_topic_target` path registers a **different root-provider
  key** than `mount_*` (`topic_spec/` vs `topic/`) — mixing the two for one topic is a trap.
- `AppBuilder::provide` panics on duplicate while most paths return `Result` (Python replaces).
