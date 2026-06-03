# Rust SDK — Ground-Up Review: Findings & Fix Status

Reviewed `rust/sdk/cocoindex/src` vs `python/cocoindex`.
`file:line` references are at review time (worktree changes — re-grep before acting).

## Status: all reviewed fixes and P1 gaps landed

Everything from this review pass is closed. For brevity the per-fix tables have
been removed now that they are done; the closed work was:

- **Postgres**: vector-index `WITH` gating (ivfflat `lists` vs hnsw
  `m`/`ef_construction`), recursive jsonb NUL sanitization, incremental column
  add/drop with drop-retry, text NUL stripping. Live tests green.
- **SQLite vec0**: DELETE+INSERT now run as two statements (a single `sqlx::query`
  silently dropped the INSERT). Live suite green.
- **P1 parity gaps (all ✅)**: file memo-state (mtime fast-path + content
  fingerprint), general `Embedder` trait, exception-handler chaining + richer
  `ExceptionContext`, `preview` mode, `LlmPairResolver` + `ApiChatClient`,
  Cypher value parameter binding, vector breadth (named + f16), Kafka & Iggy
  sources, `FilePath` method surface, `decode_bytes` UTF-32.
- **Dead code**: `FileEntry::fingerprint()` deleted, `profile.rs` blanket
  `allow(dead_code)` removed, `lib.rs`↔`prelude.rs` drift closed.
- **Refactors**: neo4j/falkordb wrappers unified via
  `cypher_graph::graph_target_api!`; triplicate `validate_ident` moved to a shared
  `sql_ident` module.

The remaining connector-level work lives in
[RUST_PYTHON_SDK_PARITY_REVIEW.md](RUST_PYTHON_SDK_PARITY_REVIEW.md), not here.

## Durable decisions — do NOT re-flag these

These were investigated and intentionally left as-is. Re-raising them is churn.

### Verified false positives

- **LanceDB `recreate`**: computed from `diff_composite` (Replace / incompatible
  column change); the recreate path sets `child_invalidation: Destructive`, so rows
  are cleaned up. Works correctly.
- **entity-resolution `chain_walk` cycle fallback**: returning the
  lexicographically smallest visited name is a deterministic, stable cycle guard —
  not a bug.

### Kept with reason

- **qdrant/turbopuffer fingerprint `.expect()`**: serialization of `(vec, json map)`
  is infallible; the `.expect(...)` documents that invariant. Not a bug.
- **`FileEntry::key()` (relative) vs `FileSourceItem::key()` (full posix)**: serve
  different layers (component path vs source identity). `FileEntry::{content,
  content_str,key}` are used across examples/tests — not dead. SurrealDB
  `index_names`/`field_names` are used by integration tests.
- **kafka.rs ↔ iggy.rs**: kept separate. Container/spec plumbing rhymes, but the
  message sinks diverge at the client layer (`rskafka` `PartitionClient` produce vs
  iggy `send_messages`/partitioning); a shared abstraction would be more indirection
  than payoff.
- **`pk_stable_key` / `row_state` in sqlite vs lancedb**: identical-looking but
  depend on `json_scalar_to_stable_key`, which **differs by design** (sqlite accepts
  bool / errs on null keys; lancedb is stricter). Forcing a merge would change
  semantics.
- ✅ **kafka/iggy two-path trap — fixed by deletion.** The composable `*_topic_target`
  path (a no-op `TopicHandler` *container* over a user-managed topic, plus
  `TopicSpec`/`TopicAction`) was unused (zero callers/tests) and registered a *different*
  provider key than `mount_*`, so mixing the two for one topic produced conflicting
  providers. Removed it (~190 lines); the single remaining path registers the message
  handler as a root provider — the right shape for a user-managed topic. Live kafka/iggy
  target + source tests still pass.
- **`AppBuilder::provide`/`provide_key` panic on duplicate**: fail-fast on a
  builder-construction programming error, covered by a test, consistent across both
  methods.
