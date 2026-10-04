# Plan: ergonomic Rust SDK — full workstream spec

Tracking issue: [#2273](https://github.com/cocoindex-io/cocoindex/issues/2273) (plus maintainer
follow-ups in the thread). Goal: a Rust user should write pipelines that read line-for-line
like the Python SDK, without hand-wiring engine machinery (hash constants, `LazyLock` statics,
stringly-typed schemas, custom `main` plumbing).

Each workstream below is independently implementable by an agent. Dependencies and suggested
PR order are at the end. The CLI workstream has its own spec: [rust-cli.md](rust-cli.md).

Reference points used throughout:
- Macro crate: `rust/sdk/cocoindex_macros/src/lib.rs` (existing `#[function]`, `use_mount!`,
  `mount_each!`, `#[derive(SchemaFields)]`).
- SDK crate: `rust/sdk/cocoindex/`.
- Python parity targets: `python/cocoindex/` and `examples/text_embedding/main.py`.

---

## WS1 — `#[cocoindex::function(batching)]` and `(memo, batching)`

**Problem.** Python: `@coco.fn(batching=True)` and done. Rust today: write a ctx-free batch fn,
then hand-wire `static EMBED: LazyLock<Batched<String, Vec<f32>>> =
LazyLock::new(|| Batched::new(embed_batch, __COCO_FN_HASH_EMBED_BATCH))` and call
`EMBED.call(&ctx, item)` (`rust/sdk/cocoindex/src/batched.rs`). `SHOWCASE.md` documents the
macro form but it was never implemented (the parser accepts only `memo`, `memo_key`, `version`,
`logic_tracking`).

**Design.** Match Python's contract exactly: the function is *declared* batch-shaped, *called*
item-shaped.

```rust
#[cocoindex::function(memo, batching, max_batch_size = 32)]
async fn embed(ctx: &Ctx, texts: Vec<String>) -> Result<Vec<Vec<f32>>> {
    ctx.get_key(&EMBEDDER)?.embed_batch(texts).await   // body sees only the cache misses
}
// call site:
let v: Vec<f32> = embed(&ctx, text).await?;
```

The macro emits: the hash const + logic registration (as today), a hidden module-level
`Batched` static wired with the hash const, a private batch-impl fn holding the user's body,
and a public wrapper with the rewritten signature — first non-ctx param `Vec<T>` → `T`, return
`Result<Vec<U>>` → `Result<U>`. Extra params beyond the items collection are cloned into the
closure and folded into each item's memo key (parity with `Batched` + Python).
`(batching)` without `memo` uses a pass-through (no per-item cache probe; body gets all items)
— mirror Python's `batching=True` without memo. `max_batch_size = N` maps to
`Batched::with_max_batch`.

Compile errors (via `syn::Error`): `batching` with no non-ctx param; first non-ctx param not
`Vec<_>`; return type not `Result<Vec<_>>`; `max_batch_size` without `batching`.

**Acceptance.** The `SHOWCASE.md` batching examples compile as written (after WS7 fixes the
rest of that file). Test in `rust/sdk/cocoindex/tests/pipeline.rs`: per-item cache hits skip
the body, misses batch, body-edit (hash change) invalidates, extra-param change invalidates.

---

## WS2 — `context_key!` declarative macro

**Problem.** Python: `PG_DB = coco.ContextKey[asyncpg.Pool]("db")`. Rust: a 5-line
`static LazyLock<ContextKey<T>>` ritual (see `examples/rust/text_embedding/src/main.rs:33-42`).

**Design.** Plain `macro_rules!` in the SDK crate (no proc macro). The Rust identifier is the
context key's default stable name; `key = "..."` is available when the persisted identity must
differ from the identifier or survive a rename. Change detection is disabled by default and uses
the value type's `MemoInput` behavior when enabled:

```rust
cocoindex::context_key!(static CONFIG: AppConfig);
cocoindex::context_key!(static EMBEDDER: SentenceTransformerEmbedder, detect_change);
cocoindex::context_key!(static DB: postgres::Database, key = "text_embedding_db");
```

`MemoInput` is structural: the derive visits fields, standard containers recurse into their
children, and nested resource state validation is preserved. UUID is always available; standalone
`chrono`, `serde_json`, and `rust_decimal` features cover those ecosystem leaves, and connectors
enable the corresponding leaf features automatically. Unknown foreign leaves require a local
newtype. Ordered containers write framed children into one fingerprinter; maps and sets sort by
child fingerprints. Nested states remain one opaque state per top-level argument/context slot, so
the engine storage schema does not change. `Vec<u8>` follows the generic sequence path; large
buffers should use `bytes::Bytes` for bulk hashing or a precomputed fingerprint.

When both optional arguments are present, `key =` appears before `detect_change`.

**Acceptance.** Expansion produces the typed `LazyLock` pattern; migrate the statics in
`examples/rust/text_embedding` and connector tests (`tests/sqlite_target.rs:16` etc.); the macro
documentation explains identifier-default naming, stable-name overrides, default-off change
detection, `MemoInput`, the foreign-type newtype pattern, and the argument order.

---

## WS3 — memo guidance fix (documentation, not a new macro)

**Problem.** `rust/sdk/cocoindex/tests/pipeline.rs` (~2691) calls hand-rolled
`ctx.memo(&(__COCO_FN_HASH_ANALYZE, input), ...)` "the realistic pattern" for memoizing calls
that use non-serializable resources. This is wrong on both counts: the memo body closure
receives an owned `Ctx` (`cached_by_fingerprint_with_state`, `memo.rs:117-126`), so
`ctx.get_or_err::<T>()`/`get_key` work inside `(memo)` bodies; and `memo_key(param = skip)`
params need only `Clone`, not `MemoInput` or `Serialize`,
`memo.rs:254`). Manual `ctx.memo` is also a foot-gun: its closure is not logic-tracked, so
forgetting the hash const silently serves stale results after a code edit.

**Work.**
1. Rewrite the misleading test comment; add a test demonstrating `(memo)` + `ctx.get_or_err`
   inside the body and `(memo, memo_key(client = skip))` with a non-`Serialize` `Clone` client.
2. Rustdoc on the `function` macro and `memo` module: the default is the attribute; manual
   `ctx.memo` is for block-level memoization only, and must fold `__COCO_FN_HASH_*` into the
   key by hand.
3. Do **not** implement a `memo!` block macro now; revisit only if block-level memoization
   demand shows up.

---

## WS4 — `SchemaFields` wiring into remaining connectors

**Problem.** `#[derive(SchemaFields)]` exists (`cocoindex_macros/src/lib.rs:960`), but
`TableSchema::from_row` is implemented only for Doris (`doris.rs:337`) and SQLite
(`sqlite.rs:160`). Postgres, LanceDB, Qdrant, and Turbopuffer still require hand-written
column strings, and zero `examples/rust/` projects use the derive.

**Work.**
1. Implement `TableSchema::from_row::<T: SchemaFields>(primary_key)` for `postgres`,
   `lancedb`, `qdrant`, `turbopuffer`, mapping `LogicalType` (`row_schema.rs:34`) to each
   connector's native types — mirror the per-connector `_LEAF_TYPE_MAPPINGS` in the Python
   connectors (`python/cocoindex/connectors/*/`). Follow the sqlite impl as the template.
2. Add a runtime vector-dim override so the dim need not be hardcoded in the attribute
   (Python parity: `Annotated[NDArray, EMBEDDER]` infers dim from the provided embedder):
   `schema.with_vector_dim("embedding", embedder.dim())` on each connector's `TableSchema`.
3. Convert `examples/rust/*` that declare table schemas to the derive.

**Acceptance.** `examples/rust/text_embedding` has no `ColumnDef::new("...")` strings left;
`tests/schema_from_row.rs` gains per-connector cases; unknown-`LogicalType` errors name the
field and connector.

---

## WS5 — module regroup (mirror Python package layout)

**Problem.** Connectors (`postgres`, `qdrant`, `lancedb`, `kafka`, …), resources (`file`,
`fs`, `id`), and machinery (`batched`, `memo`, `mount`, `statediff`) all sit at the crate root
(`rust/sdk/cocoindex/src/lib.rs:1-51`). Python groups them: `connectors/`, `resources/`,
`ops/`.

**Work.** Move to `connectors::{postgres, sqlite, qdrant, lancedb, turbopuffer, doris,
surrealdb, kafka, iggy, valkey, neo4j, falkordb, amazon_s3, gdrive, oci_object_storage}`,
`resources::{file, fs, id}`; keep `ops::*` as-is; keep engine machinery (`app`, `ctx`,
`memo`, `mount`, `batched`, `logic`, `statediff`, `target_state`, `live_component`) at root.
Feature gates unchanged. Update `prelude`, all tests, all `examples/rust/*`, and
`dev/agent-skills/target-connector/SKILL.md`. No deprecated re-exports — the crate is
unreleased (workspace version is a placeholder), this is the moment for the breaking change.

**Do this workstream first**: every other WS touches paths it moves.

---

## WS6 — `ops::sentence_transformers` adopts engine batching + memoization

**Problem.** Python's `SentenceTransformerEmbedder` uses
`@coco.fn.as_async(batching=True, runner=coco.GPU, max_batch_size=64)` and
`@coco.fn(memo=True, ...)` internally (`python/cocoindex/ops/sentence_transformers.py:122,167,205`)
— callers get batching and caching for free. The Rust op (`ops/sentence_transformers.rs`) is a
plain method.

**Work (depends on WS1).** Restructure the op so single-text embedding goes through a
`#[cocoindex::function(memo, batching, max_batch_size = 64)]` entry; keep the raw
`embed_batch` public for direct use. GPU-pool runners are Python-only for now — out of scope.

**Acceptance.** In an update run, repeated/unchanged texts hit cache; distinct texts group
into batches (assert via a counting fake embedder in tests).

---

## WS7 — docs truth pass

**Problem.** `rust/sdk/SHOWCASE.md` documents APIs that do not exist (`ctx.write_file`,
`ctx.batch`, sync `App::open`); the docs site has zero Rust SDK pages; the accurate reference
today is `tests/pipeline.rs`.

**Work.**
1. Fix `SHOWCASE.md` to match the implemented API (after WS1 the batching sections become
   true; fix `App::open` async-ness, replace `ctx.write_file` with `DirTarget`).
2. Add one docs-site page: Rust quickstart mirroring the Python `text_embedding` walkthrough
   (docs live under `docs/src/content/docs/`; follow existing page conventions).
3. Document `memo_key(...)` (WS3), `context_key!` (WS2), and the mount spellings guidance:
   `use_mount!`/`mount_each!` are the default; `ctx.scope`/`ctx.mount_each` are the
   explicit-key/no-fingerprint variants.

**Run last** — docs describe what has landed.

---

## WS8 — unified Rust CLI

Own spec: [rust-cli.md](rust-cli.md). Phase 1 = Rust CLI for Rust projects, `#[app]` /
`#[lifespan]` / `#[main]` macros, stdio protocol, `Environment::from_registered()`. Phase 2 =
replace the Python click CLI (binary shipped in wheels, `python -m cocoindex` protocol child).

---

## Dependencies and suggested PR order

```
WS5 (module regroup)  ──►  everything else (paths)
WS1 (batching macro)  ──►  WS6 (ops adoption)
WS2, WS3, WS4         ──►  independent of each other; after WS5
WS8 (CLI)             ──►  independent (new crate + macros); after WS5 to avoid path churn
WS7 (docs)            ──►  last
```

1. PR 1: WS5 (mechanical, review-once)
2. PR 2: WS1  → PR 3: WS6
3. PR 4: WS2 + WS3 (small, can share a PR)
4. PR 5: WS4
5. PR 6+: WS8 phase 1 (SDK macros + dispatch, then CLI crate — see rust-cli.md milestones)
6. PR final: WS7

Every PR: `cargo test` + `cargo clippy` clean; when Python files are touched (WS7 docs only),
`uv run mypy && uv run pytest python/` per AGENTS.md.
