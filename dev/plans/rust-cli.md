# Plan: `cocoindex` CLI for Rust projects

Tracking issue: [#2279](https://github.com/cocoindex-io/cocoindex/issues/2279), part of
[#2273](https://github.com/cocoindex-io/cocoindex/issues/2273) (WS8 in
[rust-sdk-ergonomics.md](rust-sdk-ergonomics.md)). Goal: a Rust project gets the same
`cocoindex ls | show | update | drop | init` commands the Python SDK has, from one standalone
binary, without writing a `main` that parses arguments and drives the engine.

Reference points:
- Python CLI: `python/cocoindex/cli.py`, docs `docs/src/content/docs/cli.mdx`.
- Python lifespan + default environment: `python/cocoindex/_internal/environment.py`.
- SDK: `rust/sdk/cocoindex/src/{app.rs, registry.rs, cli/}`; macros in
  `rust/sdk/cocoindex_macros/src/lib.rs`.
- CLI crate: `rust/cli/` (binary `cocoindex`, package `cocoindex_cli`).
- Core inspection used without user code: `rust/core/src/inspect/db_inspect.rs`.

---

## Shape

A Rust pipeline is compiled into the user's binary; the CLI cannot import it the way the Python
CLI imports a module. So the CLI works like `cargo run`:

1. **Build** the user's crate (`cargo build --message-format=json-render-diagnostics`) and find
   the binary from the `compiler-artifact` records.
2. **Run** it as `<bin> __coco <verb> --protocol 1 [flags]`.
3. **Talk** to it over a versioned JSON-lines protocol on stdout; the user's own stdout and
   stderr pass through.

Two commands never need the user's code: `ls --db PATH` and `show --db PATH --app-name NAME`
open the LMDB state db directly through `cocoindex_core::inspect` (both SDKs share the core and
the LMDB format).

## Registration (SDK)

**Problem.** Python puts environment setup in `@coco.lifespan` and constructs `coco.App(...)` at
import time, so the CLI and the user's own code share one environment. Rust had neither: every
`main` built its `Environment` inline and the CLI could not find apps.

**Design.** Three attributes, registered at link time through `linkme` distributed slices (the
mechanism `#[cocoindex::function]` already uses for logic fingerprints):

```rust
#[cocoindex::lifespan]
async fn lifespan(builder: EnvironmentBuilder) -> Result<EnvironmentBuilder> {
    let db = postgres::Database::connect(&url).await?;
    let pool = db.pool().clone();
    Ok(builder
        .db_path("./.cocoindex_db")
        .provide_key(&DB, db)
        .on_shutdown(move || async move { pool.close().await }))
}

#[cocoindex::app(name = "TextEmbedding")]          // name defaults to the fn name
async fn app_main(ctx: Ctx) -> Result<()> { ... }   // Ctx by value; stays callable

#[cocoindex::main]
async fn main() -> Result<()> { ... }               // runs cli::try_dispatch() first
```

- The lifespan takes the builder **by value and returns it**: every `EnvironmentBuilder` setter
  consumes `self`, so a `&mut` signature would be unusable. Teardown is
  `EnvironmentBuilder::on_shutdown(hook)`; hooks run most-recent-first in
  `Environment::shutdown()`, before the state db closes (the code after `yield` in a Python
  lifespan).
- `Environment::from_registered()` folds a fresh builder through every registered lifespan, then
  requires a db path from the lifespan or `COCOINDEX_DB`, applies `COCOINDEX_LMDB_MAX_DBS` /
  `COCOINDEX_LMDB_MAP_SIZE` when the lifespan did not set them, and opens the db. The CLI's
  protocol child and a user's `main` call the same function.
- `#[cocoindex::app]` registers `AppEntry { module, name, run: fn(Ctx) -> BoxFuture<Result<()>> }`;
  the user's return value is dropped by a generated shim, which is why the entry has no type
  parameter. App names are LMDB namespaces: `check_unique_app_names()` rejects duplicates.
- `#[cocoindex::main]` expands to a sync `main` that builds a multi-thread tokio runtime, calls
  `cocoindex::cli::try_dispatch()` (handles `__coco ...` and returns the exit code) and otherwise
  runs the body. A hand-written `main` calls `try_dispatch` itself as its first statement.
- Order of lifespans across crates is link order; a binary normally has one.
- The SDK's `serde_json` dependency became unconditional (the protocol needs it); the former
  `serde_json` feature is gone.

**Acceptance.** `tests/registered.rs`: a lifespan provides a value, two shutdown hooks run LIFO,
`find_app` resolves both a named and a default-named app, and the protocol child answers `ls`,
`update --preview`, `update --full-reprocess` and `drop` in-process. `tests/registered_nodb.rs`:
no lifespan and no `COCOINDEX_DB` is a clear error.

## Protocol

**Framing.** One record per line on the child's stdout: ASCII record separator `U+001E`, then a
JSON object with an `event` tag, then `\n`. The child flushes after every record. Any stdout
text without the separator (or before it on the same line) is user output; the CLI forwards it
verbatim. Stderr is inherited. Lines whose JSON does not parse are reported as malformed and
otherwise ignored.

**Invocation.** `<bin> __coco <ls|update|drop> --protocol 1 [--app NAME] [--live]
[--full-reprocess] [--preview] [--reset] [--db-only]`. `--app` is required when the binary has
more than one app. `--preview` excludes `--reset` and `--live`.

| event | fields | sent by | meaning |
|---|---|---|---|
| `hello` | `protocol`, `sdk` | all verbs, first | handshake; the CLI exits 2 on an unknown `protocol` |
| `apps` | `apps: [{name, persisted, registered}]`, `db_path` | `ls` | registered ∪ persisted apps |
| `start` | `app`, `db_path` | `update`, `drop` | the verb begins |
| `progress` | `version`, `ready`, `by_component: {name: counters}` | `update` | one per engine change; counters are `num_execution_starts`, `num_unchanged`, `num_adds`, `num_deletes`, `num_reprocesses`, `num_errors` |
| `preview` | `kind: create\|update\|delete`, `value` (JSON) | `update --preview` | one planned target action |
| `done` | `stats?` (`processed`, `skipped`, `written`, `deleted`, `elapsed_secs`), `cancelled`, `dropped?` | terminal | success; `cancelled: true` after Ctrl-C; `dropped: false` when there was nothing to drop |
| `error` | `message` | terminal | failure |

**Exit codes** (child and CLI alike): `0` success (also a clean Ctrl-C cancel), `1` error
(including a failed build), `2` usage or protocol mismatch.

**Cancellation.** The child turns every SIGINT into `cocoindex_core::engine::runtime::cancel_all()`
(idempotent: the terminal and the CLI may both deliver one) and reports the cancelled run as
`done {cancelled: true}` rather than `error`. The CLI prints `Stopping...` on the first Ctrl-C,
forwards SIGINT to the child on Unix, and kills the child on the second.

**Versioning.** `protocol` is a single integer; a change that an old CLI or an old SDK cannot
understand bumps it. Additive fields are not a bump (unknown fields are ignored).

## CLI

**TARGET grammar.** `[path][:app_name]`, default path `.`. A directory is a crate, a `Cargo.toml`
is a manifest, any other existing file is an already-built executable (no build; used by tests
and scripts). Build options: `--release`, `--bin NAME` (required when the manifest builds
several binaries). A Windows drive letter before `:` is not an app separator.

**Global options.** `-e/--env-file PATH` (else `.env` found upward from the current directory;
existing variables are never overridden; the child inherits the environment), `-V/--version`.

| command | Python | Rust CLI |
|---|---|---|
| `ls [TARGET] [--db PATH]` | same | `--db` reads the db directly (exists-check first: core creates missing dirs); otherwise build + `__coco ls`. `[+]` marks apps without persisted state |
| `show [TARGET] [STABLE_PATH] [--db] [--app-name] [--tree] [-l] [-r] [-p] [--fingerprints] [--target-states]` | same options | `--db --app-name` reads the db directly; with `--db` the single positional is `STABLE_PATH` (Python cannot express that). With `TARGET`, `__coco ls` resolves the db path and app, then the db is read in-process |
| `update TARGET [-f] [-q] [--reset] [--full-reprocess] [-L/--live] [--preview]` | same | `--reset` prompts `Type 'yes' to reset app ...` unless `-f`; progress renders on stderr; `[Stats] ...` on stdout |
| `drop TARGET [-f] [-q] [--db-only]` | no `--db-only` | `--db-only` clears the app's db namespace without reverting target states (core `Storage::drop_app`) |
| `init [PROJECT_NAME] [--dir] [--sdk-path]` | no `--sdk-path` | scaffolds `Cargo.toml`, `src/main.rs` with the three attributes, `.env.example`, `README.md`, `.gitignore`; `--sdk-path` writes a path dependency (the crate is not on crates.io yet) |

Output wording follows `cli.py` so the two CLIs read the same.

**What `show` can name.** Reading the db without user code has no provider registry, so a
target-state segment that was never persisted with a readable name renders as `#hex`. Carrying
provider names over the protocol is phase 2.

## Known gap: `drop` does not revert Rust root targets yet

`cocoindex drop` calls `App::drop_state`, which clears the app's state db namespace. In the Rust
SDK it does not revert target states that hang off root providers registered during the run
(`mount_dir_target`, connector tables): the engine deletes a root target only through the
environment's provider registry (`rust/core/src/engine/execution.rs`, "skip deleting target
states ... provider not found"), and Rust root providers are dynamic (the name embeds the
directory or table) and live in the root component's build state. Python registers its root
providers at import time, so it does not hit this. This predates the CLI and affects in-process
`drop_state` too; `tests/registered.rs::drop_reverts_root_provider_targets` (ignored) pins the
intended behaviour. Fixing it needs a decision on root-provider lifetime in the Rust SDK; the
CLI needs no change once `drop_state` reverts targets.

## Known limit: `--preview` and child target providers

`cocoindex update --preview` passes the engine's preview mode through. The engine previews flat
(leaf) target actions only and rejects actions that need child target providers
(`rust/core/src/engine/execution.rs`, "preview currently supports flat/leaf target actions
only"), for example rows under a Postgres table target. The CLI reports that error (exit 1).
This is an engine limit shared with the Python SDK; the fixture app's `DirTarget` files preview
fine.

## Example

`examples/rust/text_embedding` is the validating example: `#[cocoindex::lifespan]` connects
Postgres and loads the embedder, `#[cocoindex::app(name = "TextEmbeddingRust")]` walks the
markdown files with the live walker (`fs_live`) so `cocoindex update -L` watches for changes,
and `#[cocoindex::main]` keeps `cargo run -- query "..."` unchanged.

## Milestones

Phase 1 lands as one pull request in three commits:

1. `feat(sdk)`: this spec, `registry.rs`, the three attributes, `Environment::from_registered`,
   `on_shutdown` / `shutdown`, `cli::try_dispatch` and the protocol.
2. `feat(cli)`: `rust/cli/` with `ls`, `show`, `update`, `drop`, `init`, a fixture app and an
   end-to-end test that drives the real binary; a small core change making the store-scoped
   inspect queries public.
3. `feat(examples)`: the `text_embedding` migration and README.

## Phase 2 (follow-up issue)

- Ship the `cocoindex` binary in the Python wheels (ruff-style) and replace the click front-end
  with a thin `python -m cocoindex __coco ...` protocol child, so one CLI serves both SDKs.
- Send provider names in `apps` so `show TARGET` renders readable target-state paths.
- A docs page for the Rust CLI (`generate-cli-docs` covers only the click CLI today).
- `cargo install cocoindex-cli` once the SDK is published.
