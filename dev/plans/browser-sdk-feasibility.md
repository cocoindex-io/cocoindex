# Browser SDK feasibility: investigation for #482

Status: reproducible compile probe and design note, **not a browser SDK**.
No runtime behavior, dependency, storage format, or public API is changed.
This does not resolve #482 or establish a roadmap commitment.

## Reproduce the probe

From the repository root on a Unix development host with Python 3.9+, Cargo,
rustc, rustup, and this checkout's locked dependencies already cached:

```sh
python3 dev/probe_browser_sdk.py --out /tmp/cocoindex-browser-482-unique-run
```

Choose an output path that does not already exist and whose parent exists.
The script records the revision, exact compiler/Cargo versions, installed
targets, minimal SDK dependency tree, a standard-library canary, and an SDK
`cargo check`. Commands, exit codes, timings, output size, and free disk appear
in `summary.json`; each command has its own log. Cargo is offline and locked,
uses two jobs, and writes all build artifacts into the new output directory.
It does not install targets, change toolchains, edit Cargo.lock, create bindings,
or download models. Each command has a 120-second limit, at least 5GB free disk
is required, and the probe stops its own process group at 200MB output growth.
Resource polling is periodic, so these are stop thresholds, not allocation quotas.

Exit 0 means the SDK check and canary compiled; browser execution is still
untested. Exit 1 means SDK compilation or dependency inventory failed after the
canary succeeded. Exit 2
means the standard-library prerequisite failed (or the run was stopped before
that prerequisite completed). Inspect the logs before attributing an error to
CocoIndex. A failed dependency inventory is also recorded and must be resolved
before interpreting an apparently successful compile as a complete probe.

## Observed result, 2026-10-23

Probed revision: `6593232b559628bf52872736a210c19d606b1d18`.

| Check | Observed result |
| --- | --- |
| `rustc --version --verbose` | `rustc 1.93.0 (254b59607 2026-01-19)`, host `aarch64-apple-darwin`, LLVM `21.1.8` |
| `cargo --version` | `cargo 1.93.0 (083ac5135 2025-12-15)` |
| `rustup target list --installed` | Only `aarch64-apple-darwin` |
| `cargo tree --offline --locked -p cocoindex --no-default-features --target wasm32-unknown-unknown --edges normal` | Exit 0; includes core, Tokio, heed/LMDB, nix, axum, and reqwest |
| `rustc --edition=2024 --crate-type=lib --target wasm32-unknown-unknown <std_canary.rs> --out-dir <output>` | Exit 1, `E0463: can't find crate for std` |
| `cargo check --offline --locked -p cocoindex --no-default-features --lib --target wasm32-unknown-unknown` | Exit 101; `serde_core` fails with the same `E0463` |
| Probe script | Exit 2 on both runs; approximately 4.5MB output on the final run (31.5MB on the initial run); over 23GB disk free |

The initial runs above stopped at a missing standard-library prerequisite. They
neither compiled the SDK nor reached its downstream portability checks.

### Follow-up with the wasm target installed

A separate, task-owned Rust 1.95.0 toolchain was prepared with the
`wasm32-unknown-unknown` target, then the same probe was rerun against
`edd024f730f747808bbdda5e57ac5bb3a37ce35a` (the probe commit; SDK runtime source
is unchanged from the base above). This did not replace the user's global
Rust toolchain.

| Check | Follow-up result |
| --- | --- |
| Compiler | `rustc 1.95.0 (59807616e 2026-04-14)`, host `aarch64-apple-darwin` |
| Installed targets | `aarch64-apple-darwin` and `wasm32-unknown-unknown` |
| Offline locked SDK dependency inventory | Exit 0 |
| Standard-library canary | Exit 0; the target prerequisite is now established |
| SDK `cargo check` | Exit 101; the first error is Mio 1.2.2's explicit rejection of `wasm32-unknown-unknown` with networking enabled |
| Probe exit | 1, correctly distinguishing an SDK/dependency failure from prerequisite exit 2 |

The first compiler diagnostic is:

```text
This wasm target is unsupported by mio. If using Tokio, disable the net feature.
```

The compiler then reports missing platform networking symbols in Mio. This is
**actual downstream compilation evidence**, not just a source-based prediction.
It does not establish that simply disabling networking would make the SDK
portable: later native runtime/storage dependencies were not fully checked,
and no browser execution, JS binding, persistence, PGlite, or TransformersJS
integration was demonstrated. In particular, no LMDB compiler failure is
claimed from this run.

To reproduce with an appropriately prepared toolchain, use a fresh output
location and set `RUSTUP_TOOLCHAIN=1.95.0` when invoking the probe. Keep the
failed output and its exact dependency/compiler versions with the review.
The Rust target documentation also distinguishes compilation from host
capability: ordinary filesystem operations are unsupported and native thread
spawning cannot be used as on the host. See the
[rustc target documentation](https://doc.rust-lang.org/rustc/platform-support/wasm32-unknown-unknown.html).

## Source-grounded boundaries

These are architectural observations and migration risks. The Mio networking
blocker above was reached by compilation; the other entries are not claims of
compiler diagnostics reached by that run.

| Area | Current implementation | Implication for browser work |
| --- | --- | --- |
| Smallest SDK | [SDK manifest](../../rust/sdk/cocoindex/Cargo.toml) unconditionally depends on core and utils; text/matching/fastembed and database connectors are optional | `--no-default-features` does not remove the engine, LMDB, native filesystem facade, or runtime. A chunking-only package would need a separate entry point. |
| Runtime | [workspace Tokio configuration](../../Cargo.toml) enables `full`, filesystem, and the multithread runtime; [engine runtime](../../rust/core/src/engine/runtime.rs) creates `Runtime::new()` | A browser host executor needs a distinct scheduling/timer/cancellation strategy. Merely switching to a current-thread Tokio constructor does not supply browser storage, timers, or host callbacks. The locked [Tokio 1.53.1 source](https://github.com/tokio-rs/tokio/blob/tokio-1.53.1/tokio/src/lib.rs) explicitly restricts wasm feature combinations. Mio's networking rejection was reached by the follow-up compile; this specific Tokio compile guard was inspected, not reached. |
| Durable state | [Storage](../../rust/core/src/state_store/storage.rs) opens `heed::Env` on a filesystem path, uses map resizing and a blocking transaction runner; [transaction types](../../rust/core/src/state_store/txn.rs) directly wrap heed transactions | State storage is not an interchangeable key/value callback today. A replacement must preserve ordering, atomicity, retries, and interrupted-update recovery. Native LMDB assumptions cannot be removed by wrapping the SDK in JS exports. |
| Host boundary | [EngineProfile](../../rust/core/src/engine/profile.rs) abstracts host values/processors but requires `Send`/`Sync` in several associated types; [Python bridge](../../rust/py/src/runtime.rs) uses PyO3/task locals; [Rust profile](../../rust/sdk/cocoindex/src/profile.rs) is a separate sealed implementation | A JS profile/bridge needs an explicit ownership and callback model, persistence codecs, and async error/cancellation propagation. Python bindings are not JS bindings. Do not remove engine-wide thread bounds without a design and tests. |
| Files and networking | [SDK filesystem facade](../../rust/sdk/cocoindex/src/resources/fs.rs) uses `std::fs`, `tokio::fs`, and blocking tasks; [core manifest](../../rust/core/Cargo.toml) and [utils](../../rust/utils/Cargo.toml) include native-oriented dependencies | Browser `File`/`Blob`/fetch sources and permission/error semantics require host adapters or feature boundaries. A Node.js native addon is a different target from a browser wasm32 SDK. |
| Pure operations | [Text operation facade](../../rust/sdk/cocoindex/src/ops/text.rs), [ops_text](../../rust/ops_text/Cargo.toml), and [code_ast](../../rust/code_ast/Cargo.toml) already separate splitting/parsing from the engine | These are plausible candidates for an initial experiment. Grammar C builds, toolchain support, Unicode offsets, package size, and browser execution still need measurement. No wasm chunking build was demonstrated here. |

## Proposed migration sequence

1. **Isolate native networking at the build boundary.** The prepared-target
   probe now reaches Mio's unsupported-target error. Design a browser-specific
   dependency/feature boundary without removing networking from native users,
   then rerun to reveal the next actual blocker. Do not treat the first fix as
   proof that runtime or storage is portable. Add a compile job once it can run
   predictably and keep native SDK/core checks as regressions.
2. **Prove one small browser operation.** If useful independently, expose an
   operation-only entry point without importing core/storage. Test real browser
   loading, UTF-8 byte spans versus JS string offsets, cancellation/cooperative
   yielding for large inputs, and bounded memory. Label this as operations, not
   a workflow/checkpointing SDK.
3. **Specify checkpoint semantics before changing storage.** Model the current
   component/memo/target tracking transactions and failure isolation. Prototype
   a browser persistence backend and executor behind that contract. An
   IndexedDB/OPFS or SQL design is a candidate, not a drop-in decision: async
   transaction lifetimes, crash recovery, quota failures, multi-tab writers,
   and migrations must be proven. Keep native LMDB writes on `Storage::run_txn`;
   its batching and thread-affinity constraints remain valid for native users.
4. **Add the JS host adapter.** Bridge JS functions and Promises, stable keys,
   serializable return values, target tracking records, exception handling,
   and lifecycle cleanup. Consider a dedicated worker for engine ownership;
   message passing and transferable data need a defined API. This proposal
   does not choose or add a bindings dependency.
5. **Integrate PGlite and TransformersJS as real host services.** PGlite as a
   derived-index target and CocoIndex's own checkpoint store are separate
   responsibilities. A PGlite target needs actual idempotent upserts/deletes,
   transactional sink failures, and restart behavior. A TransformersJS
   embedding function needs actual async calls, model/config fingerprints,
   cancellation, and reload behavior. No integration with either is supplied
   by this probe. Model downloads belong in a later explicit test budget.
6. **Demonstrate the requested full workflow.** In an actual supported browser,
   ingest a document, split/embed/index, reload, verify no unchanged recompute,
   edit and delete it, and recover from interrupted storage/target writes.
   Check model and pipeline-version invalidation, quota/permission failures,
   and concurrent tab behavior. Only this establishes browser workflow support.

Design questions for upstream review include the supported browser/worker
baseline, durability backend, whether an operations-only distribution is useful,
and JS callback ownership. They are feature-design decisions, not bugs that this
note claims to fix. Keep #482 open until the requested behavior is demonstrated.
