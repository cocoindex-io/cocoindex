# Rust guidelines

Distilled from every review comment George (georgeh0) left on
cocoindex-io/cocoindex between July 2025 and September 2026: 576 inline
comments, 143 conversation comments and his review summaries, across 122
pull requests. Each rule names the pull request where he said it, so the
reasoning can be read in context (`https://github.com/cocoindex-io/cocoindex/pull/<n>`).

The rules are grouped by theme. The first three groups come up in nearly
every review; the rest are specific but recur.

## 2. Types carry meaning

- **Use `Duration` and `Instant`, never raw `u64` seconds.** "We should
  almost always use types like `Instant` and `Duration` instead of raw
  `u64`. They carry more concrete semantics" (#2251, #1241). The Python
  side mirrors this with `datetime.timedelta`, not `int` or `float`
  (#1241, #2232).
- **A struct with named fields beats a tuple.** `Vec<(u64, String)>` and
  `type PendingTask = (GPUCapacity, Sender)` were both sent back: "what each
  element of the tuple means is unclear" (#1920, #2277).
- **An enum beats a `bool`; a struct beats a loose return tuple.** A function
  returning `(StablePath, bool)` became a struct holding a
  `StablePathNodeType` (#1627).
- **`Option<T>` instead of a sentinel variant.** `KeepSep::{None, Left,
  Right}` became `Option<KeepSep>` with two variants; a global `Default`
  impl and `is_default()` disappeared with it (#1010, #1050). The same in
  Python: `None` instead of a `_NO_DEPS` or `_KEY_FN_UNSET` object when
  `None` is not otherwise a valid value (#1836, #2256), but reuse an existing
  `NotSetType` singleton when "unset" and `None` must differ (#1888).
- **No `Option` for a value that is never `None`.** `current_scope` was never
  assigned `None`, so it should not be an `Option` (#1105).
- **No floating-point accumulators.** `x - f + f` drifts. Store a scaled
  integer (#2277).
- **Default type parameter on the `Result` alias:**
  `pub type Result<T, E = Error> = std::result::Result<T, E>;` so callers can
  write both `Result<T>` and `Result<T, Other>` (#1380).

## 3. Errors

- **Library code never panics.** No `assert!` or `panic!` on runtime
  conditions, no `unwrap()` on fallible calls. Return an error and let the
  caller decide; through the FFI it becomes a Python exception (#2277 three
  times, #1459 four times: "Please use `?` here too. Change the return type
  of the function please").
- **Inside `Drop`, never panic.** Use
  `lock().unwrap_or_else(|e| e.into_inner())`, not `.expect(...)`; a panic
  in `Drop` during unwinding aborts the process (#2277).
- **Propagate with `?`, do not wrap with `map_err`.** Re-wrapping strips
  structured information such as backtraces. Implement `From` for the
  conversion once so `?` works at every call site (#1625, #1380).
- **Raise explicit errors for unsupported or inconsistent states.** Never
  silently disable a feature, return `None`, or `unwrap_or(usize::MAX)`
  when the situation is a bug or an unsupported input (#1153, #1122, #1134).
- **Choose the error class by who is at fault.** A caller mistake is a client
  error; a violated internal invariant is an internal error. Use the shared
  macros (`client_bail!`, `internal_error!`) and keep sibling branches
  consistent (#2277, #1704).
- **Fail loudly on explicit bad config; fall back quietly on an expected
  absence.** An unparsable `COCOINDEX_NUM_GPUS` is an error; a missing
  `nvidia-smi` falls back to 1 (#2277).
- **Error messages name things users can see.** Say which two fields are in
  conflicting order, not their indices. George rewrote three messages
  himself (#1153).
- **Attach useful context.** Wrap errors from user callbacks with the
  function name (#1275). Format context chains as `Caused by:` lines and
  append the backtrace for internal errors (#1380).
- **Do not swallow errors with a timeout that returns `Ok(empty)`.** Let a
  long wait wait; a hidden error is worse (#1189).
- **Validate at the boundary, not on the hot path.** Check positive settings
  once when they arrive (#1704). Do not validate what cannot be meaningfully
  validated, and trust type hints inside the crate (#2256).
- **Untrusted sizes do not drive allocation.** `Vec::new()`, not
  `Vec::with_capacity(size_hint)`, when deserializing (#2403).
- **Error types:** one `Error` enum plus a `Result` alias in the prelude, as
  `sqlx` and `anyhow` do. Keep a generic `HostLang(Box<dyn HostError>)`
  variant for other host languages. Drop `thiserror` when it stops adding
  value (#1380).

## 4. Idiomatic Rust

- **Let chains.** Collapse nested `if let` into `if let ... && let ...`
  (Rust 1.88) (#1262, #1468).
- **`while let`** instead of `loop { if let ... else { break } }` (#1105).
- **Borrow, do not clone.** `name.as_str()` over `name.clone()`; no clones
  when `&self` outlives the local futures that use it (#1105, #1408, #1454).
- **Avoid `Box<dyn Iterator>` per element.** An enum wrapping the two
  concrete iterators, with `impl Iterator`, avoids a heap allocation per
  node (#1127).
- **Derive what can be derived.** `Clone` on a struct holding an `Arc`,
  `Serialize` and `Deserialize` on data types (#2221, #1109).
- **Trait upcasting (Rust 1.86) replaces `as_any()`.** Make `Any` a supertrait
  and add a blanket impl (#1380).
- **Extension traits are called as methods.** `x.into_py_result()`, never
  `Trait::method(x)`; bring the trait into scope (#1380).
- **Blanket impls keep frameworks open.** A `MemoInput` trait with a blanket
  impl for `Serialize` types, not concrete types baked into the SDK (#2287).
- **Enum variants stay PascalCase;** use `#[serde(rename_all = ...)]` to
  change the wire form (#1010). Tagged enums use `tag = "kind"` with
  PascalCase literal values, since `type` is a keyword in many languages
  (#1050).
- **`std::time::Duration`, not the `tokio::time` alias** (#1241).
- **Named, typed constants for defaults.** `const TIMEOUT: Duration`, not a
  bare `u64` (#1241).
- **No `crate::` prefix when the prelude already brings the module in**
  (#1105).
- **Public fields over trivial getters.** `stats.num_starts.get()` is fine;
  `get_total_starts()` wrappers and three forwarding helpers are not
  (#1105 twice).
- **Remove layers that add no value.** A `PyErrWrapper` around `PyErr`, a
  `_canonicalize_key_fragment()` that only calls `_canonicalize()`, an
  `apply()` method that only forwards, a `SplitSortedVec` abstraction over
  a simple `skip(n)` (#1380, #2256, #2221, #2277).
- **`pub(crate)` for ad-hoc methods** that are not part of the public shape
  (#2306).
- **`HashMap` entry API** for insert-if-absent; take `&mut HashMap<K, V>`
  rather than a `seen` set plus an output `Vec` (#2306).
- **One struct, one lock.** When two fields are always accessed together,
  wrap them in a struct behind one `Mutex` rather than two locks (#2277).
  Prefer "list of struct" over parallel lists keyed the same way (#2034).
- **`std::sync::Mutex` when nothing awaits under the lock.** It is lighter
  than `tokio::sync::Mutex` (#2277).
- **A lock protects a resource.** Prefer `RwLock<Env>` over `RwLock<()>`
  beside the env, and name a bare coordinator by its field, not a new type
  (#2231).
- **Ownership first, then `Weak`.** When two things map 1:1, let one own the
  other. Maps of `Weak` keyed by addresses, cleanup callbacks and
  `register_cleanup` hooks all disappeared once the keeper owned the batcher
  (#2221).
- **Symmetric branches.** When one `match` arm returns a typed value, make
  the other return the same type rather than a string (#1267).
- **Collect all parts, then join once.** Push every piece into `parts` and
  `join(", ")`, instead of special-casing an empty prefix (#1111, #1105).

## 5. Performance: precompute, stream, read only what you need

- **Precompute in analysis, not per row.** Field index mappings, schema
  vectors and memo-key plans are computed once at build or decorator time
  and reused per row (#1153 twice, #1468, #1888, #2256).
- **O(n²) string comparison on a hot path is a bug.** Build a map first
  (#1153).
- **Stream, do not buffer.** Return an async stream (`try_stream!`,
  `ReceiverStream`) or a Python `AsyncIterator`; never collect into a `Vec`
  first. Flagged five times across three pull requests (#1627, #1687,
  #1920, #2303). Process rows with bounded parallelism taken from the
  existing execution options, not a local constant (#1468).
- **Read the entry you need.** A point read or prefix scan, not list-then-
  filter (#2078, #1920).
- **Fetch related state together.** Function memos and user states are
  fetched in one transaction, in one function named for the intent
  (`prefetch_fn_processing_states`) (#2034, #2076).
- **Avoid cloning `Vec<u8>` of arbitrary size;** use the engine's shared data
  type (#2034 twice). Avoid intermediate `Vec`s; filter in a closure or build
  the string directly (#1010, #1153, #1105).
- **Import once, not per call** when crossing into Python (#1627).
- **Scheduler fairness.** Watch for thundering herd (notify all waiters on
  every release), starvation (small requests forever pre-empting a large
  one), and O(n²) queue scans. Strict FIFO is acceptable; reservation is
  better if it does not freeze later tasks (#2277, a 20-comment thread).
- **`tokio::select!` over `tokio::spawn`** for a timeout or warning alongside
  a future (#1241, #1408). `#[instrument]` on the async fn instead of
  manual spans that cannot cross awaits (#1363).

## 6. Async, concurrency and cancellation

- **Cancellation safety.** If a future is dropped mid-`await` after acquiring
  part of a resource, the resource leaks. Use a drop guard that releases
  what was acquired and disarms on success. Test with `task.abort()`, which
  is the shape production cancellation takes (#2277).
- **Counters are cumulative.** Model "in process" as `starts - ends`, and
  read `ends` before `starts` so the difference is never negative (#1105).
- **Do not block the event loop.** A `join(timeout)` fallback on the loop
  thread was flagged; put all blocking work in one function and call it
  through `to_thread` once (#2232).
- **Use the native async API when the library has one;** otherwise be
  consistently sync rather than wrapping in `to_thread` (#1889, #1548).
- **Shared mutable state needs a lock or a scope.** A module dict filled from
  several threads races (#2240). A module-level "current selector" breaks
  when two updates run in parallel; pass it as an argument (#2195).
- **Transactions: deterministic lock order and retry.** Sort tables before
  acquiring locks so concurrent processes cannot deadlock; retry on
  `TransactionRollbackError` since Postgres kills exactly one side (#2240).
- **Kill, then wait.** `child.kill()` without `child.wait()` leaves a
  zombie (#2277).
- **External probes have a bounded timeout** and release the GIL while they
  run (#2277).
- **Register one shutdown hook,** not one per subsystem (#1747).
- **Re-raise `asyncio.CancelledError`** before any broad `except` (#2383).

## 8. Tests

- **Test behavior, not definitions.** Tests that assert a derived serde
  round-trip, a dataclass's default values, or that a constructor stored
  its arguments "don't provide extra value" and were removed (#1109,
  #1173, #1475, #1780).
- **Test code is production code.** No code path kept only so a test can
  call it; migrate the test to the real method (#2076).
- **Every fix gets a regression test** (#1705).
- **Cover the property, not the happy path.** For a selector feature:
  change an unselected input and assert it is untouched, then delete one
  and assert the same (#2195, insisted on three times). For `use_state`
  inside memoized functions: also the indirect call (#2034). For enums:
  also the nullable case (#1218). For a preview flag: the specific actions
  printed, not just the header (#1945).
- **Smoke-test generated output.** Run the generated project, not just check
  the files exist (#1535).
- **Say how it was tested** in the pull request; George asks whenever it is
  missing (#1138, #1468, #1548, #2231).
- **Run checks locally before pushing:** `cargo test`, `cargo fmt`, the
  pre-commit suite, and commit the updated `Cargo.lock` (#1127, #1134,
  #1955).
- **Keep CI cheap.** Order jobs so later steps reuse the compiled artifacts
  of earlier ones (#1135).
