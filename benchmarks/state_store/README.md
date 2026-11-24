# State-store latency benchmark

A focused benchmark that isolates the state-store cost of three core
operations as the component count grows, in the Python SDK and in the Rust
SDK:

| Phase | What runs | What gets measured |
| ----- | --------- | ------------------ |
| `cold` | first update against a fresh state | path bookkeeping + memo writes for all `N` components |
| `warm` | a second update against the populated state | memo lookups (all cache hits) for all `N` components |
| `drop` | drop against the populated state | drop cascade + tombstone GC + final clear |

The pipeline is written twice, once per SDK ([python/main.py](python/main.py)
and [rust/src/main.rs](rust/src/main.rs)): a single root component that
`mount_each`'s `N` child components, each a memoized function that declares
`M` target states against a no-op fake target. The engine still runs the
full pre_commit / commit lifecycle and writes the per-target tracking
records, but the user-facing sink does nothing — so the only work measured
is cocoindex's own state-store traffic, isolated from any external IO. With
`M = 0` (the default) no target states are declared at all, leaving just
component-path bookkeeping, memo read/write, and tombstone GC on drop.

`N` and `M` are read from the `BENCH_N` / `BENCH_M` env vars. The runner
sweeps across multiple values, pointing `COCOINDEX_DB` at a fresh temp
directory per cell, so each `(N, M)` cell starts from a truly cold state.
The Python SDK runs through the cocoindex CLI (`python -m cocoindex.cli
update|drop`); the Rust SDK runs the `rust/` crate's release binary
(`state_store update|drop`). Both are timed as whole processes, so the
Python numbers include interpreter and import start-up.

One asymmetry on `drop` with `M > 0`: the Python side registers its no-op
provider at module level, so `drop` reaches the handler and runs a delete
action per target state. The Rust SDK registers a root provider per run,
and `App::drop_state` cannot reach it, so the Rust `drop` skips those
deletes (the engine logs a warning per path) and clears the state only.

## Running

```sh
# Default: N ∈ {100, 1000, 10000}, M = 0
./run.sh

# Just one N
./run.sh --n 100

# Also exercise the target-state write path (M states per component)
./run.sh --n 1000 --m 0,10

# One SDK only
./run.sh --lang rust
./run.sh --lang python

# JSON output (for piping into something)
./run.sh --format json
```

## Requirements

- For the Python SDK: `cocoindex` importable by the current `python3` (e.g.
  built from this repo via `uv run maturin develop`); the runner invokes the
  CLI as `python -m cocoindex.cli` subprocesses.
- For the Rust SDK: `cargo`; the runner builds `rust/` in release mode
  (into `CARGO_TARGET_DIR` when set, else `rust/target`).

Alternatively, run through the benchmark's own uv project from the repo root
(builds the editable install automatically):

```sh
uv run --project benchmarks/state_store/python python benchmarks/state_store/python/runner.py --n 100
```

## How to read the output

```
       N     M  lang        cold      warm      drop
----------------------------------------------------
     100     0  rust       0.021     0.016     0.019
     100     0  python     0.500     0.300     0.250
    1000     0  rust       0.038     0.033     0.033
    1000     0  python     0.700     0.350     0.300
```

(Illustrative numbers; run the benchmark for the real ones.) If state-store
round-trip cost dominates, times should grow roughly linearly with `N` for
`cold` and `warm`; `drop` additionally exercises the cascade, which is
serialized through the single-writer transaction batcher.

## Baseline before the Rust SDK performance fixes

Measured once on `main` at commit `f8105f0f` (2026-11-06) on a shared, busy
64-core Linux machine (load average about 19), so treat the numbers as
rough. Wall-clock seconds per process.

| N | M | SDK | cold | warm | drop |
| --: | --: | --- | ---: | ---: | ---: |
| 100 | 0 | rust | 0.021 | 0.016 | 0.019 |
| 100 | 10 | rust | 0.023 | 0.017 | 0.021 |
| 1000 | 0 | rust | 0.038 | 0.033 | 0.033 |
| 1000 | 10 | rust | 0.070 | 0.040 | 0.061 |
| 10000 | 0 | rust | 0.334 | 0.281 | 0.173 |
| 10000 | 10 | rust | 0.714 | 0.267 | 0.535 |
| 100 | 0 | python | 0.450 | 0.271 | 0.236 |
| 100 | 10 | python | 0.380 | 0.265 | 0.296 |
| 1000 | 0 | python | 0.925 | 0.664 | 0.286 |
| 1000 | 10 | python | 1.912 | 0.653 | 1.127 |
| 10000 | 0 | python | 8.875 | 4.787 | 1.017 |
| 10000 | 10 | python | 17.105 | 4.363 | 8.871 |

The Rust rows exclude the Python interpreter start-up that the Python rows
carry (about 0.2 s per process). At N = 10000 the Rust SDK's cold update is
about 25× faster and its warm update about 17× faster; its `drop` with
M = 10 skips the per-state deletes (see the asymmetry above), so that cell
is not comparable.

## Files

```
benchmarks/state_store/
├─ README.md          ← you are here
├─ run.sh             ← thin wrapper → python3 python/runner.py
├─ python/
│  ├─ runner.py       ← orchestrator: builds Rust, runs both SDKs, table/JSON output
│  ├─ main.py         ← the Python SDK pipeline (N memoized children × M no-op target states)
│  └─ pyproject.toml  ← uv project pinning the local cocoindex editable install
└─ rust/              ← the same pipeline on the Rust SDK (Cargo crate)
   ├─ Cargo.toml
   └─ src/main.rs
```
