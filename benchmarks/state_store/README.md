# State-store latency benchmark

A focused benchmark that isolates the state-store cost of three core
operations as the component count grows:

| Phase | What the CLI does | What gets measured |
| ----- | ----------------- | ------------------ |
| `cold` | first `cocoindex update` against a fresh state | path bookkeeping + memo writes for all `N` components |
| `warm` | a second `cocoindex update` against the populated state | memo lookups (all cache hits) for all `N` components |
| `drop` | `cocoindex drop -f` against the populated state | drop cascade + tombstone GC + final clear |

The pipeline ([main.py](main.py)) is a single `app_main` that
`mount_each`'s `N` child components, each running a memoized function that
declares `M` target states against a no-op fake target. The engine still
runs the full pre_commit / commit lifecycle and writes the per-target
tracking records, but the user-facing sink does nothing — so the only work
measured is cocoindex's own state-store traffic, isolated from any external
IO. With `M = 0` (the default) no target states are declared at all, leaving
just component-path bookkeeping, memo read/write, and tombstone GC on drop.

`N` and `M` are read from the `BENCH_N` / `BENCH_M` env vars. The runner
sweeps across multiple values, pointing `COCOINDEX_DB` at a fresh temp
directory per cell, so each `(N, M)` cell starts from a truly cold state.

## Running

```sh
# Default: N ∈ {100, 1000, 10000}, M = 0
./run.sh

# Just one N
./run.sh --n 100

# Also exercise the target-state write path (M states per component)
./run.sh --n 1000 --m 0,10

# JSON output (for piping into something)
./run.sh --format json
```

## Requirements

- `cocoindex` importable by the current `python3` (e.g. built from this repo
  via `uv run maturin develop`); the runner invokes the CLI as
  `python -m cocoindex.cli` subprocesses.

Alternatively, run through the benchmark's own uv project from the repo root
(builds the editable install automatically):

```sh
uv run --project benchmarks/state_store python benchmarks/state_store/runner.py --n 100
```

## How to read the output

```
       N     M      cold      warm      drop
---------------------------------------------
     100     0     0.500     0.300     0.250
    1000     0     0.700     0.350     0.300
   10000     0     2.100     0.900     0.500
```

(Illustrative numbers; run the benchmark for the real ones.) If state-store
round-trip cost dominates, times should grow roughly linearly with `N` for
`cold` and `warm`; `drop` additionally exercises the cascade, which is
serialized through the single-writer transaction batcher.

## Files

```
benchmarks/state_store/
├─ README.md       ← you are here
├─ run.sh          ← thin wrapper → python3 runner.py
├─ runner.py       ← orchestrator: cell loop + table/JSON output
├─ main.py         ← the benchmarked pipeline (N memoized children × M no-op target states)
└─ pyproject.toml  ← uv project pinning the local cocoindex editable install
```
