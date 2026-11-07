# Rust SDK benchmarks

Criterion benches of the Rust SDK (`cocoindex` crate). Each is self-contained
and runs against a temp LMDB state store; none needs a service.

| Bench | What it measures | Run |
| ----- | ---------------- | --- |
| `sdk_engine` | Engine-facing hot paths end to end through the public SDK API: memo calls, per-component overhead, content fingerprints, target reconcile, batched functions, app startup | `cargo bench -p cocoindex --bench sdk_engine` |
| `sdk_microbench` | File walking, MessagePack encoding and memo-key fingerprinting in isolation | `cargo bench -p cocoindex --bench sdk_microbench` |
| `state_store_read` | State-store read and inspect paths (`show`, listing) over a seeded store | `cargo bench -p cocoindex --features bench-support --bench state_store_read` |

Append `-- <group>` to run one group, for example
`cargo bench -p cocoindex --bench sdk_engine -- memo_call`. Criterion keeps
the previous run under `target/criterion/` and prints the change against it,
so run a bench once on `main` and once on the branch to quote before and
after.

The cross-SDK scenario benchmarks (the same pipeline in Rust and in Python)
live under [`benchmarks/`](../../../../benchmarks/) at the repository root.

## `sdk_engine` groups

| Group | Benchmark ids | Reported as |
| ----- | ------------- | ----------- |
| `memo_call` | `hit/small`, `hit/large_100k`, `miss/small`, `miss/large_100k` | per call of a `#[cocoindex::function(memo)]`, inside one update |
| `component_overhead` | `mount_each/warm/{1000,10000}`, `use_mount/warm/{1000,10000}` | per trivial component on a warm run |
| `fingerprint` | `from_vec/{1k,100k,10m}`, `from_bytes/{1k,100k,10m}` | bytes per second of `Fingerprint::from(&Vec<u8>)` against `Fingerprint::from_bytes` |
| `target_reconcile` | `cold/10000`, `no_change/10000` | per row declared into a counting no-op `TargetHandler` |
| `batched_fn` | `concurrent_calls/{1000,10000}` | per call of a `#[cocoindex::function(batching)]` with a trivial body |
| `startup` | `async/open_and_empty_update`, `blocking/open_and_empty_update` | one environment open, app creation and empty update |

## Baseline before the performance fixes

Measured once on `main` at commit `f8105f0f` (2026-11-06) on a shared, busy
64-core Linux machine (load average about 19 during the run), so treat the
numbers as rough. Criterion mean of 10 samples.

| Benchmark | Mean | Per element |
| --------- | ---: | ----------: |
| `memo_call/hit/small` | 1.36 µs | 1.36 µs/call |
| `memo_call/hit/large_100k` | 117 µs | 117 µs/call |
| `memo_call/miss/small` | 1.94 µs | 1.94 µs/call |
| `memo_call/miss/large_100k` | 121 µs | 121 µs/call |
| `component_overhead/mount_each/warm/1000` | 15.9 ms | 15.9 µs/component |
| `component_overhead/mount_each/warm/10000` | 218 ms | 21.8 µs/component |
| `component_overhead/use_mount/warm/1000` | 452 ms | 452 µs/component |
| `component_overhead/use_mount/warm/10000` | 3.45 s | 345 µs/component |
| `fingerprint/from_vec/1k` | 6.51 µs | 150 MiB/s |
| `fingerprint/from_vec/100k` | 647 µs | 151 MiB/s |
| `fingerprint/from_vec/10m` | 66.2 ms | 151 MiB/s |
| `fingerprint/from_bytes/1k` | 1.11 µs | 883 MiB/s |
| `fingerprint/from_bytes/100k` | 112 µs | 873 MiB/s |
| `fingerprint/from_bytes/10m` | 11.5 ms | 872 MiB/s |
| `target_reconcile/cold/10000` | 44.3 ms | 4.43 µs/row |
| `target_reconcile/no_change/10000` | 47.2 ms | 4.71 µs/row |
| `batched_fn/concurrent_calls/1000` | 3.53 ms | 3.53 µs/call |
| `batched_fn/concurrent_calls/10000` | 69.9 ms | 6.99 µs/call |
| `startup/async/open_and_empty_update` | 1.00 ms | |
| `startup/blocking/open_and_empty_update` | 15.4 ms | |

What the baseline shows:

- `Fingerprint::from(&Vec<u8>)` goes through serde and hashes byte by byte,
  about 6× slower than `Fingerprint::from_bytes` on the same bytes.
- A memo call with a 100 KB string argument costs about 117 µs even on a hit,
  which is the hash of the argument at the `from_bytes` rate.
- A sequential `use_mount!` (`Ctx::scope`) costs 20× a `mount_each!`
  component: each foreground mount waits for its own commit.
- The `*_blocking` entry points build a tokio runtime per call, which is most
  of their 15 ms against 1 ms for the async path.
