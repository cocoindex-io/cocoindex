# Declared target states memory benchmark

Measures the peak memory of a processing component that declares many target
states, with and without spilling them to disk
(`COCOINDEX_TARGET_STATE_SPILL_THRESHOLD`, see the
[custom target connector guide](../../docs/src/content/docs/advanced_topics/custom_target_connector.mdx)).

[main.py](main.py) mounts one component that declares `BENCH_N` dict rows of
about `BENCH_ROW_BYTES` bytes of text each against a recording target: the
handler tracks each row's fingerprint, the sink counts the actions it is given
and digests them. One invocation is one `update` against `BENCH_DB`, printing a
JSON line with the process's peak RSS, the counts, and the digest. Run it twice
against the same `BENCH_DB` for a cold pass (every row inserted) and a warm one
(nothing changed).

## Running

```sh
cd benchmarks/target_state_spill

# Spilling off (every declared value and action held in memory)
export BENCH_DB=$(mktemp -d)
COCOINDEX_TARGET_STATE_SPILL_THRESHOLD=1099511627776 uv run python main.py   # cold
COCOINDEX_TARGET_STATE_SPILL_THRESHOLD=1099511627776 uv run python main.py   # warm

# Spilling on (the default threshold, 32 MiB)
export BENCH_DB=$(mktemp -d)
uv run python main.py   # cold
uv run python main.py   # warm
```

`applied`, `sink_calls` and `digest` show what the sink received: the digest
must be the same with and without spilling; `sink_calls` grows with spilling,
since a component hands its actions over in chunks.

## Requirements

`cocoindex` importable by the interpreter, e.g. built from this repo via
`uv run maturin develop` at the repo root and run with the repo's venv.
