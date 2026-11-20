# Evaluate profile-guided optimization for the Python SDK

This experiment builds a normal release, an instrumented release, and a
profile-optimized release of the Linux x86-64 Python extension from one checkout.
The existing file-summarization pipeline supplies both training and evaluation
workloads. The script alternates which build runs first, uses fresh state per
trial and build, and checks that the output hashes match after every phase.
It reports time spent in `app.update()`, excluding Python startup and data
generation.

## Reproduce

Run from the repository root on Linux x86-64, with standard CPython 3.11 or newer and
the same Rust toolchain for all builds. Install the Python dependencies
without building a separate copy of the extension.

```bash
uv sync --no-install-project --no-dev
rustup component add llvm-tools-preview

export PYO3_PYTHON="$PWD/.venv/bin/python"
target=x86_64-unknown-linux-gnu
work="$(mktemp -d)"
profdata="$(rustc --print sysroot)/lib/rustlib/$target/bin/llvm-profdata"
runner=benchmarks/file_summarization/python/compare_pgo.py

build() {
  cargo build --release --locked -p cocoindex_py --features pyo3/extension-module \
    --target "$target" --target-dir "$work/$1"
}
extension() {
  printf '%s/%s/%s/release/libcocoindex_py.so\n' "$work" "$1" "$target"
}

RUSTFLAGS= build baseline
RUSTFLAGS="-Cprofile-generate=$work/profiles" build instrumented
mkdir -p "$work/profiles"
LLVM_PROFILE_FILE="$work/profiles/%m-%p.profraw" "$PYO3_PYTHON" "$runner" \
  "$(extension instrumented)" --scenario codebase --profile mixed --scale tiny --trials 2
"$profdata" merge -o "$work/merged.profdata" "$work/profiles/"*.profraw
RUSTFLAGS="-Cprofile-use=$work/merged.profdata" build pgo

"$PYO3_PYTHON" "$runner" "$(extension baseline)" --candidate "$(extension pgo)" \
  --scenario codebase --profile cpu --scale medium --trials 7
"$PYO3_PYTHON" "$runner" "$(extension baseline)" --candidate "$(extension pgo)" \
  --scenario docs --profile io --scale medium --trials 7
```

The evaluation uses different workload profiles and a larger dataset than
training. Each trial measures cold indexing, a warm rerun, and an incremental
edit. The JSON includes all samples so spread and outliers are visible.
Run on an otherwise idle machine and repeat on hardware representative of
the intended release target. These synthetic workloads do not exercise
external services or every engine path.

See the [Rust PGO guide](https://doc.rust-lang.org/rustc/profile-guided-optimization.html)
for the instrumentation and merge flags.

## Example results

Linux x86-64 workspace on an AMD EPYC 7763, CPython 3.12.14 and Rust 1.91.1.
Both binaries came from `1f01fe437241`.
The instrumented build collected six profiles across two tiny training trials.
All 42 phase comparisons had matching output hashes.

Each cell shows the median and observed range in milliseconds across seven
trials. Negative changes indicate lower median time with PGO.

| Workload | Phase | Release (ms) | PGO (ms) | Median change |
| --- | --- | ---: | ---: | ---: |
| codebase / cpu | cold | 8,074.4 (6,659.4–9,734.7) | 7,974.9 (6,969.3–9,955.6) | -1.2% |
| codebase / cpu | warm | 651.1 (523.0–1,142.4) | 532.0 (393.3–639.3) | -18.3% |
| codebase / cpu | edit | 611.5 (498.7–1,096.7) | 622.1 (501.0–724.5) | +1.7% |
| docs / io | cold | 5,255.4 (5,024.1–5,843.3) | 5,143.4 (4,768.5–8,175.6) | -2.1% |
| docs / io | warm | 1,325.4 (966.9–1,638.1) | 1,255.6 (1,072.7–1,466.2) | -5.3% |
| docs / io | edit | 1,534.2 (1,226.2–1,874.3) | 1,376.7 (1,048.5–1,740.8) | -10.3% |

The largest median changes were in warm codebase runs and incremental
documentation edits. Cold-run medians moved by 2.1% or less and individual
times overlapped across builds. The codebase edit median increased slightly.
Further runs on representative hardware and workloads would help establish
which effects carry over to release builds.
