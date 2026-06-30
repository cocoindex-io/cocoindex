#!/usr/bin/env bash
# Verifies that every isolated Rust workspace outside the main workspace — the
# Rust SDK examples and benchmark implementations — still *compiles* against the
# current SDK, via `cargo check --locked`. These are intentionally separate
# workspaces (each has its own [workspace] in Cargo.toml) so they mimic
# downstream user projects.
#
# Why `cargo check` and not `cargo test`: the goal on every PR is to catch SDK
# API breakage in the examples, which type-checking already does. Full
# build+test of the examples is far heavier (codegen, linking, running) and is
# left to a scheduled/nightly job. With ~30 example crates each pulling a large,
# distinct dependency tree (surrealdb, arrow/lancedb, candle, aws-sdk, …),
# `cargo test` here both blew past the runner's disk (os error 28 building
# example .rlibs) and dominated CI wall-clock; `cargo check` avoids the heavy
# codegen artifacts entirely.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Check every external workspace into one shared target dir. The `cocoindex`
# SDK is a `path` dependency, so without this each workspace re-checks the SDK
# and its whole transitive dep tree into its own `target/` — N example crates
# means N copies of the shared artifacts on disk, which is what exhausted the
# runner. Pointing them all at one CARGO_TARGET_DIR stores each (crate, feature
# set) once and reuses it across workspaces. Each workspace keeps its own
# Cargo.lock and `--locked`, so downstream-project resolution fidelity is
# unchanged — only compiled output is shared. Respects a caller-set value.
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT/target/rust-externs}"

shopt -s nullglob
dirs=(examples/rust/*/ benchmarks/*/rust/)
shopt -u nullglob

if [[ ${#dirs[@]} -eq 0 ]]; then
  echo "No external Rust workspaces found under examples/rust/ or benchmarks/*/rust/"
  exit 0
fi

for dir in "${dirs[@]}"; do
  [[ -f "$dir/Cargo.toml" ]] || continue
  echo "==> cargo check ($dir)"
  cargo check --locked --manifest-path "$dir/Cargo.toml"
done
