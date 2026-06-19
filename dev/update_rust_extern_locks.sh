#!/usr/bin/env bash
# Regenerates Cargo.lock for every isolated Rust workspace that lives outside
# the main workspace — Rust SDK examples and Rust benchmark implementations.
#
# These workspaces depend on the cocoindex SDK via a `path = ...` dependency, so
# whenever the SDK's (transitive) dependencies change, their lockfiles go stale.
# `run_cargo_test_rust_externs.sh` then fails because it runs `cargo test
# --locked`. This script keeps those lockfiles current in place (the same role
# `uv-lock` plays for `uv.lock`); the matching pre-commit hook stages the
# refreshed lockfiles so the `--locked` test passes.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

shopt -s nullglob
dirs=(examples/rust/*/ benchmarks/*/rust/)
shopt -u nullglob

for dir in "${dirs[@]}"; do
  [[ -f "$dir/Cargo.toml" ]] || continue
  # `--workspace` only repins the local workspace members, leaving registry
  # deps at their existing versions unless a constraint forces a change — so
  # this is a minimal refresh, not a blanket dependency bump.
  cargo update --workspace --quiet --manifest-path "$dir/Cargo.toml"
done
