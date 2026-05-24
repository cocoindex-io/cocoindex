#!/usr/bin/env bash
# Runs `cargo test --locked` on every isolated Rust workspace that lives outside
# the main workspace — Rust SDK examples and Rust benchmark implementations.
# These are intentionally separate workspaces (each has its own [workspace] in
# Cargo.toml) so they mimic downstream user projects.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

shopt -s nullglob
dirs=(examples/rust/*/ benchmarks/*/rust/)
shopt -u nullglob

if [[ ${#dirs[@]} -eq 0 ]]; then
  echo "No external Rust workspaces found under examples/rust/ or benchmarks/*/rust/"
  exit 0
fi

for dir in "${dirs[@]}"; do
  [[ -f "$dir/Cargo.toml" ]] || continue
  echo "==> cargo test ($dir)"
  cargo test --locked --manifest-path "$dir/Cargo.toml"
done
