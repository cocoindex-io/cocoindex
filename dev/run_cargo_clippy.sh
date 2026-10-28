#!/usr/bin/env bash
# Lints the Rust workspace with clippy, failing on any warning: every crate
# with its default features, except the Rust SDK's crates, which
# dev/run_cargo_clippy_sdk.sh lints.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# `uv run` puts the project venv's Python first, for pyo3's build script.
uv run cargo clippy --workspace --exclude cocoindex --exclude cocoindex_macros \
  --all-targets -- -D warnings
