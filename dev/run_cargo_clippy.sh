#!/usr/bin/env bash
# Lints the Rust workspace with clippy, failing on any warning: every crate
# with its default features, and the Rust SDK with all of its features.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# `uv run` puts the project venv's Python first, for pyo3's build script.
uv run cargo clippy --workspace --all-targets -- -D warnings
uv run cargo clippy -p cocoindex --all-features --all-targets -- -D warnings
