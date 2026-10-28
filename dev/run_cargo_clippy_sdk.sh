#!/usr/bin/env bash
# Lints the Rust SDK's crates with clippy, failing on any warning: without
# features (the SDK has no default features), and with all of them. The SDK
# doesn't depend on pyo3, so this needs no Python.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

cargo clippy -p cocoindex -p cocoindex_macros --all-targets -- -D warnings
cargo clippy -p cocoindex --all-features --all-targets -- -D warnings
