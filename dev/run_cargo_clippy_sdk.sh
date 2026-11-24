#!/usr/bin/env bash
# Lints the Rust SDK's crates with clippy, failing on any warning: without
# features (the SDK has no default features), and with all of them. Also lints
# the `cocoindex` CLI for Rust projects (rust/cli) and its test fixture, which
# build on the SDK. None of them depends on pyo3, so this needs no Python.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

cargo clippy -p cocoindex -p cocoindex_macros --all-targets -- -D warnings
cargo clippy -p cocoindex --all-features --all-targets -- -D warnings
cargo clippy -p cocoindex_cli -p cocoindex_cli_fixture_app --all-targets -- -D warnings
