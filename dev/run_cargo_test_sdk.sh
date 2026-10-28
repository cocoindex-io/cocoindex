#!/usr/bin/env bash
# Tests the Rust SDK. The workspace `cargo test` hook leaves the SDK's crates
# out, so this is their test run. The `cocoindex` SDK crate has no default
# features: this type-checks all of its optional connectors and ops, and runs
# the tests of the ones that need no external server.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Type-check every feature on every target, to keep the features that no test
# run enables from bit-rotting. This also covers the bench targets, which
# `cargo test` never compiles.
cargo check -p cocoindex --all-features --all-targets

# Run the SDK tests under the features that need no external server: their
# tests are hermetic, or skip unless an env var names a live server. The
# features left out probe localhost for a server, or pull in a large dependency
# tree only to skip. The build without features has no tests of its own
# (cargo-clippy-sdk compiles it). Arguments narrow the run, e.g.
# `--test fs_live`.
SDK_TEST_FEATURES="code_match,doris,embed_api,fs_live,google_drive,kafka,lancedb"
SDK_TEST_FEATURES+=",oci_object_storage,postgres,sqlite,text,turbopuffer,valkey"
cargo test -p cocoindex --features "$SDK_TEST_FEATURES" "$@"
