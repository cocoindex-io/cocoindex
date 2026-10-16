#!/usr/bin/env bash
# Covers the Rust SDK's optional features. The `cocoindex` SDK crate has no
# default features, so the workspace `cargo test` (dev/run_cargo_test.sh)
# builds none of its optional connectors and ops: this type-checks all of them
# and runs the tests of the ones that need no external server.
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
# tree only to skip. Arguments narrow the run, e.g. `--test fs_live`.
SDK_TEST_FEATURES="code_match,doris,embed_api,fs_live,google_drive,kafka,lancedb"
SDK_TEST_FEATURES+=",oci_object_storage,postgres,sqlite,text,turbopuffer,valkey"
cargo test -p cocoindex --features "$SDK_TEST_FEATURES" "$@"
