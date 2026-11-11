#!/usr/bin/env bash
# Tests the Rust SDK and the `cocoindex` CLI for Rust projects (rust/cli),
# which builds on it. The workspace `cargo test` hook leaves these crates
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
SDK_TEST_FEATURES="azure_blob,bigquery,code_match,doris,embed_api,fs_live,google_drive,kafka,lancedb"
SDK_TEST_FEATURES+=",oci_object_storage,postgres,snowflake,sqlite,surrealdb,surrealdb_mem,testing,text,turbopuffer,valkey"
# The zvec tests link the prebuilt `zvec_c_api` shared library that the
# `zvec-rust` build script downloads. On Windows Cargo does not put that DLL on
# the test binaries' search path (STATUS_DLL_NOT_FOUND), so the tests run on
# the other platforms; the all-features check above still compiles the target.
case "${OSTYPE:-}" in
  msys* | cygwin* | win32*) ;;
  *) SDK_TEST_FEATURES+=",zvec" ;;
esac
cargo test -p cocoindex --features "$SDK_TEST_FEATURES" "$@"

# The CLI's tests: unit tests, and end-to-end tests that build the fixture app
# (rust/cli/tests/fixture_app) and drive it through the real binary. Arguments
# narrow the SDK run above, so a narrowed run leaves these out.
if [[ $# -eq 0 ]]; then
  cargo test -p cocoindex_cli
fi
