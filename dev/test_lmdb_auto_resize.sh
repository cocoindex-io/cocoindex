#!/usr/bin/env bash
# Exercise LMDB map-full auto-resize: opens a tiny map, writes until MapFull,
# and asserts the env doubles its map size and retries successfully.
#
# Usage (from repo root):
#   ./dev/test_lmdb_auto_resize.sh
#
# Optional env:
#   COCOINDEX_SKIP_UV=1   use `cargo` directly instead of `uv run cargo`
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Ensure rustup toolchain is on PATH when ~/.cargo/bin isn't linked yet.
if ! command -v cargo >/dev/null 2>&1; then
  RUSTUP_BIN="${HOME}/.rustup/toolchains/stable-aarch64-apple-darwin/bin"
  if [[ -x "${RUSTUP_BIN}/cargo" ]]; then
    export PATH="${RUSTUP_BIN}:${PATH}"
  fi
fi

if ! command -v cargo >/dev/null 2>&1; then
  echo "error: cargo not found. Install Rust (https://rustup.rs) or add it to PATH." >&2
  exit 1
fi

TEST_FILTER="auto_resizes_on_map_full"
# tracing `warn!` in the test binary; shows "LMDB map full, auto-resizing ..."
export RUST_LOG="${RUST_LOG:-cocoindex_core=warn}"

echo "==> LMDB auto-resize test"
echo "    filter: ${TEST_FILTER}"
echo "    RUST_LOG=${RUST_LOG}"
echo

run_cargo_test() {
  cargo test -p cocoindex_core "${TEST_FILTER}" -- --nocapture "$@"
}

if [[ "${COCOINDEX_SKIP_UV:-}" == "1" ]]; then
  run_cargo_test
elif command -v uv >/dev/null 2>&1; then
  uv run cargo test -p cocoindex_core "${TEST_FILTER}" -- --nocapture "$@"
else
  echo "note: uv not found, using cargo directly (set COCOINDEX_SKIP_UV=1 to silence)" >&2
  run_cargo_test
fi

echo
echo "==> OK: LMDB map-full auto-resize behavior verified"
