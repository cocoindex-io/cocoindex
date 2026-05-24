#!/usr/bin/env bash
# Fails if the cocoindex Rust SDK has any pyo3 in its normal dependency tree.
# The SDK must stay PyO3-free so downstream consumers don't transitively
# pull in Python bindings. The PyO3 bindings live in rust/py / rust/py_utils,
# which are NOT reachable from rust/sdk/cocoindex.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if cargo tree -p cocoindex -e normal | grep -q pyo3; then
  echo "ERROR: cocoindex SDK depends on pyo3 (must stay PyO3-free)" >&2
  cargo tree -p cocoindex -e normal | grep pyo3 >&2 || true
  exit 1
fi
