#!/usr/bin/env bash
set -euo pipefail

# Wrapper script for maturin develop that automatically detects free-threaded Python
# and adjusts build flags accordingly.
#
# Free-threaded Python (3.13+) is incompatible with PyO3's abi3 feature, so we must
# build with --no-default-features to disable abi3 while keeping legacy-states-v0.
#
# Usage: ./run_maturin_develop.sh [extra-args...]
#   Extra arguments are passed directly to maturin develop.
#   Example: ./run_maturin_develop.sh -F some-feature
#
# Detection methods:
# 1. Environment variable: COCOINDEX_FREE_THREADED=1 forces free-threaded mode
# 2. Auto-detection: Uses sys._is_gil_enabled() to detect free-threaded Python

# Check if running with free-threaded Python (Python 3.13+ has _is_gil_enabled)
IS_FREE_THREADED=$(python -c "import sys; print('1' if hasattr(sys, '_is_gil_enabled') and not sys._is_gil_enabled() else '0')" 2>/dev/null || echo "0")

# Allow environment variable override
IS_FREE_THREADED="${COCOINDEX_FREE_THREADED:-$IS_FREE_THREADED}"

if [ "$IS_FREE_THREADED" = "1" ]; then
    echo "Detected free-threaded Python, building without ABI3"
    # Disable abi3 (incompatible with free-threading) but keep legacy-states-v0
    exec uv run maturin develop --no-default-features -F legacy-states-v0 "$@"
else
    exec uv run maturin develop "$@"
fi
