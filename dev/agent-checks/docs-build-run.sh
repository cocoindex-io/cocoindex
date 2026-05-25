#!/usr/bin/env bash
# Portable docs build check for coding agents and local automation.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

cd "${REPO_ROOT}/docs"
npm i 2>&1
npm run build 2>&1
