#!/usr/bin/env bash
# Portable docs build check for coding agents and local automation.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

cd "${REPO_ROOT}/docs"
# `npm ci`, not `npm i`: `npm i` re-serializes package-lock.json, which rewrites
# it whenever the local npm differs from the one that generated it. `npm ci`
# never writes the lockfile and fails if it disagrees with package.json. It also
# wipes node_modules, so skip it while npm's hidden lockfile (written by every
# install) is newer than both package.json and package-lock.json.
HIDDEN_LOCKFILE=node_modules/.package-lock.json
if [ ! -f "${HIDDEN_LOCKFILE}" ] ||
  [ package.json -nt "${HIDDEN_LOCKFILE}" ] ||
  [ package-lock.json -nt "${HIDDEN_LOCKFILE}" ]; then
  # No audit: its report says to run `npm audit fix`, which rewrites the lockfile.
  npm ci --no-audit --no-fund 2>&1
fi
npm run build 2>&1
