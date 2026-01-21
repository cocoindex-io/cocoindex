#!/usr/bin/env bash
set -euo pipefail

# Always run from repo root (important for cargo workspace + relative paths)
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Prefer an in-repo venv if present, so this works even if user didn't "source .venv/bin/activate"
# Users can override with COCOINDEX_PYTHON if they want a different interpreter.
if [[ -n "${COCOINDEX_PYTHON:-}" ]]; then
  PY="$COCOINDEX_PYTHON"
elif [[ -x "$ROOT/.venv/bin/python" ]]; then
  PY="$ROOT/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PY="python3"
elif command -v python >/dev/null 2>&1; then
  PY="python"
else
  echo "error: python not found." >&2
  echo "hint: create/activate a venv (.venv) or set COCOINDEX_PYTHON=/path/to/python" >&2
  exit 1
fi

# Compute PYTHONHOME + PYTHONPATH based on the selected interpreter.
# This is specifically to help embedded Python (pyo3) locate stdlib + site-packages.
PYTHONHOME_DETECTED="$("$PY" -c 'import sys; print(sys.base_prefix)')"

PYTHONPATH_DETECTED="$("$PY" - <<'PY'
import os
import site
import sysconfig

paths = []

for key in ("stdlib", "platstdlib"):
    p = sysconfig.get_path(key)
    if p:
        paths.append(p)

for p in site.getsitepackages():
    if p:
        paths.append(p)

# Include repo python/ package path (safe + helps imports in embedded contexts)
repo_python = os.path.abspath("python")
if os.path.isdir(repo_python):
    paths.append(repo_python)

# de-dupe while preserving order
seen = set()
out = []
for p in paths:
    if p not in seen:
        seen.add(p)
        out.append(p)

print(":".join(out))
PY
)"

# Only set these if not already set, so we don't stomp custom setups.
export PYTHONHOME="${PYTHONHOME:-$PYTHONHOME_DETECTED}"

if [[ -n "${PYTHONPATH_DETECTED}" ]]; then
  if [[ -n "${PYTHONPATH:-}" ]]; then
    export PYTHONPATH="${PYTHONPATH_DETECTED}:${PYTHONPATH}"
  else
    export PYTHONPATH="${PYTHONPATH_DETECTED}"
  fi
fi

exec uv run cargo test "$@"
