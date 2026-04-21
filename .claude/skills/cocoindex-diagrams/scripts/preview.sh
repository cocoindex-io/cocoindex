#!/usr/bin/env bash
# preview.sh — build the CocoIndex docs site, serve it at the correct
# /docs-v1 base path, screenshot a page with headless Chrome, and print
# paths to PNG outputs.
#
# Usage:
#   preview.sh <docs-slug> [crop-y-top] [crop-height]
#   preview.sh programming_guide/core_concepts
#   preview.sh programming_guide/core_concepts 3300 600
#
# Environment assumptions:
#   - Run from the cocoindex repo root (cwd doesn't matter — paths resolve
#     from this script's location).
#   - Google Chrome installed at the standard macOS path.
#   - ImageMagick (magick) and rsync available.

set -euo pipefail

SLUG="${1:-}"
CROP_Y="${2:-3300}"
CROP_H="${3:-700}"

if [[ -z "${SLUG}" ]]; then
  echo "usage: preview.sh <docs-slug> [crop-y-top] [crop-height]" >&2
  echo "  example: preview.sh programming_guide/core_concepts" >&2
  exit 1
fi

# Resolve the cocoindex repo root relative to this script.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"
DOCS_DIR="${REPO_ROOT}/docs"
PREVIEW_DIR="/tmp/dg-preview"
CHROME="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"

if [[ ! -d "${DOCS_DIR}" ]]; then
  echo "error: expected docs/ at ${DOCS_DIR}" >&2
  exit 1
fi

echo "▶ building docs site…"
(cd "${DOCS_DIR}" && npm run build >/dev/null)

echo "▶ mirroring dist → ${PREVIEW_DIR}/docs-v1/ (base path matters)"
mkdir -p "${PREVIEW_DIR}/docs-v1"
rsync -a --delete "${DOCS_DIR}/dist/" "${PREVIEW_DIR}/docs-v1/"

# Pick a free port.
PORT=8765
while lsof -ti:${PORT} >/dev/null 2>&1; do
  PORT=$((PORT + 1))
done

echo "▶ serving on :${PORT}"
(cd "${PREVIEW_DIR}" && python3 -m http.server "${PORT}" >/tmp/dg-preview-srv.log 2>&1) &
SERVER_PID=$!
trap 'kill ${SERVER_PID} 2>/dev/null || true' EXIT
sleep 1.5

# Astro's default output layout is `<slug>/index.html`. The older
# `build.format: 'file'` output was `<slug>.html`. Support both.
URL="http://localhost:${PORT}/docs-v1/${SLUG}/"
if ! curl -sfI "${URL}" >/dev/null 2>&1; then
  URL="http://localhost:${PORT}/docs-v1/${SLUG}.html"
fi
echo "▶ screenshotting ${URL}"

FULL_PNG="${PREVIEW_DIR}/full.png"
"${CHROME}" \
  --headless=new \
  --disable-gpu \
  --hide-scrollbars \
  --virtual-time-budget=3000 \
  --window-size=1400,5200 \
  --force-device-scale-factor=1 \
  --screenshot="${FULL_PNG}" \
  "${URL}" 2>&1 | grep -v 'ERROR:net/cert' | tail -1 || true

if [[ ! -s "${FULL_PNG}" ]]; then
  echo "error: screenshot empty or missing" >&2
  exit 1
fi

CROP_PNG="${PREVIEW_DIR}/crop.png"
magick "${FULL_PNG}" -crop "1400x${CROP_H}+0+${CROP_Y}" "${CROP_PNG}"

echo ""
echo "✓ rendered"
echo "  full:  ${FULL_PNG}"
echo "  crop:  ${CROP_PNG} (y=${CROP_Y}, h=${CROP_H})"
echo ""
echo "Read the crop with the Read tool; if the target diagram is not in"
echo "view, rerun with a different crop-y-top (e.g. 2500, 3500, 4000)."
