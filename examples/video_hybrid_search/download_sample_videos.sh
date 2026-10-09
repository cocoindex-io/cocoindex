#!/usr/bin/env bash
# Download a small, openly licensed sample corpus into ./videos: two short clips
# from the Blender open movies (CC BY, real scenes with English dialogue), so both
# the visual and transcript sides of the demo have something to match. ffmpeg reads
# each source over HTTP and pulls only the trimmed segment, so the download stays
# small even though the source films are large. See ATTRIBUTION.md for licenses.
#
# Usage: ./download_sample_videos.sh [--force]   (--force re-downloads existing clips)
#
# For a no-network fallback, use ./make_sample_videos.sh instead (generated clips).
set -euo pipefail

cd "$(dirname "$0")"

for tool in ffmpeg ffprobe; do
  command -v "$tool" >/dev/null || { echo "error: '$tool' not found on PATH." >&2; exit 1; }
done

FORCE=0
case "${1:-}" in
  "") ;;
  --force) FORCE=1 ;;
  *) echo "usage: $0 [--force]" >&2; exit 2 ;;
esac

mkdir -p videos

DUR=22  # seconds per clip

# name | source URL | trim start (s)
CLIPS=(
  "tears_of_steel|https://download.blender.org/demo/movies/ToS/tears_of_steel_720p.mov|25"
  "sintel|https://download.blender.org/durian/movies/Sintel.2010.720p.mkv|250"
)

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

for clip in "${CLIPS[@]}"; do
  IFS='|' read -r name url start <<<"$clip"
  out="videos/$name.mp4"
  if [ -f "$out" ] && [ "$FORCE" -eq 0 ]; then
    echo "skip $out (exists; --force to redo)"
    continue
  fi
  echo "fetching + trimming $name (${DUR}s from ${start}s)..."
  # Trim into a temp file, validate it, and only then move it into videos/, so a
  # failed run never leaves a broken clip that a later run would skip. -ss before -i
  # seeks over HTTP so only the needed segment is downloaded; reconnect flags ride
  # out transient network hiccups.
  tmp_out="$TMP/$name.mp4"
  ffmpeg -nostdin -v error -y \
    -reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5 \
    -ss "$start" -t "$DUR" -i "$url" \
    -vf "scale=640:-2" -c:v libx264 -pix_fmt yuv420p -c:a aac -movflags +faststart \
    "$tmp_out"
  ffprobe -v error -show_entries format=duration -of csv=p=0 "$tmp_out" >/dev/null
  mv "$tmp_out" "$out"
  echo "wrote $out"
done

echo "done. See ATTRIBUTION.md for licenses and credits."
