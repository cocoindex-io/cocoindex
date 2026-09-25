#!/usr/bin/env bash
# Offline fallback corpus: short clips with real speech (so the transcript FTS
# field is exercised) over solid-color backgrounds. Uses macOS `say` for
# text-to-speech, so it only runs on a Mac. The visuals are blank, so this is a
# deterministic pipeline check, not a visual-search demo.
#
# For real footage, use ./download_sample_videos.sh instead. The pipeline just
# walks whatever videos land in ./videos.
set -euo pipefail

cd "$(dirname "$0")"
mkdir -p videos
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

gen() {
  local name="$1" bg="$2" text="$3"
  say -o "$TMP/$name.aiff" "$text"
  ffmpeg -nostdin -v error -y \
    -f lavfi -i "$bg" \
    -i "$TMP/$name.aiff" \
    -shortest -c:v libx264 -pix_fmt yuv420p -c:a aac \
    "videos/$name.mp4"
  echo "wrote videos/$name.mp4"
}

gen dog_park "color=c=green:s=640x360:r=15" \
  "A dog runs across the green park chasing a bright red ball in the sunshine. \
   Two children laugh and clap as the puppy leaps into the air. \
   Later the dog rests under a tall oak tree beside the pond."
gen kitchen "testsrc2=s=640x360:r=15" \
  "The chef slices fresh tomatoes and onions on a wooden board in the kitchen. \
   She heats olive oil in a pan and stirs in garlic and basil. \
   The sauce simmers slowly while bread bakes in the oven."
gen revenue "color=c=navy:s=640x360:r=15" \
  "The analyst explains quarterly revenue growth and profit margins on a chart. \
   Sales rose fifteen percent while operating costs stayed flat. \
   The team reviews the forecast for the next fiscal year."
