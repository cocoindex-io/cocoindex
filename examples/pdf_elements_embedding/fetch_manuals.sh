#!/usr/bin/env bash
# Download four board game rulebooks into source_files/.
set -euo pipefail
mkdir -p source_files
for url in \
  https://www.catan.com/sites/default/files/2021-06/catan_base_rules_2020_200707.pdf \
  https://michalskig.wordpress.com/wp-content/uploads/2010/10/manilaenglishgame_133_gamerules.pdf \
  https://cdn.1j1ju.com/medias/2c/f9/7f-ticket-to-ride-rulebook.pdf \
  https://cdn.1j1ju.com/medias/0c/93/d6-stone-age-the-expansion-rulebook.pdf
do
  curl -fsSL -o "source_files/$(basename "$url")" "$url"
done
