#!/bin/sh

URLS=(
    https://fgbradleys.com/wp-content/uploads/rules/Carcassonne-rules.pdf
    https://michalskig.wordpress.com/wp-content/uploads/2010/10/manilaenglishgame_133_gamerules.pdf
    https://cdn.1j1ju.com/medias/2c/f9/7f-ticket-to-ride-rulebook.pdf
    https://cdn.1j1ju.com/medias/0c/93/d6-stone-age-the-expansion-rulebook.pdf
)

OUTPUT_DIR="source_files"
mkdir -p $OUTPUT_DIR
for URL in "${URLS[@]}"; do
    echo "Fetching $URL"
    wget -P $OUTPUT_DIR $URL
done
