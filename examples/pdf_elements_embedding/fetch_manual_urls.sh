#!/bin/sh

URLS=(
    https://www.catan.com/sites/default/files/2021-06/catan_base_rules_2020_200707.pdf
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
