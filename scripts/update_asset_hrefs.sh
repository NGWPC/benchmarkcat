#!/bin/bash

# a script to quickly update asset hrefs in a collection without having to activate the ingest workflow for updating the catalog in python. 

# Define the directory containing the STAC items and the new base URL
collection_dir="/home/dylan.lee/static_cat/gfm-collection"
new_base_url="/efs/fim-data"

# Find all JSON files in the collection directory and its subdirectories
find "$collection_dir" -type f -name '*.json' | while read -r file; do
  # Use jq to update the href values
  jq --arg new_base_url "$new_base_url" '
    .assets |= with_entries(
      if .value.href | startswith("s3://fimc-data/benchmark") then
        .value.href = $new_base_url + .value.href[24:]
      else
        .
      end
    )' "$file" > tmp.json && mv tmp.json "$file"

  echo "Updated href in $file"
done

