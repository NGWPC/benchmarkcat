#!/bin/bash

# The goal of this script is to list all the files in the recursive zip archives that the BLE data is stored in. There are lots of different files and the structure is complicated and different for each study so made this script so could get a quick overview.

bucket="fimc-data"
dir_bfe="benchmark/bfe_benchmark_data/"
dir_stac="benchmark/benchmark_stac_data/ble/"
dirs=$(aws s3 ls s3://$bucket/$dir_stac | awk '{print $2}' | grep -E '^[0-9]{8}/$')
ble_dirs=$(aws s3 ls s3://$bucket/$dir_bfe | awk '{print $2}')

echo "ble dirs are:"
echo $ble_dirs

# Function to list files in zip archives recursively
list_zip_contents() {
    local zip_file=$1
    local base_dir=$2

    echo "Listing contents of $zip_file"
    
    # List contents of the zip file
    unzip -l "$zip_file"

    # Extract the zip file to a temporary directory
    local temp_dir=$(mktemp -d)
    unzip -q "$zip_file" -d "$temp_dir"

    # Find and list contents of nested zip files
    shopt -s globstar
    for nested_zip in "$temp_dir"/**/*.zip; do
        [ -e "$nested_zip" ] || continue
        list_zip_contents "$nested_zip" "$base_dir"
    done

    # Clean up
    rm -rf "$temp_dir"
}

# Iterate over each huc-8 string
for dir in $ble_dirs; do
    echo "Processing directory: $dir"
    
    # Remove trailing slash 
    clean_dir=${dir%/}
    
    # Create or use existing directory for extracted files
    target_dir="./$clean_dir/docs/" 
    mkdir -p "$target_dir"
    
    # Find directory names containing the 8-digit string
    matching_dirs=$(echo "$ble_dirs" | grep "$clean_dir")

    # Iterate over each matching directory found
    for match in $matching_dirs; do
        echo "Found matching directory in bfe: $match"

        # Iterate over each matching file found
        # aws s3 ls s3://$bucket/$dir_bfe$match | grep -E '(_Documents)' | while IFS= read -r file; do
        aws s3 ls s3://$bucket/$dir_bfe$match | grep -E '(_Models)' | while IFS= read -r file; do

            echo "DEBUG: [$file]"
            file_name=$(echo "$file" | awk '{print $4}')  # Extract just the file name
            echo "Found matching file: $file_name in directory $match"
         
            # Download the file
            aws s3 cp s3://$bucket/$dir_bfe$match$file_name "$target_dir$file_name"
            
            # List all the files in the downloaded zip archive and nested zip files
            list_zip_contents "$target_dir$file_name" "$target_dir"

            # dump output of listed zip contents to a log file
            
            # Uncomment the following line if you want to remove the initial zip file after listing its contents
            rm "$target_dir$file_name"
        done
    done
done
