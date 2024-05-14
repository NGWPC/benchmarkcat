#!/bin/bash

bucket="fimc-data"
dir_bfe="benchmark/bfe_benchmark_data/"
dir_stac="benchmark/benchmark_stac_data/ble/"
dirs=$(aws s3 ls s3://$bucket/$dir_stac | awk '{print $2}' | grep -E '^[0-9]{8}/$')
ble_dirs=$(aws s3 ls s3://$bucket/$dir_bfe | awk '{print $2}')

# Iterate over each huc-8 string
for dir in $dirs; do
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
        aws s3 ls s3://$bucket/$dir_bfe$match | grep -E '(_Documents)' | while IFS= read -r file; do

            echo "DEBUG: [$file]"
            file_name=$(echo "$file" | awk '{print $4}')  # Extract just the file name
            echo "Found matching file: $file_name in directory $match"
         
            # Download the file
            aws s3 cp s3://$bucket/$dir_bfe$match$file_name $file_name
            
            # Initial unzip directly into the target directory
            find . -name "*.zip" | xargs -P 5 -I fileName sh -c 'unzip -o -d "$(dirname "fileName")/$(basename -s .zip "fileName")" "fileName"'
            aws s3 cp "$target_dir" "s3://$bucket/$dir_stac$clean_dir/docs/" --recursive
            #rm -rf $target_dir
            #rm "$file_name"  # Remove the initial zip file after extraction
        done
    done
done
