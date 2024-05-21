#!/bin/bash

# Function to display usage
usage() {
    echo "Usage: $0 -b <bucket> -d <dir_bfe> -n <nesting_level>"
    exit 1
}

# Parse input arguments
while getopts ":b:d:n:" opt; do
    case "${opt}" in
        b)
            bucket=${OPTARG}
            ;;
        d)
            dir_bfe=${OPTARG}
            ;;
        n)
            nesting_level=${OPTARG}
            ;;
        *)
            usage
            ;;
    esac
done

# Check if all required arguments are provided
if [ -z "${bucket}" ] || [ -z "${dir_bfe}" ] || [ -z "${nesting_level}" ]; then
    usage
fi

dirs=$(aws s3 ls s3://$bucket/$dir_bfe | awk '{print $2}')

echo "ble dirs are:"
echo $dirs

# Function to list and extract matching files in zip archives recursively
extract_zip_contents() {
    local zip_file=$1
    local target_dir=$2
    local current_level=$3
    local temp_dir=$(mktemp -d)

    echo "Listing contents of $zip_file"
    
    # List contents of the zip file
    unzip -l "$zip_file"

    # Extract matching files from the zip file
    unzip -q "$zip_file" -d "$temp_dir"
    find "$temp_dir" -type f \( -iname "*report*.pdf" -o -iname "*report*.docx" \) | while read -r file; do
        echo "Extracted file: $file"
        mv "$file" "$target_dir"
    done

    if [ "$current_level" -lt "$nesting_level" ]; then
        # Find and process nested zip files
        shopt -s globstar
        for nested_zip in "$temp_dir"/**/*.zip; do
            [ -e "$nested_zip" ] || continue
            extract_zip_contents "$nested_zip" "$target_dir" $((current_level + 1))
        done
    fi

    # Clean up
    rm -rf "$temp_dir"
}

# Iterate over each huc-8 string
for dir in $dirs; do
    echo "Processing directory: $dir"
    
    # Remove trailing slash 
    clean_dir=${dir%/}
    
    # Create or use existing directory for extracted files
    target_dir="./$clean_dir/docs/" 
    mkdir -p "$target_dir"
    
    # Find directory names containing the 8-digit string
    matching_dirs=$(echo "$dirs" | grep "$clean_dir")

    # Iterate over each matching directory found
    for match in $matching_dirs; do
        echo "Found matching directory in bfe: $match"

        # Iterate over each matching file found
        aws s3 ls s3://$bucket/$dir_bfe$match | grep -E '(_Documents)' | while IFS= read -r file; do

            echo "DEBUG: [$file]"
            file_name=$(echo "$file" | awk '{print $4}')  # Extract just the file name
            echo "Found matching file: $file_name in directory $match"

            # Create a temporary directory for the download
            temp_zip_dir=$(mktemp -d)
            temp_zip_file="$temp_zip_dir/$file_name"
         
            # Download the file to the temporary directory
            aws s3 cp s3://$bucket/$dir_bfe$match$file_name "$temp_zip_file"
            
            # List and extract matching files in the downloaded zip archive and nested zip files
            extract_zip_contents "$temp_zip_file" "$target_dir" 1
            
            # Clean up the temporary directory
            rm -rf "$temp_zip_dir"
        done
    done
done
