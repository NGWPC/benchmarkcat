# List local directories
echo "Listing local directories..."
find . -maxdepth 1 -type d -regex './[0-9].*' -printf "%f\n" | sort > local_dirs.txt

# Count local directories
local_count=$(wc -l < local_dirs.txt)
echo "Total local directories: $local_count"

# Copy directories to S3 with verbose output
#echo "Copying directories to S3..."
# find . -maxdepth 1 -type d -regex './[0-9].*' -exec sh -c 'aws s3 cp --recursive --verbose "$1" "s3://fimc-data/benchmark/stac-bench-cat/assets/ble/alldocs/$(basename "$1")/"' _ {} \;

# List S3 directories
echo "Listing directories in S3..."
aws s3 ls s3://fimc-data/benchmark/stac-bench-cat/assets/ble/alldocs/ --recursive | awk -F'/' '{print $6}' | sort > s3_dirs.txt

# Count S3 directories
s3_count=$(wc -l < s3_dirs.txt)
echo "Total S3 directories: $s3_count"

# Compare directories
echo "Comparing local and S3 directories..."
diff local_dirs.txt s3_dirs.txt

# Check for empty directories
echo "Checking for empty directories..."
find . -maxdepth 1 -type d -empty -regex './[0-9].*' -printf "%f\n" > empty_dirs.txt
empty_count=$(wc -l < empty_dirs.txt)
echo "Total empty directories: $empty_count"

if [ $empty_count -gt 0 ]; then
    echo "Empty directories found:"
    cat empty_dirs.txt
else
    echo "No empty directories found."
fi

