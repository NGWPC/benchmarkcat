#!/bin/bash

# Exit on any error
set -e

# Function to log messages with timestamps
log_message() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

# Parse command line arguments
while getopts "o:l:c:s:k:" opt; do
    case $opt in
        o) OUTPUT_DIR="$OPTARG";;
        l) LOG_DIR="$OPTARG";;
        c) COLLECTIONS_FILE="$OPTARG";;
        s) S3_PATH="$OPTARG";;
        k) SKIP_EXISTING="$OPTARG";;
        ?) echo "Usage: $0 -o output_dir -l log_dir -c collections_file -s s3_path -k {true|false}"; exit 1;;
    esac
done

# Validate required arguments
if [ -z "$OUTPUT_DIR" ] || [ -z "$LOG_DIR" ] || [ -z "$COLLECTIONS_FILE" ] || [ -z "$S3_PATH" ] || [ -z "$SKIP_EXISTING" ]; then
    echo "Usage: $0 -o output_dir -l log_dir -c collections_file -s s3_path -k {true|false}"
    echo "Example: $0 -o ./output -l ./logs -c collections.txt -s s3://fimc-data/benchmark/stac-bench-cat/ -k true"
    exit 1
fi

# Validate skip_existing argument
if [ "$SKIP_EXISTING" != "true" ] && [ "$SKIP_EXISTING" != "false" ]; then
    echo "Error: -k argument must be either 'true' or 'false'"
    exit 1
fi

# Create necessary directories
mkdir -p "$OUTPUT_DIR"
mkdir -p "$LOG_DIR"

# Set up logging
timestamp=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$LOG_DIR/migration_${timestamp}.log"
touch "$LOG_FILE"

log_message "Starting STAC catalog migration process"
log_message "Skip existing assets: $SKIP_EXISTING"

# Sync catalog structure from S3 (excluding assets)
log_message "Syncing catalog structure from S3: $S3_PATH to $OUTPUT_DIR"
aws s3 sync "$S3_PATH" "$OUTPUT_DIR" --exclude "assets/*" || {
    log_message "ERROR: Failed to sync catalog from S3"
    exit 1
}

# Setup conda environment
log_message "Setting up conda environment"
source /contrib/software/miniconda/miniconda/etc/profile.d/conda.sh
conda activate bench_env || {
    log_message "Failed to activate conda environment. Creating new environment..."
    conda create --name bench_env python=3.11.5 -y
    conda activate bench_env
    pip install pystac boto3
}

# Process the STAC catalog
log_message "Processing STAC catalog"
# Convert true/false to --skip-existing flag
if [ "$SKIP_EXISTING" = "true" ]; then
    SKIP_FLAG="--skip-existing"
else
    SKIP_FLAG=""
fi

python stac_processor.py \
    --s3-catalog "$S3_PATH" \
    --output-dir "$OUTPUT_DIR" \
    --log-dir "$LOG_DIR" \
    --base-url "http://0.0.0.0:8000/" \
    $SKIP_FLAG || {
    log_message "ERROR: Failed to process STAC catalog"
    exit 1
}

# Upload to STAC API
log_message "Uploading to STAC API"

# Read collections from file
while IFS= read -r collection_id || [[ -n "$collection_id" ]]; do
    # Skip empty lines and comments
    [[ -z "$collection_id" || "$collection_id" =~ ^# ]] && continue
    
    log_message "Processing collection: $collection_id"
    
    # Delete existing collection
    log_message "Deleting existing collection: $collection_id"
    curl -X DELETE "http://0.0.0.0:8082/collections/${collection_id}" || {
        log_message "ERROR: Failed to delete collection: $collection_id"
        continue
    }
    
    # Upload new collection
    log_message "Uploading new collection: $collection_id"
    curl -X POST -H "Content-Type: application/json" \
         -d @"$OUTPUT_DIR/${collection_id}/collection.json" \
         "http://0.0.0.0:8082/collections" || {
        log_message "ERROR: Failed to upload collection: $collection_id"
        continue
    
    # Upload items
    log_message "Uploading items for collection: $collection_id"
    find "$OUTPUT_DIR/${collection_id}" -mindepth 2 -name "*.json" -print0 | \
    while IFS= read -r -d '' item_file; do
        log_message "Uploading item: $item_file"
        curl -X POST -H "Content-Type: application/json" \
             -d @"$item_file" \
             "http://0.0.0.0:8082/collections/${collection_id}/items" || {
            log_message "ERROR: Failed to upload item: $item_file"
        }
    done
done < "$COLLECTIONS_FILE"

log_message "STAC catalog migration complete"
