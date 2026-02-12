#!/bin/bash

# Simple S3 Migration - Complete Workflow
# This script runs all phases of the migration with user prompts

set -e

# Configuration
SOURCE_BUCKET="fimc-data"
DEST_BUCKET="owp-benchmark"
AWS_PROFILE="${AWS_PROFILE:-}"  # Use environment variable or empty
WORKING_DIR="${HOME}/benchmark-catalog"

# Color output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo -e "${BLUE}================================${NC}"
echo -e "${BLUE}S3 Simple Migration${NC}"
echo -e "${BLUE}================================${NC}"
echo ""
echo "This script will:"
echo "  1. Download STAC catalog from source"
echo "  2. Update asset HREFs to new structure"
echo "  3. Generate asset copy commands"
echo "  4. Copy assets to destination (8-12 hours)"
echo "  5. Upload updated catalog"
echo ""
echo "Source: s3://$SOURCE_BUCKET/benchmark/"
echo "Destination: s3://$DEST_BUCKET/"
echo "  - STAC: s3://$DEST_BUCKET/stac/"
echo "  - Data: s3://$DEST_BUCKET/data/"
echo ""

if [ -n "$AWS_PROFILE" ]; then
    echo "AWS Profile: $AWS_PROFILE"
else
    echo "AWS Profile: (default credentials)"
fi

echo ""

# Check if Python script exists
if [ ! -f "$SCRIPT_DIR/migrate_s3.py" ]; then
    echo -e "${RED}Error: migrate_s3.py not found${NC}"
    exit 1
fi

# ============================================================================
# Step 1: Dry Run
# ============================================================================
echo -e "${GREEN}Step 1: Dry Run (Preview Changes)${NC}"
echo "This will show what will happen without making any changes."
echo ""
read -p "Press Enter to continue or Ctrl+C to cancel..."
echo ""

PROFILE_ARG=""
if [ -n "$AWS_PROFILE" ]; then
    PROFILE_ARG="--aws-profile $AWS_PROFILE"
fi

python3 "$SCRIPT_DIR/migrate_s3.py" \
    --source-bucket "$SOURCE_BUCKET" \
    --dest-bucket "$DEST_BUCKET" \
    --working-dir "$WORKING_DIR" \
    $PROFILE_ARG \
    --dry-run

echo ""
echo -e "${YELLOW}Review the dry run output above.${NC}"
echo ""

# ============================================================================
# Step 2: Download and Update Catalog
# ============================================================================
echo -e "${GREEN}Step 2: Download Catalog and Update HREFs${NC}"
echo "This will:"
echo "  - Download STAC catalog from source (~5 min, ~200MB)"
echo "  - Update all asset HREFs to point to new structure"
echo "  - Generate asset copy script"
echo ""
read -p "Press Enter to continue or Ctrl+C to cancel..."
echo ""

python3 "$SCRIPT_DIR/migrate_s3.py" \
    --source-bucket "$SOURCE_BUCKET" \
    --dest-bucket "$DEST_BUCKET" \
    --working-dir "$WORKING_DIR" \
    $PROFILE_ARG \
    --skip-upload

echo ""
echo -e "${GREEN}Catalog downloaded and updated!${NC}"
echo ""

# ============================================================================
# Step 3: Review Updated Catalog
# ============================================================================
echo -e "${GREEN}Step 3: Review Updated Catalog${NC}"
echo "Let's check a sample item to verify HREFs were updated correctly."
echo ""

# Find a sample item
SAMPLE_ITEM=$(find "$WORKING_DIR/dest_catalog" -name "*.json" -type f | grep -v "collection.json" | grep -v "catalog.json" | head -1)

if [ -n "$SAMPLE_ITEM" ]; then
    echo "Sample item: $(basename "$SAMPLE_ITEM")"
    echo ""
    echo "Asset HREFs:"
    jq -r '.assets[].href' "$SAMPLE_ITEM" 2>/dev/null | head -5 || echo "  (Could not parse item)"
    echo ""
    echo -e "${YELLOW}Verify these HREFs point to: s3://$DEST_BUCKET/data/<collection>/...${NC}"
else
    echo -e "${YELLOW}No sample items found. Continuing...${NC}"
fi

echo ""
read -p "Do HREFs look correct? Press Enter to continue or Ctrl+C to stop..."
echo ""

# ============================================================================
# Step 4: Copy Assets
# ============================================================================
echo -e "${GREEN}Step 4: Copy Assets to Destination${NC}"
echo ""
echo -e "${YELLOW}WARNING: This will copy ~1.5TB of data${NC}"
echo -e "${YELLOW}Estimated time: 8-12 hours${NC}"
echo ""
echo "The script will run these commands:"
echo ""
cat "$WORKING_DIR/copy_assets.sh" | grep "aws s3 sync" | head -3
echo "  ..."
echo ""
echo "You can monitor progress with:"
echo "  watch -n 30 'aws s3 ls s3://$DEST_BUCKET/data/ --recursive | wc -l'"
echo ""
read -p "Type 'yes' to start asset copy: " CONFIRM

if [ "$CONFIRM" != "yes" ]; then
    echo ""
    echo -e "${YELLOW}Asset copy skipped.${NC}"
    echo "To run manually later: $WORKING_DIR/copy_assets.sh"
    echo ""
    exit 0
fi

echo ""
echo -e "${GREEN}Starting asset copy...${NC}"
echo "Started at: $(date)"
echo ""

# Run the copy script
bash "$WORKING_DIR/copy_assets.sh"

echo ""
echo -e "${GREEN}Asset copy complete!${NC}"
echo "Finished at: $(date)"
echo ""

# ============================================================================
# Step 5: Upload Catalog
# ============================================================================
echo -e "${GREEN}Step 5: Upload Updated Catalog${NC}"
echo "This will upload the updated STAC catalog to s3://$DEST_BUCKET/stac/"
echo ""
read -p "Press Enter to continue or Ctrl+C to cancel..."
echo ""

python3 "$SCRIPT_DIR/migrate_s3.py" \
    --source-bucket "$SOURCE_BUCKET" \
    --dest-bucket "$DEST_BUCKET" \
    --working-dir "$WORKING_DIR" \
    $PROFILE_ARG \
    --skip-download \
    --skip-update

echo ""
echo -e "${GREEN}Catalog uploaded!${NC}"
echo ""

# ============================================================================
# Verification
# ============================================================================
echo -e "${GREEN}Step 6: Verification${NC}"
echo ""

echo "Checking destination bucket..."
echo ""

echo "STAC files:"
if [ -n "$AWS_PROFILE" ]; then
    aws s3 ls s3://$DEST_BUCKET/stac/ --recursive --profile "$AWS_PROFILE" | wc -l
else
    aws s3 ls s3://$DEST_BUCKET/stac/ --recursive | wc -l
fi

echo ""
echo "Data files:"
if [ -n "$AWS_PROFILE" ]; then
    aws s3 ls s3://$DEST_BUCKET/data/ --recursive --profile "$AWS_PROFILE" | wc -l
else
    aws s3 ls s3://$DEST_BUCKET/data/ --recursive | wc -l
fi

echo ""
echo -e "${GREEN}================================${NC}"
echo -e "${GREEN}Migration Complete!${NC}"
echo -e "${GREEN}================================${NC}"
echo ""
echo "Destination structure:"
echo "  s3://$DEST_BUCKET/stac/          - STAC metadata"
echo "  s3://$DEST_BUCKET/data/          - Geospatial assets"
echo ""
echo "Verify catalog:"
if [ -n "$AWS_PROFILE" ]; then
    echo "  aws s3 cp s3://$DEST_BUCKET/stac/catalog.json - --profile $AWS_PROFILE | jq '.'"
else
    echo "  aws s3 cp s3://$DEST_BUCKET/stac/catalog.json - | jq '.'"
fi
echo ""
echo "View collections:"
if [ -n "$AWS_PROFILE" ]; then
    echo "  aws s3 ls s3://$DEST_BUCKET/stac/ --profile $AWS_PROFILE"
else
    echo "  aws s3 ls s3://$DEST_BUCKET/stac/"
fi
echo ""
echo "View data:"
if [ -n "$AWS_PROFILE" ]; then
    echo "  aws s3 ls s3://$DEST_BUCKET/data/ --profile $AWS_PROFILE"
else
    echo "  aws s3 ls s3://$DEST_BUCKET/data/"
fi
echo ""
