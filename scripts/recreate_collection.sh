#!/bin/bash

# ============================================================================
# STAC Collection Upload Script
# ============================================================================
# This script deletes and recreates a STAC collection, then uploads all items.
# The collection ID is derived from the ITEM_DIR directory name.
#
# USAGE:
#   ./recreate_collection.sh <ITEM_DIR> [BASE_URL]
#
# ARGUMENTS:
#   ITEM_DIR   - Directory containing collection.json and item subdirectories
#   BASE_URL   - STAC API base URL (default: http://localhost:8082)
#
# EXAMPLES:
#   ./recreate_collection.sh /efs/benchmark/bench_stac/gfm-collection
#   ./recreate_collection.sh /efs/benchmark/bench_stac/gfm-expanded-collection http://localhost:8082
# ============================================================================

# Check if correct number of arguments provided
if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
    echo "ERROR: Incorrect number of arguments"
    echo ""
    echo "Usage: $0 <ITEM_DIR> [BASE_URL]"
    echo ""
    echo "Example:"
    echo "  $0 /efs/benchmark/bench_stac/gfm-collection"
    echo "  $0 /efs/benchmark/bench_stac/gfm-collection http://localhost:8082"
    exit 1
fi

# Parse command-line arguments
ITEM_DIR="${1%/}"
BASE_URL="${2:-http://localhost:8082}"

# Derive collection ID and collection.json path from ITEM_DIR
COLLECTION=$(basename "$ITEM_DIR")
COLLECTION_JSON="${ITEM_DIR}/collection.json"

# Log file in the same directory as this script
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_FILE="${SCRIPT_DIR}/recreate_collection.log"

log() {
    local msg="$(date '+%Y-%m-%d %H:%M:%S') - $1"
    echo "$msg"
    echo "$msg" >> "$LOG_FILE"
}

# Validate arguments
if [ ! -d "$ITEM_DIR" ]; then
    echo "ERROR: ITEM_DIR does not exist or is not a directory: ${ITEM_DIR}"
    exit 1
fi

if [ ! -f "$COLLECTION_JSON" ]; then
    echo "ERROR: collection.json not found at: ${COLLECTION_JSON}"
    exit 1
fi

log "Configuration:"
log "  ITEM_DIR: ${ITEM_DIR}"
log "  COLLECTION_JSON: ${COLLECTION_JSON}"
log "  COLLECTION: ${COLLECTION}"
log "  BASE_URL: ${BASE_URL}"

log "=== Step 1: Delete collection (cascade deletes items) ==="
log "Trying DELETE without trailing slash..."
response=$(curl -s -L -w "\n%{http_code}" -X DELETE "${BASE_URL}/collections/${COLLECTION}")
http_code=$(echo "$response" | tail -n1)
log "HTTP Code: ${http_code}"

if [ "$http_code" != "200" ] && [ "$http_code" != "204" ]; then
    log "Trying DELETE with trailing slash..."
    response=$(curl -s -L -w "\n%{http_code}" -X DELETE "${BASE_URL}/collections/${COLLECTION}/")
    http_code=$(echo "$response" | tail -n1)
    log "HTTP Code: ${http_code}"
fi

sleep 2

log "=== Step 2: Verify collection is deleted ==="
response=$(curl -s -L -w "\n%{http_code}" "${BASE_URL}/collections/${COLLECTION}")
http_code=$(echo "$response" | tail -n1)

if [ "$http_code" = "404" ]; then
    log "Collection confirmed deleted"
elif [ "$http_code" = "200" ]; then
    log "ERROR: Collection still exists!"
    log "Manually delete it with:"
    log "  curl -L -X DELETE '${BASE_URL}/collections/${COLLECTION}'"
    exit 1
else
    log "ERROR: Unexpected response (HTTP ${http_code}) -- check API connectivity"
    exit 1
fi

log "=== Step 3: Create fresh collection ==="
response=$(curl -s -L -w "\n%{http_code}" -X POST -H "Content-Type: application/json" \
    -d @"${COLLECTION_JSON}" \
    "${BASE_URL}/collections")

http_code=$(echo "$response" | tail -n1)
body=$(echo "$response" | head -n-1)

if [ "$http_code" = "200" ] || [ "$http_code" = "201" ]; then
    log "Collection created successfully"
elif [ "$http_code" = "409" ]; then
    log "ERROR: Collection still exists (409 Conflict)"
    log "Please manually delete at: ${BASE_URL}/collections/${COLLECTION}"
    exit 1
else
    log "ERROR: Failed to create collection (HTTP ${http_code})"
    log "Response: ${body}"
    exit 1
fi

sleep 2

log "=== Step 4: Upload items ==="
success_count=0
error_count=0

# Upload item JSONs where filename matches parent directory name
for item_subdir in "${ITEM_DIR}"/*/; do
    [ ! -d "$item_subdir" ] && continue
    dir_name=$(basename "$item_subdir")
    item_file="${item_subdir}${dir_name}.json"
    [ ! -f "$item_file" ] && continue

    response=$(curl -s -L -w "\n%{http_code}" -X PUT -H "Content-Type: application/json" \
        -d @"$item_file" \
        "${BASE_URL}/collections/${COLLECTION}/items/${dir_name}")

    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | head -n-1)

    if [ "$http_code" = "200" ] || [ "$http_code" = "201" ]; then
        log "  Uploaded: ${dir_name}"
        success_count=$((success_count + 1))
    elif [ "$http_code" = "404" ]; then
        # Item doesn't exist yet — fall back to POST to create it
        response=$(curl -s -L -w "\n%{http_code}" -X POST -H "Content-Type: application/json" \
            -d @"$item_file" \
            "${BASE_URL}/collections/${COLLECTION}/items")
        http_code=$(echo "$response" | tail -n1)
        body=$(echo "$response" | head -n-1)
        if [ "$http_code" = "200" ] || [ "$http_code" = "201" ]; then
            log "  Created: ${dir_name}"
            success_count=$((success_count + 1))
        else
            log "  FAILED: ${dir_name} (HTTP ${http_code}): ${body}"
            error_count=$((error_count + 1))
        fi
    else
        log "  FAILED: ${dir_name} (HTTP ${http_code}): ${body}"
        error_count=$((error_count + 1))
    fi
done

log "=== Complete ==="
log "Uploaded: ${success_count} items"
log "Errors: ${error_count} items"
log "Log file: ${LOG_FILE}"
log "View collection at: ${BASE_URL}/collections/${COLLECTION}"
