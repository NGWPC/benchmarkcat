#!/bin/bash

COLLECTION="iceye-collection"
BASE_URL="http://benchmark-stac.test.nextgenwaterprediction.com:8000"
ITEM_DIR="/home/rob.pita/projects/stac-bench-cat/${COLLECTION}"
COLLECTION_JSON="/home/rob.pita/projects/stac-bench-cat/${COLLECTION}/collection.json"

echo "=== Step 1: Delete all items first ==="
items=$(curl -s -L "${BASE_URL}/collections/${COLLECTION}/items" | jq -r '.features[]?.id // empty')

if [ -n "$items" ]; then
    echo "Found items, deleting them..."
    echo "$items" | while read -r item_id; do
        if [ -n "$item_id" ]; then
            echo "  Deleting: ${item_id}"
            # Use -L to follow redirects
            curl -s -L -X DELETE "${BASE_URL}/collections/${COLLECTION}/items/${item_id}"
        fi
    done
    echo "✓ All items deleted"
    sleep 2
else
    echo "No items found to delete"
fi

echo ""
echo "=== Step 2: Delete collection (try both URL formats) ==="

# Try without trailing slash
echo "Trying DELETE without trailing slash..."
response=$(curl -s -L -w "\n%{http_code}" -X DELETE "${BASE_URL}/collections/${COLLECTION}")
http_code=$(echo "$response" | tail -n1)
echo "HTTP Code: ${http_code}"

if [ "$http_code" != "200" ] && [ "$http_code" != "204" ]; then
    # Try with trailing slash
    echo "Trying DELETE with trailing slash..."
    response=$(curl -s -L -w "\n%{http_code}" -X DELETE "${BASE_URL}/collections/${COLLECTION}/")
    http_code=$(echo "$response" | tail -n1)
    echo "HTTP Code: ${http_code}"
fi

sleep 2

echo ""
echo "=== Step 3: Verify collection is deleted ==="
response=$(curl -s -L -w "\n%{http_code}" "${BASE_URL}/collections/${COLLECTION}")
http_code=$(echo "$response" | tail -n1)

if [ "$http_code" = "404" ]; then
    echo "✓ Collection confirmed deleted"
elif [ "$http_code" = "200" ]; then
    echo "ERROR: Collection still exists!"
    echo "Manually delete it with:"
    echo "  curl -L -X DELETE '${BASE_URL}/collections/${COLLECTION}'"
    exit 1
fi

echo ""
echo "=== Step 4: Create fresh collection ==="
if [ ! -f "$COLLECTION_JSON" ]; then
    echo "ERROR: Collection JSON not found at ${COLLECTION_JSON}"
    exit 1
fi

response=$(curl -s -L -w "\n%{http_code}" -X POST -H "Content-Type: application/json" \
    -d @"${COLLECTION_JSON}" \
    "${BASE_URL}/collections")

http_code=$(echo "$response" | tail -n1)
body=$(echo "$response" | head -n-1)

if [ "$http_code" = "200" ] || [ "$http_code" = "201" ]; then
    echo "✓ Collection created successfully"
elif [ "$http_code" = "409" ]; then
    echo "ERROR: Collection still exists (409 Conflict)"
    echo "Please manually delete at: ${BASE_URL}/collections/${COLLECTION}"
    exit 1
else
    echo "ERROR: Failed to create collection (HTTP ${http_code})"
    echo "Response: ${body}"
    exit 1
fi

sleep 2

echo ""
echo "=== Step 5: Upload items ==="
success_count=0
error_count=0

find "${ITEM_DIR}" -mindepth 2 -name "*.json" -print0 | \
while IFS= read -r -d '' item_file; do
    filename=$(basename "$item_file")
    echo "Uploading: ${filename}"
    
    response=$(curl -s -L -w "\n%{http_code}" -X POST -H "Content-Type: application/json" \
        -d @"$item_file" \
        "${BASE_URL}/collections/${COLLECTION}/items")
    
    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | head -n-1)
    
    if [ "$http_code" = "200" ] || [ "$http_code" = "201" ]; then
        echo "  ✓ Success"
        ((success_count++))
    else
        echo "  ✗ ERROR (HTTP ${http_code}): ${body}"
        ((error_count++))
    fi
done

echo ""
echo "=== Complete ==="
echo "View collection at: ${BASE_URL}/collections/${COLLECTION}"