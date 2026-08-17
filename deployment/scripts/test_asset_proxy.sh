#!/bin/bash
################################################################################
# Test Asset-Proxy Service
#
# Directly tests the asset-proxy service with sample S3 paths
################################################################################

set -euo pipefail

echo "========================================"
echo "Asset-Proxy Service Test"
echo "========================================"
echo ""

# Function to test URL
test_url() {
    local url="$1"
    local description="$2"

    echo "Testing: $description"
    echo "URL: $url"
    echo ""

    # Get response code and headers
    echo "Response headers:"
    curl -sI -L "$url" 2>&1 | head -20
    echo ""

    # Try to get content type
    CONTENT_TYPE=$(curl -sI -L "$url" 2>&1 | grep -i "content-type" | head -1 || echo "No content-type")
    echo "Content-Type: $CONTENT_TYPE"
    echo ""

    # Check final status
    FINAL_STATUS=$(curl -s -o /dev/null -w "%{http_code}" -L "$url" 2>&1)
    if [ "$FINAL_STATUS" = "200" ]; then
        echo "✓ SUCCESS - Asset accessible (HTTP 200)"
    else
        echo "✗ FAILED - HTTP $FINAL_STATUS"
    fi
    echo ""
    echo "----------------------------------------"
    echo ""
}

# Test 1: Health check
echo "Test 1: Asset-Proxy Health Check"
echo "---------------------------------"
if curl -s http://localhost:8083/health | grep -q "ok"; then
    echo "✓ Asset-proxy is running"
    curl -s http://localhost:8083/health
    echo ""
else
    echo "✗ Asset-proxy NOT responding"
    echo ""
    echo "Checking if container is running..."
    docker ps | grep asset-proxy || echo "Container not running!"
    echo ""
    echo "Recent logs:"
    docker logs --tail 30 benchmarkcat-asset-proxy 2>&1 || echo "Cannot get logs"
    exit 1
fi
echo ""
echo "----------------------------------------"
echo ""

# Test 2: AWS credentials
echo "Test 2: AWS Credentials Check"
echo "------------------------------"
if docker exec benchmarkcat-asset-proxy python3 -c "import boto3; print('Region:', boto3.Session().region_name); print('Identity:', boto3.client('sts').get_caller_identity())" 2>/dev/null; then
    echo "✓ AWS credentials working inside container"
else
    echo "✗ AWS credentials NOT working"
    echo "Check IAM instance profile"
fi
echo ""
echo "----------------------------------------"
echo ""

# Test 3: Get sample asset from database
echo "Test 3: Get Sample Asset from Database"
echo "---------------------------------------"

# Get database password
if [ -f /opt/benchmarkcat/.db_password ]; then
    PGPASSWORD=$(cat /opt/benchmarkcat/.db_password)
else
    PGPASSWORD=$(grep POSTGRES_PASSWORD /var/log/benchmarkcat/bootstrap.log | head -1 | cut -d'=' -f2 | tr -d ' ')
fi
export PGPASSWORD

SAMPLE_ASSET=$(docker exec -i benchmarkcat-db psql -U pgstac -d stacdb -t <<'SQL'
SELECT
    asset_key,
    asset_value->>'href' as href
FROM pgstac.items,
LATERAL jsonb_each(content->'assets') AS assets(asset_key, asset_value)
WHERE asset_value->>'href' IS NOT NULL
LIMIT 1;
SQL
)

if [ -n "$SAMPLE_ASSET" ]; then
    echo "Sample asset from database:"
    echo "$SAMPLE_ASSET"
    echo ""

    # Extract href
    ASSET_HREF=$(echo "$SAMPLE_ASSET" | awk '{print $3}')

    if [[ "$ASSET_HREF" == s3://* ]]; then
        # Parse S3 URI
        BUCKET=$(echo "$ASSET_HREF" | sed 's|s3://||' | cut -d'/' -f1)
        KEY=$(echo "$ASSET_HREF" | sed 's|s3://||' | cut -d'/' -f2-)

        echo "Parsed S3 URI:"
        echo "  Bucket: $BUCKET"
        echo "  Key: $KEY"
        echo ""

        # Test direct S3 access
        echo "Testing direct S3 access..."
        if aws s3 ls "s3://$BUCKET/$KEY" --request-payer requester 2>&1 | grep -q "$KEY"; then
            echo "✓ File exists in S3"
        else
            echo "✗ Cannot access file in S3"
        fi
        echo ""

        # Test through proxy
        PROXY_URL="http://localhost:8083/s3/$BUCKET/$KEY"
        echo "Testing through proxy..."
        test_url "$PROXY_URL" "Asset via proxy"

    elif [[ "$ASSET_HREF" == http://localhost:8083/s3/* ]]; then
        echo "Asset already uses proxy URL"
        test_url "$ASSET_HREF" "Existing proxy URL"

    elif [[ "$ASSET_HREF" == http://*:8083/s3/* ]]; then
        echo "Asset uses external proxy URL"
        # Try localhost version
        LOCAL_URL=$(echo "$ASSET_HREF" | sed 's|http://[^:]*:|http://localhost:|')
        test_url "$LOCAL_URL" "Proxy URL (localhost version)"
    fi
else
    echo "✗ No assets found in database"
fi
echo ""

echo "========================================"
echo "Test Complete"
echo "========================================"
echo ""
echo "Next Steps:"
echo ""
echo "If assets are S3 URIs (s3://...):"
echo "  → Run: "
echo "  → export HOST_IP=\$(hostname -I | awk '{print \$1}')"
echo "  → export PGPASSWORD=\$(grep POSTGRES_PASSWORD /var/log/benchmarkcat/bootstrap.log | sed 's/.*POSTGRES_PASSWORD=//')"
echo "  → python3 rewrite_asset_urls.py --proxy-url http://\${HOST_IP}:8083 --dry-run"
echo ""
echo "If proxy URLs fail:"
echo "  → Check docker logs: docker logs benchmarkcat-asset-proxy"
echo "  → Verify IAM role has S3 read permissions"
echo "  → Check security group allows port 8083"
echo ""
