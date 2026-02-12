# S3 Catalog Migration Guide

Complete guide for migrating STAC catalog and assets from NGWPC S3 to OWP S3 with organized structure.

## Table of Contents

- [Overview](#overview)
- [Quick Start](#quick-start)
- [Path Mappings](#path-mappings)
- [Workflow Phases](#workflow-phases)
- [Prerequisites](#prerequisites)
- [Command Reference](#command-reference)
- [Verification](#verification)
- [Troubleshooting](#troubleshooting)
- [Post-Migration](#post-migration)

## Overview

This migration reorganizes your S3 bucket to separate STAC metadata from geospatial assets:

**Destination Structure:**
```
s3://owp-benchmark/
├── stac/                           # STAC metadata (~200MB)
│   ├── catalog.json
│   └── <collection-id>/
│       ├── collection.json
│       └── items/
│           └── <item-id>/
│               └── <item-id>.json
└── data/                           # Geospatial assets (~1.5TB)
    └── <collection-id>/
        └── <item-id>/
            └── <asset-files>
```

**Key Benefits:**
- Clear separation of metadata and assets
- Organized by collection for easy management
- Easier backups (can backup `stac/` separately)
- Better access control (different IAM policies per directory)
- Future-proof for adding new collections

## Quick Start

### Interactive Migration (Recommended)

Run the interactive script that guides you through each step:

```bash
cd deployment/s3_migration

# Set your AWS profile (optional)
export AWS_PROFILE=your-profile-name

# Run interactive migration
./run_migration.sh
```

**This will:**
1. Show dry run preview
2. Download catalog and update HREFs
3. Let you review changes
4. Copy assets to destination (~8-12 hours)
5. Upload updated catalog
6. Verify results

### Manual Migration

For more control over each phase:

```bash
# 1. Dry run (preview changes)
python migrate_s3.py \
  --source-bucket fimc-data \
  --dest-bucket owp-benchmark \
  --aws-profile your-profile \
  --dry-run

# 2. Download catalog and update HREFs
python migrate_s3.py \
  --source-bucket fimc-data \
  --dest-bucket owp-benchmark \
  --aws-profile your-profile \
  --skip-upload

# 3. Review updated catalog
cat ~/benchmark-catalog/dest_catalog/gfm-collection/items/*/item.json | \
  jq '.assets[].href'
# Should show: s3://owp-benchmark/data/gfm-collection/...

# 4. Copy assets (8-12 hours)
~/benchmark-catalog/copy_assets.sh

# 5. Upload updated catalog
python migrate_s3.py \
  --source-bucket fimc-data \
  --dest-bucket owp-benchmark \
  --aws-profile your-profile \
  --skip-download \
  --skip-update
```

## Path Mappings

The migration uses these explicit source → destination mappings:

| Collection | Source Path | Destination |
|------------|-------------|-------------|
| ble-collection | `benchmark/high_resolution_validation_data_ble/` | `data/ble-collection/` |
| ripple-fim-collection | `benchmark/ripple_fim_100/` | `data/ripple-fim-collection/` |
| hwm-collection | `benchmark/high_water_marks/usgs/` | `data/hwm-collection/` |
| nws-fim-collection | `hand_fim/test_cases/nws_test_cases/validation_data_nws/` | `data/nws-fim-collection/` |
| usgs-fim-collection | `hand_fim/test_cases/usgs_test_cases/validation_data_usgs/` | `data/usgs-fim-collection/` |
| gfm-collection | `benchmark/rs/gfm/` | `data/gfm-collection/` |
| iceye-collection | `benchmark/rs/iceye/` | `data/iceye-collection/` |
| gfm-expanded-collection | `benchmark/rs/PI4/` | `data/gfm-expanded-collection/` |

**STAC Catalog:** `benchmark/stac-bench-cat/` → `stac/`

### Customizing Mappings

To add or modify collection mappings, edit `PATH_MAPPINGS` in `migrate_s3.py`:

```python
PATH_MAPPINGS = {
    'your-new-collection': {
        'source': 'benchmark/your_data_path',
        'dest': 'data/your-new-collection'
    }
}
```

## Workflow Phases

### Phase 1: Download Catalog (~5 min, ~200MB)

Downloads all STAC JSON files from source:

```bash
aws s3 sync s3://fimc-data/benchmark/stac-bench-cat/ \
  ~/benchmark-catalog/source_catalog/ \
  --exclude "*" --include "*.json"
```

**Result:** ~22,000 JSON files saved locally

### Phase 2: Update HREFs (~2 min)

Updates all asset paths to new structure:

**Before:**
```json
{
  "href": "s3://fimc-data/benchmark/rs/gfm/dfo-4336/flood.tif"
}
```

**After:**
```json
{
  "href": "s3://owp-benchmark/data/gfm-collection/dfo-4336/flood.tif"
}
```

**What happens:**
- Collection paths mapped to new structure
- All asset HREFs updated
- Catalog structure reorganized for `stac/` directory
- Updated catalog saved to `~/benchmark-catalog/dest_catalog/`

### Phase 3: Copy Assets (~8-12 hours, ~1.5TB)

Generates and executes `copy_assets.sh` with commands like:

```bash
# gfm-collection
aws s3 sync s3://fimc-data/benchmark/rs/gfm/ \
  s3://owp-benchmark/data/gfm-collection/

# iceye-collection
aws s3 sync s3://fimc-data/benchmark/rs/iceye/ \
  s3://owp-benchmark/data/iceye-collection/

# ... and so on for each collection
```

**Result:** All assets organized by collection in `s3://owp-benchmark/data/`

**Monitor progress:**
```bash
# Count files as they're copied
watch -n 30 'aws s3 ls s3://owp-benchmark/data/ --recursive | wc -l'
```

### Phase 4: Upload Catalog (~5 min, ~200MB)

Uploads updated STAC catalog:

```bash
aws s3 sync ~/benchmark-catalog/dest_catalog/ \
  s3://owp-benchmark/stac/ \
  --exclude "*" --include "*.json"
```

**Result:** All STAC metadata in `s3://owp-benchmark/stac/`

## Prerequisites

### AWS Credentials

You need access to both source and destination buckets.

**Option 1: Named AWS Profiles**

```bash
# Configure profiles
aws configure --profile ngwpc
aws configure --profile owp

# Use in migration
python migrate_s3.py \
  --source-bucket fimc-data \
  --dest-bucket owp-benchmark \
  --aws-profile owp  # Assumes cross-account bucket policy
```

**Option 2: EC2 IAM Role + Cross-Account Policy**

Run from EC2 in OWP account with IAM role that has:
- Write access to destination bucket
- Read access to source bucket (via cross-account bucket policy)

**Source bucket policy** (on fimc-data bucket):
```json
{
  "Effect": "Allow",
  "Principal": {
    "AWS": "arn:aws:iam::OWP-ACCOUNT-ID:role/owp-ec2-role"
  },
  "Action": ["s3:GetObject", "s3:ListBucket"],
  "Resource": [
    "arn:aws:s3:::fimc-data",
    "arn:aws:s3:::fimc-data/benchmark/*",
    "arn:aws:s3:::fimc-data/hand_fim/*"
  ]
}
```

### Local Requirements

- Python 3.7+
- AWS CLI installed and configured
- ~1GB free disk space (for catalog metadata)
- jq (for verification commands)

## Command Reference

### migrate_s3.py

```bash
python migrate_s3.py [options]

Required:
  --source-bucket <bucket>         Source S3 bucket
  --dest-bucket <bucket>           Destination S3 bucket

Optional:
  --source-catalog-prefix <path>   Source catalog path (default: benchmark/stac-bench-cat)
  --working-dir <path>             Local temp directory (default: ~/benchmark-catalog)
  --aws-profile <profile>          AWS profile to use
  --generate-copy-commands         Generate asset copy script only
  --skip-download                  Skip catalog download (Phase 1)
  --skip-update                    Skip HREF update (Phase 2)
  --skip-upload                    Skip catalog upload (Phase 4)
  --dry-run                        Preview operations without changes
  --verbose                        Enable debug logging
```

### run_migration.sh

Interactive script with built-in prompts and verification steps.

```bash
./run_migration.sh

# Or set AWS profile first
export AWS_PROFILE=your-profile
./run_migration.sh
```

## Verification

After migration is complete:

### Check Catalog Structure

```bash
# Count STAC files
aws s3 ls s3://owp-benchmark/stac/ --recursive | wc -l
# Expected: ~22,000

# Check catalog exists
aws s3 cp s3://owp-benchmark/stac/catalog.json - | jq '.'

# List collections
aws s3 ls s3://owp-benchmark/stac/

# Check sample collection
aws s3 cp s3://owp-benchmark/stac/gfm-collection/collection.json - | \
  jq '.id, .extent'
```

### Check Data Structure

```bash
# Count data files
aws s3 ls s3://owp-benchmark/data/ --recursive | wc -l

# List collections in data/
aws s3 ls s3://owp-benchmark/data/

# Check specific collection
aws s3 ls s3://owp-benchmark/data/gfm-collection/ --recursive | head -20
```

### Verify Asset HREFs

```bash
# Get a sample item
aws s3 cp s3://owp-benchmark/stac/gfm-collection/items/<item-id>/<item-id>.json - | \
  jq '.assets[].href'

# All HREFs should show: s3://owp-benchmark/data/gfm-collection/...
```

### Complete Verification

```bash
# Download and run verification script (if available)
curl -s http://localhost:8082/collections | jq '.collections[].id'
```

## Troubleshooting

### Assets Not Updating

**Problem:** HREFs still point to old paths after migration

**Solution:**
- Check that collection IDs in STAC exactly match `PATH_MAPPINGS` keys
- Run with `--verbose` to see detailed HREF transformation
- Verify source paths match actual S3 structure:
  ```bash
  aws s3 ls s3://fimc-data/benchmark/rs/gfm/
  ```

### Permission Errors

**Problem:** Access denied when reading source or writing to destination

**Solution:**
```bash
# Verify source bucket access
aws s3 ls s3://fimc-data/benchmark/ --profile your-profile

# Verify destination bucket access
aws s3 ls s3://owp-benchmark/ --profile your-profile

# Check your identity
aws sts get-caller-identity --profile your-profile
```

### Copy Script Fails

**Problem:** `copy_assets.sh` fails partway through

**Solution:**
- Re-run the script (aws s3 sync is idempotent and skips existing files)
- Check for specific error messages in output
- Verify sufficient permissions on both buckets
- Ensure source paths exist:
  ```bash
  aws s3 ls s3://fimc-data/benchmark/rs/ --profile your-profile
  ```

### Missing Collections

**Problem:** Some collections missing from destination

**Solution:**
- Verify collection exists in source STAC catalog
- Check that collection ID is in `PATH_MAPPINGS`
- Add missing mapping if needed and re-run Phase 2

### Partial Upload

**Problem:** Only some catalog files uploaded

**Solution:**
```bash
# Re-run upload (idempotent)
aws s3 sync ~/benchmark-catalog/dest_catalog/ \
  s3://owp-benchmark/stac/ \
  --profile your-profile
```

### Resume After Failure

The migration is idempotent - safely re-run any phase:

```bash
# Catalog already downloaded?
python migrate_s3.py ... --skip-download

# HREFs already updated?
python migrate_s3.py ... --skip-download --skip-update

# Assets partially copied?
~/benchmark-catalog/copy_assets.sh  # Skips existing files
```

## Post-Migration

### Update STAC API Configuration

Update your environment file:

```bash
# In /opt/benchmarkcat/.env
STAC_S3_BUCKET=owp-benchmark
STAC_S3_PREFIX=stac  # Note: changed from benchmark/stac-bench-cat
```

Restart services:

```bash
sudo /opt/benchmarkcat/restart-services.sh
```

### Verify STAC API

```bash
# Check API is running
curl http://localhost:8082/ | jq '.'

# List collections
curl http://localhost:8082/collections | jq '.collections[].id'

# Check sample item
curl http://localhost:8082/collections/gfm-collection/items?limit=1 | \
  jq '.features[0].assets[].href'
# Should show: s3://owp-benchmark/data/gfm-collection/...
```

### Test GDAL VSI Access

Verify that geospatial tools can read assets via S3:

```bash
# Get an asset URL from STAC
ASSET_URL=$(curl -s "http://localhost:8082/collections/gfm-collection/items?limit=1" | \
  jq -r '.features[0].assets | .[keys[0]].href')

echo "Testing: $ASSET_URL"

# Test with GDAL
docker run --rm \
  -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
  -e AWS_REGION=us-east-1 \
  osgeo/gdal:alpine-small-latest \
  gdalinfo /vsis3/${ASSET_URL#s3://}
```

### Set Up Backups

Since metadata is now separated, you can backup just the STAC catalog:

```bash
# Backup STAC metadata (small, ~200MB)
aws s3 sync s3://owp-benchmark/stac/ \
  s3://owp-benchmark-backup/stac/ \
  --profile your-profile

# Schedule regular backups
# Add to crontab:
# 0 2 * * 0 aws s3 sync s3://owp-benchmark/stac/ s3://owp-benchmark-backup/stac/
```

### Update Documentation

Update any references to S3 paths in your documentation:
- Old: `s3://fimc-data/benchmark/stac-bench-cat/`
- New: `s3://owp-benchmark/stac/`

## Time and Cost Estimates

### Time

| Phase | Duration | Size |
|-------|----------|------|
| Download catalog | 5 min | ~200MB |
| Update HREFs | 2 min | - |
| Copy assets | 8-12 hours | ~1.5TB |
| Upload catalog | 5 min | ~200MB |
| **Total** | **~12 hours** | **~1.5TB** |

*S3-to-S3 copy times vary by region and performance*
