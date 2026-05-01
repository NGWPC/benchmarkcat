# S3 Catalog Migration Guide

Migrates STAC catalog and assets from NGWPC S3 (`fimc-data`) to a destination S3 bucket with reorganized structure.

## Overview

**Destination Structure:**
```
s3://<dest-bucket>/<dest-prefix>/
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

- `--dest-prefix` scopes all content under a shared bucket (e.g., `benchmark-stac` in `hv-fim-dev-us-east-1-stac`)
- Clear separation of metadata and assets
- Organized by collection for easy management and access control
- `stac/` can be backed up independently of `data/`

## Path Mappings

| Collection | Source Path | Destination (under `<dest-prefix>/`) |
|------------|-------------|--------------------------------------|
| ble-collection | `benchmark/high_resolution_validation_data_ble/` | `data/ble-collection/` |
| ripple-fim-collection | `benchmark/ripple_fim_100/` | `data/ripple-fim-collection/` |
| hwm-collection | `benchmark/high_water_marks/usgs/` | `data/hwm-collection/` |
| nws-fim-collection | `hand_fim/test_cases/nws_test_cases/validation_data_nws/` | `data/nws-fim-collection/` |
| usgs-fim-collection | `hand_fim/test_cases/usgs_test_cases/validation_data_usgs/` | `data/usgs-fim-collection/` |
| gfm-collection | `benchmark/rs/gfm/` | `data/gfm-collection/` |
| iceye-collection | `benchmark/rs/iceye/` | `data/iceye-collection/` |
| gfm-expanded-collection | `benchmark/rs/PI4/` | `data/gfm-expanded-collection/` |

**STAC Catalog:** `benchmark/stac-bench-cat/` → `<dest-prefix>/stac/`

## Prerequisites

### AWS Credentials

You need access to both source and destination buckets.

**Option 1: Named AWS Profiles**
```bash
aws configure --profile ngwpc
aws configure --profile owp

python migrate_s3.py \
  --source-bucket fimc-data \
  --dest-bucket <dest-bucket> \
  --dest-prefix <dest-prefix> \
  --aws-profile owp
```

**Option 2: EC2 IAM Role + Cross-Account Policy**

Run from EC2 in OWP account with an IAM role that has write access to `<dest-bucket>` and read access to `fimc-data`.

**Source bucket policy** (on `fimc-data`):
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

## Procedure

All commands assume you are in `deployment/s3_migration/`.

### Pre-flight

**1. Dry run** — preview path changes without downloading, uploading, or writing any files:
```bash
python migrate_s3.py \
  --source-bucket fimc-data \
  --dest-bucket <dest-bucket> \
  --dest-prefix <dest-prefix> \
  --aws-profile your-profile \
  --generate-copy-commands \
  --dry-run --verbose
```
Verify: output should show a non-zero "Assets updated" count and list the copy-command mappings. If "Assets updated" is 0, source paths don't match `PATH_MAPPINGS`. No files are created on disk during `--dry-run`.

**2. Metadata-only inspection** — download catalog and update HREFs locally, skip upload and asset copy:
```bash
python migrate_s3.py \
  --source-bucket fimc-data \
  --dest-bucket <dest-bucket> \
  --dest-prefix <dest-prefix> \
  --aws-profile your-profile \
  --skip-upload
```
Verify: inspect a generated JSON to confirm HREFs point to the destination bucket:
```bash
grep "href" ~/benchmark-catalog/dest_catalog/gfm-collection/collection.json | head -5
# Expected: "s3://<dest-bucket>/<dest-prefix>/data/gfm-collection/..."
```

**3. Single-collection live test** — edit `PATH_MAPPINGS` in `migrate_s3.py` to keep only one small collection (e.g., `iceye-collection`), run against a test bucket, execute `copy_assets.sh`, then verify:
```bash
aws s3 ls s3://your-test-bucket/<dest-prefix>/data/iceye-collection/ --recursive
```

### Phase 1: Download Catalog (~5 min, ~200MB)

```bash
python migrate_s3.py \
  --source-bucket fimc-data \
  --dest-bucket <dest-bucket> \
  --dest-prefix <dest-prefix> \
  --aws-profile your-profile \
  --skip-upload
```
This downloads ~22,000 JSON files to `~/benchmark-catalog/source_catalog/` and updates HREFs (Phase 2) — but skips the upload. To also generate `copy_assets.sh`, add `--generate-copy-commands`.

Verify: inspect updated HREFs in `~/benchmark-catalog/dest_catalog/`:
```bash
cat ~/benchmark-catalog/dest_catalog/gfm-collection/items/*/item.json | \
  jq '.assets[].href'
```

### Phase 2: Update HREFs (~2 min)

Runs automatically with Phase 1 above. Transforms asset paths:

- `s3://fimc-data/benchmark/rs/gfm/dfo-4336/flood.tif`
- → `s3://<dest-bucket>/<dest-prefix>/data/gfm-collection/dfo-4336/flood.tif`

Updated catalog is saved to `~/benchmark-catalog/dest_catalog/`. A manifest of all HREF changes is written to `~/benchmark-catalog/migration_manifest.json` for auditing.

### Phase 3: Copy Assets (~8-12 hours, ~1.5TB)

Generate the copy script, then review and execute:
```bash
python migrate_s3.py \
  --source-bucket fimc-data \
  --dest-bucket <dest-bucket> \
  --dest-prefix <dest-prefix> \
  --aws-profile your-profile \
  --skip-download --skip-update --skip-upload \
  --generate-copy-commands
```
```bash
~/benchmark-catalog/copy_assets.sh
```

Monitor progress:
```bash
watch -n 30 'aws s3 ls s3://<dest-bucket>/<dest-prefix>/data/ --recursive | wc -l'
```

Verify:
```bash
aws s3 ls s3://<dest-bucket>/<dest-prefix>/data/
# Should list all 8 collection prefixes

aws s3 ls s3://<dest-bucket>/<dest-prefix>/data/gfm-collection/ --recursive | head -20
```

### Phase 4: Upload Catalog (~5 min)

```bash
python migrate_s3.py \
  --source-bucket fimc-data \
  --dest-bucket <dest-bucket> \
  --dest-prefix <dest-prefix> \
  --aws-profile your-profile \
  --skip-download --skip-update
```

Verify:
```bash
aws s3 ls s3://<dest-bucket>/<dest-prefix>/stac/ --recursive | wc -l
# Expected: ~22,000

aws s3 cp s3://<dest-bucket>/<dest-prefix>/stac/catalog.json - | jq '.'

aws s3 cp s3://<dest-bucket>/<dest-prefix>/stac/gfm-collection/collection.json - | \
  jq '.assets[].href'
# All HREFs should show: s3://<dest-bucket>/<dest-prefix>/data/gfm-collection/...
```

## Troubleshooting

| Problem | Solution |
|---------|----------|
| HREFs still point to old paths | Check that collection IDs in STAC match `PATH_MAPPINGS` keys. Run with `--verbose` to see HREF transformations. |
| Permission errors | Verify access: `aws s3 ls s3://fimc-data/benchmark/ --profile your-profile` and `aws s3 ls s3://<dest-bucket>/<dest-prefix>/ --profile your-profile`. Check identity with `aws sts get-caller-identity`. |
| `copy_assets.sh` fails partway | Re-run — `aws s3 sync` is idempotent and skips existing files. Check error output and verify permissions on both buckets. |
| Collections missing from destination | Verify the collection exists in source STAC and has an entry in `PATH_MAPPINGS`. Add missing mapping and re-run Phase 2. |
| Partial catalog upload | Re-run Phase 4 — upload is idempotent. |

## Resume After Failure

The migration is idempotent — safely re-run any phase:

```bash
# Catalog already downloaded?
python migrate_s3.py --source-bucket fimc-data --dest-bucket <dest-bucket> --dest-prefix <dest-prefix> ... --skip-download

# HREFs already updated?
python migrate_s3.py --source-bucket fimc-data --dest-bucket <dest-bucket> --dest-prefix <dest-prefix> ... --skip-download --skip-update

# Assets partially copied?
~/benchmark-catalog/copy_assets.sh  # Skips existing files
```
