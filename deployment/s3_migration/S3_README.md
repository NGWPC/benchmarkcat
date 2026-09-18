# S3 Catalog Migration Guide

Migrates STAC catalog and assets from NGWPC S3 (`fimc-data`) to two dedicated OWP destination buckets.

## Overview

**Destination Structure:**
```
s3://hv-fim-dev-stac/
└── benchmark-stac/                          # STAC metadata (~23,000 files, ~200 MB)
    ├── catalog.json                         # Root catalog
    ├── ble-collection/
    │   ├── collection.json
    │   └── <item-id>/<item-id>.json
    ├── gfm-collection/
    ├── gfm-expanded-collection/
    ├── hwm-collection/
    ├── iceye-collection/
    ├── nws-fim-collection/
    ├── ripple-fim-collection/
    └── usgs-fim-collection/

s3://hv-fim-dev-data/                        # Geospatial assets (~2.08 TB)
└── benchmark/
    ├── shared-assets/                       # GPKGs, PDFs, parquet caches
    │   ├── WBDHU8_webproj.gpkg              # Shared HUC8 boundaries
    │   ├── gfm_data_readme.pdf
    │   └── *.parquet                        # Derived-asset parquet caches
    ├── backups/                             # PostgreSQL dumps
    ├── ble-collection/<item-id>/
    ├── gfm-collection/<item-id>/
    │   └── S1A_IW_GRDH_[...]/
    │       ├── *_ENSEMBLE_FLOOD_*.tif
    │       ├── *_ENSEMBLE_UNCERTAINTY_*.tif
    │       └── *_ADVFLAG_*.tif
    ├── gfm-expanded-collection/<item-id>/
    ├── hwm-collection/<item-id>/
    ├── iceye-collection/<item-id>/          # ICEYE_FSD-[...] scenes
    ├── nws-fim-collection/<item-id>/
    ├── ripple-fim-collection/<item-id>/
    └── usgs-fim-collection/<item-id>/
```

## Path Mappings

| Collection | Source Path | Destination (under `hv-fim-dev-data/benchmark/`) |
|------------|-------------|--------------------------------------------------|
| ble-collection | `benchmark/high_resolution_validation_data_ble/` | `ble-collection/` |
| ripple-fim-collection | `benchmark/ripple_v0.11.x/` | `ripple-fim-collection/` |
| hwm-collection | `benchmark/high_water_marks/usgs/` | `hwm-collection/` |
| nws-fim-collection | `hand_fim/test_cases/nws_test_cases/validation_data_nws/` | `nws-fim-collection/` |
| usgs-fim-collection | `hand_fim/test_cases/usgs_test_cases/validation_data_usgs/` | `usgs-fim-collection/` |
| gfm-collection | `benchmark/rs/gfm/` | `gfm-collection/` |
| iceye-collection | `benchmark/rs/iceye/` | `iceye-collection/` |
| gfm-expanded-collection | `benchmark/rs/PI4/` | `gfm-expanded-collection/` |

**STAC Catalog:** `benchmark/stac-bench-cat/` → `hv-fim-dev-stac/benchmark-stac/`

**Shared Assets:** GPKGs, PDFs, and parquet caches → `hv-fim-dev-data/benchmark/shared-assets/`

**Known behavior — `gfm-expanded-collection` copies some assets with no catalog item:**
`migrate_s3.py` copies the whole `benchmark/rs/PI4/` source prefix, not just the assets referenced
by a linked STAC item. 1,060 scenes (761 Canada + 299 Mexico) are deliberately excluded from the
catalog by `gfm_exp_col.py`'s `is_within_neighbor_countries()` check, so they have no STAC item
and no `collection.json` link — but their assets still exist under `rs/PI4/` and still get copied
to OWP, since the migration has no per-item filtering. Confirmed in
`docs/gfm-expanded-collection_orphan_investigation.md`.

## Prerequisites

### AWS Credentials

You need access to the source bucket and both destination buckets.

**Option 1: Named AWS Profiles**
```bash
aws configure --profile ngwpc
aws configure --profile owp

python migrate_s3.py \
  --source-bucket fimc-data \
  --stac-bucket hv-fim-dev-stac \
  --stac-prefix benchmark-stac \
  --data-bucket hv-fim-dev-data \
  --data-prefix benchmark \
  --aws-profile owp
```

**Option 2: EC2 IAM Role + Cross-Account Policy**

Run from EC2 in OWP account with an IAM role that has write access to both destination buckets and read access to `fimc-data`.

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
  --stac-bucket hv-fim-dev-stac \
  --stac-prefix benchmark-stac \
  --data-bucket hv-fim-dev-data \
  --data-prefix benchmark \
  --aws-profile your-profile \
  --generate-copy-commands \
  --dry-run --verbose
```
Verify: output should show a non-zero "Assets updated" count and list the copy-command mappings. No files are created on disk during `--dry-run`.

**2. Metadata-only inspection** — download catalog and update HREFs locally, skip upload and asset copy:
```bash
python migrate_s3.py \
  --source-bucket fimc-data \
  --stac-bucket hv-fim-dev-stac \
  --stac-prefix benchmark-stac \
  --data-bucket hv-fim-dev-data \
  --data-prefix benchmark \
  --aws-profile your-profile \
  --skip-upload
```
Verify: inspect a generated JSON to confirm HREFs point to the data bucket:
```bash
grep "href" ~/benchmark-catalog/dest_catalog/gfm-collection/collection.json | head -5
# Expected: "s3://hv-fim-dev-data/benchmark/gfm-collection/..."
```

**3. Single-collection live test** — edit `PATH_MAPPINGS` in `migrate_s3.py` to keep only one small collection (e.g., `iceye-collection`), run against test buckets, execute `copy_assets.sh`, then verify:
```bash
aws s3 ls s3://hv-fim-dev-data/benchmark/iceye-collection/ --recursive
```

### Phase 1: Download Catalog (~5 min, ~200MB)

```bash
python migrate_s3.py \
  --source-bucket fimc-data \
  --stac-bucket hv-fim-dev-stac \
  --stac-prefix benchmark-stac \
  --data-bucket hv-fim-dev-data \
  --data-prefix benchmark \
  --aws-profile your-profile \
  --skip-upload
```
Downloads ~23,000 JSON files to `~/benchmark-catalog/source_catalog/` and updates HREFs (Phase 2). Add `--generate-copy-commands` to also produce `copy_assets.sh`.

Verify:
```bash
cat ~/benchmark-catalog/dest_catalog/gfm-collection/items/*/item.json | \
  jq '.assets[].href'
```

### Phase 2: Update HREFs (~2 min)

Runs automatically with Phase 1. Transforms asset paths:

- `s3://fimc-data/benchmark/rs/gfm/dfo-4336/flood.tif`
- → `s3://hv-fim-dev-data/benchmark/gfm-collection/dfo-4336/flood.tif`

Updated catalog is saved to `~/benchmark-catalog/dest_catalog/`. A manifest of all HREF changes is written to `~/benchmark-catalog/migration_manifest.json`.

### Phase 3: Copy Assets (~8-12 hours, ~2.08 TB)

Generate the copy script, then review and execute:
```bash
python migrate_s3.py \
  --source-bucket fimc-data \
  --stac-bucket hv-fim-dev-stac \
  --stac-prefix benchmark-stac \
  --data-bucket hv-fim-dev-data \
  --data-prefix benchmark \
  --aws-profile your-profile \
  --skip-download --skip-update --skip-upload \
  --generate-copy-commands
```
```bash
~/benchmark-catalog/copy_assets.sh
```

Monitor progress:
```bash
watch -n 30 'aws s3 ls s3://hv-fim-dev-data/benchmark/ --recursive | wc -l'
```

Verify:
```bash
aws s3 ls s3://hv-fim-dev-data/benchmark/
# Should list all 8 collection prefixes and shared-assets/

aws s3 ls s3://hv-fim-dev-data/benchmark/gfm-collection/ --recursive | head -20
```

### Phase 4: Upload Catalog (~5 min)

```bash
python migrate_s3.py \
  --source-bucket fimc-data \
  --stac-bucket hv-fim-dev-stac \
  --stac-prefix benchmark-stac \
  --data-bucket hv-fim-dev-data \
  --data-prefix benchmark \
  --aws-profile your-profile \
  --skip-download --skip-update
```

Verify:
```bash
aws s3 ls s3://hv-fim-dev-stac/benchmark-stac/ --recursive | wc -l
# Expected: ~23,000

aws s3 cp s3://hv-fim-dev-stac/benchmark-stac/catalog.json - | jq '.'

aws s3 cp s3://hv-fim-dev-stac/benchmark-stac/gfm-collection/collection.json - | \
  jq '.assets[].href'
# All HREFs should show: s3://hv-fim-dev-data/benchmark/gfm-collection/...

aws s3 ls s3://hv-fim-dev-data/benchmark/shared-assets/
```

## Troubleshooting

| Problem | Solution |
|---------|----------|
| HREFs still point to old paths | Check that collection IDs in STAC match `PATH_MAPPINGS` keys. Run with `--verbose` to see HREF transformations. |
| Permission errors | Verify access: `aws s3 ls s3://fimc-data/benchmark/ --profile your-profile` and `aws s3 ls s3://hv-fim-dev-stac/ --profile your-profile`. Check identity with `aws sts get-caller-identity`. |
| `copy_assets.sh` fails partway | Re-run — `aws s3 sync` is idempotent and skips existing files. Check error output and verify permissions on both destination buckets. |
| Collections missing from destination | Verify the collection exists in source STAC and has an entry in `PATH_MAPPINGS`. Add missing mapping and re-run Phase 2. |
| Partial catalog upload | Re-run Phase 4 — upload is idempotent. |

## Resume After Failure

The migration is idempotent — safely re-run any phase:

```bash
# Catalog already downloaded?
python migrate_s3.py --source-bucket fimc-data \
  --stac-bucket hv-fim-dev-stac --stac-prefix benchmark-stac \
  --data-bucket hv-fim-dev-data --data-prefix benchmark ... --skip-download

# HREFs already updated?
python migrate_s3.py --source-bucket fimc-data \
  --stac-bucket hv-fim-dev-stac --stac-prefix benchmark-stac \
  --data-bucket hv-fim-dev-data --data-prefix benchmark ... --skip-download --skip-update

# Assets partially copied?
~/benchmark-catalog/copy_assets.sh  # Skips existing files
```

## Refreshing a collection after the initial migration

The procedure above is a one-time migration: it assumes OWP's buckets start empty. Once a
collection has already been migrated, a later refresh (e.g. picking up a new generation run for
that collection) is a different operation — some upstream items may have been removed as well as
added, and `migrate_s3.py` has no notion of "diff since last sync." This section covers refreshing
a single collection in place.

### 1. Identify what changed

Compare the item IDs currently in the source STAC collection against what's already on the STAC
bucket:

```bash
aws s3 cp s3://fimc-data/benchmark/stac-bench-cat/ripple-fim-collection/collection.json - \
  --profile ngwpc | jq -r '.links[] | select(.rel=="item") | .href' | sort > /tmp/source_items.txt

aws s3 ls s3://hv-fim-dev-stac/benchmark-stac/ripple-fim-collection/ --profile owp \
  | awk '{print $2}' | sed 's#/$##' | sort > /tmp/dest_items.txt

# Items removed upstream, still present on OWP:
comm -13 /tmp/source_items.txt /tmp/dest_items.txt
```

Items in that `comm -13` output are superseded — they no longer exist in the live catalog but are
still on OWP's buckets. Everything else is a straightforward add/update, handled by a normal sync.

### 2. Sync the collection's STAC metadata and assets

Scope both sync commands to the one collection's prefix — do not sync the bucket root. Follow
the path mapping for the collection from `PATH_MAPPINGS` in `migrate_s3.py` (e.g.
`ripple-fim-collection` maps source `benchmark/ripple_v0.11.x` → dest `ripple-fim-collection`).

`migrate_s3.py` itself has no per-collection or "refresh" flag — it always downloads and rewrites
the full catalog. For a single collection, use plain `aws s3 sync` scoped to that collection's
prefixes instead of re-running the script:

```bash
# STAC metadata (item and collection JSON) — dry run first
aws s3 sync s3://fimc-data/benchmark/stac-bench-cat/ripple-fim-collection/ \
  s3://hv-fim-dev-stac/benchmark-stac/ripple-fim-collection/ \
  --profile owp --dryrun

# Then for real
aws s3 sync s3://fimc-data/benchmark/stac-bench-cat/ripple-fim-collection/ \
  s3://hv-fim-dev-stac/benchmark-stac/ripple-fim-collection/ \
  --profile owp

# Assets — dry run first
aws s3 sync s3://fimc-data/benchmark/ripple_v0.11.x/ \
  s3://hv-fim-dev-data/benchmark/ripple-fim-collection/ \
  --profile owp --dryrun

# Then for real
aws s3 sync s3://fimc-data/benchmark/ripple_v0.11.x/ \
  s3://hv-fim-dev-data/benchmark/ripple-fim-collection/ \
  --profile owp
```

Note that any item JSON copied this way still has HREFs pointing at `fimc-data` — this plain
`aws s3 sync` does not run the HREF rewrite that `migrate_s3.py` Phase 2 performs. If the source
items don't already carry post-migration HREFs, run the affected items back through
`update_catalog_hrefs` (Phase 2 of `migrate_s3.py`) before uploading, rather than syncing them
directly.

### 3. Handle removed items

`aws s3 sync` without `--delete` only adds and updates objects — it never removes anything from
the destination. Superseded items from step 1 will still be sitting in
`hv-fim-dev-stac/benchmark-stac/<collection>/` and `hv-fim-dev-data/benchmark/<collection>/` after
the sync above. Left in place, they resolve normally and look like valid items, so a consumer of
the catalog sees two generations of extents for the same area with no way to tell which is
current.

To clear them, use `--delete` scoped to the single collection prefix on both buckets:

```bash
# Dry run — review exactly what would be deleted before running for real
aws s3 sync s3://fimc-data/benchmark/stac-bench-cat/ripple-fim-collection/ \
  s3://hv-fim-dev-stac/benchmark-stac/ripple-fim-collection/ \
  --profile owp --delete --dryrun

aws s3 sync s3://fimc-data/benchmark/ripple_v0.11.x/ \
  s3://hv-fim-dev-data/benchmark/ripple-fim-collection/ \
  --profile owp --delete --dryrun

# Then for real
aws s3 sync s3://fimc-data/benchmark/stac-bench-cat/ripple-fim-collection/ \
  s3://hv-fim-dev-stac/benchmark-stac/ripple-fim-collection/ \
  --profile owp --delete

aws s3 sync s3://fimc-data/benchmark/ripple_v0.11.x/ \
  s3://hv-fim-dev-data/benchmark/ripple-fim-collection/ \
  --profile owp --delete
```

**Warning:** `--delete` only removes objects that are absent from source *within the destination
prefix given on the command line*. Always give both buckets a collection-scoped prefix
(`benchmark-stac/<collection>/`, `benchmark/<collection>/`) as shown above. Running `--delete`
against the bucket root (`s3://hv-fim-dev-stac/` or `s3://hv-fim-dev-data/`) would compare the
*entire* source tree against the *entire* destination and remove any other collection's objects
that don't happen to exist at the same relative path in `fimc-data` — this includes
`shared-assets/` and any collection with a different source layout. Never run `--delete` unscoped.

### 4. Clear stale rows from pgSTAC

If the OWP catalog has already been loaded into pgSTAC (`load_catalog.py`, see
`../scripts/load_catalog.py` and `Deployment_Runbook.md` Phase 3), syncing S3 is not enough on its
own. `load_catalog.py` only upserts the collections and items it finds under the given catalog
directory — it never queries the database for what's already there, so it has no way to know an
item was removed upstream, and it issues no deletes. Re-running it after a refresh will pick up
new and changed items but will leave rows for the superseded items sitting in `pgstac.items`
untouched.

Before re-loading, remove the stale rows for the collection explicitly. Identify them using the
same item ID list from step 1 (`comm -13` output), then delete by collection and item ID:

```bash
docker exec -i benchmarkcat-db psql -U pgstac -d stacdb -c \
  "DELETE FROM pgstac.items WHERE collection = 'ripple-fim-collection' AND id IN ('<item-id-1>', '<item-id-2>');"
```

Then re-sync `~/stac-catalog` locally (Runbook Phase 3.1) and re-run `load_catalog.py` to load the
new/changed items for the collection.
