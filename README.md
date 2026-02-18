# BenchmarkCat

A STAC (SpatioTemporal Asset Catalog) benchmark catalog for flood inundation mapping (FIM) datasets. This repository provides tools to ingest, catalog, and serve various flood modeling and observation datasets in STAC format. The purpose of the catalog is to make it easier to find FIMs that can be used to evaluate the output of the National Water Center's (NWCs) flood models.

## Overview

BenchmarkCat creates and manages a STAC catalog containing multiple collections of flood-related geospatial data from different sources:

- **Base Level Elevation (BLE) collection** - FEMA flood extent and depth maps. Thes are the "legacy" BLE FIM's used by the Office of Water Prediction's National Water Center circa 2023.
- **Global Flood Monitoring (GFM) collection** - Sentinel 1 satellite derived flood extents created by the European Space Agencies Copernicus program. There are two GFM related benchmark collections. One collection covers data from 2015 to 2021 and uses the Dartmouth Flood Observatory collection of flood events to only select GFM data likely to be associated with very large flood events. The other collection is called "GFM expanded" contains a wider variety of inundation observations from 2021 to 2025 and uses the baseline inundation supplied by the GFM system itself to only download events likely to be inundated.
- **ICEYE collection** - proprietary, satellite derived depth and extent FIMs produced by the company ICEYE by fusing data from their private SAR satellite constellation and publicly available satellite data.
- **AHPS FIM** - NOAA Advanced Hydrologic Prediction Service Flood Inundation Mapping extents derived from HEC-RAS models. There are two collections of FIMs associated with AHPS data. One set of FIMs was produced by the National Weather Service and the other set was produced by the USGS.
- **High Water Marks (HWM) collection** - USGS field surveyed flood measurements. In contrast to the other collections the HWM surveys are collections of point measurements taken along or near the boundary of a flood event's extent (the "highwater mark").
- **Ripple collection** - extent FIM's produced by Dewberry using its ripple and flows2fim software packages. These are HEC-RAS derived FIM's.

All the collections follow a flat collection structure. That is each collection only contains items and doesn't contain sub-collections. Because the main purpose of the collection is model evaluation the most important data included with each item are the FIM observations themselves and estimates of the peak discharges present during the flood event (flowfiles). Each collection contains these two things at a minimum. Where available and when deemed useful, accessory data is also provided.

## Repository Structure

```
benchmarkcat/
├── ingest/                    # Data ingestion and STAC metadata creation modules
│   ├── utils.py              # Shared utilities (S3Utils, RasterUtils, FlowfileUtils)
│   ├── batch_utils.py        # Batch pipeline helpers (manifest I/O, parquet merge)
│   ├── ble/                  # BLE ingestion
│   ├── gfm/                  # GFM ingestion (batch_split, gfm_col, batch_merge)
│   ├── gfm_exp/              # GFM expanded ingestion (batch_split, gfm_exp_col, batch_merge)
│   ├── iceye/                # ICEYE ingestion
│   ├── ahps/                 # AHPS FIM ingestion
│   ├── hwm/                  # High water marks ingestion
│   └── ripple/               # Ripple collection ingestion
├── schemas/                   # JSON Schema definitions
│   ├── ble/v1.0.0/
│   ├── iceye/v1.0.0/         # ICEYE schemas
│   ├── gfm/v1.0.0/
│   └── common_item_metadata/
├── scripts/                   # Utility scripts
│   ├── stac_processor.py     # STAC catalog processing
│   ├── normalize_cat.py      # Catalog normalization
│   └── update_asset_links.py # Asset link updates
├── Dockerfile                 # Container image for ingest
├── setup.py                   # Package setup
└── requirements.txt           # Python dependencies

```

## Installation

### Prerequisites

- Python 3.9+
- AWS credentials configured (for S3 access)
- Required Python packages

### Setup

```bash
# Clone the repository
git clone <repository-url>
cd benchmarkcat

# Install dependencies
pip install -r requirements.txt

# Install the package in development mode
pip install -e .
```

## Usage

### Ingesting Data

We call the process of creating STAC metadata for each data source "ingestion". Each data source has its own ingestion module with a collection script. The general pattern is:

```bash
python3 -m ingest.<source>.<source>_col \
  --bucket_name <s3-bucket> \
  --asset_object_key <s3-path-to-data> \
  --catalog_path <s3-path-to-catalog> \
  --link_type uri \
  --reprocess_assets  # Optional: force reprocessing
```

#### Example: Ingest ICEYE Data

```bash
python3 -m ingest.iceye.iceye_col \
  --bucket_name fimc-data \
  --asset_object_key benchmark/rs/iceye/ \
  --catalog_path benchmark/stac-bench-cat/ \
  --link_type uri
```

#### Example: Ingest BLE Data

```bash
python3 -m ingest.ble.ble_col \
  --bucket_name fimc-data \
  --asset_object_key benchmark/high_resolution_validation_data_ble/ \
  --catalog_path benchmark/stac-bench-cat/ \
  --link_type uri
```

#### Example: Ingest GFM (DFO) Data

GFM uses Dartmouth Flood Observatory (DFO) events; data lives under `benchmark/rs/gfm/`. Supports `--workers`, `--checkpoint-every`, `--profile`, and OWP QC (use `--skip-owp-qc` to disable). Optional date filters: `--after-date`, `--before-date`, `--dates` (limit by scene acquisition date parsed from the Sentinel product name).

```bash
python3 -m ingest.gfm.gfm_col \
  --bucket_name fimc-data \
  --catalog_path benchmark/stac-bench-cat/ \
  --asset_object_key benchmark/rs/gfm/ \
  --hucs_object_key benchmark/stac-bench-cat/assets/WBDHU8_webproj.gpkg \
  --boundaries_object_key benchmark/stac-bench-cat/assets/Mexico_Canada_boundaries.gpkg \
  --derived_metadata_path benchmark/stac-bench-cat/assets/derived-asset-data/gfm_collection.parquet \
  --link_type uri \
  --workers 6 \
  --checkpoint-every 10 \
  --profile Data
```

#### Example: Ingest GFM Expanded Data

GFM expanded uses date-based PI4 scenes under `benchmark/rs/PI4/`. When OWP QC is enabled (default), items get `owp:*` properties (e.g. `owp:qc_grade`, `owp:huc_summaries`). Use `--skip-owp-qc` for faster runs without QC. Optional date filters: `--after-date`, `--before-date`, `--dates` (filter by date folder; e.g. only process `2024-01-01/` through `2024-06-30/`).

```bash
python3 -m ingest.gfm_exp.gfm_exp_col \
  --bucket_name fimc-data \
  --catalog_path benchmark/stac-bench-cat/ \
  --asset_object_key benchmark/rs/PI4/ \
  --hucs_object_key benchmark/stac-bench-cat/assets/WBDHU8_webproj.gpkg \
  --boundaries_object_key benchmark/stac-bench-cat/assets/Mexico_Canada_boundaries.gpkg \
  --derived_metadata_path benchmark/stac-bench-cat/assets/derived-asset-data/gfm_expanded_collection.parquet \
  --link_type uri \
  --workers 6 \
  --checkpoint-every 10 \
  --profile Data
```

### Docker

Build and run any ingest module in a container:

- GFM
```bash
docker build -t benchmarkcat .
docker run --rm \
  -v "$HOME/.aws:/root/.aws" \
  benchmarkcat \
  ingest.gfm.gfm_col \
  --bucket_name fimc-data \
  --catalog_path benchmark/stac-bench-cat/ \
  --asset_object_key benchmark/rs/gfm/ \
  --hucs_object_key benchmark/stac-bench-cat/assets/WBDHU8_webproj.gpkg \
  --boundaries_object_key benchmark/stac-bench-cat/assets/Mexico_Canada_boundaries.gpkg \
  --derived_metadata_path benchmark/stac-bench-cat/assets/derived-asset-data/gfm_collection.parquet \
  --link_type uri \
  --workers 6 \
  --checkpoint-every 10 \
  --profile Data \
  2>&1 | tee logs/gfm_col_run_wo_batch_worker.log
```

- GFM Expanded
```bash
docker build -t benchmarkcat .
docker run --rm \
  -v "$HOME/.aws:/root/.aws" \
  benchmarkcat \
  ingest.gfm_exp.gfm_exp_col \
  --bucket_name fimc-data \
  --catalog_path benchmark/stac-bench-cat/ \
  --asset_object_key benchmark/rs/PI4/ \
  --hucs_object_key benchmark/stac-bench-cat/assets/WBDHU8_webproj.gpkg \
  --boundaries_object_key benchmark/stac-bench-cat/assets/Mexico_Canada_boundaries.gpkg \
  --derived_metadata_path benchmark/stac-bench-cat/assets/derived-asset-data/gfm_expanded_collection.parquet \
  --link_type uri \
  --workers 6 \
  --checkpoint-every 10 \
  --profile Data \
  2>&1 | tee logs/gfm_exp_col_run_wo_batch_worker.log
```


### Batch pipeline (GFM and GFM Expanded)

GFM and GFM expanded support a 3-phase batch workflow for scaling to many scenes. For local testing, run Phase 1, then Phase 2 (e.g. with `--job-index 0`), then Phase 3. All examples below use placeholder S3 paths under `benchmark/stac-bench-cat/` and `benchmark/rs/`; replace with your bucket and paths as needed.

#### GFM batch

**Phase 1 — Split** (discover DFO events, write manifest to S3; optional `--after-date`, `--before-date`, `--dates` to limit manifest to scenes in that date range by acquisition date):

```bash
python3 -m ingest.gfm.batch_split \
  --bucket_name fimc-data \
  --asset_object_key benchmark/rs/gfm/ \
  --manifest-s3-key benchmark/stac-bench-cat/batch/gfm_manifest.jsonl \
  --profile Data
```

With Docker:
```bash
docker run --rm \
  -v "$HOME/.aws:/root/.aws" \
  benchmarkcat \
  ingest.gfm.batch_split \
  --bucket_name fimc-data \
  --asset_object_key benchmark/rs/gfm/ \
  --manifest-s3-key benchmark/stac-bench-cat/batch/gfm_manifest.jsonl \
  --profile Data
```

**Phase 2 — Worker** (process a slice; for local test use `--job-index 0`):

```bash
python3 -m ingest.gfm.gfm_col \
  --mode batch-worker \
  --bucket_name fimc-data \
  --catalog_path benchmark/stac-bench-cat/ \
  --asset_object_key benchmark/rs/gfm/ \
  --hucs_object_key benchmark/stac-bench-cat/assets/WBDHU8_webproj.gpkg \
  --boundaries_object_key benchmark/stac-bench-cat/assets/Mexico_Canada_boundaries.gpkg \
  --derived_metadata_path benchmark/stac-bench-cat/assets/derived-asset-data/gfm_collection.parquet \
  --manifest-s3-key benchmark/stac-bench-cat/batch/gfm_manifest.jsonl \
  --partial-parquet-prefix benchmark/stac-bench-cat/batch/gfm_partials \
  --workers 6 \
  --job-index 0 \
  --scenes-per-job 1000 \
  --profile Data
```

With Docker:

```bash
docker run --rm \
  -v "$HOME/.aws:/root/.aws" \
  benchmarkcat \
  ingest.gfm.gfm_col \
  --mode batch-worker \
  --bucket_name fimc-data \
  --catalog_path benchmark/stac-bench-cat/ \
  --asset_object_key benchmark/rs/gfm/ \
  --hucs_object_key benchmark/stac-bench-cat/assets/WBDHU8_webproj.gpkg \
  --boundaries_object_key benchmark/stac-bench-cat/assets/Mexico_Canada_boundaries.gpkg \
  --derived_metadata_path benchmark/stac-bench-cat/assets/derived-asset-data/gfm_collection.parquet \
  --manifest-s3-key benchmark/stac-bench-cat/batch/gfm_manifest.jsonl \
  --partial-parquet-prefix benchmark/stac-bench-cat/batch/gfm_partials \
  --workers 6 \
  --job-index 0 \
  --scenes-per-job 1000 \
  --profile Data \
  2>&1 | tee logs/gfm_col_run_with_batch_worker.log
```

**Phase 3 — Merge** (concatenate partial parquets, rebuild collection.json):

```bash
python3 -m ingest.gfm.batch_merge \
  --bucket_name fimc-data \
  --partial-parquet-prefix benchmark/stac-bench-cat/batch/gfm_partials \
  --derived_metadata_path benchmark/stac-bench-cat/assets/derived-asset-data/gfm_collection.parquet \
  --catalog_path benchmark/stac-bench-cat/ \
  --asset_object_key benchmark/rs/gfm/ \
  --profile Data \
  --skip-delete-partials
```

Add `--skip-delete-partials` to keep partial parquets for debugging.

With Docker:

```bash
docker run --rm \
  -v "$HOME/.aws:/root/.aws" \
  benchmarkcat \
  ingest.gfm_exp.batch_merge \
  --bucket_name fimc-data \
  --partial-parquet-prefix scratch/biplov.bhandari/gfm-stac-test/stac/batch/gfm_exp_partials \
  --derived_metadata_path scratch/biplov.bhandari/gfm-stac-test/stac/assets/derived-asset-data/gfm_expanded_collection.parquet \
  --catalog_path scratch/biplov.bhandari/gfm-stac-test/stac/ \
  --asset_object_key scratch/biplov.bhandari/gfm-stac-test/data-gfm-exp/ \
  --profile Data \
  --skip-delete-partials \
  2>&1 | tee logs/gfm_col_run_merge.log
```

#### GFM Expanded batch

**Phase 1 — Split** (discover date/scene pairs, write manifest; GFM-exp supports `--after-date`, `--before-date`, and `--dates` (filter by date folder)):

```bash
python3 -m ingest.gfm_exp.batch_split \
  --bucket_name fimc-data \
  --asset_object_key benchmark/rs/PI4/ \
  --manifest-s3-key benchmark/stac-bench-cat/batch/gfm_exp_manifest.jsonl \
  --profile Data
```

With Docker:

```bash
docker run --rm \
  -v "$HOME/.aws:/root/.aws" \
  benchmarkcat \
  ingest.gfm_exp.batch_split \
  --bucket_name fimc-data \
  --asset_object_key benchmark/rs/PI4/ \
  --manifest-s3-key benchmark/stac-bench-cat/batch/gfm_exp_manifest.jsonl \
  --profile Data
```

**Phase 2 — Worker** (for local test use `--job-index 0`):

```bash
python3 -m ingest.gfm_exp.gfm_exp_col \
  --mode batch-worker \
  --bucket_name fimc-data \
  --catalog_path benchmark/stac-bench-cat/ \
  --asset_object_key benchmark/rs/PI4/ \
  --hucs_object_key benchmark/stac-bench-cat/assets/WBDHU8_webproj.gpkg \
  --boundaries_object_key benchmark/stac-bench-cat/assets/Mexico_Canada_boundaries.gpkg \
  --derived_metadata_path benchmark/stac-bench-cat/assets/derived-asset-data/gfm_expanded_collection.parquet \
  --manifest-s3-key benchmark/stac-bench-cat/batch/gfm_exp_manifest.jsonl \
  --partial-parquet-prefix benchmark/stac-bench-cat/batch/gfm_exp_partials \
  --workers 6 \
  --job-index 0 \
  --scenes-per-job 1000 \
  --profile Data \
  2>&1 | tee logs/gfm_exp_col_run_with_batch_worker.log
```


With Docker:

```bash
docker run --rm \
  -v "$HOME/.aws:/root/.aws" \
  benchmarkcat \
  ingest.gfm_exp.gfm_exp_col \
  --mode batch-worker \
  --bucket_name fimc-data \
  --catalog_path benchmark/stac-bench-cat/ \
  --asset_object_key benchmark/rs/PI4/ \
  --hucs_object_key benchmark/stac-bench-cat/assets/WBDHU8_webproj.gpkg \
  --boundaries_object_key benchmark/stac-bench-cat/assets/Mexico_Canada_boundaries.gpkg \
  --derived_metadata_path benchmark/stac-bench-cat/assets/derived-asset-data/gfm_expanded_collection.parquet \
  --manifest-s3-key benchmark/stac-bench-cat/batch/gfm_exp_manifest.jsonl \
  --partial-parquet-prefix benchmark/stac-bench-cat/batch/gfm_exp_partials \
  --workers 6 \
  --job-index 0 \
  --scenes-per-job 1000 \
  --profile Data \
  2>&1 | tee logs/gfm_exp_col_run_with_batch_worker.log
```

**Phase 3 — Merge**:

```bash
python3 -m ingest.gfm_exp.batch_merge \
  --bucket_name fimc-data \
  --partial-parquet-prefix benchmark/stac-bench-cat/batch/gfm_exp_partials \
  --derived_metadata_path benchmark/stac-bench-cat/assets/derived-asset-data/gfm_expanded_collection.parquet \
  --catalog_path benchmark/stac-bench-cat/ \
  --asset_object_key benchmark/rs/PI4/ \
  --profile Data \
  --skip-delete-partials
```

Add `--skip-delete-partials` for debugging.

With Docker:

```bash
docker run --rm \
  -v "$HOME/.aws:/root/.aws" \
  benchmarkcat \
  ingest.gfm_exp.batch_merge \
  --bucket_name fimc-data \
  --partial-parquet-prefix benchmark/stac-bench-cat/batch/gfm_exp_partials \
  --derived_metadata_path benchmark/stac-bench-cat/assets/derived-asset-data/gfm_expanded_collection.parquet \
  --catalog_path benchmark/stac-bench-cat/ \
  --asset_object_key benchmark/rs/PI4/ \
  --profile Data \
  --skip-delete-partials \
  2>&1 | tee logs/gfm_exp_col_run_merge.log
```

### Command Line Arguments

Common arguments across all ingestion scripts:

- `--bucket_name`: S3 bucket containing the data (default: `fimc-data`)
- `--asset_object_key`: S3 path prefix to the data directory
- `--catalog_path`: S3 path to the STAC catalog (default: `benchmark/stac-bench-cat/`)
- `--link_type`: Type of asset links - `uri` (S3 URIs) or `url` (HTTP URLs)
- `--reprocess_assets`: Force reprocessing of assets even if cached
- `--derived_metadata_path`: S3 path for cached metadata (Parquet file)
- `--profile`: AWS profile name for boto3 (optional)

GFM and GFM Expanded additionally support:

- `--workers`: Number of parallel workers (default: 1); use > 1 for local parallelism
- `--checkpoint-every`: Flush item JSONs and parquet every N scenes (default: 50); 0 = only at end
- `--skip-owp-qc`: Skip OWP QC grading and HUC-level metrics (faster runs)
- `--hucs_object_key`: S3 key for HUC8 boundaries GPKG (GFM/GFM-exp)
- `--boundaries_object_key`: S3 key for Mexico/Canada boundaries (GFM/GFM-exp; used to skip non-CONUS scenes)
- **Date filters:** `--after-date` (YYYY-MM-DD), `--before-date` (YYYY-MM-DD), `--dates` (comma-separated list). Limit processing to a date range or specific dates. **GFM-exp:** filters by date folder (top-level PI4 dirs). **GFM:** filters by scene acquisition date (parsed from Sentinel product name in path). Applied in order: after_date, then before_date, then dates list.

Batch-worker mode (GFM/GFM-exp) also uses: `--mode batch-worker`, `--manifest-s3-key`, `--partial-parquet-prefix`, `--job-index` (or `AWS_BATCH_JOB_ARRAY_INDEX`), `--scenes-per-job`

### Processing Pipeline

Each ingestion module follows this pipeline:

1. **Initialize S3 utilities** - Connect to S3 and setup helpers
2. **Create collection** - Define STAC collection metadata
3. **List data directories** - Find all event/gauge/tile directories
4. **Process assets** - For each directory:
   - Extract metadata from JSON/XML files
   - Calculate geometries from vector/raster files
   - Generate thumbnails
   - Standardize units (e.g., depth to inches)
   - Cache results in Parquet for efficiency
5. **Create STAC items** - Generate STAC items with properties and assets
6. **Validate** - Validate collection and items against schemas
7. **Upload** - Upload collection JSON to S3 catalog

### Metadata Caching

To improve performance, derived metadata (geometries, statistics, etc.) is cached in Parquet files:

- Location: `benchmark/stac-bench-cat/assets/derived-asset-data/<collection>.parquet`
- Use `--reprocess_assets` flag to force regeneration
- Cached data includes: geometries, bounding boxes, statistics, thumbnail paths, along with any other metadata that is extracted from a data source during collection creation.

## Scripts

### STAC Processor (`scripts/stac_processor.py`)

Creates a local STAC catalog from a catalog on an object store. This script also downloads the assets for each item and modifies the asset HREFs to be HTTP links meant to be served from a HTTP file server. This script is currently used in conjunction with stac-migraction.sh to create a local STAC catalog with HTTP links then load that catalog into an API:

```python
from scripts.stac_processor import STACProcessor

processor = STACProcessor(
    base_url="http://0.0.0.0:8000/",
    skip_existing=False
)

# Process a local STAC catalog
processor.process_catalog("path/to/catalog.json")
```

### Migrate benchmark STAC (`scripts/stac-migration.sh`)

This script converts a static, self-contained catalog on S3 into a catalog served via a STAC API. It was created as a way to deploy the benchmark STAC to the ParallelWorks environment. One of its main components is stac_processor.py.

For more information on using stac-migration.sh to move the static benchmark STAC on S3 to ParallelWorks see `scripts/migrating-s3-catalog-to-OE.txt`

There is also a version of the Benchmark STAC API that serves data directly from the object store. Historically this API was updated using the subset of this script that loads a catalog into a STAC API from a collection of static STAC json files.

### Normalize Catalog (`scripts/normalize_cat.py`)

Normalize all relative catalog links relative to an S3 catalog root. This is a convenience script to quickly create a catalog with absolute S3 links between catalog items from a more portable self-contained catalog with relative links.

### unzip zip files to S3 prefixs (`scripts/unzip.py`)

This script takes a directory of zip files and unzips each archives contents to its own directory inside a target destination directory. This is the script that was used to extract the data provided by ICEYE for the ICEYE collection.

### Upload a single collection to a STAC API(`scripts/recreate_collection.sh`)

This script deletes and recreates a STAC collection on a STAC API. After the collection is recreated in the API it then uploads all items to the API from a directory containing the collection's item JSON. This can be used to refresh individual collections inside the benchmark STAC API that contains S3 HREFs.

## STAC Schema

JSON Schema definitions for validating STAC collections and items are in the `schemas/` directory.

### Validating STAC Items

```bash
# Validate a collection
stac validate --schema schemas/iceye/v1.0.0/iceye_collection.json collection.json

# Validate an item
stac validate --schema schemas/iceye/v1.0.0/iceye_item.json item.json
```

### Schema Documentation

Each schema directory contains:
- `<collection>_collection.json` - Collection schema
- `<collection>_item.json` - Item schema
- `README.md` - Documentation with examples

See individual schema READMEs:
- [BLE Schema](schemas/ble/v1.0.0/)
- [ICEYE Schema](schemas/iceye/v1.0.0/)
- [USGS FIM Schema](schemas/usgs_fim/v1.0.0/)

## Adding a New Data Source

To add a new data source collection:

1. **Create module directory**: `ingest/<source>/`

2. **Create core files**:
   ```
   ingest/<source>/
   ├── __init__.py
   ├── <source>_stac.py       # Asset definitions, utilities
   ├── <source>_handle_assets.py  # Asset processing logic
   ├── <source>_col.py        # Collection generation script
   └── README.md              # Documentation
   ```

3. **Implement asset handler** (`<source>_handle_assets.py`):
   - Inherit patterns from existing handlers (BLE, ICEYE, GFM)
   - Extract metadata from source files
   - Calculate geometries and statistics
   - Organize assets by type
   - Generate thumbnails (optional)
   - Implement Parquet caching

4. **Implement collection script** (`<source>_col.py`):
   - Parse command-line arguments
   - Create STAC collection with metadata
   - Process each data unit (event/gauge/tile)
   - Create STAC items with properties and assets
   - Validate and upload to S3

5. **Create schemas**: `schemas/<source>/v1.0.0/`
   - Collection schema JSON
   - Item schema JSON
   - README with examples

6. **Document**:
   - Module README in `ingest/<source>/README.md`
   - Update main README (this file)
   - Add schema documentation

### Example Pattern (ICEYE)

Reference the ICEYE implementation as a template:
- [ingest/iceye/](ingest/iceye/) - Complete module
- [schemas/iceye/v1.0.0/](schemas/iceye/v1.0.0/) - Schemas
- Includes thumbnail generation and depth standardization features

## Key Features

### Thumbnail Generation

Many collections automatically generate PNG thumbnails from extent files for quick preview:

```python
thumbnail_s3_path = s3_utils.make_and_upload_thumbnail(
    local_extent_path,
    local_thumbnail_path,
    bucket_name,
    extent_file_key
)
```

### Depth Standardization (ICEYE)

ICEYE data automatically standardizes depth measurements to inches:

- Detects original unit (feet/inches) from metadata or raster analysis
- Applies conversion factor (12.0 for feet → inches)
- Documents conversion in STAC properties
- Preserves provenance information

### Convex Hull Geometries

Complex multipolygon flood extents are simplified to convex hulls for STAC items, reducing file size while maintaining spatial coverage.

### Flowfile Integration

Flood extent collections include associated discharge data from the NWM (National Water Model):

- Flowfiles link stream segments to discharge values
- Statistics (min, max, mean, median) computed
- Column metadata documented in STAC properties

## Shared Utilities

Shared utilities are in `ingest/utils.py` (S3Utils, RasterUtils) and `ingest/flows.py` (FlowfileUtils):

### S3Utils

```python
from ingest.utils import S3Utils

s3_utils = S3Utils(boto3.client('s3'))

# List subdirectories
dirs = s3_utils.list_subdirectories(bucket, prefix)

# List files by extension
files = s3_utils.list_files_with_extensions(bucket, prefix, ['.tif', '.gpkg'])

# Generate asset href
href, is_valid = s3_utils.generate_href(bucket, key, link_type='uri')

# Upload collection
s3_utils.update_collection(collection, 'collection-id', catalog_path, bucket)
```

### RasterUtils

```python
from ingest.utils import RasterUtils

# Count non-zero pixels
pixel_count = RasterUtils.count_pixels(raster_path)

# Get max value
max_value = RasterUtils.get_max_value(raster_path)

# Get WKT2 projection
wkt2 = RasterUtils.get_wkt2_string(raster_path)

# Create thumbnail
RasterUtils.create_preview(raster_path, preview_path)
```

### FlowfileUtils

```python
from ingest.flows import FlowfileUtils

# Download flowfiles from S3
flowfile_dfs = FlowfileUtils.download_flowfiles(bucket, keys, s3_client)

# Extract statistics
flowstats = FlowfileUtils.extract_flowstats(flowfile_dfs)

# Create flowfile object for STAC
flowfile_obj = FlowfileUtils.create_flowfile_object(ids, flowstats, columns)
```

## S3 Catalog Structure

The STAC catalog is stored in S3 with this structure:

```
s3://fimc-data/benchmark/stac-bench-cat/
├── catalog.json                           # Root catalog
├── ble-collection/
│   ├── collection.json
│   └── <huc8>-ble/
│       └── <huc8>-ble.json               # Item
├── iceye-collection/
│   ├── collection.json
│   └── <event-id>/
│       └── <event-id>.json               # Item
├── gfm-collection/
│   ├── collection.json
│   └── DFO-<event>_tile-<sentinel-id>/
│       └── DFO-<event>_tile-<sentinel-id>.json   # Item
├── gfm-expanded-collection/
│   ├── collection.json
│   └── GFM-expanded_<sentinel-id>/
│       └── GFM-expanded_<sentinel-id>.json      # Item
└── assets/
    ├── derived-asset-data/                # Cached metadata
    │   ├── ble_collection.parquet
    │   ├── iceye_collection.parquet
    │   ├── gfm_collection.parquet
    │   └── gfm_expanded_collection.parquet
    └── thumbnails/                        # Generated thumbnails
```

## Development

### Code Style

- Follow existing patterns in collection modules
- Use type hints where appropriate
- Document functions with docstrings
- Log important operations with `logging` module

### Extending Schemas

When adding new properties to schemas:

1. Add property definition to item schema
2. Update collection schema if needed
3. Document in schema README
4. Update ingestion code to populate property
5. Validate generated STAC items

## Troubleshooting

### Common Issues

**Issue**: `NoCredentialsError` when running ingestion
- **Solution**: Configure AWS credentials (`aws configure`) or copy and paste AWS credentials into the shell you are running the collection creation code from.

**Issue**: Parquet file conflicts
- **Solution**: Use `--reprocess_assets` flag to regenerate cached data

**Issue**: S3 permission errors
- **Solution**: Ensure IAM role has read/write access to bucket

**Issue**: Validation errors
- **Solution**: Check schema compatibility and required fields

### Logging

All ingestion scripts log to console. For debugging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Contributing

When contributing new data sources or features:

1. Follow the established module structure
2. Create comprehensive schemas
3. Document thoroughly (code comments, READMEs)
4. Test with real data
5. Update this README

## References

- [STAC Specification](https://stacspec.org/)
- [STAC Extensions](https://stac-extensions.github.io/)
- [PySTAC Documentation](https://pystac.readthedocs.io/)
- [FEMA BLE Data](https://www.fema.gov/flood-maps/products-tools/base-level-engineering)
- [GLOFAS GFM](https://global-flood.emergency.copernicus.eu/)
- [ICEYE Flood Monitoring](https://www.iceye.com/use-cases/flood-monitoring)

## License

[Specify license information here]

## Contact

@dylanlee

@robgpita

@biplovbhandari
