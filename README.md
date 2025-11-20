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
│   ├── bench.py              # Shared utilities (S3Utils, RasterUtils, FlowfileUtils)
│   ├── ble/                  # BLE ingestion
│   ├── gfm/                  # GFM ingestion
│   ├── gfm_exp/              # GFM expanded ingestion
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

#### Example: Ingest GFM Expanded Data

```bash
python3 -m ingest.gfm_exp.gfm_exp_col \
  --bucket_name fimc-data \
  --asset_object_key benchmark/rs/PI4/ \
  --catalog_path benchmark/stac-bench-cat/ \
  --link_type uri
```

### Command Line Arguments

Common arguments across all ingestion scripts:

- `--bucket_name`: S3 bucket containing the data (default: `fimc-data`)
- `--asset_object_key`: S3 path prefix to the data directory
- `--catalog_path`: S3 path to the STAC catalog (default: `benchmark/stac-bench-cat/`)
- `--link_type`: Type of asset links - `uri` (S3 URIs) or `url` (HTTP URLs)
- `--reprocess_assets`: Force reprocessing of assets even if cached
- `--derived_metadata_path`: S3 path for cached metadata (Parquet file)

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

The `ingest/bench.py` module provides shared utilities:

### S3Utils

```python
from ingest.bench import S3Utils

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
from ingest.bench import RasterUtils

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
from ingest.bench import FlowfileUtils

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
├── gfm-exp-collection/
│   ├── collection.json
│   └── <date>/<sentinel-id>/
│       └── <sentinel-id>.json            # Item
└── assets/
    ├── derived-asset-data/                # Cached metadata
    │   ├── ble_collection.parquet
    │   ├── iceye_collection.parquet
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
