# ICEYE Flood Detection Data Ingestion

This module ingests ICEYE flood detection and monitoring products into the STAC benchmark catalog.

## Overview

ICEYE provides synthetic aperture radar (SAR) satellite imagery for flood extent mapping, depth estimation, and building impact analysis. This ingestion module processes ICEYE flood data products and creates STAC-compliant items and collections.

## Data Structure

ICEYE data is organized by event, with each event directory containing:

```
ICEYE_FSD-{ID}_{description}_R{release}/
├── ICEYE_*_flood_extent_*.gpkg          # Flood extent vector (GeoPackage)
├── ICEYE_*_flood_extent_*.geojson       # Flood extent vector (GeoJSON)
├── ICEYE_*_flood_depth_*.tif            # Flood depth raster
├── ICEYE_*_building_statistics_*.gpkg   # Building impact statistics (GeoPackage)
├── ICEYE_*_building_statistics_*.geojson # Building impact statistics (GeoJSON)
├── ICEYE_*_flood_metadata_*.json        # Metadata JSON file
└── ICEYE_*_release_notes_*.pdf          # Release notes (optional)
```

### Event Naming Convention

Event directories follow the pattern:
- `ICEYE_FSD-{ID}_{description}_R{release_number}_{optional_suffix}`

Examples:
- `ICEYE_FSD-1279_usa_hurricane_ian_R6`
- `ICEYE_FSD-2082_flood_insights_usa_midwest_R1_imperial`

### Revision Filtering

**Important**: The collection ingestion automatically filters events to include **only the latest revision** for each FSD ID.

If multiple revisions exist on S3 for the same event (e.g., `FSD-1279_R1`, `FSD-1279_R3`, `FSD-1279_R6`), only the highest revision number (`R6`) will be processed and added to the collection. This ensures the catalog contains only the most up-to-date data for each event.

**Example**:
- Input: `FSD-1279_R1/`, `FSD-1279_R6/`, `FSD-2082_R1/`, `FSD-2227_R2/`, `FSD-2227_R3/`
- Output: `FSD-1279_R6/`, `FSD-2082_R1/`, `FSD-2227_R3/`

## Metadata Formats

ICEYE metadata JSON files come in two formats:

### Old Format (Event List)
```json
{
  "event": [{
    "uid": 1279,
    "name": "HurricaneIan - Florida (USA) - 2022",
    "description": "...",
    "event_type": "Storm Surge",
    "country": "USA",
    "country_iso": "USA",
    "flooded_area": 8547.26,
    "start_date": "2022-09-27T00:00:00+03:00",
    "end_date": "2022-10-10T15:00:00+03:00",
    "release": 6,
    "EPSG": 4326,
    "product_version": "v1.1"
  }]
}
```

### New Format (Direct Fields)
```json
{
  "flood_id": "FSD-2082",
  "release_number": 1,
  "name": "USA Midwest",
  "description": "...",
  "flood_event_type": "river",
  "countries": ["United States Of America"],
  "country_iso_codes": ["usa"],
  "flooded_area": 37.696,
  "flood_event_start_time": "2024-06-16T12:52:24Z",
  "flood_event_end_time": "2024-06-28T12:52:24Z",
  "EPSG_code": 4326,
  "product_version": "1.3"
}
```

## STAC Item Properties

Each STAC item includes the following properties:

- `title`: Event title
- `description`: Event description from metadata
- `iceye:event_id`: FSD identifier (e.g., "FSD-1279")
- `iceye:release_number`: Release number (e.g., "R6")
- `iceye:event_type`: Type of flood event (e.g., "Storm Surge", "river")
- `iceye:country`: Country name
- `iceye:country_iso`: ISO country code
- `iceye:flooded_area_km2`: Total flooded area in square kilometers
- `iceye:product_version`: ICEYE product version
- `iceye:analysis_tier`: Analysis tier (if available)
- `iceye:depth_unit`: **Standardized** depth unit (always "inches")
- `iceye:depth_unit_original`: Original depth unit from source data
- `iceye:depth_conversion_factor`: Conversion factor applied (12.0 if converted from feet)
- `iceye:depth_conversion_note`: Explanation of conversion (if applied)
- `iceye:pixel_size`: Pixel size value
- `iceye:pixel_size_unit`: Unit for pixel size (e.g., "degree")
- `proj:code`: EPSG code of the data (e.g., "EPSG:4326")
- `proj:wkt2`: WKT2 projection string
- `start_datetime`: Flood event start time
- `end_datetime`: Flood event end time
- `datetime`: The time the current release was created this can be after the event in time in the case of post processing.
## STAC Assets

Each item includes the following asset types:

1. **thumbnail**: PNG thumbnail image generated from flood extent
2. **flood_extent_gpkg** / **flood_extent_geojson**: Vector file showing flood extent
3. **flood_depth_raster**: Raster file showing flood depth values (standardized to inches)
4. **building_statistics_gpkg** / **building_statistics_geojson**: Building impact statistics
5. **flood_metadata**: JSON metadata file
6. **release_notes**: PDF release notes (when available)

## Geometry

The STAC item geometry is the **convex hull** of the flood extent multipolygon. This provides a simplified boundary that encompasses the entire flooded area while maintaining reasonable file sizes.

## Usage

### Command Line

```bash
python3 -m ingest.iceye.iceye_col \
  --bucket_name fimc-data \
  --asset_object_key benchmark/rs/iceye/ \
  --catalog_path benchmark/stac-bench-cat/ \
  --link_type uri \
  --reprocess_assets
```

### Parameters

- `--bucket_name`: S3 bucket name (default: `fimc-data`)
- `--asset_object_key`: S3 key prefix for ICEYE data (default: `benchmark/rs/iceye/`)
- `--catalog_path`: S3 path to STAC catalog (default: `benchmark/stac-bench-cat/`)
- `--link_type`: Link type for assets, either `uri` or `url` (default: `uri`)
- `--reprocess_assets`: Force reprocessing of assets even if already cached
- `--derived_metadata_path`: S3 key for derived metadata parquet file

### Programmatic Usage

```python
from ingest.iceye import ICEYEAssetHandler, create_iceye_collection
import boto3
from ingest.bench import S3Utils

# Initialize S3 utilities
s3 = boto3.client('s3')
s3_utils = S3Utils(s3)

# Create collection
collection = create_iceye_collection()

# Create asset handler
asset_handler = ICEYEAssetHandler(
    s3_utils,
    'fimc-data',
    'benchmark/stac-bench-cat/assets/derived-asset-data/iceye_collection.parquet'
)

# Process events
event_paths = s3_utils.list_subdirectories('fimc-data', 'benchmark/rs/iceye/')
for event_path in event_paths:
    asset_results = asset_handler.handle_assets(event_path)
    # Create and add item to collection...
```

## Module Structure

- **iceye_stac.py**: STAC asset definitions and utility functions
- **iceye_handle_assets.py**: Asset processing and metadata extraction
- **iceye_col.py**: Main collection generation script

## Dependencies

- pystac
- boto3
- geopandas
- shapely
- pandas
- rasterio (via ingest.bench.RasterUtils)

## Features

### Thumbnail Generation

Thumbnails are automatically generated from flood extent files (GPKG or GeoJSON) and uploaded as PNG images. The thumbnail provides a quick visual preview of the flood extent.

### Depth Unit Standardization

All depth measurements are automatically standardized to **inches** for consistency across the collection. The standardization logic:

1. **Metadata-based detection**: Checks JSON metadata for explicit depth unit
   - If "feet" or "ft" → Convert by multiplying by 12
   - If "inch" or "in" → No conversion needed

2. **Raster-based detection** (fallback if metadata unclear):
   - Analyzes maximum depth value in raster
   - Max depth ≤ 20 → Likely feet, convert by ×12
   - Max depth > 100 → Likely already inches, no conversion

3. **STAC Properties**: Conversion information is stored in item properties:
   - `iceye:depth_unit`: Always "inches" (standardized)
   - `iceye:depth_unit_original`: Original unit from source
   - `iceye:depth_conversion_factor`: 12.0 if converted from feet
   - `iceye:depth_conversion_note`: Human-readable explanation

**Example**: Hurricane Ian data (FSD-1279) has depth in feet. The system detects this from the metadata (`depth_vertical_unit: "feet"`), applies a 12× conversion factor, and documents this in the STAC properties.

## Flowfile Integration

**Note**: ICEYE data does **not** contain NWM (National Water Model) discharge or streamflow data. The `create_flowfile_object()` method is implemented but returns `None` since ICEYE is purely observational SAR-based flood detection without associated hydrologic flow data.

This is consistent with other remote sensing collections (e.g., GFM - Global Flood Monitoring) that also lack flowfile data. Collections that do include flowfiles are typically those with ground-based or model-based discharge data (BLE, AHPS, Ripple).

## Notes

- The module handles both old and new ICEYE metadata formats automatically
- Convex hull geometry is used to simplify complex multipolygon flood extents
- Both GeoPackage (.gpkg) and GeoJSON formats are supported for vector data
- Derived metadata is cached in a Parquet file to speed up re-indexing
- PDF release notes are included as metadata assets when available
- Thumbnails are generated from extent files and stored as PNG images
- All depth data is standardized to inches with conversion tracking
- No NWM flowfile data is included (SAR observation only)
