# ICEYE STAC Schema v1.0.0

JSON Schema definitions for ICEYE flood detection data in STAC format.

## Files

- **iceye_collection.json**: Schema for ICEYE STAC Collection
- **iceye_item.json**: Schema for ICEYE STAC Items

## Collection Schema

The collection schema defines the structure for ICEYE flood detection collections, including:

- Collection metadata (title, description, license)
- Spatial and temporal extents
- Provider information (ICEYE)
- Item asset definitions for all asset types
- STAC extensions (item-assets, projection)

### Key Requirements

- **id**: Must be `iceye-collection`
- **license**: Must be `proprietary`
- **provider**: Must include ICEYE as producer/licensor/processor
- **stac_version**: Must be `1.0.0`

### Item Assets

The collection defines these item asset types:

1. **thumbnail**: PNG thumbnail image (primary/single-region events)
2. **thumbnail_\***: PNG thumbnail images for specific regions (multi-region events)
   - Pattern: Assets with keys matching `^thumbnail_` (e.g., `thumbnail_north`, `thumbnail_central`, `thumbnail_south`)
   - Used for events covering multiple geographic regions (e.g., Helene with north/central/south regions)
3. **flood_extent**: Vector flood extent (GPKG or GeoJSON)
4. **flood_depth**: Raster flood depth (GeoTIFF)
5. **building_statistics**: Vector building statistics (GPKG or GeoJSON)
6. **flood_metadata**: JSON metadata file
7. **release_notes**: PDF release notes (optional)

## Multi-Region Event Support

ICEYE events may cover multiple geographic regions, each with its own depth raster and thumbnail. The schema supports this through flexible asset naming patterns:

**Single-Region Events** (e.g., Hurricane Ian):
- One depth file: `depth.tif`
- One thumbnail: `thumbnail` asset

**Multi-Region Events** (e.g., Hurricane Helene):
- Multiple depth files: `depth_north.tif`, `depth_central.tif`, `depth_south.tif`
- Multiple thumbnails: `thumbnail_north`, `thumbnail_central`, `thumbnail_south` assets

The schema uses `patternProperties` with the pattern `^thumbnail_` to validate any thumbnail asset key that starts with "thumbnail_", providing flexibility for various regional naming schemes.

## Item Schema

The item schema defines the structure for individual ICEYE flood event items, including:

- GeoJSON Feature structure with convex hull geometry
- Event-specific properties with `iceye:` prefix
- Projection extension properties
- Asset definitions for all file types
- Flexible thumbnail asset naming for single and multi-region events

### Key Requirements

- **id**: Must match pattern `ICEYE_FSD-[0-9]+.*`
- **geometry**: Convex hull of flood extent (or null)
- **datetime**: Release date or event end date
- **stac_version**: Must be `1.0.0`

### ICEYE Properties

All ICEYE-specific properties use the `iceye:` prefix:

#### Required Properties

- **iceye:event_id**: FSD identifier (e.g., "FSD-1279")
- **iceye:release_number**: Release number (e.g., "R6")

#### Optional Properties

- **iceye:event_type**: Type of flood ("Storm Surge", "river", "flash", "coastal", "urban")
- **iceye:country**: Country name
- **iceye:country_iso**: ISO country code
- **iceye:flooded_area_km2**: Flooded area in km²
- **iceye:product_version**: ICEYE product version string
- **iceye:analysis_tier**: Analysis tier (integer ≥ 1)
- **iceye:pixel_size**: Pixel size value
- **iceye:pixel_size_unit**: Pixel size unit

#### Depth Standardization Properties

All depth measurements are standardized to inches:

- **iceye:depth_unit**: Always "inches" (standardized)
- **iceye:depth_unit_original**: Original unit ("feet", "inches", or "unknown")
- **iceye:depth_conversion_factor**: Conversion factor (1.0 or 12.0)
- **iceye:depth_conversion_note**: Human-readable explanation

#### Projection Properties

- **proj:epsg**: EPSG code (integer)
- **proj:wkt2**: WKT2 projection string

## Asset Types

### Thumbnail (Primary)
- **Asset Key**: `thumbnail`
- **Media Type**: `image/png`
- **Role**: `thumbnail`
- **Description**: Visual preview of flood extent for single-region events
- **Usage**: Used when the event has a single depth file covering the entire area

### Thumbnails (Regional)
- **Asset Keys**: `thumbnail_north`, `thumbnail_central`, `thumbnail_south`, etc.
- **Pattern**: Any key matching `^thumbnail_`
- **Media Type**: `image/png`
- **Role**: `thumbnail`
- **Description**: Visual previews for multi-region events
- **Usage**: Used when the event has multiple depth files covering different geographic regions
- **Examples**:
  - Hurricane Helene (FSD-2227): Has 3 regional thumbnails for north, central, and south regions
  - Each thumbnail corresponds to a specific depth raster file for that region

### Flood Extent
- **Media Types**: `application/geopackage+sqlite3` or `application/geo+json`
- **Role**: `data`
- **Description**: Vector polygon(s) showing flood boundaries

### Flood Depth
- **Media Type**: `image/tiff; application=geotiff`
- **Role**: `data`
- **Description**: Raster showing flood depth values (standardized to inches)
- **Note**: Original values may have been converted from feet

### Building Statistics
- **Media Types**: `application/geopackage+sqlite3` or `application/geo+json`
- **Role**: `data`
- **Description**: Vector data with building impact statistics

### Flood Metadata
- **Media Type**: `application/json`
- **Role**: `metadata`
- **Description**: ICEYE metadata in JSON format

### Release Notes
- **Media Type**: `application/pdf`
- **Role**: `metadata`
- **Description**: PDF document with release notes (when available)

## Validation

To validate STAC items and collections against these schemas:

```bash
# Validate collection
stac validate --schema schemas/iceye/v1.0.0/iceye_collection.json collection.json

# Validate item
stac validate --schema schemas/iceye/v1.0.0/iceye_item.json item.json
```

## Examples

### Collection Example

```json
{
  "stac_version": "1.0.0",
  "type": "Collection",
  "id": "iceye-collection",
  "title": "ICEYE Flood Detection Collection",
  "description": "ICEYE SAR flood detection and monitoring products",
  "license": "proprietary",
  "providers": [
    {
      "name": "ICEYE",
      "roles": ["producer", "licensor", "processor"],
      "url": "https://www.iceye.com/"
    }
  ],
  "extent": {
    "spatial": {
      "bbox": [[-180, -90, 180, 90]]
    },
    "temporal": {
      "interval": [["2022-01-01T00:00:00Z", null]]
    }
  }
}
```

### Item Example (Single-Region Event)

Hurricane Ian with single depth file:

```json
{
  "stac_version": "1.0.0",
  "type": "Feature",
  "id": "ICEYE_FSD-1279_usa_hurricane_ian_R6",
  "geometry": {
    "type": "Polygon",
    "coordinates": [[...]]
  },
  "bbox": [-82.5, 26.0, -81.0, 27.5],
  "properties": {
    "datetime": "2022-10-10T13:00:00Z",
    "iceye:event_id": "FSD-1279",
    "iceye:release_number": "R6",
    "iceye:event_type": "Storm Surge",
    "iceye:country": "USA",
    "iceye:flooded_area_km2": 8547.26,
    "iceye:depth_unit": "inches",
    "iceye:depth_unit_original": "feet",
    "iceye:depth_conversion_factor": 12.0,
    "iceye:depth_conversion_note": "Original depth values in feet multiplied by 12 to standardize to inches",
    "proj:epsg": 4326
  },
  "assets": {
    "thumbnail": {
      "href": "s3://bucket/thumbnail.png",
      "type": "image/png",
      "roles": ["thumbnail"],
      "title": "Thumbnail Image"
    },
    "flood_depth_raster": {
      "href": "s3://bucket/depth.tif",
      "type": "image/tiff; application=geotiff",
      "roles": ["data"]
    }
  }
}
```

### Item Example (Multi-Region Event)

Hurricane Helene with three regional depth files:

```json
{
  "stac_version": "1.0.0",
  "type": "Feature",
  "id": "ICEYE_FSD-2227_flood_depth_usa_helene_in_R3",
  "geometry": {
    "type": "Polygon",
    "coordinates": [[...]]
  },
  "bbox": [-84.5, 32.0, -80.0, 36.5],
  "properties": {
    "datetime": "2024-10-05T13:00:00Z",
    "iceye:event_id": "FSD-2227",
    "iceye:release_number": "R3",
    "iceye:event_type": "Storm Surge",
    "iceye:country": "USA",
    "iceye:flooded_area_km2": 12500.0,
    "iceye:depth_unit": "inches",
    "proj:epsg": 4326
  },
  "assets": {
    "thumbnail_north": {
      "href": "s3://bucket/thumbnail_north.png",
      "type": "image/png",
      "roles": ["thumbnail"],
      "title": "Thumbnail Image (North)"
    },
    "thumbnail_central": {
      "href": "s3://bucket/thumbnail_central.png",
      "type": "image/png",
      "roles": ["thumbnail"],
      "title": "Thumbnail Image (Central)"
    },
    "thumbnail_south": {
      "href": "s3://bucket/thumbnail_south.png",
      "type": "image/png",
      "roles": ["thumbnail"],
      "title": "Thumbnail Image (South)"
    },
    "flood_depth_north": {
      "href": "s3://bucket/depth_north.tif",
      "type": "image/tiff; application=geotiff",
      "roles": ["data"],
      "title": "Flood Depth (North)"
    },
    "flood_depth_central": {
      "href": "s3://bucket/depth_central.tif",
      "type": "image/tiff; application=geotiff",
      "roles": ["data"],
      "title": "Flood Depth (Central)"
    },
    "flood_depth_south": {
      "href": "s3://bucket/depth_south.tif",
      "type": "image/tiff; application=geotiff",
      "roles": ["data"],
      "title": "Flood Depth (South)"
    }
  }
}
```

## Version History

### v1.0.0 (2025-01-05)
- Initial release
- Support for flood extent, depth, and building statistics
- Depth unit standardization to inches
- Single and multi-region thumbnail support
  - Primary thumbnail for single-region events
  - Regional thumbnails (pattern: `thumbnail_*`) for multi-region events
  - Schema validation using `patternProperties` for flexible thumbnail naming
- Convex hull geometry representation
- STAC 1.0.0 compliant with Projection Extension v1.0.0
