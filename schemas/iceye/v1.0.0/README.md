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

1. **thumbnail**: PNG thumbnail image
2. **flood_extent**: Vector flood extent (GPKG or GeoJSON)
3. **flood_depth**: Raster flood depth (GeoTIFF)
4. **building_statistics**: Vector building statistics (GPKG or GeoJSON)
5. **flood_metadata**: JSON metadata file
6. **release_notes**: PDF release notes (optional)

## Item Schema

The item schema defines the structure for individual ICEYE flood event items, including:

- GeoJSON Feature structure with convex hull geometry
- Event-specific properties with `iceye:` prefix
- Projection extension properties
- Asset definitions for all file types

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

### Thumbnail
- **Media Type**: `image/png`
- **Role**: `thumbnail`
- **Description**: Visual preview of flood extent

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

### Item Example

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
      "roles": ["thumbnail"]
    },
    "flood_depth_raster": {
      "href": "s3://bucket/depth.tif",
      "type": "image/tiff; application=geotiff",
      "roles": ["data"]
    }
  }
}
```

## Version History

### v1.0.0 (2025-01-05)
- Initial release
- Support for flood extent, depth, and building statistics
- Depth unit standardization to inches
- Thumbnail generation support
- Convex hull geometry representation
