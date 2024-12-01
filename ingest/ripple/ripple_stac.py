import os
import json
from shapely.geometry import shape, MultiPolygon, mapping
from shapely.ops import unary_union
import geopandas as gpd
import rasterio
from rasterio.features import shapes
import numpy as np
from typing import Dict, List, Tuple
import pystac
from pystac.extensions.item_assets import AssetDefinition
from pyproj import CRS

class RippleInfo:
    """Collection-specific constants and configurations"""
    assets = {
        "extent_raster": AssetDefinition.create(
            title="Flood Extent Raster",
            description="Raster of flood extent where 1 indicates flooded area",
            media_type="image/tiff; application=geotiff",
            roles=["data"]
        ),
        "domain_boundary": AssetDefinition.create(
            title="Model Domain Boundary",
            description="GeoPackage containing the model domain boundary",
            media_type="application/geopackage+sqlite3",
            roles=["data"]
        ),
        "conus_flows": AssetDefinition.create(
            title="CONUS Flow Data",
            description="CSV containing flow data for the continental US",
            media_type="text/csv",
            roles=["data"]
        )
    }

    # Column descriptions for flowfile object
    columns_list = [{
        "feature_id": {
            "Column description": "Feature ID that identifies the stream segment",
            "Column data source": "NWM 3.0 hydrofabric",
            "data_href": "https://water.noaa.gov/about/nwm"
        },
        "discharge": {
            "Column description": "Discharge in cubic meters per second (cms)",
            "Column data source": "NWM recurrance flows",
            "data_href": None
        }
    }]

class RasterHandler:
    @staticmethod
    def create_domain_geometry(raster_path: str) -> Tuple[Dict, List[float], Dict]:
        """Create a MultiPolygon geometry from raster data areas and its convex hull"""
        with rasterio.open(raster_path) as src:
            # Read the raster data
            data = src.read(1)
            # Create mask for valid data (not no_data) 
            valid_data = (data != 255)
            # Get shapes of valid data areas
            geoms = list(shapes(valid_data.astype(np.uint8), transform=src.transform))
            # Convert to shapely geometries and filter for value=1
            polygons = [shape(geom) for geom, val in geoms if val == 1]
            
            if not polygons:
                raise ValueError(f"No valid geometries found in {raster_path}")
            
            # Create MultiPolygon for model domain
            multi_polygon = MultiPolygon(polygons)
            # Get convex hull for item geometry
            convex_hull = multi_polygon.convex_hull
            # Get bbox
            bbox = list(multi_polygon.bounds)
            
            return mapping(convex_hull), bbox, mapping(multi_polygon)

    @staticmethod
    def get_wkt2_string(raster_path: str) -> str:
        """Extract WKT2 string from raster CRS"""
        with rasterio.open(raster_path) as src:
            crs = CRS.from_wkt(src.crs.wkt)
            wkt2_string = crs.to_wkt(version="WKT2_2018")
            wkt2_string = wkt2_string.replace('"', "'")
            return wkt2_string

    @staticmethod
    def calculate_extent_area(raster_path: str) -> float:
        """Calculate area of flood extent in square meters"""
        with rasterio.open(raster_path) as src:
            data = src.read(1)
            # Count pixels that are not no_data (255) and are 1
            pixel_count = np.sum((data != 255) & (data == 1))
            # Convert to area using 3m resolution
            area = pixel_count * 3 * 3  
            return area
