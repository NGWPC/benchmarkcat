import os
import json
from shapely.geometry import shape, MultiPolygon, mapping
from shapely.ops import unary_union
from shapely.geometry import box
from typing import List, Tuple, Union, Dict, Any
import pystac
from pystac.extensions.item_assets import AssetDefinition
import boto3
import tempfile
from ingest.bench import S3Utils, RasterUtils
import pygeohydro as pgh

class GeoJSONHandler:
    @staticmethod
    def get_huc8_geometry(huc8):
        wbd = pgh.WBD("huc8")
        huc8_geom = wbd.byids("huc8", [huc8])
        return huc8_geom.geometry.iloc[0]

class AssetUtils:
    @staticmethod
    def determine_asset_type(tile_asset: str) -> str:
        if 'extent' in tile_asset:
            return 'Flood Extent'
        elif 'depth' in tile_asset:
            return 'Flood Depth'
        elif 'flow' in tile_asset:
            return 'Flow File'
        elif 'thumbnail' in tile_asset:
            return 'Thumbnail'
        else:
            return 'Unknown'

    @staticmethod
    def get_media_type(file_name: str) -> str:
        media_types = {
            ".tif": pystac.MediaType.GEOTIFF,
            ".tiff": pystac.MediaType.GEOTIFF,
            ".csv": pystac.MediaType.TEXT,
            ".png": pystac.MediaType.PNG,
        }
        ext = os.path.splitext(file_name)[1]
        return media_types.get(ext, "application/octet-stream")

class BLEInfo:
    assets = {
        "thumbnail": AssetDefinition.create(
            title="Extent thumbnail",
            description="A quicklook showing one of the modeled flood extents for the region",
            media_type="image/png",
            roles=["thumbnail"]
        ),
        "extent_raster": AssetDefinition.create(
            title="Extent Raster",
            description="Raster of flood extent",
            media_type="image/tiff; application=geotiff",
            roles=["data"]
        ),
        "depth_raster": AssetDefinition.create(
            title="Depth Raster",
            description="Raster of flood depth",
            media_type="image/tiff; application=geotiff",
            roles=["data"]
        ),
        "flow_file": AssetDefinition.create(
            title="Flow File",
            description="CSV of flow data",
            media_type="text/csv",
            roles=["data"]
        ),
    }
    # column description for flowfile object     
    columns_list = [{
        "feature_id": {
            "Column description": "Feature ID that identifies the stream segment being modeled or measured",
            "Column data source": None,
            "data_href": None
        },
        "discharge": {
            "Column description": "Discharge in m^3/s",
            "Column data source": None,
            "data_href": None
        }
    }]
