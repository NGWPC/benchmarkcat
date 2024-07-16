import os
import json
from shapely.geometry import shape, MultiPolygon, mapping
from shapely.ops import unary_union
import fiona
from fiona.transform import transform_geom
from typing import List, Tuple, Union, Dict, Any
import pystac
from pystac.extensions.item_assets import AssetDefinition
import boto3
import tempfile

class GeoJSONHandler:
    @staticmethod
    def process_shapefile(bucket_name: str, prefix: str, s3_client) -> Tuple[dict, List[float]]:
        files = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        shapefile_files = {file['Key'].split('.')[-1]: file['Key'] for file in files['Contents']}
        
        required_exts = ['shp', 'shx', 'dbf']
        for ext in required_exts:
            if ext not in shapefile_files:
                raise FileNotFoundError(f"Missing {ext} file in the specified S3 path")
        
        file_streams = {}
        for ext in shapefile_files:
            response = s3_client.get_object(Bucket=bucket_name, Key=shapefile_files[ext])
            file_streams[ext] = response['Body'].read()
        
        with tempfile.TemporaryDirectory() as tmpdirname:
            file_paths = {}
            for ext, content in file_streams.items():
                file_path = f"{tmpdirname}/file.{ext}"
                with open(file_path, 'wb') as f:
                    f.write(content)
                file_paths[ext] = file_path
            
            with fiona.open(file_paths['shp']) as src:
                crs = src.crs
                features = [feature for feature in src]
        
        geometries = []
        for feature in features:
            geom = feature['geometry']
            transformed_geom = transform_geom(crs, 'EPSG:4326', geom)
            shapely_geom = shape(transformed_geom)
            geometries.append(shapely_geom)
        
        combined_geometry = unary_union(geometries)
        if not isinstance(combined_geometry, MultiPolygon):
            combined_geometry = MultiPolygon([combined_geometry])
        
        bbox = combined_geometry.bounds
        combined_bbox = [bbox[0], bbox[1], bbox[2], bbox[3]]
        geojson_geometry = json.loads(json.dumps(mapping(combined_geometry)))
        
        return geojson_geometry, combined_bbox

class AssetUtils:
    @staticmethod
    def determine_asset_type(tile_asset: str) -> str:
        if 'EXTENT' in tile_asset:
            return 'Flood Extent'
        elif 'DEPTH' in tile_asset:
            return 'Flood Depth'
        elif 'FLOW' in tile_asset:
            return 'Flow File'
        elif 'RATING_CURVE' in tile_asset:
            return 'Rating Curve'
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

class AHPSFIMInfo:
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
            description="CSV of flow file data for a given modeled flood magnitude",
            media_type="text/csv",
            roles=["data"]
        ),
        "rating_curve": AssetDefinition.create(
            title="Rating Curve",
            description="CSV of the rating curve used to compute modeled flows",
            media_type="application/csv",
            roles=["data"]
        )
    }
    # column description for flowfile object     
    columns_list = [{
                    "feature_id": {
                        "Column description": "feature id that identifies the stream segment being modeled or measured",
                        "Column data source": None,
                        "data_href": None
                    },
                    "discharge": {
                        "Column description": "Discharge in m^3/s",
                        "Column data source": None,
                        "data_href": None
                    }
                }]

