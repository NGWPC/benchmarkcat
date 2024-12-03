import os
import pdb
import json
from shapely.geometry import shape, MultiPolygon, Polygon, mapping
from shapely.ops import transform
import re
from datetime import datetime, timezone
import pystac
from pystac.extensions.item_assets import AssetDefinition
from typing import List, Tuple, Union
from pyproj import Transformer
import geopandas as gpd

class GeoJSONHandler:
    def __init__(self, transformer: Transformer):
        self.transformer = transformer

    def process_geojson(self, geojson_content: str) -> List[Union[Polygon, MultiPolygon]]:
        geojson_dict = json.loads(geojson_content)

        if geojson_dict['type'] == 'FeatureCollection':
            features = geojson_dict['features']
        elif geojson_dict['type'] == 'Feature':
            features = [geojson_dict]
        else:
            raise ValueError(f"Unsupported GeoJSON type: {geojson_dict['type']}")

        geometries = []
        for feature in features:
            geom = feature['geometry']
            shapely_geom = shape(geom)
            transformed_geom = transform(self.transformer.transform, shapely_geom)
            geometries.append(transformed_geom)
        
        return geometries

    def combine_geometries(self, geometries: List[Union[Polygon, MultiPolygon]]) -> Tuple[dict, List[float]]:
        flattened_geometries = []
        for geom in geometries:
            if isinstance(geom, Polygon):
                flattened_geometries.append(geom)
            elif isinstance(geom, MultiPolygon):
                flattened_geometries.extend([poly for poly in geom.geoms])
            else:
                raise ValueError(f"Unsupported geometry type: {type(geom)}")

        combined_geometry = MultiPolygon(flattened_geometries)
        bbox = combined_geometry.bounds
        combined_bbox = [bbox[0], bbox[1], bbox[2], bbox[3]]
        geojson_geometry = json.loads(json.dumps(mapping(combined_geometry)))
        return geojson_geometry, combined_bbox

class SentinelName:
    @staticmethod
    def extract_datetimes(sentinel_string: str) -> Tuple[datetime, datetime]:
        datetime_pattern = re.compile(r'_(\d{8}T\d{6})_(\d{8}T\d{6})_')
        match = datetime_pattern.search(sentinel_string)
        
        if match:
            start_datetime_str = match.group(1)
            end_datetime_str = match.group(2)
            start_datetime = datetime.strptime(start_datetime_str, '%Y%m%dT%H%M%S').replace(tzinfo=timezone.utc)
            end_datetime = datetime.strptime(end_datetime_str, '%Y%m%dT%H%M%S').replace(tzinfo=timezone.utc)
            return start_datetime, end_datetime
        else:
            raise ValueError("No valid datetime strings found in the input data string")

    @staticmethod
    def extract_orbit_state(filename: str) -> str:
        pattern = re.compile(r'.*?_[VH]{2}_([AD]).*')
        match = pattern.match(filename)
        
        if match:
            return match.group(1)
        else:
            raise ValueError("No valid orbit state found in the input filename")

    @staticmethod
    def extract_orbit_number(filename: str) -> str:
        pattern = re.compile(r'.*?_\d{8}T\d{6}_\d{8}T\d{6}_(\d{6})_.*')
        match = pattern.match(filename)
        
        if match:
            return match.group(1)
        else:
            raise ValueError("No valid orbit number found in the input filename")

    @staticmethod
    def extract_version_string(filepath: str) -> str:
        filename = os.path.basename(filepath)
        pattern = re.compile(r'_(V\d+M\d+R\d+)_S1')
        match = pattern.search(filename)
        
        if match:
            return match.group(1)
        else:
            raise ValueError("No valid version string found in the input filename")

class AssetUtils:
    @staticmethod
    def determine_asset_type(tile_asset: str) -> str:
        if 'ENSEMBLE_FLOOD' in tile_asset:
            return 'Observed Flood Extent'
        elif 'ENSEMBLE_OBSWATER' in tile_asset:
            return 'Observed Water Extent'
        elif 'REFERENCE_WATER_OUT' in tile_asset:
            return 'Reference Water Mask'
        elif 'ENSEMBLE_EXCLAYER' in tile_asset:
            return 'Exclusion Mask'
        elif 'ENSEMBLE_UNCERTAINTY' in tile_asset:
            return 'Likelihood Values'
        elif 'ADVFLAG' in tile_asset:
            return 'Advisory Flags'
        elif 'schedule' in tile_asset:
            return 'Schedule'
        # add second clause after or since not all footprint files have footprint in them.
        elif ('footprint' in tile_asset) or (tile_asset.startswith('S1') and tile_asset.endswith('.geojson')):
            return 'Footprint'
        elif 'metadata' in tile_asset:
            return 'Metadata'
        elif 'POP' in tile_asset:
            return 'Affected population'
        elif 'CGLS' in tile_asset:
            return 'Affected Landcover'
        elif 'thumbnail' in tile_asset:
            return 'Thumbnail'
        else:
            return 'Unknown'

    @staticmethod
    def get_media_type(file_name: str) -> str:
        media_types = {
            ".tif": pystac.MediaType.GEOTIFF,
            ".tiff": pystac.MediaType.GEOTIFF,
            ".geojson": pystac.MediaType.GEOJSON,
            ".json": pystac.MediaType.JSON,
            ".pdf": pystac.MediaType.PDF,
            ".jpeg": pystac.MediaType.JPEG,
            ".jpg": pystac.MediaType.JPEG,
            ".png": pystac.MediaType.PNG,
            ".xml": pystac.MediaType.XML,
            ".txt": pystac.MediaType.TEXT,
            ".hdf": pystac.MediaType.HDF,
            ".h5": pystac.MediaType.HDF5,
            ".jp2": pystac.MediaType.JPEG2000,
            ".kml": pystac.MediaType.KML,
            ".fgb": pystac.MediaType.FLATGEOBUF,
            ".gpkg": pystac.MediaType.GEOPACKAGE,
            ".parquet": pystac.MediaType.PARQUET,
            ".zarr": pystac.MediaType.ZARR,
            ".html": pystac.MediaType.HTML
        }
        ext = os.path.splitext(file_name)[1]
        return media_types.get(ext, "application/octet-stream")

class GFMGeometryCreator:
    def __init__(self, bucket_name: str, s3_client, gdf_geom=None):
        self.bucket_name = bucket_name
        self.s3_client = s3_client
        self.gdf_geom = gdf_geom
        self.transformer = Transformer.from_crs('EPSG:4326', 'EPSG:4326', always_xy=True)
        self.geojson_handler = GeoJSONHandler(self.transformer)

    def make_item_geom(self, key: str) -> Tuple[dict, List[float]]:
        geojson_geometries = []

        if self.gdf_geom is not None:
            geojson_geometries.append(self.gdf_geom)

        response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
        geojson_content = response['Body'].read().decode('utf-8')
        geometries = self.geojson_handler.process_geojson(geojson_content)
        geojson_geometries.extend(geometries)

        return self.geojson_handler.combine_geometries(geojson_geometries)

####### Encoded data specific to the gfm collection not derived by asset handling #######
class GFMInfo:
    # Add list of item assets
    assets = {
        "thumbnail": AssetDefinition.create(
            title="Observed flood extent thumbnail",
            description="A black and white thumbnail showing the observed water in the Sentinel-1 tile.",
            media_type="image/png",
            roles=["thumbnail"]
        ),
        "observed-flood-extent": AssetDefinition.create(
            title="Observed flood extent",
            description="Observed water extent mask. Includes negative for areas observed as non-flooded in the Sentinel-1 image. Three layers (or three bands) of JS SQL backscatter intensity.",
            media_type="image/tiff; application=geotiff",
            roles=["data"]
        ),
        "observed-water-extent": AssetDefinition.create(
            title="Observed water extent",
            description="Open water extent mask for areas of regular or non-flooded open water. Does not assess reference mask.",
            media_type="image/tiff; application=geotiff",
            roles=["data"]
        ),
        "reference-water-mask": AssetDefinition.create(
            title="Reference water mask",
            description="Reference water mask of non-flooded open water. Includes negative for areas observed as non-water. Three bands (for each of three Sentinel-1 observations serving as a reference derived from the water.",
            media_type="image/tiff; application=geotiff",
            roles=["data"]
        ),
        "exclusion-mask": AssetDefinition.create(
            title="Exclusion mask",
            description="Areas where JS-SQL flood classification can be masked (e.g., river channels).",
            media_type="image/tiff; application=geotiff",
            roles=["data"]
        ),
        "likelihood-values": AssetDefinition.create(
            title="Likelihood values",
            description="Estimated likelihood of flood classification, for all areas outside the exclusion mask.",
            media_type="image/tiff; application=geotiff",
            roles=["data"]
        ),
        "affected-landcover": AssetDefinition.create(
            title="Affected landcover",
            description="Land cover / use (e.g. artificial surfaces, agricultural areas) in flooded areas, mapped by a spatial overlay of observed flood extent and the Copernicus GLS land cover.",
            media_type="image/tiff; application=geotiff",
            roles=["data"]
        ),
        "affected-population": AssetDefinition.create(
            title="Affected population",
            description="Number of people in flooded areas, mapped by a spatial overlay of observed flood extent and gridded population, from the Copernicus GHSL project.",
            media_type="image/tiff; application=geotiff",
            roles=["data"]
        ),
        "advisory-flags": AssetDefinition.create(
            title="Advisory flags",
            description="Flags indicating potential reduced quality of flood mapping, due to prevailing environmental conditions (e.g. wind, ice, snow, dry soil), or degraded input data quality due to signal interference from other SAR missions.",
            media_type="image/tiff; application=geotiff",
            roles=["data"]
        ),
        "sentinel-1-metadata": AssetDefinition.create(
            title="Sentinel-1 metadata",
            description="Information on the acquisition parameters of the Sentinel-1 data used.",
            media_type="application/json",
            roles=["metadata"]
        ),
        "dfo-event-footprint": AssetDefinition.create(
            title="DFO event footprint",
            description="This is the DFO footprint that was identified as intersecting with the scene. This asset is not included in items in the gfm-expanded-collection that adds to the original GFM collection associated that were associated with DFO events.",
            media_type="application/geo+json",
            roles=["data"]
        )
    }

    columns_list = [{
                    "feature_id": {
                        "Column description": "feature id that identifies the stream segment being modeled or measured",
                        "Column data source": "NWM 3.0 hydrofabric",
                        "data_href": "https://water.noaa.gov/resources/downloads/nwm/NWM_channel_hydrofabric.tar.gz"
                    },
                    "discharge": {
                        "Column description": "Discharge in m^3/s",
                        "Column data source": "NWM 3.0 retrospective discharge data",
                        "data_href": "https://registry.opendata.aws/nwm-archive/"
                    }
                }]

    layers = {
        "observed_flood_extent": {
            "label": "Floodwater",
            "quantity": 1,
            "color": "#e84c78"
        },
        "observed_water_extent": {
            "label": "Water",
            "quantity": 1,
            "color": "#0584AA"
        },
        "reference_water_mask": {
            "labels": [
                {"label": "No Water", "quantity": 0, "color": "#79de13", "opacity": "0"},
                {"label": "Permanent Water Body", "quantity": 1, "color": "#004B72"},
                {"label": "Seasonal Water Body (for the current month)", "quantity": 2, "color": "#457896"}
            ]
        },
        "exclusion_mask": {
            "label": "Exclusion Mask set",
            "quantity": 1,
            "color": "#858686"
        },
        "likelihood_values": {
            "labels": [
                {"label": "High flood extent confidence", "quantity": 1, "color": "#FEF4F0"},
                {"label": "25", "quantity": 25, "color": "#F8BEA2"},
                {"label": "50", "quantity": 50, "color": "#EE7058"},
                {"label": "75", "quantity": 75, "color": "#DA1F1D"},
                {"label": "Low flood extent confidence", "quantity": 100, "color": "#6A1417"}
            ]
        },
        "affected_landcover": {
            "labels": [
                {"label": "Shrubs", "quantity": 20, "color": "#ffbb22"},
                {"label": "Herbaceous vegetation", "quantity": 30, "color": "#ffff4c"},
                {"label": "Cultivated and managed vegetation/agriculture (cropland)", "quantity": 40, "color": "#f096ff"},
                {"label": "Urban / built up", "quantity": 50, "color": "#fa0000"},
                {"label": "Bare / sparse vegetation", "quantity": 60, "color": "#b4b4b4"},
                {"label": "Snow and Ice", "quantity": 70, "color": "#f0f0f0"},
                {"label": "Herbaceous wetland", "quantity": 90, "color": "#0096a0"},
                {"label": "Moss and lichen", "quantity": 100, "color": "#fae6a0"},
                {"label": "Closed forest, evergreen needle leaf", "quantity": 111, "color": "#58481f"},
                {"label": "Closed forest, evergreen, broad leaf", "quantity": 112, "color": "#009900"},
                {"label": "Closed forest, deciduous needle leaf", "quantity": 113, "color": "#70663e"},
                {"label": "Closed forest, deciduous broad leaf", "quantity": 114, "color": "#00cc00"},
                {"label": "Closed forest, mixed", "quantity": 115, "color": "#4e751f"},
                {"label": "Closed forest, unknown", "quantity": 116, "color": "#007800"},
                {"label": "Open forest, evergreen needle leaf", "quantity": 121, "color": "#666000"},
                {"label": "Open forest, evergreen broad leaf", "quantity": 122, "color": "#8db400"},
                {"label": "Open forest, deciduous needle leaf", "quantity": 123, "color": "#8d7400"},
                {"label": "Open forest, deciduous broad leaf", "quantity": 124, "color": "#a0dc00"},
                {"label": "Open forest, mixed", "quantity": 125, "color": "#929900"},
                {"label": "Open forest, unknown", "quantity": 126, "color": "#648c00"}
            ]
        },
        "affected_population": {
            "labels": [
                {"label": "0.01", "quantity": 0.01, "color": "#F9F5C0"},
                {"label": "2", "quantity": 2, "color": "#FBC68D"},
                {"label": "4", "quantity": 4, "color": "#F18B68"},
                {"label": "8", "quantity": 8, "color": "#E45563"},
                {"label": "12", "quantity": 12, "color": "#AC347B"},
                {"label": "20", "quantity": 20, "color": "#6A247A"},
                {"label": "> 30", "quantity": 30, "color": "#2C255B"}
            ]
        },
        "advisory_flags": {
            "labels": [
                {"label": "Low regional backscatter (snow, ice, dryness)", "quantity": 1, "color": "#E94D79"},
                {"label": "Rough water surface (wind)", "quantity": 2, "color": "#AED07A"},
                {"label": "Low regional backscatter and rough water surface", "quantity": 3, "color": "#41BEDD"}
            ]
        }
    }
