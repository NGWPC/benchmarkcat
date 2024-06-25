import pandas as pd
import io
import os
import json
from shapely.geometry import shape, MultiPolygon, mapping
from shapely.ops import unary_union
from fiona.transform import transform_geom
import re
from datetime import datetime, timezone
import pystac

def make_item_geom(bucket_name, keys, s3):
    geojson_geometries = []

    for key in keys:
        response = s3.get_object(Bucket=bucket_name, Key=key)
        geojson_content = response['Body'].read().decode('utf-8')
        geojson_dict = json.loads(geojson_content)

         # Check if the GeoJSON is a FeatureCollection or a single Feature
        if geojson_dict['type'] == 'FeatureCollection':
            features = geojson_dict['features']
        elif geojson_dict['type'] == 'Feature':
            features = [geojson_dict]
        else:
            raise ValueError(f"Unsupported GeoJSON type: {geojson_dict['type']}")
               
        for feature in features:
            geom = feature['geometry']
            transformed_geom = transform_geom('EPSG:4326', 'EPSG:4326', geom)  # Assuming GeoJSON is in EPSG:4326
            shapely_geom = shape(transformed_geom)
            geojson_geometries.append(shapely_geom)
    
    # Combine all geometries into a single MultiPolygon
    combined_geometry = unary_union(geojson_geometries)

    if not isinstance(combined_geometry, MultiPolygon):
        combined_geometry = MultiPolygon([combined_geometry])
    
    # Calculate the combined bbox
    bbox = combined_geometry.bounds
    combined_bbox = [bbox[0], bbox[1], bbox[2], bbox[3]]
    
    # Ensure the output is JSON-serializable
    geojson_geometry = json.loads(json.dumps(mapping(combined_geometry)))
    
    return geojson_geometry, combined_bbox

# Example usage:
# s3 = boto3.client('s3')
# bucket_name = 'my-bucket'
# geojson_keys = ['path/to/footprint1.geojson', 'path/to/footprint2.geojson', ...]
# geojson_geometry, combined_bbox = download_geojson_files(bucket_name, geojson_keys, s3)

def extract_datetimes(sentinel_string):
    # Regular expression to extract datetime strings
    datetime_pattern = re.compile(r'_(\d{8}T\d{6})_(\d{8}T\d{6})_')
    match = datetime_pattern.search(sentinel_string)
    
    if match:
        start_datetime_str = match.group(1)
        end_datetime_str = match.group(2)
        
        # Convert to datetime objects
        start_datetime = datetime.strptime(start_datetime_str, '%Y%m%dT%H%M%S').replace(tzinfo=timezone.utc)
        end_datetime = datetime.strptime(end_datetime_str, '%Y%m%dT%H%M%S').replace(tzinfo=timezone.utc)
        return start_datetime, end_datetime
    else:
        raise ValueError("No valid datetime strings found in the input data string")

def download_flowfile(bucket_name, flowfile_key, s3_client):
    response = s3_client.get_object(Bucket=bucket_name, Key=flowfile_key)
    flowfile_content = response['Body'].read().decode('utf-8')
    return pd.read_csv(io.StringIO(flowfile_content))

def extract_flowstats(flowfile_df):
    flowstats = {}
    for column in flowfile_df.columns:
        if flowfile_df[column].dtype in ['float64', 'int64']:  # Only consider numeric columns
            min_value = flowfile_df[column].min()
            max_value = flowfile_df[column].max()
            mean_value = flowfile_df[column].mean()
            flowstats[column] = {
                'Min': min_value,
                'Max': max_value,
                'Mean': mean_value
            }
    return flowstats

def create_flowfile_object(flowfile_id, flowstats):
    # Assuming the second column is always "discharge"
    second_column = "discharge"

    # Check if the second column exists in the flowstats
    if second_column in flowstats:
        # Compute the flowstats summaries only for the second column
        flowstats_summaries = {
            second_column: {
                "Min": float(flowstats[second_column]['Min']),
                "Max": float(flowstats[second_column]['Max']),
                "Mean": float(flowstats[second_column]['Mean'])
            }
        }
    else:
        raise KeyError(f"Column {second_column} not found in flowstats")  

    flowfile_object = {
        "flowfile_asset_summaries": {
            flowfile_id: {
                "Flowstats": flowstats_summaries
            }
        },
        "columns": {
            "Column name": ["feature_id", "discharge"],
            "Column description": ["feature id that identifies the stream segment being modeled or measured", "Discharge in m^3/s"],
            "column data source": ["NWM 3.0 hydrofabric", "NWM 3.0 retrospective discharge data"]
        },
        "hydrofabric_href": "https://water.noaa.gov/resources/downloads/nwm/NWM_channel_hydrofabric.tar.gz",
        "discharge_href":"https://registry.opendata.aws/nwm-archive/"
        }
    return flowfile_object

def extract_orbit_state(filename):
    # Regular expression to match the filename pattern and extract the orbit state (A or D)
    pattern = re.compile(r'.*?_[VH]{2}_([AD]).*')
    match = pattern.match(filename)
    
    if match:
        orbit_state = match.group(1)
        return orbit_state
    else:
        raise ValueError("No valid orbit state found in the input filename")

def extract_orbit_number(filename):
    # Regular expression to match the filename pattern and extract the orbit number (OOOOOO)
    pattern = re.compile(r'.*?_\d{8}T\d{6}_\d{8}T\d{6}_(\d{6})_.*')
    match = pattern.match(filename)
   
    if match:
        orbit_number = match.group(1)
        return orbit_number
    else:
        raise ValueError("No valid orbit number found in the input filename")

def extract_version_string(filepath):
    # Extract the filename from the full path
    filename = os.path.basename(filepath)
    
    # Regular expression to match the version string immediately preceding "_S1"
    pattern = re.compile(r'_(V\d+M\d+R\d+)_S1')
    match = pattern.search(filename)
    
    if match:
        version_string = match.group(1)
        return version_string
    else:
        raise ValueError("No valid version string found in the input filename")

# Helper function to get media type based on file extension
def get_media_type(file_name):
    if file_name.endswith(".tif") or file_name.endswith(".tiff"):
        return pystac.MediaType.GEOTIFF
    elif file_name.endswith(".geojson"):
        return pystac.MediaType.GEOJSON
    elif file_name.endswith(".json"):
        return pystac.MediaType.JSON
    elif file_name.endswith(".pdf"):
        return pystac.MediaType.PDF
    elif file_name.endswith(".jpeg") or file_name.endswith(".jpg"):
        return pystac.MediaType.JPEG
    elif file_name.endswith(".png"):
        return pystac.MediaType.PNG
    elif file_name.endswith(".xml"):
        return pystac.MediaType.XML
    elif file_name.endswith(".txt"):
        return pystac.MediaType.TEXT
    elif file_name.endswith(".hdf"):
        return pystac.MediaType.HDF
    elif file_name.endswith(".h5"):
        return pystac.MediaType.HDF5
    elif file_name.endswith(".jp2"):
        return pystac.MediaType.JPEG2000
    elif file_name.endswith(".kml"):
        return pystac.MediaType.KML
    elif file_name.endswith(".fgb"):
        return pystac.MediaType.FLATGEOBUF
    elif file_name.endswith(".gpkg"):
        return pystac.MediaType.GEOPACKAGE
    elif file_name.endswith(".parquet"):
        return pystac.MediaType.PARQUET
    elif file_name.endswith(".zarr"):
        return pystac.MediaType.ZARR
    elif file_name.endswith(".html"):
        return pystac.MediaType.HTML
    else:
        return "application/octet-stream"

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
