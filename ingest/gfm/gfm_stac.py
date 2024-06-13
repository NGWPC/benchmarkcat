import os
import json
from shapely.geometry import shape, MultiPolygon, mapping
from shapely.ops import unary_union
from fiona.transform import transform_geom
import re
from datetime import datetime

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
        start_datetime = datetime.strptime(start_datetime_str, '%Y%m%dT%H%M%S')
        end_datetime = datetime.strptime(end_datetime_str, '%Y%m%dT%H%M%S')
        
        return start_datetime, end_datetime
    else:
        raise ValueError("No valid datetime strings found in the input data string")

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
