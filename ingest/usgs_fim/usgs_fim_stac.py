import boto3
import fiona
import json
import tempfile
from fiona.transform import transform_geom
from shapely.geometry import shape, mapping
from shapely.ops import transform
import pyproj

def load_domain_geometry(bucket_name, prefix, s3):
    files = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    shapefile_files = {file['Key'].split('.')[-1]: file['Key'] for file in files['Contents']}
    
    # Ensure required files are present
    required_exts = ['shp', 'shx', 'dbf']
    for ext in required_exts:
        if ext not in shapefile_files:
            raise FileNotFoundError(f"Missing {ext} file in the specified S3 path")
    
    # Download necessary files to a dictionary of byte streams
    file_streams = {}
    for ext in shapefile_files:
        response = s3.get_object(Bucket=bucket_name, Key=shapefile_files[ext])
        file_streams[ext] = response['Body'].read()
    
    # Create a temporary directory
    with tempfile.TemporaryDirectory() as tmpdirname:
        file_paths = {}
        
        # Write each file to the temporary directory
        for ext, content in file_streams.items():
            file_path = f"{tmpdirname}/file.{ext}"
            with open(file_path, 'wb') as f:
                f.write(content)
            file_paths[ext] = file_path
        
        # Read the shapefile using fiona
        with fiona.open(file_paths['shp']) as src:
            crs = src.crs
            features = [feature for feature in src]
    
    geojson = {
        "type": "FeatureCollection",
        "features": []
    }
    
    bboxes = []

    for feature in features:
        geom = feature['geometry']
        transformed_geom = transform_geom(crs, 'EPSG:4326', geom)
        shapely_geom = shape(transformed_geom)
        bbox = shapely_geom.bounds
        bboxes.append(bbox)
        geojson['features'].append({
            "type": "Feature",
            "geometry": mapping(shapely_geom),
            "properties": feature['properties']
        })
    
    # Calculate the combined bbox
    minx = min(bbox[0] for bbox in bboxes)
    miny = min(bbox[1] for bbox in bboxes)
    maxx = max(bbox[2] for bbox in bboxes)
    maxy = max(bbox[3] for bbox in bboxes)
    combined_bbox = [minx, miny, maxx, maxy]
    
    # Ensure the output is JSON-serializable
    for feature in geojson['features']:
        feature['properties'] = json.loads(json.dumps(feature['properties'], default=str))
        feature['geometry'] = json.loads(json.dumps(feature['geometry']))
    
    return geojson, combined_bbox
