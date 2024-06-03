import boto3
import fiona
import json
import tempfile
from fiona.transform import transform_geom
from shapely.geometry import shape, mapping, MultiPolygon
from shapely.ops import unary_union

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
    
    geometries = []

    for feature in features:
        geom = feature['geometry']
        transformed_geom = transform_geom(crs, 'EPSG:4326', geom)
        shapely_geom = shape(transformed_geom)
        geometries.append(shapely_geom)
    
    # Combine all geometries into a single MultiPolygon
    combined_geometry = unary_union(geometries)

    if not isinstance(combined_geometry, MultiPolygon):
        combined_geometry = MultiPolygon([combined_geometry])
    
    # Calculate the combined bbox
    bbox = combined_geometry.bounds
    combined_bbox = [bbox[0], bbox[1], bbox[2], bbox[3]]
    
    # Ensure the output is JSON-serializable
    geojson_geometry = json.loads(json.dumps(mapping(combined_geometry)))
    
    return geojson_geometry, combined_bbox
