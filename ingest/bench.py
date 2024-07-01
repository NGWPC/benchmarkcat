import tempfile
import pandas as pd
import boto3
import io
import json
import pystac
import pygeohydro as pgh
import os
import rioxarray
import rasterio
import numpy as np
from PIL import Image

def create_preview(raster, preview_path, size=(256, 256)):
    with rasterio.open(raster) as src:
        # Read the single band
        img_data = src.read(1)
        
        try:
            # Attempt to retrieve the colormap from the raster
            colormap = src.colormap(1)
        except ValueError:
            colormap = None
        
        if colormap:
            # Initialize an empty RGB(A) array
            img_data_rgba = np.zeros((img_data.shape[0], img_data.shape[1], 4), dtype=np.uint8)
            
            # Apply the colormap to create an RGBA representation
            for index, color in colormap.items():
                mask = img_data == index
                img_data_rgba[mask] = color  # Color is expected to be RGBA
        else:
            # Apply a binary colormap (white for zero values, black for nonzero values)
            img_data_rgba = np.zeros((img_data.shape[0], img_data.shape[1], 4), dtype=np.uint8)
            img_data_rgba[img_data == 0] = [255, 255, 255, 255]  # White color for zero values (RGBA)
            img_data_rgba[img_data != 0] = [0, 0, 0, 255]        # Black color for nonzero values (RGBA)
        
        # Convert the RGBA array to a PIL Image
        pil_image = Image.fromarray(img_data_rgba, 'RGBA')

        # Calculate new size to maintain aspect ratio
        img_width, img_height = pil_image.size
        max_width, max_height = size

        # Determine the scale factor to fit within the given size
        scale = min(max_width / img_width, max_height / img_height)

        # Compute new size to maintain aspect ratio
        new_width = int(img_width * scale)
        new_height = int(img_height * scale)

        # Resize the image with new size
        preview = pil_image.resize((new_width, new_height), resample=Image.Resampling.LANCZOS)

        # Save the preview
        preview.save(preview_path, format="PNG")

def count_pixels(raster_path, values=None):
    """Function to count pixels in a raster matching specific values.
    
    Args:
        raster_path (str): Path to the raster file.
        values (list of int, optional): List of integer values to count. Counts non-zero pixels if None.
                                        
    Returns:
        int: Count of pixels matching the criteria.
    """

    raster = rioxarray.open_rasterio(raster_path, masked=True, chunks=True)
    band1 = raster.sel(band=1)
    
    if values is None:
        # Default behavior: count non-zero pixels
        pixel_count = (band1 != 0).sum().compute().item()
    else:
        # Count pixels matching any of the specified values
        mask = False
        for value in values:
            mask |= (band1 == value)
        pixel_count = mask.sum().compute().item()
    
    return pixel_count

def get_huc8_geometry(huc8):
    wbd = pgh.WBD("huc8")
    huc8_geom = wbd.byids("huc8", [huc8])
    return huc8_geom.geometry.iloc[0]

def list_s3_objects(bucket, prefix, client, filter_func=None, process_func=None, delimiter=None):
    """
    List objects in an S3 bucket under the given prefix with optional filtering and processing.

    :param bucket: The name of the S3 bucket.
    :param prefix: The prefix (path) to list objects from.
    :param client: The S3 client.
    :param filter_func: Optional function to filter objects.
    :param process_func: Optional function to process objects.
    :param delimiter: Optional delimiter to group common prefixes (e.g., directories).
    :return: A list of processed objects.
    """
    paginator = client.get_paginator('list_objects_v2')
    operation_parameters = {
        'Bucket': bucket,
        'Prefix': prefix
    }
    if delimiter:
        operation_parameters['Delimiter'] = delimiter

    pages = paginator.paginate(**operation_parameters)
    
    results = []
    for page in pages:
        if delimiter and 'CommonPrefixes' in page:
            for common_prefix in page['CommonPrefixes']:
                prefix = common_prefix['Prefix']
                if filter_func is None or filter_func(prefix):
                    if process_func:
                        results.append(process_func(bucket, {'Key': prefix}))
                    else:
                        results.append(prefix)
        else:
            for obj in page.get('Contents', []):
                if filter_func is None or filter_func(obj):
                    if process_func:
                        results.append(process_func(bucket, obj))
                    else:
                        results.append(obj['Key'])
    return results

# Specific filtering and processing functions

def process_directory(bucket, obj):
    return obj['Key']

# Applications of list_s3_objects with specific filter and process functions
def list_files_with_extensions(bucket, prefix, client, extensions):
    """
    List files in an S3 bucket under the given prefix that end with any of the specified extensions.

    :param bucket: The name of the S3 bucket.
    :param prefix: The prefix (path) to list objects from.
    :param client: The S3 client.
    :param extensions: A list of file extensions to filter by (e.g., ['.pdf', '.tif']).
    :return: A list of URLs to the files that match the specified extensions.
    """
    def filter_files_with_extensions(obj):
        return any(obj['Key'].endswith(ext) for ext in extensions)
    
    def process_file(bucket, obj):
        return obj['Key']
    
    return list_s3_objects(bucket, prefix, client, filter_files_with_extensions, process_file)

def list_subdirectories(bucket_name, prefix, s3):
    return list_s3_objects(bucket_name, prefix, s3, delimiter='/')

def list_resources_with_string(bucket, prefix, client, keywords,delimiter=None):
    """
    List files or directories in an S3 bucket under the given prefix that contain any of the list of strings (keywords).

    If you want to return directories then set delimiter equal to '/'
    """

    def filter_func(obj):
        return any(keyword in obj['Key'] for keyword in keywords)    
    def process_func(bucket, obj):
        return obj['Key']
    
    return list_s3_objects(bucket, prefix, client, filter_func, process_func, delimiter=delimiter)


def download_catalog_and_collections(catalog_key, s3, bucket_name, tmp_dir):
    """Download a catalog and all its top-level collections."""
    # Download the catalog
    catalog_response = s3.get_object(Bucket=bucket_name, Key=catalog_key)
    catalog_content = catalog_response['Body'].read().decode('utf-8')
    catalog_dict = json.load(io.StringIO(catalog_content))
    
    # Save the catalog file to the temporary directory
    catalog_local_path = os.path.join(tmp_dir, os.path.basename(catalog_key))
    with open(catalog_local_path, 'w') as f:
        json.dump(catalog_dict, f, indent=4)

    catalog = pystac.Catalog.from_dict(catalog_dict)
    
    # Download each top-level collection
    for link in catalog.get_child_links():
        child_relative_path = link.target  # Get the relative path from the link
        catalog_dir = os.path.dirname(catalog_key)
        child_s3_key = os.path.normpath(os.path.join(catalog_dir, child_relative_path))
        child_local_path = os.path.join(tmp_dir, child_relative_path)
        
        os.makedirs(os.path.dirname(child_local_path), exist_ok=True)
        
        # Download the child collection
        child_response = s3.get_object(Bucket=bucket_name, Key=child_s3_key)
        child_content = child_response['Body'].read().decode('utf-8')
        child_dict = json.load(io.StringIO(child_content))
        
        # Save the child collection to the temporary directory
        with open(child_local_path, 'w') as f:
            json.dump(child_dict, f, indent=4)
    
    return catalog, catalog_local_path

# Function to upload an entire directory to S3
def upload_directory_to_s3(directory_path, bucket_name, destination_path,client):
    for root, _, files in os.walk(directory_path):
        for file in files:
            file_path = os.path.join(root, file)
            s3_key = os.path.join(destination_path, os.path.relpath(file_path, directory_path))
            try:
                client.upload_file(file_path, bucket_name, s3_key)
                print(f"Uploaded {file_path} to s3://{bucket_name}/{s3_key}")
            except (NoCredentialsError, ClientError) as e:
                print(f"Failed to upload {file_path} to s3://{bucket_name}/{s3_key}: {e}")

def update_collection(collection, catalog_id, catalog_path, s3, bucket_name):
    """
    Update the given collection in the catalog and upload it to S3.

    Args:
        collection (pystac.Collection): The collection object to be added or updated.
        catalog_id (str): The ID of the collection to be updated or added.
        catalog_path (str): The S3 path to the catalog. Needs to end in a "/"
        s3 (boto3.client): The S3 client.
        bucket_name (str): The S3 bucket name.
        bench (object): The object containing download_catalog_and_collections and upload_directory_to_s3 methods.
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        # Download the catalog and all child collections to the temporary directory
        catalog_key = f'{catalog_path}catalog.json'
        catalog, catalog_local_path = download_catalog_and_collections(catalog_key, s3, bucket_name, temp_dir)

        # Set root and self href for the catalog so can add/update the collection
        catalog.set_root(catalog)
        catalog.set_self_href(catalog_local_path)

        # Remove child in case collection being updated
        try:
            catalog.remove_child(catalog_id)
        except KeyError:
            pass

        # Add collection to catalog
        catalog.add_child(collection)

        # Resave the catalog to the temporary directory after adding in the collection
        catalog.normalize_and_save(root_href=temp_dir, catalog_type=pystac.CatalogType.SELF_CONTAINED, skip_unresolved=True)    

        # Upload the contents of the temporary directory to S3
        upload_directory_to_s3(temp_dir, bucket_name, catalog_path, s3)

def generate_href(bucket_name, path, s3_client, link_type, expiration=7*24*60*60):
    """
    Generates either a signed URL or an S3 URI.

    param bucket_name: The name of the S3 bucket.
    :param extent_path: The path to the object in the S3 bucket.
    :param s3_client: The boto3 S3 client.
    :param link_type: The type of link to generate ('url' for signed URL, 'uri' for S3 URI).
    :param expiration: The expiration time for the signed URL in seconds (default is 1 week).
    :return: The generated link (signed URL or S3 URI).
    """
    try:
        if link_type == 'url':
            # Generate the signed URL
            signed_url = s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket_name, 'Key': path},
                ExpiresIn=expiration
            )
            return signed_url
        elif link_type == 'uri':
            # Generate the S3 URI
            s3_uri = f"s3://{bucket_name}/{path}"
            return s3_uri
        else:
            raise ValueError("link_type must be either 'url' or 'uri'")
    except NoCredentialsError:
        return "Credentials not available"

# flowfile functions
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

def create_flowfile_object(flowfile_ids, flowstats, columns_list):
    """
    Create a flowfile object with given flowfile IDs, flowstats, and columns.

    Args:
        flowfile_ids (list): List of flowfile IDs.
        flowstats (dict): Dictionary containing flow statistics for each column.
        columns_list (list): List of dictionaries, where each dictionary contains column descriptions 
                             for the corresponding flowfile ID.

    Returns:
        dict: A dictionary representing the flowfile object.

    Raises:
        KeyError: If the second column ('discharge') is not found in flowstats.
    """
    # Assuming the second column is always "discharge"
    second_column = "discharge"

    # Check if the second column exists in the flowstats
    if second_column in flowstats:
        flow_summaries = {
            "Flowstats": {
                second_column: {
                    "Min": float(flowstats[second_column]['Min']),
                    "Max": float(flowstats[second_column]['Max']),
                    "Mean": float(flowstats[second_column]['Mean'])
                }
            }
        }

        flowfile_object = {}

        for i, flowfile_id in enumerate(flowfile_ids):
            # Ensure columns_list has enough dictionaries for each flowfile_id
            if i < len(columns_list):
                columns = columns_list[i]
            else:
                raise IndexError("Not enough column dictionaries provided for the flowfile IDs.")

            flowfile_object[flowfile_id] = {
                **flow_summaries,
                "columns": columns
            }

        return flowfile_object
    else:
        raise KeyError(f"Column {second_column} not found in flowstats")


# test usage
if __name__ == "__main__":
    s3_client = boto3.client('s3')

    bucket_name = 'your-bucket-name'
    prefix = 'your/prefix/path/'
    digit_sequence = '12100202'

    # List subdirectories in bucket
    subdirs = list_subdirectories(bucket_name, prefix, s3_client)
    print("\nSubdirectories:")
    for subdir in subdirs:
        print(subdir)

    # Find directories with specific sequence
    dirs_with_sequence = find_directories_with_sequence(bucket_name, prefix, s3_client, digit_sequence)
    print("\nDirectories with sequence:")
    for dir in dirs_with_sequence:
        print(dir)

