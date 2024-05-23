import boto3
import pygeohydro as pgh
import os

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
def filter_contains_sequence(sequence):
    def filter_func(key):
        return sequence in key
    return filter_func

def process_directory(bucket, obj):
    return obj['Key']

def filter_pdfs(obj):
    return obj['Key'].endswith('.pdf')

def process_pdf(bucket, obj):
    return obj['Key']

# Applications of list_s3_objects with specific filter and process functions
def list_tifs_in_bucket(bucket, prefix, client):
    def filter_tif_files(obj):
        return obj['Key'].endswith('.tif')
    def process_tif_file(bucket, obj):
        return f"https://{bucket}.s3.amazonaws.com/{obj['Key']}"
    return list_s3_objects(bucket, prefix, client, filter_tif_files, process_tif_file)

def list_subdirectories(bucket_name, prefix, s3):
    return list_s3_objects(bucket_name, prefix, s3, delimiter='/')

def find_directories_with_sequence(bucket_name, prefix, s3, digit_sequence):
    def combined_filter_func(key):
        return filter_contains_sequence(digit_sequence)(key)
    return list_s3_objects(bucket_name, prefix, s3, combined_filter_func, process_directory, delimiter='/')

def list_pdfs_in_directory(bucket, prefix, client):
    return list_s3_objects(bucket, prefix, client, filter_pdfs, process_pdf)

# Example usage
if __name__ == "__main__":
    s3_client = boto3.client('s3')

    bucket_name = 'your-bucket-name'
    prefix = 'your/prefix/path/'
    digit_sequence = '12100202'

    # List TIFF files in bucket
    tifs = list_tifs_in_bucket(bucket_name, prefix, s3_client)
    print("TIFF files:")
    for tif in tifs:
        print(tif)

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

