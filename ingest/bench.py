def list_tifs_in_bucket(bucket, prefix, client):
    """List all TIFF files in the bucket under the given prefix."""
    paginator = client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    tif_urls = []
    for page in pages:
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.tif'):
                tif_url = f"https://{bucket}.s3.amazonaws.com/{obj['Key']}"
                tif_urls.append(tif_url)
    return tif_urls

def list_subdirectories(bucket_name, prefix, s3):
    """
    List the subdirectories in an S3 bucket at a specified prefix.

    :param bucket_name: The name of the S3 bucket.
    :param prefix: The prefix (path) to list subdirectories from.
    :s3: s3 client
    :return: A list of subdirectory prefixes.
    """
    paginator = s3.get_paginator('list_objects_v2')
    operation_parameters = {
        'Bucket': bucket_name,
        'Prefix': prefix,
        'Delimiter': '/'
    }

    subdirectories = []
    for page in paginator.paginate(**operation_parameters):
        if 'CommonPrefixes' in page:
            for common_prefix in page['CommonPrefixes']:
                subdirectories.append(common_prefix['Prefix'])

    return subdirectories
