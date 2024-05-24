import os
import json
import tempfile
import boto3
import pystac
from botocore.exceptions import NoCredentialsError, ClientError

'''
Initializes and uploads an empty catalog to be filled in with the collection creation and update scripts inside each submodule.
'''

# Create an S3 client
s3 = boto3.client('s3')

# Specify bucket parameters
bucket_name = 'fimc-data'
collection_object_key = 'benchmark/stac-bench-cat/collections/ble/ble.json'
catalog_key = 'benchmark/stac-bench-cat/bench_cat.json'
base_s3_url = f'https://{bucket_name}.s3.amazonaws.com/benchmark/stac-bench-cat/'

# Define the catalog
bench_catalog = pystac.Catalog(
    id='benchmark-catalog',
    description="Benchmark catalog for NWC FIM models",
    title="FIM Benchmark Catalog",
    catalog_type=pystac.CatalogType.SELF_CONTAINED
    )
    
# Check catalog links
for link in bench_catalog.get_links():
    print(f"Catalog link: {link.rel}, {link.get_absolute_href()}")

# Write catalog to S3
with tempfile.TemporaryDirectory() as temp_dir:
    # normalize catalog hrefs
    bench_catalog.normalize_hrefs("./")
    print(json.dumps(bench_catalog.to_dict(), indent=4))
    # Save the catalog to the temporary directory
    bench_catalog.save(catalog_type=pystac.CatalogType.SELF_CONTAINED, dest_href=temp_dir)
    # List all files and directories in the specified directory
    contents = os.listdir(temp_dir)
    
    print(f"Contents of '{temp_dir}':")
    for item in contents:
        print(item)

    # Path to the saved catalog JSON file
    catalog_json_path = os.path.join(temp_dir, 'catalog.json')
    
    # Upload the saved catalog JSON file to S3
    try:
        s3.upload_file(catalog_json_path, bucket_name, catalog_key)
        print(f"Uploaded {catalog_json_path} to s3://{bucket_name}/{catalog_key}")
    except (NoCredentialsError, ClientError) as e:
        print(f"Failed to upload catalog to S3: {e}")

# Validate catalog
bench_catalog.validate()

print("STAC catalog created and uploaded successfully.")
