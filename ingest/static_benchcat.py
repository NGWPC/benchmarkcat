import os
import json
import tempfile
import boto3
import pystac
from botocore.exceptions import NoCredentialsError, ClientError

# Create an S3 client
s3 = boto3.client('s3')

# Specify bucket parameters
bucket_name = 'fimc-data'
collection_object_key = 'benchmark/stac-bench-cat/collections/ble/ble.json'
catalog_key = 'benchmark/stac-bench-cat/bench_cat.json'
base_s3_url = f'https://{bucket_name}.s3.amazonaws.com/benchmark/stac-bench-cat/'

# Define the catalog
catalog = pystac.Catalog(
    id='benchmark-catalog',
    description="Benchmark catalog for NWC FIM models",
    title="FIM Benchmark Catalog"
)

# Add the collection to the catalog
catalog.add_link(pystac.Link('root', f'{base_s3_url}bench_cat.json'))
catalog.add_link(pystac.Link('self', f'{base_s3_url}bench_cat.json'))
catalog.add_link(pystac.Link('child', './collections/ble/ble.json'))

# Check catalog links
for link in catalog.get_links():
    print(f"Catalog link: {link.rel}, {link.get_absolute_href()}")

# Write catalog to S3
with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
    catalog_json = catalog.to_dict()
    json.dump(catalog_json, temp_file, indent=4)
    temp_file.close()
    s3.upload_file(temp_file.name, bucket_name, catalog_key)
    os.remove(temp_file.name)

# Validate catalog
catalog.validate()

print("STAC catalog created and uploaded successfully.")
