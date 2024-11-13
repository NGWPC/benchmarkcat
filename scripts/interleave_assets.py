# On the OE this script needs to be run in the >python 3.10 conda env that has pystac installed.
# Can activate conda by running: source /contrib/software/miniconda/miniconda/etc/profile.d/conda.sh  

import pdb
import os
import logging
from logging.handlers import RotatingFileHandler
import boto3
from botocore.exceptions import ClientError
from pystac import Catalog, Collection, Item, Asset, CatalogType
from urllib.parse import urlparse

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Remove any existing handlers
for handler in logger.handlers[:]:
    logger.removeHandler(handler)

# Create a new file handler for errors that overwrites the existing file
error_log = RotatingFileHandler('asset_errors.log', mode='w', maxBytes=5*1024*1024, backupCount=2)
error_log.setLevel(logging.ERROR)
error_log_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
error_log.setFormatter(error_log_format)
logger.addHandler(error_log)

def download_s3_file(s3_uri, local_path):
    # Parse the S3 URI
    parsed_uri = urlparse(s3_uri)
    bucket = parsed_uri.netloc
    key = parsed_uri.path.lstrip('/')
    
    # Create S3 client
    s3 = boto3.client('s3')
    
    # Ensure the directory exists
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    
    try:
        # Attempt to download the file
        s3.download_file(bucket, key, local_path)
#        logger.info(f"Successfully downloaded: {s3_uri}")
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            logger.error(f"Asset not found: {s3_uri}")
        else:
            logger.error(f"Error downloading {s3_uri}: {str(e)}")
        return False

def get_relative_path(base_path, target_path):
    return os.path.relpath(target_path, os.path.dirname(base_path))

def localize_asset(parent_href, asset_key, asset):
    if asset.href is None:
        logger.warning(f"Asset {asset_key} in {parent_href} has None href")
        return False

    if asset.href.startswith('s3://'):
        # Determine the local path for the asset
        parent_dir = os.path.dirname(parent_href)
        asset_filename = os.path.basename(asset.href)
        local_asset_path = os.path.join(parent_dir, asset_filename)
        
        if download_s3_file(asset.href, local_asset_path):
            # Update the asset href to be relative
            asset.href = get_relative_path(parent_href, local_asset_path)
            return True
        else:
            logger.error(f"Failed to localize asset {asset.href}")
            return False
    else:
        return True  # We're not removing non-S3 assets, so return True

def process_assets(stac_object):
    assets_to_remove = []
    for asset_key, asset in stac_object.assets.items():
        if not localize_asset(stac_object.get_self_href(), asset_key, asset):
            assets_to_remove.append(asset_key)
    
    # Remove failed assets
    for asset_key in assets_to_remove:
        del stac_object.assets[asset_key]
        logger.warning(f"Removed asset {asset_key} from {stac_object.__class__.__name__} {stac_object.id} due to localization failure")

def process_item(item):
    process_assets(item)

def process_collection(collection):
    process_assets(collection)

def process_catalog(catalog):
    if isinstance(catalog, Collection):
        process_collection(catalog)
    
    for item in catalog.get_items(recursive=True):
        process_item(item)
    
    for child in catalog.get_children():
        process_catalog(child)

def main():
    logger.info("Starting catalog processing")
    # Load the root catalog
    root_catalog = Catalog.from_file("/efs/benchmark/bench_stac_rel_asset_hrefs/catalog.json")
    
    # Process the entire catalog
    process_catalog(root_catalog)
    
    # Save the updated catalog
    root_catalog.normalize_and_save(
        root_href="/efs/benchmark/bench_stac_rel_asset_hrefs",
        catalog_type=CatalogType.SELF_CONTAINED
    )

    logger.info("Catalog processing complete.")
    logger.info("Check 'asset_errors.log' for any assets that couldn't be downloaded.")

if __name__ == "__main__":
    main()
