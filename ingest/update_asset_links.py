"""
Script to change out asset hrefs. This is so that one can quickly change out the type or value for the href path when data location changes. The most typical use case for this script would be to generate new signed URL's for a catalog or to switch out the asset href from an "s3://" to an http url. 

Usage:
    python -m ingest.update_asset_links --cat_dir full_path_to_stac_catalog_directory --link_type link_type

Arguments:
"--cat_dir": path to a locally mounted directory containing the catalog.
"--link_type": whether to generate a URI or a URL. Here URS is shorthand for a presigned URL.

Potential Changes:
- update asset paths from one local directory or another or from a local directory to an s3 uri/url
- In future this could be expanded so that non-relative hrefs in other parts of the catalog could be updated. For example an href inside the links object that references local documentation that might have changed locations.
"""

import argparse
import os
import pystac
from ingest.bench import S3Utils
import boto3

def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--cat_dir', type=str, required=True, help='Path to a local static catalog')
    parser.add_argument('--link_type', type=str, default='uri', help='Link type, either "url" or "uri"')
    return parser.parse_args()

def load_catalog(cat_dir):
    catalog = pystac.Catalog.from_file(os.path.join(cat_dir, 'catalog.json'))
    return catalog

def extract_s3_info(href):
    if href.startswith("s3://"):
        path = href[5:]
        bucket_name, *key_parts = path.split('/', 1)
        key = key_parts[0] if key_parts else ''
        return bucket_name, key
    elif href.startswith("http://") or href.startswith("https://"):
        if ".s3.amazonaws.com/" in href:
            path = href.split(".s3.amazonaws.com/")[1]
            bucket_name = href.split("//")[1].split(".s3.amazonaws.com")[0]
            return bucket_name, path
    raise ValueError(f"Unsupported S3 href format: {href}")

def update_asset_hrefs(catalog, s3_utils, link_type):
    for item in catalog.get_all_items():
        for asset_key, asset in item.assets.items():
            try:
                bucket_name, path = extract_s3_info(asset.href)
                new_href = s3_utils.generate_href(bucket_name, path, link_type)
                asset.href = new_href
                print(f"Updated asset href for {asset_key}: {new_href}")
            except ValueError as e:
                print(f"Error updating asset {asset_key}: {e}")
        # Save the updated item back to its original location
        item.save_object()

def main():
    args = parse_arguments()
    
    s3_client = boto3.client('s3')
    s3_utils = S3Utils(s3_client)

    catalog = load_catalog(args.cat_dir)
    update_asset_hrefs(catalog, s3_utils, args.link_type)
    
    # Save updated catalog inside of directory you call script 
    catalog.save(catalog_type=pystac.CatalogType.SELF_CONTAINED)
    print(f"Catalog updated in place at {args.cat_dir}")

if __name__ == "__main__":
    main()
