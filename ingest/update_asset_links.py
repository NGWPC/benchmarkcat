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
import pdb
from urllib.parse import urlparse

def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--cat_dir', type=str, required=True, help='Path to a local static catalog')
    parser.add_argument('--link_type', type=str, default='uri', help='Link type, either "url" or "uri"')
    return parser.parse_args()

def load_catalog(cat_dir):
    catalog = pystac.Catalog.from_file(os.path.join(cat_dir, 'catalog.json'))
    return catalog

def extract_s3_info(href):
    parsed_url = urlparse(href)
    if parsed_url.scheme == "s3":
        path = parsed_url.path.lstrip('/')
        bucket_name = parsed_url.netloc
        return bucket_name, path
    elif parsed_url.scheme in ["http", "https"] and ".s3.amazonaws.com" in parsed_url.netloc:
        path = parsed_url.path.lstrip('/')
        bucket_name = parsed_url.netloc.split(".s3.amazonaws.com")[0]
        return bucket_name, path
    raise ValueError(f"Unsupported S3 href format: {href}")

def update_hrefs(stac_object, s3_utils, link_type):
    # Update asset HREFs
    if isinstance(stac_object, pystac.Item):
        for asset_key, asset in stac_object.assets.items():
            try:
                bucket_name, path = extract_s3_info(asset.href)
                new_href = s3_utils.generate_href(bucket_name, path, link_type)
                asset.href = new_href
                print(f"Updated asset href for {asset_key}: {new_href}")
            except ValueError as e:
                print(f"Error updating asset {asset_key}: {e}")
    
    # Update link HREFs
    for link in stac_object.links:
        pdb.set_trace()
        if link.href and (link.href.startswith("s3://") or link.href.startswith("https://")):
            try:
                bucket_name, path = extract_s3_info(link.href)
                new_href = s3_utils.generate_href(bucket_name, path, link_type)
                link.target = new_href
                print(f"Updated link href for {link.rel}: {new_href}")
            except ValueError as e:
                print(f"Error updating link {link.rel}: {e}")

def update_catalog_hrefs(catalog, s3_utils, link_type):
    for root, subcatalogs, items in catalog.walk():
        # Update the root catalog itself
        update_hrefs(root, s3_utils, link_type)
        
        # Update all subcatalogs
        for subcatalog in subcatalogs:
            update_hrefs(subcatalog, s3_utils, link_type)
        
        # Update all items
        for item in items:
            update_hrefs(item, s3_utils, link_type)
            # Save the updated item back to its original location
            item.save_object()

def main():
    args = parse_arguments()
    s3 = boto3.client('s3')
    s3_utils = S3Utils(s3)

    catalog = load_catalog(args.cat_dir)
    update_catalog_hrefs(catalog, s3_utils, args.link_type)
    
    # Save updated catalog inside of directory you call script 
    catalog.save(dest_href=args.cat_dir, catalog_type=pystac.CatalogType.SELF_CONTAINED)
    print(f"Catalog updated in place at {args.cat_dir}")

if __name__ == "__main__":
    main()
