# This script is designed to modify the asset hrefs in a self contained catalog with 
# interleaved assets (ie the assets are included in the catalog itself) so that the assets
# can be served from a local/non-local fileserver inside the oe.

import os
import logging
from urllib.parse import urljoin
from pystac import Catalog, Item, Asset, CatalogType
import pdb
# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_URL = "http://0.0.0.0:8000/"
CATALOG_ROOT = "/efs/benchmark/bench_stac"

def update_asset_href(asset_href, base_url, catalog_root):
    """
    Update the asset href to use the new base URL.
    """
    if not asset_href:
        return asset_href

    # Check if the asset_href starts with the catalog_root
    if asset_href.startswith(catalog_root):
        # Replace the catalog_root with the base_url
        relative_path = asset_href[len(catalog_root):].lstrip('/')
        return urljoin(base_url, relative_path)
    else:
        # If the asset_href doesn't start with catalog_root, log a warning and return the original href
        logger.warning(f"Asset href does not start with catalog root: {asset_href}")
        return asset_href

def process_assets(stac_object, base_url, catalog_root):

    """
    Process all assets in a STAC object (Item or Collection).
    """
    for asset_key, asset in stac_object.assets.items():
        if asset.href:
            new_href = update_asset_href(asset.href, base_url, catalog_root)
            asset.href = new_href
#            logger.info(f"Updated asset {asset_key} href to: {new_href}")

def process_item(item, base_url, catalog_root):
    """
    Process an individual STAC Item.
    """
    process_assets(item, base_url, catalog_root)

def process_collection(collection, base_url, catalog_root):
    """
    Process a STAC Collection.
    """
    process_assets(collection, base_url, catalog_root)

def process_catalog(catalog, base_url, catalog_root):
    """
    Recursively process a STAC Catalog.
    """
    if isinstance(catalog, Catalog):
        for child in catalog.get_children():
            process_catalog(child, base_url, catalog_root)
        
    if isinstance(catalog, Item):
        process_item(catalog, base_url, catalog_root)
    elif hasattr(catalog, 'assets'):  # Collections have assets
        process_collection(catalog, base_url, catalog_root)
    
    for item in catalog.get_items(recursive=False):
        process_item(item, base_url, catalog_root)

def main():
    logger.info("Starting STAC asset href update process")
    
    # Load the root catalog
    root_catalog = Catalog.from_file(os.path.join(CATALOG_ROOT, "catalog.json"))

    # make the asset hrefs absolute
    root_catalog.make_all_asset_hrefs_absolute()

    # Process the entire catalog
    process_catalog(root_catalog, BASE_URL, CATALOG_ROOT)
    
    # Save the updated catalog
    root_catalog.normalize_and_save(
        root_href=CATALOG_ROOT,
        catalog_type=CatalogType.SELF_CONTAINED
    )

    logger.info("STAC asset href update process complete.")

if __name__ == "__main__":
    main()
