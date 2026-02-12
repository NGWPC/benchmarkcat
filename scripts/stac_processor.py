import os
import logging
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
from pystac import Catalog, Collection, Item, CatalogType
from urllib.parse import urlparse, urljoin
import dask
from dask.delayed import delayed
from dask.distributed import Client, LocalCluster

class STACProcessor:
    def __init__(self, base_url="http://0.0.0.0:8000/", log_dir="logs", skip_existing=False):
        self.base_url = base_url.rstrip('/') + '/'  # Ensure base_url ends with /
        self.skip_existing = skip_existing
        self.setup_logging(log_dir)

    def setup_logging(self, log_dir):
        os.makedirs(log_dir, exist_ok=True)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        # Remove existing handlers
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)

        # Create timestamp for log file
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(console_format)
        self.logger.addHandler(console_handler)

        # Single file handler for all log levels
        log_file = os.path.join(log_dir, f'stac_processor_log_{timestamp}.log')
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(console_format)
        self.logger.addHandler(file_handler)

    def download_s3_file(self, s3_uri, local_path):
        # If skip_existing is True and the file exists, skip download
        if self.skip_existing and os.path.exists(local_path):
            return True

        parsed_uri = urlparse(s3_uri)
        bucket = parsed_uri.netloc
        key = parsed_uri.path.lstrip('/')

        s3 = boto3.client('s3')
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        try:
            s3.download_file(bucket, key, local_path)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                self.logger.error(f"Asset not found: {s3_uri}")
            else:
                self.logger.error(f"Error downloading {s3_uri}: {str(e)}")
            return False

    def get_relative_to_root(self, path, root_dir):
        """Get path relative to the catalog root directory"""
        return os.path.relpath(path, root_dir).replace('\\', '/')

    def process_asset(self, parent_href, asset_key, asset, output_dir):
        """Process a single asset: download if needed and update href"""
        if asset.href is None:
            self.logger.warning(f"Asset {asset_key} in {parent_href} has None href")
            return False

        if asset.href.startswith('s3://'):
            # Determine the local path in the output directory
            rel_dir = os.path.relpath(os.path.dirname(parent_href), output_dir)
            asset_filename = os.path.basename(asset.href)
            final_path = os.path.join(output_dir, rel_dir, asset_filename)

            # If the file exists and we're skipping existing files, just update the href
            if self.skip_existing and os.path.exists(final_path):
                # Get path relative to output directory to include collection name
                rel_path = self.get_relative_to_root(final_path, output_dir)
                asset.href = urljoin(self.base_url, rel_path)
                return True

            # Download the asset directly to its final location
            if self.download_s3_file(asset.href, final_path):
                # Get path relative to output directory to include collection name
                rel_path = self.get_relative_to_root(final_path, output_dir)
                asset.href = urljoin(self.base_url, rel_path)
                return True
            return False
        return True

    def process_assets(self, stac_object, output_dir):
        assets_to_remove = []
        for asset_key, asset in stac_object.assets.items():
            if not self.process_asset(stac_object.get_self_href(), asset_key, asset, output_dir):
                assets_to_remove.append(asset_key)

        for asset_key in assets_to_remove:
            del stac_object.assets[asset_key]
            self.logger.warning(f"Removed asset {asset_key} from {stac_object.__class__.__name__} {stac_object.id}")

    def process_item_minimal(self, item_dict, output_dir):
        """Process a single item using minimal data"""
        # Reconstruct item from dictionary
        item = Item.from_dict(item_dict)

        assets_to_remove = []
        for asset_key, asset in item.assets.items():
            if not self.process_asset(item.get_self_href(), asset_key, asset, output_dir):
                assets_to_remove.append(asset_key)

        for asset_key in assets_to_remove:
            del item.assets[asset_key]
            self.logger.warning(f"Removed asset {asset_key} from Item {item.id}")

        return item.to_dict()

    @delayed
    def process_item_delayed(self, item_dict, output_dir):
        """Dask delayed wrapper for processing a single item"""
        return self.process_item_minimal(item_dict, output_dir)

    def process_catalog(self, catalog, output_dir):
        if isinstance(catalog, Collection):
            self.process_assets(catalog, output_dir)

        # Process items in chunks
        all_items = list(catalog.get_items(recursive=True))
        chunk_size = 100

        for i in range(0, len(all_items), chunk_size):
            chunk = all_items[i:i + chunk_size]
            self.logger.info(f"Processing chunk {i//chunk_size + 1} of {(len(all_items)-1)//chunk_size + 1}")

            # Convert items to dictionaries to reduce graph size
            delayed_items = [
                self.process_item_delayed(item.to_dict(), output_dir)
                for item in chunk
            ]

            # Process chunk and wait for completion
            processed_items = dask.compute(*delayed_items)

            # Update the items in the catalog
            for orig_item, processed_dict in zip(chunk, processed_items):
                # Update the original item with processed data
                processed_item = Item.from_dict(processed_dict)
                orig_item.assets = processed_item.assets

            self.logger.info(f"Completed chunk {i//chunk_size + 1}")

        # Process child catalogs
        for child in catalog.get_children():
            self.process_catalog(child, output_dir)

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Process STAC catalog: download assets and update hrefs')
    parser.add_argument('--s3-catalog', required=True, help='S3 URI of the source catalog')
    parser.add_argument('--output-dir', required=True, help='Output directory for processed catalog')
    parser.add_argument('--log-dir', default='logs', help='Directory for log files')
    parser.add_argument('--base-url', default='http://0.0.0.0:8000/', help='Base URL for asset hrefs')
    parser.add_argument('--skip-existing', action='store_true', help='Skip downloading existing assets')
    parser.add_argument('--n-workers', type=int, default=4, help='Number of Dask workers to use')
    args = parser.parse_args()

    # Set up Dask distributed client
    cluster = LocalCluster(
        n_workers=args.n_workers,
        threads_per_worker=1,
        memory_limit='4GB'  # Limit memory per worker
    )
    client = Client(cluster)

    processor = STACProcessor(base_url=args.base_url, log_dir=args.log_dir, skip_existing=args.skip_existing)
    processor.logger.info(f"Starting catalog processing with {args.n_workers} workers")
    processor.logger.info(f"Dask dashboard available at {client.dashboard_link}")

    try:
        # Download catalog.json from S3
        catalog_path = os.path.join(args.output_dir, "catalog.json")
        if not processor.download_s3_file(os.path.join(args.s3_catalog, "catalog.json"), catalog_path):
            processor.logger.error("Failed to download catalog.json")
            return

        # Load and process the catalog
        catalog = Catalog.from_file(catalog_path)
        processor.process_catalog(catalog, args.output_dir)

        # Save the final catalog
        catalog.normalize_and_save(
            root_href=args.output_dir,
            catalog_type=CatalogType.SELF_CONTAINED
        )

        processor.logger.info("Catalog processing complete")

    finally:
        # Clean up Dask resources
        client.close()
        cluster.close()

if __name__ == "__main__":
    main()
