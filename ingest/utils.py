import io
import json
import logging
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List

import numpy as np
import pygeohydro as pgh
import pystac
import rasterio
import requests
import rioxarray
from botocore.exceptions import ClientError, NoCredentialsError, ParamValidationError
from PIL import Image
from pyproj import CRS


class S3Utils:
    def __init__(self, s3_client):
        self.s3_client = s3_client

    def make_and_upload_thumbnail(self, local_asset_path, local_thumbnail_path, bucket_name, s3_path):
        try:
            # Download the file from S3
            self.s3_client.download_file(bucket_name, s3_path, local_asset_path)
            print(f"Downloaded extent raster to {local_asset_path}")
            # Create thumbnail
            RasterUtils.create_preview(local_asset_path, local_thumbnail_path)

            # Upload thumbnail to S3
            s3_dir = os.path.dirname(s3_path)
            filename = os.path.basename(local_thumbnail_path)
            thumbnail_s3_path = os.path.join(s3_dir, filename)
            self.s3_client.upload_file(local_thumbnail_path, bucket_name, thumbnail_s3_path)
            print(f"Uploaded thumbnail to s3://{bucket_name}/{thumbnail_s3_path}")

            return thumbnail_s3_path

        except NoCredentialsError:
            print("Credentials not available")
            return None
        except ClientError as e:
            print(f"Failed to download or upload files: {e}")
            return None

    def list_s3_objects(self, bucket, prefix, filter_func=None, process_func=None, delimiter=None):
        paginator = self.s3_client.get_paginator("list_objects_v2")
        operation_parameters = {"Bucket": bucket, "Prefix": prefix}
        if delimiter:
            operation_parameters["Delimiter"] = delimiter

        pages = paginator.paginate(**operation_parameters)

        results = []
        for page in pages:
            if delimiter and "CommonPrefixes" in page:
                for common_prefix in page["CommonPrefixes"]:
                    prefix = common_prefix["Prefix"]
                    if filter_func is None or filter_func({"Key": prefix}):
                        if process_func:
                            results.append(process_func(bucket, {"Key": prefix}))
                        else:
                            results.append(prefix)
            else:
                for obj in page.get("Contents", []):
                    if filter_func is None or filter_func(obj):
                        if process_func:
                            results.append(process_func(bucket, obj))
                        else:
                            results.append(obj["Key"])
        return results

    def list_files_with_extensions(self, bucket, prefix, extensions):
        def filter_files_with_extensions(obj):
            return any(obj["Key"].endswith(ext) for ext in extensions)

        def process_file(bucket, obj):
            return obj["Key"]

        return self.list_s3_objects(bucket, prefix, filter_files_with_extensions, process_file)

    def list_subdirectories(self, bucket_name, prefix):
        return self.list_s3_objects(bucket_name, prefix, delimiter="/")

    def list_resources_with_string(self, bucket, prefix, keywords, delimiter=None):
        def filter_func(obj):
            return any(keyword in obj["Key"] for keyword in keywords)

        def process_func(bucket, obj):
            return obj["Key"]

        return self.list_s3_objects(bucket, prefix, filter_func, process_func, delimiter=delimiter)

    def download_catalog_and_collections(self, catalog_key, bucket_name, tmp_dir):
        catalog_response = self.s3_client.get_object(Bucket=bucket_name, Key=catalog_key)
        catalog_content = catalog_response["Body"].read().decode("utf-8")
        catalog_dict = json.load(io.StringIO(catalog_content))

        catalog_local_path = os.path.join(tmp_dir, os.path.basename(catalog_key))
        with open(catalog_local_path, "w") as f:
            json.dump(catalog_dict, f, indent=4)

        catalog = pystac.Catalog.from_dict(catalog_dict)

        # Track seen child hrefs to avoid downloading the same collection multiple times
        seen_child_hrefs = set()

        for link in catalog.get_child_links():
            child_relative_path = link.get_href()

            # Skip if we've already downloaded this child collection
            if child_relative_path in seen_child_hrefs:
                continue
            seen_child_hrefs.add(child_relative_path)
            catalog_dir = os.path.dirname(catalog_key)
            child_s3_key = os.path.normpath(os.path.join(catalog_dir, child_relative_path))
            child_local_path = os.path.join(tmp_dir, child_relative_path)

            os.makedirs(os.path.dirname(child_local_path), exist_ok=True)

            try:
                child_response = self.s3_client.get_object(Bucket=bucket_name, Key=child_s3_key)
                child_content = child_response["Body"].read().decode("utf-8")
                child_dict = json.load(io.StringIO(child_content))

                with open(child_local_path, "w") as f:
                    json.dump(child_dict, f, indent=4)
            except ClientError as e:
                if e.response["Error"]["Code"] == "NoSuchKey":
                    print(f"Warning: Child collection not found in S3: {child_s3_key}")
                    print(f"Skipping this collection and continuing...")
                    continue
                else:
                    raise

        # Remove duplicate child links from catalog
        unique_links = []
        seen_hrefs = set()
        for link in catalog.links:
            if link.rel == pystac.RelType.CHILD:
                href = link.get_href()
                if href and href not in seen_hrefs:
                    seen_hrefs.add(href)
                    unique_links.append(link)
                # Duplicate child links are skipped
            else:
                # Keep all non-child links (root, self, etc.)
                unique_links.append(link)
        catalog.links = unique_links

        return catalog, catalog_local_path

    def upload_directory_to_s3(self, directory_path, bucket_name, destination_path, max_workers=32):
        upload_tasks = []
        for root, _, files in os.walk(directory_path):
            for file in files:
                file_path = os.path.join(root, file)
                s3_key = os.path.join(destination_path, os.path.relpath(file_path, directory_path))
                upload_tasks.append((file_path, s3_key))
        total = len(upload_tasks)
        uploaded = [0]
        log_every = 10_000

        def upload_one(args):
            file_path, s3_key = args
            try:
                self.s3_client.upload_file(file_path, bucket_name, s3_key)
                return True
            except (NoCredentialsError, ClientError) as e:
                logging.error("Failed to upload %s to s3://%s/%s: %s", file_path, bucket_name, s3_key, e)
                return False

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(upload_one, task): task for task in upload_tasks}
            for future in as_completed(futures):
                if future.result():
                    uploaded[0] += 1
                if uploaded[0] % log_every == 0 and uploaded[0] > 0:
                    logging.info("Uploaded %s / %s files to S3", uploaded[0], total)
        logging.info("Uploaded %s files to S3", total)

    def update_collection(self, collection, catalog_id, catalog_path, bucket_name):
        with tempfile.TemporaryDirectory() as temp_dir:
            catalog_key = f"{catalog_path}catalog.json"
            catalog, catalog_local_path = self.download_catalog_and_collections(catalog_key, bucket_name, temp_dir)

            catalog.set_root(catalog)
            catalog.set_self_href(catalog_local_path)

            try:
                catalog.remove_child(catalog_id)
            except (KeyError, Exception) as e:
                # KeyError: child doesn't exist
                # STACError/FileNotFoundError: child link exists but file is missing
                if "KeyError" in str(type(e).__name__):
                    pass
                elif "does not resolve to a STAC object" in str(e) or "No such file or directory" in str(e):
                    print(
                        f"Warning: Could not remove existing child '{catalog_id}' (missing file), will replace with new version"
                    )
                    pass
                else:
                    raise

            catalog.add_child(collection)

            catalog.normalize_and_save(
                root_href=temp_dir, catalog_type=pystac.CatalogType.SELF_CONTAINED, skip_unresolved=True
            )

            self.upload_directory_to_s3(temp_dir, bucket_name, catalog_path)

    def generate_href(self, bucket_name, path, link_type, expiration=7 * 24 * 60 * 60):
        try:
            if link_type == "url":
                # Generate presigned URL
                signed_url = self.s3_client.generate_presigned_url(
                    "get_object", Params={"Bucket": bucket_name, "Key": path}, ExpiresIn=expiration
                )

                # Validate URL
                try:
                    response = requests.head(signed_url, timeout=5)
                    is_valid = response.status_code == 200
                except requests.RequestException as e:
                    is_valid = False

                return signed_url, is_valid

            elif link_type == "uri":
                # Generate S3 URI
                s3_uri = f"s3://{bucket_name}/{path}"

                # Validate object exists
                try:
                    self.s3_client.head_object(Bucket=bucket_name, Key=path)
                    is_valid = True
                except (ClientError, ParamValidationError) as e:
                    is_valid = False

                return s3_uri, is_valid

            else:
                raise ValueError("link_type must be either 'url' or 'uri'")

        except NoCredentialsError:
            raise ValueError("Credentials not available")


class RasterUtils:
    @staticmethod
    def create_preview(raster_path, preview_path, size=(256, 256), chunk_size=1024):
        """Create preview using rasterio decimated read (overview levels) instead of full raster read
        - Directly creates thumbnail at target size without intermediate steps
        - Skips expensive coarsen operations
        - 10x-50x faster for large rasters

        Args:
            raster_path: Path to input raster file
            preview_path: Path to save preview image
            size: Tuple of (width, height) for final preview size
            chunk_size: Size of chunks for processing (deprecated, kept for compatibility)
        """
        # Use rasterio for efficient decimated reading
        with rasterio.open(raster_path) as src:
            # Calculate decimation factor to read directly at thumbnail size
            height, width = src.height, src.width
            max_width, max_height = size

            # Calculate output size maintaining aspect ratio
            scale = min(max_width / width, max_height / height)
            out_width = int(width * scale)
            out_height = int(height * scale)

            # Read decimated data directly at thumbnail resolution
            # This is MUCH faster than reading full resolution and downsampling
            data = src.read(1, out_shape=(out_height, out_width), resampling=rasterio.enums.Resampling.average)

            # Convert to boolean mask (non-zero = data)
            mask = data != 0

            # Create RGBA image
            img_data_rgba = np.zeros((out_height, out_width, 4), dtype=np.uint8)
            img_data_rgba[~mask] = [255, 255, 255, 255]  # White for no data
            img_data_rgba[mask] = [0, 0, 0, 255]  # Black for data

            # Create and save PIL image
            pil_image = Image.fromarray(img_data_rgba, "RGBA")
            pil_image.save(preview_path, format="PNG")

    @staticmethod
    def count_pixels(raster_path, values=None):
        raster = rioxarray.open_rasterio(raster_path, masked=True, chunks=True)
        band1 = raster.sel(band=1)

        if values is None:
            pixel_count = (band1 != 0).sum().compute().item()
        else:
            mask = False
            for value in values:
                mask |= band1 == value
            pixel_count = mask.sum().compute().item()

        return pixel_count

    @staticmethod
    def get_max_value(raster_path):
        """Get the maximum value from a raster file."""
        try:
            raster = rioxarray.open_rasterio(raster_path, masked=True, chunks=True)
            band1 = raster.sel(band=1)
            max_val = float(band1.max().compute().item())
            return max_val
        except Exception as e:
            logging.error(f"Error getting max value from {raster_path}: {e}")
            return None

    @staticmethod
    def get_wkt2_string(raster_path):
        with rasterio.open(raster_path) as src:
            crs_info = src.crs.to_wkt()
            if crs_info:
                wkt = CRS.from_wkt(crs_info)
                wkt2_string = wkt.to_wkt(version="WKT2_2018_SIMPLIFIED")
                return wkt2_string
            else:
                raise ValueError(f"EPSG code not found for raster: {raster_path}")

    @staticmethod
    def get_huc8_geometry(huc8):
        wbd = pgh.WBD("huc8")
        huc8_geom = wbd.byids("huc8", [huc8])
        return huc8_geom.geometry.iloc[0]
