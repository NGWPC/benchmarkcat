import io
import json
import logging
import os
import pdb
import tempfile
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

import boto3
import geopandas as gpd
import numpy as np
import pandas as pd
import pygeohydro as pgh
import pystac
import rasterio
import requests
import rioxarray
import xarray as xr
from botocore.exceptions import ClientError, NoCredentialsError, ParamValidationError
from google.cloud import storage
from PIL import Image
from pyproj import CRS
from shapely.geometry import Polygon, shape


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

    def upload_directory_to_s3(self, directory_path, bucket_name, destination_path):
        for root, _, files in os.walk(directory_path):
            for file in files:
                file_path = os.path.join(root, file)
                s3_key = os.path.join(destination_path, os.path.relpath(file_path, directory_path))
                try:
                    self.s3_client.upload_file(file_path, bucket_name, s3_key)
                    print(f"Uploaded {file_path} to s3://{bucket_name}/{s3_key}")
                except (NoCredentialsError, ClientError) as e:
                    print(f"Failed to upload {file_path} to s3://{bucket_name}/{s3_key}: {e}")

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


class FlowfileUtils:
    @staticmethod
    def download_flowfiles(bucket_name, flowfile_keys, s3_client):
        dataframes = []
        for flowfile_key in flowfile_keys:
            response = s3_client.get_object(Bucket=bucket_name, Key=flowfile_key)
            flowfile_content = response["Body"].read().decode("utf-8")
            df = pd.read_csv(io.StringIO(flowfile_content))
            dataframes.append(df)
        return dataframes

    @staticmethod
    def extract_flowstats(flowfile_dfs):
        flowstats_list = []
        for flowfile_df in flowfile_dfs:
            flowstats = {}
            for column in flowfile_df.columns:
                if flowfile_df[column].dtype in ["float64", "int64"]:
                    min_value = flowfile_df[column].min()
                    max_value = flowfile_df[column].max()
                    mean_value = flowfile_df[column].mean()
                    flowstats[column] = {"Min": min_value, "Max": max_value, "Mean": mean_value}
            flowstats_list.append(flowstats)
        return flowstats_list

    @staticmethod
    def create_flowfile_object(flowfile_ids, flowstats_list, columns_list):
        flowfile_objects = {}

        while len(columns_list) < len(flowfile_ids):
            columns_list.append(columns_list[-1])

        for flowfile_id, flowstats, columns in zip(flowfile_ids, flowstats_list, columns_list):
            if "discharge" in flowstats:
                second_column = "discharge"
            elif "streamflow" in flowstats:
                second_column = "streamflow"
            else:
                raise ValueError("Neither 'discharge' nor 'streamflow' found in DataFrame columns")
            if second_column in flowstats:
                flow_summaries = {
                    "Flowstats": {
                        "discharge": {
                            "Min": float(flowstats[second_column]["Min"]),
                            "Max": float(flowstats[second_column]["Max"]),
                            "Mean": float(flowstats[second_column]["Mean"]),
                        }
                    }
                }

                flowfile_objects[flowfile_id] = {**flow_summaries, "columns": columns}
            else:
                raise KeyError(f"Column discharge not found in flowstats")

        return flowfile_objects


class AnaFlowProcessor:
    """Handles all flow data processing and NWM data extraction."""

    VALID_REGIONS = {"conus", "alaska", "hawaii"}

    @staticmethod
    def detect_region(bbox: List[float]) -> str:
        """
        Detect NWM region (conus/alaska/hawaii) from bounding box. Need this so can know which ana forecast file to download.

        Args:
            bbox: [min_lon, min_lat, max_lon, max_lat] in EPSG:4326

        Returns:
            'conus', 'alaska', or 'hawaii'
        """
        min_lon, min_lat, max_lon, max_lat = bbox

        # Alaska bounds (approximate)
        if min_lat > 51 and max_lon < -130:
            return "alaska"

        # Hawaii bounds (approximate)
        if min_lat > 18 and max_lat < 23 and min_lon > -161 and max_lon < -154:
            return "hawaii"

        # Default to CONUS
        return "conus"

    def __init__(self, nwm_flows_gdf):
        """
        Initialize AnaFlowProcessor.

        Args:
            nwm_flows_gdf: GeoDataFrame containing NWM hydrofabric features.
                          Expected to have 'ID' column for feature IDs and geometry.
        """
        self.nwm_flows_gdf = nwm_flows_gdf
        self.gcs_client = storage.Client.create_anonymous_client()
        self.nwm_bucket = self.gcs_client.bucket("national-water-model")

    def find_peak_discharge_hour(
        self, polygon: Polygon, start_datetime: datetime, end_datetime: datetime, region: str = "conus"
    ) -> Optional[Tuple[datetime, pd.DataFrame, str]]:
        """
        Find the hour with maximum discharge within a time range.

        Args:
            polygon: Shapely polygon of the area of interest
            start_datetime: Start of time range
            end_datetime: End of time range
            region: 'conus', 'alaska', or 'hawaii'

        Returns:
            Tuple of (peak_datetime, flow_dataframe, nwm_version) or None if no data found

        Raises:
            ValueError: If no NWM features or flow data are found
        """
        if region not in self.VALID_REGIONS:
            raise ValueError(f"Invalid region. Must be one of {self.VALID_REGIONS}")

        try:
            # Get features in polygon
            feature_ids = self.get_features_in_polygon(polygon, region)
            if not feature_ids:
                error_message = f"No NWM features found in polygon for region {region}"
                logging.warning(error_message)
                raise ValueError(error_message)

            # Round to nearest hours
            start_hour = self._get_closest_hour(start_datetime)
            end_hour = self._get_closest_hour(end_datetime)

            # Generate list of hours to check
            hours_to_check = []
            current_hour = start_hour
            while current_hour <= end_hour:
                hours_to_check.append(current_hour)
                current_hour += timedelta(hours=1)

            logging.info(f"Checking {len(hours_to_check)} hours from {start_hour} to {end_hour}")

            # Track peak discharge
            peak_discharge = -1
            peak_hour = None
            peak_data = None
            peak_version = None

            # Check each hour
            for hour in hours_to_check:
                flow_data_result = self.get_flow_data(hour, region)

                if flow_data_result is None:
                    logging.debug(f"No data for {hour}, skipping")
                    continue

                flow_data, nwm_version = flow_data_result

                # Filter to features in polygon
                filtered_flow_data = flow_data[flow_data["feature_id"].isin(feature_ids)]

                if filtered_flow_data.empty:
                    logging.debug(f"No matching features for {hour}, skipping")
                    continue

                # Get max discharge for this hour
                max_discharge = filtered_flow_data["discharge"].max()

                if max_discharge > peak_discharge:
                    peak_discharge = max_discharge
                    peak_hour = hour
                    peak_data = filtered_flow_data
                    peak_version = nwm_version
                    logging.info(f"New peak: {max_discharge:.2f} m³/s at {hour}")

            if peak_hour is None:
                error_message = f"No NWM flow data found for time range in region {region}"
                logging.warning(error_message)
                raise ValueError(error_message)

            logging.info(f"Peak discharge: {peak_discharge:.2f} m³/s at {peak_hour}")
            return peak_hour, peak_data, peak_version

        except Exception as e:
            logging.error(f"Error finding peak discharge: {str(e)}")
            raise

    def create_flowfile(
        self, polygon: Polygon, target_datetime: datetime, region: str = "conus", item_id: Optional[str] = None
    ) -> Tuple[Optional[pd.DataFrame], Optional[str], Optional[str]]:
        """
        Create flowfile for a given polygon and datetime.

        Args:
            polygon: Shapely polygon of the area of interest
            target_datetime: Target datetime for flow data
            region: 'conus', 'alaska', or 'hawaii'
            item_id: Optional STAC item ID for generating filename

        Returns:
            Tuple of (DataFrame with feature_id and discharge columns, NWM version string, suggested filename)

        Raises:
            ValueError: If no NWM features or flow data are found
        """
        if region not in self.VALID_REGIONS:
            raise ValueError(f"Invalid region. Must be one of {self.VALID_REGIONS}")

        try:
            # Get features in polygon using region-specific hydrofabric
            feature_ids = self.get_features_in_polygon(polygon, region)
            if not feature_ids:
                error_message = f"No NWM features found in scene polygon for region {region}"
                logging.warning(error_message)
                raise ValueError(error_message)

            # Get flow data for features
            flow_data_result = self.get_flow_data(target_datetime, region)

            if flow_data_result is None:
                error_message = f"No NWM flow data found for datetime in region {region}"
                logging.warning(error_message)
                raise ValueError(error_message)

            flow_data, nwm_version = flow_data_result

            # Filter flow data to features in polygon
            filtered_flow_data = flow_data[flow_data["feature_id"].isin(feature_ids)]

            # Generate filename if item_id provided
            suggested_filename = None
            if item_id:
                suggested_filename = f"{item_id}_NWM_{nwm_version}_flowfile.csv"

            return filtered_flow_data, nwm_version, suggested_filename

        except Exception as e:
            logging.error(f"Error creating flowfile: {str(e)}")
            raise  # Re-raise the exception to be caught by calling code

    def get_features_in_polygon(self, polygon: Polygon, region: str) -> List[str]:
        """Get feature IDs that intersect with or are within the polygon."""
        if region not in self.VALID_REGIONS:
            raise ValueError(f"Invalid region. Must be one of {self.VALID_REGIONS}")

        # Use the hydrofabric GeoDataFrame provided at initialization
        features = self.nwm_flows_gdf

        # Ensure polygon is in same CRS as features
        # Polygon is assumed to be in EPSG:4326 (lat/lon) as it comes from a STAC item

        # Create a GeoDataFrame from the polygon in EPSG:4326
        polygon_gdf = gpd.GeoDataFrame([1], geometry=[polygon], crs="EPSG:4326")

        # Reproject polygon to match features CRS
        if features.crs:
            polygon_gdf = polygon_gdf.to_crs(features.crs)

        # Get the reprojected polygon
        reprojected_polygon = polygon_gdf.geometry.iloc[0]

        # Perform spatial intersection
        mask = features.intersects(reprojected_polygon) | features.within(reprojected_polygon)
        matching_features = features[mask]["ID"].tolist()

        logging.info(f"Found {len(matching_features)} NWM features in polygon")

        return matching_features

    def get_flow_data(self, target_datetime: datetime, region: str = "conus") -> Optional[tuple[pd.DataFrame, str]]:
        """
        Get NWM flow data for a specific datetime and region.

        Args:
            target_datetime: Target datetime
            region: 'conus', 'alaska', or 'hawaii'

        Returns:
            Tuple of (DataFrame with feature_id and streamflow columns, NWM version number)
            or None if no data found
        """
        closest_hour = self._get_closest_hour(target_datetime)

        file_pattern = self._construct_file_pattern(closest_hour, region)

        # Get blob
        blob = self.nwm_bucket.blob(file_pattern)
        if not blob.exists():
            logging.warning(f"No NWM file found for {closest_hour}")
            return None

        try:
            # Create temporary file for NetCDF data
            with tempfile.NamedTemporaryFile(suffix=".nc") as temp_file:
                blob.download_to_filename(temp_file.name)

                # Open NetCDF file
                with xr.open_dataset(temp_file.name) as ds:
                    # Extract NWM version number
                    nwm_version = str(ds.attrs.get("NWM_version_number", "version_unknown"))

                    # Extract feature_id and streamflow
                    df = pd.DataFrame(
                        {
                            "feature_id": ds["feature_id"].values,
                            "discharge": ds["streamflow"].values,
                        }
                    )

                    return df, nwm_version

        except Exception as e:
            logging.error(f"Error processing NWM data: {str(e)}")
            return None

    @staticmethod
    def _get_closest_hour(target_datetime: datetime) -> datetime:
        """Get the closest hour in zulu time."""
        rounded = (target_datetime + timedelta(minutes=30)).replace(minute=0, second=0, microsecond=0)
        return rounded

    @staticmethod
    def _construct_file_pattern(datetime_obj: datetime, region: str) -> str:
        """
        Construct the NWM file pattern based on datetime and region.

        Args:
            datetime_obj: The datetime object
            region: 'conus', 'alaska', or 'hawaii'
        """
        region_map = {
            "conus": {
                "directory": "analysis_assim",
                "tm_format": "tm00",
                "suffix": "conus",
            },
            "alaska": {
                "directory": "analysis_assim_alaska",
                "tm_format": "tm00",
                "suffix": "alaska",
            },
            "hawaii": {
                "directory": "analysis_assim_hawaii",
                "tm_format": "tm0000",
                "suffix": "hawaii",
            },
        }

        if region not in region_map:
            raise ValueError(f"Invalid region. Must be one of {list(region_map.keys())}")

        region_info = region_map[region]
        date_str = datetime_obj.strftime("%Y%m%d")
        hour_str = datetime_obj.strftime("%H")

        return (
            f"nwm.{date_str}/{region_info['directory']}/"
            f"nwm.t{hour_str}z.analysis_assim.channel_rt."
            f"{region_info['tm_format']}.{region_info['suffix']}.nc"
        )

    def create_and_upload_flowfile_for_peak(
        self,
        geometry: dict,
        bbox: List[float],
        start_datetime: datetime,
        end_datetime: datetime,
        item_id: str,
        s3_utils,
        bucket_name: str,
        upload_prefix: str,
    ) -> Tuple[Optional[str], Optional[dict]]:
        """
        Create flowfile for peak discharge hour and upload to S3.

        Args:
            geometry: GeoJSON geometry dict
            bbox: [min_lon, min_lat, max_lon, max_lat]
            start_datetime: Start of flood event time range
            end_datetime: End of flood event time range
            item_id: STAC item ID for filename
            s3_utils: S3Utils instance for S3 operations
            bucket_name: S3 bucket name
            upload_prefix: S3 prefix where to upload flowfile

        Returns:
            Tuple of (S3 key for flowfile, flowfile_object for properties)
        """
        try:
            # Detect region from bbox
            region = self.detect_region(bbox)
            logging.info(f"Detected region: {region}")

            polygon = shape(geometry)

            # Find peak discharge hour
            logging.info(f"Finding peak discharge hour between {start_datetime} and {end_datetime}")
            peak_result = self.find_peak_discharge_hour(polygon, start_datetime, end_datetime, region)

            if peak_result is None:
                logging.warning("No peak discharge data found")
                return None, None

            peak_hour, flow_df, nwm_version = peak_result

            if flow_df is None or flow_df.empty:
                logging.warning("Empty flow dataframe")
                return None, None

            # Generate filename
            filename = f"{item_id}_NWM_{nwm_version}_flowfile.csv"

            # Upload CSV to S3
            with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
                flow_df.to_csv(f, index=False)
                temp_path = f.name

            s3_key = f"{upload_prefix.rstrip('/')}/{filename}"
            s3_utils.s3_client.upload_file(temp_path, bucket_name, s3_key)
            os.remove(temp_path)
            logging.info(f"Uploaded flowfile to s3://{bucket_name}/{s3_key}")

            # Create flowfile object for STAC properties
            flowfile_ids = [f"NWM_{nwm_version}_flowfile"]
            columns_list = [
                {
                    "feature_id": {
                        "Column description": "feature id that identifies the stream segment",
                        "Column data source": f"NWM {nwm_version} hydrofabric",
                        "data_href": "https://water.noaa.gov/resources/downloads/nwm/NWM_channel_hydrofabric.tar.gz",
                    },
                    "discharge": {
                        "Column description": "Discharge in m^3/s",
                        "Column data source": f"NWM {nwm_version} ANA discharge data",
                        "data_href": "https://registry.opendata.aws/nwm-archive/",
                    },
                }
            ]

            flowfile_dfs = [flow_df]
            flowstats = FlowfileUtils.extract_flowstats(flowfile_dfs)
            flowfile_object = FlowfileUtils.create_flowfile_object(flowfile_ids, flowstats, columns_list)

            return s3_key, flowfile_object

        except Exception as e:
            logging.error(f"Error creating and uploading flowfile: {e}")
            return None, None
