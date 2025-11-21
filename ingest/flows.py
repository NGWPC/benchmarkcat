import io
import logging
import os
import tempfile
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

import geopandas as gpd
import pandas as pd
import xarray as xr
from google.cloud import storage
from shapely.geometry import Polygon, shape


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
