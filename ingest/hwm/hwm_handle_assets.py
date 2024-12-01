import os
import tempfile
import re
from datetime import datetime
import pdb
import pandas as pd
import logging
import json
import copy
import xarray as xr
import fsspec
import geopandas as gpd
from shapely.geometry import box
from osgeo import ogr, osr
from typing import Dict
from calendar import month_abbr, month_name

import ingest.hwm.hwm_stac as hwm_stac
from ingest.bench import FlowfileUtils

logging.basicConfig(level=logging.INFO)

class HWMAssetHandler:

    def __init__(self, s3_utils, bucket_name, derived_metadata_path, results_file="hwm_collection.parquet") -> None:
        self.s3_utils = s3_utils
        self.bucket_name = bucket_name
        self.albers_crs = hwm_stac.albers_crs
        self.derived_metadata_path = derived_metadata_path
        self.results_file = results_file
        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.local_results_file = os.path.join(script_dir, results_file)
        self.results_df = self.load_results()
        self.ds_url = hwm_stac.url_conus
        self.nwm_streams_path = os.path.join(os.path.dirname(script_dir), 'nwm_flows.gpkg')
    
        # Download the file if it doesn't exist
        if not os.path.exists(self.nwm_streams_path):
            print("downloading streams")
            self.s3_utils.s3_client.download_file(self.bucket_name, hwm_stac.nwm_streams, self.nwm_streams_path)
            print("done downloading streams")

    def load_results(self):
        try:
            # Attempt to download the Parquet file from S3
            self.s3_utils.s3_client.download_file(self.bucket_name, self.derived_metadata_path, self.local_results_file)
            logging.info(f"Successfully downloaded {self.derived_metadata_path} from s3://{self.bucket_name}/{self.derived_metadata_path}")
        except Exception as e:
            logging.warning(f"Failed to download {self.derived_metadata_path} from s3://{self.bucket_name}/{self.derived_metadata_path}: {e}")
            logging.info("Creating a new local results file.")

        # Check if the local results file exists and load it, otherwise create a new DataFrame
        if os.path.exists(self.local_results_file):
            df = pd.read_parquet(self.local_results_file)
            if 'event_id' not in df.columns:
                df['event_id'] = None
            return df
        else:
            # Initialize DataFrame with appropriate columns
            columns = {
                'event_id': pd.Series(dtype='str'),
                'flowfile_object': pd.Series(dtype='str'),  
                'flowfile_key': pd.Series(dtype='str'),
                'event_month': pd.Series(dtype='str'),
            }
            return pd.DataFrame(columns)

    def event_processed(self, event_id) -> bool:
        return event_id in self.results_df['event_id'].values

    def read_data_parquet(self, event_id):
        row = self.results_df[self.results_df['event_id'] == event_id]
        if not row.empty:
            result = row.to_dict(orient='records')[0]
            # Only convert 'flowfile_object' from JSON string back to dict
            if result.get('flowfile_object'):
                result['flowfile_object'] = json.loads(result['flowfile_object'])
            if result.get('event_month'):
                result['event_month'] = datetime.fromisoformat(result['event_month'])
            print(f"read event {event_id}")
            return result
        return {}

    def handle_assets(self, flowfile_dir, event_id, points) -> Dict:
        results = {}
        event_month, flowfile_object, flowfile_key = self.get_flowfile_object(event_id, points, flowfile_dir)

        results[event_id] = {
            "event_month": event_month,
            "flowfile_key": flowfile_key,
            "flowfile_object": flowfile_object  
        }

        self.write_data_parquet(results)
        return results[event_id]

    def get_flowfile_object(self, event_id, points, flowfile_dir):
        event_month = self.extract_date_from_event_id(event_id)
        if not event_month:
            logging.warning(f"Unable to extract date for event ID: {event_id}")
            return None, None, None
        start_time = event_month.replace(day=1)
        end_time = (start_time + pd.DateOffset(months=1)) - pd.Timedelta(days=1)
        if event_month and datetime(1979, 2, 1) <= event_month <= datetime(2023, 1, 31):
            ds = self.open_ds(self.ds_url)
            feature_ids = self.feature_ids_in_marks_bbox(points, self.nwm_streams_path, self.albers_crs)
            peak_time = self.get_peak_discharge_time(ds, feature_ids, start_time, end_time)
            flowfile_df = self.create_flowfile(ds, feature_ids, peak_time)

            if not flowfile_df.empty and 'discharge' in flowfile_df.columns:
                flowstats = FlowfileUtils.extract_flowstats([flowfile_df])
                flowfile_ids = ["NWM_v3_flowfile"]
                flowfile_key = f"{flowfile_dir.rstrip('/')}/{event_id}_flowfile.csv"
                # upload flowfile
                with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as temp_file:
                    flowfile_df.to_csv(temp_file.name, index=False)
                try:
                    self.s3_utils.s3_client.upload_file(temp_file.name, self.bucket_name, flowfile_key.lstrip('/'))
                    logging.info(f"Successfully uploaded flowfile to S3: s3://{self.bucket_name}/{flowfile_key}")
                except Exception as e:
                    logging.error(f"Failed to upload flowfile to S3: {e}")
                finally:
                    os.remove(temp_file.name)

                return event_month, FlowfileUtils.create_flowfile_object(flowfile_ids, flowstats, hwm_stac.columns_list), flowfile_key
            else:
                logging.warning(f"Empty flowfile DataFrame or missing 'discharge' column for event ID: {event_id}")
                return event_month, None, None
        else:
            logging.warning("No flowfile detected. Or event date outside of NWM v3 retrospective date range")
            return event_month, None, None

    def write_data_parquet(self, results):
        # Create a deep copy of results to avoid modifying the original
        results_copy = copy.deepcopy(results)

        # Convert objects to JSON strings for Parquet storage
        for path, data in results_copy.items():
            if 'flowfile_object' in data and isinstance(data['flowfile_object'], dict):
                data['flowfile_object'] = json.dumps(data['flowfile_object'])
            if 'event_month' in data and data['event_month'] is not None:
                if isinstance(data['event_month'], datetime):
                    data['event_month'] = data['event_month'].isoformat()

        new_df = pd.DataFrame.from_dict(results_copy, orient='index').reset_index().rename(columns={'index': 'event_id'})

        # Check if event_id already exists and remove it
        for event_id in new_df['event_id']:
            self.results_df = self.results_df[self.results_df['event_id'] != event_id]

        # Concatenate the new data
        self.results_df = pd.concat([self.results_df, new_df], ignore_index=True)
        
        # Write the updated DataFrame to the local Parquet file
        self.results_df.to_parquet(self.local_results_file, index=False)

    def open_ds(self,url):
        return xr.open_zarr(
            fsspec.get_mapper(url, anon=True), consolidated=True, mask_and_scale=True
        )['streamflow'].drop_vars(['latitude', 'elevation', 'gage_id', 'longitude', 'order'])

    def feature_ids_in_marks_bbox(self, geometries, nwm_streams_path, albers_crs):
        """Identify feature IDs inside a bbox that encapsulates all the points using OGR spatial filter."""
        # Create GeoDataFrame from points to get bounds, explicitly setting CRS
        points = gpd.GeoDataFrame(geometry=geometries, crs="EPSG:4326")
    
        points_transformed = points.to_crs(albers_crs)
        minx, miny, maxx, maxy = points_transformed.total_bounds

        # Open the GeoPackage file
        ds = ogr.Open(nwm_streams_path)
        layer = ds.GetLayerByName("nwm_streams")
    
        # Set spatial filter using transformed bbox
        layer.SetSpatialFilterRect(minx, miny, maxx, maxy)
    
        # Collect feature IDs that intersect the bbox
        feature_ids = []
        for feature in layer:
            feature_ids.append(feature.GetField('ID'))

        # Clean up
        ds = None
    
        return sorted(feature_ids)

    def extract_date_from_event_id(self, event_id):
        match = re.search(r'(\d{4})_([A-Za-z]+)', event_id)
        if match:
            year, month = match.groups()
            
            # Create a dictionary for both full and abbreviated month names
            month_dict = {**{m.lower(): i for i, m in enumerate(month_abbr) if m},
                          **{m.lower(): i for i, m in enumerate(month_name) if m}}
            
            month_lower = month.lower()
            if month_lower in month_dict:
                month_num = month_dict[month_lower]
                try:
                    return datetime(int(year), month_num, 1)
                except ValueError:
                    print(f"Warning: Invalid date for event ID: {event_id}")
                    return None
            else:
                print(f"Warning: Unknown month format in event ID: {event_id}")
                return None
        else:
            print(f"Warning: Unable to extract date from event ID: {event_id}")
            return None

    def get_peak_discharge_time(self, ds, feature_ids, start_time, end_time):
        ts = ds.sel(feature_id=feature_ids, time=slice(start_time, end_time))
        peak_times = ts.idxmax(dim='time')
        
        # Convert xarray DataArray to pandas Series
        peak_times_series = peak_times.to_series()

        mode_result = peak_times_series.mode()
        
        if mode_result.empty:
            # If there's no mode (all values occur once), use the median time
            modal_peak_time = peak_times_series.median()
        else:
            modal_peak_time = mode_result.iloc[0]
        
        return pd.Timestamp(modal_peak_time)

    def create_flowfile(self, ds, feature_ids, peak_time):
        try:
            # Use method='nearest' to select the nearest available time step
            df = ds.sel(feature_id=feature_ids, time=peak_time, method='nearest').to_dataframe().reset_index()
            
            # Log the actual time selected
            actual_time = df['time'].iloc[0]
            logging.info(f"Requested peak time: {peak_time}, Actual time used: {actual_time}")
            
            # Ensure 'streamflow' column exists and rename it to 'discharge'
            if 'streamflow' in df.columns:
                df = df[['feature_id', 'streamflow']].rename(columns={'streamflow': 'discharge'})
            else:
                logging.error(f"'streamflow' column not found in DataFrame. Available columns: {df.columns}")
                return pd.DataFrame()
            return df
        except Exception as e:
            logging.error(f"Error creating flowfile DataFrame: {e}")
            return pd.DataFrame()

    def upload_modified_parquet(self):
        try:
            # Upload the local Parquet file back to S3
            self.s3_utils.s3_client.upload_file(self.local_results_file, self.bucket_name, self.derived_metadata_path)
            logging.info(f"Successfully uploaded {self.local_results_file} to s3://{self.bucket_name}/{self.derived_metadata_path}")
        except Exception as e:
            logging.error(f"Failed to upload {self.local_results_file} to s3://{self.bucket_name}/{self.derived_metadata_path}: {e}")
        finally:
            # Remove the local Parquet file
            if os.path.exists(self.local_results_file):
                os.remove(self.local_results_file)
                logging.info(f"Removed local file {self.local_results_file}")

