import os
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
from typing import Dict

import hwm_stac 
from ingest.bench import FlowfileUtils

class HWMAssetHandler:

    def __init__(self, s3_utils, bucket_name, derived_metadata_path, results_file="hwm_collection.parquet") -> None:
        self.s3_utils = s3_utils
        self.bucket_name = bucket_name
        self.derived_metadata_path = derived_metadata_path
        self.results_file = results_file
        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.local_results_file = os.path.join(script_dir, results_file)
        self.results_df = self.load_results()
        self.ds_url = hwm_stac.url_conus
        localstreams = os.path.join(script_dir,'nwm_flows.gpkg')
        self.s3_utils.s3_client.download_file(self.bucket_name, hwm_stac.nwm_streams,localstreams)
        print("loading streams")
        self.nwm_streams =  gpd.read_file(localstreams).to_crs(4326)
        print("done loading streams")

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
            }
            return pd.DataFrame(columns)

    def event_processed(self, event_id) -> bool:
        return event_id in self.results_df['event_id'].values

    def read_data_parquet(self, event_id):
        row = self.results_df[self.results_df['event_id'] == event_id]
        if not row.empty:
            result = row.to_dict(orient='records')[0]
            # Convert JSON strings back to objects if not empty
            if result.get('flowfile_key'):
                result['flowfile_key'] = json.loads(result['flowfile_key'])
                if result.get('flowfile_object'):
                    result['flowfile_object'] = json.loads(result['flowfile_object'])
            print(f"read event {event_id}")
            return result
        return {}

    def handle_assets(self, flowfile_dir, event_id, points) -> Dict:
        results = {}
        flowfile_object, flowfile_key = self.get_flowfile_object(event_id, points, flowfile_dir)

        results[event_id] = {
            "flowfile_key": flowfile_key,
            "flowfile_object": flowfile_object  
        }

        self.write_data_parquet(results)
        return results[event_id]

    def get_flowfile_object(self, event_id, points, flowfile_dir):
        event_date = self.extract_date_from_event_id(event_id)
        if not event_date:
            print(f"Warning: Unable to extract date for event ID: {event_id}")
            return None, None
        start_time = event_date.replace(day=1)
        end_time = (start_time + pd.DateOffset(months=1)) - pd.Timedelta(days=1)
        if event_date and datetime(1979, 2, 1) <= event_date <= datetime(2023, 1, 31):

            ds = self.open_ds(self.ds_url)
            feature_ids = self.feature_ids_in_marks_bbox(points, self.nwm_streams)
        
            peak_time = self.get_peak_discharge_time(ds, feature_ids, start_time, end_time)

            flowfile_df = self.create_flowfile(ds, feature_ids, peak_time)
        
            flowstats = FlowfileUtils.extract_flowstats(flowfile_df)
            flowfile_ids = ["NWM_v3_flowfile"]

            flowfile_key = f"{flowfile_dir}/{event_id}_flowfile"
            return FlowfileUtils.create_flowfile_object(flowfile_ids, flowstats, hwm_stac.columns_list), flowfile_key

        else:
            logging.warning("No flowfile detected. Or event date outside of NWM v3 retrospective date range")
            return None, None

    def write_data_parquet(self, results):
        # Create a deep copy of results to avoid modifying the original
        results_copy = copy.deepcopy(results)

        # Convert objects to JSON strings for Parquet storage
        for path, data in results_copy.items():
            if 'flowfile_object' in data and isinstance(data['flowfile_object'], dict):
                data['flowfile_object'] = json.dumps(data['flowfile_object'])
            if 'geometry' in data and isinstance(data['geometry'], dict):
                data['geometry'] = json.dumps(data['geometry'])
            if 'bbox' in data and isinstance(data['bbox'], list):
                data['bbox'] = json.dumps(data['bbox'])

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

    def feature_ids_in_marks_bbox(self,geometries, nwm_streams):
        """Identify feature IDs inside a bbox that encapsulates all the points."""
        points = gpd.GeoDataFrame(geometry=geometries)
        bbox = box(*points.total_bounds)
        return sorted(nwm_streams[nwm_streams.intersects(bbox)]['ID'].tolist())

    def extract_date_from_event_id(self, event_id):
        match = re.search(r'(\d{4})_([A-Za-z]+)', event_id)
        if match:
            year, month = match.groups()
            # Convert abbreviated month names to full names
            month_dict = {
                "Jan": "January", "Feb": "February", "Mar": "March", "Apr": "April",
                "May": "May", "Jun": "June", "Jul": "July", "Aug": "August",
                "Sep": "September", "Oct": "October", "Nov": "November", "Dec": "December"
            }
            full_month = month_dict.get(month, month)
            try:
                return datetime.strptime(f"{year} {full_month}", "%Y %B")
            except ValueError:
                print(f"Warning: Unable to parse date for event ID: {event_id}")
                return None
        return None

    def get_peak_discharge_time(self,ds, feature_ids, start_time, end_time):
        ts = ds.sel(feature_id=feature_ids, time=slice(start_time, end_time))
        peak_times = ts.idxmax(dim='time')
        modal_peak_time = peak_times.mode(dim='feature_id').values[0]
        return pd.Timestamp(modal_peak_time)

    def create_flowfile(self,ds, feature_ids, peak_time):
        df = ds.sel(feature_id=feature_ids, time=peak_time).to_dataframe().reset_index()
        return df[['feature_id', 'streamflow']].rename(columns={'streamflow': 'discharge'})

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

