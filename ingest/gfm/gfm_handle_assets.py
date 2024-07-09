import os
import json
import logging
import tempfile
import pandas as pd
from datetime import timezone
import geopandas as gpd
from shapely.geometry import mapping, shape
from ingest.gfm.gfm_stac import GFMInfo, GFMGeometryCreator 
from ingest.bench import FlowfileUtils
from typing import Dict
import copy

class GFMAssetHandler:

    """
    This is a class that exists to create a separation of concerns between metadata and data. Doing this to avoid having to reprocess data that has already been processed when you recreate your collection/collections.
    """

    def __init__(self, s3_utils, bucket_name, results_file="gfm_collection.parquet") -> None:
        self.s3_utils = s3_utils
        self.bucket_name = bucket_name
        self.results_file = results_file
        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.results_file = os.path.join(script_dir, results_file)
        self.results_df = self.load_results()

    def load_results(self):
        if os.path.exists(self.results_file):
            df = pd.read_parquet(self.results_file)
            if 'sent_ti_path' not in df.columns:
                df['sent_ti_path'] = None
            return df
        else:
            # Initialize DataFrame with appropriate columns
            columns = {
                'sent_ti_path': pd.Series(dtype='str'),
                'flowfile_object': pd.Series(dtype='str'),  
                'flowfile_key': pd.Series(dtype='str'),
                'thumbnail_key': pd.Series(dtype='str'),
                'main_cause': pd.Series(dtype='str'),
                'geometry': pd.Series(dtype='str'), 
                'bbox': pd.Series(dtype='str') 
            }
            return pd.DataFrame(columns)

    def tile_assets_processed(self, sent_ti_path) -> bool:
        return sent_ti_path in self.results_df['sent_ti_path'].values

    def read_data_parquet(self, sent_ti_path):
        row = self.results_df[self.results_df['sent_ti_path'] == sent_ti_path]
        if not row.empty:
            result = row.to_dict(orient='records')[0]
            # Convert JSON strings back to objects
            result['geometry'] = json.loads(result['geometry'])
            result['bbox'] = json.loads(result['bbox'])
            result['flowfile_object'] = json.loads(result['flowfile_object'])  # Convert back to dict
            print(f"read tile {sent_ti_path}")
            return result
        return {}

    def handle_assets(self, sent_ti_path, event_id) -> Dict:
        results = {}
        gdf_geom, main_cause = self.process_geopackage(event_id)
        flowfile_object, flowfile_key = self.get_flowfile_object(sent_ti_path, self.bucket_name)
        thumbnail_key = self.create_and_add_thumbnail(self.s3_utils, self.bucket_name, sent_ti_path)

        gfm_geom_creator = GFMGeometryCreator(bucket_name=self.bucket_name, s3_client=self.s3_utils.s3_client, gdf_geom=gdf_geom)
        geometry_dict, bbox = gfm_geom_creator.make_item_geom(self.s3_utils.list_resources_with_string(self.bucket_name, sent_ti_path, ['footprint'])[0])

        results[sent_ti_path] = {
            "flowfile_object": flowfile_object,  
            "flowfile_key": flowfile_key[0] if flowfile_key else None,
            "thumbnail_key": thumbnail_key,
            "main_cause": main_cause,
            "geometry": geometry_dict,  
            "bbox": bbox  
        }

        self.write_data_parquet(results)
        return results[sent_ti_path]

    def get_flowfile_object(self, sent_ti_path, bucket_name):
        flowfile_key = self.s3_utils.list_resources_with_string(bucket_name, sent_ti_path, ['flows'])
        if flowfile_key:
            flowfile_df = FlowfileUtils.download_flowfile(bucket_name, flowfile_key[0], self.s3_utils.s3_client)
            flowstats = FlowfileUtils.extract_flowstats(flowfile_df)
            flowfile_ids = ["NWM_v3_flowfile"]
            return FlowfileUtils.create_flowfile_object(flowfile_ids, flowstats, GFMInfo.columns_list), flowfile_key
        else:
            logging.warning("No flowfile detected")
            flowfile_key = None
            return None, flowfile_key

    def create_and_add_thumbnail(self, s3_utils, bucket_name, sent_ti_path):
        extent_paths = s3_utils.list_resources_with_string(bucket_name, sent_ti_path, ['OBSWATER']) 
        equi7tiles_list = [os.path.basename(filename).split('_')[1] for filename in extent_paths if len(os.path.basename(filename).split('_')) > 2]
        equi7tile = equi7tiles_list[0]

        with tempfile.TemporaryDirectory() as tmpdir:
            local_extent_path = os.path.join(tmpdir, f'{equi7tile}_extent.tif')
            local_thumbnail_path = os.path.join(tmpdir, f'{equi7tile}_extent_thumbnail.png')
            thumbnail_s3_path = s3_utils.make_and_upload_thumbnail(local_extent_path, local_thumbnail_path, bucket_name, extent_paths[0])
            return thumbnail_s3_path

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

        new_df = pd.DataFrame.from_dict(results_copy, orient='index').reset_index().rename(columns={'index': 'sent_ti_path'})

        # Check if sent_ti_path already exists and remove it
        for sent_ti_path in new_df['sent_ti_path']:
            self.results_df = self.results_df[self.results_df['sent_ti_path'] != sent_ti_path]

        # Concatenate the new data
        self.results_df = pd.concat([self.results_df, new_df], ignore_index=True)
        
        # Write the updated DataFrame to the Parquet file
        self.results_df.to_parquet(self.results_file, index=False)

    def process_geopackage(self, event_id):
        local_geopackage_path = '/tmp/dfo_all_usa_events_post_2015.gpkg'    
        self.download_geopackage(self.s3_utils.s3_client, self.bucket_name, 'benchmark/rs/dfo_all_usa_events_post_2015.gpkg', local_geopackage_path)
        gdf = self.load_geopackage(local_geopackage_path)
        gdf_geom = gdf.loc[gdf['dfo_id'] == int(event_id)].geometry.values[0]
        main_cause = gdf.loc[gdf['dfo_id'] == int(event_id), 'maincause'].values[0]
        return gdf_geom, main_cause
        
    def download_geopackage(self, s3, bucket_name, geo_package_key, local_path):
        s3.download_file(bucket_name, geo_package_key, local_path)

    def load_geopackage(self, local_path):
        return gpd.read_file(local_path)

    def get_event_datetimes(self, gdf, event_id):
        event_row = gdf[gdf['dfo_id'] == int(event_id)]
        dfo_start_datetime = pd.to_datetime(event_row['began'].values[0]).replace(tzinfo=timezone.utc)
        dfo_end_datetime = pd.to_datetime(event_row['ended'].values[0]).replace(tzinfo=timezone.utc)
        return dfo_start_datetime, dfo_end_datetime
