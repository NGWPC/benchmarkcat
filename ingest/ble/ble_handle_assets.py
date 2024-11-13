import os
import copy
import pdb
import json
import logging
import tempfile
import pandas as pd
from shapely.geometry import shape
from typing import Dict, Any, List
from ingest.ble.ble_stac import GeoJSONHandler, BLEInfo
from ingest.bench import FlowfileUtils, RasterUtils, S3Utils

class BLEAssetHandler:
    def __init__(self, s3_utils, bucket_name, derived_metadata_path) -> None:
        results_file = "ble_collection.parquet"
        self.s3_utils = s3_utils
        self.bucket_name = bucket_name
        self.derived_metadata_path = derived_metadata_path
        self.results_file = results_file
        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.local_results_file = os.path.join(script_dir, results_file)
        self.results_df = self.load_results()

    def load_results(self):
        try:
            self.s3_utils.s3_client.download_file(self.bucket_name, self.derived_metadata_path, self.local_results_file)
            logging.info(f"Successfully downloaded {self.derived_metadata_path} from s3://{self.bucket_name}/{self.derived_metadata_path}")
        except Exception as e:
            logging.warning(f"Failed to download {self.derived_metadata_path} from s3://{self.bucket_name}/{self.derived_metadata_path}: {e}")
            logging.info("Creating a new local results file.")

        if os.path.exists(self.local_results_file):
            df = pd.read_parquet(self.local_results_file)
            return df
        else:
            columns = {
                'huc8_path': pd.Series(dtype='str'),
                'geometry': pd.Series(dtype='str'),
                'bbox': pd.Series(dtype='str'),
                'magnitudes': pd.Series(dtype='str'),
                'extent_area': pd.Series(dtype='str'),
                'flowfile': pd.Series(dtype='str'),
                'extent_paths': pd.Series(dtype='str'),
                'depth_paths': pd.Series(dtype='str'),
                'thumbnail': pd.Series(dtype='str'),
                'wkt2_string': pd.Series(dtype='str')
            }
            return pd.DataFrame(columns)

    def assets_processed(self, huc8_path) -> bool:
        return huc8_path in self.results_df['huc8_path'].values

    def read_data_parquet(self, huc8_path):
        row = self.results_df[self.results_df['huc8_path'] == huc8_path]
        if not row.empty:
            result = row.to_dict(orient='records')[0]
            if result.get('geometry'):
                result['geometry'] = json.loads(result['geometry'])
            if result.get('bbox'):
                result['bbox'] = json.loads(result['bbox'])
            if result.get('magnitudes'):
                result['magnitudes'] = json.loads(result['magnitudes'])
            if result.get('extent_area'):
                result['extent_area'] = json.loads(result['extent_area'])
            if result.get('flowfile'):
                result['flowfile'] = json.loads(result['flowfile'])
            if result.get('extent_paths'):
                result['extent_paths'] = json.loads(result['extent_paths'])
            if result.get('depth_paths'):
                result['depth_paths'] = json.loads(result['depth_paths'])
            if result.get('thumbnail'):
                result['thumbnail'] = result['thumbnail']
            if result.get('wkt2_string'):
                result['wkt2_string'] = result['wkt2_string']
            return result
        return {}

    def handle_assets(self, huc8_path) -> Dict[str, Any]:
        results = {}
        huc8 = huc8_path.strip('/').split('/')[-1]
        geometry = GeoJSONHandler.get_huc8_geometry(huc8)
        bbox = list(geometry.bounds)
        geometry = geometry.__geo_interface__

        magnitudes = ['100yr', '500yr']
        extent_area, extent_paths, depth_paths, wkt2_string = self.calculate_extent_area(huc8_path, magnitudes)
        flowfiles = self.create_flowfile_object(huc8_path, magnitudes)
        first_extent_path = extent_paths[magnitudes[0]]
        thumbnail = self.create_and_add_thumbnail(first_extent_path)

        results[huc8_path] = {
            "geometry": geometry,
            "bbox": bbox,
            "magnitudes": magnitudes,
            "extent_area": extent_area,
            "flowfile": flowfiles,
            "extent_paths": extent_paths,
            "depth_paths": depth_paths,
            "thumbnail": thumbnail,
            "wkt2_string": wkt2_string
        }
        self.write_data_parquet(results)
        return results[huc8_path]

    def calculate_extent_area(self, huc8_path, magnitudes):
        extent_area = {}
        extent_paths = {}
        depth_paths = {}
        wkt2_string = None
        for magnitude in magnitudes:
            magnitude_path = os.path.join(huc8_path, magnitude)
            tiff_files = self.s3_utils.list_files_with_extensions(self.bucket_name, magnitude_path, ['.tif'])
            extent_tiff = [t for t in tiff_files if 'extent' in t][0]  
            depth_tiff_list = [t for t in tiff_files if 'depth' in t]
        
            extent_paths[magnitude] = extent_tiff
            # Only add depth path if depth file exists
            if depth_tiff_list:
                depth_paths[magnitude] = depth_tiff_list[0]
        
            with tempfile.TemporaryDirectory() as tmpdir:
                local_extent_path = os.path.join(tmpdir, os.path.basename(extent_tiff))
                self.s3_utils.s3_client.download_file(self.bucket_name, extent_tiff, local_extent_path)
                area = RasterUtils.count_pixels(local_extent_path)
                # convert pixels to meters using resolution of rasters
                area = area * 3
                extent_area[magnitude] = area
                if not wkt2_string:
                    wkt2_string = RasterUtils.get_wkt2_string(local_extent_path)
    
        return extent_area, extent_paths, depth_paths, wkt2_string

    def create_flowfile_object(self, huc8_path, magnitudes):
        flowfile_ids = {}
        flowfile_keys = {}
        for magnitude in magnitudes:
            magnitude_path = os.path.join(huc8_path, magnitude)
            flowfile_paths = self.s3_utils.list_files_with_extensions(self.bucket_name, magnitude_path, ['.csv'])
            flowfile_key = flowfile_paths[0]
            flowfile_ids[magnitude] = f"{magnitude}_flow_file"
            flowfile_keys[magnitude] = flowfile_key

        flowfile_dfs = FlowfileUtils.download_flowfiles(self.bucket_name, list(flowfile_keys.values()), self.s3_utils.s3_client)
        flowstats_list = FlowfileUtils.extract_flowstats(flowfile_dfs)

        # Use columns_list from BLEInfo
        columns_list = BLEInfo.columns_list

        flowfile_object = FlowfileUtils.create_flowfile_object(list(flowfile_ids.values()), flowstats_list, columns_list)
        return {"flowfile_ids": flowfile_ids, "flowfile_keys": flowfile_keys, "flowfile_object": flowfile_object}

    def create_and_add_thumbnail(self, first_extent_path):
        with tempfile.TemporaryDirectory() as tmpdir:
            local_extent_path = os.path.join(tmpdir, os.path.basename(first_extent_path))
            local_thumbnail_path = os.path.join(tmpdir, 'thumbnail.png')
            self.s3_utils.s3_client.download_file(self.bucket_name, first_extent_path, local_extent_path)
            thumbnail_s3_path = self.s3_utils.make_and_upload_thumbnail(local_extent_path, local_thumbnail_path, self.bucket_name, first_extent_path)
        return thumbnail_s3_path

    def write_data_parquet(self, results):
        results_copy = copy.deepcopy(results)
        for path, data in results_copy.items():
            if 'geometry' in data and isinstance(data['geometry'], dict):
                data['geometry'] = json.dumps(data['geometry'])
            if 'bbox' in data and isinstance(data['bbox'], list):
                data['bbox'] = json.dumps(data['bbox'])
            if 'magnitudes' in data and isinstance(data['magnitudes'], list):
                data['magnitudes'] = json.dumps(data['magnitudes'])
            if 'extent_area' in data and isinstance(data['extent_area'], dict):
                data['extent_area'] = json.dumps(data['extent_area'])
            if 'flowfile' in data and isinstance(data['flowfile'], dict):
                data['flowfile'] = json.dumps(data['flowfile'])
            if 'extent_paths' in data and isinstance(data['extent_paths'], dict):
                data['extent_paths'] = json.dumps(data['extent_paths'])
            if 'depth_paths' in data and isinstance(data['depth_paths'], dict):
                data['depth_paths'] = json.dumps(data['depth_paths'])
            if 'thumbnail' in data and isinstance(data['thumbnail'], str):
                data['thumbnail'] = data['thumbnail']
            if 'wkt2_string' in data and isinstance(data['wkt2_string'], str):
                data['wkt2_string'] = data['wkt2_string']

        new_df = pd.DataFrame.from_dict(results_copy, orient='index').reset_index().rename(columns={'index': 'huc8_path'})
        for huc8_path in new_df['huc8_path']:
            self.results_df = self.results_df[self.results_df['huc8_path'] != huc8_path]

        self.results_df = pd.concat([self.results_df, new_df], ignore_index=True)
        self.results_df.to_parquet(self.local_results_file, index=False)

    def upload_modified_parquet(self):
        try:
            self.s3_utils.s3_client.upload_file(self.local_results_file, self.bucket_name, self.derived_metadata_path)
            logging.info(f"Successfully uploaded {self.local_results_file} to s3://{self.bucket_name}/{self.derived_metadata_path}")
        except Exception as e:
            logging.error(f"Failed to upload {self.local_results_file} to s3://{self.bucket_name}/{self.derived_metadata_path}: {e}")
        finally:
            if os.path.exists(self.local_results_file):
                os.remove(self.local_results_file)
                logging.info(f"Removed local file {self.local_results_file}")
