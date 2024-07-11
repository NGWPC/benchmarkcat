import os
import copy
import pdb
import json
import logging
import tempfile
import pandas as pd
from shapely.geometry import shape
from typing import Dict, Any, List
from ingest.gauge.gauge_stac import GeoJSONHandler, GaugeFIMInfo
from ingest.bench import FlowfileUtils, RasterUtils, S3Utils

class GaugeFIMAssetHandler:
    def __init__(self, s3_utils, bucket_name, derived_metadata_path, agency) -> None:
        results_file=f"{agency}_fim_collection.parquet"
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
                'gauge_path': pd.Series(dtype='str'),
                'geometry': pd.Series(dtype='str'), 
                'bbox': pd.Series(dtype='str'),
                'magnitude_paths': pd.Series(dtype='str'),
                'magnitudes': pd.Series(dtype='str'),
                'extent_area': pd.Series(dtype='str'),
                'flowfile': pd.Series(dtype='str'),
                'extent_paths': pd.Series(dtype='str'),
                'thumbnail': pd.Series(dtype='str'),
                'wkt2_string': pd.Series(dtype='int')
            }
            return pd.DataFrame(columns)

    def _assets_processed(self, gauge_path) -> bool:
        return gauge_path in self.results_df['gauge_path'].values

    def read_data_parquet(self, gauge_path):
        row = self.results_df[self.results_df['gauge_path'] == gauge_path]
        if not row.empty:
            result = row.to_dict(orient='records')[0]
            if result.get('geometry'):
                result['geometry'] = json.loads(result['geometry'])
            if result.get('bbox'):
                result['bbox'] = json.loads(result['bbox'])
            if result.get('magnitude_paths'):
                result['magnitude_paths'] = json.loads(result['magnitude_paths'])
            if result.get('magnitudes'):
                result['magnitudes'] = json.loads(result['magnitudes'])
            if result.get('extent_area'):
                result['extent_area'] = json.loads(result['extent_area'])
            if result.get('flowfile'):
                result['flowfile'] = json.loads(result['flowfile'])
            if result.get('extent_paths'):
                result['extent_paths'] = json.loads(result['extent_paths'])
            if result.get('thumbnail'):
                result['thumbnail'] = result['thumbnail']
            if result.get('wkt2_string'):
                result['wkt2_string'] = result['wkt2_string']
            return result
        return {}

    def handle_assets(self, gauge_path) -> Dict[str, Any]:
        results = {}
        geometry, bbox = GeoJSONHandler.process_shapefile(self.bucket_name, gauge_path, self.s3_utils.s3_client)
        magnitude_paths = self.s3_utils.list_resources_with_string(self.bucket_name, gauge_path, ['minor', 'major', 'moderate', 'action'],delimiter='/')
        magnitudes = [os.path.basename(os.path.normpath(path)) for path in magnitude_paths]
        extent_area, extent_paths, wkt2_string = self.calculate_extent_area( magnitude_paths, magnitudes)
        flowfiles = self.create_flowfile_object(magnitude_paths, magnitudes)
        first_extent_path = list(extent_paths.values())[0][0]
        thumbnail = self.create_and_add_thumbnail(first_extent_path)
        
        results[gauge_path] = {
            "geometry": geometry,
            "bbox": bbox,
            "magnitude_paths": magnitude_paths,
            "magnitudes": magnitudes,
            "extent_area": extent_area,
            "flowfile": flowfiles,
            "extent_paths": extent_paths,
            "thumbnail": thumbnail,
            "wkt2_string": wkt2_string
        }
        # pdb.set_trace()
        self.write_data_parquet(results)
        return results[gauge_path]

    def calculate_extent_area(self, magnitude_paths, magnitudes):
        extent_area = {}
        extent_paths = {}
        for magnitude, magnitude_path in zip(magnitudes, magnitude_paths):
            tiff_files = self.s3_utils.list_files_with_extensions(self.bucket_name, magnitude_path, ['.tif'])
            extent_area[magnitude] = {}
            extent_paths[magnitude] = tiff_files
            with tempfile.TemporaryDirectory() as tmpdir:
                first_tiff_processed = False  
                for tiff in tiff_files:
                    local_tiff_path = os.path.join(tmpdir, os.path.basename(tiff))
                    self.s3_utils.s3_client.download_file(self.bucket_name, tiff, local_tiff_path)
                    area = RasterUtils.count_pixels(local_tiff_path)
                    extent_area[magnitude][os.path.basename(tiff)] = area
                    if not first_tiff_processed:
                        wkt2_string = RasterUtils.get_wkt2_string(local_tiff_path)
                        first_tiff_processed = True
        return extent_area, extent_paths, wkt2_string

    def create_flowfile_object(self, magnitude_paths, magnitudes):
        flowfile_ids = []
        flowfile_keys = []
        for magnitude, magnitude_path in zip(magnitudes, magnitude_paths):
            flowfile_paths = self.s3_utils.list_files_with_extensions(self.bucket_name, magnitude_path, ['.csv'])
            for flowfile_path in flowfile_paths:
                flowfile_id = f"{magnitude}_flow_file"
                flowfile_ids.append(flowfile_id)
                flowfile_keys.append(flowfile_path)

        flowfile_dfs = FlowfileUtils.download_flowfiles(self.bucket_name, flowfile_keys, self.s3_utils.s3_client)
        flowstats_list = FlowfileUtils.extract_flowstats(flowfile_dfs)
        
        # Use columns_list from GaugeFIMInfo
        columns_list = GaugeFIMInfo.columns_list
        
        flowfile_object = FlowfileUtils.create_flowfile_object(flowfile_ids, flowstats_list, columns_list)
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
            if 'magnitude_paths' in data and isinstance(data['magnitude_paths'], list):
                data['magnitude_paths'] = json.dumps(data['magnitude_paths'])
            if 'magnitudes' in data and isinstance(data['magnitudes'], list):
                data['magnitudes'] = json.dumps(data['magnitudes'])
            if 'extent_area' in data and isinstance(data['extent_area'], dict):
                data['extent_area'] = json.dumps(data['extent_area'])
            if 'flowfile' in data and isinstance(data['flowfile'], dict):
                data['flowfile'] = json.dumps(data['flowfile'])
            if 'extent_paths' in data and isinstance(data['extent_paths'], dict):
                data['extent_paths'] = json.dumps(data['extent_paths'])
            if 'thumbnail' in data and isinstance(data['thumbnail'], str):
                data['thumbnail'] = data['thumbnail']
            if 'wkt2_string' in data and isinstance(data['wkt2_string'], int):
                data['wkt2_string'] = data['wkt2_string']

        new_df = pd.DataFrame.from_dict(results_copy, orient='index').reset_index().rename(columns={'index': 'gauge_path'})
        for gauge_path in new_df['gauge_path']:
            self.results_df = self.results_df[self.results_df['gauge_path'] != gauge_path]

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
