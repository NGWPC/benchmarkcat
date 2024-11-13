import os
import copy
import json
import logging
import tempfile
import rasterio
import pandas as pd
import geopandas as gpd
from shapely.geometry import shape
from typing import Dict, Any, List
from .ripple_stac import RippleInfo, RasterHandler
from ingest.bench import FlowfileUtils, RasterUtils, S3Utils

class RippleFIMAssetHandler:
    def __init__(self, s3_utils, bucket_name, derived_metadata_path) -> None:
        self.s3_utils = s3_utils
        self.bucket_name = bucket_name
        self.derived_metadata_path = derived_metadata_path
        self.results_file = "ripple_fim_collection.parquet"
        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.local_results_file = os.path.join(script_dir, self.results_file)
        self.results_df = self.load_results()

    def load_results(self):
        try:
            self.s3_utils.s3_client.download_file(self.bucket_name, self.derived_metadata_path, self.local_results_file)
            logging.info(f"Successfully downloaded {self.derived_metadata_path}")
        except Exception as e:
            logging.warning(f"Failed to download {self.derived_metadata_path}: {e}")
            logging.info("Creating new local results file.")

        if os.path.exists(self.local_results_file):
            return pd.read_parquet(self.local_results_file)
        else:
            columns = {
                'item_path': pd.Series(dtype='str'),
                'source': pd.Series(dtype='str'),
                'geometry': pd.Series(dtype='str'),
                'bbox': pd.Series(dtype='str'),
                'magnitudes': pd.Series(dtype='str'),
                'extent_areas': pd.Series(dtype='str'),
                'wkt2_string': pd.Series(dtype='str')
            }
            return pd.DataFrame(columns)

    def assets_processed(self, item_path: str) -> bool:
        """Check if assets for this item path have been processed"""
        return item_path in self.results_df['item_path'].values

    def read_data_parquet(self, item_path: str) -> Dict[str, Any]:
        """Read processed data for an item path from the parquet file"""
        row = self.results_df[self.results_df['item_path'] == item_path]
        if not row.empty:
            result = row.to_dict(orient='records')[0]
            # Convert JSON strings back to objects
            for field in ['geometry', 'bbox', 'magnitudes', 'extent_areas', 'model_domains']:
                if result.get(field):
                    try:
                        result[field] = json.loads(result[field])
                    except (json.JSONDecodeError, TypeError):
                        logging.warning(f"Could not decode {field} for {item_path}")
            return result
        return {}

    def handle_assets(self, item_path: str, source: str) -> Dict[str, Any]:
        results = {}
        magnitudes = []
        extent_areas = {}
        model_domains = {}
    
        # Get list of tiff files and their magnitudes
        tiff_files = self.s3_utils.list_files_with_extensions(self.bucket_name, item_path, ['.tif'])
        for tiff in tiff_files:
            magnitude = os.path.basename(tiff).split('_')[0]
            magnitudes.append(magnitude)
        
            with tempfile.TemporaryDirectory() as tmpdir:
                local_tiff = os.path.join(tmpdir, os.path.basename(tiff))
                self.s3_utils.s3_client.download_file(self.bucket_name, tiff, local_tiff)
            
                # Get geometry, bbox, and model domain for each magnitude
                convex_hull, bbox, domain = RasterHandler.create_domain_geometry(local_tiff)
            
                # For first magnitude, set item geometry and bbox
                if magnitude == magnitudes[0]:
                    wkt2_string = RasterHandler.get_wkt2_string(local_tiff)
                
                # Store model domain multipolygon
                model_domains[magnitude] = domain
            
                # Create and upload gpkg for this magnitude
                gpkg_name = f"{magnitude}_model_domain.gpkg"
                local_gpkg = os.path.join(tmpdir, gpkg_name)
            
                # Create GeoDataFrame with model domain MultiPolygon
                with rasterio.open(local_tiff) as src:
                    gdf = gpd.GeoDataFrame(
                        {
                            'magnitude': [magnitude],
                            'geometry': [shape(domain)]
                        }, 
                        crs=src.crs
                    )
                    gdf.to_file(local_gpkg, driver='GPKG')
            
                s3_gpkg_path = os.path.join(item_path, gpkg_name)
                self.s3_utils.s3_client.upload_file(local_gpkg, self.bucket_name, s3_gpkg_path)
            
                # Calculate extent area
                extent_areas[magnitude] = RasterHandler.calculate_extent_area(local_tiff)

        results[item_path] = {
            "source": source,
            "geometry": convex_hull,  # Use the last computed convex hull
            "bbox": bbox,  # Use the last computed bbox
            "magnitudes": magnitudes,
            "extent_areas": extent_areas,
            "model_domains": model_domains,
            "wkt2_string": wkt2_string
        }
    
        self.write_data_parquet(results)
        return results[item_path]

    def process_collection_flowfiles(self, asset_object_key: str) -> Dict:
        """Process collection-level CONUS flow files"""
        flowfile_ids = []
        flowfile_keys = []
        
        # Get all CONUS flow files
        flow_files = self.s3_utils.list_files_with_extensions(
            self.bucket_name, 
            asset_object_key,
            ['.csv']
        )
        
        for flow_file in flow_files:
            if 'conus_flows' in flow_file:
                magnitude = flow_file.split('_')[2]  # Extract '2yr', '5yr', etc.
                flowfile_id = f"conus_flows_{magnitude}"
                flowfile_ids.append(flowfile_id)
                flowfile_keys.append(flow_file)

        # Process flowfiles using existing utilities
        flowfile_dfs = FlowfileUtils.download_flowfiles(
            self.bucket_name,
            flowfile_keys,
            self.s3_utils.s3_client
        )
        flowstats_list = FlowfileUtils.extract_flowstats(flowfile_dfs)
        
        # Create flowfile object
        flowfile_object = FlowfileUtils.create_flowfile_object(
            flowfile_ids,
            flowstats_list,
            RippleInfo.columns_list
        )
        
        return {
            "flowfile_ids": flowfile_ids,
            "flowfile_keys": flowfile_keys,
            "flowfile_object": flowfile_object
        }

    def write_data_parquet(self, results):
        results_copy = copy.deepcopy(results)
    
        # Custom JSON encoder to handle numpy types
        class NumpyJSONEncoder(json.JSONEncoder):
            def default(self, obj):
                import numpy as np
                if isinstance(obj, np.integer):
                    return int(obj)
                if isinstance(obj, np.floating):
                    return float(obj)
                if isinstance(obj, np.ndarray):
                    return obj.tolist()
                return super().default(obj)
    
        for path, data in results_copy.items():
            # Ensure geometry and bbox are serialized
            if data.get('geometry'):
                data['geometry'] = json.dumps(data['geometry'])
            if data.get('bbox'):
                data['bbox'] = json.dumps(data['bbox'])
            if data.get('magnitudes'):
                data['magnitudes'] = json.dumps(data['magnitudes'])
            if data.get('extent_areas'):
                data['extent_areas'] = json.dumps(data['extent_areas'], cls=NumpyJSONEncoder)
            if data.get('model_domains'):
                data['model_domains'] = json.dumps(data['model_domains'])

        new_df = pd.DataFrame.from_dict(results_copy, orient='index').reset_index().rename(columns={'index': 'item_path'})
    
        for item_path in new_df['item_path']:
            self.results_df = self.results_df[self.results_df['item_path'] != item_path]
    
        self.results_df = pd.concat([self.results_df, new_df], ignore_index=True)
        self.results_df.to_parquet(self.local_results_file, index=False)

    def upload_modified_parquet(self):
        try:
            self.s3_utils.s3_client.upload_file(self.local_results_file, self.bucket_name, self.derived_metadata_path)
            logging.info(f"Successfully uploaded {self.local_results_file}")
        except Exception as e:
            logging.error(f"Failed to upload {self.local_results_file}: {e}")
        finally:
            if os.path.exists(self.local_results_file):
                os.remove(self.local_results_file)
                logging.info(f"Removed local file {self.local_results_file}")

