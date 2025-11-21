import copy
import json
import logging
import os
import pdb
import re
import tempfile
from typing import Dict

import pandas as pd
from shapely.geometry import mapping, shape

from ingest.flows import FlowfileUtils
from ingest.gfm.gfm_stac import GFMGeometryCreator, GFMInfo
from ingest.utils import RasterUtils


class GFMExpAssetHandler:
    def __init__(self, s3_utils, bucket_name, derived_metadata_path, results_file="gfm_expanded_collection.parquet"):
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
            logging.info(f"Successfully downloaded {self.derived_metadata_path}")
        except Exception as e:
            logging.warning(f"Failed to download {self.derived_metadata_path}: {e}")
            logging.info("Creating a new local results file.")

        if os.path.exists(self.local_results_file):
            df = pd.read_parquet(self.local_results_file)
            if "sent_ti_path" not in df.columns:
                df["sent_ti_path"] = None
            if "equi7tile_areas" not in df.columns:
                df["equi7tile_areas"] = None
            return df
        else:
            columns = {
                "sent_ti_path": pd.Series(dtype="str"),
                "flowfile_object": pd.Series(dtype="str"),
                "flowfile_key": pd.Series(dtype="str"),
                "thumbnail_key": pd.Series(dtype="str"),
                "geometry": pd.Series(dtype="str"),
                "bbox": pd.Series(dtype="str"),
                "equi7tile_areas": pd.Series(dtype="str"),
            }
            return pd.DataFrame(columns)

    def calculate_equi7tile_areas(self, sent_ti_path: str, equi7tiles_list: list) -> tuple:
        equi7tile_areas = {}
        wkt2_string = None

        for equi7tile in equi7tiles_list:
            obswater_files = self.s3_utils.list_resources_with_string(
                self.bucket_name, sent_ti_path, [f"{equi7tile}_ENSEMBLE_OBSWATER"]
            )

            if not obswater_files:
                logging.warning(f"No OBSWATER file found for equi7tile {equi7tile}")
                continue

            with tempfile.TemporaryDirectory() as tmpdir:
                file_path = obswater_files[0]
                local_path = os.path.join(tmpdir, os.path.basename(file_path))

                try:
                    self.s3_utils.s3_client.download_file(self.bucket_name, file_path, local_path)

                    pixel_count = RasterUtils.count_pixels(local_path)
                    area = pixel_count * 20 * 20
                    equi7tile_areas[equi7tile] = area

                    if wkt2_string is None:
                        wkt2_string = RasterUtils.get_wkt2_string(local_path)

                except Exception as e:
                    logging.error(f"Error processing {file_path}: {str(e)}")
                    equi7tile_areas[equi7tile] = None

        return equi7tile_areas, wkt2_string

    def tile_assets_processed(self, sent_ti_path) -> bool:
        return sent_ti_path in self.results_df["sent_ti_path"].values

    def read_data_parquet(self, sent_ti_path):
        row = self.results_df[self.results_df["sent_ti_path"] == sent_ti_path]
        if not row.empty:
            result = row.to_dict(orient="records")[0]
            for field in ["geometry", "bbox", "flowfile_object", "equi7tile_areas"]:
                if result.get(field):
                    result[field] = json.loads(result[field])
            return result
        return {}

    def handle_assets(self, sent_ti_path: str, equi7tiles_list: list) -> Dict:
        results = {}

        flowfile_object, flowfile_key = self.get_flowfile_object(sent_ti_path, self.bucket_name)
        thumbnail_key = self.create_and_add_thumbnail(self.s3_utils, self.bucket_name, sent_ti_path)
        equi7tile_areas, wkt2_string = self.calculate_equi7tile_areas(sent_ti_path, equi7tiles_list)

        # Try to get footprint first, then fall back to S1*.geojson
        try:
            footprint_keys = self.s3_utils.list_resources_with_string(self.bucket_name, sent_ti_path, ["footprint"])
            if footprint_keys:
                footprint_key = footprint_keys[0]
            else:
                # Look for S1*.geojson files
                geojson_files = self.s3_utils.list_resources_with_string(self.bucket_name, sent_ti_path, [".geojson"])
                s1_geojson = next((f for f in geojson_files if os.path.basename(f).startswith("S1")), None)
                if s1_geojson:
                    footprint_key = s1_geojson
                else:
                    raise IndexError("No footprint or S1*.geojson files found")

            gfm_geom_creator = GFMGeometryCreator(bucket_name=self.bucket_name, s3_client=self.s3_utils.s3_client)
            geometry_dict, bbox = gfm_geom_creator.make_item_geom(footprint_key)
        except (IndexError, Exception) as e:
            logging.warning(f"No valid geometry file found for {sent_ti_path}. Using null geometry. Error: {str(e)}")
            geometry_dict = None
            bbox = None

        results[sent_ti_path] = {
            "flowfile_object": flowfile_object,
            "flowfile_key": flowfile_key[0] if flowfile_key else None,
            "thumbnail_key": thumbnail_key,
            "geometry": geometry_dict,
            "bbox": bbox,
            "equi7tile_areas": equi7tile_areas,
        }

        self.write_data_parquet(results)
        return results[sent_ti_path]

    def get_flowfile_object(self, sent_ti_path, bucket_name):
        flowfile_key = self.s3_utils.list_resources_with_string(bucket_name, sent_ti_path, ["flow"])
        if flowfile_key:
            # Extract NWM version from the flowfile name
            flowfile_name = os.path.basename(flowfile_key[0])
            version_match = re.search(r"NWM_(v[\d.]+)_flowfile\.csv", flowfile_name)

            if version_match:
                version_string = version_match.group(1)
                flowfile_ids = [f"NWM_{version_string}_flowfile"]

                # Create a deep copy of the columns list to modify
                modified_columns = [{k: v.copy() for k, v in GFMInfo.columns_list[0].items()}]

                # Update the column data sources with correct version
                modified_columns[0]["feature_id"]["Column data source"] = f"NWM {version_string} hydrofabric"
                modified_columns[0]["discharge"]["Column data source"] = f"NWM {version_string} ANA discharge data"

                flowfile_df = FlowfileUtils.download_flowfiles(bucket_name, flowfile_key, self.s3_utils.s3_client)
                flowstats = FlowfileUtils.extract_flowstats(flowfile_df)
                return FlowfileUtils.create_flowfile_object(flowfile_ids, flowstats, modified_columns), flowfile_key
            else:
                logging.warning(f"Could not extract NWM version from filename: {flowfile_name}")
                modified_columns = [{k: v.copy() for k, v in GFMInfo.columns_list[0].items()}]
                modified_columns[0]["feature_id"]["Column data source"] = "NWM unknown version hydrofabric"
                modified_columns[0]["discharge"]["Column data source"] = "NWM unkown version ANA discharge data"

                flowfile_df = FlowfileUtils.download_flowfiles(bucket_name, flowfile_key, self.s3_utils.s3_client)
                flowstats = FlowfileUtils.extract_flowstats(flowfile_df)
                flowfile_ids = ["NWM_unknown_version_flowfile"]
                return FlowfileUtils.create_flowfile_object(flowfile_ids, flowstats, GFMInfo.columns_list), flowfile_key
        else:
            logging.warning("No flowfile detected")
            return None, None

    def create_and_add_thumbnail(self, s3_utils, bucket_name, sent_ti_path):
        extent_paths = s3_utils.list_resources_with_string(bucket_name, sent_ti_path, ["OBSWATER"])
        equi7tiles_list = [
            os.path.basename(filename).split("_")[1]
            for filename in extent_paths
            if len(os.path.basename(filename).split("_")) > 2
        ]

        if not equi7tiles_list:
            return None

        equi7tile = equi7tiles_list[0]
        with tempfile.TemporaryDirectory() as tmpdir:
            local_extent_path = os.path.join(tmpdir, f"{equi7tile}_extent.tif")
            local_thumbnail_path = os.path.join(tmpdir, f"{equi7tile}_extent_thumbnail.png")
            thumbnail_s3_path = s3_utils.make_and_upload_thumbnail(
                local_extent_path, local_thumbnail_path, bucket_name, extent_paths[0]
            )
            return thumbnail_s3_path

    def write_data_parquet(self, results):
        # Create a deep copy to avoid modifying the original. You will have issues if you don't do this.
        results_copy = copy.deepcopy(results)
        for path, data in results_copy.items():
            for field in ["flowfile_object", "geometry", "bbox", "equi7tile_areas"]:
                if field in data and (isinstance(data[field], (dict, list)) or data[field] is None):
                    data[field] = json.dumps(data[field])

        new_df = (
            pd.DataFrame.from_dict(results_copy, orient="index").reset_index().rename(columns={"index": "sent_ti_path"})
        )
        self.results_df = self.results_df[~self.results_df["sent_ti_path"].isin(new_df["sent_ti_path"])]
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
