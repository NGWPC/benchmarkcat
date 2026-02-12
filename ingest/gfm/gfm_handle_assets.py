import copy
import json
import logging
import os
import re
import tempfile
from datetime import timezone
from typing import Dict

import geopandas as gpd
import pandas as pd

from ingest.flows import FlowfileUtils
from ingest.gfm.gfm_stac import GFMGeometryCreator, GFMInfo
from ingest.utils import RasterUtils


class GFMAssetHandler:
    """
    This is a class that exists to create a separation of concerns between metadata and data. Doing this to avoid having to reprocess data that has already been processed when you recreate your collection/collections.
    """

    def __init__(self, s3_utils, bucket_name, derived_metadata_path, results_file="gfm_collection.parquet") -> None:
        self.s3_utils = s3_utils
        self.bucket_name = bucket_name
        self.derived_metadata_path = derived_metadata_path
        self.results_file = results_file
        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.local_results_file = os.path.join(script_dir, results_file)
        self.results_df = self.load_results()

    def load_results(self):
        try:
            # Attempt to download the Parquet file from S3
            self.s3_utils.s3_client.download_file(self.bucket_name, self.derived_metadata_path, self.local_results_file)
            logging.info(
                f"Successfully downloaded {self.derived_metadata_path} from s3://{self.bucket_name}/{self.derived_metadata_path}"
            )
        except Exception as e:
            logging.warning(
                f"Failed to download {self.derived_metadata_path} from s3://{self.bucket_name}/{self.derived_metadata_path}: {e}"
            )
            logging.info("Creating a new local results file.")

        # Check if the local results file exists and load it, otherwise create a new DataFrame
        if os.path.exists(self.local_results_file):
            df = pd.read_parquet(self.local_results_file)
            if "sent_ti_path" not in df.columns:
                df["sent_ti_path"] = None
            if "equi7tile_areas" not in df.columns:
                df["equi7tile_areas"] = None
            return df
        else:
            # Initialize DataFrame with appropriate columns
            columns = {
                "sent_ti_path": pd.Series(dtype="str"),
                "flowfile_object": pd.Series(dtype="str"),
                "flowfile_key": pd.Series(dtype="str"),
                "thumbnail_key": pd.Series(dtype="str"),
                "main_cause": pd.Series(dtype="str"),
                "geometry": pd.Series(dtype="str"),
                "bbox": pd.Series(dtype="str"),
                "equi7tile_areas": pd.Series(dtype="str"),
            }
            return pd.DataFrame(columns)

    def calculate_equi7tile_areas(self, sent_ti_path: str, equi7tiles_list: list) -> tuple:
        """
        Calculate flooded area for provided equi7tiles using their observed water extent assets.

        Args:
            sent_ti_path (str): Path to the Sentinel tile directory
            equi7tiles_list (list): List of equi7tile identifiers to process

        Returns:
            tuple: (dict of areas by equi7tile, wkt2 string)
        """
        equi7tile_areas = {}
        wkt2_string = None

        for equi7tile in equi7tiles_list:
            # Get the OBSWATER file for this equi7tile
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
                    # Download and process the file
                    self.s3_utils.s3_client.download_file(self.bucket_name, file_path, local_path)

                    # Calculate area
                    pixel_count = RasterUtils.count_pixels(local_path)
                    # GFM data has 20m resolution
                    area = pixel_count * 20 * 20
                    equi7tile_areas[equi7tile] = area

                    # Get WKT2 string from first processed raster
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
            # Convert JSON strings back to objects
            for field in ["geometry", "bbox", "flowfile_object", "equi7tile_areas"]:
                if result.get(field):
                    result[field] = json.loads(result[field])
            print(f"read tile {sent_ti_path}")
            return result
        return {}

    def handle_assets(self, sent_ti_path: str, event_id: str, equi7tiles_list: list) -> Dict:
        """
        Process and handle all assets for a given Sentinel tile path.

        Args:
            sent_ti_path (str): Path to the Sentinel tile directory
            event_id (str): DFO event identifier
            equi7tiles_list (list): List of equi7tile identifiers to process

        Returns:
            Dict: Dictionary containing all processed asset information
        """
        results = {}
        gdf_geom, main_cause = self.process_geopackage(event_id)
        flowfile_object, flowfile_key = self.get_flowfile_object(sent_ti_path, self.bucket_name)

        thumbnail_key = self.create_and_add_thumbnail(self.s3_utils, self.bucket_name, sent_ti_path)

        # Calculate areas for provided equi7tiles
        equi7tile_areas, wkt2_string = self.calculate_equi7tile_areas(sent_ti_path, equi7tiles_list)

        try:
            footprint_keys = self.s3_utils.list_resources_with_string(self.bucket_name, sent_ti_path, ["footprint"])
            if footprint_keys:
                footprint_key = footprint_keys[0]
            else:
                geojson_files = self.s3_utils.list_resources_with_string(self.bucket_name, sent_ti_path, [".geojson"])
                s1_geojson = next((f for f in geojson_files if os.path.basename(f).startswith("S1")), None)
                if s1_geojson:
                    footprint_key = s1_geojson
                else:
                    raise IndexError("No footprint or S1*.geojson files found")

            gfm_geom_creator = GFMGeometryCreator(
                bucket_name=self.bucket_name, s3_client=self.s3_utils.s3_client, gdf_geom=gdf_geom
            )
            geometry_dict, bbox = gfm_geom_creator.make_item_geom(footprint_key)
        except (IndexError, Exception) as e:
            logging.warning(f"No valid geometry file found for {sent_ti_path}. Using null geometry. Error: {str(e)}")
            geometry_dict = None
            bbox = None

        results[sent_ti_path] = {
            "flowfile_object": flowfile_object,
            "flowfile_key": flowfile_key[0] if flowfile_key else None,
            "thumbnail_key": thumbnail_key,
            "main_cause": main_cause,
            "geometry": geometry_dict,
            "bbox": bbox,
            "equi7tile_areas": equi7tile_areas,
        }

        self.write_data_parquet(results)
        return results[sent_ti_path]

    def get_flowfile_object(self, sent_ti_path, bucket_name):
        flowfile_key = self.s3_utils.list_resources_with_string(bucket_name, sent_ti_path, ["flows"])

        if flowfile_key:
            flowfile_name = os.path.basename(flowfile_key[0])
            # gfm_exp: NWM_v2.1_flowfile.csv; gfm: nwm_retrospective_flows_v3.csv
            version_match = re.search(r"NWM_(v[\d.]+)_flowfile\.csv", flowfile_name)
            if not version_match:
                version_match = re.search(r"flows_(v[\d.]+)\.csv", flowfile_name, re.IGNORECASE)

            if version_match:
                version_string = version_match.group(1)
                flowfile_ids = [f"NWM_{version_string}_flowfile"]
                modified_columns = [copy.deepcopy(GFMInfo.columns_list[0])]
                modified_columns[0]["feature_id"]["Column data source"] = f"NWM {version_string} hydrofabric"
                modified_columns[0]["discharge"]["Column data source"] = f"NWM {version_string} ANA discharge data"
            else:
                logging.warning(f"Could not extract NWM version from filename: {flowfile_name}")
                flowfile_ids = ["NWM_unknown_version_flowfile"]
                modified_columns = [copy.deepcopy(GFMInfo.columns_list[0])]
                modified_columns[0]["feature_id"]["Column data source"] = "NWM unknown version hydrofabric"
                modified_columns[0]["discharge"]["Column data source"] = "NWM unknown version ANA discharge data"

            flowfile_df = FlowfileUtils.download_flowfiles(bucket_name, flowfile_key, self.s3_utils.s3_client)
            flowstats = FlowfileUtils.extract_flowstats(flowfile_df)
            return FlowfileUtils.create_flowfile_object(flowfile_ids, flowstats, modified_columns), flowfile_key
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
        # Create a deep copy to avoid modifying the original
        results_copy = copy.deepcopy(results)

        # Convert objects to JSON strings for Parquet storage
        for path, data in results_copy.items():
            for field in ["flowfile_object", "geometry", "bbox", "equi7tile_areas"]:
                if field in data and (isinstance(data[field], (dict, list)) or data[field] is None):
                    data[field] = json.dumps(data[field])

        new_df = (
            pd.DataFrame.from_dict(results_copy, orient="index").reset_index().rename(columns={"index": "sent_ti_path"})
        )

        # Remove existing entries
        for sent_ti_path in new_df["sent_ti_path"]:
            self.results_df = self.results_df[self.results_df["sent_ti_path"] != sent_ti_path]

        # Concatenate the new data
        self.results_df = pd.concat([self.results_df, new_df], ignore_index=True)

        # Write to local Parquet file
        self.results_df.to_parquet(self.local_results_file, index=False)

    def process_geopackage(self, event_id):
        local_geopackage_path = "/tmp/dfo_all_usa_events_post_2015.gpkg"
        self.download_geopackage(
            self.s3_utils.s3_client,
            self.bucket_name,
            "benchmark/rs/dfo_all_usa_events_post_2015.gpkg",
            local_geopackage_path,
        )
        gdf = self.load_geopackage(local_geopackage_path)
        gdf_geom = gdf.loc[gdf["dfo_id"] == int(event_id)].geometry.values[0]
        main_cause = gdf.loc[gdf["dfo_id"] == int(event_id), "maincause"].values[0]
        return gdf_geom, main_cause

    def download_geopackage(self, s3, bucket_name, geo_package_key, local_path):
        s3.download_file(bucket_name, geo_package_key, local_path)

    def load_geopackage(self, local_path):
        return gpd.read_file(local_path)

    def get_event_datetimes(self, gdf, event_id):
        event_row = gdf[gdf["dfo_id"] == int(event_id)]
        dfo_start_datetime = pd.to_datetime(event_row["began"].values[0]).replace(tzinfo=timezone.utc)
        dfo_end_datetime = pd.to_datetime(event_row["ended"].values[0]).replace(tzinfo=timezone.utc)
        return dfo_start_datetime, dfo_end_datetime

    def upload_modified_parquet(self):
        try:
            # Upload the local Parquet file back to S3
            self.s3_utils.s3_client.upload_file(self.local_results_file, self.bucket_name, self.derived_metadata_path)
            logging.info(
                f"Successfully uploaded {self.local_results_file} to s3://{self.bucket_name}/{self.derived_metadata_path}"
            )
        except Exception as e:
            logging.error(
                f"Failed to upload {self.local_results_file} to s3://{self.bucket_name}/{self.derived_metadata_path}: {e}"
            )
        finally:
            # Remove the local Parquet file
            if os.path.exists(self.local_results_file):
                os.remove(self.local_results_file)
                logging.info(f"Removed local file {self.local_results_file}")
