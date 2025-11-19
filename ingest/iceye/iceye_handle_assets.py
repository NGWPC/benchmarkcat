import copy
import json
import logging
import os
import random
import tempfile
import time
from typing import Any, Dict, List

import geopandas as gpd
import pandas as pd
from shapely.geometry import MultiPoint, mapping, shape

from ingest.bench import AnaFlowProcessor, RasterUtils, S3Utils
from ingest.iceye.iceye_stac import AssetUtils, ICEYEInfo, extract_dates_from_metadata


class ICEYEAssetHandler:
    def __init__(self, s3_utils, bucket_name, derived_metadata_path, nwm_flows_gdf=None) -> None:
        results_file = "iceye_collection.parquet"
        self.s3_utils = s3_utils
        self.bucket_name = bucket_name
        self.derived_metadata_path = derived_metadata_path
        self.results_file = results_file
        self.nwm_flows_gdf = nwm_flows_gdf
        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.local_results_file = os.path.join(script_dir, results_file)
        self.results_df = self.load_results()

    def load_results(self):
        try:
            self.s3_utils.s3_client.download_file(self.bucket_name, self.derived_metadata_path, self.local_results_file)
            logging.info(
                f"Successfully downloaded {self.derived_metadata_path} from s3://{self.bucket_name}/{self.derived_metadata_path}"
            )
        except Exception as e:
            logging.warning(
                f"Failed to download {self.derived_metadata_path} from s3://{self.bucket_name}/{self.derived_metadata_path}: {e}"
            )
            logging.info("Creating a new local results file.")

        if os.path.exists(self.local_results_file):
            df = pd.read_parquet(self.local_results_file)
            return df
        else:
            columns = {
                "event_path": pd.Series(dtype="str"),
                "geometry": pd.Series(dtype="str"),
                "bbox": pd.Series(dtype="str"),
                "metadata": pd.Series(dtype="str"),
                "asset_paths": pd.Series(dtype="str"),
                "flooded_area": pd.Series(dtype="float"),
                "wkt2_string": pd.Series(dtype="str"),
                "thumbnails": pd.Series(dtype="str"),  # JSON list of thumbnail paths
                "depth_unit_info": pd.Series(dtype="str"),
                "flowfile_key": pd.Series(dtype="str"),
                "flowfile_object": pd.Series(dtype="str"),
            }
            return pd.DataFrame(columns)

    def assets_processed(self, event_path) -> bool:
        return event_path in self.results_df["event_path"].values

    def read_data_parquet(self, event_path):
        row = self.results_df[self.results_df["event_path"] == event_path]
        if not row.empty:
            result = row.to_dict(orient="records")[0]
            if result.get("geometry"):
                result["geometry"] = json.loads(result["geometry"])
            if result.get("bbox"):
                result["bbox"] = json.loads(result["bbox"])
            if result.get("metadata"):
                result["metadata"] = json.loads(result["metadata"])
            if result.get("asset_paths"):
                result["asset_paths"] = json.loads(result["asset_paths"])
            if result.get("wkt2_string"):
                result["wkt2_string"] = result["wkt2_string"]
            if result.get("thumbnails"):
                result["thumbnails"] = json.loads(result["thumbnails"])
            # Backward compatibility: support old 'thumbnail' field
            elif result.get("thumbnail"):
                result["thumbnails"] = [result["thumbnail"]]
            if result.get("depth_unit_info"):
                result["depth_unit_info"] = json.loads(result["depth_unit_info"])
            if result.get("flowfile_object"):
                result["flowfile_object"] = json.loads(result["flowfile_object"])
            if result.get("flowfile_key"):
                result["flowfile_key"] = result["flowfile_key"]
            return result
        return {}

    def handle_assets(self, event_path) -> Dict[str, Any]:
        """Process all assets for a given ICEYE event"""

        results = {}
        event_id = event_path.strip("/").split("/")[-1]
        logging.info(f"[{event_id}] Starting asset processing")
        start_time = time.time()

        # Get all files for this event
        logging.info(f"[{event_id}] Step 1/7: Listing files from S3...")
        step_start = time.time()
        all_files = self.s3_utils.list_files_with_extensions(
            self.bucket_name, event_path, [".tif", ".gpkg", ".geojson", ".json", ".pdf"]
        )
        logging.info(f"[{event_id}] Found {len(all_files)} files ({time.time() - step_start:.2f}s)")

        # Parse metadata from JSON file
        logging.info(f"[{event_id}] Step 2/7: Extracting metadata from JSON...")
        step_start = time.time()
        metadata = self.extract_metadata(all_files)
        logging.info(f"[{event_id}] Metadata extracted ({time.time() - step_start:.2f}s)")

        # Extract geometry and bbox from extent file (convex hull)
        logging.info(f"[{event_id}] Step 3/7: Extracting geometry and computing convex hull...")
        step_start = time.time()
        geometry, bbox, wkt2_string = self.extract_geometry(all_files)
        logging.info(f"[{event_id}] Geometry extracted ({time.time() - step_start:.2f}s)")

        # Calculate flooded area
        logging.info(f"[{event_id}] Step 4/7: Calculating flooded area...")
        step_start = time.time()
        flooded_area = self.calculate_flooded_area(all_files, metadata)
        logging.info(f"[{event_id}] Flooded area: {flooded_area} km² ({time.time() - step_start:.2f}s)")

        # Organize asset paths by type
        logging.info(f"[{event_id}] Step 5/7: Organizing asset paths...")
        step_start = time.time()
        asset_paths = self.organize_asset_paths(all_files)
        logging.info(f"[{event_id}] Assets organized ({time.time() - step_start:.2f}s)")

        # Create thumbnails from depth files (may be multiple for multi-region events)
        logging.info(f"[{event_id}] Step 6/7: Creating thumbnails from depth files...")
        step_start = time.time()
        thumbnails = self.create_and_add_thumbnails(all_files)
        logging.info(
            f"[{event_id}] Created {len(thumbnails) if thumbnails else 0} thumbnail(s) ({time.time() - step_start:.2f}s)"
        )

        # Detect and standardize depth unit (convert feet to inches)
        logging.info(f"[{event_id}] Step 7/8: Standardizing depth units...")
        step_start = time.time()
        depth_unit_info = self.standardize_depth_unit(all_files, metadata)
        logging.info(f"[{event_id}] Depth units standardized ({time.time() - step_start:.2f}s)")

        # Create flowfile from NWM ANA data
        logging.info(f"[{event_id}] Step 8/8: Creating flowfile from NWM ANA data...")
        step_start = time.time()
        flowfile_object, flowfile_key = self.create_flowfile_object(geometry, bbox, metadata, event_path, event_id)
        if flowfile_object:
            logging.info(f"[{event_id}] Flowfile created ({time.time() - step_start:.2f}s)")
        else:
            logging.info(f"[{event_id}] Flowfile creation skipped ({time.time() - step_start:.2f}s)")

        results[event_path] = {
            "geometry": geometry,
            "bbox": bbox,
            "metadata": metadata,
            "asset_paths": asset_paths,
            "flooded_area": flooded_area,
            "wkt2_string": wkt2_string,
            "thumbnails": thumbnails,
            "depth_unit_info": depth_unit_info,
            "flowfile_object": flowfile_object,
            "flowfile_key": flowfile_key,
        }

        logging.info(f"[{event_id}] Writing results to parquet...")
        self.write_data_parquet(results)

        total_time = time.time() - start_time
        logging.info(f"[{event_id}] ✓ Asset processing complete (total: {total_time:.2f}s)")
        return results[event_path]

    def extract_metadata(self, all_files: List[str]) -> Dict[str, Any]:
        """Extract metadata from JSON file"""
        metadata_files = [f for f in all_files if f.endswith(".json")]

        if not metadata_files:
            logging.warning("No metadata JSON file found")
            return {}

        metadata_file = metadata_files[0]

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                local_path = os.path.join(tmpdir, os.path.basename(metadata_file))
                self.s3_utils.s3_client.download_file(self.bucket_name, metadata_file, local_path)

                with open(local_path, "r") as f:
                    metadata = json.load(f)

                return metadata
        except Exception as e:
            logging.error(f"Error reading metadata file {metadata_file}: {e}")
            return {}

    def extract_geometry(self, all_files: List[str]) -> tuple:
        """
        Extract geometry from extent file and return convex hull.
        Returns (geometry_dict, bbox, wkt2_string)
        """
        # Find extent files (prefer .gpkg over .geojson)
        extent_files = [
            f
            for f in all_files
            if ("extent" in f.lower() or "floodextent" in f.lower()) and f.endswith((".gpkg", ".geojson"))
        ]

        if not extent_files:
            logging.warning("No extent file found")
            return None, None, None

        # Prefer .gpkg files
        extent_file = next((f for f in extent_files if f.endswith(".gpkg")), extent_files[0])

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                # Download extent file
                local_path = os.path.join(tmpdir, os.path.basename(extent_file))
                logging.info(f"Downloading extent file: {os.path.basename(extent_file)}")
                self.s3_utils.s3_client.download_file(self.bucket_name, extent_file, local_path)

                # Read and reproject to WGS84
                gdf = gpd.read_file(local_path)
                logging.info(f"Read {len(gdf)} features from extent file")

                wkt2_string = gdf.crs.to_wkt() if gdf.crs else None

                if gdf.crs and gdf.crs.to_epsg() != 4326:
                    logging.info(f"Reprojecting from EPSG:{gdf.crs.to_epsg()} to EPSG:4326")
                    gdf = gdf.to_crs(epsg=4326)

                if len(gdf) == 0:
                    logging.warning(f"No geometries found in extent file {extent_file}")
                    return None, None, None

                # Compute convex hull from all coordinates
                logging.info(f"Processing {len(gdf)} feature(s) for convex hull")

                # Simplify if dataset is large
                geometries = gdf.geometry
                if len(geometries) > 1000:
                    logging.info(f"Simplifying {len(geometries)} geometries")
                    geometries = geometries.simplify(tolerance=0.001, preserve_topology=False)

                # Extract all coordinates
                all_coords = []
                for geom in geometries:
                    coords = self._extract_coords_from_geometry(geom)
                    all_coords.extend(coords)

                logging.info(f"Extracted {len(all_coords)} coordinate points")

                # Sample if too many points
                if len(all_coords) > 10000:
                    all_coords = random.sample(all_coords, 10000)
                    logging.info(f"Sampled down to {len(all_coords)} points")

                # Compute convex hull
                convex_hull = MultiPoint(all_coords).convex_hull
                logging.info("Convex hull computed")

                geometry_dict = mapping(convex_hull)
                bbox = list(convex_hull.bounds)

                return geometry_dict, bbox, wkt2_string

        except Exception as e:
            logging.error(f"Error extracting geometry from {extent_file}: {e}")
            return None, None, None

    def _extract_coords_from_geometry(self, geom) -> list:
        """Helper method to extract coordinates from any geometry type."""
        coords = []

        if geom.geom_type == "Polygon":
            coords.extend(geom.exterior.coords)
        elif geom.geom_type == "MultiPolygon":
            for poly in geom.geoms:
                coords.extend(poly.exterior.coords)
        elif geom.geom_type == "Point":
            coords.append(geom.coords[0])
        elif geom.geom_type == "MultiPoint":
            coords.extend([pt.coords[0] for pt in geom.geoms])

        return coords

    def calculate_flooded_area(self, all_files: List[str], metadata: Dict) -> float:
        """
        Calculate flooded area from metadata or extent file.
        Prefer metadata value, fall back to calculating from extent.
        """
        # First try to get from metadata
        if metadata:
            # Handle old format (event list)
            if "event" in metadata and len(metadata["event"]) > 0:
                event = metadata["event"][0]
                if "flooded_area" in event:
                    return event["flooded_area"]
            # Handle new format (direct fields)
            elif "flooded_area" in metadata:
                return metadata["flooded_area"]

        # Fall back to calculating from extent file
        extent_files = [
            f for f in all_files if ("extent" in f.lower() or "floodextent" in f.lower()) and f.endswith(".gpkg")
        ]

        if extent_files:
            try:
                with tempfile.TemporaryDirectory() as tmpdir:
                    local_path = os.path.join(tmpdir, os.path.basename(extent_files[0]))
                    self.s3_utils.s3_client.download_file(self.bucket_name, extent_files[0], local_path)

                    gdf = gpd.read_file(local_path)
                    # Calculate area in square meters (convert to appropriate CRS if needed)
                    if gdf.crs and gdf.crs.to_epsg() == 4326:
                        # For WGS84, convert to equal area projection
                        gdf = gdf.to_crs(epsg=3857)  # Web Mercator for rough area calc

                    total_area = gdf.geometry.area.sum()
                    # Convert to km2
                    return total_area / 1e6

            except Exception as e:
                logging.error(f"Error calculating flooded area: {e}")
                return None

        return None

    def organize_asset_paths(self, all_files: List[str]) -> Dict[str, List[str]]:
        """Organize asset paths by type"""
        asset_paths = {
            "flood_extent": [],
            "flood_depth": [],
            "building_statistics": [],
            "release_notes": [],
            "flood_metadata": [],
        }

        for file_path in all_files:
            file_name = os.path.basename(file_path)
            asset_type = AssetUtils.determine_asset_type(file_name)

            if "extent" in asset_type.lower():
                asset_paths["flood_extent"].append(file_path)
            elif "depth" in asset_type.lower():
                asset_paths["flood_depth"].append(file_path)
            elif "building" in asset_type.lower():
                asset_paths["building_statistics"].append(file_path)
            elif "release" in asset_type.lower():
                asset_paths["release_notes"].append(file_path)
            elif "metadata" in asset_type.lower():
                asset_paths["flood_metadata"].append(file_path)

        return asset_paths

    def create_flowfile_object(
        self, geometry: dict, bbox: List[float], metadata: dict, event_path: str, event_id: str
    ) -> tuple:
        """
        Create flowfile object for ICEYE data using NWM Analysis Assimilation data.

        Args:
            geometry: GeoJSON geometry dict
            bbox: [min_lon, min_lat, max_lon, max_lat]
            metadata: ICEYE metadata dict
            event_path: S3 path to event directory
            event_id: Event ID for naming

        Returns:
            tuple: (flowfile_object, flowfile_key)
        """
        # Check if we have hydrofabric data
        if self.nwm_flows_gdf is None:
            logging.warning("No hydrofabric data available, skipping flowfile creation")
            return None, None

        # Check if we have geometry and bbox
        if not geometry or not bbox:
            logging.warning("No geometry or bbox available, skipping flowfile creation")
            return None, None

        try:
            start_date, end_date, release_date = extract_dates_from_metadata(metadata)

            if not start_date or not end_date:
                logging.warning("No start_date or end_date in metadata, skipping flowfile creation")
                return None, None

            # Create AnaFlowProcessor instance
            ana_processor = AnaFlowProcessor(self.nwm_flows_gdf)

            # Use the comprehensive method to create and upload flowfile
            flowfile_key, flowfile_object = ana_processor.create_and_upload_flowfile_for_peak(
                geometry=geometry,
                bbox=bbox,
                start_datetime=start_date,
                end_datetime=end_date,
                item_id=event_id,
                s3_utils=self.s3_utils,
                bucket_name=self.bucket_name,
                upload_prefix=event_path,
            )

            return flowfile_object, flowfile_key

        except Exception as e:
            logging.warning(f"Failed to create flowfile: {e}")
            return None, None

    def create_and_add_thumbnails(self, all_files: List[str]) -> List[str]:
        """
        Create thumbnails from all available depth raster files.
        Supports multi-region events (e.g., Helene with north/central/south regions).
        Uses RasterUtils.create_preview() for consistent thumbnail generation.

        Returns:
            List of S3 paths to generated thumbnails
        """
        # Find depth raster files
        depth_files = [
            f for f in all_files if ("depth" in f.lower() or "flooddepth" in f.lower()) and f.endswith(".tif")
        ]

        if not depth_files:
            logging.warning("No depth raster files found for thumbnail generation")
            return []

        logging.info(f"Found {len(depth_files)} depth file(s) for thumbnail generation")

        thumbnail_paths = []

        for idx, depth_file in enumerate(depth_files, 1):
            try:
                # Extract region name from filename if available (e.g., "north", "central", "south")
                file_basename = os.path.basename(depth_file)
                logging.info(f"Processing thumbnail {idx}/{len(depth_files)}: {file_basename}")

                with tempfile.TemporaryDirectory() as tmpdir:
                    local_depth_path = os.path.join(tmpdir, os.path.basename(depth_file))

                    # Generate thumbnail filename based on depth file name
                    # e.g., "thumbnail_north.png", "thumbnail_central.png", "thumbnail_south.png"
                    depth_name = os.path.splitext(file_basename)[0]

                    # Extract region identifier if present
                    region_suffix = ""
                    for region in [
                        "north",
                        "south",
                        "east",
                        "west",
                        "central",
                        "northeast",
                        "northwest",
                        "southeast",
                        "southwest",
                    ]:
                        if region in depth_name.lower():
                            region_suffix = f"_{region}"
                            break

                    # If no region found but multiple files, use index
                    if not region_suffix and len(depth_files) > 1:
                        region_suffix = f"_{idx}"

                    thumbnail_filename = f"thumbnail{region_suffix}.png"
                    local_thumbnail_path = os.path.join(tmpdir, thumbnail_filename)

                    # Use the standardized make_and_upload_thumbnail method from bench.py
                    logging.info(f"Generating thumbnail from {file_basename}")
                    thumbnail_s3_path = self.s3_utils.make_and_upload_thumbnail(
                        local_depth_path, local_thumbnail_path, self.bucket_name, depth_file
                    )

                    logging.info(f"✓ Thumbnail {idx}/{len(depth_files)} uploaded: {thumbnail_filename}")
                    thumbnail_paths.append(thumbnail_s3_path)

            except Exception as e:
                logging.error(f"Error creating thumbnail from {depth_file}: {e}")
                # Continue processing other thumbnails even if one fails
                continue

        if thumbnail_paths:
            logging.info(f"Successfully created {len(thumbnail_paths)}/{len(depth_files)} thumbnail(s)")
        else:
            logging.warning("Failed to create any thumbnails")

        return thumbnail_paths

    def standardize_depth_unit(self, all_files: List[str], metadata: Dict) -> Dict[str, Any]:
        """
        Standardize depth unit to inches. Convert feet to inches if needed.

        Logic:
        - If max depth is ~16 or less, it's in feet -> multiply by 12
        - If max depth goes over 100, it's already in inches
        - Check metadata for depth unit hints

        Returns dict with:
        - original_unit: str ('feet' or 'inches')
        - standardized_unit: str (always 'inches')
        - conversion_factor: float (12.0 if converted, 1.0 if already inches)
        """
        depth_files = [
            f for f in all_files if ("depth" in f.lower() or "flooddepth" in f.lower()) and f.endswith(".tif")
        ]

        if not depth_files:
            logging.warning("No depth file found for unit standardization")
            return {
                "original_unit": "unknown",
                "standardized_unit": "inches",
                "conversion_factor": 1.0,
            }

        # First check metadata for explicit unit information
        metadata_unit = None
        if metadata:
            # Handle old format (event list)
            if "event" in metadata and len(metadata["event"]) > 0:
                event = metadata["event"][0]
                metadata_unit = event.get("depth_vertical_unit")
            # Handle new format (direct fields)
            elif "depth_value_unit" in metadata:
                metadata_unit = metadata["depth_value_unit"]

        # If metadata explicitly says 'feet' or contains 'ft', convert
        if metadata_unit and ("feet" in metadata_unit.lower() or "ft" in metadata_unit.lower()):
            return {
                "original_unit": "feet",
                "standardized_unit": "inches",
                "conversion_factor": 12.0,
            }

        # If metadata explicitly says 'inches' or 'in', no conversion needed
        if metadata_unit and ("inch" in metadata_unit.lower() or metadata_unit.lower() == "in"):
            return {
                "original_unit": "inches",
                "standardized_unit": "inches",
                "conversion_factor": 1.0,
            }

        # If metadata doesn't help, analyze the depth raster
        depth_file = depth_files[0]
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                local_depth_path = os.path.join(tmpdir, os.path.basename(depth_file))
                self.s3_utils.s3_client.download_file(self.bucket_name, depth_file, local_depth_path)

                # Get max value from raster
                max_depth = RasterUtils.get_max_value(local_depth_path)

                if max_depth is None:
                    logging.warning(f"Could not determine max depth from {depth_file}")
                    return {
                        "original_unit": "unknown",
                        "standardized_unit": "inches",
                        "conversion_factor": 1.0,
                    }

                # Decision logic:
                # If max depth <= 20, likely in feet (e.g., Ian with ~16 ft max)
                # If max depth > 100, likely already in inches
                if max_depth <= 20:
                    logging.info(f"Max depth {max_depth} suggests feet, will convert to inches")
                    return {
                        "original_unit": "feet",
                        "standardized_unit": "inches",
                        "conversion_factor": 12.0,
                    }
                else:
                    logging.info(f"Max depth {max_depth} suggests inches, no conversion needed")
                    return {
                        "original_unit": "inches",
                        "standardized_unit": "inches",
                        "conversion_factor": 1.0,
                    }

        except Exception as e:
            logging.error(f"Error analyzing depth file {depth_file}: {e}")
            return {
                "original_unit": "unknown",
                "standardized_unit": "inches",
                "conversion_factor": 1.0,
            }

    def write_data_parquet(self, results):
        results_copy = copy.deepcopy(results)
        for path, data in results_copy.items():
            if "geometry" in data and isinstance(data["geometry"], dict):
                data["geometry"] = json.dumps(data["geometry"])
            if "bbox" in data and isinstance(data["bbox"], list):
                data["bbox"] = json.dumps(data["bbox"])
            if "metadata" in data and isinstance(data["metadata"], dict):
                data["metadata"] = json.dumps(data["metadata"])
            if "asset_paths" in data and isinstance(data["asset_paths"], dict):
                data["asset_paths"] = json.dumps(data["asset_paths"])
            if "wkt2_string" in data and isinstance(data["wkt2_string"], str):
                data["wkt2_string"] = data["wkt2_string"]
            if "thumbnails" in data and isinstance(data["thumbnails"], list):
                data["thumbnails"] = json.dumps(data["thumbnails"])
            if "depth_unit_info" in data and isinstance(data["depth_unit_info"], dict):
                data["depth_unit_info"] = json.dumps(data["depth_unit_info"])
            if "flowfile_object" in data and isinstance(data["flowfile_object"], dict):
                data["flowfile_object"] = json.dumps(data["flowfile_object"])
            if "flowfile_key" in data and isinstance(data["flowfile_key"], str):
                data["flowfile_key"] = data["flowfile_key"]

        new_df = (
            pd.DataFrame.from_dict(results_copy, orient="index").reset_index().rename(columns={"index": "event_path"})
        )
        for event_path in new_df["event_path"]:
            self.results_df = self.results_df[self.results_df["event_path"] != event_path]

        self.results_df = pd.concat([self.results_df, new_df], ignore_index=True)
        self.results_df.to_parquet(self.local_results_file, index=False)

    def upload_modified_parquet(self):
        try:
            self.s3_utils.s3_client.upload_file(self.local_results_file, self.bucket_name, self.derived_metadata_path)
            logging.info(
                f"Successfully uploaded {self.local_results_file} to s3://{self.bucket_name}/{self.derived_metadata_path}"
            )
        except Exception as e:
            logging.error(
                f"Failed to upload {self.local_results_file} to s3://{self.bucket_name}/{self.derived_metadata_path}: {e}"
            )
        finally:
            if os.path.exists(self.local_results_file):
                os.remove(self.local_results_file)
                logging.info(f"Removed local file {self.local_results_file}")
