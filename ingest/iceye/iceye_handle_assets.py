import os
import json
import copy
import logging
import tempfile
import pandas as pd
import geopandas as gpd
from shapely.geometry import shape, mapping
from shapely.ops import unary_union
from typing import Dict, Any, List
from ingest.iceye.iceye_stac import ICEYEInfo, AssetUtils
from ingest.bench import S3Utils, RasterUtils


class ICEYEAssetHandler:
    def __init__(self, s3_utils, bucket_name, derived_metadata_path) -> None:
        results_file = "iceye_collection.parquet"
        self.s3_utils = s3_utils
        self.bucket_name = bucket_name
        self.derived_metadata_path = derived_metadata_path
        self.results_file = results_file
        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.local_results_file = os.path.join(script_dir, results_file)
        self.results_df = self.load_results()

    def load_results(self):
        try:
            self.s3_utils.s3_client.download_file(
                self.bucket_name, self.derived_metadata_path, self.local_results_file
            )
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
                'event_path': pd.Series(dtype='str'),
                'geometry': pd.Series(dtype='str'),
                'bbox': pd.Series(dtype='str'),
                'metadata': pd.Series(dtype='str'),
                'asset_paths': pd.Series(dtype='str'),
                'flooded_area': pd.Series(dtype='float'),
                'wkt2_string': pd.Series(dtype='str'),
                'thumbnail': pd.Series(dtype='str'),
                'depth_unit_info': pd.Series(dtype='str'),
            }
            return pd.DataFrame(columns)

    def assets_processed(self, event_path) -> bool:
        return event_path in self.results_df['event_path'].values

    def read_data_parquet(self, event_path):
        row = self.results_df[self.results_df['event_path'] == event_path]
        if not row.empty:
            result = row.to_dict(orient='records')[0]
            if result.get('geometry'):
                result['geometry'] = json.loads(result['geometry'])
            if result.get('bbox'):
                result['bbox'] = json.loads(result['bbox'])
            if result.get('metadata'):
                result['metadata'] = json.loads(result['metadata'])
            if result.get('asset_paths'):
                result['asset_paths'] = json.loads(result['asset_paths'])
            if result.get('wkt2_string'):
                result['wkt2_string'] = result['wkt2_string']
            if result.get('thumbnail'):
                result['thumbnail'] = result['thumbnail']
            if result.get('depth_unit_info'):
                result['depth_unit_info'] = json.loads(result['depth_unit_info'])
            return result
        return {}

    def handle_assets(self, event_path) -> Dict[str, Any]:
        """Process all assets for a given ICEYE event"""
        results = {}
        event_id = event_path.strip('/').split('/')[-1]
        logging.info(f"Processing assets for event: {event_id}")

        # Get all files for this event
        all_files = self.s3_utils.list_files_with_extensions(
            self.bucket_name,
            event_path,
            ['.tif', '.gpkg', '.geojson', '.json', '.pdf']
        )

        # Parse metadata from JSON file
        metadata = self.extract_metadata(all_files)

        # Extract geometry and bbox from extent file (convex hull)
        geometry, bbox, wkt2_string = self.extract_geometry(all_files)

        # Calculate flooded area
        flooded_area = self.calculate_flooded_area(all_files, metadata)

        # Organize asset paths by type
        asset_paths = self.organize_asset_paths(all_files)

        # Create thumbnail from extent file
        thumbnail = self.create_and_add_thumbnail(all_files)

        # Detect and standardize depth unit (convert feet to inches)
        depth_unit_info = self.standardize_depth_unit(all_files, metadata)

        results[event_path] = {
            "geometry": geometry,
            "bbox": bbox,
            "metadata": metadata,
            "asset_paths": asset_paths,
            "flooded_area": flooded_area,
            "wkt2_string": wkt2_string,
            "thumbnail": thumbnail,
            "depth_unit_info": depth_unit_info,
        }

        self.write_data_parquet(results)
        return results[event_path]

    def extract_metadata(self, all_files: List[str]) -> Dict[str, Any]:
        """Extract metadata from JSON file"""
        metadata_files = [f for f in all_files if f.endswith('.json')]

        if not metadata_files:
            logging.warning("No metadata JSON file found")
            return {}

        metadata_file = metadata_files[0]

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                local_path = os.path.join(tmpdir, os.path.basename(metadata_file))
                self.s3_utils.s3_client.download_file(
                    self.bucket_name, metadata_file, local_path
                )

                with open(local_path, 'r') as f:
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
        # Look for extent files (gpkg or geojson)
        extent_files = [
            f for f in all_files
            if ('extent' in f.lower() or 'floodextent' in f.lower())
            and (f.endswith('.gpkg') or f.endswith('.geojson'))
        ]

        if not extent_files:
            logging.warning("No extent file found")
            return None, None, None

        extent_file = extent_files[0]  # Use first extent file (prefer gpkg if available)
        if len([f for f in extent_files if f.endswith('.gpkg')]) > 0:
            extent_file = [f for f in extent_files if f.endswith('.gpkg')][0]

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                local_path = os.path.join(tmpdir, os.path.basename(extent_file))
                self.s3_utils.s3_client.download_file(
                    self.bucket_name, extent_file, local_path
                )

                # Read the extent file
                gdf = gpd.read_file(local_path)

                # Get WKT2 string from the file
                wkt2_string = gdf.crs.to_wkt() if gdf.crs else None

                # Transform to WGS84 for STAC
                if gdf.crs and gdf.crs.to_epsg() != 4326:
                    gdf = gdf.to_crs(epsg=4326)

                # Union all geometries and get convex hull
                if len(gdf) > 0:
                    unioned_geom = unary_union(gdf.geometry)
                    convex_hull = unioned_geom.convex_hull

                    geometry_dict = mapping(convex_hull)
                    bbox = list(convex_hull.bounds)

                    return geometry_dict, bbox, wkt2_string
                else:
                    logging.warning(f"No geometries found in extent file {extent_file}")
                    return None, None, None

        except Exception as e:
            logging.error(f"Error extracting geometry from {extent_file}: {e}")
            return None, None, None

    def calculate_flooded_area(self, all_files: List[str], metadata: Dict) -> float:
        """
        Calculate flooded area from metadata or extent file.
        Prefer metadata value, fall back to calculating from extent.
        """
        # First try to get from metadata
        if metadata:
            # Handle old format (event list)
            if 'event' in metadata and len(metadata['event']) > 0:
                event = metadata['event'][0]
                if 'flooded_area' in event:
                    return event['flooded_area']
            # Handle new format (direct fields)
            elif 'flooded_area' in metadata:
                return metadata['flooded_area']

        # Fall back to calculating from extent file
        extent_files = [
            f for f in all_files
            if ('extent' in f.lower() or 'floodextent' in f.lower())
            and f.endswith('.gpkg')
        ]

        if extent_files:
            try:
                with tempfile.TemporaryDirectory() as tmpdir:
                    local_path = os.path.join(tmpdir, os.path.basename(extent_files[0]))
                    self.s3_utils.s3_client.download_file(
                        self.bucket_name, extent_files[0], local_path
                    )

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
            'flood_extent': [],
            'flood_depth': [],
            'building_statistics': [],
            'release_notes': [],
            'flood_metadata': [],
        }

        for file_path in all_files:
            file_name = os.path.basename(file_path)
            asset_type = AssetUtils.determine_asset_type(file_name)

            if 'extent' in asset_type.lower():
                asset_paths['flood_extent'].append(file_path)
            elif 'depth' in asset_type.lower():
                asset_paths['flood_depth'].append(file_path)
            elif 'building' in asset_type.lower():
                asset_paths['building_statistics'].append(file_path)
            elif 'release' in asset_type.lower():
                asset_paths['release_notes'].append(file_path)
            elif 'metadata' in asset_type.lower():
                asset_paths['flood_metadata'].append(file_path)

        return asset_paths

    def create_flowfile_object(self, all_files: List[str]) -> tuple:
        """
        Create flowfile object for ICEYE data.

        ICEYE data is purely observational SAR-based flood detection without
        associated NWM discharge/streamflow data. Therefore, this method
        returns None to indicate no flowfile data is available.

        This follows the same pattern as GFM (Global Flood Monitoring) which
        also does not have flowfile data.

        Args:
            all_files: List of all file paths for this event

        Returns:
            tuple: (flowfile_object, flowfile_key) where both are None
        """
        logging.info("ICEYE data does not contain NWM flowfile data (SAR observation only)")
        return None, None

    def create_and_add_thumbnail(self, all_files: List[str]) -> str:
        """
        Create thumbnail from the first available extent file.
        Similar to AHPS implementation.
        """
        # Find extent files (prefer GPKG, then GeoJSON)
        extent_files = [
            f for f in all_files
            if ('extent' in f.lower() or 'floodextent' in f.lower())
            and (f.endswith('.gpkg') or f.endswith('.geojson'))
        ]

        if not extent_files:
            logging.warning("No extent file found for thumbnail generation")
            return None

        # Prefer GPKG if available
        extent_file = extent_files[0]
        if any(f.endswith('.gpkg') for f in extent_files):
            extent_file = [f for f in extent_files if f.endswith('.gpkg')][0]

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                local_extent_path = os.path.join(tmpdir, os.path.basename(extent_file))
                local_thumbnail_path = os.path.join(tmpdir, 'thumbnail.png')

                self.s3_utils.s3_client.download_file(
                    self.bucket_name, extent_file, local_extent_path
                )

                thumbnail_s3_path = self.s3_utils.make_and_upload_thumbnail(
                    local_extent_path, local_thumbnail_path, self.bucket_name, extent_file
                )

                logging.info(f"Created thumbnail at {thumbnail_s3_path}")
                return thumbnail_s3_path

        except Exception as e:
            logging.error(f"Error creating thumbnail from {extent_file}: {e}")
            return None

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
            f for f in all_files
            if ('depth' in f.lower() or 'flooddepth' in f.lower())
            and f.endswith('.tif')
        ]

        if not depth_files:
            logging.warning("No depth file found for unit standardization")
            return {
                'original_unit': 'unknown',
                'standardized_unit': 'inches',
                'conversion_factor': 1.0,
            }

        # First check metadata for explicit unit information
        metadata_unit = None
        if metadata:
            # Handle old format (event list)
            if 'event' in metadata and len(metadata['event']) > 0:
                event = metadata['event'][0]
                metadata_unit = event.get('depth_vertical_unit')
            # Handle new format (direct fields)
            elif 'depth_value_unit' in metadata:
                metadata_unit = metadata['depth_value_unit']

        # If metadata explicitly says 'feet' or contains 'ft', convert
        if metadata_unit and ('feet' in metadata_unit.lower() or 'ft' in metadata_unit.lower()):
            return {
                'original_unit': 'feet',
                'standardized_unit': 'inches',
                'conversion_factor': 12.0,
            }

        # If metadata explicitly says 'inches' or 'in', no conversion needed
        if metadata_unit and ('inch' in metadata_unit.lower() or metadata_unit.lower() == 'in'):
            return {
                'original_unit': 'inches',
                'standardized_unit': 'inches',
                'conversion_factor': 1.0,
            }

        # If metadata doesn't help, analyze the depth raster
        depth_file = depth_files[0]
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                local_depth_path = os.path.join(tmpdir, os.path.basename(depth_file))
                self.s3_utils.s3_client.download_file(
                    self.bucket_name, depth_file, local_depth_path
                )

                # Get max value from raster
                max_depth = RasterUtils.get_max_value(local_depth_path)

                if max_depth is None:
                    logging.warning(f"Could not determine max depth from {depth_file}")
                    return {
                        'original_unit': 'unknown',
                        'standardized_unit': 'inches',
                        'conversion_factor': 1.0,
                    }

                # Decision logic:
                # If max depth <= 20, likely in feet (e.g., Ian with ~16 ft max)
                # If max depth > 100, likely already in inches
                if max_depth <= 20:
                    logging.info(f"Max depth {max_depth} suggests feet, will convert to inches")
                    return {
                        'original_unit': 'feet',
                        'standardized_unit': 'inches',
                        'conversion_factor': 12.0,
                    }
                else:
                    logging.info(f"Max depth {max_depth} suggests inches, no conversion needed")
                    return {
                        'original_unit': 'inches',
                        'standardized_unit': 'inches',
                        'conversion_factor': 1.0,
                    }

        except Exception as e:
            logging.error(f"Error analyzing depth file {depth_file}: {e}")
            return {
                'original_unit': 'unknown',
                'standardized_unit': 'inches',
                'conversion_factor': 1.0,
            }

    def write_data_parquet(self, results):
        results_copy = copy.deepcopy(results)
        for path, data in results_copy.items():
            if 'geometry' in data and isinstance(data['geometry'], dict):
                data['geometry'] = json.dumps(data['geometry'])
            if 'bbox' in data and isinstance(data['bbox'], list):
                data['bbox'] = json.dumps(data['bbox'])
            if 'metadata' in data and isinstance(data['metadata'], dict):
                data['metadata'] = json.dumps(data['metadata'])
            if 'asset_paths' in data and isinstance(data['asset_paths'], dict):
                data['asset_paths'] = json.dumps(data['asset_paths'])
            if 'wkt2_string' in data and isinstance(data['wkt2_string'], str):
                data['wkt2_string'] = data['wkt2_string']
            if 'thumbnail' in data and isinstance(data['thumbnail'], str):
                data['thumbnail'] = data['thumbnail']
            if 'depth_unit_info' in data and isinstance(data['depth_unit_info'], dict):
                data['depth_unit_info'] = json.dumps(data['depth_unit_info'])

        new_df = pd.DataFrame.from_dict(results_copy, orient='index').reset_index().rename(
            columns={'index': 'event_path'}
        )
        for event_path in new_df['event_path']:
            self.results_df = self.results_df[self.results_df['event_path'] != event_path]

        self.results_df = pd.concat([self.results_df, new_df], ignore_index=True)
        self.results_df.to_parquet(self.local_results_file, index=False)

    def upload_modified_parquet(self):
        try:
            self.s3_utils.s3_client.upload_file(
                self.local_results_file, self.bucket_name, self.derived_metadata_path
            )
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
