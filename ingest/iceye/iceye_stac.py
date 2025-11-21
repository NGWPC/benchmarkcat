import os
from datetime import datetime
from typing import Any, Dict

import pystac
from pystac.extensions.item_assets import AssetDefinition


class AssetUtils:
    @staticmethod
    def determine_asset_type(file_name: str) -> str:
        """Determine the type of ICEYE asset based on filename"""
        lower_name = file_name.lower()

        if "floodextent" in lower_name or "flood_extent" in lower_name:
            return "Flood Extent"
        elif "flooddepth" in lower_name or "flood_depth" in lower_name:
            return "Flood Depth"
        elif "buildingdepthestimation" in lower_name or "building_statistics" in lower_name:
            return "Building Statistics"
        elif "releasenotes" in lower_name or "release_notes" in lower_name:
            return "Release Notes"
        elif "floodmetadata" in lower_name or "flood_metadata" in lower_name:
            return "Flood Metadata"
        else:
            return "Unknown"

    @staticmethod
    def get_media_type(file_name: str) -> str:
        """Get media type based on file extension"""
        media_types = {
            ".tif": "image/tiff; application=geotiff",
            ".tiff": "image/tiff; application=geotiff",
            ".gpkg": "application/geopackage+sqlite3",
            ".geojson": "application/geo+json",
            ".json": "application/json",
            ".pdf": "application/pdf",
            ".png": "image/png",
        }
        ext = os.path.splitext(file_name)[1].lower()
        return media_types.get(ext, "application/octet-stream")

    @staticmethod
    def get_asset_role(asset_type: str) -> str:
        """Get the STAC role for an asset type"""
        role_mapping = {
            "Flood Extent": "data",
            "Flood Depth": "data",
            "Building Statistics": "data",
            "Release Notes": "metadata",
            "Flood Metadata": "metadata",
        }
        return role_mapping.get(asset_type, "data")


class ICEYEInfo:
    """Information about ICEYE assets and metadata structure"""

    assets = {
        "flood_extent": AssetDefinition.create(
            title="Flood Extent",
            description="Vector file showing the extent of flooding",
            media_type="application/geopackage+sqlite3",
            roles=["data"],
        ),
        "flood_depth": AssetDefinition.create(
            title="Flood Depth",
            description="Raster file showing flood depth",
            media_type="image/tiff; application=geotiff",
            roles=["data"],
        ),
        "building_statistics": AssetDefinition.create(
            title="Building Statistics",
            description="Statistics about buildings affected by flooding",
            media_type="application/geopackage+sqlite3",
            roles=["data"],
        ),
        "release_notes": AssetDefinition.create(
            title="Release Notes",
            description="PDF document containing release notes for the flood event",
            media_type="application/pdf",
            roles=["metadata"],
        ),
        "flood_metadata": AssetDefinition.create(
            title="Flood Metadata",
            description="JSON metadata file containing flood event information",
            media_type="application/json",
            roles=["metadata"],
        ),
        "NWM_ANA_flowfile": AssetDefinition.create(
            title="NWM Analysis Assimilation Flowfile",
            description="CSV file with NWM feature IDs and discharge values at peak flow time during the flood event",
            media_type="text/csv",
            roles=["data"],
        ),
    }

    @staticmethod
    def parse_event_id(directory_name: str) -> str:
        """
        Extract event ID from directory name.
        Examples:
        - ICEYE_FSD-1279_usa_hurricane_ian_R6 -> FSD-1279
        - ICEYE_FSD-2082_flood_insights_usa_midwest_R1_imperial -> FSD-2082
        """
        parts = directory_name.split("_")
        for part in parts:
            if part.startswith("FSD-"):
                return part
        return None

    @staticmethod
    def parse_release_number(directory_name: str) -> str:
        """
        Extract release number from directory name.
        Examples:
        - ICEYE_FSD-1279_usa_hurricane_ian_R6 -> R6
        - ICEYE_FSD-2082_flood_insights_usa_midwest_R1_imperial -> R1
        """
        parts = directory_name.split("_")
        for part in parts:
            if part.startswith("R") and len(part) > 1 and part[1:].isdigit():
                return part
        return None

    @staticmethod
    def parse_revision_number(directory_name: str) -> int:
        """
        Extract revision number as integer from directory name.
        Examples:
        - ICEYE_FSD-1279_usa_hurricane_ian_R6 -> 6
        - ICEYE_FSD-2082_flood_insights_usa_midwest_R1_imperial -> 1
        - ICEYE_FSD-2227_flood_depth_usa_helene_in_R3 -> 3

        Returns:
            int: Revision number, or 0 if not found
        """
        release_str = ICEYEInfo.parse_release_number(directory_name)
        if release_str and release_str.startswith("R"):
            try:
                return int(release_str[1:])
            except ValueError:
                return 0
        return 0


def extract_dates_from_metadata(metadata: dict):
    """
    Extract start, end, and release dates from ICEYE metadata.

    Note: This function is placed in iceye_stac.py (rather than iceye_col.py) to avoid
    circular imports, as it needs to be used by both iceye_col.py and iceye_handle_assets.py.
    """
    start_date = None
    end_date = None
    release_date = None

    # Handle old format (event list)
    if "event" in metadata and len(metadata["event"]) > 0:
        event = metadata["event"][0]
        if "start_date" in event:
            start_date = datetime.fromisoformat(event["start_date"].replace("+03:00", "+00:00"))
        if "end_date" in event:
            end_date = datetime.fromisoformat(event["end_date"].replace("+03:00", "+00:00"))
        if "release_date" in event:
            release_date = datetime.fromisoformat(event["release_date"].replace("-04:00", "+00:00"))

    # Handle new format (direct fields)
    else:
        if "flood_event_start_time" in metadata:
            start_date = datetime.fromisoformat(metadata["flood_event_start_time"].replace("Z", "+00:00"))
        if "flood_event_end_time" in metadata:
            end_date = datetime.fromisoformat(metadata["flood_event_end_time"].replace("Z", "+00:00"))
        if "release_time" in metadata:
            release_date = datetime.fromisoformat(metadata["release_time"].replace("Z", "+00:00"))

    return start_date, end_date, release_date
