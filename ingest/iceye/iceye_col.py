import os
import argparse
import logging
from datetime import datetime, timezone
import boto3
import pystac
from pystac.extensions.projection import ProjectionExtension
from pystac.extensions.item_assets import ItemAssetsExtension
from ingest.iceye.iceye_handle_assets import ICEYEAssetHandler
from ingest.iceye.iceye_stac import ICEYEInfo, AssetUtils
from ingest.bench import S3Utils

logging.basicConfig(level=logging.INFO)


def initialize_s3_utils():
    s3 = boto3.client('s3')
    s3_utils = S3Utils(s3)
    return s3_utils


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--link_type', type=str, default='uri', help='Link type, either "url" or "uri"'
    )
    parser.add_argument(
        '--bucket_name', type=str, default='fimc-data', help='S3 bucket name'
    )
    parser.add_argument(
        '--catalog_path',
        type=str,
        default='benchmark/stac-bench-cat/',
        help='Path to the STAC catalog in the S3 bucket',
    )
    parser.add_argument(
        '--asset_object_key',
        type=str,
        default='benchmark/rs/iceye/',
        help='Key for the asset object in the S3 bucket',
    )
    parser.add_argument(
        '--reprocess_assets',
        action='store_true',
        help='Set to True to reprocess assets using ICEYEAssetHandler',
    )
    parser.add_argument(
        '--derived_metadata_path',
        type=str,
        default='benchmark/stac-bench-cat/assets/derived-asset-data/iceye_collection.parquet',
        help='S3 key for the derived metadata Parquet file created by asset handling code.',
    )
    return parser.parse_args()


def create_iceye_collection():
    collection = pystac.Collection(
        id='iceye-collection',
        description="This collection contains ICEYE flood detection and monitoring products. ICEYE provides synthetic aperture radar (SAR) satellite imagery for flood extent mapping, depth estimation, and building impact analysis. The data includes flood extent vectors, depth rasters, and building statistics for various flood events.",
        title="ICEYE Flood Detection Collection",
        keywords=["ICEYE", "flood", "SAR", "satellite", "flood extent", "flood depth", "building statistics"],
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent([[-180, -90, 180, 90]]),  # Global coverage
            temporal=pystac.TemporalExtent([[datetime(2022, 1, 1, tzinfo=timezone.utc), None]])
        ),
        license='proprietary',
        providers=[
            pystac.Provider(
                name="ICEYE",
                roles=[
                    pystac.ProviderRole.PRODUCER,
                    pystac.ProviderRole.PROCESSOR,
                    pystac.ProviderRole.LICENSOR,
                ],
                description="ICEYE is a Finnish microsatellite manufacturer that specializes in synthetic aperture radar (SAR) satellites for Earth observation.",
                url="https://www.iceye.com/",
            )
        ],
    )

    item_assets_ext = ItemAssetsExtension.ext(collection, add_if_missing=True)
    item_assets_ext.item_assets = ICEYEInfo.assets

    return collection


def get_event_paths(s3_utils, bucket_name, asset_object_key):
    """Get all event directory paths"""
    return s3_utils.list_subdirectories(bucket_name, asset_object_key)


def filter_latest_revisions(event_paths):
    """
    Filter event paths to keep only the latest revision for each event.

    Events are grouped by their FSD ID (e.g., FSD-1279), and only the event
    with the highest revision number (R#) is kept.

    Examples:
        Input: ['ICEYE_FSD-1279_..._R1/', 'ICEYE_FSD-1279_..._R6/', 'ICEYE_FSD-2082_..._R1/']
        Output: ['ICEYE_FSD-1279_..._R6/', 'ICEYE_FSD-2082_..._R1/']

    Args:
        event_paths: List of S3 event directory paths

    Returns:
        List of event paths with only the latest revision for each event
    """
    from collections import defaultdict

    # Group events by FSD ID
    events_by_fsd = defaultdict(list)

    for path in event_paths:
        # Extract the directory name from the path
        event_dir = path.strip('/').split('/')[-1]

        # Parse FSD ID and revision number
        fsd_id = ICEYEInfo.parse_event_id(event_dir)
        revision = ICEYEInfo.parse_revision_number(event_dir)

        if fsd_id:
            events_by_fsd[fsd_id].append({
                'path': path,
                'revision': revision,
                'event_dir': event_dir
            })
        else:
            # If we can't parse FSD ID, keep the event anyway
            logging.warning(f"Could not parse FSD ID from {event_dir}, including anyway")
            events_by_fsd[event_dir].append({
                'path': path,
                'revision': 0,
                'event_dir': event_dir
            })

    # Keep only the latest revision for each event
    latest_paths = []
    for fsd_id, events in events_by_fsd.items():
        # Sort by revision number (descending) and take the first
        latest_event = max(events, key=lambda x: x['revision'])
        latest_paths.append(latest_event['path'])

        # Log if multiple revisions were found
        if len(events) > 1:
            all_revisions = sorted([e['revision'] for e in events], reverse=True)
            logging.info(
                f"{fsd_id}: Found {len(events)} revisions {all_revisions}, "
                f"using latest R{latest_event['revision']} from {latest_event['event_dir']}"
            )

    logging.info(f"Filtered {len(event_paths)} total events to {len(latest_paths)} latest revisions")
    return sorted(latest_paths)


def process_event(
    event_path, s3_utils, bucket_name, link_type, collection, reprocess_assets, asset_handler
):
    event_id = event_path.strip('/').split('/')[-1]
    logging.info(f"Indexing ICEYE event: {event_id}")

    # Process assets for this event
    if asset_handler.assets_processed(event_path) and not reprocess_assets:
        asset_results = asset_handler.read_data_parquet(event_path)
    else:
        asset_results = asset_handler.handle_assets(event_path)

    create_item(event_path, event_id, asset_results, s3_utils, bucket_name, link_type, collection)


def create_item(event_path, event_id, asset_results, s3_utils, bucket_name, link_type, collection):
    """Create STAC item for an ICEYE event"""

    # Extract metadata
    metadata = asset_results.get('metadata', {})

    # Parse dates from metadata
    start_date, end_date, release_date = extract_dates_from_metadata(metadata)

    # Extract event information
    event_info = extract_event_info(metadata)

    # Parse event ID and release number from directory name
    fsd_id = ICEYEInfo.parse_event_id(event_id)
    release_number = ICEYEInfo.parse_release_number(event_id)

    # Build properties
    properties = {
        "title": f"ICEYE {event_id}",
        "description": event_info.get('description', f"ICEYE flood data for event {fsd_id}"),
        "license": 'proprietary',
    }

    # Add event-specific properties
    if fsd_id:
        properties["iceye:event_id"] = fsd_id
    if release_number:
        properties["iceye:release_number"] = release_number
    if event_info.get('event_type'):
        properties["iceye:event_type"] = event_info['event_type']
    if event_info.get('country'):
        properties["iceye:country"] = event_info['country']
    if event_info.get('country_iso'):
        properties["iceye:country_iso"] = event_info['country_iso']
    if asset_results.get('flooded_area'):
        properties["iceye:flooded_area_km2"] = asset_results['flooded_area']
    if event_info.get('product_version'):
        properties["iceye:product_version"] = event_info['product_version']
    if event_info.get('analysis_tier'):
        properties["iceye:analysis_tier"] = event_info['analysis_tier']
    if event_info.get('epsg'):
        # Use proj:code instead of proj:epsg (deprecated in v2.0.0)
        properties["proj:code"] = f"EPSG:{event_info['epsg']}"
    if event_info.get('pixel_size'):
        properties["iceye:pixel_size"] = event_info['pixel_size']
    if event_info.get('pixel_size_unit'):
        properties["iceye:pixel_size_unit"] = event_info['pixel_size_unit']

    # Add standardized depth unit information
    depth_unit_info = asset_results.get('depth_unit_info', {})
    if depth_unit_info:
        # Always report standardized unit as inches
        properties["iceye:depth_unit"] = depth_unit_info.get('standardized_unit', 'inches')
        properties["iceye:depth_unit_original"] = depth_unit_info.get('original_unit', 'unknown')
        if depth_unit_info.get('conversion_factor', 1.0) != 1.0:
            properties["iceye:depth_conversion_factor"] = depth_unit_info['conversion_factor']
            properties["iceye:depth_conversion_note"] = f"Original depth values in {depth_unit_info['original_unit']} multiplied by {depth_unit_info['conversion_factor']} to standardize to inches"

    # Add temporal properties if available
    if start_date:
        properties["start_datetime"] = start_date.isoformat()
    if end_date:
        properties["end_datetime"] = end_date.isoformat()

    # Use release date as item datetime, or end date, or current time
    item_datetime = release_date or end_date or datetime.now(timezone.utc)

    # Create item
    item = pystac.Item(
        id=f"{event_id}",
        geometry=asset_results.get('geometry'),
        bbox=asset_results.get('bbox'),
        datetime=item_datetime,
        properties=properties,
    )

    # Add assets
    create_assets(item, event_id, asset_results, s3_utils, bucket_name, link_type)

    # Add projection extension
    if asset_results.get('wkt2_string'):
        ProjectionExtension.ext(item, add_if_missing=True)
        item.properties.update({"proj:wkt2": asset_results["wkt2_string"].replace('"', "'")})

    # Validate item
    item.validate()

    # Add item to collection
    collection.add_item(item)


def extract_dates_from_metadata(metadata: dict):
    """Extract start, end, and release dates from metadata"""
    start_date = None
    end_date = None
    release_date = None

    # Handle old format (event list)
    if 'event' in metadata and len(metadata['event']) > 0:
        event = metadata['event'][0]
        if 'start_date' in event:
            start_date = datetime.fromisoformat(event['start_date'].replace('+03:00', '+00:00'))
        if 'end_date' in event:
            end_date = datetime.fromisoformat(event['end_date'].replace('+03:00', '+00:00'))
        if 'release_date' in event:
            release_date = datetime.fromisoformat(event['release_date'].replace('-04:00', '+00:00'))

    # Handle new format (direct fields)
    else:
        if 'flood_event_start_time' in metadata:
            start_date = datetime.fromisoformat(metadata['flood_event_start_time'].replace('Z', '+00:00'))
        if 'flood_event_end_time' in metadata:
            end_date = datetime.fromisoformat(metadata['flood_event_end_time'].replace('Z', '+00:00'))
        if 'release_time' in metadata:
            release_date = datetime.fromisoformat(metadata['release_time'].replace('Z', '+00:00'))

    return start_date, end_date, release_date


def extract_event_info(metadata: dict) -> dict:
    """Extract event information from metadata"""
    info = {}

    # Handle old format (event list)
    if 'event' in metadata and len(metadata['event']) > 0:
        event = metadata['event'][0]
        info['description'] = event.get('description')
        info['event_type'] = event.get('event_type')
        info['country'] = event.get('country')
        info['country_iso'] = event.get('country_iso')
        info['product_version'] = event.get('product_version')
        info['analysis_tier'] = event.get('analysis_tier')
        info['epsg'] = event.get('EPSG')
        info['depth_unit'] = event.get('depth_vertical_unit')
        info['pixel_size'] = event.get('depth_horizontal_res')
        info['pixel_size_unit'] = event.get('depth_horizontal_res_unit')

    # Handle new format (direct fields)
    else:
        info['description'] = metadata.get('description')
        info['event_type'] = metadata.get('flood_event_type')
        # Handle countries array
        if 'countries' in metadata and len(metadata['countries']) > 0:
            info['country'] = metadata['countries'][0]
        # Handle country ISO codes array
        if 'country_iso_codes' in metadata and len(metadata['country_iso_codes']) > 0:
            info['country_iso'] = metadata['country_iso_codes'][0]
        info['product_version'] = metadata.get('product_version')
        info['epsg'] = metadata.get('EPSG_code')
        info['depth_unit'] = metadata.get('depth_value_unit')
        info['pixel_size'] = metadata.get('pixel_size')
        info['pixel_size_unit'] = metadata.get('pixel_size_unit')

    return info


def create_assets(item, event_id, asset_results, s3_utils, bucket_name, link_type):
    """Add assets to the STAC item"""
    asset_paths = asset_results.get('asset_paths', {})

    # Add thumbnail assets if available (supports multiple thumbnails for multi-region events)
    thumbnail_paths = asset_results.get('thumbnails', [])
    # Backward compatibility: support old single 'thumbnail' field
    if not thumbnail_paths and asset_results.get('thumbnail'):
        thumbnail_paths = [asset_results.get('thumbnail')]

    for idx, thumbnail_path in enumerate(thumbnail_paths):
        thumbnail_href, is_valid = s3_utils.generate_href(bucket_name, thumbnail_path, link_type)
        if is_valid:
            # Extract region name from filename if available
            thumbnail_basename = os.path.basename(thumbnail_path)
            thumbnail_name = os.path.splitext(thumbnail_basename)[0]

            # Determine asset ID and title
            if len(thumbnail_paths) == 1:
                asset_id = "thumbnail"
                title = "Thumbnail Image"
            else:
                # Extract region suffix (e.g., "thumbnail_north.png" -> "north")
                region = thumbnail_name.replace("thumbnail_", "").replace("thumbnail", str(idx + 1))
                asset_id = f"thumbnail_{region}" if region else f"thumbnail_{idx + 1}"
                title = f"Thumbnail Image ({region.title()})" if region.isalpha() else f"Thumbnail Image {idx + 1}"

            item.add_asset(
                asset_id,
                pystac.Asset(
                    href=thumbnail_href,
                    media_type="image/png",
                    roles=["thumbnail"],
                    title=title
                )
            )
        else:
            logging.warning(f"Skipping thumbnail asset {thumbnail_path} - invalid or inaccessible")

    # Add flood extent assets
    for extent_path in asset_paths.get('flood_extent', []):
        asset_id = f"flood_extent_{os.path.basename(extent_path).split('.')[-1]}"
        asset_href, is_valid = s3_utils.generate_href(bucket_name, extent_path, link_type)
        if is_valid:
            item.add_asset(
                asset_id,
                pystac.Asset(
                    href=asset_href,
                    media_type=AssetUtils.get_media_type(extent_path),
                    roles=["data"],
                    title=f"Flood Extent ({os.path.basename(extent_path).split('.')[-1].upper()})"
                )
            )
        else:
            logging.warning(f"Skipping extent asset {extent_path} - invalid or inaccessible")

    # Add flood depth assets
    for depth_path in asset_paths.get('flood_depth', []):
        asset_id = "flood_depth_raster"
        asset_href, is_valid = s3_utils.generate_href(bucket_name, depth_path, link_type)
        if is_valid:
            item.add_asset(
                asset_id,
                pystac.Asset(
                    href=asset_href,
                    media_type=AssetUtils.get_media_type(depth_path),
                    roles=["data"],
                    title="Flood Depth Raster"
                )
            )
        else:
            logging.warning(f"Skipping depth asset {depth_path} - invalid or inaccessible")

    # Add building statistics assets
    for building_path in asset_paths.get('building_statistics', []):
        asset_id = f"building_statistics_{os.path.basename(building_path).split('.')[-1]}"
        asset_href, is_valid = s3_utils.generate_href(bucket_name, building_path, link_type)
        if is_valid:
            item.add_asset(
                asset_id,
                pystac.Asset(
                    href=asset_href,
                    media_type=AssetUtils.get_media_type(building_path),
                    roles=["data"],
                    title=f"Building Statistics ({os.path.basename(building_path).split('.')[-1].upper()})"
                )
            )
        else:
            logging.warning(f"Skipping building stats asset {building_path} - invalid or inaccessible")

    # Add release notes (PDF)
    for release_path in asset_paths.get('release_notes', []):
        if release_path.endswith('.pdf'):
            asset_id = "release_notes"
            asset_href, is_valid = s3_utils.generate_href(bucket_name, release_path, link_type)
            if is_valid:
                item.add_asset(
                    asset_id,
                    pystac.Asset(
                        href=asset_href,
                        media_type="application/pdf",
                        roles=["metadata"],
                        title="Release Notes"
                    )
                )
            else:
                logging.warning(f"Skipping release notes asset {release_path} - invalid or inaccessible")

    # Add metadata JSON
    for metadata_path in asset_paths.get('flood_metadata', []):
        asset_id = "flood_metadata"
        asset_href, is_valid = s3_utils.generate_href(bucket_name, metadata_path, link_type)
        if is_valid:
            item.add_asset(
                asset_id,
                pystac.Asset(
                    href=asset_href,
                    media_type="application/json",
                    roles=["metadata"],
                    title="Flood Metadata"
                )
            )
        else:
            logging.warning(f"Skipping metadata asset {metadata_path} - invalid or inaccessible")


def main():
    args = parse_arguments()
    s3_utils = initialize_s3_utils()

    collection = create_iceye_collection()
    all_event_paths = get_event_paths(s3_utils, args.bucket_name, args.asset_object_key)

    # Filter to keep only the latest revision for each event
    event_paths = filter_latest_revisions(all_event_paths)

    asset_handler = ICEYEAssetHandler(s3_utils, args.bucket_name, args.derived_metadata_path)

    for event_path in event_paths:
        process_event(
            event_path, s3_utils, args.bucket_name, args.link_type, collection, args.reprocess_assets, asset_handler
        )

    s3_utils.update_collection(collection, 'iceye-collection', args.catalog_path, args.bucket_name)

    collection.validate()

    asset_handler.upload_modified_parquet()


if __name__ == "__main__":
    main()
