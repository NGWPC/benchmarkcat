import argparse
import logging
from datetime import datetime, timezone

import boto3
import pystac
from pystac.extensions.item_assets import ItemAssetsExtension
from pystac.extensions.projection import ProjectionExtension

from ingest.ble.ble_handle_assets import BLEAssetHandler
from ingest.ble.ble_stac import BLEInfo
from ingest.utils import S3Utils

logging.basicConfig(level=logging.INFO)


def initialize_s3_utils():
    s3 = boto3.client("s3")
    s3_utils = S3Utils(s3)
    return s3_utils


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--link_type", type=str, default="uri", help='Link type, either "url" or "uri"')
    parser.add_argument("--bucket_name", type=str, default="fimc-data", help="S3 bucket name")
    parser.add_argument(
        "--catalog_path",
        type=str,
        default="benchmark/stac-bench-cat/",
        help="Path to the STAC catalog in the S3 bucket",
    )
    parser.add_argument(
        "--asset_object_key",
        type=str,
        default="benchmark/high_resolution_validation_data_ble/",
        help="Key for the asset object in the S3 bucket",
    )
    parser.add_argument(
        "--reprocess_assets", action="store_true", help="Set to True to reprocess assets using BLEAssetHandler"
    )
    parser.add_argument(
        "--derived_metadata_path",
        type=str,
        default="benchmark/stac-bench-cat/assets/derived-asset-data/ble_collection.parquet",
        help="S3 key for the derived metadata Parquet file created by asset handling code.",
    )
    return parser.parse_args()


def create_ble_collection():
    collection = pystac.Collection(
        id="ble-collection",
        description="This is a collection of base level elevation (BLE) maps meant to be used to benchmark the performance of the National Water Centers Height Above Nearest Drainage (HAND) Maps",
        title="FEMA-BLE-benchmark-flood-rasters",
        keywords=["FEMA", "flood", "BLE", "model", "extents", "depths"],
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent([[-179.15, 18.91, -66.95, 71.39]]),
            temporal=pystac.TemporalExtent([[None, None]]),
        ),
        license="CC0-1.0",
    )

    item_assets_ext = ItemAssetsExtension.ext(collection, add_if_missing=True)
    item_assets_ext.item_assets = BLEInfo.assets

    return collection


def get_huc8_paths(s3_utils, bucket_name, asset_object_key):
    return s3_utils.list_subdirectories(bucket_name, asset_object_key)


def process_huc8(huc8_path, s3_utils, bucket_name, link_type, collection, reprocess_assets, asset_handler):
    huc8 = huc8_path.strip("/").split("/")[-1]
    logging.info(f"Indexing HUC8: {huc8}")

    # Process assets for this HUC8
    if asset_handler.assets_processed(huc8_path) and not reprocess_assets:
        asset_results = asset_handler.read_data_parquet(huc8_path)
    else:
        asset_results = asset_handler.handle_assets(huc8_path)

    create_item(huc8_path, huc8, asset_results, s3_utils, bucket_name, link_type, collection)


def create_item(huc8_path, huc8, asset_results, s3_utils, bucket_name, link_type, collection):
    # Create item
    item = pystac.Item(
        id=f"{huc8}-ble",
        geometry=asset_results["geometry"],
        bbox=asset_results["bbox"],
        datetime=datetime.now(timezone.utc),
        properties={
            "title": f"HUC8 {huc8} BLE Data",
            "description": "Extents and depths associated with the 100 yr and 500 yr flood magnitudes of this HUC8 BLE study",
            "license": "CC0-1.0",
            "hucs": [huc8],
            "magnitude": asset_results["magnitudes"],
            "flowfile": asset_results["flowfile"]["flowfile_object"],
            "extent_area (m^2)": asset_results["extent_area"],
            "resolution (m)": 3,
        },
    )

    # Add assets
    create_assets(item, huc8_path, huc8, asset_results, s3_utils, bucket_name, link_type)

    # Add projection extension
    ProjectionExtension.ext(item, add_if_missing=True)
    item.properties.update({"proj:wkt2": asset_results["wkt2_string"].replace('"', "'")})

    # validate item
    item.validate()

    # Add item to collection
    collection.add_item(item)


def create_assets(item, huc8_path, huc8, asset_results, s3_utils, bucket_name, link_type):
    # Add the thumbnail asset for the HUC8
    thumbnail_href, is_valid = s3_utils.generate_href(bucket_name, asset_results["thumbnail"], link_type)

    if "thumbnail" in asset_results:
        if is_valid:
            item.add_asset(
                "thumbnail",
                pystac.Asset(href=thumbnail_href, media_type="image/png", roles=["thumbnail"], title="Thumbnail Image"),
            )
        else:
            print(f"Skipping thumbnail asset for huc {huc8} - invalid or inaccessible")

    # Add extents, depths, and flow files for magnitudes
    for magnitude in asset_results["magnitudes"]:
        # Add extent raster
        extent_tiff = asset_results["extent_paths"][magnitude]
        extent_href, is_valid = s3_utils.generate_href(bucket_name, extent_tiff, link_type)
        if is_valid:
            item.add_asset(
                f"{magnitude}_extent_raster",
                pystac.Asset(
                    href=extent_href,
                    media_type="image/tiff; application=geotiff",
                    roles=["data"],
                    title=f"{magnitude} Year Flood Extent",
                ),
            )
        else:
            print(f"Skipping extent asset for huc {huc8} magnitude {magnitude} - invalid or inaccessible")

        # Add depth raster
        if asset_results["depth_paths"] and magnitude in asset_results["depth_paths"]:
            depth_tiff = asset_results["depth_paths"][magnitude]
            depth_href, is_valid = s3_utils.generate_href(bucket_name, depth_tiff, link_type)
            if is_valid:
                item.add_asset(
                    f"{magnitude}_depth_raster",
                    pystac.Asset(
                        href=depth_href,
                        media_type="image/tiff; application=geotiff",
                        roles=["data"],
                        title=f"{magnitude} Year Flood Depth",
                    ),
                )
            else:
                print(f"Skipping depth asset for huc {huc8} magnitude {magnitude} - invalid or inaccessible")

        # Add flow file
        flowfile_key = asset_results["flowfile"]["flowfile_keys"][magnitude]
        flow_href, is_valid = s3_utils.generate_href(bucket_name, flowfile_key, link_type)
        if is_valid:
            item.add_asset(
                f"{magnitude}_flow_file",
                pystac.Asset(
                    href=flow_href,
                    media_type="text/csv",
                    roles=["data"],
                    title=f"{magnitude} Year Flow Data",
                    description=f"The flow file of NWM hydrofabric feature ids and associated discharges for the {magnitude} year recurrence interval.",
                ),
            )
        else:
            print(f"Skipping flow asset for huc {huc8} magnitude {magnitude} - invalid or inaccessible")


def main():
    args = parse_arguments()
    s3_utils = initialize_s3_utils()

    collection = create_ble_collection()
    huc8_paths = get_huc8_paths(s3_utils, args.bucket_name, args.asset_object_key)
    asset_handler = BLEAssetHandler(s3_utils, args.bucket_name, args.derived_metadata_path)

    for huc8_path in huc8_paths:
        process_huc8(
            huc8_path, s3_utils, args.bucket_name, args.link_type, collection, args.reprocess_assets, asset_handler
        )

    s3_utils.update_collection(collection, "ble-collection", args.catalog_path, args.bucket_name)

    collection.validate()

    asset_handler.upload_modified_parquet()


if __name__ == "__main__":
    main()
