import pdb
import re
import os
import argparse
import logging
from datetime import datetime, timezone
import boto3
import pystac
from pystac.extensions.projection import ProjectionExtension
from pystac.extensions.item_assets import ItemAssetsExtension
from ingest.hec_ras_ext import HECRASExtension
from ingest.ahps.ahps_handle_assets import AHPSFIMAssetHandler
from ingest.ahps.ahps_stac import AHPSFIMInfo, GeoJSONHandler, AssetUtils
from ingest.bench import S3Utils

logging.basicConfig(level=logging.INFO)

def initialize_s3_utils():
    s3 = boto3.client('s3')
    s3_utils = S3Utils(s3)
    return s3_utils

def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--link_type', type=str, default='uri', help='Link type, either "url" or "uri"')
    parser.add_argument('--bucket_name', type=str, default='fimc-data', help='S3 bucket name')
    parser.add_argument('--catalog_path', type=str, default='benchmark/stac-bench-cat/', help='Path to the STAC catalog in the S3 bucket')
    # depending on which value you set for asset_object key you can process nws or usgs ahps data
    parser.add_argument('--asset_object_key', type=str, default='hand_fim/test_cases/usgs_test_cases/validation_data_usgs/', help='Key for the asset object in the S3 bucket')
    parser.add_argument('--reprocess_assets', action='store_true', help='Set to true to reprocess assets using USGSFIMAssetHandler')
    # derived_metadata_path agency needs to match the agency in "asset_object_key"
    parser.add_argument('--derived_metadata_path', type=str, default='benchmark/stac-bench-cat/assets/derived-asset-data/usgs_fim_collection.parquet', help='S3 key for the derived metadata Parquet file created by asset handling code.')
    return parser.parse_args()

def extract_agency(asset_object_key):
    match = re.search(r'([^/]+)_test_cases/[^/]+/$', asset_object_key)
    if match:
        return match.group(1)
    else:
        return None

def extract_agency_from_metadata_path(derived_metadata_path):
    basename = os.path.basename(derived_metadata_path)
    match = re.match(r'^(.*?)_', basename)
    if match:
        return match.group(1)
    else:
        return None

def validate_agencies(asset_agency, metadata_agency):
    if asset_agency != metadata_agency:
        raise ValueError(f"Agency mismatch: '{asset_agency}' from asset_object_key does not match '{metadata_agency}' from derived_metadata_path.")

def create_ahps_fim_collection(agency):
    collection = pystac.Collection(
        id=f'{agency}-fim-collection',
        description="This is a collection of base level elevation maps meant to be used to benchmark the performance of the National Water Centers Height Above Nearest Drainage (HAND) Maps",
        title=f"{agency}-fim-benchmark-flood-rasters",
        keywords=["flood", f"{agency}-fim", "model", "extents", "depths", "HEC-RAS"],
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent([[-179.15, 18.91, -66.95, 71.39]]),
            temporal=pystac.TemporalExtent([[None, None]])
        ),
        license='CC0-1.0',
    )

    item_assets_ext = ItemAssetsExtension.ext(collection, add_if_missing=True)
    item_assets_ext.item_assets = AHPSFIMInfo.assets

    return collection

def get_huc8_paths(s3_utils, bucket_name, asset_object_key):
    return s3_utils.list_subdirectories(bucket_name, asset_object_key)

def process_huc8(huc8_path, s3_utils, bucket_name, link_type, collection, agency, reprocess_assets, asset_handler):
    huc8 = huc8_path.strip('/').split('/')[-1]
    logging.info(f"Indexing HUC8: {huc8}")

    for gauge_path in s3_utils.list_subdirectories(bucket_name, huc8_path):
        process_gauge(gauge_path, agency, huc8, s3_utils, bucket_name, link_type, collection, reprocess_assets, asset_handler)

def process_gauge(gauge_path, agency, huc8, s3_utils, bucket_name, link_type, collection, reprocess_assets, asset_handler):
    gauge = gauge_path.strip('/').split('/')[-1]
    geometry, bbox = GeoJSONHandler.process_shapefile(bucket_name, gauge_path, s3_utils.s3_client)

    if asset_handler._assets_processed(gauge_path) and not reprocess_assets:
        asset_results = asset_handler.read_data_parquet(gauge_path)
    else:
        asset_results = asset_handler.handle_assets(gauge_path)

    item = pystac.Item(
        id=f"{huc8}-{gauge}-{agency}",
        geometry=geometry,
        bbox=bbox,
        datetime=datetime.now(timezone.utc),
        hucs=["huc8"],
        properties={
            "title": f"HUC8-{huc8} gauge-{gauge} {agency} fim",
            "description": "Extents and depths associated with the HEC-RAS modelling domain around the National Weather Service gauge used to model the flows",
            "license": 'CC0-1.0',
            "extent_area (m)": asset_results["extent_area"],
            "gauge": gauge,
            "flowfile": asset_results["flowfile"]["flowfile_object"],
            "magnitude": {"study magnitudes": asset_results["magnitudes"]}
        }
    )
    
    create_assets(item, gauge_path, gauge, asset_results, s3_utils, bucket_name, link_type)

    # Add wkt2 string using the projection extension
    ProjectionExtension.ext(item, add_if_missing=True)
    item.properties.update({"proj:wkt2":asset_results["wkt2_string"].replace('"', "'")})

    collection.add_item(item)

def create_assets(item, gauge_path, gauge, asset_results, s3_utils, bucket_name, link_type):
    # Add rating curve for gauge
    item.add_asset(
        "rating_curve",
        pystac.Asset(
            href=s3_utils.generate_href(bucket_name, f"{gauge_path}{gauge}_rating_curve.csv", link_type),
            description="Rating curve CSV used for event stages",
            media_type="text/csv",
            roles=["data"]
        )
    )

    # Add the thumbnail asset for the gauge
    if "thumbnail" in asset_results:
        item.add_asset(
            "thumbnail",
            pystac.Asset(
                href=s3_utils.generate_href(bucket_name, asset_results["thumbnail"], link_type),
                media_type="image/png",
                roles=["thumbnail"],
                title="Thumbnail Image"
            )
        )

    # Add extents and flowfiles for all the magnitudes
    for magnitude in asset_results["magnitudes"]:
        for tiff_path in asset_results["extent_paths"][magnitude]:
            item.add_asset(
                f"{magnitude}_extent_raster",
                pystac.Asset(
                    href=s3_utils.generate_href(bucket_name, tiff_path, link_type),
                    media_type="image/tiff; application=geotiff",
                    roles=["data"],
                    title=f"{magnitude} Flood Extent"
                )
            )
        # Find the flowfile key for the current magnitude
        flowfile_key = next(
            key for key in asset_results["flowfile"]["flowfile_keys"] if magnitude in key
        )

        item.add_asset(
            f"{magnitude}_flow_file",
            pystac.Asset(
                href=s3_utils.generate_href(bucket_name, flowfile_key, link_type),
                media_type="text/csv",
                roles=["data"],
                title=f"{magnitude} flood magnitude flowfile Data",
                description=f"The flow file of NWM hydrofabric feature ids and associated discharges for this gauge domain's {magnitude} flood magnitude."
            )
        )

def main():
    args = parse_arguments()
    s3_utils = initialize_s3_utils()
    asset_agency = extract_agency(args.asset_object_key)    
    metadata_agency = extract_agency_from_metadata_path(args.derived_metadata_path)
    
    validate_agencies(asset_agency, metadata_agency)
    
    collection = create_ahps_fim_collection(asset_agency)
    huc8_paths = get_huc8_paths(s3_utils, args.bucket_name, args.asset_object_key)
    asset_handler = AHPSFIMAssetHandler(s3_utils, args.bucket_name, args.derived_metadata_path,asset_agency)

    for huc8_path in huc8_paths:
        process_huc8(huc8_path, s3_utils, args.bucket_name, args.link_type, collection, asset_agency, args.reprocess_assets, asset_handler)

    s3_utils.update_collection(collection, f'{asset_agency}-fim-collection', args.catalog_path, args.bucket_name)
    collection.validate()

    asset_handler.upload_modified_parquet()

if __name__ == "__main__":
    main()
