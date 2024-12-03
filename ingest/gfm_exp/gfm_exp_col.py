import argparse
import pdb
import geopandas as gpd
import pandas as pd
import boto3
from botocore.exceptions import ClientError
import os
import re
import tempfile
from shapely.geometry import shape
import logging
from datetime import datetime, timezone
import json
import pystac
from pystac.extensions.sat import SatExtension
from pystac.extensions.projection import ProjectionExtension
from pystac.extensions.item_assets import ItemAssetsExtension
from pystac.summaries import Summaries
from ingest.gfm_exp.gfm_exp_handle_assets import GFMExpAssetHandler
from ingest.gfm.gfm_stac import SentinelName, AssetUtils, GFMInfo
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
    parser.add_argument('--asset_object_key', type=str, default='benchmark/rs/PI4/', help='Key for the asset object in the S3 bucket')
    parser.add_argument('--reprocess_assets', action='store_true', help='Set to true to reprocess assets using GFMAssetHandler')
    parser.add_argument('--derived_metadata_path', type=str, default='benchmark/stac-bench-cat/assets/derived-asset-data/gfm_expanded_collection.parquet', help='S3 key for the derived metadata Parquet file created by asset handling code.')
    return parser.parse_args()

def create_gfm_exp_collection(link_type, bucket_name, asset_object_key, s3_utils):
    collection = pystac.Collection(
        id='gfm-expanded-collection',
        description="This collection contains Global Flood Monitoring (GFM) flood tile groups contained within a given Sentinel-1 datatake footprint. For each footprint a flowfile created from NWM ANA data is provided that estimates the flows present during the data take. Each tile within a data take footprint is also associated with a flood to baseline ratio that gives the percentage of flooded pixels relative to what is normally inundated according to GFM.",
        title="Expanded Global Flood Monitoring Collection",
        keywords=["flood", "GFM"],
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent([[-179.9, 7.2, -64.5, 61.8]]),
            temporal=pystac.TemporalExtent([[datetime(2021, 8, 1, tzinfo=timezone.utc), None]])
        ),
        license='CC-BY-4.0',
        providers=[pystac.Provider(
            name='GLOFAS',
            roles=[pystac.ProviderRole.PRODUCER, pystac.ProviderRole.PROCESSOR, pystac.ProviderRole.LICENSOR],
            description='The Global Flood Awareness System (GLOFAS) provides real-time flood monitoring and early warning information.',
            url='https://global-flood.emergency.copernicus.eu/'
        )],
        summaries=Summaries({
            'platform': ['Sentinel-1'],
            'constellation': ['Copernicus'],
            'instruments': ['SAR'],
            'providers': ['GLOFAS'],
            'GFM_layers': GFMInfo.layers
        })
    )

    collection.assets['naming_conventions'] = pystac.Asset(
        href=s3_utils.generate_href(bucket_name, 's3://fimc-data/benchmark/rs/gfm/gfm_data_readme.pdf', link_type),
        title="GFM Data Readme",
        description="This document contains the naming conventions for the GFM data.",
        media_type="application/pdf"
    )

    item_assets_ext = ItemAssetsExtension.ext(collection, add_if_missing=True)
    item_assets_ext.item_assets = GFMInfo.assets

    return collection

def get_gfm_exp_dates(s3_utils, bucket_name, asset_object_key):
    return s3_utils.list_subdirectories(bucket_name, asset_object_key)

def process_date(date_path, s3_utils, bucket_name, link_type, collection, reprocess_assets, asset_handler):
    date_id = date_path.strip('/').split('/')[-1]
    logging.info(f"Indexing date: {date_id}")
    
    sent_ti_list = s3_utils.list_subdirectories(bucket_name, date_path)
    for sent_ti_path in sent_ti_list:
        process_tile(sent_ti_path, date_id, s3_utils, bucket_name, link_type, 
                    collection, reprocess_assets, asset_handler)

def get_flood_ratios(s3_utils, bucket_name, sent_ti_path):
    try:
        ratio_key = s3_utils.list_resources_with_string(bucket_name, sent_ti_path, ['flood_ratios.json'])[0]
        response = s3_utils.s3_client.get_object(Bucket=bucket_name, Key=ratio_key)
        flood_ratios = json.loads(response['Body'].read().decode('utf-8'))
        return flood_ratios
    except (IndexError, ClientError) as e:
        logging.warning(f"No flood_ratios.json found for {sent_ti_path}: {str(e)}")
        return {}

def process_tile(sent_ti_path, date_id, s3_utils, bucket_name, link_type, collection, reprocess_assets, asset_handler):
    sent_ti = sent_ti_path.strip('/').split('/')[-1]
    equi7tiles_list = [m.group() for filename in s3_utils.list_resources_with_string(bucket_name, sent_ti_path, ['OBSWATER']) 
                       if len(os.path.basename(filename).split('_')) > 2 
                       for m in [re.search(r'[E]\d{3}[N]\d{3}T\d', os.path.basename(filename))]
                       if m is not None]

    gfm_version, orbit_state, abs_orbit_num = get_orbit_info(sent_ti_path, s3_utils, bucket_name)
    start_datetime, end_datetime = SentinelName.extract_datetimes(sent_ti)

    if asset_handler.tile_assets_processed(sent_ti_path) and not reprocess_assets:
        asset_results = asset_handler.read_data_parquet(sent_ti_path)
    else:
        asset_results = asset_handler.handle_assets(sent_ti_path, equi7tiles_list)

        
    geometry = asset_results["geometry"]
    
    bbox = asset_results["bbox"]
    flowfile_object = asset_results["flowfile_object"]
    equi7tile_areas = asset_results["equi7tile_areas"]

    flood_ratios = get_flood_ratios(s3_utils, bucket_name, sent_ti_path)

    item = create_item(date_id, sent_ti, geometry, bbox, 
                      start_datetime, end_datetime, orbit_state, abs_orbit_num, 
                      gfm_version, flowfile_object, equi7tile_areas, flood_ratios)  # Added flood_ratios

    SatExtension.ext(item, add_if_missing=True)
    ProjectionExtension.ext(item, add_if_missing=True)

    add_assets_to_item(item, sent_ti_path, equi7tiles_list, s3_utils, bucket_name, link_type, asset_results["flowfile_key"])

    item.validate()
    collection.add_item(item)

def get_orbit_info(sent_ti_path, s3_utils, bucket_name):
    advflag_list = s3_utils.list_resources_with_string(bucket_name, sent_ti_path, ['ADVFLAG'])
    if advflag_list:
        gfm_version = SentinelName.extract_version_string(advflag_list[0])
        orbit_direction = SentinelName.extract_orbit_state(advflag_list[0])
        orbit_state = 'ascending' if orbit_direction == 'A' else 'descending'
    else:
        logging.warning(f"Skipping GFM version and orbit direction for {sent_ti_path}")
        orbit_state, gfm_version = None, None

    abs_orbit_num = SentinelName.extract_orbit_number(sent_ti_path)
    return gfm_version, orbit_state, abs_orbit_num

def create_item(date_id, sent_ti, geometry, bbox, start_datetime, end_datetime, 
                orbit_state, abs_orbit_num, gfm_version, flowfile_object, 
                equi7tile_areas, flood_ratios): 
    properties = {
        "title": f"GFM-expanded_{sent_ti}",
        "description": f"This item lists assets associated with the GFM scene {sent_ti}.",
        "gfm_data_take_start_datetime": start_datetime.isoformat(),  
        "gfm_data_take_end_datetime": end_datetime.isoformat(),
        "proj:epsg": 27705,
        "proj:wkt2": '+proj=aeqd +lat_0=52 +lon_0=-97.5 +x_0=8264722.17686 +y_0=4867518.35323 +datum=WGS84 +units=m +no_defs',
        "gsd": 20,
        "gfm_version": gfm_version,
        "flowfiles": flowfile_object,
        "tile_total_inundated_area (m^2)": equi7tile_areas,
        "flood_to_baseline_ratios": flood_ratios 
    }
    
    if orbit_state is not None:
        properties["sat:orbit_state"] = orbit_state
    if abs_orbit_num is not None:
        properties["sat:absolute_orbit"] = int(abs_orbit_num)

    return pystac.Item(
        id=f"GFM-expanded_{sent_ti}",
        geometry=geometry,
        bbox=bbox,
        datetime=start_datetime,
        properties=properties
    )

def add_assets_to_item(item, sent_ti_path, equi7tiles_list, s3_utils, bucket_name, link_type, flowfile_key):
    equi7tile_assets = {}
    if flowfile_key:
        equi7tile = None
        asset_id, asset = create_asset(flowfile_key, bucket_name, link_type, equi7tile, s3_utils, flowfile=True)
        item.add_asset(asset_id, asset)

    for equi7tile in equi7tiles_list:
        tile_asset_list = s3_utils.list_resources_with_string(bucket_name, sent_ti_path, [equi7tile])
        equi7tile_assets[equi7tile] = []

        for tile_asset_path in tile_asset_list:
            asset_id, asset = create_asset(tile_asset_path, bucket_name, link_type, equi7tile, s3_utils)
            equi7tile_assets[equi7tile].append(asset_id)
            item.add_asset(asset_id, asset)

    item.properties['equi7tile_assets'] = equi7tile_assets

def create_asset(asset_path, bucket_name, link_type, equi7tile, s3_utils, flowfile=False):
    if flowfile:
        asset_id = "NWM_ANA_flowfile"
        asset = pystac.Asset(
            href=s3_utils.generate_href(bucket_name, asset_path, link_type),
            roles=["data"],
            description="NWM flowfile produced from ANA data, see flowfiles key in properties for more information"
        )
    else:
        tile_asset = asset_path.strip('/').split('/')[-1]
        asset_type = AssetUtils.determine_asset_type(tile_asset)
        role = 'thumbnail' if asset_type == 'Thumbnail' else 'metadata' if asset_type in ['Footprint', 'Metadata', 'Schedule'] else 'data'
        media_type = AssetUtils.get_media_type(tile_asset)
        asset_id = f"{equi7tile}_{asset_type.replace(' ', '_')}"
        asset = pystac.Asset(
            href=s3_utils.generate_href(bucket_name, asset_path, link_type),
            roles=[role],
            media_type=media_type,
            title=f"{equi7tile} {asset_type}"
        )
    return asset_id, asset

def main():
    args = parse_arguments()
    s3_utils = initialize_s3_utils()
    collection = create_gfm_exp_collection(args.link_type, args.bucket_name, args.asset_object_key, s3_utils)
    dates = get_gfm_exp_dates(s3_utils, args.bucket_name, args.asset_object_key)
    asset_handler = GFMExpAssetHandler(s3_utils, args.bucket_name, args.derived_metadata_path)

    for date in dates:
        #stopping here temporarily to look at items produced
        if "2021-10" in date:
            break
        print(f"===============processing {date}===============")
        process_date(date, s3_utils, args.bucket_name, args.link_type, 
                    collection, args.reprocess_assets, asset_handler)

    s3_utils.update_collection(collection, 'gfm-exp-collection', args.catalog_path, args.bucket_name)
    collection.validate()

    asset_handler.upload_modified_parquet()

if __name__ == "__main__":
    main()

