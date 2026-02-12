import argparse
import logging
import os
import re
import tempfile
from datetime import datetime, timezone

import boto3
import geopandas as gpd
import pandas as pd
import pystac
from pystac.extensions.item_assets import ItemAssetsExtension
from pystac.extensions.projection import ProjectionExtension
from pystac.extensions.sat import SatExtension
from pystac.summaries import Summaries
from shapely.geometry import shape

from ingest.gfm.gfm_handle_assets import GFMAssetHandler
from ingest.gfm.gfm_stac import AssetUtils, GFMInfo, SentinelName
from ingest.gfm_exp.gfm_qc import compute_scene_qc
from ingest.utils import S3Utils

logging.basicConfig(level=logging.INFO)


def initialize_s3_utils(profile=None):
    if profile is not None:
        session = boto3.Session(profile_name=profile)
        s3 = session.client("s3")
    else:
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
        "--asset_object_key", type=str, default="benchmark/rs/", help="Key for the asset object in the S3 bucket"
    )
    parser.add_argument(
        "--hucs_object_key",
        type=str,
        default="benchmark/stac-bench-cat/assets/WBDHU8_webproj.gpkg",
        help="Where to download the gpkg with the huc8 info",
    )
    parser.add_argument(
        "--reprocess_assets", action="store_true", help="Set to true to reprocess assets using GFMAssetHandler"
    )
    parser.add_argument(
        "--derived_metadata_path",
        type=str,
        default="benchmark/stac-bench-cat/assets/derived-asset-data/gfm_collection.parquet",
        help="S3 key for the derived metadata Parquet file created by asset handling code.",
    )
    parser.add_argument("--skip-owp-qc", action="store_true", help="Skip OWP QC grading (faster runs).")
    parser.add_argument("--profile", type=str, default=None, help="AWS profile name for boto3.")
    return parser.parse_args()


def create_gfm_collection(link_type, bucket_name, asset_object_key, s3_utils):
    collection = pystac.Collection(
        id="gfm-collection",
        description="This collection contains the 50+ Global Flood Monitoring (GFM) flood tile groups identified by using the Dartmouth Flood Observatory (DFO) event data.",
        title="Global Flood Monitoring Collection",
        keywords=["flood", "GFM", "DFO"],
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent([[-179.9, 7.2, -64.5, 61.8]]),
            temporal=pystac.TemporalExtent(
                [[datetime(2015, 1, 1, tzinfo=timezone.utc), datetime(2021, 7, 31, tzinfo=timezone.utc)]]
            ),
        ),
        license="CC-BY-4.0",
        providers=[
            pystac.Provider(
                name="GLOFAS",
                roles=[pystac.ProviderRole.PRODUCER, pystac.ProviderRole.PROCESSOR, pystac.ProviderRole.LICENSOR],
                description="The Global Flood Awareness System (GLOFAS) provides real-time flood monitoring and early warning information.",
                url="https://global-flood.emergency.copernicus.eu/",
            )
        ],
        summaries=Summaries(
            {
                "platform": ["Sentinel-1"],
                "constellation": ["Copernicus"],
                "instruments": ["SAR"],
                "datetime": [datetime(2015, 1, 1, tzinfo=timezone.utc).isoformat(), None],
                "providers": ["GLOFAS"],
                "GFM_layers": GFMInfo.layers,
            }
        ),
    )
    readme_href, is_valid = s3_utils.generate_href(bucket_name, f"{asset_object_key}gfm/gfm_data_readme.pdf", link_type)
    if is_valid:
        collection.assets["naming_conventions"] = pystac.Asset(
            href=readme_href,
            title="GFM Data Readme",
            description="This document contains the naming conventions for the GFM data.",
            media_type="application/pdf",
        )
    else:
        print("Skipping gfm readme asset creation - invalid or inaccessible")

    item_assets_ext = ItemAssetsExtension.ext(collection, add_if_missing=True)
    item_assets_ext.item_assets = GFMInfo.assets

    return collection


def get_dfo_events(s3_utils, bucket_name, asset_object_key):
    return s3_utils.list_subdirectories(bucket_name, f"{asset_object_key}")


def process_event(dfo_path, s3_utils, bucket_name, link_type, collection, reprocess_assets, asset_handler, hucs_gdf, skip_owp_qc=False):
    event_id = dfo_path.strip("/").split("/")[-1]
    logging.info(f"Indexing DFO event: {event_id}")

    sent_ti_list = s3_utils.list_subdirectories(bucket_name, dfo_path)
    for sent_ti_path in sent_ti_list:
        process_tile(
            sent_ti_path,
            event_id,
            s3_utils,
            bucket_name,
            link_type,
            collection,
            reprocess_assets,
            asset_handler,
            hucs_gdf,
            skip_owp_qc=skip_owp_qc,
        )


def process_tile(
    sent_ti_path, event_id, s3_utils, bucket_name, link_type, collection, reprocess_assets, asset_handler, hucs_gdf=None, skip_owp_qc=False
):
    sent_ti = sent_ti_path.strip("/").split("/")[-1]
    equi7tiles_list = [
        m.group()
        for filename in s3_utils.list_resources_with_string(bucket_name, sent_ti_path, ["OBSWATER"])
        if len(os.path.basename(filename).split("_")) > 2
        for m in [re.search(r"[E]\d{3}[N]\d{3}T\d", os.path.basename(filename))]
        if m is not None
    ]

    gfm_version, orbit_state, abs_orbit_num = get_orbit_info(sent_ti_path, s3_utils, bucket_name)
    start_datetime, end_datetime = SentinelName.extract_datetimes(sent_ti)

    if asset_handler.tile_assets_processed(sent_ti_path) and not reprocess_assets:
        asset_results = asset_handler.read_data_parquet(sent_ti_path)
    else:
        asset_results = asset_handler.handle_assets(sent_ti_path, event_id, equi7tiles_list)

    # Get the second polygon from the multipolygon geometry
    geometry = asset_results["geometry"]
    geometry_shape = shape(asset_results["geometry"])
    if hasattr(geometry_shape, "geoms"):
        # If it's a multipolygon, get the second polygon
        try:
            flood_geometry = list(geometry_shape.geoms)[1].__geo_interface__
        except IndexError:
            logging.warning(f"Multipolygon has fewer than 2 polygons for {sent_ti_path}")
            flood_geometry = geometry_shape.__geo_interface__
    else:
        flood_geometry = geometry_shape.__geo_interface__

    # Find intersecting HUC8s if HUCs data is provided
    huc8_list = []
    if hucs_gdf is not None:
        # Create a GeoDataFrame with the flood geometry
        flood_gdf = gpd.GeoDataFrame(geometry=[shape(flood_geometry)], crs=hucs_gdf.crs)
        # Perform spatial join
        huc_join = gpd.sjoin(flood_gdf, hucs_gdf, how="left", predicate="intersects")
        huc8_list = huc_join["HUC8"].tolist() if "HUC8" in huc_join.columns else []

    owp_properties = {}
    if not skip_owp_qc and huc8_list and equi7tiles_list and hucs_gdf is not None:
        try:
            owp_properties = compute_scene_qc(
                huc8_list=huc8_list,
                hucs_gdf=hucs_gdf,
                sent_ti_path=sent_ti_path,
                equi7tiles_list=equi7tiles_list,
                bucket_name=bucket_name,
                s3_utils=s3_utils,
            )
        except Exception as e:
            logging.warning("OWP QC failed for %s: %s", sent_ti, e)

    bbox = asset_results["bbox"]
    flowfile_object = asset_results["flowfile_object"]
    main_cause = asset_results["main_cause"]
    equi7tile_areas = asset_results["equi7tile_areas"]

    item = create_item(
        event_id,
        main_cause,
        sent_ti,
        geometry,
        bbox,
        start_datetime,
        end_datetime,
        orbit_state,
        abs_orbit_num,
        gfm_version,
        flowfile_object,
        huc8_list,
        equi7tile_areas,
        owp_properties=owp_properties,
    )

    # add extensions to item
    SatExtension.ext(item, add_if_missing=True)
    ProjectionExtension.ext(item, add_if_missing=True)

    add_assets_to_item(
        item, sent_ti_path, equi7tiles_list, s3_utils, bucket_name, link_type, asset_results["flowfile_key"]
    )

    # validate item
    item.validate()

    collection.add_item(item)


def get_orbit_info(sent_ti_path, s3_utils, bucket_name):
    advflag_list = s3_utils.list_resources_with_string(bucket_name, sent_ti_path, ["ADVFLAG"])
    if advflag_list:
        gfm_version = SentinelName.extract_version_string(advflag_list[0])
        orbit_direction = SentinelName.extract_orbit_state(advflag_list[0])
        orbit_state = "ascending" if orbit_direction == "A" else "descending"
    else:
        logging.warning(f"Skipping GFM version and orbit direction for {sent_ti_path}")
        orbit_state, gfm_version = None, None

    abs_orbit_num = SentinelName.extract_orbit_number(sent_ti_path)
    return gfm_version, orbit_state, abs_orbit_num


def create_item(
    event_id,
    main_cause,
    sent_ti,
    geometry,
    bbox,
    start_datetime,
    end_datetime,
    orbit_state,
    abs_orbit_num,
    gfm_version,
    flowfile_object,
    huc8_list,
    equi7tile_areas,
    owp_properties=None,
):
    properties = {
        "title": f"DFO-{event_id}_tile-{sent_ti}",
        "description": f"This item lists some of assets associated with the GFM scene {sent_ti}.",
        "main_cause": main_cause,
        "gfm_data_take_start_datetime": start_datetime.isoformat(),
        "gfm_data_take_end_datetime": end_datetime.isoformat(),
        "dfo_event_id": event_id,
        "proj:code": "EPSG:27705",
        "proj:wkt2": "+proj=aeqd +lat_0=52 +lon_0=-97.5 +x_0=8264722.17686 +y_0=4867518.35323 +datum=WGS84 +units=m +no_defs",
        "gsd (m)": 20,
        "gfm_version": gfm_version,
        "flowfiles": flowfile_object,
        "hucs": huc8_list,
        "tile_total_inundated_area (m^2)": equi7tile_areas,
    }

    # Add orbit properties only if they exist
    if orbit_state is not None:
        properties["sat:orbit_state"] = orbit_state
    if abs_orbit_num is not None:
        properties["sat:absolute_orbit"] = int(abs_orbit_num)

    if owp_properties:
        properties.update(owp_properties)

    return pystac.Item(
        id=f"DFO-{event_id}_tile-{sent_ti}",
        geometry=geometry,
        bbox=bbox,
        datetime=start_datetime,
        properties=properties,
    )


def add_assets_to_item(item, sent_ti_path, equi7tiles_list, s3_utils, bucket_name, link_type, flowfile_key):
    equi7tile_assets = {}
    if flowfile_key:
        equi7tile = None
        asset_id, asset = create_asset(flowfile_key, bucket_name, link_type, equi7tile, s3_utils, flowfile=True)
        if asset:
            item.add_asset(asset_id, asset)
        else:
            print(f"Skipping creating flowfile asset for {equi7tile} - invalid or inaccessible")

    for equi7tile in equi7tiles_list:
        tile_asset_list = s3_utils.list_resources_with_string(bucket_name, sent_ti_path, [equi7tile])
        equi7tile_assets[equi7tile] = []

        for tile_asset_path in tile_asset_list:
            asset_id, asset = create_asset(tile_asset_path, bucket_name, link_type, equi7tile, s3_utils)
            equi7tile_assets[equi7tile].append(asset_id)
            if asset:
                item.add_asset(asset_id, asset)
            else:
                print(f"Skipping creating asset for {asset_id} - invalid or inaccessible")

    item.properties["equi7tile_assets"] = equi7tile_assets


def create_asset(asset_path, bucket_name, link_type, equi7tile, s3_utils, flowfile=False):
    if flowfile:
        asset_id = "NWM_v3_flowfile"
        flowfile_href, is_valid = s3_utils.generate_href(bucket_name, asset_path, link_type)
        if is_valid:
            asset = pystac.Asset(
                href=flowfile_href,
                roles=["data"],
                description="NWM 3.0 flowfile see flowfiles key in properties for more information",
            )
        else:
            asset = None
    else:
        tile_asset = asset_path.strip("/").split("/")[-1]
        asset_type = AssetUtils.determine_asset_type(tile_asset)
        role = (
            "thumbnail"
            if asset_type == "Thumbnail"
            else "metadata"
            if asset_type in ["Footprint", "Metadata", "Schedule"]
            else "data"
        )
        media_type = AssetUtils.get_media_type(tile_asset)
        asset_id = f"{equi7tile}_{asset_type.replace(' ', '_')}"
        tile_asset_href, is_valid = s3_utils.generate_href(bucket_name, asset_path, link_type)
        if is_valid:
            asset = pystac.Asset(
                href=tile_asset_href, roles=[role], media_type=media_type, title=f"{equi7tile} {asset_type}"
            )
        else:
            asset = None
    return asset_id, asset


def main():
    os.environ["GDAL_DISABLE_READDIR_ON_OPEN"] = "EMPTY_DIR"
    os.environ["CPL_VSIL_CURL_CHUNK_SIZE"] = "65536"

    args = parse_arguments()

    if getattr(args, 'profile', None):
        os.environ["AWS_PROFILE"] = args.profile
    else:
        os.environ.pop("AWS_PROFILE", None)

    s3_utils = initialize_s3_utils(profile=args.profile)

    collection = create_gfm_collection(args.link_type, args.bucket_name, args.asset_object_key, s3_utils)
    dfo_events = get_dfo_events(s3_utils, args.bucket_name, args.asset_object_key)
    asset_handler = GFMAssetHandler(s3_utils, args.bucket_name, args.derived_metadata_path)

    # Download and read HUCs data
    _, hucs_gpkg = os.path.split(args.hucs_object_key)
    with tempfile.TemporaryDirectory() as tmpdir:
        local_hucs_path = f"{tmpdir}/{hucs_gpkg}"
        s3_utils.s3_client.download_file(args.bucket_name, args.hucs_object_key, local_hucs_path)
        hucs_gdf = gpd.read_file(local_hucs_path)

        for dfo_event in dfo_events:
            process_event(
                dfo_event,
                s3_utils,
                args.bucket_name,
                args.link_type,
                collection,
                args.reprocess_assets,
                asset_handler,
                hucs_gdf,
                skip_owp_qc=args.skip_owp_qc,
            )

    s3_utils.update_collection(collection, "gfm-collection", args.catalog_path, args.bucket_name)
    collection.validate()

    # Upload the modified Parquet file of asset handler output back to S3 and delete local copy
    asset_handler.upload_modified_parquet()


if __name__ == "__main__":
    main()
