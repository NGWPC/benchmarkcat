import argparse
import json
import logging
import multiprocessing
import os
import re
import tempfile
import time
from datetime import datetime, timezone
from functools import partial
from typing import Optional

import boto3
import geopandas as gpd
import pystac
from botocore.exceptions import ClientError
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
        "--asset_object_key", type=str, default="benchmark/rs/gfm/", help="S3 prefix for GFM data (parent of DFO event dirs)."
    )
    parser.add_argument(
        "--hucs_object_key",
        type=str,
        default="benchmark/stac-bench-cat/assets/WBDHU8_webproj.gpkg",
        help="Where to download the gpkg with the huc8 info",
    )
    parser.add_argument(
        "--boundaries_object_key",
        type=str,
        default="benchmark/stac-bench-cat/assets/Mexico_Canada_boundaries.gpkg",
        help="S3 key for the Mexico/Canada boundaries GeoPackage",
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
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of parallel workers for scene processing; 1 = sequential.",
    )
    parser.add_argument(
        "--checkpoint-every",
        type=int,
        default=50,
        help="Every N merged scenes, flush item JSONs to S3 and write/upload parquet; 0 = only at end.",
    )
    parser.add_argument(
        "--after-date",
        type=str,
        default=None,
        help="Process only scenes with acquisition date >= YYYY-MM-DD.",
    )
    parser.add_argument(
        "--before-date",
        type=str,
        default=None,
        help="Process only scenes with acquisition date <= YYYY-MM-DD.",
    )
    parser.add_argument(
        "--dates",
        type=str,
        default=None,
        help="Comma-separated list of acquisition dates (YYYY-MM-DD).",
    )
    # Batch-worker mode args
    parser.add_argument(
        "--mode",
        choices=["local", "batch-worker"],
        default="local",
        help="'local' = normal run; 'batch-worker' = read manifest slice, write partial parquet.",
    )
    parser.add_argument(
        "--manifest-s3-key",
        type=str,
        default=None,
        help="S3 key of manifest JSONL produced by batch_split.py (required in batch-worker mode).",
    )
    parser.add_argument(
        "--job-index",
        type=int,
        default=None,
        help="Array job index; defaults to AWS_BATCH_JOB_ARRAY_INDEX env var, then 0.",
    )
    parser.add_argument(
        "--scenes-per-job",
        type=int,
        default=50,
        help="Number of scenes each batch-worker array child processes.",
    )
    parser.add_argument(
        "--partial-parquet-prefix",
        type=str,
        default=None,
        help="S3 prefix where per-job partial parquets are written (required in batch-worker mode).",
    )
    return parser.parse_args()


def get_conus_neighbors(gpkg_path):
    """Load Canada and Mexico boundaries from a GeoPackage file."""
    try:
        boundaries_gdf = gpd.read_file(gpkg_path)
        canada = boundaries_gdf[boundaries_gdf["ADMIN"] == "Canada"].geometry.iloc[0]
        mexico = boundaries_gdf[boundaries_gdf["ADMIN"] == "Mexico"].geometry.iloc[0]
        return {"canada": canada, "mexico": mexico}
    except Exception as e:
        raise ValueError(f"Failed to load country boundaries from {gpkg_path}: {str(e)}")


def is_within_neighbor_countries(geometry, country_boundaries):
    """Check if geometry lies completely within Canada or Mexico."""
    geom_shape = shape(geometry) if isinstance(geometry, dict) else geometry
    if country_boundaries["canada"].contains(geom_shape):
        return True
    if country_boundaries["mexico"].contains(geom_shape):
        return True
    return False


def item_id_from_sent_ti_path(sent_ti_path):
    """Derive STAC item id from sent_ti_path (matches create_item)."""
    parts = sent_ti_path.strip("/").split("/")
    event_id = parts[-2]
    sent_ti = parts[-1]
    return f"DFO-{event_id}_tile-{sent_ti}"


def scene_already_uploaded(sent_ti_path, results_df, s3_utils, bucket_name, catalog_path, catalog_id):
    """True if scene row is in parquet and item JSON exists on S3 (fully committed)."""
    if sent_ti_path not in results_df["sent_ti_path"].values:
        return False
    item_id = item_id_from_sent_ti_path(sent_ti_path)
    base = catalog_path.rstrip("/") + "/" + catalog_id.strip("/")
    key = f"{base}/{item_id}/{item_id}.json"
    try:
        s3_utils.s3_client.head_object(Bucket=bucket_name, Key=key)
        return True
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "404":
            return False
        raise


def flush_item_batch(s3_utils, bucket_name, catalog_path, catalog_id, collection, item_buffer):
    """Upload item JSONs to S3, add item links to collection, clear buffer."""
    if not item_buffer:
        return
    base = catalog_path.rstrip("/") + "/" + catalog_id.strip("/")
    for item in item_buffer:
        key = f"{base}/{item.id}/{item.id}.json"
        item.set_parent(collection)
        d = item.to_dict()
        d["collection"] = catalog_id
        links = [
            {
                "rel": "root",
                "href": "../../catalog.json",
                "type": "application/json",
                "title": "FIM Benchmark Catalog",
            },
            {
                "rel": "collection",
                "href": "../collection.json",
                "type": "application/json",
                "title": "Global Flood Monitoring Collection",
            },
            {
                "rel": "parent",
                "href": "../collection.json",
                "type": "application/json",
                "title": "Global Flood Monitoring Collection",
            },
        ]
        d["links"] = links
        body = json.dumps(d, indent=2)
        s3_utils.s3_client.put_object(Bucket=bucket_name, Key=key, Body=body, ContentType="application/json")
        collection.add_link(
            pystac.Link(
                rel=pystac.RelType.ITEM,
                target=f"./{item.id}/{item.id}.json",
                media_type="application/geo+json",
            )
        )
    item_buffer.clear()
    logging.info(f"Flushed batch of items to S3 (prefix {base})")


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
    readme_href, is_valid = s3_utils.generate_href(bucket_name, f"{asset_object_key}gfm_data_readme.pdf", link_type)
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


def _scene_date_from_sent_ti_path(sent_ti_path: str) -> Optional[str]:
    """Extract scene acquisition date (YYYY-MM-DD) from sent_ti_path product name. Returns None if unparseable."""
    product_name = sent_ti_path.strip("/").split("/")[-1]
    try:
        start_datetime, _ = SentinelName.extract_datetimes(product_name)
        return start_datetime.date().strftime("%Y-%m-%d")
    except ValueError:
        return None


def filter_scenes_by_date_scope(work_items, after_date=None, before_date=None, dates_list=None):
    """Filter work_items (dfo_path, event_id, sent_ti_path) by scene acquisition date. Apply after_date, then before_date, then dates_list."""
    if after_date is None and before_date is None and dates_list is None:
        return work_items
    filtered = []
    for dfo_path, event_id, sent_ti_path in work_items:
        scene_date = _scene_date_from_sent_ti_path(sent_ti_path)
        if scene_date is None:
            continue
        if after_date is not None and scene_date < after_date:
            continue
        if before_date is not None and scene_date > before_date:
            continue
        if dates_list is not None:
            allowed = set(s.strip() for s in dates_list.split(",") if s.strip())
            if scene_date not in allowed:
                continue
        filtered.append((dfo_path, event_id, sent_ti_path))
    return filtered


def process_event(
    dfo_path,
    s3_utils,
    bucket_name,
    link_type,
    collection,
    reprocess_assets,
    asset_handler,
    hucs_gdf,
    country_boundaries=None,
    skip_owp_qc=False,
    on_scene_done=None,
    catalog_path=None,
    catalog_id=None,
):
    event_id = dfo_path.strip("/").split("/")[-1]
    logging.info(f"Indexing DFO event: {event_id}")

    sent_ti_list = s3_utils.list_subdirectories(bucket_name, dfo_path)
    for sent_ti_path in sent_ti_list:
        if catalog_path is not None and catalog_id is not None and on_scene_done is not None:
            if scene_already_uploaded(
                sent_ti_path, asset_handler.results_df, s3_utils, bucket_name, catalog_path, catalog_id
            ):
                item_id = item_id_from_sent_ti_path(sent_ti_path)
                collection.add_link(
                    pystac.Link(
                        rel=pystac.RelType.ITEM,
                        target=f"{item_id}/{item_id}.json",
                        media_type="application/geo+json",
                    )
                )
                continue

        item, asset_results = process_tile(
            sent_ti_path,
            event_id,
            s3_utils,
            bucket_name,
            link_type,
            reprocess_assets,
            asset_handler,
            hucs_gdf,
            country_boundaries=country_boundaries,
            skip_owp_qc=skip_owp_qc,
        )
        if item is not None and asset_results is not None:
            if on_scene_done is not None:
                on_scene_done(item, sent_ti_path, asset_results)
            else:
                collection.add_item(item)


def process_tile(
    sent_ti_path,
    event_id,
    s3_utils,
    bucket_name,
    link_type,
    reprocess_assets,
    asset_handler,
    hucs_gdf=None,
    country_boundaries=None,
    skip_owp_qc=False,
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

    geometry = asset_results["geometry"]
    bbox = asset_results["bbox"]
    huc8_list = []

    if geometry:
        # Skip scenes entirely within Canada or Mexico
        if country_boundaries and is_within_neighbor_countries(geometry, country_boundaries):
            logging.info(f"Skipping {sent_ti} - geometry lies completely within Canada or Mexico")
            return (None, None)

        # Extract the flood polygon from multipolygon (gfm-specific: second geom is flood extent)
        geometry_shape = shape(geometry)
        if hasattr(geometry_shape, "geoms"):
            try:
                flood_geometry = list(geometry_shape.geoms)[1].__geo_interface__
            except IndexError:
                logging.warning(f"Multipolygon has fewer than 2 polygons for {sent_ti_path}")
                flood_geometry = geometry_shape.__geo_interface__
        else:
            flood_geometry = geometry_shape.__geo_interface__

        # Find intersecting HUC8s if HUCs data is provided
        if hucs_gdf is not None:
            flood_gdf = gpd.GeoDataFrame(geometry=[shape(flood_geometry)], crs=hucs_gdf.crs)
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

    SatExtension.ext(item, add_if_missing=True)
    ProjectionExtension.ext(item, add_if_missing=True)

    add_assets_to_item(
        item, sent_ti_path, equi7tiles_list, s3_utils, bucket_name, link_type, asset_results["flowfile_key"]
    )

    item.validate()
    return (item, asset_results)


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


def _process_one_scene(
    work_item,
    profile,
    bucket_name,
    link_type,
    reprocess_assets,
    skip_owp_qc,
    hucs_gdf,
    country_boundaries,
    derived_metadata_path,
    initial_results_df=None,
):
    """Top-level worker for parallel scene processing. Creates own s3_utils and asset_handler."""
    dfo_path, event_id, sent_ti_path = work_item
    logging.info(f"Processing scene: {sent_ti_path} of event: {event_id}")

    if profile is not None:
        os.environ["AWS_PROFILE"] = profile
    else:
        os.environ.pop("AWS_PROFILE", None)
    os.environ["GDAL_DISABLE_READDIR_ON_OPEN"] = "EMPTY_DIR"
    os.environ["CPL_VSIL_CURL_CHUNK_SIZE"] = "65536"

    s3_utils = initialize_s3_utils(profile=profile)
    asset_handler = GFMAssetHandler(
        s3_utils, bucket_name, derived_metadata_path, initial_results_df=initial_results_df
    )
    item, asset_results = process_tile(
        sent_ti_path,
        event_id,
        s3_utils,
        bucket_name,
        link_type,
        reprocess_assets,
        asset_handler,
        hucs_gdf,
        country_boundaries=country_boundaries,
        skip_owp_qc=skip_owp_qc,
    )
    return (item, sent_ti_path, asset_results)


def main_batch_worker(args):
    """Batch-worker entry point: process one manifest slice, flush item JSONs, write partial parquet."""
    from ingest.batch_utils import read_manifest, upload_partial_parquet

    if not args.manifest_s3_key:
        raise ValueError("--manifest-s3-key is required in batch-worker mode")
    if not args.partial_parquet_prefix:
        raise ValueError("--partial-parquet-prefix is required in batch-worker mode")

    job_index = args.job_index
    if job_index is None:
        job_index = int(os.environ.get("AWS_BATCH_JOB_ARRAY_INDEX", "0"))

    if getattr(args, "profile", None):
        os.environ["AWS_PROFILE"] = args.profile
    else:
        os.environ.pop("AWS_PROFILE", None)
    os.environ["GDAL_DISABLE_READDIR_ON_OPEN"] = "EMPTY_DIR"
    os.environ["CPL_VSIL_CURL_CHUNK_SIZE"] = "65536"

    s3_utils = initialize_s3_utils(profile=args.profile)

    all_scenes = read_manifest(s3_utils, args.bucket_name, args.manifest_s3_key)
    start = job_index * args.scenes_per_job
    my_scenes = all_scenes[start: start + args.scenes_per_job]

    if not my_scenes:
        logging.info("Batch worker %d: no scenes in slice — exiting", job_index)
        return

    logging.info("Batch worker %d: processing %d scenes (indices %d–%d)",
                 job_index, len(my_scenes), start, start + len(my_scenes) - 1)

    asset_handler = GFMAssetHandler(s3_utils, args.bucket_name, args.derived_metadata_path)
    catalog_id = "gfm-collection"
    collection = create_gfm_collection(args.link_type, args.bucket_name, args.asset_object_key, s3_utils)
    item_buffer = []

    _, hucs_gpkg = os.path.split(args.hucs_object_key)
    with tempfile.TemporaryDirectory() as tmpdir:
        local_hucs_path = os.path.join(tmpdir, hucs_gpkg)
        s3_utils.s3_client.download_file(args.bucket_name, args.hucs_object_key, local_hucs_path)
        hucs_gdf = gpd.read_file(local_hucs_path)

        local_boundaries_path = os.path.join(tmpdir, os.path.basename(args.boundaries_object_key))
        s3_utils.s3_client.download_file(args.bucket_name, args.boundaries_object_key, local_boundaries_path)
        country_boundaries = get_conus_neighbors(local_boundaries_path)

        work_items = [(s["dfo_path"], s["event_id"], s["sent_ti_path"]) for s in my_scenes]

        if args.workers <= 1:
            for scene in my_scenes:
                event_id = scene["event_id"]
                sent_ti_path = scene["sent_ti_path"]
                try:
                    item, asset_results = process_tile(
                        sent_ti_path,
                        event_id,
                        s3_utils,
                        args.bucket_name,
                        args.link_type,
                        args.reprocess_assets,
                        asset_handler,
                        hucs_gdf,
                        country_boundaries=country_boundaries,
                        skip_owp_qc=args.skip_owp_qc,
                    )
                    if item is not None and asset_results is not None:
                        item_buffer.append(item)
                        asset_handler.merge_single_result(sent_ti_path, asset_results)
                except Exception as e:
                    logging.warning("Scene %s failed: %s", sent_ti_path, e)
        else:
            worker = partial(
                _process_one_scene,
                profile=args.profile,
                bucket_name=args.bucket_name,
                link_type=args.link_type,
                reprocess_assets=args.reprocess_assets,
                skip_owp_qc=args.skip_owp_qc,
                hucs_gdf=hucs_gdf,
                country_boundaries=country_boundaries,
                derived_metadata_path=args.derived_metadata_path,
                initial_results_df=None,
            )
            with multiprocessing.Pool(args.workers) as pool:
                for result in pool.imap_unordered(worker, work_items):
                    item, sent_ti_path, asset_results = result
                    if item is not None and asset_results is not None:
                        item_buffer.append(item)
                        asset_handler.merge_single_result(sent_ti_path, asset_results)

    flush_item_batch(s3_utils, args.bucket_name, args.catalog_path, catalog_id, collection, item_buffer)

    if not asset_handler.results_df.empty:
        upload_partial_parquet(
            s3_utils,
            args.bucket_name,
            args.partial_parquet_prefix,
            job_index,
            asset_handler.results_df,
        )

    logging.info("Batch worker %d done.", job_index)


def main():
    os.environ["GDAL_DISABLE_READDIR_ON_OPEN"] = "EMPTY_DIR"
    os.environ["CPL_VSIL_CURL_CHUNK_SIZE"] = "65536"

    args = parse_arguments()

    if args.after_date and args.before_date and args.after_date > args.before_date:
        raise ValueError(
            f"--after-date ({args.after_date}) must be <= --before-date ({args.before_date})"
        )

    if args.mode == "batch-worker":
        main_batch_worker(args)
        return

    if getattr(args, "profile", None):
        os.environ["AWS_PROFILE"] = args.profile
    else:
        os.environ.pop("AWS_PROFILE", None)

    s3_utils = initialize_s3_utils(profile=args.profile)

    collection = create_gfm_collection(args.link_type, args.bucket_name, args.asset_object_key, s3_utils)
    dfo_events = get_dfo_events(s3_utils, args.bucket_name, args.asset_object_key)
    asset_handler = GFMAssetHandler(s3_utils, args.bucket_name, args.derived_metadata_path)
    catalog_id = "gfm-collection"
    item_buffer = []
    items_merged = [0]  # mutable so callback can update

    def do_parquet_checkpoint():
        if args.checkpoint_every <= 0:
            return
        asset_handler.results_df.to_parquet(asset_handler.local_results_file, index=False)
        asset_handler.s3_utils.s3_client.upload_file(
            asset_handler.local_results_file,
            asset_handler.bucket_name,
            asset_handler.derived_metadata_path,
        )
        logging.info(f"Parquet checkpoint: uploaded after {items_merged[0]} scenes")

    def on_scene_done(item, sent_ti_path, asset_results):
        if args.checkpoint_every <= 0:
            collection.add_item(item)
        else:
            item_buffer.append(item)
        asset_handler.merge_single_result(sent_ti_path, asset_results)
        items_merged[0] += 1
        if (
            args.checkpoint_every > 0
            and items_merged[0] % args.checkpoint_every == 0
            and items_merged[0] > 0
        ):
            flush_item_batch(s3_utils, args.bucket_name, args.catalog_path, catalog_id, collection, item_buffer)
            do_parquet_checkpoint()

    _, hucs_gpkg = os.path.split(args.hucs_object_key)
    with tempfile.TemporaryDirectory() as tmpdir:
        local_hucs_path = f"{tmpdir}/{hucs_gpkg}"
        s3_utils.s3_client.download_file(args.bucket_name, args.hucs_object_key, local_hucs_path)
        hucs_gdf = gpd.read_file(local_hucs_path)

        local_boundaries_path = os.path.join(tmpdir, os.path.basename(args.boundaries_object_key))
        s3_utils.s3_client.download_file(args.bucket_name, args.boundaries_object_key, local_boundaries_path)
        country_boundaries = get_conus_neighbors(local_boundaries_path)

        # Build flat work list: (dfo_path, event_id, sent_ti_path)
        work_items = []
        for dfo_path in dfo_events:
            event_id = dfo_path.strip("/").split("/")[-1]
            sent_ti_list = s3_utils.list_subdirectories(args.bucket_name, dfo_path)
            for sent_ti_path in sent_ti_list:
                work_items.append((dfo_path, event_id, sent_ti_path))

        work_items = filter_scenes_by_date_scope(
            work_items,
            after_date=args.after_date,
            before_date=args.before_date,
            dates_list=args.dates,
        )

        if args.workers <= 1:
            catalog_path_opt = args.catalog_path if args.checkpoint_every > 0 else None
            catalog_id_opt = catalog_id if args.checkpoint_every > 0 else None
            for dfo_path, event_id, sent_ti_path in work_items:
                logging.info("Processing %s (event %s)", sent_ti_path, event_id)
                if catalog_path_opt is not None and catalog_id_opt is not None:
                    if scene_already_uploaded(
                        sent_ti_path,
                        asset_handler.results_df,
                        s3_utils,
                        args.bucket_name,
                        catalog_path_opt,
                        catalog_id_opt,
                    ):
                        item_id = item_id_from_sent_ti_path(sent_ti_path)
                        collection.add_link(
                            pystac.Link(
                                rel=pystac.RelType.ITEM,
                                target=f"./{item_id}/{item_id}.json",
                                media_type="application/geo+json",
                            )
                        )
                        continue
                item, asset_results = process_tile(
                    sent_ti_path,
                    event_id,
                    s3_utils,
                    args.bucket_name,
                    args.link_type,
                    args.reprocess_assets,
                    asset_handler,
                    hucs_gdf,
                    country_boundaries=country_boundaries,
                    skip_owp_qc=args.skip_owp_qc,
                )
                if item is not None and asset_results is not None:
                    on_scene_done(item, sent_ti_path, asset_results)
        else:
            # Filter already-uploaded scenes when checkpointing is enabled
            if args.checkpoint_every > 0:
                work_items_to_process = []
                for dfo_path, event_id, sent_ti_path in work_items:
                    if scene_already_uploaded(
                        sent_ti_path,
                        asset_handler.results_df,
                        s3_utils,
                        args.bucket_name,
                        args.catalog_path,
                        catalog_id,
                    ):
                        item_id = item_id_from_sent_ti_path(sent_ti_path)
                        collection.add_link(
                            pystac.Link(
                                rel=pystac.RelType.ITEM,
                                target=f"./{item_id}/{item_id}.json",
                                media_type="application/geo+json",
                            )
                        )
                    else:
                        work_items_to_process.append((dfo_path, event_id, sent_ti_path))
                work_items = work_items_to_process

            logging.info(f"Work items to process: {len(work_items)}")

            worker = partial(
                _process_one_scene,
                profile=args.profile,
                bucket_name=args.bucket_name,
                link_type=args.link_type,
                reprocess_assets=args.reprocess_assets,
                skip_owp_qc=args.skip_owp_qc,
                hucs_gdf=hucs_gdf,
                country_boundaries=country_boundaries,
                derived_metadata_path=args.derived_metadata_path,
                initial_results_df=asset_handler.results_df,
            )
            with multiprocessing.Pool(args.workers) as pool:
                for result in pool.imap_unordered(worker, work_items):
                    item, sent_ti_path, asset_results = result
                    if item is not None and asset_results is not None:
                        on_scene_done(item, sent_ti_path, asset_results)

    # Flush remaining item buffer
    if args.checkpoint_every > 0:
        flush_item_batch(s3_utils, args.bucket_name, args.catalog_path, catalog_id, collection, item_buffer)

    # When using workers, main only merged into results_df; write to parquet before final upload
    if args.workers > 1:
        asset_handler.results_df.to_parquet(asset_handler.local_results_file, index=False)

    # When checkpoint_every <= 0, items were added via collection.add_item; set self href so pystac writes .json files
    if args.checkpoint_every <= 0:
        for item in list(collection.get_items()):
            item.set_self_href(f"{catalog_id}/{item.id}/{item.id}.json")

    s3_utils.update_collection(collection, catalog_id, args.catalog_path, args.bucket_name)
    collection.validate()

    asset_handler.upload_modified_parquet(remove_local=True)


if __name__ == "__main__":
    start_time = time.time()
    try:
        multiprocessing.set_start_method("spawn", force=True)
        main()
    finally:
        end_time = time.time()
        elapsed_time = end_time - start_time
        hours, rem = divmod(elapsed_time, 3600)
        minutes, seconds = divmod(rem, 60)
        logging.info(f"Total execution time: {int(hours)}h {int(minutes)}m {seconds:.2f}s")
