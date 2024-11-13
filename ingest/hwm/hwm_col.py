import pdb
import pandas as pd
import tempfile
import argparse
import geopandas as gpd
from dateutil.parser import parse as parse_date
import boto3
import os
import logging
from datetime import datetime, timezone
import pystac
from shapely.geometry import MultiPoint, Point, box, mapping
from pystac.extensions.projection import ProjectionExtension
from ingest.bench import S3Utils
from ingest.hwm.hwm_stac import create_wkt_string, flowfile_dir
from ingest.hwm.hwm_handle_assets import HWMAssetHandler
import matplotlib
matplotlib.use('Agg')  # Use the 'Agg' backend, which is non-interactive
import matplotlib.pyplot as plt

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
    parser.add_argument('--asset_object_key', type=str, default='benchmark/high_water_marks/usgs/outputs/all_events.gpkg', help='Key for the asset object in the S3 bucket. Is a single file in the case of the HWM data.')
    parser.add_argument('--hucs_object_key', type=str, default='benchmark/stac-bench-cat/assets/WBDHU8_webproj.gpkg', help='Where to download the gpkg with the huc8 info')
    parser.add_argument('--reprocess_assets', action='store_true', help='Set to true to reprocess assets using HWMAssetHandler')
    parser.add_argument('--derived_metadata_path', type=str, default='benchmark/stac-bench-cat/assets/derived-asset-data/hwm_collection.parquet', help='S3 key for the derived metadata Parquet file created by asset handling code.')
    return parser.parse_args()

def create_hwm_collection():
    collection = pystac.Collection(
        id='hwm-collection',
        description="This collection contains field observations of highwater marks for various flood events throughout the United States from the years 1888 to 2023.",
        title="High-Water Mark Collection",
        keywords=["flood", "field", "points","USGS"],
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent([[-179.15, 18.91, -66.95, 71.39]]),
            temporal=pystac.TemporalExtent([[datetime(1888, 7, 1, tzinfo=timezone.utc), datetime(2023, 7, 14)]])
        ),
        license='CC-BY-4.0',
        providers=[pystac.Provider(
            name='USGS',
            roles=[pystac.ProviderRole.PRODUCER, pystac.ProviderRole.PROCESSOR, pystac.ProviderRole.LICENSOR],
            description='The United States Geological Survey.',
            url='https://www.usgs.gov'
        )],
    )

    return collection

def create_thumbnail(gdf, output_path):
    fig, ax = plt.subplots(figsize=(10, 10))
    gdf.plot(ax=ax, color='blue', markersize=5)
    ax.set_axis_off()
    plt.tight_layout()
    plt.savefig(output_path, dpi=100, bbox_inches='tight', pad_inches=0)
    plt.close(fig)
    
def process_flood_events(s3_utils, bucket_name, asset_object_key, hucs_object_key, link_type, asset_handler, reprocess_assets, top_collection):
    _, hwm_gpkg = os.path.split(asset_object_key)
    _, hucs_gpkg = os.path.split(hucs_object_key)

    with tempfile.TemporaryDirectory() as tmpdir:
        local_hwm_path = f"{tmpdir}/{hwm_gpkg}"
        local_hucs_path = f"{tmpdir}/{hucs_gpkg}"

        # download the hwm and huc8 gpkg files
        s3_utils.s3_client.download_file(bucket_name, asset_object_key, local_hwm_path)
        s3_utils.s3_client.download_file(bucket_name, hucs_object_key, local_hucs_path)

        # read files into a geopandas 
        hwm_gdf = gpd.read_file(local_hwm_path)
        hucs_gdf = gpd.read_file(local_hucs_path)

        # group by event name
        hwm_events = hwm_gdf.groupby('eventName')
    
    for event_name, event_df in hwm_events:
        event_id = event_name.replace(" ", "_")
        print(f"processing {event_id}")

        # Create a STAC item for the event
        points = event_df.geometry.tolist()
        all_points = MultiPoint(points)
        event_bbox = all_points.bounds

        # Get the temporal extent for the event
        start_date = event_df['flag_date'].min()
        end_date = event_df['flag_date'].max()
        start_date = parse_date(start_date).replace(tzinfo=timezone.utc)
        end_date = parse_date(end_date).replace(tzinfo=timezone.utc)

         # Perform spatial join to find HUCs
        event_gdf = gpd.GeoDataFrame(geometry=[all_points], crs=event_df.crs)
        huc_join = gpd.sjoin(event_gdf, hucs_gdf, how="left", predicate="intersects")
        huc8_list = huc_join['HUC8'].tolist() if 'HUC8' in huc_join.columns else []

        event_item = pystac.Item(
            id=f'{event_id}-item',
            # Use convex hull around all the points as the item's geometry instead of the points themselves so that they render more quickly in stac-browser
            geometry=all_points.convex_hull.__geo_interface__,
            bbox=event_bbox,
            datetime=start_date,
            properties={
                "start_datetime": start_date.isoformat(),
                "end_datetime": end_date.isoformat(),
                "point_count": len(points),
                "hucs": huc8_list
            }
        )

        # Create a GeoPackage for the event
        with tempfile.TemporaryDirectory() as gpkg_tmpdir:
            event_gpkg_path = f"{gpkg_tmpdir}/{event_id}.gpkg"
            event_df.to_file(event_gpkg_path, driver="GPKG")
            # Upload the GeoPackage to S3
            gpkg_key = f"benchmark/high_water_marks/usgs/event_gpkgs/{event_id}.gpkg"
            s3_utils.s3_client.upload_file(event_gpkg_path, bucket_name, gpkg_key)

        # Add the GeoPackage as an asset to the event item
        gpkg_asset = pystac.Asset(
            href=s3_utils.generate_href(bucket_name, gpkg_key, link_type),
            media_type="application/geopackage+sqlite3",
            roles=["data"],
            title=f"GeoPackage for {event_id}",
            description="Contains point data and attributes for high water marks in this event"
        )
        event_item.add_asset("data", gpkg_asset)

        # Create and add thumbnail
        with tempfile.TemporaryDirectory() as thumb_tmpdir:
            thumbnail_path = f"{thumb_tmpdir}/{event_id}_thumbnail.png"
            create_thumbnail(event_df, thumbnail_path)
            thumbnail_key = f"benchmark/high_water_marks/usgs/thumbnails/{event_id}_thumbnail.png"
            s3_utils.s3_client.upload_file(thumbnail_path, bucket_name, thumbnail_key)

        thumbnail_asset = pystac.Asset(
            href=s3_utils.generate_href(bucket_name, thumbnail_key, link_type),
            media_type="image/png",
            roles=["thumbnail"],
            title=f"Thumbnail for {event_id}",
            description="Thumbnail of the event high water marks"
        )
        event_item.add_asset("thumbnail", thumbnail_asset)

        # Handle flowfile asset
        if asset_handler.event_processed(event_id) and not reprocess_assets:
            asset_results = asset_handler.read_data_parquet(event_id)
        else:
            asset_results = asset_handler.handle_assets(flowfile_dir, event_id, points)

        event_item.properties['flowfiles'] = asset_results["flowfile_object"] 
        flowfile_asset = pystac.Asset(
            href=s3_utils.generate_href(bucket_name, asset_results["flowfile_key"], link_type),
            roles=["data"],
            description="NWM 3.0 flowfile. see flowfiles key in properties for more information."
        )
        event_item.add_asset(f"{event_id}-flowfile", flowfile_asset)

        # Update the temporal extent of the event item using event_month
        if asset_results.get("event_month") is not None:
            event_month = asset_results["event_month"]
            start_of_month = event_month.replace(day=1, tzinfo=timezone.utc)
            end_of_month = (start_of_month + pd.offsets.MonthEnd(1)).replace(tzinfo=timezone.utc)
            event_item.properties["month_start"] = start_of_month.isoformat()
            event_item.properties["month_end"] = end_of_month.isoformat()

        # Add the event item to the top-level collection
        top_collection.add_item(event_item)

def main():
    args = parse_arguments()
    s3_utils = initialize_s3_utils()
    asset_handler = HWMAssetHandler(s3_utils, args.bucket_name, args.derived_metadata_path)
    top_collection = create_hwm_collection()
    process_flood_events(s3_utils, args.bucket_name, args.asset_object_key, args.hucs_object_key, args.link_type, asset_handler, args.reprocess_assets, top_collection)
    s3_utils.update_collection(top_collection, 'hwm-collection', args.catalog_path, args.bucket_name)
    top_collection.validate()
    asset_handler.upload_modified_parquet()

if __name__ == "__main__":
    main()
