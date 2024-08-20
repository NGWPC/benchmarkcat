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
from shapely.geometry import MultiPoint, Point
from pystac.extensions.projection import ProjectionExtension
from ingest.bench import S3Utils
from ingest.hwm.hwm_stac import create_wkt_string
logging.basicConfig(level=logging.INFO)

def initialize_s3_utils():
    s3 = boto3.client('s3')
    s3_utils = S3Utils(s3)
    return s3_utils

def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket_name', type=str, default='fimc-data', help='S3 bucket name')
    parser.add_argument('--catalog_path', type=str, default='benchmark/stac-bench-cat/', help='Path to the STAC catalog in the S3 bucket')
    parser.add_argument('--asset_object_key', type=str, default='benchmark/high_water_marks/usgs/outputs/all_events.gpkg', help='Key for the asset object in the S3 bucket. Is a single file in the case of the HWM data.')
    parser.add_argument('--hucs_object_key', type=str, default='benchmark/stac-bench-cat/assets/WBDHU8_webproj.gpkg', help='Where to download the gpkg with the huc8 info')
    parser.add_argument('--derived_metadata_path', type=str, default='benchmark/stac-bench-cat/assets/derived-asset-data/gfm_collection.parquet', help='S3 key for the derived metadata Parquet file created by asset handling code.')
    return parser.parse_args()

def create_hwm_collection():
    collection = pystac.Collection(
        id='hwm-collection',
        description="This collection contains field observations of highwater marks for various flood events throughout the United States from the years 1888 to 2023.",
        title="High-Water Mark Collection",
        keywords=["flood", "field", "points","USGS"],
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent([[-179.9, 7.2, -64.5, 61.8]]),
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

def process_flood_events(s3_utils, bucket_name, asset_object_key, hucs_object_key, top_collection):
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

        # Create a sub-collection for the event
        sub_collection = pystac.Collection(
            id=f'{event_id}-collection',
            description=f"Sub-collection for {event_id} event",
            extent=pystac.Extent(
                spatial=pystac.SpatialExtent([[-179.9, 7.2, -64.5, 61.8]]),
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

        # Process each point in the event
        for idx, row in event_df.iterrows():
            point = Point(row.geometry.x, row.geometry.y, row['elev_ft'])
            geometry = point

            # find the bounding box of the point
            bbox = geometry.bounds

            # extract datetime from "flag_date" column and format as YYYY-MM-DDTHH:MM:SSZ
            date_str = row['flag_date']
            datetime_obj = parse_date(date_str).replace(tzinfo=timezone.utc)

            # create a wkt string with the available information
            proj = create_wkt_string(
                verticalDatumName=row['verticalDatumName'],
                horizontalDatumName=row['horizontalDatumName']
            )

            # lift all the other attribute table columns and put into a dictionary. "latitude_dd" is just presenting latitude and longitude in a specific format. Including "latitude" and "longitude" in point properties because they might eb in a different projection than the EPSG4326 projection the gpkg all the points were pulled from are in.
            excluded_columns = [
                "eventName","flag_date", "site_latitude", 
                "site_longitude", "files", "lat4326", "lon4326", 
                "vertical_datums", "elev_ft", "adj_elev_ft","latitude_dd", "longitude_dd", 
                "horizontalDatumName", "verticalDatumName","geometry"
            ]
            prop_dict = row.drop(labels=excluded_columns).to_dict()

            # Filter out keys with NULL (None or NaN) values
            prop_dict = {k: v for k, v in prop_dict.items() if pd.notnull(v)}

            # Perform a join to find which HUC polygons the point is in
            point_gdf = gpd.GeoDataFrame([row], geometry='geometry', crs = "EPSG:4326")
            point_in_hucs = gpd.sjoin(point_gdf, hucs_gdf, how="left", predicate="within")

            huc8_list = point_in_hucs['HUC8'].unique().tolist()
            prop_dict['HUC8'] = huc8_list

            # Create a STAC item for the point
            point_item = create_item(f"{event_id}-{idx}", geometry, bbox, proj, datetime_obj, prop_dict)

            # Add the item to the event sub-collection
            sub_collection.add_item(point_item)

        # Add the sub-collection to the top-level collection
        top_collection.add_child(sub_collection)

def create_item(event_id, geometry, bbox, proj, datetime, prop_dict):
    item = pystac.Item(id=event_id,
                geometry=geometry.__geo_interface__,
                bbox=bbox,
                datetime=datetime,
                properties=prop_dict)
    item.properties["description"] = "This STAC item documents a single point in a HWM collection documenting an event. Each point's properties contains latitude and longitude fields that should correspond tot he horizontal datum provided in the item's WKT string."

    # Add projection information
    proj_ext = ProjectionExtension.ext(item, add_if_missing=True)
    proj_ext.wkt2 = proj
    return item

def main():
    args = parse_arguments()
    s3_utils = initialize_s3_utils()
    
    top_collection = create_hwm_collection()
    process_flood_events(s3_utils, args.bucket_name, args.asset_object_key, args.hucs_object_key, top_collection)
    s3_utils.update_collection(top_collection, 'hwm-collection', args.catalog_path, args.bucket_name)
    top_collection.validate()

if __name__ == "__main__":
    main()
