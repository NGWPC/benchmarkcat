import argparse
import geopandas as gpd
import pdb
import pandas as pd
import boto3
import os
import tempfile
import logging
from datetime import datetime, timezone
import pystac
from pystac.extensions.sat import SatExtension
from pystac.extensions.projection import ProjectionExtension
from pystac.extensions.item_assets import ItemAssetsExtension 
from pystac.summaries import Summaries
from botocore.exceptions import NoCredentialsError, ClientError

from gfm_stac import *
from ingest import bench

# Set logging level for boto3
logging.basicConfig(level=logging.INFO)

# Create an S3 client
s3 = boto3.client('s3')

parser = argparse.ArgumentParser()

# Add arguments
parser.add_argument('--link_type', type=str, default='url', help='Link type, either "url" or "uri"')
parser.add_argument('--bucket_name', type=str, default='fimc-data', help='S3 bucket name')
parser.add_argument('--catalog_path', type=str, default='benchmark/stac-bench-cat/', help='Path to the STAC catalog in the S3 bucket')
parser.add_argument('--asset_object_key', type=str, default='benchmark/rs/', help='Key for the asset object in the S3 bucket')

args = parser.parse_args()

link_type = args.link_type
bucket_name = args.bucket_name
catalog_path = args.catalog_path
asset_object_key = args.asset_object_key

# Define the collection
gfm_col = pystac.Collection(
    id='gfm-collection',
    description="This collection contains the 50+ Global Flood Monitoring (GFM) flood tile groups identified by using the Dartmouth Flood Observatory (DFO) event data. The events are a subset of the 900+ DFO events selected based on size, time frame (2015-present), and overlap with GFM scenes in the United States.",
    title="Global Flood Monitoring Collection",
    keywords=["flood", "GFM", "DFO"],
    extent=pystac.Extent(
        spatial=pystac.SpatialExtent([[-179.9, 7.2, -64.5, 61.8]]),
        temporal=pystac.TemporalExtent([[datetime(2015, 1, 1, tzinfo=timezone.utc), None]])
    ),
    license='CC-BY-4.0',
    providers=[pystac.Provider(
        name='GLOFAS',
        roles=[pystac.ProviderRole.PRODUCER,pystac.ProviderRole.PROCESSOR,pystac.ProviderRole.LICENSOR],
        description='The Global Flood Awareness System (GLOFAS) is a global hydrological forecasting and monitoring system that provides real-time flood monitoring and early warning information.',
        url='https://global-flood.emergency.copernicus.eu/'
    )],
    summaries = Summaries({
        'platform': ['Sentinel-1'],
        'constellation': ['Copernicus'],
        'instruments': ['SAR'],
        'datetime': [datetime(2015, 1, 1, tzinfo=timezone.utc).isoformat(), None],
        'providers': ['GLOFAS'],
        'GFM_layers': layers
    })
)

# attach the naming conventions metadata doc to the collection
gfm_col.assets['naming_conventions'] = pystac.Asset(
    href=bench.generate_href(bucket_name, f'{asset_object_key}/gfm/gfm_data_readme.pdf', s3, link_type),
    title="GFM Data Readme",
    description="This document contains the naming conventions for the GFM data as well as information on the output layers for each tile",
    media_type="application/pdf"
)

item_assets_ext = ItemAssetsExtension.ext(gfm_col, add_if_missing=True)
item_assets_ext.item_assets = assets

# Download the GeoPackage file from S3
geo_package_key = 'benchmark/rs/dfo_all_usa_events_post_2015.gpkg'
tmp_geo_package = '/tmp/dfo_all_usa_events_post_2015.gpkg'

s3.download_file(bucket_name, geo_package_key, tmp_geo_package)

# Load the GeoPackage file into a GeoDataFrame
gdf = gpd.read_file(tmp_geo_package)

# Get the list of DFO events
dfolist = bench.list_subdirectories(bucket_name, f"{asset_object_key}gfm/", s3)

for dfo_path in dfolist:
    eventid = dfo_path.strip('/').split('/')[-1]
    print(f"indexing DFO event: {eventid}")
    # get list of sentinel 1 tiles
    sent_ti_list = bench.list_subdirectories(bucket_name,dfo_path,s3)

    for sent_ti_path in sent_ti_list:
        thumbnail_created = False
        sent_ti = sent_ti_path.strip('/').split('/')[-1]
        # create geometry
        fp_list = bench.list_resources_with_string(bucket_name,sent_ti_path, s3, ['footprint'])     
        geometry, bbox = make_item_geom(bucket_name, fp_list, s3)  

        # get orbit info and gfm version  
        advflag_list = bench.list_resources_with_string(bucket_name,sent_ti_path, s3, ['ADVFLAG'])     

        if advflag_list:
            gfm_version = extract_version_string(advflag_list[0])
            orbit_direction = extract_orbit_state(advflag_list[0])
            if orbit_direction == 'A':
                orbit_state = 'ascending'
            else:
                orbit_state = 'descending'
        else:
            print(f"skipping gfm version and orbit direction for {sent_ti_path}")
            orbit_state = None
            gfm_version = None

        abs_orbit_num = extract_orbit_number(sent_ti_path)
        
        # get number of equi7grid tiles
        equi7tiles_list = [os.path.basename(filename).split('_')[1] for filename in advflag_list if len(os.path.basename(filename).split('_')) > 2]

        # get datetime
        start_datetime, end_datetime = extract_datetimes(sent_ti)

        # Extract dfo_start_datetime and dfo_end_datetime from the GeoDataFrame
        event_row = gdf[gdf['dfo_id'] == int(eventid)]
        dfo_start_datetime = pd.to_datetime(event_row['began'].values[0]).replace(tzinfo=timezone.utc)
        dfo_end_datetime = pd.to_datetime(event_row['ended'].values[0]).replace(tzinfo=timezone.utc)

        # create flowfile object
        flowfile_key = bench.list_resources_with_string(bucket_name, sent_ti_path, s3, ['flows'])
        if flowfile_key:
            flowfile_df = bench.download_flowfile(bucket_name, flowfile_key[0], s3)
            flowstats = bench.extract_flowstats(flowfile_df)
            flowfile_ids = ["NWM_v3_flowfile"]
            columns_list = [{
                "feature_id": {
                    "Column description": "feature id that identifies the stream segment being modeled or measured",
                    "Column data source": "NWM 3.0 hydrofabric",
                    "data_href": "https://water.noaa.gov/resources/downloads/nwm/NWM_channel_hydrofabric.tar.gz"
                },
                "discharge": {
                    "Column description": "Discharge in m^3/s",
                    "Column data source": "NWM 3.0 retrospective discharge data",
                    "data_href": "https://registry.opendata.aws/nwm-archive/"
                }
            }]
            flowfile_object = bench.create_flowfile_object(flowfile_ids,flowstats, columns_list)
        else:
            print("no flowfile detected")
            flowfile_object = None    

        # initialize item
        item = pystac.Item(
            id=f"DFO-{eventid}_tile-{sent_ti}",
            geometry=geometry,
            bbox=bbox,
            datetime=start_datetime,
            properties={
                "title": f"DFO-{eventid}_tile-{sent_ti}",
                "description": f"This item lists some of assets associated with the GFM scene {sent_ti}. Each asset is associated with an equi7grid tile within the GFM scene.",
                "sat:orbit_state": orbit_state,
                "sat:absolute_orbit": abs_orbit_num,
                "dfo_event_id": eventid,
                "dfo_start_datetime": dfo_start_datetime.replace(tzinfo=timezone.utc).isoformat(),
                "dfo_end_datetime": dfo_end_datetime.replace(tzinfo=timezone.utc).isoformat(),
                "proj:epsg": 27705,
                "gfm_version": gfm_version,
                "flowfiles": flowfile_object
            }
        )
        # add the sat and projection extension to the item
        sat_ext = SatExtension.ext(item, add_if_missing=True)
        proj_ext = ProjectionExtension.ext(item, add_if_missing=True)

        # add the flowfile asset
        if flowfile_key:
            item.add_asset(
                "NWM_v3_flowfile",
                pystac.Asset(
                    href= bench.generate_href(bucket_name,flowfile_key[0],s3,link_type),
                    media_type="application/json",
                    roles=["data"],
                    description=f"flowfile for granule: {sent_ti}"
                )
            )
       
        # loop through the equi7grid tiles in the sentinel tile and add those assets also create an equi7tile_assets object that can be attached to the items properties
        equi7tile_assets = {}
        for equi7tile in equi7tiles_list:
            tile_asset_list = bench.list_resources_with_string(bucket_name, sent_ti_path, s3, [equi7tile])
            equi7tile_assets[equi7tile] = []

            for tile_asset_path in tile_asset_list:
                tile_asset =  tile_asset_path.strip('/').split('/')[-1]  

                # Extract the asset type from the tile_asset name
                asset_type = determine_asset_type(tile_asset)
                if asset_type in ['Footprint','Metadata', 'Schedule']:
                    role = 'metadata'
                else:
                    role = 'data'

                media_type = get_media_type(tile_asset)
                asset_id = f"{equi7tile}_{asset_type.replace(' ', '_')}"
                equi7tile_assets[equi7tile].append(asset_id)

                item.add_asset(
                    asset_id,
                    pystac.Asset(
                        href=bench.generate_href(bucket_name, tile_asset_path, s3, link_type),
                        roles=[role],
                        media_type=media_type,
                        title=asset_type
                    )
                )
                with tempfile.TemporaryDirectory() as tmpdir:
                    if not thumbnail_created and asset_type == 'Observed Water Extent':
                
                        local_extent_path = os.path.join(tmpdir, f'{equi7tile}_extent.tif')

                        # Download the TIFF files and flow files from S3
                        try:
                            s3.download_file(bucket_name, tile_asset_path, local_extent_path)
                            print(f"Downloaded extent raster to {tmpdir}")
                        except NoCredentialsError:
                            print("Credentials not available")
                            continue
                        except ClientError as e:
                            print(f"Failed to download files: {e}")
                            continue

                        # Create a thumbnail for the first gauge
                        if not thumbnail_created:
                            local_thumbnail_path = os.path.join(tmpdir, f'{equi7tile}_extent_thumbnail.png')

                            # Create thumbnail
                            bench.create_preview(local_extent_path, local_thumbnail_path)                
                            # Upload thumbnail to S3
                            thumbnail_s3_key = f'{sent_ti_path}{equi7tile}_extent_thumbnail.png'
                            s3.upload_file(local_thumbnail_path, bucket_name, thumbnail_s3_key)

                            # add thumbnail to item
                            item.add_asset(
                                f"{equi7tile}_thumbnail",
                                pystac.Asset(
                                    href= bench.generate_href(bucket_name, thumbnail_s3_key, s3, link_type),
                                    media_type="image/png",
                                    roles=["thumbnail"],
                                    title=f"{equi7tile} thumbnail"
                                )
                            )
                    
                            thumbnail_created = True

        item.properties['equi7tile_assets'] = equi7tile_assets

        # add item to collection
        gfm_col.add_item(item)

# pdb.set_trace()
# add collection to catalog then write directory to s3
bench.update_collection(gfm_col, 'gfm-collection', catalog_path, s3, bucket_name)

 # Validate 
gfm_col.validate()
