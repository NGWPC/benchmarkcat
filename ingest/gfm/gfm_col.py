import geopandas as gpd
import pdb
import pandas as pd
import boto3
import re
import os
import tempfile
import logging
from datetime import datetime, timezone
import pystac
from pystac.extensions.sat import SatExtension
from pystac.extensions.projection import ProjectionExtension
from pystac.extensions.item_assets import ItemAssetsExtension, AssetDefinition
from pystac.summaries import Summaries
from botocore.exceptions import NoCredentialsError, ClientError

from gfm_stac import *
from ingest import bench

# Set logging level for boto3
logging.basicConfig(level=logging.INFO)

# Create an S3 client
s3 = boto3.client('s3')

# link type set to 'url' for a signed url and 'uri' for an s3 uri
link_type = 'uri'

# Specify bucket parameters
bucket_name = 'fimc-data'
catalog_path = 'benchmark/stac-bench-cat/'
asset_object_key = 'benchmark/rs/'

# Define the collection
gfm_col = pystac.Collection(
    id='gfm-collection',
    description="This collection contains the 50+ Global Flood Monitoring (GFM) flood tile groups identified by using the Dartmouth Flood Observatory (DFO) event data. The events are a subset of the 900+ DFO events selected based on size, time frame (2015-present), and overlap with GFM scenes in the United States.",
    title="Global Flood Monitoring Collection",
    keywords=["flood", "GFM", "DFO", "NWS"],
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

# Add list of item assets
assets = {
    "thumbnail": AssetDefinition.create(
        title="Observed flood extent thumbnail",
        description="A black and white thumbnail showing the observed water in the Sentinel-1 tile.",
        media_type="image/png",
        roles=["thumbnail"]
    ),
    "observed-flood-extent": AssetDefinition.create(
        title="Observed flood extent",
        description="Observed water extent mask. Includes negative for areas observed as non-flooded in the Sentinel-1 image. Three layers (or three bands) of JS SQL backscatter intensity.",
        media_type="image/tiff; application=geotiff",
        roles=["data"]
    ),
    "observed-water-extent": AssetDefinition.create(
        title="Observed water extent",
        description="Open water extent mask for areas of regular or non-flooded open water. Does not assess reference mask.",
        media_type="image/tiff; application=geotiff",
        roles=["data"]
    ),
    "reference-water-mask": AssetDefinition.create(
        title="Reference water mask",
        description="Reference water mask of non-flooded open water. Includes negative for areas observed as non-water. Three bands (for each of three Sentinel-1 observations serving as a reference derived from the water.",
        media_type="image/tiff; application=geotiff",
        roles=["data"]
    ),
    "exclusion-mask": AssetDefinition.create(
        title="Exclusion mask",
        description="Areas where JS-SQL flood classification can be masked (e.g., river channels).",
        media_type="image/tiff; application=geotiff",
        roles=["data"]
    ),
    "likelihood-values": AssetDefinition.create(
        title="Likelihood values",
        description="Estimated likelihood of flood classification, for all areas outside the exclusion mask.",
        media_type="image/tiff; application=geotiff",
        roles=["data"]
    ),
    "affected-landcover": AssetDefinition.create(
        title="Affected landcover",
        description="Land cover / use (e.g. artificial surfaces, agricultural areas) in flooded areas, mapped by a spatial overlay of observed flood extent and the Copernicus GLS land cover.",
        media_type="image/tiff; application=geotiff",
        roles=["data"]
    ),
    "affected-population": AssetDefinition.create(
        title="Affected population",
        description="Number of people in flooded areas, mapped by a spatial overlay of observed flood extent and gridded population, from the Copernicus GHSL project.",
        media_type="image/tiff; application=geotiff",
        roles=["data"]
    ),
    "advisory-flags": AssetDefinition.create(
        title="Advisory flags",
        description="Flags indicating potential reduced quality of flood mapping, due to prevailing environmental conditions (e.g. wind, ice, snow, dry soil), or degraded input data quality due to signal interference from other SAR missions.",
        media_type="image/tiff; application=geotiff",
        roles=["data"]
    ),
    "sentinel-1-metadata": AssetDefinition.create(
        title="Sentinel-1 metadata",
        description="Information on the acquisition parameters of the Sentinel-1 data used.",
        media_type="application/json",
        roles=["metadata"]
    ),
    "dfo-event-footprint": AssetDefinition.create(
        title="DFO event footprint",
        description="This is the DFO footprint that was identified as intersecting with the scene.",
        media_type="application/geo+json",
        roles=["data"]
    )
}

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
        dfo_start_datetime = pd.to_datetime(event_row['began'].values[0])
        dfo_end_datetime = pd.to_datetime(event_row['ended'].values[0])

        # create flowfile object
        flowfile_key = bench.list_resources_with_string(bucket_name, sent_ti_path, s3, ['flows'])
        if flowfile_key:
            flowfile_df = download_flowfile(bucket_name, flowfile_key[0], s3)
            flowstats = extract_flowstats(flowfile_df)
            flowfile_object = create_flowfile_object("NWM_v3_flowfile",flowstats)
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
                "start_datetime": start_datetime.isoformat(),
                "end_datetime": end_datetime.isoformat(),
                "dfo_event_id": eventid,
                "dfo_start_datetime": dfo_start_datetime.isoformat(),
                "dfo_end_datetime": dfo_end_datetime.isoformat(),
                "proj:epsg": 27705,
                "gfm_version": gfm_version,
                "flowfile": flowfile_object
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
       
        # loop through the equi7grid tiles in the sentinel tile and add those assets
        for equi7tile in equi7tiles_list:
            tile_asset_list = bench.list_resources_with_string(bucket_name, sent_ti_path, s3, [equi7tile])
            for tile_asset_path in tile_asset_list:
                tile_asset =  tile_asset_path.strip('/').split('/')[-1]  
                item.add_asset(
                    tile_asset,
                    pystac.Asset(
                        href= bench.generate_href(bucket_name,tile_asset_path,s3, link_type)
                    )
                )

        # add item to collection
        gfm_col.add_item(item)

# pdb.set_trace()
# add collection to catalog then write directory to s3
with tempfile.TemporaryDirectory() as temp_dir:

    # Download the catalog and all child collections to the temporary directory
    catalog_key = f'{catalog_path}catalog.json'
    catalog, catalog_local_path = bench.download_catalog_and_collections(catalog_key, s3, bucket_name, temp_dir)

    # set root and self href for the catalog so can add/update the collection
    catalog.set_root(catalog)
    catalog.set_self_href(catalog_local_path)

    # remove child in case collection being updated
    try:
        catalog.remove_child('gfm-collection')
    except KeyError:
        pass

    # Add collection to catalog
    catalog.add_child(gfm_col)

    # Resave the catalog to the temporary directory after adding in the collection
    catalog.normalize_and_save(root_href=temp_dir, catalog_type=pystac.CatalogType.SELF_CONTAINED, skip_unresolved=True)    
    
    # Upload the contents of the temporary directory to S3
    bench.upload_directory_to_s3(temp_dir, bucket_name, catalog_path,s3)

 # Validate 
gfm_col.validate()
