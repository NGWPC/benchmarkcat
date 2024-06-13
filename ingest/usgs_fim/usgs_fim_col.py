import pdb
from shapely.geometry import mapping
import tempfile
import logging
import os
import json
import rasterio
import io
import pystac
from pystac.extensions.item_assets import ItemAssetsExtension, AssetDefinition
from datetime import datetime, timezone
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

from ingest.usgs_fim.usgs_fim_stac import *
from ingest.usgs_fim.usgs_fim_ext import USGSFIMExtension
from ingest import bench

# Set logging level for boto3
logging.basicConfig(level=logging.INFO)

# Create an S3 client
s3 = boto3.client('s3')

# link type set to 'url' for a signed url and 'uri' for an s3 uri
link_type = 'url'

# Specify bucket parameters
bucket_name = 'fimc-data'
catalog_path = 'benchmark/stac-bench-cat/'
asset_object_key = 'hand_fim/test_cases/usgs_test_cases/validation_data_usgs/'

# Define the collection
usgs_fim_col = pystac.Collection(
    id='usgs-fim-collection',
    description="This is a collection of base level elevation (usgs-fim) maps meant to be used to benchmark the performance of the National Water Centers Height Above Nearest Drainage (HAND) Maps",
    title="usgs-fim-benchmark-flood-rasters",
    keywords=["flood", "usgs-fim", "model", "extents", "depths"],
    extent=pystac.Extent(
        spatial=pystac.SpatialExtent([[-180, -90, 180, 90]]),
        temporal=pystac.TemporalExtent([[None, None]])
    ),
    license='CC0-1.0',
)

# Add list of item assets
ItemAssetsExtension.add_to(usgs_fim_col)

assets = {
    "thumbnail": AssetDefinition.create(
        title="Extent thumbnail",
        description="An quicklook showing one of the modeled flood extents for the region",
        media_type="image/png",
        roles=["thumbnail"],
    ),
    "extent_raster": AssetDefinition.create(
        title="Extent Raster",
        description="Raster of flood extent",
        media_type="image/tiff; application=geotiff",
        roles=["data"],
    ),
    "depth_raster": AssetDefinition.create(
        title="Depth Raster",
        description="Raster of flood depth",
        media_type="image/tiff; application=geotiff",
        roles=["data"],
    ),
    "flow_file": AssetDefinition.create(
        title="Flow File",
        description="CSV of flow file data for a given modeled flood magnitude",
        media_type="text/csv",
        roles=["data"],
    ),
    "study_report": AssetDefinition.create(
        title="Rating Curve",
        description="CSV of the rating curve used to compute modeled flows",
        media_type="application/csv",
        roles=["data"],
    )
}

# Add the assets to the collection
item_assets_ext = ItemAssetsExtension.ext(usgs_fim_col, add_if_missing=True)
item_assets_ext.item_assets = assets

# Get the list of HUCs
huc8list = bench.list_subdirectories(bucket_name, asset_object_key, s3)

for huc8_path in huc8list:
    print(f"huc8_path: {huc8_path}")
    huc8 = huc8_path.strip('/').split('/')[-1]
    print(f"indexing HUC8: {huc8}")
    # pdb.set_trace()
    thumbnail_created = False
    # need to go gauge by gauge
    for gauge_path in bench.list_subdirectories(bucket_name,huc8_path,s3):
        print(f"gauge_path: {gauge_path}")
        gauge = gauge_path.strip('/').split('/')[-1]
        # get geometry and initialize item
        geometry, bbox = load_domain_geometry(bucket_name,gauge_path,s3) 
        item = pystac.Item(
            id=f"{huc8}-{gauge}-usgs",
            geometry=geometry,
            bbox=bbox,
            collection=usgs_fim_col,
            datetime=datetime.now(timezone.utc), 
            properties={
                "title": f"HUC8-{huc8} gauge-{gauge} usgs fim",
                "description": "Extents and depths associated with the HEC-RAS modelling domain around the National Weather Service gauge used to model the flows",
                "license": 'CC0-1.0'
            }
        )

        # add the assets in the top level gauge directory
        item.add_asset(
        "rating curve",
        pystac.Asset(
                href= bench.generate_href(bucket_name, f"{gauge_path}/{gauge}_rating_curve.csv", s3, link_type),
                description="rating curve csv used for event stages",
                media_type="text/csv",
                roles=["data"]
            )
        )

        # Get the magnitudes available for this domain
        magnitudes = bench.list_directories_with_keywords(bucket_name,gauge_path,s3,['minor', 'major', 'moderate', 'action'])

        # Dictionary to store extent areas and flowfile assets
        extent_areas = {}
        flowfile_asset_ids = {"flowfile_ids": []}
        magnitude_list = []
        for magnitude_path in magnitudes:
            print(f"magnitude_path: {magnitude_path}")
            magnitude = magnitude_path.strip('/').split('/')[-1]
            magnitude_list.append(magnitude)
            # get the s3 path to the extent tif
            extent_path = bench.list_files_with_extensions(bucket_name,magnitude_path,s3,['.tif'])[0]
            print(f"extent_path: {extent_path}")
            # get the s3 path to the flowfile
            flow_path = bench.list_files_with_extensions(bucket_name,magnitude_path,s3,['csv'])[0]
            print(f"flow_path: {flow_path}")
                          
            # Temporary directory to download the extent files to extract extent area
            with tempfile.TemporaryDirectory() as tmpdir:
                local_extent_path = os.path.join(tmpdir, f'{magnitude}_extent.tif')

                # Download the TIFF files and flow files from S3
                try:
                    s3.download_file(bucket_name, extent_path, local_extent_path)
                    print(f"Downloaded {magnitude} extent raster to {tmpdir}")
                except NoCredentialsError:
                    print("Credentials not available")
                    continue
                except ClientError as e:
                    print(f"Failed to download files: {e}")
                    continue

                # Create a thumbnail for the first gauge
                if not thumbnail_created:
                    local_thumbnail_path = os.path.join(tmpdir, f'{magnitude}_thumbnail.png')

                    # Create thumbnail
                    bench.create_preview(local_extent_path, local_thumbnail_path)                
                    # Upload thumbnail to S3
                    thumbnail_s3_key = f'{gauge_path}/{gauge}_{magnitude}_thumbnail.png'
                    s3.upload_file(local_thumbnail_path, bucket_name, thumbnail_s3_key)

                    # add thumbnail to item
                    item.add_asset(
                        f"{magnitude}_thumbnail",
                        pystac.Asset(
                            href= bench.generate_href(bucket_name, thumbnail_s3_key, s3, link_type),
                            media_type="image/png",
                            roles=["thumbnail"],
                            title=f"{magnitude} thumbnail"
                        )
                    )
                    
                    thumbnail_created = True

                # get total inundated extent areas
                extent_area = bench.count_pixels(local_extent_path)
                extent_areas[f"{magnitude}_extent_raster"] = extent_area
                
            # Define assets for the item
            item.add_asset(
                f"{magnitude}_extent_raster",
                pystac.Asset(
                    href= bench.generate_href(bucket_name, extent_path, s3, link_type),
                    media_type="image/tiff; application=geotiff",
                    roles=["data"],
                    title=f"{magnitude} Flood Extent"
                )
            )
            item.add_asset(
                f"{magnitude}_flow_file",
                pystac.Asset(
                    href= bench.generate_href(bucket_name, flow_path, s3, link_type),
                    media_type="text/csv",
                    roles=["data"],
                    title=f"{magnitude} flood magnitude flowfile Data",
                    description="The flow file of NWM hydrofabric feature ids and associated discharges for this gauge domains {magnitude} flood magnitude."
                )
            )
            flowfile_asset_ids["flowfile_ids"].append(f"{magnitude}_flow_file")



        # Apply usgs-fim properties to the item
        item_usgs_fim_ext = USGSFIMExtension.ext(item, add_if_missing=True)
        item_usgs_fim_ext.apply(
            extent_area=extent_areas,
            huc8=int(huc8),
            gauge=gauge,
            flowfile=flowfile_asset_ids,
            magnitude= {"study magnitudes":magnitude_list}
        )


        # Add the item to the collection
        usgs_fim_col.add_item(item)

        # validate item
        # item.validate()

# add collection to catalog then write directory to s3
with tempfile.TemporaryDirectory() as temp_dir:

    # Download the catalog and all child collections to the temporary directory
    catalog_key = f'{catalog_path}catalog.json'
    pdb.set_trace()
    catalog, catalog_local_path = bench.download_catalog_and_collections(catalog_key, s3, bucket_name, temp_dir)

    # set root and self href for the catalog so can add/update the collection
    catalog.set_root(catalog)
    catalog.set_self_href(catalog_local_path)

    # remove child in case collection being updated
    try:
        catalog.remove_child('usgs-fim-collection')
    except KeyError:
        pass

    # Add collection to catalog
    catalog.add_child(usgs_fim_col)

    # Resave the catalog to the temporary directory after adding in the collection
    catalog.normalize_and_save(root_href=temp_dir, catalog_type=pystac.CatalogType.SELF_CONTAINED, skip_unresolved=True)    
    
    # Upload the contents of the temporary directory to S3
    bench.upload_directory_to_s3(temp_dir, bucket_name, catalog_path,s3)

 # Validate 
usgs_fim_col.validate()
