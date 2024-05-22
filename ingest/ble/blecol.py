import pdb
from shapely.geometry import mapping
import tempfile
import logging
import os
import json
import rasterio
import pystac
from pystac.extensions.table import Column, TableExtension
from pystac.extensions.item_assets import ItemAssetsExtension, AssetDefinition
from datetime import datetime, timezone
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

from ingest.ble.blestac import *
from ingest.ble.ble_ext import BLEExtension
from ingest import bench

# Set logging level for boto3
logging.basicConfig(level=logging.INFO)

# Create an S3 client
s3 = boto3.client('s3')

# Specify bucket parameters
bucket_name = 'fimc-data'
collection_object_key = 'benchmark/stac-bench-cat/collections/ble/ble.json'
item_base_key = 'benchmark/stac-bench-cat/items/ble/'
catalog_key = 'benchmark/stac-bench-cat/bench_cat.json'
asset_object_key = 'benchmark/stac-bench-cat/assets/ble/'

# Define the collection
ble_collection = pystac.Collection(
    id='ble-collection',
    description="This is a collection of base level elevation (BLE) maps meant to be used to benchmark the performance of the National Water Centers Height Above Nearest Drainage (HAND) Maps",
    title="FEMA-BLE-benchmark-flood-rasters",
    keywords=["FEMA", "flood", "BLE", "model", "extents", "depths"],
    extent=pystac.Extent(
        spatial=pystac.SpatialExtent([[-180, -90, 180, 90]]),
        temporal=pystac.TemporalExtent([[None, None]])
    ),
    license='CC0-1.0',
)

# Add links to the collection
ble_collection.add_link(pystac.Link('self', 'ble.json'))
ble_collection.add_link(pystac.Link('root', '../../bench_cat.json'))
ble_collection.add_link(pystac.Link('parent','../../bench_cat.json'))

# Add table extension
TableExtension.add_to(ble_collection)

# Define the table columns schema
table_columns = [
    Column({
        "name": "feature_id",
        "description": "NWM hydrofabric feature_id",
        "type": "integer"
    }),
    Column({
        "name": "discharge",
        "description": "Discharge value in m^3/s",
        "type": "number"
    })
]

TableExtension.columns = table_columns
table_ext = TableExtension.ext(ble_collection, add_if_missing=True)
table_ext.columns = table_columns

# Add list of item assets
ItemAssetsExtension.add_to(ble_collection)
assets = {
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
    "feature_ids": AssetDefinition.create(
        title="Feature IDs",
        description="GeoJSON of feature IDs",
        media_type="application/geo+json",
        roles=["data"],
    ),
    "flow_file": AssetDefinition.create(
        title="Flow File",
        description="CSV of flow data",
        media_type="text/csv",
        roles=["data"],
    ),
    "hydraulic_parameters": AssetDefinition.create(
        title="Hydraulic Parameters",
        description="XML file of hydraulic parameters",
        media_type="text/xml",
        roles=["metadata"],
    ),
    "study_map": AssetDefinition.create(
        title="Study Map",
        description="PDF or image of the study map",
        media_type="application/pdf",
        roles=["map"],
    ),
    "study_report": AssetDefinition.create(
        title="Study Report",
        description="PDF of the study report",
        media_type="application/pdf",
        roles=["report"],
    )
}

# Add the assets to the collection
item_assets_ext = ItemAssetsExtension.ext(ble_collection, add_if_missing=True)
item_assets_ext.item_assets = assets

# Get the list of HUCs
huc8list = bench.list_subdirectories(bucket_name, asset_object_key, s3)

for huc8_path in huc8list:
    huc8 = huc8_path.strip('/').split('/')[-1]
    print(f"indexing HUC8: {huc8}")

    # Asset paths (relative to data object key)
    one_hund_flow = f'{asset_object_key}{huc8}/100yr/ble_huc_{huc8}_flows_100yr.csv'
    one_hund_extent = f'{asset_object_key}{huc8}/100yr/ble_huc_{huc8}_extent_100yr.tif'
    one_hund_depth = f'{asset_object_key}{huc8}/100yr/ble_huc_{huc8}_depth_100yr.tif'
    five_hund_flow = f'{asset_object_key}{huc8}/500yr/ble_huc_{huc8}_flows_500yr.csv'
    five_hund_extent = f'{asset_object_key}{huc8}/500yr/ble_huc_{huc8}_extent_500yr.tif'
    five_hund_depth = f'{asset_object_key}{huc8}/500yr/ble_huc_{huc8}_depth_500yr.tif'

    # find doc subdirectories for report asset
    docdir = bench.find_directories_with_sequence(bucket_name, f'{asset_object_key}alldocs/', s3, huc8)[0]
    report_doc = bench.list_pdfs_in_directory(bucket_name,docdir,s3)[0]

    # Temporary directory to download the file
    with tempfile.TemporaryDirectory() as tmpdir:
        one_hund_extent_path = os.path.join(tmpdir, 'extent_100yr.tif')
        five_hund_extent_path = os.path.join(tmpdir, 'extent_500yr.tif')
        one_hund_flow_path = os.path.join(tmpdir, 'flows_100yr.csv')
        five_hund_flow_path = os.path.join(tmpdir, 'flows_500yr.csv')

        # Download the TIFF files and flow files from S3
        try:
            s3.download_file(bucket_name, one_hund_extent, one_hund_extent_path)
            s3.download_file(bucket_name, five_hund_extent, five_hund_extent_path)
            s3.download_file(bucket_name, one_hund_flow, one_hund_flow_path)
            s3.download_file(bucket_name, five_hund_flow, five_hund_flow_path)
            print(f"Downloaded {one_hund_extent}, {five_hund_extent}, {one_hund_flow}, and {five_hund_flow} to {tmpdir}")
        except NoCredentialsError:
            print("Credentials not available")
            continue
        except ClientError as e:
            print(f"Failed to download files: {e}")
            continue
        # get total inundated extent areas
        one_hund_extent_area = count_nonzero_pixels(one_hund_extent_path)
        five_hund_extent_area = count_nonzero_pixels(five_hund_extent_path)

        # Use rasterio to extract bbox, resolution, and projection for 100yr extent
        with rasterio.open(one_hund_extent_path) as src:
            bbox = src.bounds
            resolution = src.res
            projection = src.crs.to_string()
            geometry = mapping(bench.get_huc8_geometry(huc8))
            # Create item
            item = pystac.Item(
                id=f"{huc8}-ble",
                geometry=geometry,
                bbox=list(bbox),
                collection=ble_collection,
                datetime=datetime.now(timezone.utc), 
                properties={
                    "title": f"HUC8 {huc8} BLE Data",
                    "description": "Extents and depths associated with the 100 yr and 500 yr flood magnitudes of this HUC8 BLE study",
                    "resolution": resolution,
                    "projection": projection,
                    "license": 'CC0-1.0',
                }
            )

            # Add links to the item
            item_object_key = f'{item_base_key}{huc8}_ble.json'
            item.add_link(pystac.Link('self', f'{huc8}_ble.json'))
            item.add_link(pystac.Link('parent', '../collections/ble/ble.json'))
            item.add_link(pystac.Link('root', '../../bench_cat.json'))

        # Apply BLE properties to the item
        item_ble_ext = BLEExtension.ext(item, add_if_missing=True)
        BLEExtension.get_schema_uri
        item_ble_ext.apply(
            extent_area={"100 yr extent area": one_hund_extent_area, "500 yr extent area": five_hund_extent_area},
            model_dimension=[2],
            magnitude=[100, 500],
            huc8=int(huc8),
        )

        # Define assets for the item
        item.add_asset(
            "extent_raster_100yr",
            pystac.Asset(
                href=f"s3://{bucket_name}/{one_hund_extent}",
                media_type="image/tiff; application=geotiff",
                roles=["data"],
                title="100 Year Flood Extent"
            )
        )
        item.add_asset(
            "extent_raster_500yr",
            pystac.Asset(
                href=f"s3://{bucket_name}/{five_hund_extent}",
                media_type="image/tiff; application=geotiff",
                roles=["data"],
                title="500 Year Flood Extent"
            )
        )

        item.add_asset(
            "depth_raster_100yr",
            pystac.Asset(
                href=f"s3://{bucket_name}/{one_hund_depth}",
                media_type="image/tiff; application=geotiff",
                roles=["data"],
                title="100 Year Flood Depth"
            )
        )
        item.add_asset(
            "depth_raster_500yr",
            pystac.Asset(
                href=f"s3://{bucket_name}/{five_hund_depth}",
                media_type="image/tiff; application=geotiff",
                roles=["data"],
                title="500 Year Flood Depth"
            )
        )
        item.add_asset(
            "flow_file_100yr",
            pystac.Asset(
                href=f"s3://{bucket_name}/{one_hund_flow}",
                media_type="text/csv",
                roles=["data"],
                title="100 Year Flow Data",
                description="The flow file of NWM hydrofabric feature ids and associated discharges for the 100 yr recurrance interval. See the item's table:columns for a description of flow-file columns"
            )
        )
        item.add_asset(
            "flow_file_500yr",
            pystac.Asset(
                href=f"s3://{bucket_name}/{five_hund_flow}",
                media_type="text/csv",
                roles=["data"],
                title="500 Year Flow Data",
                description="The flow file of NWM hydrofabric feature ids and associated discharges for the 500 yr recurrance interval. See the item's table:columns for a description of flow-file columns"
            )
        )
        item.add_asset(
        "Study Report",
        pystac.Asset(
                href=f"s3://{bucket_name}/{report_doc}",
                description="PDF of the study report",
                media_type="application/pdf",
                roles=["report"]
        )
        )
        # Add Table Extension to the item and configure tables
        TableExtension.add_to(item)
        table_ext = TableExtension.ext(item, add_if_missing=True)
        table_ext.columns = table_columns

        # Add the item to the collection
        ble_collection.add_item(item)

        # Write item to S3
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
            item_json = item.to_dict()
            json.dump(item_json, temp_file, indent=4)
            temp_file.close()
            s3.upload_file(temp_file.name, bucket_name, item_object_key)
            os.remove(temp_file.name)

        # validate item
        item.validate()


# Write collection to S3
with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
    collection_json = ble_collection.to_dict()
    json.dump(collection_json, temp_file, indent=4)
    temp_file.close()
    s3.upload_file(temp_file.name, bucket_name, collection_object_key)
    os.remove(temp_file.name)

# Validate 
ble_collection.validate()

# TODO:
# - make catalog json, add ble collection, replace absolute paths with relative paths, test resolving links with pystac using absolute catalog root
