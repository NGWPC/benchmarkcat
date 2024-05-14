import tempfile
import logging
import requests
import os
import json
import rasterio
import urllib.request
import pystac
from pystac.extensions.projection import ProjectionExtension
from pystac.extensions.eo import EOExtension
from datetime import datetime, timezone, timedelta
from shapely.geometry import Polygon, mapping, box
from pyproj import Transformer
from tempfile import TemporaryDirectory
import numpy as np
import boto3
import re
from datetime import date
from botocore.exceptions import NoCredentialsError, ClientError
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles

from ble import blestac
from ingest import bench

# set logging level for boto3
logging.basicConfig(level=logging.INFO)

# Create an S3 client 
s3 = boto3.client('s3')

# Specify bucket parameters
bucket_name = 'fimc-data'
collection_object_key = 'benchmark/stac-bench-cat/collections/ble/ble.json'
asset_object_key = 'benchmark/stac-bench-cat/assets/ble/'

# define the collection 
ble_collection = pystac.Collection(
    id='ble-collection',
    description='This is a collection of base level elevation (BLE) maps meant to be used to benchmark the performance of the National Water Centers Height Above Nearest Drainage (HAND) Maps',
    title = "FEMA-BLE-benchmark-flood-rasters",
    keywords = ["FEMA", "flood", "BLE", "model", "extents", "depths"],
    extent=pystac.Extent(
        spatial=pystac.SpatialExtent([[-180, -90, 180, 90]]),
        temporal=pystac.TemporalExtent([[None, None]])
    ),
    license='CC0-1.0',
)

# get the list of hucs 
huc8list = bench.list_subdirectories(bucket_name,asset_object_key,s3)
print(f"huc8list:{huc8list}")

# loop through each huc8 directory and create items

# validation
## add the ble extension to pystac
