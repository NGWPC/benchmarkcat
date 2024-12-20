## Overview
- I am working towards the code in the "ingest" directory of this project being structured like:
-
- **`collection_processors/`**: Contains modules for each dataset.
	- `ahps.py`
	- `ble.py`
	- `gfm.py`
	- `hwm.py`
- **`core/`**: Contains core classes and utilities shared across all datasets.
	- `asset_handler.py`
	- `collection_creator.py`
	- `stac_utils.py`
	- `s3_utils.py`
	- `flowfile_utils.py`
	- `geo_utils.py`
- **`main.py`**: Entry point for processing datasets.
- **`config/`**: Contains configuration files for datasets. This configuration ranges from fields that will go into a collection or item properties to paths needed to access the data.
	- `ahps.json`
	- `ble.json`
	- `gfm.json`
	- `hwm.json`
- ## **`core/`** files and classes
- `s3_utils.py`,`flowfile_utils.py`,`raster_utils.py` already exist to a large degree in bench but will need to be broken up into different files. In some cases like `raster_utils.py` it might be helpful to broaden the functionality to try to generalize as much of the raster processing as possible. Could also consider broadening this to `geo_utils.py` . This would contain classes that try to generalize functionality around both handling raster data (like thumbnail creation) but also generalize out common functionality that is used to handle vector data.
-
- `stac_utils.py` would hold methods used in more than 1 collection creation process that chain together pystac calls to do something you've found useful.
-
- `asset_handler.py` and `collection_creator.py` are abstract base classes that implements shared functionality and defines a contract for the minimum methods that the classes that inherit from it need to have.
-
- Things you want `collection_creator.py` to do:
	- Read in the "item_common_properties" and "collection_common_properties" object in your dataset config file and add those common properties to a given item or collection after validating the config file.
-
- ## Collection processors
- These files would take the place of the "datasource_col.py" and "data_handle_assets.py" files you currently have. They would create classes that inherit from `asset_handler.py` and `collection_creator.py` respectively and then implement the methods specific to that collection.
-
- Here is an example processor file (remember the actual details are subject to change this is meant to be a sketch):
  
  
  ```python
    import os
    from core.asset_handler import AssetHandler
    from core.collection_creator import CollectionCreator
    from core.stac_utils import create_stac_item
    from .stac_info import AHPS_STAC_Info #Really want this info to be read in from the AHPS config file
  
    class AHPSAssetHandler(AssetHandler):
        def load_results(self):
            # Implementation specific to AHPS
            pass
  
        def assets_processed(self, identifier) -> bool:
            # Check if assets have been processed
            pass
  
        def read_data_parquet(self, identifier):
            # Read processed data
            pass
  
        def handle_assets(self, identifier):
            # Process assets and extract metadata
            pass
  
        def write_data_parquet(self, results):
            # Write results to Parquet
            pass
  
    class AHPSCollectionCreator(CollectionCreator):
        def create_collection(self):
            # Use configurations to create collection
            pass
  
        def process_items(self):
            # Process items for the collection
            pass
  ```
- ## Config files
- The goal of the config files is to create a better separation of code and data. When the code doesn't need to do any work to add a field to a collection (because it is provided by the author) it should just go in the config file.
- Other things that would go into the config file are:
	- paths that will be used to read in data and write paths. This means that there are several arguments that right now are set when a dataset_col.py script is called that can be set from config files instead.
	- s3 bucket name
	- asset types and other asset information
-
- required elements:
	- dataset_name
	- description
	- keywords
	- spatial_extent
	- temporal_extent
	- item_common_properties
	- collection_common_properties
- An example portion of a config file:
- {
    "dataset_name": "ahps",
    "description": "This is a collection of base-level elevation maps...",
    "title": "AHPS FIM Benchmark Flood Rasters",
    "keywords": [
      "flood",
      "ahps-fim",
      "model",
      "extents",
      "depths",
      "HEC-RAS"
    ],
    "spatial_extent": [
      -179.15,
      18.91,
      -66.95,
      71.39
    ],
    "temporal_extent": [
      null,
      null
    ],
    "license": "CC0-1.0",
    "provider": {
      "name": "NWS",
      "roles": [
        "producer",
        "processor",
        "licensor"
      ],
      "url": "https://www.weather.gov/"
    },
    "derived_metadata_path": "benchmark/stac-bench-cat/assets/derived-asset-data/ahps_fim_collection.parquet",
    "item_assets": {
      "thumbnail": {
        "title": "Extent thumbnail",
        "description": "A quicklook showing...",
        "media_type": "image/png",
        "roles": [
          "thumbnail"
        ]
      },
      "extent_raster": {
        "title": "Extent Raster",
        "description": "Raster of flood extent",
        "media_type": "image/tiff; application=geotiff",
        "roles": [
          "data"
        ]
      }
    }
  }
- ## main.py
- Below is an example sketch of main.py. The specifics are subject to change but note how know main.py servers as a way to specify which collections you want run with arguments:
- ```python
    import argparse
    from core.s3_utils import S3Utils
    from dataset_processors.ahps.processor import AHPSCollectionCreator
    from dataset_processors.ble.processor import BLECollectionCreator
    # Import other dataset processors
  
    def parse_arguments():
        parser = argparse.ArgumentParser()
        parser.add_argument('dataset', choices=['ahps', 'ble', 'gfm', 'hwm'], help='Dataset to process')
        parser.add_argument('--reprocess_assets', action='store_true', help='Reprocess assets')
        # ... other arguments
        return parser.parse_args()
  
    def main():
        args = parse_arguments()
        s3_utils = S3Utils()
  
        # Load dataset-specific configuration
        config = load_config(f'config/{args.dataset}.yaml')
  
        if args.dataset == 'ahps':
            collection_creator = AHPSCollectionCreator(config, s3_utils)
        elif args.dataset == 'ble':
            collection_creator = BLECollectionCreator(config, s3_utils)
        # ... other datasets
  
        collection_creator.create_collection()
        collection_creator.process_items()
  
    if __name__ == '__main__':
        main()
  ```
-
-
- ## Notes
- The config files collection and item information boilerplate can actually probably be called from both the base class and each dataset's processors.

- Want to use pydantic to perform additional validation both at the level of the generic collection base class (for catalog wide fields) and at the level of individual collections
