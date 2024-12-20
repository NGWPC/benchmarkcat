from abc import ABC, abstractmethod
import pystac
from pystac.extensions.item_assets import ItemAssetsExtension
import logging

class CollectionCreator(ABC):
    def __init__(self, config, s3_utils):
        self.config = config
        self.s3_utils = s3_utils
        self.collection = None
        self.load_config()
        self.create_collection()

    def load_config(self):
        # Load and validate the configuration
        required_fields = [
            'dataset_name', 'description', 'title', 'keywords',
            'spatial_extent', 'temporal_extent', 'license'
        ]
        for field in required_fields:
            if field not in self.config:
                raise ValueError(f"Missing required config field: {field}")

    def create_collection(self):
        self.collection = pystac.Collection(
            id=self.config['dataset_name'],
            description=self.config['description'],
            title=self.config['title'],
            keywords=self.config.get('keywords', []),
            extent=pystac.Extent(
                spatial=pystac.SpatialExtent([self.config['spatial_extent']]),
                temporal=pystac.TemporalExtent([self.config['temporal_extent']])
            ),
            license=self.config['license'],
            providers=self.config.get('providers', []),
            summaries=self.config.get('summaries', None)
        )
        self.apply_common_collection_properties()
        # Set up Item Assets if provided
        if 'item_assets' in self.config:
            item_assets_ext = ItemAssetsExtension.ext(self.collection, add_if_missing=True)
            item_assets_ext.item_assets = self.config['item_assets']

    def apply_common_collection_properties(self):
        common_props = self.config.get('collection_common_properties', {})
        for key, value in common_props.items():
            self.collection.properties[key] = value

    @abstractmethod
    def process_items(self):
        # This method must be implemented by subclasses
        pass

    def apply_common_item_properties(self, item):
        common_props = self.config.get('item_common_properties', {})
        for key, value in common_props.items():
            item.properties[key] = value

    def add_assets_to_item(self, item, asset_info):
        for asset_id, asset_data in asset_info.items():
            href, is_valid = self.s3_utils.generate_href(asset_data['bucket'], asset_data['key'], asset_data['link_type'])
            if is_valid:
                item.add_asset(
                    asset_id,
                    pystac.Asset(
                        href=href,
                        media_type=asset_data.get('media_type'),
                        roles=asset_data.get('roles', []),
                        title=asset_data.get('title'),
                        description=asset_data.get('description')
                    )
                )
            else:
                logging.warning(f"Skipping asset {asset_id} - invalid or inaccessible")

    def finalize_collection(self):
        self.collection.validate()
        # Code to save or update the collection, e.g., upload to S3
