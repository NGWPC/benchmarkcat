from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Union, Any
import pystac
from pystac.extensions.item_assets import ItemAssetsExtension
import logging
from pydantic import BaseModel, ValidationError, create_model
import json
import os

class CollectionCreator(ABC):
    def __init__(self, config, s3_utils):
        self.config = config
        self.s3_utils = s3_utils
        self.collection = None
        self.load_config()
        self.create_collection()

    def load_config(self):
        try:
            # Load cross-collection schema
            schema_path = os.path.join('config', 'cross_collection.json')
            with open(schema_path, 'r') as f:
                cross_collection_schema = json.load(f)

            # Convert JSON Schema to Pydantic model
            CrossCollectionModel = self.create_pydantic_model_from_schema(cross_collection_schema)

            # Extract cross-collection properties from config
            cross_collection_props = self.config.get('collection_properties', {})

            # Validate properties using Pydantic
            validated_props = CrossCollectionModel(**cross_collection_props)

            # Update config with validated properties
            self.config['collection_properties'] = validated_props.dict()

        except ValidationError as e:
            raise ValueError(f"Config validation error: {e}")

    def create_pydantic_model_from_schema(self, schema: Dict[str, Any]) -> BaseModel:
        # Helper function to map JSON Schema types to Pydantic types
        def map_type(json_type):
            type_mapping = {
                "string": str,
                "number": float,
                "integer": int,
                "boolean": bool,
                "array": List[Any],
                "object": Dict[str, Any],
            }
            return type_mapping.get(json_type, Any)

        # Generate fields for Pydantic model
        fields = {}
        properties = schema.get('properties', {})
        required_fields = schema.get('required', [])

        for field_name, field_info in properties.items():
            field_type = map_type(field_info.get('type', 'string'))
            default = ... if field_name in required_fields else None
            fields[field_name] = (field_type, default)

        # Create Pydantic model dynamically
        return create_model('CrossCollectionModel', **fields)

    def create_collection(self):
        collection_props = self.config['collection_properties']
        self.collection = pystac.Collection(
            id=collection_props['id'],
            description=collection_props['description'],
            title=collection_props['title'],
            keywords=collection_props.get('keywords', []),
            extent=pystac.Extent(
                spatial=pystac.SpatialExtent([collection_props['extent']['spatial']]),
                temporal=pystac.TemporalExtent([collection_props['extent']['temporal']])
            ),
            license=collection_props['license'],
            providers=collection_props.get('providers', []),
            summaries=collection_props.get('summaries', None)
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
