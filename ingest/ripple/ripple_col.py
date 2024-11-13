import argparse
import logging
from datetime import datetime, timezone
import boto3
import pystac
from pystac.extensions.projection import ProjectionExtension
from pystac.extensions.item_assets import ItemAssetsExtension
from .ripple_handle_assets import RippleFIMAssetHandler
from .ripple_stac import RippleInfo
from ingest.bench import S3Utils

logging.basicConfig(level=logging.INFO)

def initialize_s3_utils():
    s3 = boto3.client('s3')
    return S3Utils(s3)

def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--link_type', type=str, default='uri', help='Link type, either "url" or "uri"')
    parser.add_argument('--bucket_name', type=str, default='fimc-data', help='S3 bucket name')
    parser.add_argument('--catalog_path', type=str, default='benchmark/stac-bench-cat/', help='Path to STAC catalog')
    parser.add_argument('--asset_object_key', type=str, default='benchmark/ripple/', help='Key for asset object')
    parser.add_argument('--reprocess_assets', action='store_true', help='Reprocess assets')
    parser.add_argument('--derived_metadata_path', type=str, default='benchmark/stac-bench-cat/assets/derived-asset-data/ripple_fim_collection.parquet')
    return parser.parse_args()

def create_ripple_collection(s3_utils, bucket_name, asset_object_key, link_type, flowfile_info):
    collection = pystac.Collection(
        id='ripple-fim-collection',
        description="Collection of flood inundation maps produced using HEC-RAS libraries from FEMA's BLE and MIP datasets",
        title="Ripple Flood Inundation Mapping Collection",
        keywords=["flood", "HEC-RAS", "BLE", "MIP", "inundation"],
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent([[-125.0, 24.396308, -66.934570, 49.384358]]),
            temporal=pystac.TemporalExtent([[datetime.now(timezone.utc), None]])
        ),
        license='CC0-1.0'
    )

    # Add flowfile object to collection properties
    collection.extra_fields = {
        "flowfile": flowfile_info["flowfile_object"]
    }

    # Add collection-level flowfile assets
    for flowfile_id, flowfile_key in zip(
        flowfile_info["flowfile_ids"],
        flowfile_info["flowfile_keys"]
    ):
        collection.add_asset(
            flowfile_id,
            pystac.Asset(
                href=s3_utils.generate_href(bucket_name, flowfile_key, link_type),
                title=f"CONUS Flow Data for {flowfile_id.split('_')[2]}",
                description=f"Continental US flow data for {flowfile_id.split('_')[2]} flood magnitude",
                media_type="text/csv",
                roles=["data"]
            )
        )

    item_assets_ext = ItemAssetsExtension.ext(collection, add_if_missing=True)
    item_assets_ext.item_assets = RippleInfo.assets

    return collection

def process_source_directory(source_path, source, s3_utils, bucket_name, link_type, collection, reprocess_assets, asset_handler):
    subdirs = s3_utils.list_subdirectories(bucket_name, source_path)
    
    for subdir in subdirs:
        identifier = subdir.strip('/').split('/')[-1]
        logging.info(f"Processing {source} {identifier}")
        
        if asset_handler.assets_processed(subdir) and not reprocess_assets:
            asset_results = asset_handler.read_data_parquet(subdir)
        else:
            asset_results = asset_handler.handle_assets(subdir, source)

        # Convert numpy types to Python types in extent_areas
        if 'extent_areas' in asset_results:
            extent_areas = {}
            for mag, area in asset_results['extent_areas'].items():
                if hasattr(area, 'item'):  
                    extent_areas[mag] = area.item()
                else:
                    extent_areas[mag] = area
            asset_results['extent_areas'] = extent_areas

        # Create STAC item
        item = pystac.Item(
            id=f"ripple-{source}-{identifier}",
            geometry=asset_results["geometry"],
            bbox=asset_results["bbox"],
            datetime=datetime.now(timezone.utc),
            properties={
                "title": f"Ripple FIM {source.upper()} - {identifier}",
                "description": f"Flood inundation mapping for {identifier} using {source.upper()} data",
                "source": source,
                "magnitudes": asset_results["magnitudes"],
                "extent_areas (m)": extent_areas,  
            }
        )
        # Add source-specific spatial region identifier
        if source == 'ble':
            item.properties["hucs"] = [identifier]  
        else:  
            item.properties["region"] = identifier  

        # Add projection extension
        ProjectionExtension.ext(item, add_if_missing=True)
        item.properties.update({"proj:wkt2": asset_results["wkt2_string"]})

        # Add assets for each magnitude
        for magnitude in asset_results["magnitudes"]:
            # Add extent raster
            item.add_asset(
                f"{magnitude}_extent",
                pystac.Asset(
                    href=s3_utils.generate_href(bucket_name, f"{subdir}/{magnitude}_EastForkTrinity.tif", link_type),
                    media_type="image/tiff; application=geotiff",
                    roles=["data"],
                    title=f"{magnitude} Flood Extent"
                )
            )
            
            # Add domain boundary geopackage
            item.add_asset(
                f"{magnitude}_domain",
                pystac.Asset(
                    href=s3_utils.generate_href(bucket_name, f"{subdir}/{magnitude}_model_domain.gpkg", link_type),
                    media_type="application/geopackage+sqlite3",
                    roles=["data"],
                    title=f"{magnitude} Model Domain Boundary"
                )
            )

        collection.add_item(item)

def main():
    args = parse_arguments()
    s3_utils = initialize_s3_utils()
    
    # Initialize asset handler
    asset_handler = RippleFIMAssetHandler(s3_utils, args.bucket_name, args.derived_metadata_path)
    
    # Process collection-level flowfiles
    flowfile_info = asset_handler.process_collection_flowfiles(args.asset_object_key)
    
    # Create collection with flowfile information
    collection = create_ripple_collection(
        s3_utils,
        args.bucket_name,
        args.asset_object_key,
        args.link_type,
        flowfile_info
    )
    
    # Process BLE data
    ble_path = f"{args.asset_object_key}ble/"
    process_source_directory(
        ble_path, 
        'ble', 
        s3_utils, 
        args.bucket_name, 
        args.link_type, 
        collection, 
        args.reprocess_assets, 
        asset_handler
    )
    
    # Process MIP data
    mip_path = f"{args.asset_object_key}mip/"
    process_source_directory(
        mip_path,
        'mip',
        s3_utils,
        args.bucket_name,
        args.link_type,
        collection,
        args.reprocess_assets,
        asset_handler
    )
    
    # Update and validate collection
    s3_utils.update_collection(collection, 'ripple-fim-collection', args.catalog_path, args.bucket_name)
    collection.validate()
    
    # Upload modified parquet file
    asset_handler.upload_modified_parquet()

if __name__ == "__main__":
    main()
