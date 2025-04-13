import argparse
import logging
from datetime import datetime, timezone
import boto3
import pystac
from pystac.extensions.projection import ProjectionExtension
from pystac.extensions.item_assets import ItemAssetsExtension
import pdb
import re
from .ripple_handle_assets import RippleFIMAssetHandler
from .ripple_stac import RippleInfo
from ingest.bench import S3Utils

logging.basicConfig(level=logging.INFO)


def initialize_s3_utils():
    s3 = boto3.client("s3")
    return S3Utils(s3)


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--link_type", type=str, default="uri", help='Link type, either "url" or "uri"'
    )
    parser.add_argument(
        "--bucket_name", type=str, default="fimc-data", help="S3 bucket name"
    )
    parser.add_argument(
        "--catalog_path",
        type=str,
        default="benchmark/stac-bench-cat/",
        help="Path to STAC catalog",
    )
    parser.add_argument(
        "--asset_object_key",
        type=str,
        default="benchmark/ripple_fim_100/",
        help="Key for asset object",
    )
    parser.add_argument(
        "--reprocess_assets", action="store_true", help="Reprocess assets"
    )
    parser.add_argument(
        "--derived_metadata_path",
        type=str,
        default="benchmark/stac-bench-cat/assets/derived-asset-data/ripple_fim_collection.parquet",
    )
    parser.add_argument(
        "--f2fim_ver",
        type=str,
        default="0_3_0",
        help="flows2fim version",
    )
    parser.add_argument(
        "--ripple_ver",
        type=str,
        default="0_10_3",
        help="ripple version",
    )
    return parser.parse_args()


def extract_huc_code(identifier):
    """Extract 2-12 digit length HUC code from identifier using regex."""
    match = re.search(r"\b(\d{2}|\d{6}|\d{8}|\d{10}|\d{12})\b", identifier)
    if match:
        return match.group(0)
    logging.warning(f"No valid HUC code found in identifier: {identifier}")
    return None


def create_ripple_collection(
    s3_utils, bucket_name, asset_object_key, link_type, flowfile_info
):
    collection = pystac.Collection(
        id="ripple-fim-collection",
        description="Collection of flood inundation maps produced using HEC-RAS libraries from FEMA's BLE and MIP datasets",
        title="Ripple Flood Inundation Mapping Collection",
        keywords=["flood", "HEC-RAS", "BLE", "MIP", "inundation"],
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent([[-125.0, 24.396308, -66.934570, 49.384358]]),
            temporal=pystac.TemporalExtent([[datetime.now(timezone.utc), None]]),
        ),
        license="CC0-1.0",
    )

    # Add flowfile object to collection properties
    collection.extra_fields = {"flowfile": flowfile_info["flowfile_object"]}

    # Add collection-level flowfile assets
    for flowfile_id, flowfile_key in zip(
        flowfile_info["flowfile_ids"], flowfile_info["flowfile_keys"]
    ):
        flow_href, is_valid = s3_utils.generate_href(
            bucket_name, flowfile_key, link_type
        )
        if is_valid:
            collection.add_asset(
                flowfile_id,
                pystac.Asset(
                    href=flow_href,
                    title=f"CONUS Flow Data for {flowfile_id.split('_')[2]}",
                    description=f"Continental US flow data for {flowfile_id.split('_')[2]} flood magnitude",
                    media_type="text/csv",
                    roles=["data"],
                ),
            )
        else:
            print(
                f"Skipping flowfile asset for flowfile_id {flowfile_id} - invalid or inaccessible"
            )

    item_assets_ext = ItemAssetsExtension.ext(collection, add_if_missing=True)
    item_assets_ext.item_assets = RippleInfo.assets

    return collection


def process_source_directory(
    source_path,
    source,
    s3_utils,
    bucket_name,
    link_type,
    collection,
    reprocess_assets,
    asset_handler,
    f2fim_ver,
    ripple_ver,
):
    subdirs = s3_utils.list_subdirectories(bucket_name, source_path)

    for subdir in subdirs:
        identifier = subdir.strip("/").split("/")[-1]
        logging.info(f"Processing {source} {identifier}")

        huc_code = extract_huc_code(identifier)
        hucs_list = [huc_code] if huc_code else []

        if asset_handler.assets_processed(subdir) and not reprocess_assets:
            asset_results = asset_handler.read_data_parquet(subdir)
        else:
            asset_results = asset_handler.handle_assets(subdir, source)

        # Convert numpy types to Python types in extent_areas
        if "extent_areas" in asset_results:
            extent_areas = {}
            for mag, area in asset_results["extent_areas"].items():
                if hasattr(area, "item"):
                    extent_areas[mag] = area.item()
                else:
                    extent_areas[mag] = area
            asset_results["extent_areas"] = extent_areas

        # Create STAC item
        item = pystac.Item(
            id=f"{source}_{identifier}_ripple_{ripple_ver}_f2fim_{f2fim_ver}",
            geometry=asset_results["geometry"],
            bbox=asset_results["bbox"],
            datetime=datetime.now(timezone.utc),
            properties={
                "title": f"Ripple FIM {source.upper()} - {identifier}",
                "description": f"Flood inundation mapping for {identifier} using {source.upper()} data",
                "source": source,
                "magnitudes": asset_results["magnitudes"],
                "extent_areas (m^2)": extent_areas,
                "hucs": hucs_list,
                "flows2fim_version": f2fim_ver,
                "ripple_version": ripple_ver,
                "resolution (m)": 3,
            },
        )

        # Add projection extension
        ProjectionExtension.ext(item, add_if_missing=True)
        item.properties.update({"proj:wkt2": asset_results["wkt2_string"]})

        # Add thumbnail
        if "thumbnail" in asset_results and asset_results["thumbnail"]:
            thumbnail_href, is_valid = s3_utils.generate_href(
                bucket_name, asset_results["thumbnail"], link_type
            )
            if is_valid:
                item.add_asset(
                    "thumbnail",
                    pystac.Asset(
                        href=thumbnail_href,
                        media_type="image/png",
                        roles=["thumbnail"],
                        title="Extent thumbnail",
                    ),
                )
            else:
                print(
                    f"Skipping thumbnail extent asset for {identifier} - invalid or inaccessible"
                )

        # Add model domain boundary geopackage
        domain_href, is_valid = s3_utils.generate_href(
            bucket_name, f"{subdir}model_domain.gpkg", link_type
        )
        if is_valid:
            item.add_asset(
                "model_domain",
                pystac.Asset(
                    href=domain_href,
                    media_type="application/geopackage+sqlite3",
                    roles=["data"],
                    title="Model Domain Boundary",
                ),
            )
        else:
            print(
                f"Skipping model domain asset for {identifier} - invalid or inaccessible"
            )

        # Add assets for each magnitude
        for magnitude in asset_results["magnitudes"]:
            # Add extent raster
            if "mip" in source:
                extent_href, is_valid = s3_utils.generate_href(
                    bucket_name,
                    f"{subdir}{magnitude}_extent_f2f_ver_{f2fim_ver}.tif",
                    link_type,
                )
                if is_valid:
                    item.add_asset(
                        f"{magnitude}_extent",
                        pystac.Asset(
                            href=extent_href,
                            media_type="image/tiff; application=geotiff",
                            roles=["data"],
                            title=f"{magnitude} Flood Extent",
                        ),
                    )
                else:
                    print(
                        f"Skipping extent asset for magnitude {magnitude} for {identifier} - invalid or inaccessible"
                    )

            else:
                common_name = identifier.split("_")[1]
                extent_href, is_valid = s3_utils.generate_href(
                    bucket_name,
                    f"{subdir}{magnitude}_{common_name}_extent_f2f_ver_{f2fim_ver}.tif",
                    link_type,
                )
                if is_valid:
                    item.add_asset(
                        f"{magnitude}_extent",
                        pystac.Asset(
                            href=extent_href,
                            media_type="image/tiff; application=geotiff",
                            roles=["data"],
                            title=f"{magnitude} Flood Extent",
                        ),
                    )
                else:
                    print(
                        f"Skipping extent asset for magnitude {magnitude} for {identifier} - invalid or inaccessible"
                    )

        # validate item
        item.validate()

        collection.add_item(item)


def main():
    args = parse_arguments()
    s3_utils = initialize_s3_utils()

    # Initialize asset handler
    asset_handler = RippleFIMAssetHandler(
        s3_utils, args.bucket_name, args.derived_metadata_path
    )

    # Process collection-level flowfiles
    flowfile_info = asset_handler.process_collection_flowfiles(args.asset_object_key)

    # Create collection with flowfile information
    collection = create_ripple_collection(
        s3_utils, args.bucket_name, args.asset_object_key, args.link_type, flowfile_info
    )

    # Process BLE data
    ble_path = f"{args.asset_object_key}ble/"
    process_source_directory(
        ble_path,
        "ble",
        s3_utils,
        args.bucket_name,
        args.link_type,
        collection,
        args.reprocess_assets,
        asset_handler,
        args.f2fim_ver,
        args.ripple_ver,
    )

    # Process MIP data
    mip_path = f"{args.asset_object_key}mip/"
    process_source_directory(
        mip_path,
        "mip",
        s3_utils,
        args.bucket_name,
        args.link_type,
        collection,
        args.reprocess_assets,
        asset_handler,
        args.f2fim_ver,
        args.ripple_ver,
    )

    # Update and validate collection
    s3_utils.update_collection(
        collection, "ripple-fim-collection", args.catalog_path, args.bucket_name
    )
    collection.validate()

    # Upload modified parquet file
    asset_handler.upload_modified_parquet()


if __name__ == "__main__":
    main()
