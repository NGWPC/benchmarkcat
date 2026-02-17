"""
gfm_exp batch_merge — merge partial parquets into master, rebuild collection.json, clean up partials.

Run this *once* after all batch-worker array jobs complete.

Usage:
    python -m ingest.gfm_exp.batch_merge \
        --bucket_name fimc-data \
        --partial-parquet-prefix benchmark/stac-bench-cat/batch/gfm_exp_partials \
        --derived_metadata_path benchmark/stac-bench-cat/assets/derived-asset-data/gfm_expanded_collection.parquet \
        --catalog_path benchmark/stac-bench-cat/ \
        [--profile my-profile]
        [--skip-delete-partials]
"""

import argparse
import logging
import os

import boto3
import pystac

from ingest.batch_utils import (
    delete_partial_parquets,
    merge_partial_parquets,
    rebuild_collection_links,
)
from ingest.utils import S3Utils

logging.basicConfig(level=logging.INFO)

CATALOG_ID = "gfm-expanded-collection"


def parse_arguments():
    parser = argparse.ArgumentParser(description="Merge gfm_exp batch-worker outputs.")
    parser.add_argument("--bucket_name", type=str, default="fimc-data")
    parser.add_argument(
        "--partial-parquet-prefix",
        type=str,
        required=True,
        help="S3 prefix where per-job partial parquets were written.",
    )
    parser.add_argument(
        "--derived_metadata_path",
        type=str,
        default="benchmark/stac-bench-cat/assets/derived-asset-data/gfm_expanded_collection.parquet",
        help="S3 key of the master parquet file.",
    )
    parser.add_argument(
        "--catalog_path",
        type=str,
        default="benchmark/stac-bench-cat/",
        help="S3 prefix of the STAC catalog.",
    )
    parser.add_argument(
        "--asset_object_key",
        type=str,
        default="benchmark/rs/PI4/",
        help="S3 prefix for PI4 data (used when creating collection if missing).",
    )
    parser.add_argument(
        "--link_type",
        type=str,
        default="uri",
        help='Link type for collection href generation ("uri" or "url").',
    )
    parser.add_argument(
        "--skip-delete-partials",
        action="store_true",
        help="Do not delete partial parquets after merging (useful for debugging).",
    )
    parser.add_argument("--profile", type=str, default=None, help="AWS profile name.")
    return parser.parse_args()


def main():
    args = parse_arguments()

    if args.profile is not None:
        os.environ["AWS_PROFILE"] = args.profile
    else:
        os.environ.pop("AWS_PROFILE", None)

    if args.profile is not None:
        session = boto3.Session(profile_name=args.profile)
        s3 = session.client("s3")
    else:
        s3 = boto3.client("s3")
    s3_utils = S3Utils(s3)

    # 1. Merge partial parquets into master
    logging.info("Merging partial parquets...")
    merge_partial_parquets(
        s3_utils,
        args.bucket_name,
        args.partial_parquet_prefix,
        args.derived_metadata_path,
    )

    # 2. Rebuild collection.json by paginating item JSONs on S3 (or create if missing)
    logging.info("Rebuilding collection links...")
    collection_key = (
        args.catalog_path.rstrip("/") + "/" + CATALOG_ID + "/collection.json"
    )
    try:
        response = s3_utils.s3_client.get_object(Bucket=args.bucket_name, Key=collection_key)
        collection_dict = __import__("json").loads(response["Body"].read().decode("utf-8"))
        collection = pystac.Collection.from_dict(collection_dict)
    except Exception:
        logging.warning("Could not load existing collection.json — creating from scratch")
        from ingest.gfm_exp.gfm_exp_col import create_gfm_exp_collection

        collection = create_gfm_exp_collection(
            args.link_type, args.bucket_name, args.asset_object_key, s3_utils
        )

    # Remove existing item links so we can re-add from S3 listing
    collection.links = [lk for lk in collection.links if lk.rel != pystac.RelType.ITEM]
    rebuild_collection_links(
        s3_utils,
        args.bucket_name,
        args.catalog_path,
        CATALOG_ID,
        collection,
    )
    s3_utils.update_collection_or_bootstrap(
        collection, CATALOG_ID, args.catalog_path, args.bucket_name
    )
    logging.info("Collection.json updated.")

    # 3. Delete partial parquets
    if not args.skip_delete_partials:
        logging.info("Deleting partial parquets...")
        delete_partial_parquets(s3_utils, args.bucket_name, args.partial_parquet_prefix)

    logging.info("gfm_exp batch merge complete.")


if __name__ == "__main__":
    main()
