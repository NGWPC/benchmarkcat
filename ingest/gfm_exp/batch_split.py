"""
gfm_exp batch_split — discover all scenes under the PI4 prefix and write a JSONL manifest.

Each manifest line is a JSON object:
    {"date_path": "benchmark/rs/PI4/2024-01-01/", "date_id": "2024-01-01", "sent_ti_path": "benchmark/rs/PI4/2024-01-01/S1A_IW_..."}

Run this *once* before submitting the AWS Batch array job.

Usage:
    python -m ingest.gfm_exp.batch_split \
        --bucket_name fimc-data \
        --asset_object_key benchmark/rs/PI4/ \
        --manifest-s3-key benchmark/stac-bench-cat/batch/gfm_exp_manifest.jsonl \
        [--after-date 2024-01-01] \
        [--before-date 2024-12-31] \
        [--dates 2024-01-01,2024-01-15] \
        [--profile my-profile]
"""

import argparse
import logging
import os

import boto3

from ingest.batch_utils import write_manifest
from ingest.utils import S3Utils

logging.basicConfig(level=logging.INFO)


def parse_arguments():
    parser = argparse.ArgumentParser(description="Build gfm_exp scene manifest for batch processing.")
    parser.add_argument("--bucket_name", type=str, default="fimc-data")
    parser.add_argument("--asset_object_key", type=str, default="benchmark/rs/PI4/")
    parser.add_argument(
        "--manifest-s3-key",
        type=str,
        required=True,
        help="S3 key where the output manifest JSONL will be written.",
    )
    parser.add_argument("--after-date", type=str, default=None, help="Only include dates >= YYYY-MM-DD.")
    parser.add_argument(
        "--dates",
        type=str,
        default=None,
        help="Comma-separated list of specific dates to include (YYYY-MM-DD).",
    )
    parser.add_argument("--before-date", type=str, default=None, help="Only include dates <= YYYY-MM-DD.")
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

    # Discover all date directories
    prefix = args.asset_object_key.rstrip("/") + "/"
    paginator = s3_utils.s3_client.get_paginator("list_objects_v2")
    date_paths = []
    for page in paginator.paginate(Bucket=args.bucket_name, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            date_paths.append(cp["Prefix"])

    # Filter by --after-date, --before-date, --dates (order: after, before, then dates list)
    if args.after_date:
        date_paths = [dp for dp in date_paths if dp.rstrip("/").split("/")[-1] >= args.after_date]
    if args.before_date:
        date_paths = [dp for dp in date_paths if dp.rstrip("/").split("/")[-1] <= args.before_date]
    if args.dates:
        allowed = set(d.strip() for d in args.dates.split(","))
        date_paths = [dp for dp in date_paths if dp.rstrip("/").split("/")[-1] in allowed]

    logging.info("Found %d date directories", len(date_paths))

    # Build flat scene list
    scenes = []
    for date_path in sorted(date_paths):
        date_id = date_path.rstrip("/").split("/")[-1]
        sent_ti_paths = s3_utils.list_subdirectories(args.bucket_name, date_path)
        for sent_ti_path in sent_ti_paths:
            scenes.append({"date_path": date_path, "date_id": date_id, "sent_ti_path": sent_ti_path})

    logging.info("Total scenes: %d", len(scenes))

    write_manifest(s3_utils, args.bucket_name, args.manifest_s3_key, scenes)
    logging.info("Manifest written to s3://%s/%s", args.bucket_name, args.manifest_s3_key)


if __name__ == "__main__":
    main()
