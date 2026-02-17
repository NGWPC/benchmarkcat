"""
gfm batch_split — discover all scenes under the GFM DFO-event prefix and write a JSONL manifest.

Each manifest line is a JSON object:
    {"dfo_path": "benchmark/rs/gfm/4688/", "event_id": "4688", "sent_ti_path": "benchmark/rs/gfm/4688/S1A_IW_..."}

Run this *once* before submitting the AWS Batch array job.

Usage:
    python -m ingest.gfm.batch_split \
        --bucket_name fimc-data \
        --asset_object_key benchmark/rs/gfm/ \
        --manifest-s3-key benchmark/stac-bench-cat/batch/gfm_manifest.jsonl \
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
    parser = argparse.ArgumentParser(description="Build gfm scene manifest for batch processing.")
    parser.add_argument("--bucket_name", type=str, default="fimc-data")
    parser.add_argument("--asset_object_key", type=str, default="benchmark/rs/gfm/")
    parser.add_argument(
        "--manifest-s3-key",
        type=str,
        required=True,
        help="S3 key where the output manifest JSONL will be written.",
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

    # Discover all DFO event directories
    prefix = args.asset_object_key.rstrip("/") + "/"
    paginator = s3_utils.s3_client.get_paginator("list_objects_v2")
    dfo_paths = []
    for page in paginator.paginate(Bucket=args.bucket_name, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            dfo_paths.append(cp["Prefix"])

    logging.info("Found %d DFO event directories", len(dfo_paths))

    # Build flat scene list
    scenes = []
    for dfo_path in sorted(dfo_paths):
        event_id = dfo_path.rstrip("/").split("/")[-1]
        sent_ti_paths = s3_utils.list_subdirectories(args.bucket_name, dfo_path)
        for sent_ti_path in sent_ti_paths:
            scenes.append({"dfo_path": dfo_path, "event_id": event_id, "sent_ti_path": sent_ti_path})

    logging.info("Total scenes: %d", len(scenes))

    write_manifest(s3_utils, args.bucket_name, args.manifest_s3_key, scenes)
    logging.info("Manifest written to s3://%s/%s", args.bucket_name, args.manifest_s3_key)


if __name__ == "__main__":
    main()
