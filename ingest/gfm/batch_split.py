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
        [--after-date YYYY-MM-DD] [--before-date YYYY-MM-DD] [--dates date1,date2] \
        [--profile my-profile]
"""

import argparse
import logging
import os

import boto3

from ingest.batch_utils import write_manifest
from ingest.gfm.gfm_stac import SentinelName
from ingest.utils import S3Utils

logging.basicConfig(level=logging.INFO)


def _scene_date_from_sent_ti_path(sent_ti_path: str):
    """Extract scene acquisition date (YYYY-MM-DD) from sent_ti_path product name. Returns None if unparseable."""
    product_name = sent_ti_path.strip("/").split("/")[-1]
    try:
        start_datetime, _ = SentinelName.extract_datetimes(product_name)
        return start_datetime.date().strftime("%Y-%m-%d")
    except ValueError:
        return None


def _filter_scenes_by_date_scope(scenes, after_date=None, before_date=None, dates_list=None):
    """Filter scenes by scene acquisition date. Apply after_date, then before_date, then dates_list."""
    if after_date is None and before_date is None and dates_list is None:
        return scenes
    filtered = []
    for s in scenes:
        scene_date = _scene_date_from_sent_ti_path(s["sent_ti_path"])
        if scene_date is None:
            continue
        if after_date is not None and scene_date < after_date:
            continue
        if before_date is not None and scene_date > before_date:
            continue
        if dates_list is not None:
            allowed = set(x.strip() for x in dates_list.split(",") if x.strip())
            if scene_date not in allowed:
                continue
        filtered.append(s)
    return filtered


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
    parser.add_argument(
        "--after-date",
        type=str,
        default=None,
        help="Only include scenes with acquisition date >= YYYY-MM-DD.",
    )
    parser.add_argument(
        "--before-date",
        type=str,
        default=None,
        help="Only include scenes with acquisition date <= YYYY-MM-DD.",
    )
    parser.add_argument(
        "--dates",
        type=str,
        default=None,
        help="Comma-separated list of acquisition dates (YYYY-MM-DD).",
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

    scenes = _filter_scenes_by_date_scope(
        scenes,
        after_date=args.after_date,
        before_date=args.before_date,
        dates_list=args.dates,
    )

    logging.info("Total scenes: %d", len(scenes))

    meta_extra = {}
    if args.after_date is not None:
        meta_extra["after_date"] = args.after_date
    if args.before_date is not None:
        meta_extra["before_date"] = args.before_date
    if args.dates is not None:
        meta_extra["dates"] = args.dates
    write_manifest(
        s3_utils,
        args.bucket_name,
        args.manifest_s3_key,
        scenes,
        meta_extra=meta_extra if meta_extra else None,
    )
    logging.info("Manifest written to s3://%s/%s", args.bucket_name, args.manifest_s3_key)


if __name__ == "__main__":
    main()
