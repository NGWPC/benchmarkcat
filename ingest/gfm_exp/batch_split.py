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

import logging

from ingest.batch_utils import run_batch_split

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def discover_gfm_exp_scenes(s3_utils, bucket_name, asset_object_key, after_date, before_date, dates):
    """Discover GFM-exp scenes: date directories -> sent_ti directories, filtered by date folder name."""
    prefix = asset_object_key.rstrip("/") + "/"
    paginator = s3_utils.s3_client.get_paginator("list_objects_v2")
    date_paths = []
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            date_paths.append(cp["Prefix"])

    # Filter by date folder name (string comparison on YYYY-MM-DD directory names)
    if after_date:
        date_paths = [dp for dp in date_paths if dp.rstrip("/").split("/")[-1] >= after_date]
    if before_date:
        date_paths = [dp for dp in date_paths if dp.rstrip("/").split("/")[-1] <= before_date]
    if dates:
        allowed = set(d.strip() for d in dates.split(","))
        date_paths = [dp for dp in date_paths if dp.rstrip("/").split("/")[-1] in allowed]

    logger.info("Found %d date directories", len(date_paths))

    scenes = []
    for date_path in sorted(date_paths):
        date_id = date_path.rstrip("/").split("/")[-1]
        sent_ti_paths = s3_utils.list_subdirectories(bucket_name, date_path)
        for sent_ti_path in sent_ti_paths:
            scenes.append({"date_path": date_path, "date_id": date_id, "sent_ti_path": sent_ti_path})

    return scenes


def main():
    run_batch_split(
        description="gfm_exp",
        default_asset_object_key=None,
        discover_scenes=discover_gfm_exp_scenes,
    )


if __name__ == "__main__":
    main()
