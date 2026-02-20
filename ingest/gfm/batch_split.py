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

import logging

from ingest.batch_utils import run_batch_split
from ingest.gfm.gfm_stac import SentinelName

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _scene_date_from_sent_ti_path(sent_ti_path):
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


def discover_gfm_scenes(s3_utils, bucket_name, asset_object_key, after_date, before_date, dates):
    """Discover GFM scenes: DFO events -> sent_ti directories, filtered by acquisition date."""
    prefix = asset_object_key.rstrip("/") + "/"
    paginator = s3_utils.s3_client.get_paginator("list_objects_v2")
    dfo_paths = []
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            dfo_paths.append(cp["Prefix"])

    logger.info("Found %d DFO event directories", len(dfo_paths))

    scenes = []
    for dfo_path in sorted(dfo_paths):
        event_id = dfo_path.rstrip("/").split("/")[-1]
        sent_ti_paths = s3_utils.list_subdirectories(bucket_name, dfo_path)
        for sent_ti_path in sent_ti_paths:
            scenes.append({"dfo_path": dfo_path, "event_id": event_id, "sent_ti_path": sent_ti_path})

    scenes = _filter_scenes_by_date_scope(
        scenes,
        after_date=after_date,
        before_date=before_date,
        dates_list=dates,
    )
    return scenes


def main():
    run_batch_split(
        description="gfm",
        default_asset_object_key="benchmark/rs/gfm/",
        discover_scenes=discover_gfm_scenes,
    )


if __name__ == "__main__":
    main()
