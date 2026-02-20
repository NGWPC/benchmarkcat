"""
Shared batch utilities for both gfm and gfm_exp pipelines.

Handles manifest I/O, partial parquet merge/upload, and collection link
rebuilding from S3 item JSONs.
"""

import json
import logging
import os
import tempfile
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import pystac

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Manifest helpers
# ---------------------------------------------------------------------------

def read_manifest(s3_utils: Any, bucket_name: str, manifest_key: str) -> List[Dict]:
    """Download a JSONL manifest from S3 and return a list of scene dicts."""
    response = s3_utils.s3_client.get_object(Bucket=bucket_name, Key=manifest_key)
    raw = response["Body"].read().decode("utf-8")
    scenes = [json.loads(line) for line in raw.splitlines() if line.strip()]
    logger.info("Read %d scenes from manifest s3://%s/%s", len(scenes), bucket_name, manifest_key)
    return scenes


def write_manifest(
    s3_utils: Any,
    bucket_name: str,
    manifest_key: str,
    scenes: List[Dict],
    meta_extra: Optional[Dict[str, Any]] = None,
) -> None:
    """Write scenes as JSONL to S3 and upload a sidecar metadata JSON.

    Sidecar is written to ``<manifest_key>.meta.json``. Optional ``meta_extra``
    is merged into the sidecar (e.g. date filter args: after_date, before_date, dates).
    """
    body = "\n".join(json.dumps(s) for s in scenes)
    s3_utils.s3_client.put_object(
        Bucket=bucket_name,
        Key=manifest_key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )
    logger.info("Wrote manifest (%d scenes) to s3://%s/%s", len(scenes), bucket_name, manifest_key)

    meta = {
        "total_scenes": len(scenes),
        "manifest_s3_key": f"s3://{bucket_name}/{manifest_key}",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    if meta_extra:
        meta.update(meta_extra)
    meta_key = manifest_key + ".meta.json"
    s3_utils.s3_client.put_object(
        Bucket=bucket_name,
        Key=meta_key,
        Body=json.dumps(meta, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    logger.info("Wrote manifest metadata to s3://%s/%s", bucket_name, meta_key)


# ---------------------------------------------------------------------------
# Partial parquet helpers
# ---------------------------------------------------------------------------

def upload_partial_parquet(
    s3_utils: Any,
    bucket_name: str,
    partial_parquet_prefix: str,
    job_index: int,
    df: pd.DataFrame,
) -> str:
    """Write df to a temp file and upload as ``<prefix>/<job_index>.parquet``.

    Returns the S3 key of the uploaded partial parquet.
    """
    key = f"{partial_parquet_prefix.rstrip('/')}/{job_index}.parquet"
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        df.to_parquet(tmp_path, index=False)
        s3_utils.s3_client.upload_file(tmp_path, bucket_name, key)
        logger.info("Uploaded partial parquet (%d rows) to s3://%s/%s", len(df), bucket_name, key)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
    return key


def merge_partial_parquets(
    s3_utils: Any,
    bucket_name: str,
    partial_parquet_prefix: str,
    master_key: str,
) -> pd.DataFrame:
    """Download all partial parquets + existing master, merge, dedup, re-upload.

    Deduplication keeps the last occurrence of each ``sent_ti_path``.
    Returns the merged DataFrame.
    """
    prefix = partial_parquet_prefix.rstrip("/") + "/"
    paginator = s3_utils.s3_client.get_paginator("list_objects_v2")
    partial_keys = []
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                partial_keys.append(obj["Key"])

    if not partial_keys:
        logger.warning("No partial parquets found under s3://%s/%s", bucket_name, prefix)

    frames = []

    # Load existing master parquet if present
    with tempfile.TemporaryDirectory() as tmpdir:
        master_local = os.path.join(tmpdir, "master.parquet")
        try:
            s3_utils.s3_client.download_file(bucket_name, master_key, master_local)
            frames.append(pd.read_parquet(master_local))
            logger.info("Loaded master parquet (%d rows)", len(frames[-1]))
        except Exception:
            logger.info("No existing master parquet at %s — starting fresh", master_key)

        # Load each partial
        for key in partial_keys:
            local = os.path.join(tmpdir, os.path.basename(key))
            try:
                s3_utils.s3_client.download_file(bucket_name, key, local)
                frames.append(pd.read_parquet(local))
                logger.info("Loaded partial %s (%d rows)", key, len(frames[-1]))
            except Exception as e:
                logger.warning("Failed to load partial %s: %s", key, e)

        if not frames:
            return pd.DataFrame()

        merged = pd.concat(frames, ignore_index=True)
        # Dedup: keep last entry for each scene (partial takes precedence over master)
        merged = merged.drop_duplicates(subset=["sent_ti_path"], keep="last").reset_index(drop=True)
        logger.info("Merged parquet: %d rows", len(merged))

        # Upload merged as new master
        merged_local = os.path.join(tmpdir, "merged.parquet")
        merged.to_parquet(merged_local, index=False)
        s3_utils.s3_client.upload_file(merged_local, bucket_name, master_key)
        logger.info("Uploaded merged master parquet to s3://%s/%s", bucket_name, master_key)

    return merged


def delete_partial_parquets(
    s3_utils: Any,
    bucket_name: str,
    partial_parquet_prefix: str,
) -> None:
    """Delete all .parquet objects under the partial prefix."""
    prefix = partial_parquet_prefix.rstrip("/") + "/"
    paginator = s3_utils.s3_client.get_paginator("list_objects_v2")
    keys_to_delete = []
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys_to_delete.append({"Key": obj["Key"]})

    if not keys_to_delete:
        return

    # S3 DeleteObjects accepts up to 1000 keys per call
    for i in range(0, len(keys_to_delete), 1000):
        s3_utils.s3_client.delete_objects(
            Bucket=bucket_name,
            Delete={"Objects": keys_to_delete[i : i + 1000]},
        )
    logger.info("Deleted %d partial parquet(s) from s3://%s/%s", len(keys_to_delete), bucket_name, prefix)


# ---------------------------------------------------------------------------
# Collection rebuild helper
# ---------------------------------------------------------------------------

def rebuild_collection_links(
    s3_utils: Any,
    bucket_name: str,
    catalog_path: str,
    catalog_id: str,
    collection: pystac.Collection,
) -> None:
    """Paginate item JSONs on S3 and add a link for each to *collection*.

    Item JSONs are expected at ``<catalog_path>/<catalog_id>/<item_id>/<item_id>.json``.
    Skips ``collection.json`` and ``catalog.json``.
    """
    base = catalog_path.rstrip("/") + "/" + catalog_id.strip("/") + "/"
    paginator = s3_utils.s3_client.get_paginator("list_objects_v2")
    count = 0
    for page in paginator.paginate(Bucket=bucket_name, Prefix=base):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            filename = key.split("/")[-1]
            if not filename.endswith(".json"):
                continue
            if filename in ("collection.json", "catalog.json"):
                continue
            # Relative link from collection.json to item JSON
            rel_path = key[len(base):]  # e.g. "item_id/item_id.json"
            collection.add_link(
                pystac.Link(
                    rel=pystac.RelType.ITEM,
                    target=f"./{rel_path}",
                    media_type="application/geo+json",
                )
            )
            count += 1

    logger.info("Added %d item links to collection from s3://%s/%s", count, bucket_name, base)


# ---------------------------------------------------------------------------
# Unified batch_merge entry point
# ---------------------------------------------------------------------------

def run_batch_merge(
    catalog_id: str,
    collection_creator: Any,
    description: str,
    default_asset_object_key: str,
    default_derived_metadata_path: str,
) -> None:
    """Shared batch_merge logic for gfm and gfm_exp pipelines.

    Parses CLI arguments, merges partial parquets, rebuilds collection.json,
    and optionally deletes the partials.

    Args:
        catalog_id: STAC collection id (e.g. "gfm-collection").
        collection_creator: Callable(link_type, bucket_name, asset_object_key, s3_utils) -> pystac.Collection.
        description: Short description for argparse help text.
        default_asset_object_key: Default --asset_object_key value.
        default_derived_metadata_path: Default --derived_metadata_path value.
    """
    import argparse
    import os

    import boto3

    parser = argparse.ArgumentParser(description=f"Merge {description} batch-worker outputs.")
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
        default=default_derived_metadata_path,
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
        default=default_asset_object_key,
        help=f"S3 prefix for {description} data (used when creating collection if missing).",
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
    args = parser.parse_args()

    if args.profile is not None:
        os.environ["AWS_PROFILE"] = args.profile
    else:
        os.environ.pop("AWS_PROFILE", None)

    if args.profile is not None:
        session = boto3.Session(profile_name=args.profile)
        s3 = session.client("s3")
    else:
        s3 = boto3.client("s3")

    from ingest.utils import S3Utils

    s3_utils = S3Utils(s3)

    # 1. Merge partial parquets into master
    logger.info("Merging partial parquets...")
    merge_partial_parquets(
        s3_utils,
        args.bucket_name,
        args.partial_parquet_prefix,
        args.derived_metadata_path,
    )

    # 2. Rebuild collection.json (or create if missing)
    logger.info("Rebuilding collection links...")
    collection_key = (
        args.catalog_path.rstrip("/") + "/" + catalog_id + "/collection.json"
    )
    try:
        response = s3_utils.s3_client.get_object(Bucket=args.bucket_name, Key=collection_key)
        collection_dict = json.loads(response["Body"].read().decode("utf-8"))
        collection = pystac.Collection.from_dict(collection_dict)
    except Exception:
        logger.warning("Could not load existing collection.json — creating from scratch")
        collection = collection_creator(
            args.link_type, args.bucket_name, args.asset_object_key, s3_utils
        )

    # Remove existing item links so we can re-add from S3 listing
    collection.links = [lk for lk in collection.links if lk.rel != pystac.RelType.ITEM]
    rebuild_collection_links(
        s3_utils,
        args.bucket_name,
        args.catalog_path,
        catalog_id,
        collection,
    )
    s3_utils.update_collection_or_bootstrap(
        collection, catalog_id, args.catalog_path, args.bucket_name
    )
    logger.info("Collection.json updated.")

    # 3. Delete partial parquets
    if not args.skip_delete_partials:
        logger.info("Deleting partial parquets...")
        delete_partial_parquets(s3_utils, args.bucket_name, args.partial_parquet_prefix)

    logger.info("%s batch merge complete.", description)


# ---------------------------------------------------------------------------
# Unified batch_split entry point
# ---------------------------------------------------------------------------

def run_batch_split(
    description: str,
    default_asset_object_key: str,
    discover_scenes: Any,
) -> None:
    """Shared batch_split logic for gfm and gfm_exp pipelines.

    Parses CLI arguments, calls the pipeline-specific ``discover_scenes``
    callback to build the scene list, and writes the manifest to S3.

    Args:
        description: Short description for argparse help text (e.g. "gfm", "gfm_exp").
        default_asset_object_key: Default --asset_object_key value.
        discover_scenes: Callable(s3_utils, bucket_name, prefix, after_date, before_date, dates)
            -> List[Dict].  Returns the scene dicts to write to the manifest.
    """
    import argparse
    import os

    import boto3

    parser = argparse.ArgumentParser(description=f"Build {description} scene manifest for batch processing.")
    parser.add_argument("--bucket_name", type=str, default="fimc-data")
    parser.add_argument("--asset_object_key", type=str, default=default_asset_object_key)
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
        help="Only include scenes/dates >= YYYY-MM-DD.",
    )
    parser.add_argument(
        "--before-date",
        type=str,
        default=None,
        help="Only include scenes/dates <= YYYY-MM-DD.",
    )
    parser.add_argument(
        "--dates",
        type=str,
        default=None,
        help="Comma-separated list of dates (YYYY-MM-DD).",
    )
    parser.add_argument("--profile", type=str, default=None, help="AWS profile name.")
    args = parser.parse_args()

    if args.profile is not None:
        os.environ["AWS_PROFILE"] = args.profile
    else:
        os.environ.pop("AWS_PROFILE", None)

    if args.profile is not None:
        session = boto3.Session(profile_name=args.profile)
        s3 = session.client("s3")
    else:
        s3 = boto3.client("s3")

    from ingest.utils import S3Utils

    s3_utils = S3Utils(s3)

    scenes = discover_scenes(
        s3_utils,
        args.bucket_name,
        args.asset_object_key,
        args.after_date,
        args.before_date,
        args.dates,
    )

    logger.info("Total scenes: %d", len(scenes))

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
    logger.info("Manifest written to s3://%s/%s", args.bucket_name, args.manifest_s3_key)
