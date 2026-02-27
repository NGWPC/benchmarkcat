#!/usr/bin/env python3
"""
Benchmarkcat — AWS Batch Pipeline Orchestrator

3-phase fan-out/fan-in pipeline:
  Phase 1: Split  — discover scenes, write manifest to S3
  Phase 2: Workers — array job, each child processes a slice of scenes
  Phase 3: Merge  — concatenate parquets, rebuild collection.json

Usage:
  python scripts/submit_pipeline.py --pipeline gfm
  python scripts/submit_pipeline.py --pipeline gfm_exp
  python scripts/submit_pipeline.py --pipeline gfm --after-date 2024-01-01 --before-date 2024-03-31
  python scripts/submit_pipeline.py --pipeline gfm --scenes-per-job 5 --dry-run
"""

import argparse
import json
import math
import subprocess
import sys
import time
from datetime import datetime, timezone

import boto3


# ---------------------------------------------------------------------------
# Defaults (used when terraform outputs are unavailable)
# ---------------------------------------------------------------------------
DEFAULTS = {
    "aws_region": "us-east-1",
    "aws_profile": "test-se",
    "s3_bucket": "fimc-data",
    "scenes_per_job": 50,
    "catalog_path": "benchmark/stac-bench-cat/",
    "hucs_object_key": "benchmark/stac-bench-cat/assets/WBDHU8_webproj.gpkg",
    "boundaries_object_key": "benchmark/stac-bench-cat/assets/Mexico_Canada_boundaries.gpkg",
    "job_queue_name": "benchmarkcat-queue",
    "gfm": {
        "split_job_def": "benchmarkcat-gfm-split",
        "worker_job_def": "benchmarkcat-gfm-worker",
        "merge_job_def": "benchmarkcat-gfm-merge",
        "asset_object_key": "benchmark/rs/gfm/",
        "manifest_s3_key": "benchmark/stac-bench-cat/batch/gfm_manifest.jsonl",
        "partial_parquet_prefix": "benchmark/stac-bench-cat/batch/gfm_partials",
        "derived_metadata_path": "benchmark/stac-bench-cat/assets/derived-asset-data/gfm_collection.parquet",
    },
    "gfm_exp": {
        "split_job_def": "benchmarkcat-gfm-exp-split",
        "worker_job_def": "benchmarkcat-gfm-exp-worker",
        "merge_job_def": "benchmarkcat-gfm-exp-merge",
        "asset_object_key": "benchmark/rs/PI4/",
        "manifest_s3_key": "benchmark/stac-bench-cat/batch/gfm_exp_manifest.jsonl",
        "partial_parquet_prefix": "benchmark/stac-bench-cat/batch/gfm_exp_partials",
        "derived_metadata_path": "benchmark/stac-bench-cat/assets/derived-asset-data/gfm_expanded_collection.parquet",
    },
}

MAX_ARRAY_SIZE = 10000
MIN_ARRAY_SIZE = 2


# ---------------------------------------------------------------------------
# Terraform output helpers
# ---------------------------------------------------------------------------

def get_terraform_outputs():
    """Read terraform outputs. Returns empty dict if terraform is unavailable."""
    try:
        result = subprocess.run(
            ["terraform", "output", "-json"],
            cwd="terraform",
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode == 0:
            raw = json.loads(result.stdout)
            # {"key": {"value": ..., "type": ...}}
            return {k: v["value"] for k, v in raw.items()}
    except (subprocess.TimeoutExpired, FileNotFoundError, json.JSONDecodeError, KeyError):
        pass
    return {}


def build_config(args, tf_outputs):
    """Merge terraform outputs, defaults, and CLI overrides into a config dict."""
    cfg = {}

    cfg["aws_region"] = args.region or tf_outputs.get("aws_region") or DEFAULTS["aws_region"]
    cfg["aws_profile"] = args.profile or DEFAULTS["aws_profile"]
    cfg["s3_profile"] = args.s3_profile or cfg["aws_profile"]
    cfg["s3_bucket"] = args.bucket_name or tf_outputs.get("s3_bucket") or DEFAULTS["s3_bucket"]
    cfg["scenes_per_job"] = args.scenes_per_job or tf_outputs.get("scenes_per_job") or DEFAULTS["scenes_per_job"]
    cfg["catalog_path"] = tf_outputs.get("catalog_path") or DEFAULTS["catalog_path"]
    cfg["hucs_object_key"] = tf_outputs.get("hucs_object_key") or DEFAULTS["hucs_object_key"]
    cfg["boundaries_object_key"] = tf_outputs.get("boundaries_object_key") or DEFAULTS["boundaries_object_key"]
    cfg["job_queue_name"] = tf_outputs.get("job_queue_name") or DEFAULTS["job_queue_name"]

    pipeline = args.pipeline
    pipeline_defaults = DEFAULTS[pipeline]

    # Pipeline-specific paths from terraform or defaults
    tf_pipeline = tf_outputs.get(f"{pipeline}_config") or {}
    job_def_names = tf_outputs.get("job_definition_names") or {}

    cfg["split_job_def"] = job_def_names.get(f"{pipeline.replace('_', '-')}-split") or pipeline_defaults["split_job_def"]
    cfg["worker_job_def"] = job_def_names.get(f"{pipeline.replace('_', '-')}-worker") or pipeline_defaults["worker_job_def"]
    cfg["merge_job_def"] = job_def_names.get(f"{pipeline.replace('_', '-')}-merge") or pipeline_defaults["merge_job_def"]

    cfg["asset_object_key"] = tf_pipeline.get("asset_object_key") or pipeline_defaults["asset_object_key"]
    cfg["manifest_s3_key"] = tf_pipeline.get("manifest_s3_key") or pipeline_defaults["manifest_s3_key"]
    cfg["partial_parquet_prefix"] = tf_pipeline.get("partial_parquet_prefix") or pipeline_defaults["partial_parquet_prefix"]
    cfg["derived_metadata_path"] = tf_pipeline.get("derived_metadata_path") or pipeline_defaults["derived_metadata_path"]

    return cfg


# ---------------------------------------------------------------------------
# AWS Batch helpers
# ---------------------------------------------------------------------------

def get_batch_client(profile, region):
    if profile:
        session = boto3.Session(profile_name=profile, region_name=region)
    else:
        session = boto3.Session(region_name=region)
    return session.client("batch")


def get_s3_client(profile, region):
    if profile:
        session = boto3.Session(profile_name=profile, region_name=region)
    else:
        session = boto3.Session(region_name=region)
    return session.client("s3")


def submit_job(batch_client, job_name, job_definition, job_queue, parameters,
               array_size=None, container_overrides=None, dry_run=False):
    """Submit a Batch job. Returns job_id (or None for dry-run)."""
    kwargs = {
        "jobName": job_name,
        "jobDefinition": job_definition,
        "jobQueue": job_queue,
        "parameters": parameters,
    }
    if array_size and array_size >= MIN_ARRAY_SIZE:
        kwargs["arrayProperties"] = {"size": array_size}
    if container_overrides:
        kwargs["containerOverrides"] = container_overrides

    if dry_run:
        print(f"[DRY RUN] Would submit: {json.dumps(kwargs, indent=2)}")
        return None

    response = batch_client.submit_job(**kwargs)
    return response["jobId"]


def poll_until_complete(batch_client, job_id, job_name, poll_interval=30):
    """Poll until job reaches SUCCEEDED or FAILED. Exits process on failure."""
    terminal_states = {"SUCCEEDED", "FAILED"}
    while True:
        response = batch_client.describe_jobs(jobs=[job_id])
        if not response["jobs"]:
            print(f"  WARNING: job {job_id} not found, retrying...")
            time.sleep(poll_interval)
            continue

        job = response["jobs"][0]
        status = job["status"]
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")

        if "arrayProperties" in job:
            summary = job["arrayProperties"].get("statusSummary", {})
            summary_str = "  ".join(f"{k}={v}" for k, v in sorted(summary.items()) if v > 0)
            print(f"  [{ts}] {job_name} | {status} | {summary_str}")
        else:
            print(f"  [{ts}] {job_name} | {status}")

        if status in terminal_states:
            if status == "FAILED":
                reason = job.get("statusReason", "unknown")
                print(f"\nERROR: Job '{job_name}' ({job_id}) FAILED: {reason}")
                sys.exit(1)
            print(f"\nJob '{job_name}' SUCCEEDED.")
            return

        time.sleep(poll_interval)


def read_manifest_total(s3_client, bucket, manifest_s3_key):
    """Read the manifest metadata sidecar to get total_scenes."""
    meta_key = manifest_s3_key + ".meta.json"
    try:
        response = s3_client.get_object(Bucket=bucket, Key=meta_key)
        meta = json.loads(response["Body"].read().decode("utf-8"))
        return meta["total_scenes"]
    except Exception as e:
        raise RuntimeError(
            f"Could not read manifest metadata from s3://{bucket}/{meta_key}: {e}\n"
            "Did the split job complete successfully?"
        )


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def run_pipeline(args):
    breakpoint()
    tf_outputs = get_terraform_outputs()
    cfg = build_config(args, tf_outputs)

    print(f"\n{'='*60}")
    print(f"  Benchmarkcat Batch Pipeline — {args.pipeline.upper()}")
    print(f"{'='*60}")
    print(f"  Queue:          {cfg['job_queue_name']}")
    print(f"  Bucket:         {cfg['s3_bucket']}")
    print(f"  Scenes/job:     {cfg['scenes_per_job']}")
    print(f"  Manifest:       {cfg['manifest_s3_key']}")
    if args.after_date:
        print(f"  After date:     {args.after_date}")
    if args.before_date:
        print(f"  Before date:    {args.before_date}")
    if args.dates:
        print(f"  Dates:          {args.dates}")
    if args.dry_run:
        print(f"  *** DRY RUN — no jobs will be submitted ***")
    print()

    batch_client = get_batch_client(cfg["aws_profile"], cfg["aws_region"])
    s3_client = get_s3_client(cfg["s3_profile"], cfg["aws_region"])
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    return
    break
    # -----------------------------------------------------------------------
    # Phase 1: Split
    # -----------------------------------------------------------------------
    print(f"[Phase 1/3] Submitting SPLIT job...")
    split_params = {
        "bucket_name": cfg["s3_bucket"],
        "asset_object_key": cfg["asset_object_key"],
        "manifest_s3_key": cfg["manifest_s3_key"],
    }

    # Date filters: append via containerOverrides.command when present
    split_container_overrides = None
    if args.after_date or args.before_date or args.dates:
        pipeline_module = (
            "ingest.gfm.batch_split" if args.pipeline == "gfm"
            else "ingest.gfm_exp.batch_split"
        )
        split_cmd = [
            pipeline_module,
            "--bucket_name", cfg["s3_bucket"],
            "--asset_object_key", cfg["asset_object_key"],
            "--manifest-s3-key", cfg["manifest_s3_key"],
        ]
        if args.after_date:
            split_cmd += ["--after-date", args.after_date]
        if args.before_date:
            split_cmd += ["--before-date", args.before_date]
        if args.dates:
            split_cmd += ["--dates", args.dates]
        split_container_overrides = {"command": split_cmd}
        split_params = {}  # parameters unused when command is fully overridden

    split_job_name = f"{args.pipeline}-split-{timestamp}"
    split_job_id = submit_job(
        batch_client,
        job_name=split_job_name,
        job_definition=cfg["split_job_def"],
        job_queue=cfg["job_queue_name"],
        parameters=split_params,
        container_overrides=split_container_overrides,
        dry_run=args.dry_run,
    )

    if not args.dry_run:
        print(f"  Split job submitted: {split_job_id}")
        poll_until_complete(batch_client, split_job_id, split_job_name, args.poll_interval)

    # -----------------------------------------------------------------------
    # Phase 2: Workers (array job, dynamic sizing)
    # -----------------------------------------------------------------------
    print(f"\n[Phase 2/3] Computing array size from manifest metadata...")

    if args.dry_run:
        total_scenes = 100  # dummy for dry-run display
        print(f"  [DRY RUN] Assuming {total_scenes} scenes for array size calculation")
    else:
        total_scenes = read_manifest_total(s3_client, cfg["s3_bucket"], cfg["manifest_s3_key"])
        print(f"  Total scenes in manifest: {total_scenes}")

    scenes_per_job = int(cfg["scenes_per_job"])
    if total_scenes <= scenes_per_job:
        array_size = None  # submit as single (non-array) job
        print(f"  {total_scenes} scenes <= {scenes_per_job}/job → submitting as single job")
    else:
        array_size = math.ceil(total_scenes / scenes_per_job)
        array_size = min(array_size, MAX_ARRAY_SIZE)
        actual_chunk = math.ceil(total_scenes / array_size)
        print(f"  Array size: {array_size} children × ~{actual_chunk} scenes each")

    worker_params = {
        "bucket_name": cfg["s3_bucket"],
        "catalog_path": cfg["catalog_path"],
        "asset_object_key": cfg["asset_object_key"],
        "hucs_object_key": cfg["hucs_object_key"],
        "boundaries_object_key": cfg["boundaries_object_key"],
        "derived_metadata_path": cfg["derived_metadata_path"],
        "manifest_s3_key": cfg["manifest_s3_key"],
        "partial_parquet_prefix": cfg["partial_parquet_prefix"],
        "scenes_per_job": str(scenes_per_job),
    }

    print(f"\n[Phase 2/3] Submitting WORKER job...")
    worker_job_name = f"{args.pipeline}-worker-{timestamp}"
    worker_job_id = submit_job(
        batch_client,
        job_name=worker_job_name,
        job_definition=cfg["worker_job_def"],
        job_queue=cfg["job_queue_name"],
        parameters=worker_params,
        array_size=array_size,
        dry_run=args.dry_run,
    )

    if not args.dry_run:
        array_str = f" (array size={array_size})" if array_size else " (single job)"
        print(f"  Worker job submitted: {worker_job_id}{array_str}")
        poll_until_complete(batch_client, worker_job_id, worker_job_name, args.poll_interval)

    # -----------------------------------------------------------------------
    # Phase 3: Merge
    # -----------------------------------------------------------------------
    print(f"\n[Phase 3/3] Submitting MERGE job...")
    merge_params = {
        "bucket_name": cfg["s3_bucket"],
        "partial_parquet_prefix": cfg["partial_parquet_prefix"],
        "derived_metadata_path": cfg["derived_metadata_path"],
        "catalog_path": cfg["catalog_path"],
        "asset_object_key": cfg["asset_object_key"],
    }

    merge_job_name = f"{args.pipeline}-merge-{timestamp}"
    merge_job_id = submit_job(
        batch_client,
        job_name=merge_job_name,
        job_definition=cfg["merge_job_def"],
        job_queue=cfg["job_queue_name"],
        parameters=merge_params,
        dry_run=args.dry_run,
    )

    if not args.dry_run:
        print(f"  Merge job submitted: {merge_job_id}")
        poll_until_complete(batch_client, merge_job_id, merge_job_name, args.poll_interval)

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------
    print(f"\n{'='*60}")
    if args.dry_run:
        print(f"  DRY RUN complete — no jobs were submitted")
    else:
        print(f"  Pipeline '{args.pipeline}' COMPLETE")
        print(f"  Split:   {split_job_id}")
        print(f"  Workers: {worker_job_id}")
        print(f"  Merge:   {merge_job_id}")
        console_url = (
            f"https://console.aws.amazon.com/batch/home"
            f"?region={cfg['aws_region']}#jobs"
        )
        print(f"  Monitor: {console_url}")
    print(f"{'='*60}\n")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Submit benchmarkcat 3-phase batch pipeline to AWS Batch"
    )
    parser.add_argument(
        "--pipeline",
        required=True,
        choices=["gfm", "gfm_exp"],
        help="Which pipeline to run",
    )
    parser.add_argument("--bucket-name", default=None, help="Override S3 bucket")
    parser.add_argument(
        "--scenes-per-job",
        type=int,
        default=None,
        help="Override scenes per worker (default: terraform output or 50)",
    )
    parser.add_argument("--after-date", default=None, help="Only scenes >= YYYY-MM-DD")
    parser.add_argument("--before-date", default=None, help="Only scenes <= YYYY-MM-DD")
    parser.add_argument(
        "--dates",
        default=None,
        help="Comma-separated specific dates (YYYY-MM-DD)",
    )
    parser.add_argument("--profile", default=None, help="AWS profile for Batch operations")
    parser.add_argument(
        "--s3-profile",
        default=None,
        help="AWS profile for S3 access (defaults to --profile if not set)",
    )
    parser.add_argument("--region", default=None, help="AWS region")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be submitted without actually submitting",
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=30,
        help="Seconds between status polls (default: 30)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_pipeline(args)
