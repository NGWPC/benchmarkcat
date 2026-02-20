"""
gfm batch_merge — merge partial parquets into master, rebuild collection.json, clean up partials.

Run this *once* after all batch-worker array jobs complete.

Usage:
    python -m ingest.gfm.batch_merge \
        --bucket_name fimc-data \
        --partial-parquet-prefix benchmark/stac-bench-cat/batch/gfm_partials \
        --derived_metadata_path benchmark/stac-bench-cat/assets/derived-asset-data/gfm_collection.parquet \
        --catalog_path benchmark/stac-bench-cat/ \
        [--profile my-profile]
        [--skip-delete-partials]
"""

import logging

from ingest.batch_utils import run_batch_merge
from ingest.gfm.gfm_col import create_gfm_collection

logging.basicConfig(level=logging.INFO)


def main():
    run_batch_merge(
        catalog_id="gfm-collection",
        collection_creator=create_gfm_collection,
        description="gfm",
        default_asset_object_key="benchmark/rs/gfm/",
        default_derived_metadata_path="benchmark/stac-bench-cat/assets/derived-asset-data/gfm_collection.parquet",
    )


if __name__ == "__main__":
    main()
