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

import logging

from ingest.batch_utils import run_batch_merge
from ingest.gfm_exp.gfm_exp_col import create_gfm_exp_collection

logging.basicConfig(level=logging.INFO)


def main():
    run_batch_merge(
        catalog_id="gfm-expanded-collection",
        collection_creator=create_gfm_exp_collection,
        description="gfm_exp",
        default_asset_object_key=None,
        default_derived_metadata_path=None,
    )


if __name__ == "__main__":
    main()
