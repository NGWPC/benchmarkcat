#!/bin/bash
# Translate the cloud provider's array-job index env var into --job-index
# so application code stays cloud-agnostic.
#
#   AWS Batch:   AWS_BATCH_JOB_ARRAY_INDEX
#   Azure Batch: AZ_BATCH_TASK_ID
#   GCP Batch:   BATCH_TASK_INDEX
#
# --job-index is only injected when the env var is actually set by the cloud
# provider (i.e., for array worker jobs). Split and merge jobs, which don't
# accept --job-index, run without it.
#
# Usage:
#   docker run <image> ingest.gfm_exp.gfm_exp_col --mode batch-worker ...

# Resolve job index from whichever cloud provider's env var is set
if [ -n "$AWS_BATCH_JOB_ARRAY_INDEX" ]; then
    JOB_INDEX="$AWS_BATCH_JOB_ARRAY_INDEX"
fi

if [ -n "$JOB_INDEX" ]; then
    exec python3 -m "$@" --job-index "$JOB_INDEX"
else
    exec python3 -m "$@"
fi
