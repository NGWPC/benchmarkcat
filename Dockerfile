# Base image provides GDAL 3.8.4 + Python 3 on Ubuntu 22.04
FROM ghcr.io/osgeo/gdal:ubuntu-small-3.8.4

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Install pip and build essentials
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3-pip \
        python3-dev \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy dependency files
COPY requirements.txt setup.py ./

# Install Python deps.
# Skip GDAL — already provided by the base image at the correct version.
# rasterio[s3] pulls in boto3/botocore for direct s3:// URI reads.
RUN pip3 install --no-cache-dir --upgrade pip \
    && grep -iv '^gdal' requirements.txt \
       | pip3 install --no-cache-dir -r /dev/stdin

# Copy source and install package
COPY ingest/ ./ingest/
RUN pip3 install --no-cache-dir -e .

# Entrypoint translates cloud-provider array-job index env vars (AWS_BATCH_JOB_ARRAY_INDEX,
# AZ_BATCH_TASK_ID, BATCH_TASK_INDEX) into --job-index so application code stays cloud-agnostic.
# Examples:
#   gfm:     docker run <image> ingest.gfm.gfm_col --bucket_name fimc-data ...
#   gfm_exp: docker run <image> ingest.gfm_exp.gfm_exp_col --bucket_name fimc-data ...
COPY ./scripts/batch-entrypoint.sh /usr/local/bin/batch-entrypoint.sh
RUN chmod +x /usr/local/bin/batch-entrypoint.sh
ENTRYPOINT ["/usr/local/bin/batch-entrypoint.sh"]
