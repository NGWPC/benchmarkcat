#!/bin/bash

# Small script to setup the conda environment necessary to run the catalog manipulation scripts in the OE so that can update OE catalog items, move data, update asset hrefs, etc
# run this command with "source" to avoid it being run in a subshell. This will leave you in the conda env in the calling shell.
source /contrib/software/miniconda/miniconda/etc/profile.d/conda.sh
conda create --name bench_env python=3.11.5 -y
conda activate bench_env
pip install pystac boto3
