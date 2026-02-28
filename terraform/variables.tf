# -----------------------------------------------------------------------------
# Benchmarkcat — AWS Batch Infrastructure Variables
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# AWS / General
# -----------------------------------------------------------------------------
variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "aws_profile" {
  description = "AWS CLI profile to use"
  type        = string
  default     = "test-se"
}

variable "project_name" {
  description = "Project name used for resource naming"
  type        = string
  default     = "benchmarkcat"
}

variable "ecr_force_delete" {
  description = "Allow force delete of ECR repository (default true for dev)"
  type        = bool
  default     = true
}

variable "aws_account_id" {
  description = "AWS account ID"
  type        = string
  default     = "591210920133"
}

# -----------------------------------------------------------------------------
# IAM (existing roles — not managed by this Terraform config)
# -----------------------------------------------------------------------------
variable "batch_job_role_arn" {
  description = "IAM role ARN for Batch job containers (S3 access, CloudWatch Logs)"
  type        = string
}

variable "batch_instance_profile" {
  description = "EC2 instance profile ARN for Batch compute instances"
  type        = string
}

variable "spot_fleet_role_arn" {
  description = "IAM role ARN for EC2 Spot Fleet requests"
  type        = string
}

variable "batch_service_role_arn" {
  description = "Service-linked role ARN for AWS Batch"
  type        = string
}

# -----------------------------------------------------------------------------
# Networking
# -----------------------------------------------------------------------------
variable "subnets" {
  description = "Subnet IDs for Batch compute instances"
  type        = list(string)
}

variable "security_group_ids" {
  description = "Security group IDs for Batch compute instances"
  type        = list(string)
}

# -----------------------------------------------------------------------------
# Batch Compute
# -----------------------------------------------------------------------------
variable "max_vcpus" {
  description = "Maximum vCPUs for the Batch compute environment"
  type        = number
  default     = 256
}

variable "instance_types" {
  description = "EC2 instance types for Batch compute (CPU-only)"
  type        = list(string)
  default     = ["m5.xlarge", "m5.2xlarge", "r5.xlarge", "r5.2xlarge"]
}

variable "use_spot" {
  description = "Use Spot instances (true) or On-Demand (false)"
  type        = bool
  default     = true
}

# -----------------------------------------------------------------------------
# Job Definition Resources — per phase
# -----------------------------------------------------------------------------
variable "split_vcpus" {
  description = "vCPUs for split job (S3 pagination, lightweight)"
  type        = number
  default     = 2
}

variable "split_memory" {
  description = "Memory (MB) for split job"
  type        = number
  default     = 4096
}

variable "split_timeout" {
  description = "Timeout (seconds) for split job"
  type        = number
  default     = 1800 # 30 min
}

variable "worker_vcpus" {
  description = "vCPUs for worker job (rasterio mosaics are CPU-intensive)"
  type        = number
  default     = 4
}

variable "worker_memory" {
  description = "Memory (MB) for worker job"
  type        = number
  default     = 16384 # 16 GB
}

variable "worker_timeout" {
  description = "Timeout (seconds) for worker job"
  type        = number
  default     = 14400 # 4 hr
}

variable "merge_vcpus" {
  description = "vCPUs for merge job (parquet concat + collection rebuild)"
  type        = number
  default     = 2
}

variable "merge_memory" {
  description = "Memory (MB) for merge job"
  type        = number
  default     = 8192 # 8 GB
}

variable "merge_timeout" {
  description = "Timeout (seconds) for merge job"
  type        = number
  default     = 3600 # 1 hr
}

variable "retry_attempts" {
  description = "Number of retry attempts for failed jobs (handles Spot interruptions)"
  type        = number
  default     = 2
}

# -----------------------------------------------------------------------------
# S3 / Pipeline Config (defaults match existing code defaults)
# -----------------------------------------------------------------------------
variable "s3_bucket" {
  description = "S3 bucket for all pipeline data"
  type        = string
  default     = "fimc-data"
}

variable "scenes_per_job" {
  description = "Number of scenes each worker array child processes"
  type        = number
  default     = 50
}

variable "catalog_path" {
  description = "S3 prefix for the STAC catalog"
  type        = string
  default     = "benchmark/stac-bench-cat/"
}

variable "hucs_object_key" {
  description = "S3 key for the HUC8 GeoPackage"
  type        = string
  default     = "benchmark/stac-bench-cat/assets/WBDHU8_webproj.gpkg"
}

variable "boundaries_object_key" {
  description = "S3 key for the Mexico/Canada boundaries GeoPackage"
  type        = string
  default     = "benchmark/stac-bench-cat/assets/Mexico_Canada_boundaries.gpkg"
}

# GFM pipeline paths
variable "gfm_asset_object_key" {
  description = "S3 prefix for GFM source data (DFO event directories)"
  type        = string
  default     = "benchmark/rs/gfm/"
}

variable "gfm_manifest_s3_key" {
  description = "S3 key where the GFM manifest JSONL is written"
  type        = string
  default     = "benchmark/stac-bench-cat/batch/gfm_manifest.jsonl"
}

variable "gfm_partial_parquet_prefix" {
  description = "S3 prefix for GFM per-job partial parquets"
  type        = string
  default     = "benchmark/stac-bench-cat/batch/gfm_partials"
}

variable "gfm_derived_metadata_path" {
  description = "S3 key for the GFM master parquet"
  type        = string
  default     = "benchmark/stac-bench-cat/assets/derived-asset-data/gfm_collection.parquet"
}

# GFM_EXP pipeline paths
variable "gfm_exp_asset_object_key" {
  description = "S3 prefix for GFM_EXP source data (PI4 date directories)"
  type        = string
  default     = "benchmark/rs/PI4/"
}

variable "gfm_exp_manifest_s3_key" {
  description = "S3 key where the GFM_EXP manifest JSONL is written"
  type        = string
  default     = "benchmark/stac-bench-cat/batch/gfm_exp_manifest.jsonl"
}

variable "gfm_exp_partial_parquet_prefix" {
  description = "S3 prefix for GFM_EXP per-job partial parquets"
  type        = string
  default     = "benchmark/stac-bench-cat/batch/gfm_exp_partials"
}

variable "gfm_exp_derived_metadata_path" {
  description = "S3 key for the GFM_EXP master parquet"
  type        = string
  default     = "benchmark/stac-bench-cat/assets/derived-asset-data/gfm_expanded_collection.parquet"
}
