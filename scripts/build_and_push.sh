#!/bin/bash
set -e
set -o pipefail

# ---------------------------------------------------------------------------
# Benchmarkcat — Build Docker image and push to ECR
#
# Usage:
#   ./scripts/build_and_push.sh
#
# Reads aws_account_id, aws_region, aws_profile from terraform outputs.
# Falls back to AWS_ACCOUNT_ID, AWS_REGION, AWS_PROFILE env vars.
# Override ECR URL with ECR_REPO env var.
# ---------------------------------------------------------------------------

# Read from terraform outputs first, then env vars (no hardcoded defaults)
if [ -d "terraform" ] && command -v terraform &>/dev/null; then
  _tf_account=$(cd terraform && terraform output -raw aws_account_id 2>/dev/null) || true
  _tf_region=$(cd terraform && terraform output -raw aws_region 2>/dev/null) || true
  _tf_profile=$(cd terraform && terraform output -raw aws_profile 2>/dev/null) || true
  [ -n "$_tf_account" ] && AWS_ACCOUNT_ID="$_tf_account"
  [ -n "$_tf_region" ] && AWS_REGION="$_tf_region"
  [ -n "$_tf_profile" ] && AWS_PROFILE="$_tf_profile"
fi

# Require terraform or env vars
missing=""
[ -z "$AWS_ACCOUNT_ID" ] && missing="${missing}AWS_ACCOUNT_ID "
[ -z "$AWS_REGION" ] && missing="${missing}AWS_REGION "
[ -z "$AWS_PROFILE" ] && missing="${missing}AWS_PROFILE "
if [ -n "$missing" ]; then
  echo "ERROR: Missing: $missing" >&2
  echo "Run 'cd terraform && terraform init && terraform apply' or set env vars." >&2
  exit 1
fi

# Get ECR repo URL from terraform or env var
if [ -n "$ECR_REPO" ]; then
  echo "Using ECR_REPO from environment: $ECR_REPO"
elif [ -d "terraform" ] && command -v terraform &>/dev/null; then
  ECR_REPO=$(cd terraform && terraform output -raw ecr_repository_url 2>/dev/null) || true
fi

if [ -z "$ECR_REPO" ]; then
  # Fallback: construct from account ID and project name
  _tf_project=$(cd terraform && terraform output -raw project_name 2>/dev/null) || true
  PROJECT_NAME=${_tf_project:-${PROJECT_NAME:-benchmarkcat}}
  ECR_REPO="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${PROJECT_NAME}"
  echo "Using constructed ECR URL: $ECR_REPO"
else
  echo "Using ECR URL: $ECR_REPO"
fi

# 1. Login to ECR
echo "Logging in to ECR..."
aws ecr get-login-password \
  --region "$AWS_REGION" \
  --profile "$AWS_PROFILE" \
  | docker login --username AWS --password-stdin \
    "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

# 2. Build
echo "Building Docker image (linux/amd64)..."
docker build --platform linux/amd64 -t benchmarkcat .

# 3. Tag
docker tag benchmarkcat:latest "${ECR_REPO}:latest"

# 4. Push
echo "Pushing to ECR..."
docker push "${ECR_REPO}:latest"

echo ""
echo "Done. Image pushed to: ${ECR_REPO}:latest"
