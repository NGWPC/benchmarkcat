#!/bin/bash

# Refresh cache by making an AWS call
aws sts get-caller-identity --profile SoftwareEngineersFull-218573839066

# Define the paths to AWS cache and credentials files
AWS_CACHE_DIR="$HOME/.aws/cli/cache"
AWS_CREDENTIALS_FILE="$HOME/.aws/credentials"

# Find the most recent cache file
LATEST_CACHE_FILE=$(find "$AWS_CACHE_DIR" -type f -printf '%T+ %p\n' | sort -r | head -n 1 | cut -d' ' -f2-)

if [ -z "$LATEST_CACHE_FILE" ]; then
    echo "No AWS cache files found."
    exit 1
fi

# Extract credentials from the cache file
AWS_ACCESS_KEY_ID=$(jq -r '.Credentials.AccessKeyId' "$LATEST_CACHE_FILE")
AWS_SECRET_ACCESS_KEY=$(jq -r '.Credentials.SecretAccessKey' "$LATEST_CACHE_FILE")
AWS_SESSION_TOKEN=$(jq -r '.Credentials.SessionToken' "$LATEST_CACHE_FILE")

# Check if credentials are present
if [ -z "$AWS_ACCESS_KEY_ID" ] || [ -z "$AWS_SECRET_ACCESS_KEY" ] || [ -z "$AWS_SESSION_TOKEN" ]; then
    echo "Failed to extract AWS credentials."
    exit 1
fi

# Update ~/.aws/credentials file
{
    echo '[default]'
    echo "aws_access_key_id=$AWS_ACCESS_KEY_ID"
    echo "aws_secret_access_key=$AWS_SECRET_ACCESS_KEY"
    echo "aws_session_token=$AWS_SESSION_TOKEN"
} > "$AWS_CREDENTIALS_FILE"

echo "AWS credentials updated successfully."
