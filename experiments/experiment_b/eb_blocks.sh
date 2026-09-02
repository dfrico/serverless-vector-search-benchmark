#!/usr/bin/env bash
# Experiment B launcher — Blocks on real AWS.
# harness/.venv (pinned lithops 3.6.3 + faiss) + thesis profile. NO MinIO
# env: boto3 and Lithops must hit real S3 + aws_lambda. venv bin on PATH so
# Lithops' localhost bits (if any) resolve the pinned python. Requires
# ~/.lithops/config = aws_lambda/aws_s3 (write_aws_lithops_config.py).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE="$(dirname "$(dirname "$HERE")")"
VENV_BIN="$WORKSPACE/harness/.venv/bin"

unset AWS_ENDPOINT_URL_S3 AWS_ENDPOINT_URL AWS_ENDPOINT_URL_S3VECTORS \
      AWS_ENDPOINT_URL_LAMBDA AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY \
      AWS_SESSION_TOKEN || true
export AWS_PROFILE="thesis"
export AWS_DEFAULT_REGION="us-east-1"
export AWS_REGION="us-east-1"
export PATH="$VENV_BIN:$PATH"

exec "$VENV_BIN/python" "$@"
