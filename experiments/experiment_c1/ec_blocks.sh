#!/usr/bin/env bash
# Experiment C1 launcher — Blocks on real AWS (same env as Experiment B).
# harness/.venv (pinned lithops 3.6.3 + faiss) + thesis profile. NO MinIO
# env: boto3 and Lithops must hit real S3 + aws_lambda. Requires
# ~/.lithops/config = aws_lambda/aws_s3 (set by Experiment B's deploy).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE="$(dirname "$(dirname "$HERE")")"
VENV_BIN="$WORKSPACE/harness/.venv/bin"

unset AWS_ENDPOINT_URL_S3 AWS_ENDPOINT_URL AWS_ENDPOINT_URL_S3VECTORS \
      AWS_ENDPOINT_URL_LAMBDA AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY \
      AWS_SESSION_TOKEN || true
# Use the laptop profile when it exists; on the EC2 client the instance role applies.
if grep -qsE '^\[(profile )?thesis\]' ~/.aws/config ~/.aws/credentials; then
  export AWS_PROFILE="thesis"
else
  unset AWS_PROFILE
fi
export AWS_DEFAULT_REGION="us-east-1"
export AWS_REGION="us-east-1"
export PATH="$VENV_BIN:$PATH"

# Output folder (2026-10-16): RESULTS_TAG must be set so a re-run can't overwrite
# the August evidence in results/. RESULTS_TAG=orig targets results/ explicitly.
: "${RESULTS_TAG:?set RESULTS_TAG (e.g. RESULTS_TAG=2026-10) or RESULTS_TAG=orig for results/}"
export RESULTS_TAG
if [ "$RESULTS_TAG" = "orig" ]; then RESULTS_DIR="$HERE/results"; else RESULTS_DIR="$HERE/results_$RESULTS_TAG"; fi
mkdir -p "$RESULTS_DIR"
exec "$VENV_BIN/python" "$@"
