#!/usr/bin/env bash
# Experiment B launcher — S3 Vectors and LanceDB-on-Lambda on real AWS.
# spike-localhost/.venv + thesis profile. Real AWS: pop every endpoint override
# (both adapters raise if AWS_ENDPOINT_URL* is set) and any static keys, so the
# thesis profile is the single credential source.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE="$(dirname "$HERE")"
VENV_BIN="$WORKSPACE/spike-localhost/.venv/bin"

unset AWS_ENDPOINT_URL_S3 AWS_ENDPOINT_URL AWS_ENDPOINT_URL_S3VECTORS \
      AWS_ENDPOINT_URL_LAMBDA AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY \
      AWS_SESSION_TOKEN || true
export AWS_PROFILE="thesis"
export AWS_DEFAULT_REGION="us-east-1"
export AWS_REGION="us-east-1"

exec "$VENV_BIN/python" "$@"
