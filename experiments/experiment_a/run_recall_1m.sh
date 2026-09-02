#!/usr/bin/env bash
# Experiment A (local) — Blocks recall at 1M.
# Blocks needs harness/.venv (lithops/faiss/orjson) + the MinIO control-plane
# env, and the venv bin on PATH (Lithops execs a bare `python` for localhost
# workers). No thesis profile, no real AWS. See findings/16 "Keep the venv split".
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE="$(dirname "$(dirname "$HERE")")"
VENV_BIN="$WORKSPACE/harness/.venv/bin"

export AWS_ENDPOINT_URL_S3="http://localhost:9000"
export AWS_ACCESS_KEY_ID="minioadmin"
export AWS_SECRET_ACCESS_KEY="minioadmin"
export AWS_DEFAULT_REGION="us-east-1"
export PATH="$VENV_BIN:$PATH"

exec "$VENV_BIN/python" "$HERE/recall_1m.py" "$@"
