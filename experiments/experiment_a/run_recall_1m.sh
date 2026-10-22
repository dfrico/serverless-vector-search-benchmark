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

# Output folder (2026-10-16): RESULTS_TAG must be set so a re-run can't overwrite
# the August evidence in results/. RESULTS_TAG=orig targets results/ explicitly.
: "${RESULTS_TAG:?set RESULTS_TAG (e.g. RESULTS_TAG=2026-10) or RESULTS_TAG=orig for results/}"
export RESULTS_TAG
if [ "$RESULTS_TAG" = "orig" ]; then RESULTS_DIR="$HERE/results"; else RESULTS_DIR="$HERE/results_$RESULTS_TAG"; fi
mkdir -p "$RESULTS_DIR"
exec "$VENV_BIN/python" "$HERE/recall_1m.py" "$@"
