#!/usr/bin/env bash
# Usage (from the "MS Thesis" folder, in Terminal on your Mac):
#   bash scripts/fixes/run_checks.sh cloudwatch            # ~2 min, pennies
#   bash scripts/fixes/run_checks.sh postingest s3vectors  # ~5 min
#   bash scripts/fixes/run_checks.sh postingest lancedb    # ~10 min (50 queries x 51 s / 10 parallel)
#   bash scripts/fixes/run_checks.sh postingest blocks     # ~8 min (30 queries x 13 s)
# Results land in scripts/fixes/results/. Nothing is deleted or modified on AWS.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
unset AWS_ENDPOINT_URL_S3 AWS_ENDPOINT_URL AWS_ENDPOINT_URL_S3VECTORS AWS_ENDPOINT_URL_LAMBDA \
      AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN || true
# Use the laptop profile when it exists; on the EC2 client the instance role applies.
if grep -qsE '^\[(profile )?thesis\]' ~/.aws/config ~/.aws/credentials; then
  export AWS_PROFILE="thesis"
else
  unset AWS_PROFILE
fi
export AWS_DEFAULT_REGION=us-east-1 AWS_REGION=us-east-1
PY="${PY:-}"
[ -z "$PY" ] && PY=python3 && for v in "$ROOT/harness/.venv" "$ROOT/spike-localhost/.venv" "$ROOT/.venv"; do
  [ -x "$v/bin/python" ] && PY="$v/bin/python" && break
done || true
# Lithops keeps files/sockets open per job; macOS defaults to 256 open files, which a
# 20-task-per-query Blocks run exhausts after a few queries (Errno 24, then EndpointConnectionError).
ulimit -n 10240 2>/dev/null || ulimit -n 4096 2>/dev/null || true
echo "open-files limit: $(ulimit -n)"
echo "python: $PY"; aws sts get-caller-identity --query Account --output text 2>/dev/null || true
cmd="$1"; shift
case "$cmd" in
  cloudwatch) exec "$PY" "$HERE/cloudwatch_billed.py" ;;
  postingest) exec "$PY" "$HERE/postingest_checks.py" "$@" ;;
  *) echo "unknown: $cmd"; exit 2 ;;
esac
