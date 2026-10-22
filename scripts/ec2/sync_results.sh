#!/usr/bin/env bash
# Move results between the EC2 client and the laptop through S3.
#
#   EC2:    bash scripts/ec2/sync_results.sh push <RESULTS_TAG>
#   laptop: bash scripts/ec2/sync_results.sh pull <RESULTS_TAG>
#
# Only experiments/*/results_<tag>/ and scripts/fixes/results/ are synced.
set -euo pipefail
MODE="${1:?push|pull}"; TAG="${2:?RESULTS_TAG}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PROFILE=(); [ "$MODE" = pull ] && PROFILE=(--profile thesis)
ACCT="$(aws sts get-caller-identity "${PROFILE[@]}" --query Account --output text)"
DEST="s3://thesis-blocks-$ACCT/client-results/$TAG"
cd "$ROOT"
for d in experiments/experiment_*; do
  src="$d/results_$TAG"; key="$DEST/$(basename "$d")"
  if [ "$MODE" = push ]; then
    # --delete: the instance's copy is authoritative. Without it, files a stage moved into a
    # superseded_*/ folder stayed at the top level in S3 and came back on every pull (18 Oct).
    [ -d "$src" ] && aws s3 sync --only-show-errors --delete "$src" "$key"
  else
    # --exact-timestamps: a same-size file whose time differs is re-downloaded (without it, sync kept
    # 16 Oct copies of files the 18 Oct runs had replaced).
    aws s3 sync "${PROFILE[@]}" --only-show-errors --exact-timestamps "$key" "$src"
  fi
done
if [ "$MODE" = push ]; then
  [ -d scripts/fixes/results ] && aws s3 sync --only-show-errors scripts/fixes/results "$DEST/fixes"
else
  aws s3 sync "${PROFILE[@]}" --only-show-errors "$DEST/fixes" "scripts/fixes/results_$TAG"
fi
echo "$MODE done: $DEST"
