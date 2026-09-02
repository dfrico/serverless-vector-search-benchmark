#!/usr/bin/env bash
# Experiment C3, Phase 3 — run all sweep cells SEQUENTIALLY (never concurrent:
# the C2 sequencing rule + the C3 client-side network-contention finding).
# Order is short cells first so a structural fault shows early; the 46-min
# LanceDB concurrency-1 cell runs last.
#
# Per-adapter sample size (agreed): Blocks 50, LanceDB 50, S3 Vectors 500.
# Durations target those counts under closed-loop concurrency (n ~= dur*conc/L).
#   Blocks   L~=16s   S3 Vectors L~=0.65s   LanceDB L~=54s
# Warmup: LanceDB must be 1 (query_batch packs the warmup into ONE invoke; 5
# would be ~270s and exceed the 120s read timeout). Blocks 1, S3 Vectors 5.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
TS="$(date +%s)"
R="$HERE/results"

cell() {  # launcher script adapter conc duration warmup
  local launcher="$1" adapter="$2" conc="$3" dur="$4" warm="$5"
  local log="$R/sweep_${adapter}_c${conc}.run.log"
  echo "=== CELL ${adapter} c=${conc} dur=${dur}s warmup=${warm} @ $(date -u +%H:%M:%SZ) ==="
  "./$launcher" c3_sweep.py "$adapter" "$conc" --duration "$dur" --warmup "$warm" --ts "$TS" > "$log" 2>&1
  echo "    rc=$? ($(date -u +%H:%M:%SZ)) -> $log"
  tail -3 "$log"
}

cell c3_blocks.sh blocks    1  800  1   # ~50 queries
cell c3_aws.sh    s3vectors 1  330  5   # ~500 queries
cell c3_aws.sh    s3vectors 4  85   5   # ~500 queries
cell c3_aws.sh    lancedb   10 280  1   # ~50 queries
cell c3_aws.sh    lancedb   4  690  1   # ~50 queries
cell c3_aws.sh    lancedb   1  2760 1   # ~50 queries (long pole ~46min)

echo "=== ALL SWEEP CELLS DONE @ $(date -u +%H:%M:%SZ) ==="
ls -la "$R"/sweep_*.jsonl 2>/dev/null
