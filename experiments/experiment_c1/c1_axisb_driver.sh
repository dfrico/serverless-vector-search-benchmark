#!/usr/bin/env bash
# Experiment C1, Axis B rerun — num_workers in {4,8,32}.
#
# The Phase-2 driver stopped when b_nw4 hit a 295s query timeout. Code review of
# indexator.py shows nw4 and nw8 build a complete 16-partition index (partitions
# 0..15), so their queries should match the baseline. The nw4 timeout looks
# transient. nw32 over-partitions (writes 0..31; query reads only 0..15) so we
# expect a recall collapse there.
#
# Unlike the Phase-2 driver, this one CONTINUES on a cell failure so one repeat
# timeout does not block the other cells. Each cell rebuilds (delete + re-ingest),
# so a broken index does not carry over.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

run() {
  echo "=== CELL $* ==="
  if ./ec_blocks.sh c1_run.py "$@"; then
    echo "OK: cell $*"
  else
    echo "FAIL: cell $* (exit $?) — continuing"
  fi
}

run --axis B --value 4  --label b_nw4
run --axis B --value 8  --label b_nw8
run --axis B --value 32 --label b_nw32

echo "=== AXIS B DRIVER COMPLETE ==="
