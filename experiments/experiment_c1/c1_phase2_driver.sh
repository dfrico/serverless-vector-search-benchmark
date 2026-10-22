#!/usr/bin/env bash
# Experiment C1, Phase 2 driver — run all sweep cells in index-state order.
#
# Order matters because there is one index per dataset:
#   1. Axis C first, on the Phase-1 baseline index (n_probe=10, num_workers=16):
#      config-rewrite only, bpt in {8,4,2,1}. No rebuild.
#   2. Axis A: rebuild per n_probe in {7,5,3,1} (num_workers=16, bpt=16).
#   3. Axis B: rebuild per num_workers in {4,8,32} (n_probe=10, bpt=16).
# base (n_probe=10, num_workers=16, bpt=16) was already measured in Phase 1.
#
# Each cell runs via ec_blocks.sh c1_run.py; a non-zero exit (e.g. the $0.20
# ingest guard, exit 3) stops the whole chain.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

# C1_START_AT=<label>: skip the cells before it (resume after a failure, 2026-10-18).
# Each cell first gets fresh Lithops worker containers (c1_fresh_workers.py: /tmp fills up in
# long-lived warm containers).
START="${C1_START_AT:-}"
run() {
  local label="${*: -1}"
  if [ -n "$START" ]; then [ "$label" = "$START" ] && START="" || { echo "=== SKIP $label (C1_START_AT) ==="; return 0; }; fi
  ./ec_blocks.sh c1_fresh_workers.py || { echo "STOP: fresh workers before $label failed"; exit 1; }
  echo "=== CELL $* ==="; ./ec_blocks.sh c1_run.py "$@" || { echo "STOP: cell $* failed"; exit 1; }; }

# Axis C — config rewrite on the baseline index (no rebuild)
run --axis C --value 8 --label c_bpt8
run --axis C --value 4 --label c_bpt4
run --axis C --value 2 --label c_bpt2
run --axis C --value 1 --label c_bpt1

# Axis A — rebuild per n_probe (writes fresh config: bpt=16, num_workers=16)
run --axis A --value 7 --label a_np7
run --axis A --value 5 --label a_np5
run --axis A --value 3 --label a_np3
run --axis A --value 1 --label a_np1

# Axis B — rebuild per num_workers (n_probe=10, bpt=16)
run --axis B --value 4  --label b_nw4
run --axis B --value 8  --label b_nw8
run --axis B --value 32 --label b_nw32

echo "=== PHASE 2 DRIVER COMPLETE ==="
