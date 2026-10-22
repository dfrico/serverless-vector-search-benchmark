#!/usr/bin/env bash
# Lithops polling test (20 Oct): does the 2-s grid in Blocks latency come from Lithops' storage job monitor
# (monitoring_interval 2 s by default)? Rebuilds the 1M index (C1 phase 1, also the recall gate and a base
# control), then runs bpt 8 at the default interval and base + bpt 8 with monitoring_interval 0.5.
# Run on the EC2 client from ~/thesis:  bash scripts/ec2/poll_test.sh   (RESULTS_TAG 2026-10-20-poll)
set -euo pipefail
export RESULTS_TAG=2026-10-20-poll
cd ~/thesis/experiments/experiment_c1
POLL=/tmp/lithops_poll05.yaml
sed 's/^  log_level: INFO$/  log_level: INFO\n  monitoring_interval: 0.5/' ~/.lithops/config > "$POLL"
LITHOPS_CONFIG_FILE="$POLL" ../../harness/.venv/bin/python -c \
  "import lithops.config as c; print('poll config monitoring_interval =', c.load_config()['lithops'].get('monitoring_interval'))"
../../harness/.venv/bin/python -c \
  "import lithops.config as c; print('default config monitoring_interval =', c.load_config()['lithops'].get('monitoring_interval', 'unset (2)'))"
./ec_blocks.sh c1_fresh_workers.py
./ec_blocks.sh c1_phase1.py
./ec_blocks.sh c1_fresh_workers.py
./ec_blocks.sh c1_run.py --axis C --value 8 --label c_bpt8_poll2
./ec_blocks.sh c1_fresh_workers.py
LITHOPS_CONFIG_FILE="$POLL" ./ec_blocks.sh c1_run.py --axis C --value 8 --label c_bpt8_poll05
./ec_blocks.sh c1_fresh_workers.py
LITHOPS_CONFIG_FILE="$POLL" ./ec_blocks.sh c1_run.py --axis C --value 16 --label base_poll05
echo "=== POLL TEST COMPLETE ==="
