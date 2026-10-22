#!/usr/bin/env bash
# Baseline re-run (2026-10-16): repeat Experiments B, C1, C2 and C3 with the same
# parameters as the August runs, writing to results_$RESULTS_TAG/ next to the
# original results/. Each stage runs the experiment's own ingest, measurement,
# aggregation and cleanup, in the August order. Stages run sequentially (the C3
# client-side contention finding); C2 runs S3 Vectors and LanceDB together, as in
# August, then Blocks.
#
#   RESULTS_TAG=2026-10 caffeinate -i ./rerun_baseline.sh b     # one stage
#   stages: preflight b c1 c2 c3 c3_rest c3_sweep s3v_rate b_lancedb_c10
#
# Stops at the first failing step. Logs: experiment_*/results_$RESULTS_TAG/*.log
set -euo pipefail
# Run from a snapshot copy: bash reads scripts as it goes, so editing this file
# during a multi-hour stage would corrupt the running stage (happened 2026-10-16).
if [ -z "${RERUN_SNAPSHOT:-}" ]; then
  _here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; _snap="$(mktemp "${TMPDIR:-/tmp}/rerun_baseline.XXXXXX")"
  cp "${BASH_SOURCE[0]}" "$_snap"
  RERUN_SNAPSHOT="$_snap" RERUN_HERE="$_here" exec bash "$_snap" "$@"
fi
HERE="$RERUN_HERE"
: "${RESULTS_TAG:?set RESULTS_TAG, e.g. RESULTS_TAG=2026-10}"
[ "$RESULTS_TAG" = "orig" ] && { echo "refusing to write into the August results/"; exit 2; }
export RESULTS_TAG
# AWS CLI profile: the laptop's "thesis" profile if present, else the instance role (EC2).
AWSP=(); grep -qsE '^\[(profile )?thesis\]' ~/.aws/config ~/.aws/credentials && AWSP=(--profile thesis)
ulimit -n 10240   # Lithops opens many files per fan-out job (fix list §4.1)

ts() { date -u +%H:%M:%SZ; }
step() {  # step <exp dir> <launcher> <log name> <args...>
  local dir="$1" launcher="$2" log="$3"; shift 3
  local out="$HERE/$dir/results_$RESULTS_TAG"; mkdir -p "$out"
  echo "=== $(ts) $dir: $launcher $* -> $log"
  ( cd "$HERE/$dir" && "./$launcher" "$@" ) > "$out/$log" 2>&1 \
    || { echo "STOP: $dir $* failed (rc=$?), see $out/$log"; tail -5 "$out/$log"; exit 1; }
  tail -2 "$out/$log"
}

wake_lambdas() {
  # Image functions go Inactive after weeks idle; the first call then takes 1-2 min
  # and pollutes the first measured cell. Wait until every function is Active.
  local fns
  fns=$(aws lambda list-functions "${AWSP[@]}" --region us-east-1 \
        --query 'Functions[?starts_with(FunctionName,`lithops-worker`)||FunctionName==`thesis-lancedb-lambda`].FunctionName' --output text)
  for f in $fns; do
    local st
    st=$(aws lambda get-function-configuration "${AWSP[@]}" --region us-east-1 \
         --function-name "$f" --query State --output text)
    echo "    $f: $st"
    if [ "$st" != "Active" ]; then
      aws lambda invoke "${AWSP[@]}" --region us-east-1 --function-name "$f" \
        --payload '{}' --cli-binary-format raw-in-base64-out /dev/null >/dev/null 2>&1 || true
      aws lambda wait function-active-v2 "${AWSP[@]}" --region us-east-1 --function-name "$f"
      echo "    $f: Active"
    fi
  done
}

stage_preflight() {
  echo "=== $(ts) preflight"
  aws sts get-caller-identity "${AWSP[@]}" --region us-east-1 --query Account --output text
  git -C "$HERE/../references/blocks-db-wip" rev-parse --short HEAD | grep -q '^4916370' \
    || { echo "STOP: blocks-db-wip is not at 4916370e"; exit 1; }
  "$HERE/../harness/.venv/bin/python" -c "import lithops; assert lithops.__version__=='3.6.3'"
  # Client network gate (2026-10-16): a slow link inflated every latency in the first
  # October B run. Median TCP connect to the us-east-1 endpoints must be below
  # MAX_CONNECT_S (default 0.2 s; a normal Europe-us-east-1 round trip is ~0.1 s).
  ( cd "$HERE/.." && harness/.venv/bin/python -c "
import sys, statistics; sys.path.insert(0, '.')
from harness.records import client_network_probe
p = client_network_probe(7); m = statistics.median(v for v in p.values() if v)
print('    client TCP connect (s):', p, 'median', round(m, 3))
sys.exit(0 if m < float('${MAX_CONNECT_S:-0.2}') else 1)" ) \
    || { echo "STOP: client network too slow for latency measurements (set MAX_CONNECT_S to override)"; exit 1; }
  wake_lambdas
}

stage_b() {  # Experiment B: 1M three-way (findings 20)
  stage_preflight
  step experiment_b eb_blocks.sh recall_blocks.log run_recall.py blocks
  step experiment_b eb_aws.sh recall_s3vectors.log run_recall.py s3vectors
  step experiment_b eb_aws.sh recall_lancedb_ingest.log run_recall.py lancedb --ingest-only
  step experiment_b eb_aws.sh recall_lancedb_query.log recall_query_lancedb.py
  local t; t=$(date +%s)
  for c in 1 4 10; do
    step experiment_b eb_blocks.sh sweep_blocks_c$c.log run_sweep.py blocks $c --duration 150 --ts "$t"
    step experiment_b eb_aws.sh sweep_s3vectors_c$c.log run_sweep.py s3vectors $c --duration 45 --ts "$t"
    step experiment_b eb_aws.sh sweep_lancedb_c$c.log run_sweep.py lancedb $c --duration 120 --ts "$t"
  done
  step experiment_b eb_aws.sh aggregate.log eb_aggregate.py
  step experiment_b eb_aws.sh cleanup.log eb_cleanup.py
}

stage_b_lancedb_c10() {  # Redo B LanceDB c=10 (2026-10-17): on the 2-vCPU EC2 client the
  # thread pool held it at 6 in flight (harness/workload.py fix). Re-ingests the 1M table,
  # reruns the cell and the aggregate, keeps the superseded files, then cleans up 1M again.
  stage_preflight
  local out="$HERE/experiment_b/results_$RESULTS_TAG" old
  old="$out/superseded_pool6"; mkdir -p "$old"
  mv "$out"/sweep_lancedb_c10.* "$out"/comparison_table.* "$out"/aggregate.log "$old"/
  cp "$out"/recall_lancedb.ingest.json "$old"/
  step experiment_b eb_aws.sh redo_lancedb_ingest.log run_recall.py lancedb --ingest-only
  mv "$out"/recall_lancedb.ingest.json "$out"/redo_lancedb.ingest.json
  cp "$old"/recall_lancedb.ingest.json "$out"/
  step experiment_b eb_aws.sh sweep_lancedb_c10.log run_sweep.py lancedb 10 --duration 120 --ts "$(date +%s)"
  step experiment_b eb_aws.sh aggregate.log eb_aggregate.py
  step experiment_b eb_aws.sh cleanup_redo.log eb_cleanup.py
}

# --- 2026-10-18 re-measures so every number comes from the EC2 client (fix list F41) ---

stage_blocks_10m_rebuild() {  # Blocks 10M: re-upload corpus + rebuild index IN PLACE from EC2
  # (Aug build was from the laptop), then redo the gate and the C3 Blocks cell on the new index.
  stage_preflight
  local out="$HERE/experiment_c3/results_$RESULTS_TAG" old
  old="$out/superseded_aug_index"; mkdir -p "$old"
  mv "$out"/phase2_blocks.* "$out"/sweep_blocks_c1.* "$out"/comparison_table_10m.* "$out"/aggregate.log "$old"/
  step experiment_c3 c3_blocks.sh phase2_blocks.log c3_phase2.py blocks --recall-n 50
  step experiment_c3 c3_blocks.sh sweep_blocks_c1.run.log c3_sweep.py blocks 1 --duration 800 --warmup 1 --ts "$(date +%s)"
  step experiment_c3 c3_aws.sh aggregate.log c3_aggregate.py
}

stage_lancedb_10m_ingest() {  # LanceDB 10M ingest from EC2 into deep_10m_reingest (measured
  # table untouched), 5-query gate on it, then drop it.
  stage_preflight
  export C3_DATASET=deep_10m_reingest
  step experiment_c3 c3_aws.sh reingest_lancedb_10m.log c3_phase2.py lancedb --recall-n 5 --out-name reingest_lancedb_10m
  unset C3_DATASET
  step experiment_c3 c3_aws.sh drop_lancedb_reingest.log c3_drop_lancedb.py deep_10m_reingest
}

stage_c2_serial() {  # C2 S3V and LanceDB again, one at a time (the 17 Oct run had them concurrent,
  # as in August, which shared the 2-vCPU client). Blocks C2 already ran alone and is kept.
  stage_preflight
  local out="$HERE/experiment_c2/results_$RESULTS_TAG" old
  old="$out/superseded_concurrent"; mkdir -p "$old"
  mv "$out"/phase1_s3vectors.* "$out"/phase1_lancedb.* "$out"/phase2_s3vectors.log "$out"/phase2_lancedb.log \
     "$out"/cold_s3vectors.* "$out"/cold_lancedb.* "$out"/cold_start_table.* "$out"/aggregate.log \
     "$out"/cleanup_s3vectors.log "$out"/cleanup_lancedb.log "$old"/
  step experiment_c2 c2_aws.sh phase1_s3vectors.log c2_phase1.py s3vectors
  step experiment_c2 c2_aws.sh phase1_lancedb.log c2_phase1.py lancedb
  step experiment_c2 c2_aws.sh phase2_s3vectors.log c2_driver.py s3vectors
  step experiment_c2 c2_aws.sh phase2_lancedb.log c2_driver.py lancedb
  step experiment_c2 c2_aws.sh aggregate.log c2_aggregate.py
  step experiment_c2 c2_aws.sh cleanup_s3vectors.log c2_cleanup.py s3vectors
  step experiment_c2 c2_aws.sh cleanup_lancedb.log c2_cleanup.py lancedb
}

# --- 2026-10-18 Blocks cells redone with the leak fix (harness/adapters/blocks.py _release_blocks_db):
# before it, every per-query Lithops executor stayed alive and its job monitor kept LISTing the
# Lithops bucket, so S3 requests per query grew within a cell (C1 base: 3.3 -> 28 LIST/s). Lambda
# charge unaffected; Level 2 was inflated. Old files go to superseded_leak/.

stage_b_blocks() {  # B Blocks: 1M ingest + recall, the three cells, aggregate, cleanup
  stage_preflight
  local out="$HERE/experiment_b/results_$RESULTS_TAG" old t
  old="$out/superseded_leak"; mkdir -p "$old"
  mv "$out"/recall_blocks.* "$out"/sweep_blocks_c*.* "$out"/comparison_table.* "$out"/aggregate.log "$old"/
  step experiment_b eb_blocks.sh recall_blocks.log run_recall.py blocks
  t=$(date +%s)
  for c in 1 4 10; do
    step experiment_b eb_blocks.sh sweep_blocks_c$c.log run_sweep.py blocks $c --duration 150 --ts "$t"
  done
  step experiment_b eb_aws.sh aggregate.log eb_aggregate.py
  step experiment_b eb_aws.sh cleanup_blocks_redo.log eb_cleanup.py
}

stage_c1_redo() {  # C1 again, whole stage
  local out="$HERE/experiment_c1/results_$RESULTS_TAG" old
  old="$out/superseded_leak"; mkdir -p "$old"
  find "$out" -maxdepth 1 -type f -exec mv {} "$old"/ \;
  stage_c1
}

stage_c1_resume() {  # C1 after the 18 Oct ENOSPC stop: keep today's cells, resume at a_np7
  # The redo chain stopped in a_np7 when a warm Lithops worker's /tmp filled up (old per-job module
  # dirs, c1_fresh_workers.py). The cells before it (base, cap-10, axis C) are valid and stay.
  stage_preflight
  local out="$HERE/experiment_c1/results_$RESULTS_TAG" old
  old="$out/failed_attempt_enospc"; mkdir -p "$old"
  mv "$out"/phase2.log "$old"/ 2>/dev/null || true
  mv "$out"/measure_a_np7.* "$old"/ 2>/dev/null || true
  export C1_START_AT=a_np7
  step experiment_c1 c1_phase2_driver.sh phase2.log
  unset C1_START_AT
  step experiment_c1 c1_axisb_driver.sh phase2b.log
  step experiment_c1 ec_blocks.sh aggregate.log c1_aggregate.py
  step experiment_c1 ec_blocks.sh cleanup.log c1_cleanup.py
}

stage_c2_blocks() {  # C2 Blocks again (phase 1 index + cold-start driver), aggregate, cleanup
  stage_preflight
  step experiment_c1 ec_blocks.sh fresh_workers_c2.log c1_fresh_workers.py
  local out="$HERE/experiment_c2/results_$RESULTS_TAG" old
  old="$out/superseded_leak"; mkdir -p "$old"
  mv "$out"/phase1_blocks.* "$out"/cold_blocks.* "$out"/phase2_blocks.log "$out"/cleanup_blocks.log \
     "$out"/cold_start_table.* "$out"/aggregate.log "$old"/
  step experiment_c2 c2_blocks.sh phase1_blocks.log c2_phase1.py blocks
  step experiment_c2 c2_blocks.sh phase2_blocks.log c2_driver.py blocks
  step experiment_c2 c2_aws.sh aggregate.log c2_aggregate.py
  step experiment_c2 c2_blocks.sh cleanup_blocks.log c2_cleanup.py blocks
}

MAP_FN=lithops-worker-rb26-363-11170a3116   # Blocks map/build function (3008 MB)

cap10_on()  { aws lambda put-function-concurrency "${AWSP[@]}" --region us-east-1 \
                --function-name "$MAP_FN" --reserved-concurrent-executions 10 >/dev/null
              echo "    $MAP_FN reserved concurrency = 10"; }
cap10_off() { aws lambda delete-function-concurrency "${AWSP[@]}" --region us-east-1 \
                --function-name "$MAP_FN" >/dev/null 2>&1 || true
              echo "    $MAP_FN reserved concurrency removed"; }

stage_c1() {  # Experiment C1: Blocks tuning at 1M (finding 22)
  stage_preflight
  step experiment_c1 ec_blocks.sh phase1.log c1_phase1.py
  # Cap-10 check (2026-10-16, user decision): August C1 ran under an account cap of 10,
  # which throttled the 16-task cell (context/2026-10-16 Concurrency cap timeline.md).
  # Repeat the 8- and 16-task cells with the map function limited to 10, on the
  # baseline index, before the main sweep. The trap removes the limit on any exit.
  trap cap10_off EXIT
  cap10_on
  step experiment_c1 ec_blocks.sh cap10_bpt2.log c1_run.py --axis C --value 2 --label c_bpt2_cap10
  step experiment_c1 ec_blocks.sh cap10_bpt1.log c1_run.py --axis C --value 1 --label c_bpt1_cap10
  cap10_off
  trap - EXIT
  step experiment_c1 c1_phase2_driver.sh phase2.log
  step experiment_c1 c1_axisb_driver.sh phase2b.log
  step experiment_c1 ec_blocks.sh aggregate.log c1_aggregate.py
  step experiment_c1 ec_blocks.sh cleanup.log c1_cleanup.py
}

stage_c2() {  # Experiment C2: cold start at 1M (finding 24)
  stage_preflight
  step experiment_c2 c2_blocks.sh phase1_blocks.log c2_phase1.py blocks
  step experiment_c2 c2_aws.sh phase1_s3vectors.log c2_phase1.py s3vectors
  step experiment_c2 c2_aws.sh phase1_lancedb.log c2_phase1.py lancedb
  local out="$HERE/experiment_c2/results_$RESULTS_TAG"
  echo "=== $(ts) c2 phase 2: s3vectors + lancedb concurrently (as in August)"
  ( cd "$HERE/experiment_c2" && ./c2_aws.sh c2_driver.py s3vectors > "$out/phase2_s3vectors.log" 2>&1 ) &
  local p1=$!
  ( cd "$HERE/experiment_c2" && ./c2_aws.sh c2_driver.py lancedb > "$out/phase2_lancedb.log" 2>&1 ) &
  local p2=$!
  wait $p1 || { echo "STOP: c2 s3vectors driver failed"; exit 1; }
  wait $p2 || { echo "STOP: c2 lancedb driver failed"; exit 1; }
  step experiment_c2 c2_blocks.sh phase2_blocks.log c2_driver.py blocks
  step experiment_c2 c2_aws.sh aggregate.log c2_aggregate.py
  # 1M post-ingest checks (fix list v2 F1/F35), while C2's 1M indexes still exist.
  # LanceDB runs in scripts/fixes/.venv (lancedb 0.33.0); the others in harness/.venv.
  local fx="$HERE/../scripts/fixes" chk="$HERE/experiment_c2/results_$RESULTS_TAG"
  echo "=== $(ts) c2: 1M post-ingest checks"
  bash "$fx/run_checks.sh" postingest s3vectors blocks --dataset deep_1m > "$chk/postingest_1m_s3v_blocks.log" 2>&1 \
    || { echo "STOP: 1M post-ingest checks failed"; tail -5 "$chk/postingest_1m_s3v_blocks.log"; exit 1; }
  PY="$fx/.venv/bin/python" bash "$fx/run_checks.sh" postingest lancedb --dataset deep_1m > "$chk/postingest_1m_lancedb.log" 2>&1 \
    || { echo "STOP: 1M LanceDB post-ingest check failed"; tail -5 "$chk/postingest_1m_lancedb.log"; exit 1; }
  for a in blocks s3vectors lancedb; do
    launcher=c2_aws.sh; [ "$a" = blocks ] && launcher=c2_blocks.sh
    step experiment_c2 $launcher cleanup_$a.log c2_cleanup.py $a
  done
}

stage_replicate() {  # D14 replicate (approved 20 Oct): stage B, then the C3 Blocks c1 and S3 Vectors c1/c4
  # cells on the retained 10M indexes, with their recall gates. LanceDB 10M is skipped (2.3 h; reproduced
  # within 0.1 s in August). Same settings as the 2026-10-ec2 cells; run with a new RESULTS_TAG.
  stage_b
  stage_preflight
  step experiment_c3 c3_blocks.sh phase2_blocks.log c3_phase2.py blocks --skip-ingest --recall-n 50
  step experiment_c3 c3_aws.sh phase2_s3vectors.log c3_phase2.py s3vectors --skip-ingest --recall-n 50
  local t; t=$(date +%s)
  step experiment_c3 c3_blocks.sh sweep_blocks_c1.run.log c3_sweep.py blocks 1 --duration 800 --warmup 1 --ts "$t"
  step experiment_c3 c3_aws.sh sweep_s3vectors_c1.run.log c3_sweep.py s3vectors 1 --duration 330 --warmup 5 --ts "$t"
  step experiment_c3 c3_aws.sh sweep_s3vectors_c4.run.log c3_sweep.py s3vectors 4 --duration 85 --warmup 5 --ts "$t"
}

stage_c3() {  # Experiment C3: 10M three-way on the resident 10M data (finding 25)
  stage_preflight
  step experiment_c3 c3_blocks.sh phase2_blocks.log c3_phase2.py blocks --skip-ingest --recall-n 50
  step experiment_c3 c3_aws.sh phase2_s3vectors.log c3_phase2.py s3vectors --skip-ingest --recall-n 50
  stage_c3_rest
}

stage_c3_rest() {  # C3 from the LanceDB gate on (restart point after a LanceDB failure)
  stage_preflight
  step experiment_c3 c3_aws.sh phase2_lancedb.log c3_phase2.py lancedb --skip-ingest --recall-n 50
  stage_c3_sweep
}

stage_c3_sweep() {  # C3 sweep + aggregate (restart point after a sweep failure)
  stage_preflight
  step experiment_c3 c3_run_sweep.sh sweep_ALL.log
  step experiment_c3 c3_aws.sh aggregate.log c3_aggregate.py
}

stage_s3v_rate() {  # S3 Vectors 10M ingest rate into a separate index (~5.6 h, ~$0.77)
  stage_preflight
  step experiment_c3 c3_aws.sh s3v_ingest_rate.log c3_s3v_ingest_rate.py --delete
}

[ $# -ge 1 ] || { echo "usage: RESULTS_TAG=... $0 preflight|b|c1|c2|c3|c3_rest|c3_sweep|s3v_rate|b_lancedb_c10|blocks_10m_rebuild|lancedb_10m_ingest|c2_serial|b_blocks|c1_redo|c1_resume|c2_blocks ..."; exit 2; }
for s in "$@"; do "stage_$s"; done
echo "=== $(ts) done: $*"
