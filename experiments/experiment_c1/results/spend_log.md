# Experiment C1 — spend log (bottom-up, from measured durations)

Cost Explorer has ~24h latency, so spend is estimated bottom-up from actual
ingest times, request counts, and per-query billed server time. Prices: Lambda
x86 $0.0000166667/GB-s, requests $0.20/1M. Blocks memory 3.008 GB. The honest
per-query charge is the billed model (per-task compute time), not the wall model.

Account <AWS_ACCOUNT_ID>, us-east-1, thesis profile. Budget alarm active at start.

## Phase 1 — baseline ingest + recall gate
- Ingest: index build 16.16s across 16 workers -> $0.003907 (recorded).
- 1000-query recall gate: one batched query job, 36.1s wall -> ~$0.004 (est).
- Subtotal: ~$0.008.

## Phase 2 — sweep (12 cells, 50 queries each)
- 600 measured queries, billed sums per cell (from raw_cost_billed_usd):
  a_np1 $0.00923, a_np3 $0.00915, a_np5 $0.00935, a_np7 $0.00946,
  base $0.00930, c_bpt1 $0.01079, c_bpt2 $0.00752, c_bpt4 $0.00641,
  c_bpt8 $0.00629, b_nw4 $0.00953, b_nw8 $0.00941, b_nw32 $0.00528.
  Total measured queries: $0.1017. Warmups (36 queries): ~$0.007.
- Rebuild ingests (Axis A x4 + Axis B x4), each ~$0.004: $0.0300 (recorded).
- Transient nw4 timeout (first attempt): ingest $0.0044 + one 295s hung map task
  (295 x 3.008 x 0.0000166667) $0.0148 -> ~$0.019. Retry passed at 100% recall.
- Subtotal: ~$0.158.

## Phase 3 — aggregate + cleanup
- Aggregate: local only, $0.
- Cleanup teardown: S3 deletes only, ~$0.
- Subtotal: ~$0.00.

## Total
- ~$0.17 of the $15 cap.
- Largest single ingest: $0.0046 (well under the $0.20 stop line; guard never fired).
- No single operation approached the $5 ask-first line.

## Guards and stops
- INGEST_GUARD_USD = 0.20 per rebuild: never tripped (max $0.0046).
- No operation estimated over $5, so no stop-and-ask was needed.
