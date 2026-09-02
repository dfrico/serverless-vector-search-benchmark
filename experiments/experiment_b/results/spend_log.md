# Experiment B — spend log (bottom-up, from measured durations)

Cost Explorer has ~24h latency, so today's spend is estimated bottom-up from
actual ingest times, request counts, and per-query server time. Prices: Lambda
x86 $0.0000166667/GB-s, arm64 $0.0000133334/GB-s, requests $0.20/1M; S3 Vectors
PUT $0.20/GB (128KB min/request), query $2.50/M.

## Phase 0-1 (pre-flight + deploy)
- Budget alarm active; account <AWS_ACCOUNT_ID>, us-east-1, thesis profile.
- LanceDB deploy: IAM/S3/Lambda create = free; ECR image pushed in a prior task.
- Estimate: ~$0.00 today.

## Phase 2 (recall preconditions) — estimate ~$0.30
- Blocks: index build (~200 Lambda-s) + 1M-query job (~1790 GB-s) @3008MB x86  -> ~$0.04
- S3 Vectors: 0.375 GB PutVectors @ $0.20/GB + 1000 QueryVectors @ $2.50/M      -> ~$0.08
- LanceDB: ingest ~1250 server-s + recall ~5200 server-s + ~300s wasted
  (two query timeouts + a timing probe), all @2048MB arm64                      -> ~$0.18
- Matches the pre-phase ~$0.30 estimate (well under 2x). No Phase 3 revision.

## Phase 3 (latency sweep) — pre-phase estimate ~$0.50
- Blocks: 3 runs x (5 warmup + ~6 measured) full-job queries @3008MB x86        -> ~$0.15
- S3 Vectors: 3 runs, hundreds of fast QueryVectors @ $2.50/M                    -> ~$0.01
- LanceDB: 3 runs, ~250 single queries @ ~5.2s each @2048MB arm64               -> ~$0.05
- Rounded up for orchestration overhead                                         -> ~$0.50

### Phase 3 actual (from comparison_table.json cost_total per cell)
- Blocks: 3 cells 0.00524 + 0.00528 + 0.00522 = $0.0157 measured; add ~15
  discarded warmup full-job queries @ ~$0.00048 = ~$0.0072                       -> ~$0.023
- S3 Vectors: 0.00043 + 0.00215 + 0.00250                                        -> ~$0.005
- LanceDB: 0.00321 + 0.01285 + 0.03203                                           -> ~$0.048
- Phase 3 total actual                                                           -> ~$0.076
- Well under the ~$0.50 estimate (the estimate over-counted Blocks orchestration).

## Phase 4 (cleanup) — deletes only, no billable compute                         -> ~$0.00

## Running total: est ~$0.80, actual ~$0.38 of the $25 cap.
