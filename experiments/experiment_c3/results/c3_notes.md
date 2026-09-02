# Experiment C3 — running notes for the deliverable (findings/25)

These are observations to fold into findings/25_three_way_10m.md. Written in
Simplified Technical English.

## Methodology observations

### 1. Live ingest progress by binary search on ordered keys
S3 Vectors gives no live vector count. `get_index` returns no count field, and
there is no S3 Vectors CloudWatch namespace. But the corpus IDs are sequential
(0..N-1) and the ingest runs in file order, so the loaded set is always the
prefix [0, k). We found k by binary-searching the largest key that exists, using
`get_vectors` with `returnData=False`. This needs about 24 API calls for 10M
(log2(10M) ~= 23.3) and costs a few cents. It gives an exact count, not an
estimate.

How to reuse: any adapter with ordered integer IDs and an in-order ingest can use
the same probe to measure progress and rate mid-run. Two counts a fixed time
apart give the instantaneous rate and a real ETA.

### 2. S3 Vectors ingest rate ramps before steady state
The S3 Vectors ingest rate is not constant. It starts slow and climbs to a
steady state. On this experiment the steady-state rate was about 1045
vectors/second, measured over a 96-second window near the mid-point. The average
rate over the first ~4.5 hours was only about 366 vectors/second, because the
slow start dragged it down.

Why it matters: a short ingest (for example the 1M baseline in Experiment B) runs
mostly in the slow-start region, so its average rate is much lower than the
asymptotic rate a long ingest reaches. Extrapolating a 1M average straight to 10M
over-predicts the time. The 1M S3 Vectors ingest (4114s) implied ~11.4 hours for
10M by linear scaling; the real steady-state rate put it near 5.5 hours. Anyone
sizing a fresh-corpus or bursty ingest should expect this ramp.

### 3. Client-side network contention (separate from the C2 sequencing rule)
LanceDB Phase 2 crashed with InvalidSignatureException at 20:01 UTC on
2026-08-23. A LanceDB bulk insert sends each batch of 2000 vectors inline in the
Lambda invoke payload (~3.5MB per call). While S3 Vectors was also ingesting from
the same client, the shared uplink slowed the LanceDB upload enough that the
request outran the 5-minute SigV4 signature window. The service was healthy; the
bottleneck was the single client's uplink.

This is a different finding from the C2 operational rule ("run adapters
sequentially"). C2's rule was about fair latency measurement. This one is about a
hard failure: one client driving several managed serverless vector services at
once becomes network-bound at the client, not at any service. The fix is to run
the heavy inline-payload ingest (LanceDB) alone.

### 4. LanceDB exact flat KNN query time scales linearly with corpus size
LanceDB (naive serverless baseline) does exact flat KNN: each query scans the
whole table. Measured query time:
- 1M (Experiment B): ~5.1s per query.
- 10M (this run): ~54s per query (probe 55.8s; gate queries ~51s each,
  server_ms ~= 53,370). The Lambda reads the ~3.8GB table from S3 and scans all
  10M vectors per query.
This is a clean 10M scaling-shape finding: per-query latency rose ~10x (5.1s ->
54s) for a 10x corpus, so it scales about linearly. There is no ANN index, so
the whole table is read per query. That is the exact-scan trade-off: perfect
recall (100% at the gate) in exchange for linear latency scaling. Headline
finding, alongside S3 Vectors' slow-start-then-steady-state ingest. It also broke the
recall gate: 20 queries packed into one invoke (~1080s) exceeded the 900s Lambda
read timeout. The gate must send one query per invoke (chunk=1) at 10M.

### Real ingest figures at 10M (for the headline ingest table)
- Blocks:    ingest 17.0s,   cost $0.0426  (build only; corpus already in S3).
- S3 Vectors: ingest 20,111s (5h35m), cost $0.768.
- LanceDB:   ingest 4,512.6s (1h15m), cost $0.05635.
(LanceDB JSON was not written because the gate crashed after ingest; these come
from the log and must be carried into findings/25 by hand.)

## Phase 3 sizing decision (recorded for the manifest)
Asymmetric sizing for asymmetric latency shapes, not asymmetric fairness. Slow
adapters get 50 queries; the fast adapter gets 500. Percentiles are honest at
both sizes because the slow adapters have near-deterministic latency.
- Blocks: 50 queries per cell. A 10M Blocks query is ~16s serial, cap 1, so 500
  would be ~2.2h. Finding 20 showed the warm p95/p50 ratio is tight (~1.2), so
  50 give stable percentiles.
- LanceDB: 50 queries per cell. A 10M query is ~54s (exact flat scan), so 500
  would be ~7.5h on the concurrency-1 cell alone. The earlier "keep 500" rule
  assumed a short per-query time that no longer holds at 10M; 54s is Blocks'
  territory, not S3 Vectors'. Exact-flat latency is near-deterministic, so 50 is
  a well-defended sample size, not a compromise.
- S3 Vectors: 500 queries per cell. Per-query time is still short (~0.65s), so
  the larger sample costs little wall-clock.
- The actual sample size per cell is written to each cell manifest, so the
  aggregator's percentiles are honest about their n. The comparison table notes
  the per-adapter sample size.
