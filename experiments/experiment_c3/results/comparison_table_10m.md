# Experiment C3 — three-way comparison at 10M (deep_10m, top_k=10)

Fairness: corpus sha256 identical across all 6 cells: yes (72e0840f1f4605ac...). metric = l2 across all cells: yes.

Latency is harness-side submit->ack (OpResult.latency_s), seconds. Throughput is realised ok-ops/second (closed-loop). Cost is the BILLED AWS estimate (finding 22): Blocks sums per-task Lambda GB-s over its 20 map tasks/query; LanceDB is server_ms x 2.048GB; S3 Vectors is one QueryVectors request.

| adapter | conc (cfg/eff) | n | throughput ops/s | p50 s | p95 s | p99 s | mean s | cost/query $ | cost/1k q $ |
|---|---|---|---|---|---|---|---|---|---|
| s3vectors | 1/1 | 736 | 2.23 | 0.459 | 0.583 | 0.652 | 0.449 | 0.0000025 | 0.0025 |
| s3vectors | 4/4 | 1044 | 12.24 | 0.349 | 0.478 | 0.521 | 0.326 | 0.0000025 | 0.0025 |
| lancedb | 1/1 | 54 | 0.02 | 51.452 | 51.975 | 52.596 | 51.411 | 0.0013994 | 1.3994 |
| lancedb | 4/4 | 56 | 0.08 | 51.362 | 51.914 | 52.383 | 51.308 | 0.0013967 | 1.3967 |
| lancedb | 10/10 | 60 | 0.19 | 51.327 | 55.462 | 56.018 | 51.900 | 0.0014043 | 1.4043 |
| blocks | 1/1 | 62 | 0.08 | 13.117 | 15.145 | 15.557 | 13.072 | 0.0014754 | 1.4754 |

