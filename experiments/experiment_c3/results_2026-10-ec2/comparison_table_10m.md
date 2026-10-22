# Experiment C3 — three-way comparison at 10M (deep_10m, top_k=10)

Fairness: corpus sha256 identical across all 6 cells: yes (72e0840f1f4605ac...). metric = l2 across all cells: yes.

Latency is harness-side submit->ack (OpResult.latency_s), seconds. Throughput is realised ok-ops/second (closed-loop). Cost is the BILLED AWS estimate (finding 22): Blocks sums per-task Lambda GB-s over its 20 map tasks/query; LanceDB is server_ms x 2.048GB; S3 Vectors is one QueryVectors request.

| adapter | conc (cfg/eff) | n | throughput ops/s | p50 s | p95 s | p99 s | mean s | cost/query $ | cost/1k q $ |
|---|---|---|---|---|---|---|---|---|---|
| s3vectors | 1/1 | 1057 | 3.20 | 0.325 | 0.463 | 0.549 | 0.312 | 0.0000025 | 0.0025 |
| s3vectors | 4/4 | 1785 | 20.96 | 0.208 | 0.341 | 0.382 | 0.191 | 0.0000025 | 0.0025 |
| lancedb | 1/1 | 54 | 0.02 | 51.365 | 51.959 | 52.547 | 51.357 | 0.0014020 | 1.4020 |
| lancedb | 4/4 | 56 | 0.08 | 51.394 | 51.852 | 52.559 | 51.331 | 0.0014013 | 1.4013 |
| lancedb | 10/10 | 60 | 0.19 | 51.462 | 55.960 | 56.655 | 52.059 | 0.0014108 | 1.4108 |
| blocks | 1/1 | 125 | 0.16 | 6.221 | 8.132 | 9.254 | 6.443 | 0.0013033 | 1.3034 |

