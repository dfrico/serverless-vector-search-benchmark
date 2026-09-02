# Experiment B — three-way comparison at 1M (deep_1m, top_k=10)

Fairness: corpus sha256 identical across all 9 cells: yes (aa56f86460774bca...). metric = l2 across all cells: yes.

Latency is harness-side submit->ack (OpResult.latency_s), seconds. Throughput is realised ok-ops/second (closed-loop; nominal was 100 ops/s). Cost is an AWS estimate: Lambda GB-s + request fees (Blocks/LanceDB) or QueryVectors request fees (S3 Vectors).

| adapter | conc (cfg/eff) | n | throughput ops/s | p50 s | p95 s | p99 s | mean s | cost/query $ | cost/1k q $ |
|---|---|---|---|---|---|---|---|---|---|---|
| blocks | 1/1 | 11 | 0.07 | 13.664 | 16.428 | 16.448 | 13.867 | 0.0004768 | 0.4768 |
| blocks | 4/1 | 11 | 0.07 | 13.933 | 16.298 | 16.596 | 13.954 | 0.0004801 | 0.4801 |
| blocks | 10/1 | 11 | 0.07 | 13.795 | 15.556 | 16.327 | 13.814 | 0.0004741 | 0.4741 |
| s3vectors | 1/1 | 172 | 3.82 | 0.251 | 0.390 | 0.417 | 0.262 | 0.0000025 | 0.0025 |
| s3vectors | 4/4 | 861 | 19.02 | 0.173 | 0.332 | 0.405 | 0.209 | 0.0000025 | 0.0025 |
| s3vectors | 10/4 | 999 | 22.10 | 0.171 | 0.240 | 0.325 | 0.180 | 0.0000025 | 0.0025 |
| lancedb | 1/1 | 24 | 0.20 | 5.125 | 5.345 | 5.760 | 5.034 | 0.0001339 | 0.1339 |
| lancedb | 4/4 | 95 | 0.76 | 5.159 | 5.807 | 6.705 | 5.192 | 0.0001353 | 0.1353 |
| lancedb | 10/10 | 238 | 1.90 | 5.174 | 5.723 | 6.143 | 5.149 | 0.0001346 | 0.1346 |

