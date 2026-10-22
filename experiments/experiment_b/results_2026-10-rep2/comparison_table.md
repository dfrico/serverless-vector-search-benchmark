# Experiment B — three-way comparison at 1M (deep_1m, top_k=10)

Fairness: corpus sha256 identical across all 9 cells: yes (aa56f86460774bca...). metric = l2 across all cells: yes.

Latency is harness-side submit->ack (OpResult.latency_s), seconds. Throughput is realised ok-ops/second (closed-loop; nominal was 100 ops/s). Cost is an AWS estimate: Lambda GB-s + request fees (Blocks/LanceDB) or QueryVectors request fees (S3 Vectors).

| adapter | conc (cfg/eff) | n | throughput ops/s | p50 s | p95 s | p99 s | mean s | cost/query $ | cost/1k q $ |
|---|---|---|---|---|---|---|---|---|---|---|
| blocks | 1/1 | 15 | 0.10 | 10.060 | 10.244 | 10.255 | 10.060 | 0.0004634 | 0.4634 |
| blocks | 4/1 | 15 | 0.10 | 10.031 | 10.354 | 10.483 | 10.089 | 0.0004636 | 0.4636 |
| blocks | 10/1 | 15 | 0.10 | 10.033 | 10.200 | 10.268 | 10.060 | 0.0004633 | 0.4633 |
| s3vectors | 1/1 | 219 | 4.85 | 0.214 | 0.356 | 0.461 | 0.206 | 0.0000025 | 0.0025 |
| s3vectors | 4/4 | 1806 | 40.02 | 0.065 | 0.238 | 0.328 | 0.100 | 0.0000025 | 0.0025 |
| s3vectors | 10/4 | 2705 | 60.04 | 0.062 | 0.082 | 0.166 | 0.067 | 0.0000025 | 0.0025 |
| lancedb | 1/1 | 24 | 0.20 | 5.172 | 5.686 | 5.906 | 5.101 | 0.0001390 | 0.1390 |
| lancedb | 4/4 | 96 | 0.79 | 5.118 | 5.608 | 5.696 | 5.054 | 0.0001377 | 0.1377 |
| lancedb | 10/10 | 240 | 1.93 | 5.118 | 5.680 | 6.022 | 5.114 | 0.0001377 | 0.1377 |

