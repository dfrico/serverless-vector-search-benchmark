# Experiment C1 — Blocks tuning at 1M (deep_1m, top_k=10)

Baseline anchor: n_probe=10, num_workers=16, blocks_per_task=16 -> recall 100.0%, p50 13.436s, billed $0.000186/query, wall $0.00046218/query.

## Axis A — n_probe (rebuild per value; recall + latency)

| n_probe | recall % | p50 s | p95 s | mean s | billed $/q | wall $/q | ops/s |
|---|---|---|---|---|---|---|---|
| 1 | 83.2 | 13.2821 | 16.5134 | 13.6305 | 0.00018453 | 0.00045421 | 0.0734 |
| 3 | 97.8 | 13.0407 | 16.5216 | 13.4913 | 0.00018304 | 0.00044938 | 0.0741 |
| 5 | 99.4 | 13.3542 | 16.4031 | 13.6354 | 0.00018704 | 0.00045572 | 0.0733 |
| 7 | 100.0 | 13.677 | 17.2359 | 14.0231 | 0.00018924 | 0.00047651 | 0.0713 |
| 10 | 100.0 | 13.436 | 16.6003 | 13.6591 | 0.000186 | 0.00046218 | 0.0732 |

## Axis B — num_workers (ingest-only; cost signal + query flatness)

| num_workers | ingest $ | ingest index_s | recall % | p50 s | billed $/q |
|---|---|---|---|---|---|
| 4 | 0.004308 | 27.284 | 100.0 | 13.5441 | 0.00019054 |
| 8 | 0.004554 | 17.101 | 100.0 | 13.6287 | 0.00018815 |
| 16 | None | None | 100.0 | 13.436 | 0.000186 |
| 32 | 0.004507 | 39.452 | 51.2 | 12.8014 | 0.00010569 |

## Axis C — blocks_per_task (config rewrite; latency/cost trade)

| blocks_per_task | map tasks | recall % | p50 s | p95 s | billed $/q | wall $/q | ops/s |
|---|---|---|---|---|---|---|---|
| 1 | 16.0 | 100.0 | 12.7746 | 523.0385 | 0.00021585 | 0.00330152 | 0.0142 |
| 2 | 8.0 | 100.0 | 10.7363 | 14.7089 | 0.00015044 | 0.00216123 | 0.021 |
| 4 | 4.0 | 100.0 | 10.6518 | 12.2105 | 0.00012821 | 0.00031878 | 0.0925 |
| 8 | 2.0 | 100.0 | 10.52 | 12.3429 | 0.00012588 | 0.00031988 | 0.0925 |
| 16 | 1.0 | 100.0 | 13.436 | 16.6003 | 0.000186 | 0.00046218 | 0.0732 |

## Headline

- Best Blocks cost/query at full recall (>=99.99%): $0.00012588 billed ($0.00031988 wall) at n_probe=10, num_workers=16, blocks_per_task=8.
- Absolute cheapest cell: $0.00010569 billed at recall 51.2% (n_probe=10, bpt=16).
- Experiment B Blocks baseline: $0.000477/query (100% recall).
- S3 Vectors: $2.5e-06/query (97.3% recall). Gap = 190.8x.
- Gap after tuning (best full-recall billed vs S3V): 50.4x.
