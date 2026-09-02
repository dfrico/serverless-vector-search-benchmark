# Experiment C2 — cold-start table (deep_1m, 1M, top_k=10)

Wall latency is the client-side round trip, in seconds — the primary cold-start metric. Billed cost is the honest per-query Lambda charge (finding 22 model); for a cold query the container init is mostly outside the billed handler time, so billed moves far less than wall.

## Recall gate (Phase 1)

| adapter | recall@10 | bar | pass | ingest cost $ |
|---|---|---|---|---|
| blocks | 100.0% | 99.0% | yes | 0.003899 |
| s3vectors | 98.0% | 90.0% | yes | 0.0768 |
| lancedb | 100.0% | 99.0% | yes | 0.00376 |

## Cold-start table

| adapter | config | n | wall min s | wall median s | wall p95 s | wall max s | billed median $ | cold signal |
|---|---|---|---|---|---|---|---|---|
| blocks | abs_cold | 5 | 14.6573 | 14.8682 | 17.1794 | 17.7217 | 0.00016063 | map spin-up median 2.783s |
| blocks | idle_cold | 5 | 14.6144 | 15.2544 | 16.011 | 16.0341 | 0.00017386 | map spin-up median 2.8984s |
| blocks | warm | 20 | 9.8662 | 10.8586 | 12.8842 | 13.7178 | 0.00014571 | map spin-up median 1.6539s |
| s3vectors | abs_cold | 5 | 0.4487 | 0.754 | 0.7568 | 0.7568 | 2.5e-06 | none (managed) |
| s3vectors | idle_cold | 5 | 0.7279 | 0.7509 | 1.0974 | 1.1667 | 2.5e-06 | none (managed) |
| s3vectors | warm | 20 | 0.1675 | 0.4452 | 0.5175 | 0.5261 | 2.5e-06 | none (managed) |
| lancedb | abs_cold | 5 | 6.118 | 6.2595 | 6.683 | 6.7423 | 9.029e-05 | cold True/False = 5/0 |
| lancedb | idle_cold | 5 | 4.9161 | 6.3144 | 811.6251 | 1005.607 | 7.717e-05 | cold True/False = 5/0 |
| lancedb | warm | 20 | 4.6404 | 5.0996 | 5.5336 | 5.6679 | 0.00013514 | cold True/False = 0/20 |

## Headline: idle-cold vs warm median wall latency

| adapter | abs_cold median s | idle_cold median s | warm median s | idle/warm ratio |
|---|---|---|---|---|
| blocks | 14.8682 | 15.2544 | 10.8586 | 1.405 |
| s3vectors | 0.754 | 0.7509 | 0.4452 | 1.686 |
| lancedb | 6.2595 | 6.3144 | 5.0996 | 1.238 |

