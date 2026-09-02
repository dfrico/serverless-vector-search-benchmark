# deep_1m — 1,000,000-vector Deep1B sample

Prep dataset for the first real experiment (Blocks recall at 1M).

## Provenance

- Source URL: https://storage.yandexcloud.net/yandex-research/ann-datasets/DEEP/base.10M.fbin
- Source file: references/deep1b/base.10M.fbin (DEEP 10M base, .fbin, 96-dim, L2)
- Sampling: 1,000,000 of 10,000,000 vectors, uniform, without replacement,
  then sorted by original index. Sequential CSV IDs from 0 (NOT the original
  .fbin row index).
- Sampling seed: 42
- Sampling script: scripts/sample_deep1b.py  (re-run to regenerate; `--verify` to re-check)

## Produced file — datasets/deep_1m/vectors_deep_1m.csv

- Row count: 1,000,000
- Byte size: 1,150,131,354
- content_sha256: `aa56f86460774bcaeec4675120c5d07de8000c58a008097abca2e6ccd75d21df`
- Generated (UTC): 2026-08-22T15:05:45Z

## Companion files (for the recall experiment)

- query.public.10K.fbin — 10,000 query vectors, 96-dim float32 (.fbin header
  (10000, 96)).
- groundtruth.public.10K.ibin — top-100 neighbour IDs per query, int32 (.ibin
  header (10000, 100)).

WARNING — the public ground truth references the ORIGINAL 10M/1B corpus row
indices, not this 1M subset's re-numbered IDs. It cannot be used directly for
recall on this sample. The recall experiment must either (a) preserve original
row indices as CSV IDs, or (b) compute fresh ground truth over this 1M sample.
See findings/17_dataset_sampling.md.

## Regenerate

    curl -L 'https://storage.yandexcloud.net/yandex-research/ann-datasets/DEEP/base.10M.fbin' -o references/deep1b/base.10M.fbin
    python scripts/sample_deep1b.py
