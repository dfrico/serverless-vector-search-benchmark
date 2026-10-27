# harness

The benchmark harness for the thesis. It compares four serverless vector-search
systems — Blocks, S3 Vectors, LanceDB-on-Lambda and LanceDB on Lambda MicroVMs — on
the same corpus, through one abstract client interface, and writes comparable results.

This package holds the reusable code only. The experiments that drive it live in
`experiments/`. Datasets live in `datasets/`. The external Blocks reference repo
lives in `references/blocks-db-wip/`.

## Layout

```
harness/
  client.py            abstract BenchmarkVectorClient + ClientCapabilities
  workload.py          workload generator, WorkloadConfig, OpResult
  sink.py              JSONL result sink + run manifest
  aggregate.py         RunAggregator — rolls raw results into per-cell metrics
  timings_parsers.py   per-adapter timing parsers used by aggregate
  dataset_hash.py      corpus fingerprint (content_sha256) — the fairness anchor
  adapters/
    blocks.py          BlocksClient — local Blocks over MinIO
    blocks_aws.py      BlocksClientAWS — Blocks on real S3 + AWS Lambda
    s3vectors.py       S3VectorsClient — AWS S3 Vectors
    lancedb_lambda.py  LanceDBLambdaClient — LanceDB inside one AWS Lambda
    lancedb_microvm.py LanceDBMicroVMClient — LanceDB in a stateful Lambda MicroVM
  deploy/
    blocks_setup.py    one-time Blocks infra setup on AWS
    verify_infra.py    check the AWS infra is present
    lancedb_build_deploy.py  build + deploy the LanceDB Lambda image
    microvm/           MicroVM image: HTTP server (query + lifecycle hooks) and Dockerfile
  tests/               smoke tests + teardown self-test (one per adapter)
```

## Dependency graph

`client` is the base. Everything points inward to it.

```
client  <-- workload  <-- sink
client  <-- adapters/blocks     <-- adapters/blocks_aws
client  <-- adapters/s3vectors
client  <-- adapters/lancedb_lambda
client  <-- adapters/lancedb_microvm
dataset_hash  <-- adapters/{blocks, s3vectors, lancedb_lambda, lancedb_microvm}
timings_parsers  <-- aggregate
```

`adapters/blocks` also adds the external `references/blocks-db-wip` repo to
`sys.path`, so importing any Blocks adapter makes the reference repo importable.

## Quickstart

```sh
uv venv --python 3.10 harness/.venv && uv pip install -r requirements.txt
```

The package is not pip-installed. Put the repo root on `sys.path`, then import.

```python
import os, sys
ROOT = "<REPO_ROOT>"
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from harness.adapters.blocks import BlocksClient

client = BlocksClient(bucket="lithops-data", vectors_csv_path="vectors.csv")
client.setup("deep_100k", dimensions=96, top_k_default=10, create_if_missing=True)
results, timings = client.query_batch([[0.1] * 96], top_k=10)
print(results[0])   # [(vector_id, distance), ...]
```

The AWS adapters (`blocks_aws`, `s3vectors`, `lancedb_lambda`, `lancedb_microvm`) need real AWS
credentials (`AWS_PROFILE=thesis`) and no MinIO endpoint env. The Blocks adapters
need the Blocks reference repo present at `references/blocks-db-wip/`.

The MicroVM adapter does not ingest: the table is built by `scripts/microvm/make_table.py`
and baked into the MicroVM image by `scripts/microvm/build_image.py`, so the snapshot holds
the loaded table. The adapter then launches a MicroVM from that image (or attaches to a
running one) and queries it over HTTPS. `build_for_dataset()` is the constructor the
experiments use. The contract check is `harness/tests/smoke_lancedb_microvm.py`, run from
the EC2 client.

## Reorganization note (2026-08-25)

This package was consolidated on 2026-08-25 from an earlier day-by-day spike
layout. Every module moved with `git mv`, so its history follows. Code moved into
`harness/`; the experiments moved into `experiments/`.

The `findings/` and `context/` files are historical records. They still name the
old pre-reorganization paths on purpose. Do not read those paths as current.

One consolidated virtual environment (`harness/.venv`, built with uv) now runs
the whole harness and all experiments. The smoke tests and `smoke_three_way.py`
write their output under `harness/tests/results/`; `run_one.py` moved into
`harness/tests/`. The per-experiment launchers (`experiments/experiment_*/*.sh`)
point at `harness/.venv`.
