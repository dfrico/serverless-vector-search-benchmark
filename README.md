# Serverless vector-search benchmark

This repository holds the benchmark harness and experiment code from my master's
thesis. The thesis compares serverless vector-search systems on the same corpus,
through one client interface, and with the same fairness checks:

- Blocks DB with Lithops (fan-out map/reduce over AWS Lambda and S3)
- AWS S3 Vectors (managed vector index)
- LanceDB on AWS Lambda (exact flat scan inside one container-image Lambda)
- LanceDB on AWS Lambda MicroVMs (the same scan in a long-lived MicroVM; added later)

The datasets are Deep1B samples at 1M and 10M vectors. All runs used real AWS, driven
from an EC2 client in the same region (us-east-1).

## What is here

```
harness/            reusable benchmark code: client interface, adapters, deploy, tests
experiments/        run scripts and recorded results for each experiment (a, b, c1..c3)
                    and rerun_baseline.sh, which runs a whole stage
scripts/            data preparation (ground-truth build, Deep1B sampling)
scripts/analysis/   cost from the platform records (Lambda billed duration, S3 access
                    logs, DynamoDB, S3 Vectors fees) and latency summaries
scripts/ec2/        the EC2 benchmark client: bootstrap, code bundle, remote runs, sync
scripts/fixes/      post-ingest checks and CloudWatch helpers
scripts/microvm/    LanceDB-on-MicroVM image build, spikes and billing readouts
datasets/           README only — how to get the data (no data bytes are shared)
requirements.txt   pinned dependencies for the harness environment
```

See `harness/README.md` for the package layout, the dependency graph, and a
quickstart.

## What is NOT here

- The Blocks reference implementation. The Blocks adapters expect it at
  `references/blocks-db-wip/`. It is a separate public repository, not included
  here. Clone it there before you use the Blocks adapters:
  https://github.com/Oct-HI/blocks-db-wip
  ```sh
  git clone https://github.com/Oct-HI/blocks-db-wip references/blocks-db-wip
  ```
- Dataset bytes. Only `datasets/**/README.md` is tracked. The prep scripts under
  `scripts/` build the samples and the ground truth.
- AWS credentials and Lithops config. Provide your own (see below).

## Setup

```sh
uv venv --python 3.10 harness/.venv && uv pip install -r requirements.txt
```

The package is not pip-installed. Put the repo root on `sys.path`, then import
(see `harness/README.md`).

## Running against AWS

The AWS adapters need real AWS credentials and no local MinIO endpoint. Set:

```sh
export AWS_PROFILE=<your profile>
```

The scripts look up the account ID with STS at run time and build the bucket names
from it (`thesis-blocks-<account id>`). If you also set `AWS_ACCOUNT_ID`, the deploy
and setup scripts refuse to run against any other account. These operations create
real AWS resources and cost real money. Read each script before you run it.

## Notes on the recorded results

Each experiment writes to `results_<tag>/`. The tags are:

- `2026-10-ec2`: the run the thesis reports (17–18 October 2026).
- `2026-10-rep1`, `2026-10-rep2`, ...: replicates of experiments B and C3 on later
  days, with the same code, to show the between-day spread.
- `2026-10-20-poll`: a check of the Lithops status-polling interval on Blocks latency.
- `2026-10-mvm`: the LanceDB-on-MicroVM experiments (22–24 October 2026). The thesis reports
  their cost trace (`experiment_c2/results_2026-10-mvm/e3_trace_*`).
- `2026-10-mvm6`: the MicroVM warm-latency sweeps (under `experiment_b` and `experiment_c3`)
  and launch/resume runs (under `experiment_c2`) repeated on the final server, which writes
  each reply in one send (25–26 October 2026). The thesis reports these. The MicroVM spikes,
  image builds and billing readouts are under `scripts/microvm/results/`.

An earlier run (August 2026, from a laptop client) was superseded and is not part of
the thesis; its results were removed from this repository. Account ids, local paths
and the client hostname in the records were replaced with the placeholders
`<AWS_ACCOUNT_ID>`, `<REPO_ROOT>`, `<HOME>` and `<EC2_HOST>`.

## Reproducibility

Each run writes a manifest with a corpus fingerprint (`content_sha256`), the git
commit, and the workload configuration. The manifest is how a run shows that all
four systems searched the same corpus file.

## License

MIT. See `LICENSE`.
