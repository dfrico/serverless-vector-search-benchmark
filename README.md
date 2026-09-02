# Serverless vector-search benchmark

This repository holds the benchmark harness and experiment code from my master's
thesis. The thesis compares three serverless vector-search systems on the same
corpus, through one client interface, and with the same fairness checks:

- Blocks DB with Lithops (fan-out map/reduce over AWS Lambda and S3)
- AWS S3 Vectors (managed vector index)
- LanceDB on AWS Lambda (exact flat scan inside one container-image Lambda)

The datasets are Deep1B samples at 1M and 10M vectors. All runs used real AWS.

## What is here

```
harness/       reusable benchmark code: client interface, adapters, deploy, tests
experiments/   the run scripts and recorded results for each experiment (a, b, c1..c3)
scripts/       data preparation (ground-truth build, Deep1B sampling)
datasets/      README only — how to get the data (no data bytes are shared)
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
export AWS_ACCOUNT_ID=<your 12-digit AWS account id>
export AWS_PROFILE=<your profile>
```

The deploy and smoke scripts read `AWS_ACCOUNT_ID` to build the bucket name
(`thesis-blocks-<AWS_ACCOUNT_ID>`). These operations create real AWS resources and
cost real money. Read each script before you run it.

## Notes on the recorded results

The files under `experiments/**/results/` are the recorded outputs from the thesis
runs. Account ids and local file paths in those records were replaced with the
placeholders `<AWS_ACCOUNT_ID>`, `<REPO_ROOT>`, and `<HOME>`.

## Reproducibility

Each run writes a manifest with a corpus fingerprint (`content_sha256`), the git
commit, and the workload configuration. The manifest is how a run proves that all
three systems searched the same corpus bytes.

## License

MIT. See `LICENSE`.
