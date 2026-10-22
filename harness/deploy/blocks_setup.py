"""
Bridge task, Phase 2 — deploy Blocks to real AWS.

Drives blocks-db-wip's `vectordb.infra.run_setup` directly instead of the
`blocks-db` console script, because blocks-db-wip is a read-only clone that is
NOT pip-installed in harness/.venv (it is only added to sys.path). This is
functionally identical to the CLI's `setup` handler (vectordb/cli.py) with no
extra flags: config_overrides={}, create_vector_table=True, build_runtime=True.

run_setup builds the FAISS Lambda layer (Docker), creates the auto-indexer
Lambda + IAM roles + DynamoDB table, logs into ECR, overwrites ~/.lithops/config
with an aws_lambda/aws_s3 config, then builds and pushes the Lithops runtime
image (Docker). The local ~/.lithops/config was backed up to config.local first.

Run via run_deploy.sh (sets AWS_PROFILE=thesis, region, venv bin on PATH, and
deliberately does NOT set AWS_ENDPOINT_URL_S3 — this must hit real AWS, not MinIO).
"""
import os
import sys
import time

WORKSPACE = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BLOCKS_REPO = os.path.join(WORKSPACE, "references", "blocks-db-wip")
if BLOCKS_REPO not in sys.path:
    sys.path.insert(0, BLOCKS_REPO)



def main():
    # Guard: this MUST hit real AWS. Fail loudly if the MinIO redirect is set.
    if os.environ.get("AWS_ENDPOINT_URL_S3"):
        raise SystemExit(
            "AWS_ENDPOINT_URL_S3 is set — refusing to run: setup would target MinIO, "
            "not real AWS. Unset it (do not use the local run_recall env)."
        )

    import boto3
    ident = boto3.client("sts").get_caller_identity()
    region = boto3.Session().region_name or "us-east-1"
    print(f"[deploy] account={ident['Account']} arn={ident['Arn']} region={region}")
    expected = os.environ.get("AWS_ACCOUNT_ID", ident["Account"])
    if ident["Account"] != expected:
        raise SystemExit(f"Unexpected account {ident['Account']}; expected {expected} (AWS_ACCOUNT_ID).")
    BUCKET = f"thesis-blocks-{ident['Account']}"

    from vectordb.infra import run_setup

    t0 = time.time()
    print(f"[deploy] run_setup(bucket={BUCKET}) start")
    run_setup(
        s3_bucket=BUCKET,
        config_overrides={},
        create_vector_table=True,
        build_runtime=True,
    )
    print(f"[deploy] run_setup returned in {time.time() - t0:.1f}s "
          f"(does NOT imply success — verify each resource independently)")


if __name__ == "__main__":
    main()
