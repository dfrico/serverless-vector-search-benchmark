"""
Experiment B — regenerate the AWS Lithops config for Blocks.

Finding 19 restored ~/.lithops/config to the local MinIO version at cleanup, and
no AWS config was saved. BlocksClientAWS.setup() requires ~/.lithops/config to be
backend=aws_lambda storage=aws_s3. We regenerate ONLY that config by calling
blocks-db-wip's generate_lithops_config directly — no Docker rebuild, no resource
creation (the execution role already exists, so get_or_create just reads it).

We then add runtime_memory=3008 and runtime_timeout=300 under aws_lambda to match
the working finding-19 config (3008 MB is this account's Lambda memory ceiling).

The current localhost config is already backed up as ~/.lithops/config.local
(finding 19, byte-identical). Cleanup restores from it.

Run under harness/.venv with AWS_PROFILE=thesis and NO MinIO env.
"""
import os
import sys

WORKSPACE = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BLOCKS_REPO = os.path.join(WORKSPACE, "references", "blocks-db-wip")
if BLOCKS_REPO not in sys.path:
    sys.path.insert(0, BLOCKS_REPO)



def main():
    if os.environ.get("AWS_ENDPOINT_URL_S3"):
        raise SystemExit("AWS_ENDPOINT_URL_S3 set — refusing (would target MinIO).")
    import boto3
    acct = boto3.client("sts").get_caller_identity()["Account"]
    if acct != os.environ.get("AWS_ACCOUNT_ID", acct):
        raise SystemExit(f"unexpected account {acct} (AWS_ACCOUNT_ID)")
    BUCKET = f"thesis-blocks-{acct}"

    from vectordb.infra.setup import generate_lithops_config
    generate_lithops_config("us-east-1", BUCKET, "blocks-db-runtime")

    cfgpath = os.path.expanduser("~/.lithops/config")
    with open(cfgpath) as f:
        text = f.read()
    if "runtime_memory" not in text:
        marker = "aws_lambda:\n  runtime: blocks-db-runtime\n"
        if marker not in text:
            raise SystemExit("could not find aws_lambda/runtime marker to patch memory")
        text = text.replace(
            marker,
            "aws_lambda:\n  runtime: blocks-db-runtime\n"
            "  runtime_memory: 3008\n  runtime_timeout: 300\n",
        )
        with open(cfgpath, "w") as f:
            f.write(text)
        print("patched runtime_memory=3008 runtime_timeout=300")

    # verify guard 2 will pass
    from lithops.config import default_config
    lcfg = default_config()
    backend = lcfg.get("lithops", {}).get("backend")
    storage = lcfg.get("lithops", {}).get("storage")
    print(f"lithops backend={backend} storage={storage}")
    if backend != "aws_lambda" or storage != "aws_s3":
        raise SystemExit("config is NOT aws_lambda/aws_s3 — Blocks guard 2 would fail")
    print("AWS lithops config OK (Blocks guard 2 will pass)")


if __name__ == "__main__":
    main()
