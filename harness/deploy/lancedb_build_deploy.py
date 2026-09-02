#!/usr/bin/env python3
"""
Idempotent build + deploy (and teardown) for the LanceDB-on-Lambda baseline
(day 8). Provisions the durable AWS infra the LanceDBLambdaClient adapter drives:

  1. ECR repository   thesis-lancedb-lambda        (holds the container image)
  2. S3 bucket        thesis-lancedb-<account-id>  (LanceDB table storage)
  3. IAM role         thesis-lancedb-lambda-role   (Lambda execution role + S3 access)
  4. Lambda function  thesis-lancedb-lambda        (arm64 container, our handler)

Every step is idempotent: re-running `deploy` rebuilds/pushes the image and
updates the function without erroring on already-existing resources.

Verbs:
  plan     Read-only. Print the resource plan + what exists now. No changes, no cost.
  deploy   Build the image, push to ECR, create/update all resources.
  destroy  Delete ALL of the above (mandatory end-of-day-8 cleanup) and verify.

This script COSTS MONEY / creates idle-billable resources (ECR image storage,
Lambda). Per the day-8 brief, get approval before running `deploy`. Docker must be
running for `deploy` (it builds the image).

Env: AWS_PROFILE / AWS_REGION as usual (defaults region us-east-1). Uses the
`thesis` profile in this project (findings/14).
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time

import boto3
from botocore.exceptions import ClientError

REGION = os.environ.get("AWS_REGION", "us-east-1")
ECR_REPO = "thesis-lancedb-lambda"
IMAGE_TAG = "latest"
ROLE_NAME = "thesis-lancedb-lambda-role"
FUNCTION_NAME = "thesis-lancedb-lambda"
ARCH = "arm64"                     # Graviton; matches the arm64 build host
PLATFORM = "linux/arm64"
MEMORY_MB = 2048                   # more memory == more CPU for LanceDB search
TIMEOUT_S = 120                    # cold start + S3 + exact KNN headroom
BUCKET_PREFIX = "thesis-lancedb"   # bucket == <prefix>-<account-id>

_HERE = os.path.dirname(os.path.abspath(__file__))


def _session():
    return boto3.Session(region_name=REGION)


def _account_id(sess):
    return sess.client("sts").get_caller_identity()["Account"]


def _bucket_name(account_id):
    return f"{BUCKET_PREFIX}-{account_id}"


def _run(cmd, **kw):
    """Run a shell command, streaming output; raise on non-zero."""
    print(f"  $ {' '.join(cmd)}")
    subprocess.run(cmd, check=True, **kw)


# --------------------------------------------------------------------------- #
# existence probes (read-only)                                                #
# --------------------------------------------------------------------------- #
def _ecr_exists(ecr):
    try:
        ecr.describe_repositories(repositoryNames=[ECR_REPO])
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "RepositoryNotFoundException":
            return False
        raise


def _bucket_exists(s3, bucket):
    try:
        s3.head_bucket(Bucket=bucket)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchBucket", "NotFound"):
            return False
        if e.response["Error"]["Code"] == "403":
            # Exists but owned elsewhere — surface loudly.
            raise RuntimeError(f"S3 bucket {bucket} exists but is not accessible (403).")
        raise


def _role_exists(iam):
    try:
        iam.get_role(RoleName=ROLE_NAME)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchEntity":
            return False
        raise


def _function_exists(lam):
    try:
        lam.get_function(FunctionName=FUNCTION_NAME)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            return False
        raise


# --------------------------------------------------------------------------- #
# plan                                                                        #
# --------------------------------------------------------------------------- #
def cmd_plan(sess):
    account = _account_id(sess)
    bucket = _bucket_name(account)
    ecr, s3, iam, lam = (sess.client(s) for s in ("ecr", "s3", "iam", "lambda"))
    print(f"account={account} region={REGION}")
    print("resource plan (idempotent deploy):")
    print(f"  ECR repo   {ECR_REPO:32} exists={_ecr_exists(ecr)}")
    print(f"  S3 bucket  {bucket:32} exists={_bucket_exists(s3, bucket)}")
    print(f"  IAM role   {ROLE_NAME:32} exists={_role_exists(iam)}")
    print(f"  Lambda fn  {FUNCTION_NAME:32} exists={_function_exists(lam)}")
    print(f"  image      {account}.dkr.ecr.{REGION}.amazonaws.com/{ECR_REPO}:{IMAGE_TAG}")
    print(f"  arch={ARCH} memory={MEMORY_MB}MB timeout={TIMEOUT_S}s")
    return 0


# --------------------------------------------------------------------------- #
# deploy                                                                      #
# --------------------------------------------------------------------------- #
def _ensure_ecr(ecr):
    if _ecr_exists(ecr):
        print(f"ECR repo {ECR_REPO} already exists.")
    else:
        ecr.create_repository(repositoryName=ECR_REPO, imageScanningConfiguration={"scanOnPush": False})
        print(f"created ECR repo {ECR_REPO}.")


def _build_and_push(account):
    registry = f"{account}.dkr.ecr.{REGION}.amazonaws.com"
    image_uri = f"{registry}/{ECR_REPO}:{IMAGE_TAG}"
    # docker login to ECR via the CLI's password.
    pw = subprocess.check_output(
        ["aws", "ecr", "get-login-password", "--region", REGION]
    ).decode().strip()
    _run(["docker", "login", "--username", "AWS", "--password-stdin", registry],
         input=pw.encode())
    # --provenance=false --sbom=false: buildx otherwise attaches attestation
    # manifests, producing an image INDEX that Lambda rejects with "image manifest
    # ... media type ... is not supported". Lambda needs a single-platform manifest.
    _run(["docker", "build", "--platform", PLATFORM,
          "--provenance=false", "--sbom=false", "-t", image_uri, _HERE])
    _run(["docker", "push", image_uri])
    return image_uri


def _ensure_bucket(s3, bucket):
    if _bucket_exists(s3, bucket):
        print(f"S3 bucket {bucket} already exists.")
        return
    # us-east-1 rejects a LocationConstraint; other regions require it.
    if REGION == "us-east-1":
        s3.create_bucket(Bucket=bucket)
    else:
        s3.create_bucket(Bucket=bucket,
                         CreateBucketConfiguration={"LocationConstraint": REGION})
    print(f"created S3 bucket {bucket}.")


_TRUST_POLICY = {
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Principal": {"Service": "lambda.amazonaws.com"},
        "Action": "sts:AssumeRole",
    }],
}


def _s3_policy(bucket):
    return {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject",
                       "s3:ListBucket", "s3:GetBucketLocation"],
            "Resource": [f"arn:aws:s3:::{bucket}", f"arn:aws:s3:::{bucket}/*"],
        }],
    }


def _ensure_role(iam, bucket):
    created = False
    if _role_exists(iam):
        print(f"IAM role {ROLE_NAME} already exists.")
    else:
        iam.create_role(
            RoleName=ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(_TRUST_POLICY),
            Description="Execution role for the thesis LanceDB-on-Lambda baseline.",
        )
        created = True
        print(f"created IAM role {ROLE_NAME}.")
    # Basic execution (CloudWatch Logs) + scoped S3 access. Both idempotent.
    iam.attach_role_policy(
        RoleName=ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
    )
    iam.put_role_policy(
        RoleName=ROLE_NAME,
        PolicyName="lancedb-s3-access",
        PolicyDocument=json.dumps(_s3_policy(bucket)),
    )
    role_arn = iam.get_role(RoleName=ROLE_NAME)["Role"]["Arn"]
    if created:
        # New roles take a few seconds to be assumable by Lambda.
        print("waiting 10s for IAM role propagation...")
        time.sleep(10)
    return role_arn


def _ensure_function(lam, image_uri, role_arn):
    if _function_exists(lam):
        print(f"Lambda {FUNCTION_NAME} exists — updating code to {image_uri}.")
        lam.update_function_code(FunctionName=FUNCTION_NAME, ImageUri=image_uri,
                                 Architectures=[ARCH])
        waiter = lam.get_waiter("function_updated")
        waiter.wait(FunctionName=FUNCTION_NAME)
        return
    # create_function may race IAM role propagation; retry a few times.
    last = None
    for attempt in range(6):
        try:
            lam.create_function(
                FunctionName=FUNCTION_NAME,
                PackageType="Image",
                Code={"ImageUri": image_uri},
                Role=role_arn,
                Architectures=[ARCH],
                MemorySize=MEMORY_MB,
                Timeout=TIMEOUT_S,
                Description="Thesis LanceDB-on-Lambda naive-serverless baseline (day 8).",
            )
            print(f"created Lambda {FUNCTION_NAME}.")
            waiter = lam.get_waiter("function_active_v2")
            waiter.wait(FunctionName=FUNCTION_NAME)
            return
        except ClientError as e:
            code = e.response["Error"]["Code"]
            msg = e.response.get("Error", {}).get("Message", "")
            # Only retry the specific "role not yet assumable" race — any other
            # InvalidParameterValueException (e.g. an unsupported image manifest)
            # is a real error and must fail fast, not burn 25s of retries.
            if code == "InvalidParameterValueException" and "cannot be assumed" in msg and attempt < 5:
                last = e
                print(f"  role not assumable yet (attempt {attempt + 1}); retrying in 5s...")
                time.sleep(5)
                continue
            raise
    raise RuntimeError(f"create_function failed after retries: {last}")


def cmd_deploy(sess):
    account = _account_id(sess)
    bucket = _bucket_name(account)
    ecr, s3, iam, lam = (sess.client(s) for s in ("ecr", "s3", "iam", "lambda"))

    print("== 1. ECR repository ==")
    _ensure_ecr(ecr)
    print("== 2. build + push image ==")
    image_uri = _build_and_push(account)
    print("== 3. S3 bucket ==")
    _ensure_bucket(s3, bucket)
    print("== 4. IAM role ==")
    role_arn = _ensure_role(iam, bucket)
    print("== 5. Lambda function ==")
    _ensure_function(lam, image_uri, role_arn)

    print("\nDEPLOY COMPLETE")
    print(f"  function_name : {FUNCTION_NAME}")
    print(f"  image_uri     : {image_uri}")
    print(f"  data_bucket   : {bucket}")
    print(f"  role_arn      : {role_arn}")
    print("\nrun the smoke:  AWS_PROFILE=thesis AWS_REGION=%s python harness/tests/smoke_lancedb.py" % REGION)
    return 0


# --------------------------------------------------------------------------- #
# destroy (mandatory cleanup)                                                 #
# --------------------------------------------------------------------------- #
def _empty_bucket(s3, bucket):
    paginator = s3.get_paginator("list_object_versions")
    to_delete = []
    for page in paginator.paginate(Bucket=bucket):
        for key in ("Versions", "DeleteMarkers"):
            for obj in page.get(key, []):
                to_delete.append({"Key": obj["Key"], "VersionId": obj["VersionId"]})
        if len(to_delete) >= 900:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": to_delete})
            to_delete = []
    if to_delete:
        s3.delete_objects(Bucket=bucket, Delete={"Objects": to_delete})


def cmd_destroy(sess):
    account = _account_id(sess)
    bucket = _bucket_name(account)
    ecr, s3, iam, lam = (sess.client(s) for s in ("ecr", "s3", "iam", "lambda"))
    failures = []

    # 1. Lambda function
    try:
        if _function_exists(lam):
            lam.delete_function(FunctionName=FUNCTION_NAME)
            print(f"deleted Lambda {FUNCTION_NAME}.")
        else:
            print(f"Lambda {FUNCTION_NAME} already absent.")
    except Exception as e:  # noqa: BLE001
        failures.append(f"lambda: {e}")

    # 2. ECR repo (force removes images)
    try:
        if _ecr_exists(ecr):
            ecr.delete_repository(repositoryName=ECR_REPO, force=True)
            print(f"deleted ECR repo {ECR_REPO}.")
        else:
            print(f"ECR repo {ECR_REPO} already absent.")
    except Exception as e:  # noqa: BLE001
        failures.append(f"ecr: {e}")

    # 3. IAM role (detach managed, delete inline, delete role)
    try:
        if _role_exists(iam):
            iam.detach_role_policy(
                RoleName=ROLE_NAME,
                PolicyArn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
            )
            for pol in iam.list_role_policies(RoleName=ROLE_NAME).get("PolicyNames", []):
                iam.delete_role_policy(RoleName=ROLE_NAME, PolicyName=pol)
            iam.delete_role(RoleName=ROLE_NAME)
            print(f"deleted IAM role {ROLE_NAME}.")
        else:
            print(f"IAM role {ROLE_NAME} already absent.")
    except Exception as e:  # noqa: BLE001
        failures.append(f"iam: {e}")

    # 4. S3 bucket (empty then delete)
    try:
        if _bucket_exists(s3, bucket):
            _empty_bucket(s3, bucket)
            s3.delete_bucket(Bucket=bucket)
            print(f"deleted S3 bucket {bucket}.")
        else:
            print(f"S3 bucket {bucket} already absent.")
    except Exception as e:  # noqa: BLE001
        failures.append(f"s3: {e}")

    # 5. verify
    print("\n== verification ==")
    still = {
        "lambda": _function_exists(lam),
        "ecr": _ecr_exists(ecr),
        "iam_role": _role_exists(iam),
        "s3_bucket": _bucket_exists(s3, bucket),
    }
    for k, v in still.items():
        print(f"  {'STILL PRESENT' if v else 'gone'}: {k}")
    if failures or any(still.values()):
        print("\nCLEANUP INCOMPLETE — remaining resources listed above. STOP and inspect.")
        for f in failures:
            print(f"  failure: {f}")
        return 1
    print("\nCLEANUP COMPLETE — all day-8 resources removed.")
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("verb", choices=["plan", "deploy", "destroy"])
    args = ap.parse_args()
    sess = _session()
    if args.verb == "plan":
        return cmd_plan(sess)
    if args.verb == "deploy":
        return cmd_deploy(sess)
    return cmd_destroy(sess)


if __name__ == "__main__":
    sys.exit(main())
