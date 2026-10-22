"""Independently verify each deployed Blocks resource via boto3.

Does NOT trust setup's "Setup complete". Prints a table of resource -> state.
Run through run_deploy.sh's env (AWS_PROFILE=thesis, no MinIO endpoint).
"""
import boto3

REGION = "us-east-1"
ACCOUNT = boto3.client("sts").get_caller_identity()["Account"]
BUCKET = f"thesis-blocks-{ACCOUNT}"

s = boto3.Session(region_name=REGION)

print("== STS ==")
ident = s.client("sts").get_caller_identity()
print(f"  account={ident['Account']} arn={ident['Arn']}")
assert ident["Account"] == ACCOUNT, "wrong account!"

print("== S3 bucket ==")
try:
    s.client("s3").head_bucket(Bucket=BUCKET)
    print(f"  {BUCKET}: EXISTS")
except Exception as e:  # noqa: BLE001
    print(f"  {BUCKET}: MISSING ({e})")

print("== DynamoDB table BlocksDB-default ==")
try:
    d = s.client("dynamodb").describe_table(TableName="BlocksDB-default")["Table"]
    print(f"  status={d['TableStatus']} billing={d.get('BillingModeSummary', {}).get('BillingMode')} items={d['ItemCount']}")
except Exception as e:  # noqa: BLE001
    print(f"  MISSING ({e})")

print("== Lambda blocksdb-autoindexer-default ==")
try:
    f = s.client("lambda").get_function(FunctionName="blocksdb-autoindexer-default")["Configuration"]
    print(f"  state={f.get('State')} mem={f['MemorySize']} timeout={f['Timeout']} layers={[l['Arn'].split(':')[-2:] for l in f.get('Layers', [])]}")
except Exception as e:  # noqa: BLE001
    print(f"  MISSING ({type(e).__name__})")

print("== Lambda layer blocksdb-layer-faiss-default ==")
try:
    lv = s.client("lambda").list_layer_versions(LayerName="blocksdb-layer-faiss-default")["LayerVersions"]
    if lv:
        print(f"  latest version={lv[0]['Version']} arn={lv[0]['LayerVersionArn']}")
    else:
        print("  NO VERSIONS")
except Exception as e:  # noqa: BLE001
    print(f"  MISSING ({type(e).__name__})")

print("== IAM roles ==")
iam = s.client("iam")
for role in ("lambdaLithopsExecutionRole", "blocksdb-autoindexer-role-default"):
    try:
        r = iam.get_role(RoleName=role)["Role"]
        print(f"  {role}: EXISTS ({r['Arn']})")
    except Exception as e:  # noqa: BLE001
        print(f"  {role}: MISSING ({type(e).__name__})")

print("== ECR runtime image (lithops_v363_rb26/blocks-db-runtime) ==")
ecr = s.client("ecr")
try:
    repos = ecr.describe_repositories()["repositories"]
    match = [r for r in repos if "blocks-db-runtime" in r["repositoryName"]]
    for r in match:
        imgs = ecr.describe_images(repositoryName=r["repositoryName"]).get("imageDetails", [])
        for im in imgs:
            mb = im.get("imageSizeInBytes", 0) / 1e6
            print(f"  {r['repositoryName']} tags={im.get('imageTags')} size={mb:.1f}MB pushed={im.get('imagePushedAt')}")
    if not match:
        print("  NO blocks-db-runtime repo")
except Exception as e:  # noqa: BLE001
    print(f"  ERROR ({e})")

print("== done ==")
