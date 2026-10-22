#!/usr/bin/env bash
# Laptop side: package the benchmark code for the EC2 client and upload it.
#
#   bash scripts/ec2/make_bundle.sh            # prints the S3 key it uploaded
#
# The bundle holds the code that produces numbers (harness/, experiments/ without
# results, scripts/ without venvs/results, requirements.txt) and the Blocks clone
# with its .git (so the client can record and verify commit 4916370e). It also
# holds CODE_BUNDLE.json + CODE_BUNDLE.patch: the laptop's git HEAD, dirty flag and
# uncommitted diff, which harness/provenance.py reports on a client without git.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ACCT="$(aws sts get-caller-identity --profile thesis --query Account --output text)"
BUCKET="thesis-blocks-$ACCT"
TS="$(date -u +%Y%m%dT%H%M%SZ)"
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT

cd "$ROOT"
harness/.venv/bin/python - "$STAGE" <<'EOF'
import json, os, sys
sys.path.insert(0, ".")
from harness import provenance as P
stage = sys.argv[1]
prov = P.code_provenance()
prov["bundled_utc"] = __import__("time").strftime("%Y-%m-%dT%H:%M:%SZ", __import__("time").gmtime())
if P.write_patch(os.path.join(stage, "CODE_BUNDLE.patch")):
    prov["patch_file"] = "CODE_BUNDLE.patch"
json.dump(prov, open(os.path.join(stage, "CODE_BUNDLE.json"), "w"), indent=2)
print("bundle provenance:", {k: prov[k] for k in ("git_sha", "dirty", "diff_sha256", "blocks_commit")})
EOF

COPYFILE_DISABLE=1 tar --no-mac-metadata --no-xattrs -czf "$STAGE/bundle.tgz" \
  --exclude='__pycache__' --exclude='.venv' --exclude='*/results*' --exclude='*.pyc' \
  --exclude='harness/tests/tmp_*' \
  harness experiments scripts requirements.txt references/blocks-db-wip \
  -C "$STAGE" CODE_BUNDLE.json $( [ -f "$STAGE/CODE_BUNDLE.patch" ] && echo CODE_BUNDLE.patch )

KEY="client-bundle/bundle-$TS.tgz"
aws s3 cp --profile thesis --region us-east-1 --only-show-errors "$STAGE/bundle.tgz" "s3://$BUCKET/$KEY"
echo "uploaded s3://$BUCKET/$KEY ($(du -h "$STAGE/bundle.tgz" | cut -f1))"
