#!/usr/bin/env bash
# EC2 side (Amazon Linux 2023, x86_64, instance role thesis-bench-client):
# install tools, unpack the code bundle, build the venvs, write the Lithops config,
# and rebuild the datasets from the public Deep1B mirror, then verify their hashes
# against the values the August manifests recorded.
#
#   sudo -u ec2-user bash bootstrap.sh <bundle s3 key>
#
# Idempotent: re-running skips finished steps. Logs to ~/bootstrap.log.
set -euo pipefail
KEY="${1:?usage: bootstrap.sh client-bundle/bundle-<ts>.tgz}"
ACCT="$(aws sts get-caller-identity --query Account --output text)"
LITHOPS_USER_ID="${LITHOPS_USER_ID:?set LITHOPS_USER_ID (IAM UserId of the laptop user)}"
HOME_DIR="$HOME"; W="$HOME_DIR/thesis"
exec > >(tee -a "$HOME_DIR/bootstrap.log") 2>&1
echo "=== bootstrap $(date -u +%FT%TZ) bundle=$KEY"

sudo dnf install -y -q git tmux tar gzip >/dev/null
command -v uv >/dev/null || { curl -LsSf https://astral.sh/uv/install.sh | sh >/dev/null; }
export PATH="$HOME_DIR/.local/bin:$PATH"

mkdir -p "$W" && cd "$W"
aws s3 cp --only-show-errors "s3://thesis-blocks-$ACCT/$KEY" bundle.tgz
tar --warning=no-unknown-keyword -xzf bundle.tgz && rm bundle.tgz
git -C references/blocks-db-wip rev-parse --short HEAD

[ -x harness/.venv/bin/python ] || {
  uv venv -q --python 3.10 harness/.venv
  uv pip install -q --python harness/.venv/bin/python -r requirements.txt; }
[ -x scripts/fixes/.venv/bin/python ] || {
  uv venv -q --python 3.10 scripts/fixes/.venv
  uv pip install -q --python scripts/fixes/.venv/bin/python lancedb==0.33.0 pyarrow==17.0.0 numpy==1.26.4 boto3==1.43.79; }
harness/.venv/bin/python -c "import lithops, faiss, numpy; print('lithops', lithops.__version__, 'faiss', faiss.__version__, 'numpy', numpy.__version__)"

# Lithops: same runtime, bucket and role as the laptop. user_id/account_id are
# pinned so the function names (lithops-worker-<last 4 of UserId>-...) match the
# deployed workers; credentials come from the instance role.
mkdir -p "$HOME_DIR/.lithops"
cat > "$HOME_DIR/.lithops/config" <<EOF
lithops:
  backend: aws_lambda
  storage: aws_s3
  log_level: INFO
aws:
  region: us-east-1
aws_lambda:
  runtime: blocks-db-runtime
  execution_role: arn:aws:iam::$ACCT:role/lambdaLithopsExecutionRole
  user_id: $LITHOPS_USER_ID
  account_id: '$ACCT'
aws_s3:
  # Lithops reads storage_bucket (bucket_name is ignored). The laptop never set it, so
  # Lithops fell back to lithops-<region>-<last 4 of UserId> = lithops-us-east-1-rb26;
  # pin that so job data goes to the same bucket from EC2.
  storage_bucket: lithops-us-east-1-rb26
EOF

# Datasets, rebuilt from the public source with the same scripts and seed.
mkdir -p references/deep1b datasets/deep_1m datasets/deep_10m
BASE=https://storage.yandexcloud.net/yandex-research/ann-datasets/DEEP
[ -s references/deep1b/base.10M.fbin ] || curl -sSL "$BASE/base.10M.fbin" -o references/deep1b/base.10M.fbin
for f in query.public.10K.fbin groundtruth.public.10K.ibin; do
  [ -s "datasets/deep_1m/$f" ] || curl -sSL "$BASE/$f" -o "datasets/deep_1m/$f"; done
PY=harness/.venv/bin/python
[ -s datasets/deep_1m/vectors_deep_1m.csv ] || $PY scripts/sample_deep1b.py
[ -s datasets/deep_1m/true_neighbours_deep_1m_1000q.csv ] || $PY scripts/build_ground_truth.py
[ -s datasets/deep_10m/vectors_deep_10m.csv ] || ( cd experiments/experiment_c3 && ../../$PY c3_phase0.py )

echo "=== dataset hashes (expected from the August manifests / laptop files)"
cat > /tmp/expected.sha256 <<'EOF'
aa56f86460774bcaeec4675120c5d07de8000c58a008097abca2e6ccd75d21df  datasets/deep_1m/vectors_deep_1m.csv
72e0840f1f4605ac5d8715422f3d77154b310414e908ce6768b1bcebda0aa849  datasets/deep_10m/vectors_deep_10m.csv
cb32e9e5623b15ba12ccaa703ef769a2b057ccfe4f5bfb77a8d6c494d059fcfb  datasets/deep_1m/queries_deep_1m_1000q.csv
7acb5a8f95fa4308181ef2ae8c4de8e40987fe8e69961893bb988bc9a64d1c5a  datasets/deep_1m/true_neighbours_deep_1m_1000q.csv
cb32e9e5623b15ba12ccaa703ef769a2b057ccfe4f5bfb77a8d6c494d059fcfb  datasets/deep_10m/queries_deep_10m_1000q.csv
46758ad1a4bbee5710f92b6ed007fe9c778c5243b2b65a5b350587dc6af08d59  datasets/deep_10m/true_neighbours_deep_10m_1000q.csv
EOF
sha256sum -c /tmp/expected.sha256
echo "=== bootstrap done $(date -u +%FT%TZ)"
