"""
Bridge task, Phase 3 — AWS-capable Blocks adapter.

The local `harness/adapters/blocks.py` hard-guards `setup()` to localhost+MinIO only
(lines 229-246): it fails unless AWS_ENDPOINT_URL_S3 points at MinIO and the
Lithops config is backend=localhost/storage=minio. That is correct for the
mature local path and MUST NOT change — the local smoke tests depend on it.

This subclass adds the AWS path with the SMALLEST possible change: it overrides
only `setup()` to swap the two guards, and `_build_config()` to cap Lambda memory
at this account's ceiling. Everything else — bulk_insert_from_path, query_batch,
teardown, capabilities — is inherited unchanged. That is the whole point of
Lithops: the same client code runs on localhost or aws_lambda; only the config
surface differs.

The two swapped guards (aws_mode is always on for this class):
  1. Object store: instead of "AWS_ENDPOINT_URL_S3 must be set (MinIO)", we
     require it to be UNSET (so boto3 hits real S3) and verify boto3 credentials
     resolve to a real AWS account via STS.
  2. Data plane: instead of "Lithops backend=localhost storage=minio", we require
     backend=aws_lambda storage=aws_s3 (what `blocks-db setup` writes).

Memory cap: this account's Lambda per-function memory quota is 3008 MB (a
CreateFunction ValidationException at 10240 during deploy proved it). The local
adapter's _build_config asks for 8192 MB (index_mem / search_map_mem). We cap
those at 3008 here so nothing in the AWS path requests more than the account
allows. Blocks' FunctionExecutor() reads runtime_memory from ~/.lithops/config
(also set to 3008), so both surfaces agree.

Usage (harness/.venv + AWS_PROFILE=thesis, NO MinIO env):
    c = BlocksClientAWS(bucket="thesis-blocks-<account id>",
                        vectors_csv_path="datasets/.../vectors_deep_100k.csv")
    c.setup("deep_100k", dimensions=96, top_k_default=10)
    c.bulk_insert_from_path(path, build_index=True)
    results, timings = c.query_batch(queries, top_k=10)
"""
from __future__ import annotations

import os
import sys

from harness.adapters.blocks import BlocksClient, VectorDBClient

# This account's Lambda per-function memory ceiling (Service Quotas). Requesting
# more fails CreateFunction with a ValidationException.
_LAMBDA_MEMORY_CAP_MB = 3008


class BlocksClientAWS(BlocksClient):
    """BlocksClient that drives real AWS Lambda + S3 instead of localhost + MinIO."""

    def __init__(self, bucket: str, *, vectors_csv_path=None, region: str = "us-east-1"):
        super().__init__(bucket, vectors_csv_path=vectors_csv_path)
        self.region = region
        self.aws_mode = True

    def _build_config(self) -> dict:
        """Same as the parent, but cap the memory asks at the account ceiling."""
        cfg = super()._build_config()
        for key in ("index_mem", "search_map_mem", "search_reduce_mem"):
            if cfg.get(key, 0) > _LAMBDA_MEMORY_CAP_MB:
                cfg[key] = _LAMBDA_MEMORY_CAP_MB
        return cfg

    def setup(
        self,
        dataset_name: str,
        *,
        dimensions: int,
        top_k_default: int = 10,
        num_blocks=None,
        partitions_per_block=None,
        n_probe=None,
        blocks_per_task=None,
        create_if_missing: bool = True,
    ) -> None:
        # --- Guard 1 (swapped): object store must be REAL AWS S3, not MinIO. ---
        if os.environ.get("AWS_ENDPOINT_URL_S3"):
            raise RuntimeError(
                "AWS_ENDPOINT_URL_S3 is set — that redirects boto3 to MinIO. "
                "The AWS adapter needs real S3: unset it (do not use the local "
                "run_recall/MinIO env)."
            )
        import boto3
        try:
            ident = boto3.client("sts").get_caller_identity()
        except Exception as e:  # noqa: BLE001
            raise RuntimeError(
                f"Could not resolve AWS credentials via STS ({e}). Set AWS_PROFILE "
                f"(e.g. thesis) so boto3 reaches a real account."
            )
        account = ident.get("Account", "")
        if not (account.isdigit() and len(account) == 12):
            raise RuntimeError(f"STS returned a non-AWS account id {account!r}.")
        print(f"[blocks-aws] boto3 -> account {account} ({ident.get('Arn')})")

        # --- Guard 2 (swapped): Lithops must be aws_lambda + aws_s3. ---
        from lithops.config import default_config
        lcfg = default_config()
        backend = lcfg.get("lithops", {}).get("backend")
        storage = lcfg.get("lithops", {}).get("storage")
        if backend != "aws_lambda" or storage != "aws_s3":
            raise RuntimeError(
                f"~/.lithops/config must be backend=aws_lambda storage=aws_s3 for "
                f"the AWS adapter; got backend={backend!r} storage={storage!r}. "
                f"Run the Blocks deploy (blocks-db setup) first."
            )

        # --- Tail: identical to the parent's setup(), unchanged. ---
        self.dataset_name = dataset_name
        self.dimensions = dimensions
        self.top_k_default = top_k_default
        self.num_blocks = num_blocks or self._DEFAULT_NUM_BLOCKS
        self.partitions_per_block = partitions_per_block or self._DEFAULT_PARTITIONS_PER_BLOCK
        self.n_probe = n_probe or self._DEFAULT_N_PROBE
        self.blocks_per_task = blocks_per_task or self.num_blocks

        # Control-plane client (boto3 -> real S3; no endpoint override).
        self._client = VectorDBClient(bucket=self.bucket)

        if self._index_exists():
            self._hydrate_params_from_config()
        elif not create_if_missing:
            raise RuntimeError(
                f"No index found for dataset '{dataset_name}' and create_if_missing=False."
            )

        self._ready = True

    def teardown(self, *, drop_data: bool = False) -> None:
        """Delete this dataset's S3 objects on real AWS, then release handles.

        The parent BlocksClient.teardown only deletes when AWS_ENDPOINT_URL_S3 is
        set — a MinIO-only path — so on real AWS it is a no-op (finding 20). This
        override deletes directly via boto3 with no MinIO guard.

        With drop_data=True it removes, for this dataset:
          - vectors_{dataset}.csv and true_neighbours_{dataset}.csv (bucket root),
          - the indexes/{dataset}/ prefix (all block .ann files + config.json),
          - the queries_{dataset}_* staging objects (bucket root),
          - the reduce/ prefix (shared orchestrator intermediates, finding 11).

        The reduce/ prefix is NOT dataset-namespaced (the orchestrator writes a
        flat reduce/ shared across datasets), so this clears all reduce artifacts.
        That matches the parent contract and is safe when no run is in flight.

        Idempotent: absent keys are not an error. Logs the deleted key count for
        an audit trail. With drop_data=False it only releases local handles, to
        match the parent contract.
        """
        if drop_data and self.dataset_name:
            import boto3

            ds = self.dataset_name
            s3 = boto3.client("s3", region_name=self.region)
            paginator = s3.get_paginator("list_objects_v2")

            # (prefix, exact) — exact keys match only themselves, not siblings
            # that merely share the string prefix.
            targets = [
                (f"vectors_{ds}.csv", True),
                (f"true_neighbours_{ds}.csv", True),
                (f"indexes/{ds}/", False),
                (f"queries_{ds}_", False),
                ("reduce/", False),
            ]
            keys = set()
            for prefix, exact in targets:
                for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                    for obj in page.get("Contents", []):
                        if exact and obj["Key"] != prefix:
                            continue
                        keys.add(obj["Key"])

            ordered = sorted(keys)
            for i in range(0, len(ordered), 1000):  # delete_objects caps at 1000
                batch = [{"Key": k} for k in ordered[i:i + 1000]]
                s3.delete_objects(Bucket=self.bucket, Delete={"Objects": batch})
            print(f"[blocks-aws] teardown deleted {len(ordered)} S3 objects for "
                  f"dataset '{ds}' (bucket {self.bucket})")

        self._client = None
        self._ready = False
