"""
Concrete BlocksClient adapter — wraps blocks-db-wip's VectorDBClient behind the
harness's abstract BenchmarkVectorClient (harness/client.py).

Local-only, no AWS. Honours the two-config-surfaces finding from
findings/06_blocks_local.md:
  - DATA PLANE (index build + map/reduce query workers): Lithops Storage(),
    driven by ~/.lithops/config (backend: localhost, storage: minio).
  - CONTROL PLANE (client init, index discovery, dataset ops): module-level
    boto3.client("s3") with no endpoint_url -> redirected to MinIO via the
    AWS_ENDPOINT_URL_S3 / AWS_*_KEY env vars.

setup() verifies both surfaces are wired before any operation runs. The pending
path is out of scope: every query uses hybrid=False and put_pending stays the
inherited no-op. No DynamoDB is ever touched (bulk_insert builds the index via
ServerlessVectorDB.indexing() + save_index_config directly, bypassing
VectorDBClient.index_dataset which seeds DynamoDB auto-indexer state).

Repos stay read-only: blocks-db-wip is only added to sys.path and imported.
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from typing import Iterable, List, Optional, Sequence, Tuple

# --- locate the external Blocks reference repo (references/blocks-db-wip) ------
# The harness modules come from the consolidated `harness` package (repo root on
# sys.path via the entry-point script). Only blocks-db-wip is an external
# read-only clone, so it alone still needs a manual sys.path insert.
_HERE = os.path.dirname(os.path.abspath(__file__))
_WORKSPACE = os.path.dirname(os.path.dirname(_HERE))  # harness/adapters -> repo root
_BLOCKS_REPO = os.path.join(_WORKSPACE, "references", "blocks-db-wip")
if _BLOCKS_REPO not in sys.path:
    sys.path.insert(0, _BLOCKS_REPO)

import numpy as np

from harness.dataset_hash import fingerprint_csv, unknown_fingerprint

from harness.client import (
    BenchmarkVectorClient,
    ClientCapabilities,
    Hit,
    QueryResults,
    Timings,
    Vector,
)

# Blocks internals (data plane build + control plane discovery/dataset ops)
from vectordb.serverless_vectordb import ServerlessVectorDB
from vectordb.client import VectorDBClient
from vectordb.utils.index_ops import (
    save_index_config,
    load_index_config,
    list_indexes,
    delete_index_configs,
)
from vectordb.utils.dataset_ops import upload_dataset, delete_dataset, delete_vectors_from_dataset


class BlocksClient(BenchmarkVectorClient):
    """BenchmarkVectorClient backed by blocks-db-wip on Lithops localhost + MinIO."""

    # Blocks index defaults (mirror blocks-db-wip/vectordb/config/indexconfig.json,
    # tuned to the validated 100k smoke-test shape from day 1).
    _DEFAULT_NUM_BLOCKS = 16
    _DEFAULT_PARTITIONS_PER_BLOCK = 10   # IVF nlist (Blocks `k`)
    _DEFAULT_N_PROBE = 10
    _DEFAULT_NUM_WORKERS = 16

    def __init__(self, bucket: str, *, vectors_csv_path: Optional[str] = None):
        """
        Args:
            bucket: Object-store bucket (MinIO bucket name, e.g. 'lithops-data').
            vectors_csv_path: Local path to the dataset CSV (id,<space floats>),
                used by bulk_insert to upload the corpus. Optional if the dataset
                is already present in the bucket and only querying is needed.
        """
        self.bucket = bucket
        self.vectors_csv_path = vectors_csv_path

        # Populated by setup()
        self.dataset_name: Optional[str] = None
        self.dimensions: Optional[int] = None
        self.top_k_default: int = 10
        self.num_blocks: int = self._DEFAULT_NUM_BLOCKS
        self.partitions_per_block: int = self._DEFAULT_PARTITIONS_PER_BLOCK
        self.n_probe: int = self._DEFAULT_N_PROBE
        self.blocks_per_task: int = self._DEFAULT_NUM_BLOCKS
        self._client: Optional[VectorDBClient] = None
        self._ready = False

    # ----- identity ----------------------------------------------------------

    @property
    def name(self) -> str:
        """Stable adapter id; the parser registry key and OpResult.adapter stamp."""
        return "blocks"

    def backend_version(self) -> dict:
        """
        Version of the vendored blocks-db-wip clone, read via git. `commit_sha` /
        `commit_date` are "unknown" if the reference dir isn't a git repo or the
        read fails (with a logged warning) — see the pinned-commit finding (11):
        Blocks behaviour depends on this SHA, so recording it matters.
        """
        sha = date = "unknown"
        try:
            sha = subprocess.run(
                ["git", "-C", _BLOCKS_REPO, "rev-parse", "HEAD"],
                capture_output=True, text=True, timeout=5,
            ).stdout.strip() or "unknown"
            date = subprocess.run(
                ["git", "-C", _BLOCKS_REPO, "log", "-1", "--format=%cI"],
                capture_output=True, text=True, timeout=5,
            ).stdout.strip() or "unknown"
        except Exception as e:  # noqa: BLE001
            print(f"[blocks] backend_version: could not read git sha ({e}); reporting 'unknown'.")
            sha = date = "unknown"
        if sha == "unknown":
            print(f"[blocks] backend_version: {_BLOCKS_REPO} is not a readable git repo; sha='unknown'.")
        return {"repo": "blocks-db-wip", "commit_sha": sha, "commit_date": date}

    def dataset_fingerprint(self, dataset_name: str) -> dict:
        """
        Content fingerprint of the local corpus CSV (`vectors_csv_path`) this
        client ingests from. If no local path is set (e.g. querying a dataset
        already resident in MinIO), returns the shape with null hash fields —
        the hash is not fabricated. `dataset_name` is accepted for interface
        symmetry; Blocks binds one client to one dataset, so it's informational.
        """
        if self.vectors_csv_path and os.path.exists(self.vectors_csv_path):
            return fingerprint_csv(self.vectors_csv_path)
        return unknown_fingerprint(self.vectors_csv_path)

    # ----- capabilities ------------------------------------------------------

    def capabilities(self) -> ClientCapabilities:
        """
        Local Blocks config: no pending path (DynamoDB is out of scope, so
        supports_pending=False); L2 distance via FAISS IVF,Flat; the manual
        build path preserves CSV IDs (id_stability="preserved" — the auto-indexer
        reassignment only affects the pending->indexed path we don't use here);
        ingest is file-native only (Blocks partitions an uploaded CSV object), so
        ingest_modes={"path"} and `bulk_insert` (iterable) is not offered.

        max_safe_concurrency=1: Blocks' query map workers each download index
        blocks to a FIXED, non-namespaced /tmp/index_{n}.ann and os.remove it
        after reading (finding 10). On the Lithops localhost backend all workers
        share one /tmp, so two concurrent queries collide on /tmp/index_0.ann
        (one deletes it mid-read of the other). Safe only serially here; the
        workload generator caps concurrency at this value. On AWS Lambda each
        invocation has an isolated /tmp, so a Lambda-backed adapter could raise it.
        """
        return ClientCapabilities(
            supports_pending=False,
            metric="l2",
            id_stability="preserved",
            ingest_modes={"path"},
            max_safe_concurrency=1,
        )

    # ----- helpers -----------------------------------------------------------

    def _build_config(self) -> dict:
        """Assemble the SvlessVectorDBParams-shaped dict for indexing/search."""
        return {
            "dataset": self.dataset_name,
            "features": self.dimensions,
            "k_search": self.top_k_default,
            "k_result": self.top_k_default,
            "implementation": "blocks",
            "num_index": self.num_blocks,
            "query_batch_size": self.blocks_per_task,
            "k": self.partitions_per_block,
            "n_probe": self.n_probe,
            "storage_bucket": self.bucket,
            "index_mem": 8192,
            "search_map_mem": 8192,
            "search_reduce_mem": 2048,
        }

    def _index_exists(self) -> bool:
        """True if a config.json index marker exists for this dataset in the bucket."""
        return len(list_indexes(self.bucket, self.dataset_name)) > 0

    def _hydrate_params_from_config(self) -> None:
        """
        Read num_index / nlist / n_probe back from the stored config.json so the
        adapter's pinned params match the on-disk index. Per finding 06, read
        num_index from config.json directly — NEVER parse it from the key path
        (list_indexes returns 'config.json' in the num_index slot).
        """
        cfg = load_index_config(self.bucket, self.dataset_name, "blocks", None)
        self.num_blocks = int(cfg.get("num_index", self.num_blocks))
        self.partitions_per_block = int(cfg.get("k", self.partitions_per_block))
        self.n_probe = int(cfg.get("n_probe", self.n_probe))
        self.blocks_per_task = int(cfg.get("query_batch_size", self.num_blocks))
        if cfg.get("features"):
            self.dimensions = int(cfg["features"])
        if cfg.get("k_result"):
            self.top_k_default = int(cfg["k_result"])

    def _require_ready(self):
        if not self._ready:
            raise RuntimeError("BlocksClient.setup() must be called before this operation.")

    # ----- lifecycle ---------------------------------------------------------

    def setup(
        self,
        dataset_name: str,
        *,
        dimensions: int,
        top_k_default: int = 10,
        num_blocks: Optional[int] = None,
        partitions_per_block: Optional[int] = None,
        n_probe: Optional[int] = None,
        blocks_per_task: Optional[int] = None,
        create_if_missing: bool = True,
    ) -> None:
        # 1. Control-plane surface: boto3 must be redirected to MinIO. We do NOT
        #    set env vars here (they belong in the shell, per finding 06) — we
        #    fail loudly if they're missing so misconfiguration is obvious.
        if not os.environ.get("AWS_ENDPOINT_URL_S3"):
            raise RuntimeError(
                "AWS_ENDPOINT_URL_S3 is not set. The Blocks control plane uses "
                "boto3 with no endpoint_url; export AWS_ENDPOINT_URL_S3 (+ "
                "AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_DEFAULT_REGION) "
                "to point it at MinIO. See findings/06_blocks_local.md."
            )

        # 2. Data-plane surface: Lithops config must select localhost+minio.
        from lithops.config import default_config
        lcfg = default_config()
        backend = lcfg.get("lithops", {}).get("backend")
        storage = lcfg.get("lithops", {}).get("storage")
        if backend != "localhost" or storage != "minio":
            raise RuntimeError(
                f"~/.lithops/config must be backend=localhost storage=minio for "
                f"local runs; got backend={backend!r} storage={storage!r}."
            )

        self.dataset_name = dataset_name
        self.dimensions = dimensions
        self.top_k_default = top_k_default
        self.num_blocks = num_blocks or self._DEFAULT_NUM_BLOCKS
        self.partitions_per_block = partitions_per_block or self._DEFAULT_PARTITIONS_PER_BLOCK
        self.n_probe = n_probe or self._DEFAULT_N_PROBE
        self.blocks_per_task = blocks_per_task or self.num_blocks

        # Control-plane client (boto3 -> MinIO via env vars).
        self._client = VectorDBClient(bucket=self.bucket)

        # 3. Index presence. If one exists, hydrate pinned params from it so query
        #    config matches the on-disk index. If not and create_if_missing=False,
        #    fail. (Actual build happens in bulk_insert, not setup.)
        if self._index_exists():
            self._hydrate_params_from_config()
        elif not create_if_missing:
            raise RuntimeError(
                f"No index found for dataset '{dataset_name}' and create_if_missing=False."
            )

        self._ready = True

    def _cleanup_run_artifacts(self) -> None:
        """
        Remove transient orchestrator artifacts that delete_dataset doesn't know
        about (finding 09): the query-staging object and the reduce intermediates.

          - queries_{dataset}_{num_index}.csv — staged per search() before the map
            phase. Dataset+num_index scoped; we match the `queries_{dataset}_`
            prefix to catch any num_index variant.
          - reduce/res_{j}.json — NOT dataset-namespaced (the orchestrator writes a
            flat `reduce/` prefix shared across datasets). So this deletes the whole
            `reduce/` prefix. Safe for the single-process local harness where
            teardown follows a completed run; revisit if runs ever overlap.

        pending/* is intentionally NOT cleaned here — it belongs to the pending
        path, which is out of scope until interactivity work.
        """
        s3 = self._client.s3  # boto3 client (-> MinIO via env)
        for prefix in (f"queries_{self.dataset_name}_", "reduce/"):
            paginator = s3.get_paginator("list_objects_v2")
            to_delete = []
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    to_delete.append({"Key": obj["Key"]})
            # delete_objects caps at 1000 keys/call
            for i in range(0, len(to_delete), 1000):
                s3.delete_objects(Bucket=self.bucket, Delete={"Objects": to_delete[i:i + 1000]})

    def teardown(self, *, drop_data: bool = False) -> None:
        # No long-lived executors are held at the adapter level (Blocks builds a
        # fresh FunctionExecutor per ServerlessVectorDB call), so the only work is
        # optional data cleanup. Idempotent: safe after a partial setup.
        if drop_data and self.dataset_name and os.environ.get("AWS_ENDPOINT_URL_S3"):
            try:
                delete_dataset(self.bucket, self.dataset_name)  # deletes vectors + .ann blocks
                # delete_dataset's delete_indexes deliberately PRESERVES config.json
                # (finding 07.3), leaving a stale index marker that _index_exists
                # would still report. Remove the config too so a drop is truly clean.
                delete_index_configs(self.bucket, self.dataset_name)
                # delete_dataset also doesn't touch the orchestrator's staging /
                # reduce artifacts (finding 09); clean those too.
                self._cleanup_run_artifacts()
            except Exception as e:
                print(f"teardown: drop_data warning (non-fatal): {e}")
        self._client = None
        self._ready = False

    # ----- ingest ------------------------------------------------------------

    def bulk_insert(
        self,
        vectors: Iterable[Tuple[int, Vector]],
        *,
        build_index: bool = True,
    ) -> Tuple[None, Timings]:
        """
        NOT SUPPORTED on Blocks: ingest is file-native (Blocks uploads a CSV
        object and a Lithops job partitions it). This adapter declares
        ingest_modes={"path"} only — use `bulk_insert_from_path`. Raising here is
        the contract (capabilities tell the caller which entry point to use), not
        a failure.
        """
        raise NotImplementedError(
            "BlocksClient is file-native; use bulk_insert_from_path "
            "(capabilities().ingest_modes == {'path'})."
        )

    def bulk_insert_from_path(
        self,
        path: str,
        *,
        id_column: str = "id",
        build_index: bool = True,
    ) -> Tuple[None, Timings]:
        """
        Upload a corpus CSV and build the index via Blocks' own data-plane path.

        `path` is a local `id,<space floats>` CSV (Blocks' native row shape, so
        `id_column` is informational — Blocks reads column 0 as the ID regardless).
        Upload is control plane (boto3 -> MinIO); the index build is data plane
        (Lithops localhost workers -> MinIO). config.json is persisted so query
        discovery works.

        IDs: this manual build path preserves the CSV's first-column IDs (no
        auto-indexer reassignment), matching capabilities().id_stability ==
        "preserved".

        build_index=False uploads the corpus only. CAVEAT (finding 07.2): on
        Blocks the deferred build is the auto-indexer (DynamoDB), which is out of
        scope locally — a build_index=False corpus is NOT locally indexable later;
        call bulk_insert_from_path(build_index=True) instead to index it.
        """
        self._require_ready()
        if not os.path.exists(path):
            raise FileNotFoundError(f"{path} not found.")

        t0 = time.time()
        # Upload corpus to vectors_{dataset}.csv (control plane: boto3 -> MinIO).
        upload_dataset(self.bucket, self.dataset_name, path)
        timings: Timings = {"op": "bulk_insert_from_path",
                            "bulk_insert_upload_s": time.time() - t0}

        if build_index:
            cfg = self._build_config()
            sv = ServerlessVectorDB(**cfg)
            t1 = time.time()
            # Data plane: Lithops localhost workers build per-block FAISS indexes.
            index_timers = sv.indexing(f"vectors_{self.dataset_name}.csv", num_workers=self._DEFAULT_NUM_WORKERS)
            timings["bulk_insert_index_s"] = time.time() - t1
            timings["blocks_index_timers"] = index_timers
            # Persist config.json (pure S3/MinIO) so query discovery works.
            save_index_config(self.bucket, self.dataset_name, cfg)

        timings["wall_clock_s"] = time.time() - t0
        return None, timings

    # ----- query -------------------------------------------------------------

    def query(
        self,
        vector: Vector,
        *,
        top_k: Optional[int] = None,
    ) -> Tuple[List[Hit], Timings]:
        results, timings = self.query_batch([vector], top_k=top_k)
        # query_batch already stamped the {op, wall_clock_s} envelope; relabel op
        # so a single-vector call reports "query", not "query_batch".
        timings = {**timings, "op": "query"}
        return results[0], timings

    def query_batch(
        self,
        vectors: Sequence[Vector],
        *,
        top_k: Optional[int] = None,
    ) -> Tuple[QueryResults, Timings]:
        self._require_ready()
        k = top_k if top_k is not None else self.top_k_default
        t0 = time.time()
        # hybrid=False -> indexed-only path: boto3->MinIO discovery, Lithops->MinIO search.
        results, times = self._client.query_batch(
            self.dataset_name, [list(v) for v in vectors], k=k, hybrid=False
        )
        # Wrap the raw Blocks orchestrator dict (the 0_*_blocks phase breakdown) in
        # the common minimal envelope required by the interface: {op, wall_clock_s}.
        # We do NOT replace the phase keys — the metrics layer still parses them;
        # the envelope just lets the aggregator group ops without special-casing.
        timings: Timings = dict(times)
        timings["op"] = "query_batch"
        timings["wall_clock_s"] = time.time() - t0
        return results, timings

    # ----- mutate ------------------------------------------------------------

    def delete(
        self,
        vector_ids: Sequence[int],
        *,
        reindex: bool = True,
    ) -> Timings:
        self._require_ready()
        t0 = time.time()
        # delete_vectors_from_dataset rewrites vectors_{dataset}.csv minus the IDs
        # (no renumbering) and, if reindex, rebuilds via reindex_after_update.
        # Both are pure S3(MinIO) + Lithops; no DynamoDB.
        changed = delete_vectors_from_dataset(
            self.bucket, self.dataset_name, list(vector_ids), reindex=reindex
        )
        return {"delete_s": time.time() - t0, "deleted_any": bool(changed)}
