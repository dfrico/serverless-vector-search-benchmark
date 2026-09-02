"""
Concrete S3VectorsClient adapter — wraps AWS S3 Vectors (boto3 `s3vectors`
service) behind the harness's abstract BenchmarkVectorClient
(harness/client.py).

This is the FIRST adapter that talks to real AWS (real credentials, real spend),
so the design is deliberately careful about three things:
  1. Endpoint hygiene. The Blocks adapter redirects boto3 to MinIO via
     AWS_ENDPOINT_URL* env vars. S3 Vectors must hit real AWS, so setup() fails
     loudly if a generic AWS_ENDPOINT_URL (or an s3vectors-specific override) is
     set — a stray MinIO endpoint would silently send vectors nowhere.
  2. Credentials. Missing/expired credentials raise a clear RuntimeError at
     setup(), not an opaque botocore error mid-ingest.
  3. Spend. Every operation is a metered AWS request; the adapter does the minimum
     API calls and batches writes (100/put by default).

S3 Vectors data model (API version 2025-07-15):
  vector bucket  ->  index  ->  vectors
  - A *vector bucket* (CreateVectorBucket) is an account+region-scoped container.
  - An *index* (CreateIndex) inside a bucket fixes dimension, dataType (float32),
    and distanceMetric (euclidean | cosine). One index == one benchmark dataset.
  - A *vector* is {key: str, data: {float32: [...]}, metadata?: {...}}. The key is
    the client-supplied identity (a STRING, max 1024 chars).

Mapping to the harness contract:
  - The harness uses integer vector IDs; S3 Vectors keys are strings. We map
    int -> str(int) on ingest and str -> int on query. The identity round-trips
    unchanged, so id_stability = "preserved".
  - distanceMetric = "euclidean" is the L2 family (ascending distance, lower =
    closer), so capabilities().metric = "l2" — directly rank-comparable with the
    Blocks adapter's FAISS L2. See the metric note in capabilities().

The repos stay read-only; this module only imports the day-1 interface and the
day-6 fingerprint helper.
"""
from __future__ import annotations

import os
import re
import sys
import time
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import boto3
import botocore
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError

from harness.dataset_hash import fingerprint_csv, unknown_fingerprint

from harness.client import (
    BenchmarkVectorClient,
    ClientCapabilities,
    Hit,
    QueryResults,
    Timings,
    Vector,
)

# S3 Vectors service limits (from the boto3 service model, API 2025-07-15).
_MAX_PUT_BATCH = 500      # PutVectors: 1..500 vectors/request
_MAX_DELETE_BATCH = 500   # DeleteVectors: 1..500 keys/request
_DEFAULT_PUT_BATCH = 100  # conservative default per the day-7 brief


class S3VectorsClient(BenchmarkVectorClient):
    """BenchmarkVectorClient backed by AWS S3 Vectors via boto3."""

    # euclidean == the L2 family; picked for cross-adapter comparability with the
    # Blocks (FAISS L2) adapter. See capabilities() for the squared-vs-true nuance.
    _DISTANCE_METRIC = "euclidean"
    _DATA_TYPE = "float32"

    def __init__(
        self,
        vector_bucket_name: str,
        *,
        region: str = "us-east-1",
        source_csv_path: Optional[str] = None,
        put_batch_size: int = _DEFAULT_PUT_BATCH,
    ):
        """
        Args:
            vector_bucket_name: S3 Vectors vector-bucket name (account+region
                scoped). setup() creates it if missing (create_if_missing).
            region: AWS region. Defaults to us-east-1 (the day-7 brief's default;
                S3 Vectors is a regional service and the account's default profile
                region is also us-east-1). Documented in findings/14.
            source_csv_path: Local path to the SHARED corpus CSV the harness loads
                the ingest iterable from. Used ONLY by dataset_fingerprint so all
                three adapters report the SAME content_sha256 for identical bytes
                (the cross-adapter reproducibility anchor). Optional.
            put_batch_size: Vectors per PutVectors request (1..500). Default 100.
        """
        if not (1 <= put_batch_size <= _MAX_PUT_BATCH):
            raise ValueError(f"put_batch_size must be 1..{_MAX_PUT_BATCH}, got {put_batch_size}")
        # S3 Vectors bucket names share the index naming rules (lowercase, digits,
        # hyphens; no underscores). Canonicalise so a caller-supplied name always
        # lands on a valid bucket; warn if it changed so it is not a silent surprise.
        canonical_bucket = self._sanitize_name(vector_bucket_name)
        if canonical_bucket != vector_bucket_name:
            print(f"[s3vectors] bucket name {vector_bucket_name!r} -> {canonical_bucket!r} "
                  f"(S3 Vectors names are lowercase/digits/hyphens only).")
        self.vector_bucket_name = canonical_bucket
        self.region = region
        self.source_csv_path = source_csv_path
        self.put_batch_size = put_batch_size

        # Populated by setup()
        self.dataset_name: Optional[str] = None      # logical dataset label (manifest)
        self.index_name: Optional[str] = None         # sanitized S3 Vectors index name
        self.dimensions: Optional[int] = None
        self.top_k_default: int = 10
        self.index_arn: Optional[str] = None
        self.bucket_arn: Optional[str] = None
        self._client = None                           # boto3 s3vectors client
        self._created_bucket = False                  # did WE create the bucket?
        self._ready = False

    # ----- identity ----------------------------------------------------------

    @property
    def name(self) -> str:
        """Stable adapter id; parser-registry key and OpResult.adapter stamp."""
        return "s3vectors"

    def backend_version(self) -> Dict[str, object]:
        """
        Version identifiers for the managed backend. No vendored git repo exists
        for a managed service (per finding 13's day-7 guidance), so we record the
        SDK versions + region + the frozen S3 Vectors API version, which is what
        pins behaviour for a managed service.

        api_version is read from a live client's service model if setup() has run;
        otherwise from a throwaway client (no network call — the model is bundled
        in botocore). All fields are concrete; none should be "unknown" on a
        machine with boto3 installed.
        """
        try:
            client = self._client or boto3.client("s3vectors", region_name=self.region)
            api_version = client.meta.service_model.api_version
        except Exception as e:  # noqa: BLE001
            print(f"[s3vectors] backend_version: could not read api_version ({e}); 'unknown'.")
            api_version = "unknown"
        return {
            "service": "s3vectors",
            "region": self.region,
            "boto3": boto3.__version__,
            "botocore": botocore.__version__,
            "api_version": api_version,
        }

    def dataset_fingerprint(self, dataset_name: str) -> Dict[str, object]:
        """
        Content fingerprint of the SHARED corpus CSV (`source_csv_path`) this run
        ingests from. S3 Vectors ingests via the iterable path, not a file, but
        the harness loads that iterable from a local CSV — hashing that same file
        makes all three adapters report an identical content_sha256 for identical
        bytes (the cross-adapter anchor, finding 13). If no source path was given,
        returns the null-hash placeholder (shape recorded, hash not fabricated).
        `dataset_name` is accepted for interface symmetry.
        """
        if self.source_csv_path and os.path.exists(self.source_csv_path):
            return fingerprint_csv(self.source_csv_path)
        return unknown_fingerprint(self.source_csv_path)

    # ----- capabilities ------------------------------------------------------

    def capabilities(self) -> ClientCapabilities:
        """
        supports_pending=False: S3 Vectors has no event-driven pending path; every
            PutVectors write goes straight to the live, queryable index. put_pending
            stays the inherited no-op.
        metric="l2": the index uses distanceMetric="euclidean", which is the L2
            family — ascending distance, lower = closer — so query RANKINGS are
            directly comparable with the Blocks FAISS-L2 adapter (recall against the
            same ground truth is valid). CAVEAT: S3 Vectors "euclidean" returns the
            true euclidean distance, whereas FAISS L2 returns the SQUARED distance;
            the neighbour ORDER is identical but the absolute distance VALUES are
            not, so never compare raw distance magnitudes across the two backends.
        id_stability="preserved": S3 Vectors stores and returns the client-supplied
            key verbatim. We map int<->str(int) at the boundary, so an integer ID
            round-trips unchanged.
        ingest_modes={"iterable"}: bulk_insert (in-memory (id, vector) stream) is
            the only ingest path; bulk_insert_from_path stays the base
            NotImplementedError.
        max_safe_concurrency=4: a CONSERVATIVE, honest default. S3 Vectors is a
            managed service bounded by a per-index request-rate limit, not a /tmp
            race, so it is concurrency-safe for correctness; but AWS does not
            publish a high request-rate ceiling (the service is cost-optimised, not
            QPS-optimised), so we cap at 4 to avoid throttling until a measured
            value replaces this. The workload generator takes
            min(config.max_in_flight, 4).
        """
        return ClientCapabilities(
            supports_pending=False,
            metric="l2",
            id_stability="preserved",
            ingest_modes={"iterable"},
            max_safe_concurrency=4,
        )

    # ----- helpers -----------------------------------------------------------

    def _require_ready(self):
        if not self._ready:
            raise RuntimeError("S3VectorsClient.setup() must be called before this operation.")

    @staticmethod
    def _sanitize_name(raw: str) -> str:
        """
        Map a logical harness name (which may contain underscores/uppercase, e.g.
        'deep_100k' or 'smoke_s3v_123') to a valid S3 Vectors bucket/index name:
        3-63 chars of lowercase letters, digits, and single hyphens, starting and
        ending with an alphanumeric. Deterministic, so the same dataset always maps
        to the same index (see the naming-gap note in findings/14). Raises if the
        input has no usable characters.
        """
        s = re.sub(r"[^a-z0-9-]+", "-", raw.lower())
        s = re.sub(r"-+", "-", s).strip("-")
        if not s:
            raise ValueError(f"name {raw!r} has no valid S3 Vectors name characters.")
        if len(s) < 3:
            s = (s + "-idx")[:63]
        return s[:63].rstrip("-")

    @staticmethod
    def _guard_no_local_endpoint() -> None:
        """
        Fail loudly if a boto3 endpoint override that would divert the s3vectors
        client away from real AWS is set. The Blocks adapter/tooling exports
        AWS_ENDPOINT_URL_S3 (MinIO); that one is S3-specific and does NOT affect
        s3vectors. But the GENERIC AWS_ENDPOINT_URL and an s3vectors-specific
        override WOULD, so we reject them — sending real vectors to a MinIO
        endpoint would fail confusingly or silently.
        """
        for var in ("AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3VECTORS"):
            val = os.environ.get(var)
            if val:
                raise RuntimeError(
                    f"{var}={val!r} is set; this would divert the S3 Vectors client "
                    f"away from real AWS. Unset it before running the S3 Vectors adapter "
                    f"(it is only needed for the MinIO-backed Blocks adapter)."
                )

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
        """
        Ensure the vector bucket and the dataset's index exist and are queryable.

        The IVF/partition params (num_blocks, partitions_per_block, n_probe,
        blocks_per_task) are Blocks-specific and IGNORED here: S3 Vectors is a
        managed index with no client-tunable partitioning.

        Fails loudly (RuntimeError) if credentials/region are not wired, if a
        MinIO endpoint override is set, or if the index is missing with
        create_if_missing=False. If the index already exists, its dimension is
        verified against `dimensions`.
        """
        self._guard_no_local_endpoint()
        self.dataset_name = dataset_name
        self.index_name = self._sanitize_name(dataset_name)
        if self.index_name != dataset_name:
            print(f"[s3vectors] dataset {dataset_name!r} -> index name {self.index_name!r} "
                  f"(S3 Vectors index names are lowercase/digits/hyphens only).")
        self.dimensions = dimensions
        self.top_k_default = top_k_default

        # boto3 client bound to the region. No endpoint_url -> real AWS.
        self._client = boto3.client("s3vectors", region_name=self.region)

        # 1. Vector bucket: open if present, else create (create_if_missing).
        try:
            resp = self._client.get_vector_bucket(vectorBucketName=self.vector_bucket_name)
            self.bucket_arn = resp["vectorBucket"]["vectorBucketArn"]
        except NoCredentialsError as e:
            raise RuntimeError(
                "AWS credentials are not available for the S3 Vectors client. "
                "Configure a profile / run `aws sso login` and export AWS_PROFILE, "
                "then retry. (S3 Vectors requires real AWS credentials.)"
            ) from e
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in ("NotFoundException", "ResourceNotFoundException", "NoSuchBucket"):
                if not create_if_missing:
                    raise RuntimeError(
                        f"Vector bucket '{self.vector_bucket_name}' does not exist "
                        f"and create_if_missing=False."
                    ) from e
                create_resp = self._client.create_vector_bucket(
                    vectorBucketName=self.vector_bucket_name
                )
                self.bucket_arn = create_resp["vectorBucketArn"]
                self._created_bucket = True
            elif code in ("AccessDenied", "AccessDeniedException", "UnauthorizedException"):
                raise RuntimeError(
                    f"Access denied calling S3 Vectors (get_vector_bucket): {e}. "
                    f"The active AWS role may lack s3vectors permissions."
                ) from e
            else:
                raise
        except BotoCoreError as e:
            raise RuntimeError(f"Could not reach S3 Vectors (region={self.region!r}): {e}") from e

        # 2. Index: open if present (verify dimension), else create.
        try:
            idx = self._client.get_index(
                vectorBucketName=self.vector_bucket_name, indexName=self.index_name
            )["index"]
            self.index_arn = idx["indexArn"]
            existing_dim = int(idx["dimension"])
            if existing_dim != dimensions:
                raise RuntimeError(
                    f"Index '{dataset_name}' exists with dimension {existing_dim}, "
                    f"but setup requested {dimensions}."
                )
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in ("NotFoundException", "ResourceNotFoundException"):
                if not create_if_missing:
                    raise RuntimeError(
                        f"Index '{dataset_name}' does not exist and create_if_missing=False."
                    ) from e
                create_resp = self._client.create_index(
                    vectorBucketName=self.vector_bucket_name,
                    indexName=self.index_name,
                    dataType=self._DATA_TYPE,
                    dimension=dimensions,
                    distanceMetric=self._DISTANCE_METRIC,
                )
                self.index_arn = create_resp["indexArn"]
            else:
                raise

        self._ready = True

    def teardown(self, *, drop_data: bool = False) -> None:
        """
        Release local handles. If drop_data=True, delete this run's index (the
        vector store) and — if THIS client created the vector bucket — delete the
        now-empty bucket too. Idempotent and best-effort: warnings, not raises, on
        cleanup failure (safe after a partial setup).
        """
        if drop_data and self._client and self.index_name:
            try:
                self._client.delete_index(
                    vectorBucketName=self.vector_bucket_name, indexName=self.index_name
                )
            except ClientError as e:
                code = e.response.get("Error", {}).get("Code", "")
                if code not in ("NotFoundException", "ResourceNotFoundException"):
                    print(f"teardown: delete_index warning (non-fatal): {e}")
            except Exception as e:  # noqa: BLE001
                print(f"teardown: delete_index warning (non-fatal): {e}")

            if self._created_bucket:
                try:
                    # Bucket must be empty of indexes; the index above is now gone.
                    self._client.delete_vector_bucket(vectorBucketName=self.vector_bucket_name)
                except Exception as e:  # noqa: BLE001
                    print(f"teardown: delete_vector_bucket warning (non-fatal): {e}")

        self._client = None
        self.index_arn = None
        self._ready = False

    # ----- ingest ------------------------------------------------------------

    def bulk_insert(
        self,
        vectors: Iterable[Tuple[int, Vector]],
        *,
        build_index: bool = True,
    ) -> Tuple[None, Timings]:
        """
        Ingest an in-memory stream of (id, vector) pairs via PutVectors, batched
        (default 100/request, max 500). Integer IDs are stored as string keys
        (str(id)); they round-trip unchanged (id_stability="preserved").

        build_index: True is the only supported value — the S3 Vectors index is
        always live (a PutVectors write is immediately queryable), so there is no
        deferred-build path. build_index=False raises NotImplementedError.
        """
        if not build_index:
            raise NotImplementedError(
                "build_index=False is not supported on S3 Vectors: the index is "
                "always live (writes are immediately queryable), so there is no "
                "deferred index build to defer."
            )
        self._require_ready()

        t0 = time.time()
        n_vectors = 0
        n_batches = 0
        batch: List[dict] = []

        def _flush(records: List[dict]) -> None:
            nonlocal n_batches
            if not records:
                return
            self._client.put_vectors(
                vectorBucketName=self.vector_bucket_name,
                indexName=self.index_name,
                vectors=records,
            )
            n_batches += 1

        for vid, vec in vectors:
            batch.append({"key": str(vid), "data": {"float32": [float(x) for x in vec]}})
            n_vectors += 1
            if len(batch) >= self.put_batch_size:
                _flush(batch)
                batch = []
        _flush(batch)

        timings: Timings = {
            "op": "bulk_insert",
            "wall_clock_s": time.time() - t0,
            "n_vectors": n_vectors,
            "n_put_requests": n_batches,
            "put_batch_size": self.put_batch_size,
        }
        return None, timings

    # ----- query -------------------------------------------------------------

    def _query_one(self, vector: Vector, k: int) -> List[Hit]:
        """Single QueryVectors call -> ranked [(int_id, distance)] ascending."""
        resp = self._client.query_vectors(
            vectorBucketName=self.vector_bucket_name,
            indexName=self.index_name,
            topK=k,
            queryVector={"float32": [float(x) for x in vector]},
            returnDistance=True,
            returnMetadata=False,
        )
        # Keys are the str(int) IDs we ingested; distances ascending (euclidean).
        return [(int(v["key"]), float(v["distance"])) for v in resp.get("vectors", [])]

    def query(
        self,
        vector: Vector,
        *,
        top_k: Optional[int] = None,
    ) -> Tuple[List[Hit], Timings]:
        """Nearest neighbours of one query vector via a single QueryVectors call."""
        self._require_ready()
        k = top_k if top_k is not None else self.top_k_default
        t0 = time.time()
        hits = self._query_one(vector, k)
        # S3 Vectors exposes no server-side timing in the response, so the envelope
        # carries wall_clock_s only (no extra backend timing keys, unlike Blocks).
        timings: Timings = {"op": "query", "wall_clock_s": time.time() - t0}
        return hits, timings

    def query_batch(
        self,
        vectors: Sequence[Vector],
        *,
        top_k: Optional[int] = None,
    ) -> Tuple[QueryResults, Timings]:
        """
        Nearest neighbours for many query vectors.

        S3 Vectors has NO multi-query batch API (QueryVectors takes exactly one
        queryVector), so this issues one QueryVectors request per vector,
        sequentially, and blocks until all return. The workload generator owns
        inter-request concurrency (capped at max_safe_concurrency); this call does
        not fan out internally.
        """
        self._require_ready()
        k = top_k if top_k is not None else self.top_k_default
        t0 = time.time()
        results: QueryResults = [self._query_one(v, k) for v in vectors]
        timings: Timings = {
            "op": "query_batch",
            "wall_clock_s": time.time() - t0,
            "n_queries": len(results),
        }
        return results, timings

    # ----- mutate ------------------------------------------------------------

    def delete(
        self,
        vector_ids: Sequence[int],
        *,
        reindex: bool = False,
    ) -> Timings:
        """
        Delete vectors by ID via DeleteVectors (int IDs -> str keys), batched (max
        500 keys/request). `reindex` is a NO-OP: the S3 Vectors index is always
        live, so a delete is reflected immediately with no rebuild step. Accepted
        for interface symmetry only.
        """
        self._require_ready()
        t0 = time.time()
        ids = list(vector_ids)
        n_requests = 0
        for i in range(0, len(ids), _MAX_DELETE_BATCH):
            chunk = ids[i:i + _MAX_DELETE_BATCH]
            if not chunk:
                continue
            self._client.delete_vectors(
                vectorBucketName=self.vector_bucket_name,
                indexName=self.index_name,
                keys=[str(vid) for vid in chunk],
            )
            n_requests += 1
        return {
            "op": "delete",
            "wall_clock_s": time.time() - t0,
            "n_deleted_requested": len(ids),
            "n_delete_requests": n_requests,
        }
