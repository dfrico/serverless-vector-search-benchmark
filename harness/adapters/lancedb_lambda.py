"""
Concrete LanceDBLambdaClient adapter — the THIRD and final system-under-test for
the thesis harness, and the "naive-serverless baseline". It wraps LanceDB packaged
into a single AWS Lambda container function (harness/deploy/lambda_handler.py) that
is invoked once per operation. The LanceDB dataset lives in S3 (s3:// URI), so
every invocation reads/writes object storage — no warm index server, no EFS.

Contrast with the other two adapters:
  - Blocks (day 2): Lithops map/reduce over an object store, file-native ingest.
  - S3 Vectors (day 7): a managed vector index; one boto3 request per op.
  - LanceDB-on-Lambda (here): we own the whole stack — a general-purpose Lambda +
    LanceDB + S3. This is the honest "just put a vector DB in a Lambda" baseline,
    the packaging-and-cold-start story the thesis measures against the managed and
    map/reduce designs.

Architecture (decided in the day-8 brief — option 1, S3-backed):
  harness (this client, boto3 `lambda`)  --Invoke-->  Lambda container function
      |                                                     |
      |  event {op, db_uri, table, ...}                     |  lancedb.connect(s3://...)
      |  <-- JSON {ok, results/n_inserted, cold, server_ms} |  reads/writes S3
  The LanceDB *table* is created on the first insert and lives in
  s3://<data_bucket>/<db_prefix>. The Lambda function itself is durable infra
  (built/torn down by build_and_deploy.py), NOT per-run data.

Mapping to the harness contract:
  - Integer harness IDs are stored in an explicit `id` column and returned
    verbatim, so id_stability = "preserved" (no int<->str dance like S3 Vectors).
  - LanceDB's default distance is L2 (ascending, lower = closer), so
    capabilities().metric = "l2" — rank-comparable with Blocks and S3 Vectors.
    Whether the reported value is squared or true L2 is CONFIRMED EMPIRICALLY by
    the smoke test (see findings/15) rather than asserted from docs.

The repos stay read-only; this module imports only the day-1 interface and the
day-6 fingerprint helper. It needs NO lancedb/pyarrow locally — only boto3 — since
all LanceDB work happens inside the Lambda.
"""
from __future__ import annotations

import json
import os
import sys
import time
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import boto3
import botocore
from botocore.config import Config
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

# Lambda synchronous (RequestResponse) request/response payloads are capped at
# 6 MB each. A 96-dim float32 vector is ~10 chars/float as JSON, so 500 vectors is
# ~0.5 MB — a safe default insert batch well under the cap.
_DEFAULT_INSERT_BATCH = 500


class LanceDBLambdaClient(BenchmarkVectorClient):
    """BenchmarkVectorClient backed by LanceDB inside an AWS Lambda container."""

    def __init__(
        self,
        function_name: str,
        *,
        region: str = "us-east-1",
        data_bucket: Optional[str] = None,
        db_prefix: str = "lancedb",
        source_csv_path: Optional[str] = None,
        image_uri: Optional[str] = None,
        runtime: str = "python3.11",
        insert_batch_size: int = _DEFAULT_INSERT_BATCH,
        read_timeout_s: int = 120,
        connect_timeout_s: int = 15,
        max_retries: int = 8,
        retry_mode: str = "adaptive",
    ):
        """
        Args:
            function_name: Lambda function name (or ARN) to invoke. setup()
                verifies it exists; it does NOT create it (the Lambda is durable
                infra provisioned by build_and_deploy.py, not per-run data).
            region: AWS region. Defaults to us-east-1 (day-7/8 brief default; also
                the account's default profile region).
            data_bucket: S3 bucket where LanceDB stores the table. The connection
                URI is s3://<data_bucket>/<db_prefix>. Required before setup().
            db_prefix: Key prefix under data_bucket for the LanceDB database.
            source_csv_path: Local path to the SHARED corpus CSV, hashed by
                dataset_fingerprint so all three adapters report the SAME
                content_sha256 (the cross-adapter reproducibility anchor).
            image_uri: The ECR image URI:tag the Lambda runs, recorded verbatim in
                backend_version. Known at build time; optional.
            runtime: Base-image runtime label for backend_version (container
                Lambdas report no Runtime via the API, so we record the base image
                we built on). Default "python3.11".
            insert_batch_size: Vectors per insert invocation (payload-bounded).
            read_timeout_s: boto3 socket read timeout for an Invoke. Default 120s,
                well above boto3's 60s default: a cold container loads the whole
                S3 table before the first query, which the 60s default can clip.
                A single long batch (e.g. a 100-query recall gate) still exceeds
                this, so raise it for that call with configure_client().
            connect_timeout_s: boto3 connect timeout.
            max_retries: boto3 retry attempts. Default 8 in adaptive mode, so a
                transient TooManyRequestsException (Lambda concurrency throttle
                while another adapter runs a burst) is ridden out instead of
                killing the run. Set 0 for a single long invoke you must not have
                auto-duplicated (see configure_client / the recall-gate use).
            retry_mode: boto3 retry mode ("adaptive" | "standard" | "legacy").
        """
        if insert_batch_size < 1:
            raise ValueError(f"insert_batch_size must be >= 1, got {insert_batch_size}")
        self.function_name = function_name
        self.region = region
        self.data_bucket = data_bucket
        self.db_prefix = db_prefix
        self.source_csv_path = source_csv_path
        self.image_uri = image_uri
        self.runtime = runtime
        self.insert_batch_size = insert_batch_size
        self.read_timeout_s = read_timeout_s
        self.connect_timeout_s = connect_timeout_s
        self.max_retries = max_retries
        self.retry_mode = retry_mode

        # Populated by setup()
        self.dataset_name: Optional[str] = None       # logical dataset label == table name
        self.table_name: Optional[str] = None
        self.dimensions: Optional[int] = None
        self.top_k_default: int = 10
        self.function_arn: Optional[str] = None
        self._client = None                            # boto3 lambda client
        self._lancedb_version: str = "unknown"         # learned via a ping in setup()
        self._ready = False

    # ----- identity ----------------------------------------------------------

    @property
    def name(self) -> str:
        """
        Stable adapter id; parser-registry key and OpResult.adapter stamp. Chosen
        once and hyphenated (`lancedb-lambda`, not `lancedb_lambda`) to match the
        lowercase-hyphen house style S3 Vectors' naming rules forced on day 7 — a
        stable, filesystem/URL-safe id that never needs re-sanitising.
        """
        return "lancedb-lambda"

    def backend_version(self) -> Dict[str, object]:
        """
        Version identifiers for the LanceDB-on-Lambda backend. Per finding 13's
        guidance for a service with no vendored git repo: name the source + a
        version. We record the lancedb package version (learned from a ping to the
        deployed Lambda in setup(), so it is the version ACTUALLY running, not a
        local guess), the runtime label, the ECR image URI:tag, region, and the
        local boto3/botocore used to drive it. Fields we cannot determine are
        "unknown" (honest), never faked.
        """
        return {
            "service": "lambda",
            "region": self.region,
            "lancedb": self._lancedb_version,
            "runtime": self.runtime,
            "image_uri": self.image_uri or "unknown",
            "function_name": self.function_name,
            "boto3": boto3.__version__,
            "botocore": botocore.__version__,
        }

    def dataset_fingerprint(self, dataset_name: str) -> Dict[str, object]:
        """
        Content fingerprint of the SHARED corpus CSV (`source_csv_path`) this run
        ingests from — the cross-adapter anchor (finding 13). Hashing the same file
        the harness feeds the iterable makes all three adapters report an identical
        content_sha256 for identical bytes. Null-hash placeholder if no path given
        (shape recorded, hash not fabricated). `dataset_name` is accepted for
        interface symmetry.
        """
        if self.source_csv_path and os.path.exists(self.source_csv_path):
            return fingerprint_csv(self.source_csv_path)
        return unknown_fingerprint(self.source_csv_path)

    # ----- capabilities ------------------------------------------------------

    def capabilities(self) -> ClientCapabilities:
        """
        supports_pending=False: there is no event-driven pending path; every insert
            is written straight to the LanceDB table (queryable once committed to
            S3). put_pending stays the inherited no-op.
        metric="l2": LanceDB's default distance is L2 (ascending, lower = closer),
            so query RANKINGS are comparable with Blocks (FAISS L2) and S3 Vectors
            (euclidean). Whether LanceDB reports the SQUARED or TRUE L2 value is
            confirmed empirically by the day-8 smoke (findings/15) — the same
            rank-not-value caveat as S3 Vectors applies: compare recall / neighbour
            sets across backends, never raw distance magnitudes.
        id_stability="preserved": the harness integer ID is stored in an explicit
            `id` column and returned verbatim. LanceDB does not renumber rows, so an
            ID round-trips unchanged — no boundary cast (cleaner than S3 Vectors'
            int<->str keys).
        ingest_modes={"iterable"}: bulk_insert (in-memory (id, vector) stream) is
            the only ingest path; bulk_insert_from_path stays the base
            NotImplementedError (the corpus is streamed to the Lambda, not read from
            a file by the backend).
        max_safe_concurrency=10: CONSERVATIVE and honest. Each Lambda invocation is
            an isolated container with its own /tmp, and QUERIES are read-only
            against S3, so the read path is concurrency-safe up to the account's
            Lambda concurrency limit (default 1000). We cap at 10 because (a) LanceDB
            WRITES to S3 use optimistic concurrency and can hit commit conflicts if
            parallelised, so inserts should stay effectively serial, and (b) a
            naive-baseline should not assume a tuned high-QPS ceiling. 10 is a safe
            floor for the read-heavy workload; a measured value can replace it later.
        """
        return ClientCapabilities(
            supports_pending=False,
            metric="l2",
            id_stability="preserved",
            ingest_modes={"iterable"},
            max_safe_concurrency=10,
        )

    # ----- helpers -----------------------------------------------------------

    def _require_ready(self):
        if not self._ready:
            raise RuntimeError("LanceDBLambdaClient.setup() must be called before this operation.")

    @property
    def db_uri(self) -> str:
        if not self.data_bucket:
            raise RuntimeError(
                "data_bucket is not set; LanceDBLambdaClient needs an S3 bucket for "
                "the LanceDB table (pass data_bucket=... to the constructor)."
            )
        return f"s3://{self.data_bucket}/{self.db_prefix}"

    @staticmethod
    def _guard_no_local_endpoint() -> None:
        """
        Fail loudly if a boto3 endpoint override that would divert the `lambda`
        client away from real AWS is set. Mirrors the S3 Vectors guard: the Blocks
        tooling exports AWS_ENDPOINT_URL_S3 (MinIO, S3-only, harmless here), but the
        generic AWS_ENDPOINT_URL / an AWS_ENDPOINT_URL_LAMBDA override WOULD divert
        Invoke calls, so reject them.
        """
        for var in ("AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_LAMBDA"):
            val = os.environ.get(var)
            if val:
                raise RuntimeError(
                    f"{var}={val!r} is set; this would divert the Lambda client away "
                    f"from real AWS. Unset it before running the LanceDB-on-Lambda "
                    f"adapter (it is only needed for the MinIO-backed Blocks adapter)."
                )

    def _make_lambda_client(self, read_timeout_s, connect_timeout_s,
                            max_retries, retry_mode):
        """Build a boto3 `lambda` client with an explicit timeout + retry Config.
        Kept in one place so setup() and configure_client() stay consistent."""
        cfg = Config(
            read_timeout=read_timeout_s,
            connect_timeout=connect_timeout_s,
            retries={"max_attempts": max_retries, "mode": retry_mode},
        )
        return boto3.client("lambda", region_name=self.region, config=cfg)

    def configure_client(self, *, read_timeout_s=None, connect_timeout_s=None,
                         max_retries=None, retry_mode=None):
        """Rebuild the boto3 client with new timeout/retry settings after setup().

        Use this for one long single invoke that must not be auto-retried, e.g. a
        100-query recall-gate batch that runs ~500s in ONE Invoke:
            client.configure_client(read_timeout_s=900, max_retries=0)
        Any argument left None keeps the current instance value. Raises if setup()
        has not run yet (there is no client to reconfigure)."""
        if self._client is None:
            raise RuntimeError("configure_client() requires setup() first.")
        if read_timeout_s is not None:
            self.read_timeout_s = read_timeout_s
        if connect_timeout_s is not None:
            self.connect_timeout_s = connect_timeout_s
        if max_retries is not None:
            self.max_retries = max_retries
        if retry_mode is not None:
            self.retry_mode = retry_mode
        self._client = self._make_lambda_client(
            self.read_timeout_s, self.connect_timeout_s,
            self.max_retries, self.retry_mode)

    def _invoke(self, event: Dict[str, object]) -> Dict[str, object]:
        """
        Invoke the Lambda synchronously (RequestResponse) with a JSON event and
        return the parsed JSON response. Raises RuntimeError on a Lambda-side
        FunctionError, a non-2xx status, or a handler-reported {"ok": false}, so a
        backend failure is loud, not a silently empty result.
        """
        resp = self._client.invoke(
            FunctionName=self.function_name,
            InvocationType="RequestResponse",
            Payload=json.dumps(event).encode("utf-8"),
        )
        status = resp.get("StatusCode")
        payload_raw = resp["Payload"].read()
        if resp.get("FunctionError"):
            raise RuntimeError(
                f"Lambda op {event.get('op')!r} raised FunctionError "
                f"({resp['FunctionError']}): {payload_raw[:2000].decode('utf-8', 'replace')}"
            )
        if status not in (200, 202, 204):
            raise RuntimeError(f"Lambda op {event.get('op')!r} returned status {status}.")
        try:
            out = json.loads(payload_raw)
        except json.JSONDecodeError as e:
            raise RuntimeError(
                f"Lambda op {event.get('op')!r} returned non-JSON payload: "
                f"{payload_raw[:2000].decode('utf-8', 'replace')}"
            ) from e
        if not out.get("ok", False):
            raise RuntimeError(
                f"Lambda op {event.get('op')!r} reported failure: {out.get('error', out)}"
            )
        return out

    def _base_event(self, op: str) -> Dict[str, object]:
        return {"op": op, "db_uri": self.db_uri, "table": self.table_name}

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
        Verify the Lambda function exists and is reachable, pin the run's table
        name / dimensions, and learn the deployed lancedb version via a ping.

        Does NOT create the Lambda (that is build_and_deploy.py's job — durable
        infra, not per-run data) and does NOT create the LanceDB table (the first
        bulk_insert creates it). The Blocks IVF/partition params are ignored
        (LanceDB does exact flat KNN here; see build_index note in bulk_insert).

        Fails loudly (RuntimeError) on missing credentials, a MinIO endpoint
        override, a missing data_bucket, or a missing Lambda function (the
        create_if_missing flag governs the LanceDB TABLE, which is created lazily on
        first insert, not the Lambda function — a missing function is always fatal
        because the harness does not provision infra).
        """
        self._guard_no_local_endpoint()
        if not self.data_bucket:
            raise RuntimeError(
                "data_bucket is not set; LanceDBLambdaClient needs an S3 bucket for the "
                "LanceDB table. Pass data_bucket=... (created by build_and_deploy.py)."
            )
        self.dataset_name = dataset_name
        self.table_name = dataset_name  # LanceDB table names allow underscores; no sanitise.
        self.dimensions = dimensions
        self.top_k_default = top_k_default

        self._client = self._make_lambda_client(
            self.read_timeout_s, self.connect_timeout_s,
            self.max_retries, self.retry_mode)

        # Verify the function exists (loud, specific errors — mirrors S3 Vectors).
        try:
            fn = self._client.get_function(FunctionName=self.function_name)
            self.function_arn = fn["Configuration"]["FunctionArn"]
        except NoCredentialsError as e:
            raise RuntimeError(
                "AWS credentials are not available for the Lambda client. Configure a "
                "profile / run `aws sso login` and export AWS_PROFILE, then retry."
            ) from e
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in ("ResourceNotFoundException", "NotFoundException"):
                raise RuntimeError(
                    f"Lambda function {self.function_name!r} does not exist in region "
                    f"{self.region!r}. Deploy it first with build_and_deploy.py "
                    f"(the harness does not provision the function)."
                ) from e
            if code in ("AccessDenied", "AccessDeniedException", "UnauthorizedException"):
                raise RuntimeError(
                    f"Access denied calling Lambda get_function: {e}. The active AWS "
                    f"role may lack lambda:GetFunction / lambda:InvokeFunction."
                ) from e
            raise
        except BotoCoreError as e:
            raise RuntimeError(f"Could not reach Lambda (region={self.region!r}): {e}") from e

        # Learn the lancedb version actually running in the deployed image.
        try:
            ping = self._invoke({"op": "ping"})
            self._lancedb_version = str(ping.get("lancedb", "unknown"))
        except Exception as e:  # noqa: BLE001 — ping is best-effort metadata
            print(f"[lancedb-lambda] ping failed ({e}); lancedb version = 'unknown'.")
            self._lancedb_version = "unknown"

        self._ready = True

    def teardown(self, *, drop_data: bool = False) -> None:
        """
        Release the local boto3 handle. If drop_data=True, invoke the "drop" op so
        the Lambda deletes this run's LanceDB TABLE from S3 (the per-run data) — the
        analogue of the S3 Vectors adapter deleting its index. It does NOT delete
        the Lambda function, ECR repo, IAM role, or S3 bucket: those are durable
        infra torn down by `build_and_deploy.py --destroy` (the mandatory end-of-day
        cleanup), not by a per-run teardown. Idempotent, best-effort — warnings, not
        raises, on cleanup failure so it is safe after a partial setup.
        """
        if drop_data and self._client and self.table_name and self.data_bucket:
            try:
                self._invoke(self._base_event("drop"))
            except Exception as e:  # noqa: BLE001
                print(f"teardown: drop-table warning (non-fatal): {e}")
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
        Ingest an in-memory stream of (id, vector) pairs by invoking the "insert" op
        in batches (default 500/invocation, payload-bounded). The first insert
        creates the LanceDB table; later inserts append. Integer IDs go into the
        `id` column and round-trip unchanged (id_stability="preserved").

        build_index is a documented NO-OP: this naive baseline does EXACT flat KNN
        (LanceDB searches the raw vectors, no ANN index is built), so there is no
        index-build step to do or defer. Both True and False are accepted and behave
        identically; the flag is kept for interface symmetry. (An ANN index —
        create_index(IVF_PQ) — is a deliberate later experiment, out of scope for
        the baseline.)
        """
        self._require_ready()
        t0 = time.time()
        n_vectors = 0
        n_invocations = 0
        n_cold = 0
        server_ms_total = 0.0
        ids_batch: List[int] = []
        vecs_batch: List[List[float]] = []

        def _flush() -> None:
            nonlocal n_invocations, n_cold, server_ms_total, ids_batch, vecs_batch
            if not ids_batch:
                return
            event = self._base_event("insert")
            event.update({
                "dimensions": self.dimensions,
                "ids": ids_batch,
                "vectors": vecs_batch,
            })
            out = self._invoke(event)
            n_invocations += 1
            if out.get("cold"):
                n_cold += 1
            server_ms_total += float(out.get("server_ms", 0.0))
            ids_batch = []
            vecs_batch = []

        for vid, vec in vectors:
            ids_batch.append(int(vid))
            vecs_batch.append([float(x) for x in vec])
            n_vectors += 1
            if len(ids_batch) >= self.insert_batch_size:
                _flush()
        _flush()

        timings: Timings = {
            "op": "bulk_insert",
            "wall_clock_s": time.time() - t0,
            "n_vectors": n_vectors,
            "n_invocations": n_invocations,
            "n_cold_starts": n_cold,
            "server_ms_total": server_ms_total,
            "insert_batch_size": self.insert_batch_size,
        }
        return None, timings

    # ----- query -------------------------------------------------------------

    def query(
        self,
        vector: Vector,
        *,
        top_k: Optional[int] = None,
    ) -> Tuple[List[Hit], Timings]:
        """
        Nearest neighbours of one query vector via a single "query" invocation.
        timings carry the client wall clock plus the Lambda's own server_ms and the
        cold/warm flag, so the metrics layer can separate invocation overhead + cold
        start from in-handler search time.
        """
        self._require_ready()
        k = top_k if top_k is not None else self.top_k_default
        t0 = time.time()
        event = self._base_event("query")
        event.update({"queries": [[float(x) for x in vector]], "top_k": k})
        out = self._invoke(event)
        results = out.get("results", [[]])
        hits: List[Hit] = [(int(i), float(d)) for i, d in results[0]] if results else []
        timings: Timings = {
            "op": "query",
            "wall_clock_s": time.time() - t0,
            "server_ms": float(out.get("server_ms", 0.0)),
            "cold": bool(out.get("cold", False)),
        }
        return hits, timings

    def query_batch(
        self,
        vectors: Sequence[Vector],
        *,
        top_k: Optional[int] = None,
    ) -> Tuple[QueryResults, Timings]:
        """
        Nearest neighbours for many query vectors in ONE Lambda invocation: all
        query vectors are sent in a single event and the handler loops LanceDB
        searches internally, returning one ranked list per query.

        Design choice (differs from S3 Vectors, which had no batch API and issued N
        sequential requests): batching all queries into one invocation amortises the
        cold start and per-invoke overhead across the batch — the natural, cheapest
        shape for a Lambda. The workload generator still drives PER-REQUEST latency
        through single query() calls and owns inter-request concurrency (capped at
        max_safe_concurrency); query_batch is the bulk/eval convenience.
        """
        self._require_ready()
        k = top_k if top_k is not None else self.top_k_default
        t0 = time.time()
        event = self._base_event("query")
        event.update({
            "queries": [[float(x) for x in v] for v in vectors],
            "top_k": k,
        })
        out = self._invoke(event)
        results: QueryResults = [
            [(int(i), float(d)) for i, d in hits] for hits in out.get("results", [])
        ]
        timings: Timings = {
            "op": "query_batch",
            "wall_clock_s": time.time() - t0,
            "n_queries": len(results),
            "server_ms": float(out.get("server_ms", 0.0)),
            "cold": bool(out.get("cold", False)),
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
        Delete vectors by ID via the "delete" op (a LanceDB `id IN (...)` predicate).
        `reindex` is a NO-OP: this baseline keeps no ANN index (exact flat KNN), so a
        delete takes effect on the next query with no rebuild. Accepted for interface
        symmetry only.
        """
        self._require_ready()
        t0 = time.time()
        ids = [int(v) for v in vector_ids]
        server_ms = 0.0
        cold = False
        if ids:
            event = self._base_event("delete")
            event.update({"ids": ids})
            out = self._invoke(event)
            server_ms = float(out.get("server_ms", 0.0))
            cold = bool(out.get("cold", False))
        return {
            "op": "delete",
            "wall_clock_s": time.time() - t0,
            "n_deleted_requested": len(ids),
            "server_ms": server_ms,
            "cold": cold,
        }
