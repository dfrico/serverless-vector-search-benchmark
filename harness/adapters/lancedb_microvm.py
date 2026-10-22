"""
LanceDBMicroVMClient: the fourth system under test (fix list F39). The same LanceDB engine,
version and table layout as LanceDB-on-Lambda, but the table lives in a stateful AWS Lambda
MicroVM instead of being read from S3 on every request.

Architecture (findings 28 and 29):
  harness (this client, requests)  --HTTPS + X-aws-proxy-auth-->  MicroVM endpoint (port 8080)
                                                                   harness/deploy/microvm/server.py
  The table is baked into the image ("M-snap"): during the image build the server copies the
  LanceDB table from the MicroVM bucket to local disk, opens it and runs a full-scan query before
  answering /ready, so the snapshot holds the loaded table. A /validate hook replays a full read so
  Lambda can prefetch the snapshot pages at launch (without it the first query after a launch took
  22 s at 1M; finding 29). Queries use the Lambda handler's exact LanceDB call.

Mapping to the harness contract:
  - Ingest is not an operation of this adapter. The equivalent is scripts/microvm/make_table.py
    (same rows, schema and 2,000-vector batches as the Lambda ingest) plus the image build, done
    before the run. ingest_modes is empty and bulk_insert raises; setup records the table and build
    provenance (`ingest_record`) instead of pretending to ingest.
  - dataset_fingerprint hashes the same corpus CSV as the other three adapters.
  - Lifecycle: setup either launches a MicroVM from the image (and then owns it) or attaches to an
    existing one by ID (E2 needs both). teardown terminates only a MicroVM this client launched.
    Every launch carries maximumDurationInSeconds as a backstop, and an atexit hook terminates an
    owned MicroVM if teardown was never reached.
  - Timings carry op, wall_clock_s, server_ms, the CPU part of the vCPUs that served the request,
    and first_after_launch / resumed flags derived from the server's /run and /resume hook counts,
    so E2 can label those queries the way the Lambda adapter labels cold ones.
  - max_safe_concurrency is the documented concurrent-connection limit per MicroVM for the image's
    baseline size (Service Quotas, applied values read 18 Oct): 8 at 1 vCPU, 16 at 2, 32 at 4.
    A known service property, not a measurement.

Not measured here: the warm-up rule. A launched MicroVM keeps prefetching snapshot pages after it
reports RUNNING (first query 1.2 s at once vs 0.19 s 30 s later, 1M). setup(settle_s=...) waits a
fixed time after RUNNING; the experiment driver chooses it and reports it.
"""
from __future__ import annotations

import atexit
import os
import threading
import time
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import boto3
import botocore
import requests
from requests.adapters import HTTPAdapter

from harness.client import (
    BenchmarkVectorClient,
    ClientCapabilities,
    Hit,
    QueryResults,
    Timings,
    Vector,
)
from harness.dataset_hash import fingerprint_csv, unknown_fingerprint

_NET = "arn:aws:lambda:{region}:aws:network-connector:aws-network-connector:"


class MicroVMThrottled(RuntimeError):
    """HTTP 429 from the MicroVM endpoint: a request-rate quota, not a server fault. The workload generator
    records the message as the op's error (ok=False), so a throttled cell is recognisable in the JSONL.
    AWS documents 40 req/s per 4-vCPU and 160 per 16-vCPU MicroVM; no figure is published for 1 or 2 vCPU."""

    def __init__(self, path: str, body: str):
        super().__init__(f"THROTTLED HTTP 429 on {path}: {body[:200]}")
        self.status = 429
# Concurrent connections per MicroVM by vCPU count (Service Quotas, lambda, 18 Oct 2026).
_CONNECTIONS_PER_VCPU = {1: 8, 2: 16, 4: 32, 8: 64, 16: 128}
_TOKEN_MAX_MIN = 60          # CreateMicrovmAuthToken rejects longer (finding 28)
_TOKEN_REFRESH_S = 50 * 60   # refresh well before expiry


class LanceDBMicroVMClient(BenchmarkVectorClient):
    """BenchmarkVectorClient backed by LanceDB inside a Lambda MicroVM (table baked into the image)."""

    def __init__(
        self,
        image_name: str,
        *,
        account_id: str,
        region: str = "us-east-1",
        execution_role_arn: Optional[str] = None,
        source_csv_path: Optional[str] = None,
        attach_microvm_id: Optional[str] = None,
        idle_suspend_s: int = 900,
        suspended_ttl_s: int = 1800,
        auto_resume: bool = True,
        max_lifetime_s: int = 4 * 3600,
        settle_s: float = 0.0,
        request_timeout_s: float = 300.0,
        table_record: Optional[Dict[str, object]] = None,
    ):
        """
        Args:
            image_name: MicroVM image name, e.g. "thesis-lancedb-mvm-deep1m" (one image per dataset).
            account_id: AWS account, to build the image ARN.
            execution_role_arn: Role the MicroVM runs with (read access to the MicroVM bucket).
            source_csv_path: The shared corpus CSV, hashed by dataset_fingerprint.
            attach_microvm_id: Attach to this running/suspended MicroVM instead of launching one.
                The client then never terminates it.
            idle_suspend_s / suspended_ttl_s / auto_resume: the idle policy of a launched MicroVM.
            max_lifetime_s: maximumDurationInSeconds of a launched MicroVM (backstop, <= 8 h).
            settle_s: seconds to wait after RUNNING before setup returns (prefetch, see module doc).
            request_timeout_s: HTTP timeout per request.
            table_record: provenance of the baked table (make_table_<dataset>.json), recorded in
                ingest_record.
        """
        self.image_name = image_name
        self.account_id = account_id
        self.region = region
        self.image_arn = f"arn:aws:lambda:{region}:{account_id}:microvm-image:{image_name}"
        self.execution_role_arn = execution_role_arn
        self.source_csv_path = source_csv_path
        self.attach_microvm_id = attach_microvm_id
        self.idle_policy = {"autoResumeEnabled": auto_resume, "maxIdleDurationSeconds": idle_suspend_s,
                            "suspendedDurationSeconds": suspended_ttl_s}
        self.max_lifetime_s = max_lifetime_s
        self.settle_s = settle_s
        self.request_timeout_s = request_timeout_s
        self.table_record = table_record

        # Populated by setup()
        self.dataset_name: Optional[str] = None
        self.dimensions: Optional[int] = None
        self.top_k_default = 10
        self.microvm_id: Optional[str] = None
        self.endpoint: Optional[str] = None
        self.owned = False
        self.image_version = "unknown"
        self.baseline_memory_mib: Optional[int] = None
        self.ingest_record: Dict[str, object] = {}
        self.lifecycle_record: Dict[str, object] = {}
        self._lancedb_version = "unknown"
        self._cpu_part: object = "unknown"
        self._mvm = None
        self._session: Optional[requests.Session] = None
        self.reconnects = 0  # ConnectionErrors retried on a new connection (see _request)
        self._token_at = 0.0
        self._token_lock = threading.Lock()
        self._seen = {"runs": None, "resumes": None}
        self._seen_lock = threading.Lock()
        self._first_query_done = False
        self.transition_record: Dict[str, object] = {}
        self._ready = False
        self._atexit_registered = False

    # ----- identity ----------------------------------------------------------

    @property
    def name(self) -> str:
        return "lancedb-microvm"

    def backend_version(self) -> Dict[str, object]:
        """Image ARN and version, baseline size, the LanceDB version reported by the running server
        (/ping), and the chipset of the MicroVM that served the run (/proc/cpuinfo CPU part:
        0xd40 = Graviton 3, 0xd4f = Graviton 4). Unknown fields are "unknown", never guessed."""
        return {
            "service": "lambda-microvms",
            "region": self.region,
            "image_arn": self.image_arn,
            "image_version": self.image_version,
            "baseline_memory_mib": self.baseline_memory_mib or "unknown",
            "lancedb": self._lancedb_version,
            "cpu_part": self._cpu_part,
            "microvm_id": self.microvm_id or "unknown",
            "microvm_owned": self.owned,
            "settle_s": self.settle_s,
            "boto3": boto3.__version__,
            "botocore": botocore.__version__,
            "requests": requests.__version__,
        }

    def dataset_fingerprint(self, dataset_name: str) -> Dict[str, object]:
        """Hash of the shared corpus CSV, the same file the other three adapters hash."""
        if self.source_csv_path and os.path.exists(self.source_csv_path):
            return fingerprint_csv(self.source_csv_path)
        return unknown_fingerprint(self.source_csv_path)

    # ----- capabilities ------------------------------------------------------

    def capabilities(self) -> ClientCapabilities:
        """
        supports_pending=False, metric="l2", id_stability="preserved": as LanceDB-on-Lambda (same
            engine, same `id` column, same search call).
        ingest_modes=set(): the table is baked into the image; see the module docstring.
        max_safe_concurrency: the documented concurrent-connection limit for the baseline size
            (8 per MicroVM at 1 vCPU, 16 at 2, 32 at 4). Before setup has read the image size it is
            the 1-vCPU value, the smallest.
        """
        vcpu = max(1, (self.baseline_memory_mib or 2048) // 2048)
        return ClientCapabilities(
            supports_pending=False,
            metric="l2",
            id_stability="preserved",
            ingest_modes=set(),
            max_safe_concurrency=_CONNECTIONS_PER_VCPU.get(vcpu, 8),
        )

    # ----- helpers -----------------------------------------------------------

    @staticmethod
    def _guard_no_local_endpoint() -> None:
        """Refuse endpoint overrides that would divert the MicroVM control-plane client (the Blocks
        tooling may export AWS_ENDPOINT_URL_S3 for MinIO; that one is harmless here)."""
        for var in ("AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_LAMBDA", "AWS_ENDPOINT_URL_LAMBDA_MICROVMS"):
            val = os.environ.get(var)
            if val:
                raise RuntimeError(f"{var}={val!r} is set; unset it before running the LanceDB-on-MicroVM adapter.")

    def _require_ready(self):
        if not self._ready:
            raise RuntimeError("LanceDBMicroVMClient.setup() must be called before this operation.")

    def _state(self) -> str:
        return self._mvm.get_microvm(microvmIdentifier=self.microvm_id)["state"]

    def _wait_state(self, want: str, timeout_s: float = 600.0) -> float:
        t0 = time.time()
        while True:
            s = self._state()
            if s == want:
                return time.time() - t0
            if s in ("TERMINATING", "TERMINATED") and want not in ("TERMINATING", "TERMINATED"):
                raise RuntimeError(f"MicroVM {self.microvm_id} is {s} while waiting for {want}")
            if time.time() - t0 > timeout_s:
                raise RuntimeError(f"MicroVM {self.microvm_id} not {want} after {timeout_s:.0f}s (last {s})")
            time.sleep(0.5)

    def _refresh_token(self, force: bool = False) -> None:
        with self._token_lock:
            if not force and time.time() - self._token_at < _TOKEN_REFRESH_S:
                return
            tok = self._mvm.create_microvm_auth_token(
                microvmIdentifier=self.microvm_id, expirationInMinutes=_TOKEN_MAX_MIN,
                allowedPorts=[{"port": 8080}])["authToken"]["X-aws-proxy-auth"]
            self._session.headers["X-aws-proxy-auth"] = tok
            self._token_at = time.time()

    def _request(self, method: str, path: str, **kw) -> Dict[str, object]:
        """One HTTPS call on the persistent session; refreshes the token on age or on a 401/403.
        After a long idle the endpoint closes the pooled keep-alive connection (test 2, 19 Oct: the first call
        after 600 s idle failed with RemoteDisconnected), so a ConnectionError is retried once on a new
        connection and counted in self.reconnects. All endpoints are read-only, so a retry is safe."""
        self._refresh_token()
        url = f"https://{self.endpoint}{path}"
        try:
            r = self._session.request(method, url, timeout=self.request_timeout_s, **kw)
        except requests.exceptions.ConnectionError:
            self.reconnects += 1
            r = self._session.request(method, url, timeout=self.request_timeout_s, **kw)
        if r.status_code in (401, 403):
            self._refresh_token(force=True)
            r = self._session.request(method, url, timeout=self.request_timeout_s, **kw)
        if r.status_code == 429:
            raise MicroVMThrottled(path, r.text)
        if r.status_code != 200:
            raise RuntimeError(f"MicroVM {method} {path} returned HTTP {r.status_code}: {r.text[:500]}")
        out = r.json()
        if isinstance(out, dict) and out.get("ok") is False:
            raise RuntimeError(f"MicroVM {method} {path} reported failure: {out}")
        return out

    def _lifecycle_flags(self, out: Dict[str, object]) -> Dict[str, object]:
        """first_after_launch / resumed: the /run or /resume hook count changed since the previous
        request this client saw (setup reads the baseline from /ping). In launch mode the /run hook has
        fired before setup's /ping, so first_after_launch stays False and the first query after a
        launch is simply the first query after setup, marked first_after_setup (E1 aggregation drops or
        reports it; it is a prefetch-race sample). The flag first_after_launch matters in attach mode.

        Concurrency: one "last seen" counter is shared by all threads, so under concurrent requests the
        resumed flag lands on whichever response arrives first after the change. That is exact for E2
        (serial) and irrelevant for E1 (no suspends inside a cell)."""
        runs, resumes = out.get("runs"), out.get("resumes")
        with self._seen_lock:
            prev_runs, prev_res = self._seen["runs"], self._seen["resumes"]
            self._seen = {"runs": runs, "resumes": resumes}
            first_setup = not self._first_query_done
            self._first_query_done = True
        return {"first_after_setup": first_setup,
                "first_after_launch": prev_runs is not None and runs is not None and runs != prev_runs,
                "resumed": prev_res is not None and resumes is not None and resumes != prev_res}

    def _atexit_terminate(self) -> None:
        if self.owned and self.microvm_id and self._mvm is not None:
            try:
                self._mvm.terminate_microvm(microvmIdentifier=self.microvm_id)
                print(f"[lancedb-microvm] atexit: terminated {self.microvm_id}")
            except Exception as e:  # noqa: BLE001
                print(f"[lancedb-microvm] atexit: terminate {self.microvm_id} failed: {e}")

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
        Launch a MicroVM from the image (or attach to `attach_microvm_id`), open the HTTPS session,
        and check that the server holds the expected table, loaded. The Blocks index parameters are
        ignored. create_if_missing is accepted for symmetry: this adapter never builds an image (the
        image is the ingest step, see the module doc), so a missing image always raises.
        """
        self._guard_no_local_endpoint()
        self.dataset_name, self.dimensions, self.top_k_default = dataset_name, dimensions, top_k_default
        self._mvm = boto3.client("lambda-microvms", region_name=self.region)

        img = self._mvm.get_microvm_image(imageIdentifier=self.image_arn)
        if img["state"] not in ("CREATED", "UPDATED"):
            raise RuntimeError(f"MicroVM image {self.image_name} is {img['state']}; build it first.")
        self.image_version = img.get("latestActiveImageVersion") or img.get("imageVersion") or "unknown"
        ver = self._mvm.get_microvm_image_version(imageIdentifier=self.image_arn, imageVersion=self.image_version)
        res = ver.get("resources") or [{}]
        self.baseline_memory_mib = res[0].get("minimumMemoryInMiB")
        self.ingest_record = {
            "kind": "table baked into the MicroVM image (make_table.py + image build), not ingested by the adapter",
            "table": self.table_record,
            "image_version_created": str(ver.get("createdAt")),
            "image_version_updated": str(ver.get("updatedAt")),
            "image_env_table_uri": (ver.get("environmentVariables") or {}).get("TABLE_URI"),
            "validate_hook": ((ver.get("hooks") or {}).get("microvmImageHooks") or {}).get("validate"),
        }

        t0 = time.time()
        if self.attach_microvm_id:
            self.microvm_id, self.owned = self.attach_microvm_id, False
            g = self._mvm.get_microvm(microvmIdentifier=self.microvm_id)
            self.endpoint = g["endpoint"]
            self.lifecycle_record = {"mode": "attach", "state_at_attach": g["state"]}
        else:
            net = _NET.format(region=self.region)
            kw = dict(imageIdentifier=self.image_arn, imageVersion=self.image_version,
                      ingressNetworkConnectors=[net + "ALL_INGRESS"], egressNetworkConnectors=[net + "INTERNET_EGRESS"],
                      idlePolicy=self.idle_policy, maximumDurationInSeconds=self.max_lifetime_s)
            if self.execution_role_arn:
                kw["executionRoleArn"] = self.execution_role_arn
            r = self._mvm.run_microvm(**kw)
            self.microvm_id, self.endpoint, self.owned = r["microvmId"], r["endpoint"], True
            if not self._atexit_registered:
                atexit.register(self._atexit_terminate)
                self._atexit_registered = True
            to_running = self._wait_state("RUNNING")
            self.lifecycle_record = {"mode": "launch", "run_to_running_s": time.time() - t0,
                                     "polled_running_s": to_running}

        self._session = requests.Session()
        pool = self.capabilities().max_safe_concurrency
        self._session.mount("https://", HTTPAdapter(pool_connections=1, pool_maxsize=pool))
        self._refresh_token(force=True)
        ping = self._request("GET", "/ping")
        expected_suffix = f"/{dataset_name}.lance"
        if ping.get("table_status") != "loaded" or not str(ping.get("table_uri", "")).endswith(expected_suffix):
            raise RuntimeError(f"MicroVM {self.microvm_id} serves table {ping.get('table_uri')!r} "
                               f"({ping.get('table_status')}), expected ...{expected_suffix} loaded")
        self._lancedb_version = str(ping.get("lancedb", "unknown"))
        self._cpu_part = ping.get("cpu_part", "unknown")
        self.lifecycle_record["rows"] = ping.get("rows")
        with self._seen_lock:
            self._seen = {"runs": len(ping.get("run_hooks", [])), "resumes": len(ping.get("resume_hooks", []))}
        if self.settle_s > 0:
            time.sleep(self.settle_s)
        self.lifecycle_record["setup_s"] = time.time() - t0
        self._ready = True

    def teardown(self, *, drop_data: bool = False) -> None:
        """Close the session; terminate the MicroVM only if this client launched it. drop_data is a
        no-op: the table lives in the image and the MicroVM bucket, which are run infrastructure
        (deleted by hand after the thesis, like the 10M data). Idempotent."""
        # E3 has no per-invocation billed duration for MicroVMs: running/suspended seconds come from these
        # timestamps (server hook times + control-plane start/terminate), cross-checked against Cost Explorer.
        if self._session is not None and self._ready and not self.transition_record:
            try:
                st = self._request("GET", "/stats")
                self.transition_record = {k: st.get(k) for k in ("run_hooks", "resume_hooks", "suspend_hooks")}
                self.transition_record["stats_at_teardown"] = st.get("proc")
            except Exception as e:  # noqa: BLE001 — a suspended/unreachable MicroVM must still be terminated
                self.transition_record = {"stats_error": f"{type(e).__name__}: {e}"}
        if self._session is not None:
            self._session.close()
            self._session = None
        if self.owned and self.microvm_id and self._mvm is not None:
            try:
                if self._state() not in ("TERMINATING", "TERMINATED"):
                    self._mvm.terminate_microvm(microvmIdentifier=self.microvm_id)
                self._wait_state("TERMINATED", timeout_s=180)
            except Exception as e:  # noqa: BLE001
                print(f"[lancedb-microvm] teardown: terminate {self.microvm_id} warning: {e}")
            try:
                g = self._mvm.get_microvm(microvmIdentifier=self.microvm_id)
                self.transition_record.update({"microvm_id": self.microvm_id, "state": g["state"],
                                               "started_at": str(g.get("startedAt")),
                                               "terminated_at": str(g.get("terminatedAt")),
                                               "lifecycle": self.lifecycle_record})
            except Exception as e:  # noqa: BLE001
                self.transition_record["get_microvm_error"] = str(e)
            self.owned = False
        self._ready = False

    # ----- ingest ------------------------------------------------------------

    def bulk_insert(
        self,
        vectors: Iterable[Tuple[int, Vector]],
        *,
        build_index: bool = True,
    ) -> Tuple[None, Timings]:
        raise NotImplementedError(
            "lancedb-microvm has no ingest operation: the table is written by scripts/microvm/make_table.py "
            "and baked into the MicroVM image at build time (see ingest_record).")

    # ----- query -------------------------------------------------------------

    def query(self, vector: Vector, *, top_k: Optional[int] = None) -> Tuple[List[Hit], Timings]:
        self._require_ready()
        k = top_k if top_k is not None else self.top_k_default
        t0, r0 = time.time(), self.reconnects
        out = self._request("POST", "/query", json={"queries": [[float(x) for x in vector]], "top_k": k})
        results = out.get("results") or [[]]
        hits: List[Hit] = [(int(i), float(d)) for i, d in results[0]]
        timings: Timings = {"op": "query", "wall_clock_s": time.time() - t0,
                            "server_ms": float(out.get("server_ms", 0.0)), "cpu_part": out.get("cpu_part"),
                            "reconnected": self.reconnects > r0, **self._lifecycle_flags(out)}
        return hits, timings

    def query_batch(self, vectors: Sequence[Vector], *, top_k: Optional[int] = None) -> Tuple[QueryResults, Timings]:
        """All query vectors in one HTTPS request; the server loops the searches."""
        self._require_ready()
        k = top_k if top_k is not None else self.top_k_default
        t0, r0 = time.time(), self.reconnects
        out = self._request("POST", "/query", json={"queries": [[float(x) for x in v] for v in vectors], "top_k": k})
        results: QueryResults = [[(int(i), float(d)) for i, d in hits] for hits in out.get("results", [])]
        timings: Timings = {"op": "query_batch", "wall_clock_s": time.time() - t0, "n_queries": len(results),
                            "server_ms": float(out.get("server_ms", 0.0)), "cpu_part": out.get("cpu_part"),
                            "reconnected": self.reconnects > r0, **self._lifecycle_flags(out)}
        return results, timings

    # ----- mutate ------------------------------------------------------------

    def delete(self, vector_ids: Sequence[int], *, reindex: bool = True) -> Timings:
        raise NotImplementedError(
            "lancedb-microvm serves a read-only table baked into the image; deletes are out of scope.")

    def manifest_extra(self) -> Dict[str, object]:
        """Extra manifest fields (JSONLSink includes them when present): how the table got there and how
        this MicroVM was started. State transitions are known only at teardown; drivers write them to a
        <cell>.lifecycle.json sidecar."""
        return {"ingest_record": self.ingest_record, "lifecycle_record": self.lifecycle_record}

    # ----- E2 helpers ----------------------------------------------------------

    def suspend(self) -> float:
        """Explicitly suspend the MicroVM; returns seconds until SUSPENDED."""
        self._require_ready()
        t0 = time.time()
        self._mvm.suspend_microvm(microvmIdentifier=self.microvm_id)
        self._wait_state("SUSPENDED")
        return time.time() - t0

    def resume(self) -> float:
        """Explicitly resume the MicroVM; returns seconds until RUNNING."""
        self._require_ready()
        t0 = time.time()
        self._mvm.resume_microvm(microvmIdentifier=self.microvm_id)
        self._wait_state("RUNNING")
        return time.time() - t0

    def stats(self) -> Dict[str, object]:
        """Server /stats: build-time load report, memory, faults, CPU counters, chipset."""
        self._require_ready()
        return self._request("GET", "/stats")


def build_for_dataset(dataset: str, source_csv_path: str, account_id: str, *, settle_s: float = 60.0,
                      **kw) -> LanceDBMicroVMClient:
    """The experiments' constructor: image thesis-lancedb-mvm-<dataset without '_'>, the MicroVM execution
    role, and the table provenance written by scripts/microvm/make_table.py. settle_s defaults to the E1 rule
    (60 s after RUNNING, methodology-decisions.md); E2 passes 0."""
    import json
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    rec = os.path.join(root, "scripts", "microvm", "results", f"make_table_{dataset}.json")
    # The bundle ships no results/ folders; without this record a run would carry no table provenance
    # (the 10M gate on 21 Oct failed its ingest_record check for this reason).
    if not os.path.exists(rec):
        raise FileNotFoundError(f"{rec} missing: copy the make_table/copy_table record to this client first")
    return LanceDBMicroVMClient(
        f"thesis-lancedb-mvm-{dataset.replace('_', '')}", account_id=account_id,
        execution_role_arn=f"arn:aws:iam::{account_id}:role/thesis-microvm-exec",
        source_csv_path=source_csv_path, settle_s=settle_s,
        table_record=json.load(open(rec)), **kw)
