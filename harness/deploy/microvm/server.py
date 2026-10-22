"""
HTTP server for LanceDB on Lambda MicroVMs (fix list F39).

Lambda routes endpoint traffic to port 8080 and calls the lifecycle hooks on the same port
(/aws/lambda-microvms/runtime/v1/<hook>). Stdlib HTTP only.

M-snap loading: when TABLE_URI is set (s3://bucket/prefix/<name>.lance), the server copies the
table's objects to local disk at start-up, opens it with LanceDB and runs one query (a full scan,
which pulls every data page into memory). /ready answers 503 until that is done, so the image
snapshot holds the loaded table. Queries use the same LanceDB call as the Lambda handler
(harness/deploy/lambda_handler.py), on a table opened once instead of read from S3 per request.

  GET  /ping                 -> counters and lifecycle timestamps (no AWS calls)
  GET  /stats                -> load report, memory, page faults, chipset
  POST /query                -> {"queries": [[...]], "top_k": 10, "metric": "l2"} like the Lambda op
  GET  /creds, /s3, /lance   -> Phase 0 checks
"""
import concurrent.futures as cf
import json
import os
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BOOT_WALL = time.time()
STATE = {"requests": 0, "queries": 0, "run_hooks": [], "resume_hooks": [], "suspend_hooks": [], "ready_hooks": 0}
LOAD = {"status": "not_configured"}
LOCK = threading.Lock()
HOOK = "/aws/lambda-microvms/runtime/v1/"
LOCAL = os.environ.get("LOCAL_DIR", "/data")
TABLE = None


def _proc():
    """Memory and fault counters, so a reader can tell whether the table survived the snapshot."""
    mem = {}
    for line in open("/proc/meminfo"):
        k, v = line.split(":")
        if k in ("MemTotal", "MemAvailable", "Cached"):
            mem[k + "_MB"] = int(v.split()[0]) // 1024
    f = open("/proc/self/stat").read().rsplit(")", 1)[1].split()
    rss = int(open("/proc/self/statm").read().split()[1]) * os.sysconf("SC_PAGE_SIZE") // 2**20
    parts = sorted({l.split(":")[1].strip() for l in open("/proc/cpuinfo") if l.startswith("CPU part")})
    # Busy CPU seconds summed over all vCPUs since boot (user+nice+system+irq+softirq+steal), and the
    # process's own; with wall time they show how much CPU ran above the 1-vCPU baseline (burst, E3).
    cpu = [int(x) for x in open("/proc/stat").readline().split()[1:]]
    tck = os.sysconf("SC_CLK_TCK")
    return {**mem, "rss_MB": rss, "minflt": int(f[7]), "majflt": int(f[9]),
            "proc_cpu_s": (int(f[11]) + int(f[12])) / tck,
            "host_busy_cpu_s": (cpu[0] + cpu[1] + cpu[2] + cpu[5] + cpu[6] + cpu[7]) / tck,
            "uptime_s": float(open("/proc/uptime").read().split()[0]), "t": time.time(),
            "cpu_part": parts, "ncpu": os.cpu_count()}  # 0xd40 = Neoverse V1 (Graviton3), 0xd4f = V2 (Graviton4)


def _load(uri):
    global TABLE
    import boto3
    import lancedb
    LOAD.update(status="loading", uri=uri, t_start=time.time())
    try:
        LOAD["identity"] = boto3.client("sts").get_caller_identity()["Arn"]
    except Exception as e:  # record which credentials (if any) the build-time process has
        LOAD["identity"] = f"ERROR {type(e).__name__}: {e}"
    bucket, prefix = uri[5:].split("/", 1)
    name = prefix.rstrip("/").rsplit("/", 1)[-1]
    s3 = boto3.client("s3")
    t0 = time.perf_counter()
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix.rstrip("/") + "/"):
        keys += [(o["Key"], o["Size"]) for o in page.get("Contents", [])]
    t_list = time.perf_counter() - t0

    def get(k):
        dst = os.path.join(LOCAL, name, k[len(prefix.rstrip("/")) + 1:])
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        s3.download_file(bucket, k, dst)

    with cf.ThreadPoolExecutor(int(os.environ.get("COPY_THREADS", "64"))) as ex:
        list(ex.map(get, [k for k, _ in keys]))
    t_copy = time.perf_counter() - t0 - t_list
    nbytes = sum(s for _, s in keys)
    db = lancedb.connect(LOCAL)
    TABLE = db.open_table(name[:-6] if name.endswith(".lance") else name)
    t1 = time.perf_counter()
    _search([0.0] * TABLE.schema.field("vector").type.list_size, 10, "l2")
    t_warm = time.perf_counter() - t1
    LOAD.update(status="loaded", objects=len(keys), bytes=nbytes, list_s=t_list, copy_s=t_copy,
                copy_MB_s=nbytes / 2**20 / t_copy if t_copy else None, warm_query_s=t_warm,
                rows=TABLE.count_rows(), t_done=time.time(), proc_after_load=_proc())


def _cpu_part():
    """CPU part of this vCPU set, read per call: a resumed MicroVM is not assumed to be on the same host."""
    return sorted({l.split(":")[1].strip() for l in open("/proc/cpuinfo") if l.startswith("CPU part")})


def _lancedb_version():
    import sys
    m = sys.modules.get("lancedb")
    return getattr(m, "__version__", "unknown") if m else "not_loaded"


def _search(q, top_k, metric):
    rows = TABLE.search([float(x) for x in q]).metric(metric).limit(top_k).select(["id"]).to_list()
    return [[int(r["id"]), float(r["_distance"])] for r in rows]


def _loader():
    try:
        _load(os.environ["TABLE_URI"])
    except Exception as e:
        LOAD.update(status="error", error=f"{type(e).__name__}: {e}")
    print(f"load {json.dumps(LOAD, default=str)[:1500]}", flush=True)


class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _send(self, code, obj):
        b = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(n) if n else b""
        if self.path.startswith(HOOK):
            name = self.path[len(HOOK):]
            with LOCK:
                if name == "ready":
                    STATE["ready_hooks"] += 1
                elif name in ("run", "resume", "suspend"):
                    STATE[f"{name}_hooks"].append(time.time())
            print(f"hook {name} load={LOAD.get('status')}", flush=True)
            if name == "ready" and LOAD["status"] == "loading":
                return self._send(503, {"ok": False, "load": "loading"})  # answer at once; Lambda retries
            if name == "validate" and TABLE is not None:
                # Lambda runs /validate on a MicroVM restored from the new snapshot and records which
                # snapshot regions the request touches, to prefetch them at run time (docs). A few
                # full-scan queries touch every data page; VALIDATE_MODE=full also reads every file
                # under the table directory once and runs one query with a real corpus vector (not one
                # of the measured queries).
                t0 = time.perf_counter()
                dim = TABLE.schema.field("vector").type.list_size
                nread = 0
                if os.environ.get("VALIDATE_MODE") == "full":
                    for d, _, files in os.walk(LOCAL):
                        for fn in files:
                            with open(os.path.join(d, fn), "rb") as fh:
                                while chunk := fh.read(1 << 22):
                                    nread += len(chunk)
                    vid = int(os.environ.get("VALIDATE_VECTOR_ID", "123456"))
                    try:
                        row = TABLE.search().where(f"id = {vid}").limit(1).to_list()
                        _search(row[0]["vector"], 10, "l2")
                    except Exception as e:  # log and keep validating with the synthetic scans
                        print(f"validate real-vector query failed: {type(e).__name__}: {e}", flush=True)
                for i in range(int(os.environ.get("VALIDATE_QUERIES", "3"))):
                    _search([0.01 * (i + 1)] * dim, 10, "l2")
                LOAD["validate_s"] = time.perf_counter() - t0
                print(f"validate {LOAD['validate_s']:.2f}s read={nread}", flush=True)
            return self._send(200, {"ok": True})
        if self.path == "/query":
            if TABLE is None:
                return self._send(503, {"ok": False, "error": f"table {LOAD.get('status')}"})
            t0 = time.perf_counter()
            ev = json.loads(body)
            res = [_search(q, int(ev.get("top_k", 10)), ev.get("metric", "l2")) for q in ev["queries"]]
            server_ms = (time.perf_counter() - t0) * 1e3
            with LOCK:
                STATE["queries"] += len(res)
                lifecycle = {"runs": len(STATE["run_hooks"]), "resumes": len(STATE["resume_hooks"])}
            # runs/resumes let the client flag the first query after a launch or a resume (E2).
            return self._send(200, {"ok": True, "results": res, "server_ms": server_ms,
                                    "cpu_part": _cpu_part(), **lifecycle})
        return self._send(404, {"error": "unknown path"})

    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        q = urllib.parse.parse_qs(u.query)
        with LOCK:
            STATE["requests"] += 1
        try:
            if u.path == "/ping":
                return self._send(200, {"now": time.time(), "boot_wall": BOOT_WALL, "pid": os.getpid(),
                                        "lancedb": _lancedb_version(), "cpu_part": _cpu_part(),
                                        "table_status": LOAD.get("status"), "table_uri": LOAD.get("uri"),
                                        "rows": LOAD.get("rows"), **STATE})
            if u.path == "/stats":
                return self._send(200, {"load": LOAD, "proc": _proc(), **STATE})
            if u.path == "/creds":
                return self._send(200, _creds())
            if u.path == "/s3":
                return self._send(200, _s3(q))
            if u.path == "/lance":
                return self._send(200, _lance(q))
            return self._send(404, {"error": "unknown path"})
        except Exception as e:  # report, don't crash the server
            return self._send(500, {"error": f"{type(e).__name__}: {e}"})

    def log_message(self, *a):
        pass


def _creds():
    import boto3
    s = boto3.Session()
    c = s.get_credentials()
    return {"method": getattr(c, "method", None) if c else None, "region": s.region_name,
            "aws_env_vars": sorted(k for k in os.environ if k.startswith(("AWS_", "ECS_")))}


def _s3(q):
    import boto3
    mb = float(q.get("mb", ["10"])[0])
    t0 = time.perf_counter()
    body = boto3.client("s3").get_object(Bucket=q["bucket"][0], Key=q["key"][0],
                                         Range=f"bytes=0-{int(mb * 2**20) - 1}")["Body"].read()
    dt = time.perf_counter() - t0
    return {"bytes": len(body), "seconds": dt, "mb_per_s": len(body) / 2**20 / dt}


def _lance(q):
    import lancedb
    t0 = time.perf_counter()
    tbl = lancedb.connect(q["uri"][0]).open_table(q["table"][0])
    t1 = time.perf_counter()
    n = tbl.count_rows()
    return {"lancedb": lancedb.__version__, "schema": str(tbl.schema), "rows": n,
            "open_s": t1 - t0, "count_s": time.perf_counter() - t1}


if __name__ == "__main__":
    if os.environ.get("TABLE_URI"):
        LOAD["status"] = "loading"
        threading.Thread(target=_loader, daemon=True).start()
    print("listening on 8080", flush=True)
    ThreadingHTTPServer(("0.0.0.0", 8080), H).serve_forever()
