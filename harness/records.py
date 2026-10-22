"""
Raw-record helpers for experiment scripts (added 2026-10-16).

The 2026-10-16 traceability audit (context/baseline-audit.md) found that recall was
saved only as hit-count summaries, warm-up queries were dropped before any file,
and C1/C2 had no manifest. These helpers let each experiment script keep the raw
evidence next to its summary without changing what it measures.
"""
from __future__ import annotations

import os
import platform
import socket
import time
from typing import Iterable, List, Optional, Sequence

from harness.provenance import code_provenance, write_patch


def hits_to_lists(hits) -> tuple:
    """(ids, dists) from an adapter's [(id, dist), ...] hit list."""
    hits = hits or []
    return [int(h[0]) for h in hits], [float(h[1]) for h in hits]


def per_query_records(results: Sequence, truth: Sequence, k: int,
                      query_indices: Optional[Iterable[int]] = None) -> List[dict]:
    """One record per query: query row, returned ids/dists, and hits vs truth.

    results[i] is the adapter's hit list for the query at query_indices[i]
    (default: 0..n-1); truth[i] is the matching ground-truth id list.
    """
    idx = list(query_indices) if query_indices is not None else list(range(len(results)))
    out = []
    for qi, r, g in zip(idx, results, truth):
        ids, dists = hits_to_lists(r)
        gset = {int(x) for x in list(g)[:k]}
        out.append({"query_index": int(qi), "ids": ids, "dists": dists,
                    "hits": len(set(ids[:k]) & gset)})
    return out


_PROBE_HOSTS = ("s3vectors.us-east-1.api.aws", "lambda.us-east-1.amazonaws.com",
                "s3.us-east-1.amazonaws.com")


def client_network_probe(n: int = 5) -> dict:
    """TCP connect time (median of n, seconds) from this client to the us-east-1
    endpoints. Added 2026-10-16: the client's network moved Blocks and S3 Vectors
    latency between the August and October runs, so every run records it."""
    import socket as _s
    import statistics
    out = {}
    for host in _PROBE_HOSTS:
        times = []
        for _ in range(n):
            t = time.time()
            try:
                with _s.create_connection((host, 443), timeout=5):
                    times.append(time.time() - t)
            except OSError:
                pass
        out[host] = round(statistics.median(times), 4) if times else None
    return out


def run_provenance(client=None, dataset_name: Optional[str] = None,
                   patch_path: Optional[str] = None) -> dict:
    """Manifest-equivalent block for scripts that don't use JSONLSink.

    If `patch_path` is given and the code differs from HEAD, the diff is written
    there and its file name recorded.
    """
    prov = {
        "recorded_utc": time.time(),
        "code": code_provenance(),
        "hostname": socket.gethostname(),
        "python_version": platform.python_version(),
        "client_tcp_connect_s": client_network_probe(),
    }
    if patch_path and prov["code"]["dirty"]:
        written = write_patch(patch_path)
        prov["code"]["patch_file"] = os.path.basename(written) if written else None
    if client is not None:
        for key, fn in (("adapter", lambda: client.name),
                        ("backend_version", client.backend_version),
                        ("dataset_fingerprint",
                         lambda: client.dataset_fingerprint(dataset_name))):
            try:
                prov[key] = fn()
            except Exception as e:  # noqa: BLE001 — provenance must never kill a run
                prov[key] = f"unavailable: {type(e).__name__}: {e}"
    return prov


def write_sidecar(summary_path: str, kind: str, payload: dict) -> str:
    """Write `<summary stem>.<kind>.json` next to a summary file and return its path.

    Used for the bulky raw evidence (per-query ids, full timings, warm-up records)
    so the human-readable summary and its log stay small.
    """
    import json
    stem = summary_path[:-5] if summary_path.endswith(".json") else summary_path
    path = f"{stem}.{kind}.json"
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(payload, f, default=str)
    os.replace(tmp, path)
    return path


def record_warmup(client, queries: Sequence, n: int, top_k: int, jsonl_path: str) -> str:
    """Run the warm-up exactly as the sweeps always did (one query_batch over the
    first n queries) and keep its output in `<stem>.warmup.json` instead of
    discarding it, so the exclusion is visible in the data. Returns the path."""
    import json
    t0 = time.time()
    results, timings = client.query_batch(list(queries[:n]), top_k=top_k)
    t1 = time.time()
    stem = jsonl_path[:-6] if jsonl_path.endswith(".jsonl") else jsonl_path
    path = f"{stem}.warmup.json"
    with open(path, "w") as f:
        json.dump({"n": n, "query_indices": list(range(n)),
                   "wall_start_utc": t0, "wall_end_utc": t1,
                   "results": [dict(zip(("ids", "dists"), hits_to_lists(r))) for r in results],
                   "timings": timings}, f, default=str)
    return path
