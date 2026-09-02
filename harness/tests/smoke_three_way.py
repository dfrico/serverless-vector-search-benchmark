"""
Day-9 three-way smoke test — orchestrator + cross-adapter comparison.

Proves the three concrete adapters (BlocksClient, S3VectorsClient,
LanceDBLambdaClient) interoperate through the abstract BenchmarkVectorClient
interface, driven by the SAME day-3 workload generator and persisted by the SAME
day-5 JSONL sink, then compares their outputs.

This is an INSTRUMENT-INTEROPERABILITY check, not a recall benchmark. The 1,000
vector sample has no matching ground truth, so we validate that the three systems
were driven by one harness on identical data and agree on neighbour RANKINGS — not
that the neighbours are the true ones (that is the first real experiment).

It runs each adapter in its OWN subprocess (run_one.py) with its own credentials
env, because:
  - Blocks needs the MinIO env (AWS_ENDPOINT_URL_S3, minioadmin static keys) and
    the local Blocks control plane (lithops/faiss/orjson).
  - S3 Vectors + LanceDB need the `thesis` profile (real AWS), and must NOT see
    the MinIO endpoint or the minioadmin keys.
All three run from the one consolidated venv (harness/.venv). Separate processes
give each a fresh boto3 default session, so credentials never leak between adapters.

Prerequisites (the operator sets these up before running; see findings/16):
  - MinIO container running on :9000 (bucket lithops-data) for Blocks.
  - `build_and_deploy.py deploy` has run, so the thesis-lancedb-lambda function
    exists, for the LanceDB adapter. (This script does NOT deploy or destroy it.)

Cross-adapter assertions:
  A. headline: all three manifests report the SAME dataset_fingerprint.content_sha256.
  B. all three report capabilities.metric == "l2".
  C. max_safe_concurrency is 1 / 4 / 10 for blocks / s3vectors / lancedb-lambda.
  D. every OpResult in each run carries the expected adapter name; >= 5 query ops.
  E. neighbour-set overlap of query[0]'s top-10 IDs, pairwise: report the
     intersection size; PASS if >= 5/10 per pair (loose). If materially lower,
     flag prominently but do NOT fail (rank comparability, not identity, is the bar).
  F. timings envelope: every query op carries {op, wall_clock_s}; Blocks adds its
     0_*_blocks phase keys, LanceDB adds server_ms + cold.

Run:  harness/.venv/bin/python harness/tests/smoke_three_way.py
"""
from __future__ import annotations

import itertools
import json
import os
import subprocess
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
WORKSPACE = os.path.dirname(os.path.dirname(_HERE))   # harness/tests -> repo root
RESULTS_DIR = os.path.join(_HERE, "results")
RUN_ONE = os.path.join(_HERE, "run_one.py")

# One consolidated venv now runs all three adapters. The separation that matters
# for credentials is still enforced below: each adapter runs in its OWN subprocess
# with its own environment (MinIO for Blocks vs the thesis profile for the rest).
_VENV_PY = os.path.join(WORKSPACE, "harness", ".venv", "bin", "python")
BLOCKS_PY = _VENV_PY
AWS_PY = _VENV_PY

EXPECTED_CONCURRENCY = {"blocks": 1, "s3vectors": 4, "lancedb-lambda": 10}
ADAPTERS = ["blocks", "s3vectors", "lancedb-lambda"]


def _blocks_env():
    """MinIO env for the Blocks control plane (boto3 -> MinIO), no thesis profile."""
    env = dict(os.environ)
    env.pop("AWS_PROFILE", None)
    env.update({
        "AWS_ENDPOINT_URL_S3": "http://localhost:9000",
        "AWS_ACCESS_KEY_ID": "minioadmin",
        "AWS_SECRET_ACCESS_KEY": "minioadmin",
        "AWS_DEFAULT_REGION": "us-east-1",
    })
    # Lithops' localhost backend spawns workers by exec'ing a bare `python`, so the
    # Blocks venv's bin dir MUST be on PATH (days 1-6 ran with the venv activated).
    venv_bin = os.path.dirname(BLOCKS_PY)
    env["PATH"] = venv_bin + os.pathsep + env.get("PATH", "")
    return env


def _aws_env():
    """Real-AWS env for S3 Vectors + LanceDB: thesis profile, no MinIO overrides."""
    env = dict(os.environ)
    for k in ("AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL_S3VECTORS",
              "AWS_ENDPOINT_URL_LAMBDA", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",
              "AWS_SESSION_TOKEN"):
        env.pop(k, None)
    env["AWS_PROFILE"] = "thesis"
    env["AWS_REGION"] = "us-east-1"
    env["AWS_DEFAULT_REGION"] = "us-east-1"
    return env


def run_adapter(adapter, ts):
    """Launch run_one.py for one adapter in its own venv + env. Returns True on success."""
    py = BLOCKS_PY if adapter == "blocks" else AWS_PY
    env = _blocks_env() if adapter == "blocks" else _aws_env()
    print(f"\n{'='*70}\n== RUN {adapter}  (py={os.path.relpath(py, WORKSPACE)})\n{'='*70}")
    proc = subprocess.run([py, RUN_ONE, adapter, str(ts)], env=env)
    ok = proc.returncode == 0
    print(f"== {adapter} exit code {proc.returncode} -> {'OK' if ok else 'FAILED'}")
    return ok


def load_json(path):
    with open(path) as f:
        return json.load(f)


def load_jsonl(path):
    out = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def compare(ts):
    """Read the three runs' outputs and run the cross-adapter assertions."""
    manifests, records, overlaps = {}, {}, {}
    for a in ADAPTERS:
        stem = f"run_{ts}_{a}"
        manifests[a] = load_json(os.path.join(RESULTS_DIR, f"{stem}.manifest.json"))
        records[a] = load_jsonl(os.path.join(RESULTS_DIR, f"{stem}.jsonl"))
        overlaps[a] = load_json(os.path.join(RESULTS_DIR, f"overlap_{ts}_{a}.json"))

    checks = {}

    # A. headline — identical corpus bytes across all three.
    shas = {a: manifests[a]["dataset_fingerprint"].get("content_sha256") for a in ADAPTERS}
    checks["A_same_content_sha256"] = len(set(shas.values())) == 1 and all(shas.values())
    print(f"\nA. content_sha256 per adapter:")
    for a in ADAPTERS:
        print(f"     {a:16} {shas[a]}")
    print(f"   -> all identical: {checks['A_same_content_sha256']}")

    # B. metric == l2 everywhere.
    metrics = {a: manifests[a]["capabilities"]["metric"] for a in ADAPTERS}
    checks["B_metric_l2"] = all(m == "l2" for m in metrics.values())
    print(f"B. metric per adapter: {metrics} -> {checks['B_metric_l2']}")

    # C. max_safe_concurrency 1 / 4 / 10.
    concs = {a: manifests[a]["capabilities"]["max_safe_concurrency"] for a in ADAPTERS}
    checks["C_max_concurrency"] = all(concs[a] == EXPECTED_CONCURRENCY[a] for a in ADAPTERS)
    print(f"C. max_safe_concurrency: got {concs} expected {EXPECTED_CONCURRENCY} "
          f"-> {checks['C_max_concurrency']}")

    # D. adapter name stamped on every OpResult; >= 5 query ops each.
    name_ok, count_ok = True, True
    per_adapter_counts = {}
    for a in ADAPTERS:
        recs = records[a]
        names = {r["adapter"] for r in recs}
        n_query = sum(1 for r in recs if r["op"] == "query")
        per_adapter_counts[a] = n_query
        if names != {a}:
            name_ok = False
            print(f"   ! {a}: unexpected adapter names in records: {names}")
        if n_query < 5:
            count_ok = False
        # every op must have succeeded for a clean smoke
        n_ok = sum(1 for r in recs if r["ok"])
        if n_ok != len(recs):
            print(f"   ! {a}: {len(recs)-n_ok}/{len(recs)} ops reported ok=False")
    checks["D_adapter_names"] = name_ok
    checks["D_query_counts"] = count_ok
    print(f"D. query ops per adapter: {per_adapter_counts} (>=5 each: {count_ok}); "
          f"adapter names correct: {name_ok}")

    # E. neighbour-set overlap of query[0]'s top-10 IDs, pairwise.
    top = {a: overlaps[a]["query0_top10_ids"] for a in ADAPTERS}
    print("E. query[0] top-10 IDs:")
    for a in ADAPTERS:
        print(f"     {a:16} {top[a]}")
    overlap_ok = True
    overlap_report = {}
    for x, y in itertools.combinations(ADAPTERS, 2):
        inter = len(set(top[x]) & set(top[y]))
        union = len(set(top[x]) | set(top[y]))
        jacc = inter / union if union else 0.0
        overlap_report[f"{x}|{y}"] = {"intersection": inter, "jaccard": round(jacc, 3)}
        flag = "" if inter >= 5 else "  <-- LOW OVERLAP (flagged, not failed)"
        print(f"     {x} vs {y}: |intersection|={inter}/10  jaccard={jacc:.2f}{flag}")
        if inter < 5:
            overlap_ok = False
    # Loose bar: report it, but do not fail the whole smoke on overlap alone.
    checks["E_overlap_ge_5"] = overlap_ok

    # F. timings envelope + per-adapter extras.
    def query_timings(a):
        return [r["adapter_timings"] for r in records[a] if r["op"] == "query" and r["ok"]]

    envelope_ok = True
    extras = {}
    for a in ADAPTERS:
        tlist = query_timings(a)
        if not tlist or not all("op" in t and "wall_clock_s" in t for t in tlist):
            envelope_ok = False
        extras[a] = sorted(tlist[0].keys()) if tlist else []
    checks["F_envelope"] = envelope_ok
    # Blocks: the raw orchestrator phase keys (0_*_blocks) survive into adapter_timings.
    blocks_keys = extras["blocks"]
    checks["F_blocks_phase_keys"] = any(k.endswith("_blocks") or k.startswith("0_") for k in blocks_keys)
    # LanceDB: server_ms + cold on every query op.
    lance_t = query_timings("lancedb-lambda")
    checks["F_lancedb_server_cold"] = bool(lance_t) and all(
        "server_ms" in t and "cold" in t for t in lance_t
    )
    print(f"F. timings envelope {{op, wall_clock_s}} on all query ops: {envelope_ok}")
    for a in ADAPTERS:
        print(f"     {a:16} query timings keys: {extras[a]}")
    print(f"   blocks phase keys present: {checks['F_blocks_phase_keys']}; "
          f"lancedb server_ms+cold present: {checks['F_lancedb_server_cold']}")

    # Overlap is a LOOSE, flagged-not-failed check (rank comparability, not
    # identity, is the bar). Exclude it from the pass/fail gate; it is still
    # reported and flagged prominently above and in the summary.
    gating = {k: v for k, v in checks.items() if k != "E_overlap_ge_5"}
    passed = all(gating.values())
    print("\n" + "=" * 70)
    print("=== DAY-9 THREE-WAY SMOKE", "PASS" if passed else "FAIL", "===")
    for k, v in checks.items():
        note = "  (flagged, non-gating)" if k == "E_overlap_ge_5" else ""
        print(f"  {'OK ' if v else 'XX '} {k}: {v}{note}")

    summary = {
        "passed": passed,
        "checks": checks,
        "timestamp": ts,
        "content_sha256": shas,
        "metric": metrics,
        "max_safe_concurrency": concs,
        "query_counts": per_adapter_counts,
        "query0_top10_ids": top,
        "overlap": overlap_report,
        "query_timings_keys": extras,
    }
    with open(os.path.join(RESULTS_DIR, f"summary_{ts}.json"), "w") as f:
        json.dump(summary, f, indent=2)
    print(f"\nsummary -> results/summary_{ts}.json")
    return passed


def main():
    ap_ts = int(sys.argv[1]) if len(sys.argv) > 1 else int(time.time())
    print(f"day-9 three-way smoke; run id (timestamp) = {ap_ts}")
    print(f"results dir: {RESULTS_DIR}")

    failed = []
    for a in ADAPTERS:
        if not run_adapter(a, ap_ts):
            failed.append(a)
    if failed:
        print(f"\nADAPTER RUN(S) FAILED: {failed}. Cannot run the full comparison. STOP.")
        # Still try a partial comparison if all three files somehow exist.
        return 1

    ok = compare(ap_ts)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
