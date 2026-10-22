"""
Experiment C3, Phase 2 — ingest 10M + recall gate for ONE adapter.

This is the correctness precondition for the 10M latency/cost sweep. For the
chosen adapter it: setup vs the 10M corpus (source_csv_path anchor), ingests all
10M vectors, then checks recall@10 on the first RECALL_N ground-truth queries.
If an adapter misses its bar (Blocks >=95%, S3V >=70%, LanceDB >=99%) the run
stops before any sweep spend.

Recall gate size: RECALL_N defaults to 100 (not the 1000 the 1M runs used). At
10M a Blocks query is ~15-30s serial and a LanceDB query scans a ~3.9 GB table,
so 1000 gate queries would cost hours of wall time for no extra correctness
signal. 100 queries is the same gate C2 used at 1M (c2_phase1). This is a
time/cost decision, recorded in the manifest and the finding.

Cost guards (from the brief):
  - S3 Vectors ingest is estimated BEFORE any PutVectors; if > $2 we STOP and ask.
  - Any adapter's ingest estimated > $5 STOPs and asks.

Per-adapter query mechanics:
  - Blocks : query_batch runs the queries serially (one Lithops job each); the
             pinned 10M params (num_blocks=num_workers=160, n_probe=7,
             blocks_per_task=8) come from c3_common.build_and_setup + c1_common.
  - S3 Vec : query_batch issues N boto3 QueryVectors calls internally.
  - LanceDB: query_batch packs a chunk into ONE invoke. At 10M one invoke over
             many queries could exceed the 900s Lambda timeout, so we chunk
             (default 1/invoke; 20/invoke took ~1080s and timed out in August
             and again in October, finding 25) and set a 900s no-retry client.
             We PROBE one query first to measure time and catch an OOM before
             committing to the whole gate.

Run one adapter per process (SEQUENCE, not concurrent — 10M ingests are heavy):
    ./c3_blocks.sh c3_phase2.py blocks
    ./c3_aws.sh    c3_phase2.py s3vectors
    ./c3_aws.sh    c3_phase2.py lancedb
Writes results/phase2_<adapter>.json.
"""
from __future__ import annotations

import argparse
import functools
import json
import math
import os
import sys
import time
from datetime import datetime, timezone

print = functools.partial(print, flush=True)  # noqa: A001

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)
import c3_common as K  # noqa: E402
import eb_common as C  # noqa: E402

DEFAULT_RECALL_N = 100
DEFAULT_CHUNK = {"blocks": 100, "s3vectors": 100, "lancedb": 1}  # lancedb: the August gate ran chunk=1
INGEST_STOP_USD = 5.0        # any ingest over this stops and asks
S3V_INGEST_STOP_USD = 2.0    # S3 Vectors ingest gate (brief)


def estimate_s3v_ingest_usd(n_vectors, put_batch):
    """Deterministic PutVectors cost estimate, no AWS call. Billed on data
    uploaded, min 128KB/request (eb_common)."""
    n_req = math.ceil(n_vectors / put_batch)
    bytes_per_req = put_batch * C.DIMENSIONS * 4
    billable = max(bytes_per_req, C.S3V_PUT_MIN_KB * 1024)
    gb = (n_req * billable) / 1e9
    return gb * C.S3V_PUT_PER_GB, n_req


def ingest(adapter, client, *, upload=True):
    """Ingest all 10M vectors for one adapter. Returns (timings, cost_usd).

    upload=False reuses a corpus CSV already in S3 (a prior run uploaded it) and
    only clears stale .ann index files before the rebuild — saves the ~17 min
    11.5 GB re-upload on a Blocks retry."""
    if adapter == "blocks":
        import c1_common
        timings, cost = c1_common.build_index(
            client, num_workers=K.BLOCKS_PARAMS["num_workers"], upload=upload)
        return timings, cost
    _none, timings = client.bulk_insert(K.iter_vectors_csv(), build_index=True)
    return timings, ingest_cost(adapter, timings)


def ingest_cost(adapter, t):
    req = C.LAMBDA_REQ_PER_M / 1e6
    if adapter == "lancedb":
        server_ms = float(t.get("server_ms_total", 0.0))
        n_inv = int(t.get("n_invocations", 0))
        return (server_ms / 1000.0) * K.LANCEDB_MEM_GB * C.LAMBDA_ARM_GBS + n_inv * req
    if adapter == "s3vectors":
        n_req = int(t.get("n_put_requests", 0))
        batch = int(t.get("put_batch_size", 0)) or 1
        bytes_per_req = batch * C.DIMENSIONS * 4
        billable = max(bytes_per_req, C.S3V_PUT_MIN_KB * 1024)
        gb = (n_req * billable) / 1e9
        return gb * C.S3V_PUT_PER_GB
    return 0.0


def run_gate(adapter, client, gate_q, chunk):
    """Run the gate queries, chunked, and concatenate results. Returns
    (results, query_seconds, n_chunks)."""
    results = []
    t0 = time.time()
    n_chunks = 0
    for start in range(0, len(gate_q), chunk):
        sub = gate_q[start:start + chunk]
        tc = time.time()
        res, _tim = client.query_batch(sub, top_k=C.TOP_K)
        results.extend(res)
        n_chunks += 1
        print(f"[c3/p2/{adapter}] gate chunk {n_chunks} "
              f"({start + len(sub)}/{len(gate_q)}) in {time.time() - tc:.1f}s")
    return results, time.time() - t0, n_chunks


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("adapter", choices=["blocks", "s3vectors", "lancedb"])
    ap.add_argument("--recall-n", type=int, default=DEFAULT_RECALL_N, dest="recall_n")
    ap.add_argument("--chunk", type=int, default=0, help="queries/invoke (0=auto)")
    ap.add_argument("--put-batch", type=int, default=500, dest="put_batch")
    ap.add_argument("--skip-ingest", action="store_true", dest="skip_ingest",
                    help="reconnect to an existing 10M index; do the gate only")
    ap.add_argument("--no-upload", action="store_true", dest="no_upload",
                    help="Blocks only: reuse the corpus CSV already in S3, skip the "
                         "11.5 GB re-upload (still rebuilds the index)")
    ap.add_argument("--out-name", default="", dest="out_name",
                    help="result file stem (default phase2_<adapter>)")
    ap.add_argument("--probe-only", action="store_true", dest="probe_only",
                    help="ingest (unless --skip-ingest) then run ONE query and stop")
    args = ap.parse_args()
    adapter = args.adapter
    chunk = args.chunk or DEFAULT_CHUNK[adapter]

    os.makedirs(K.RESULTS_DIR, exist_ok=True)
    out_path = os.path.join(K.RESULTS_DIR, f"{args.out_name or 'phase2_' + adapter}.json")
    started = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    t_start = time.time()
    acct = K.acct()
    print(f"[c3/p2/{adapter}] start={started} account={acct} recall_n={args.recall_n} "
          f"chunk={chunk}")

    # Pre-flight cost estimate + gate (brief: S3V >$2 or any >$5 -> STOP).
    if adapter == "s3vectors":
        est, n_req = estimate_s3v_ingest_usd(10_000_000, args.put_batch)
        print(f"[c3/p2/s3vectors] ingest estimate: {n_req} PutVectors requests, "
              f"~${est:.4f} (min 128KB/req model)")
        if not args.skip_ingest and est > S3V_INGEST_STOP_USD:
            print(f"STOP: S3V ingest estimate ${est:.2f} exceeds the ${S3V_INGEST_STOP_USD} "
                  "ask-first line. Re-run only after confirming with the user.")
            return 2
    else:
        print(f"[c3/p2/{adapter}] ingest estimate: "
              f"{'~$0.05 (160 index workers)' if adapter == 'blocks' else '~$0.8 (arm64 insert invokes)'}")

    queries = K.load_queries()
    truth = K.load_truth()
    assert len(queries) == len(truth) == 1000, "expected 1000 queries + truth rows"
    gate_q = queries[:args.recall_n]
    gate_t = truth[:args.recall_n]

    client = K.build_and_setup(adapter, create_if_missing=not args.skip_ingest)
    fp = client.dataset_fingerprint(K.DATASET)
    anchor_ok = fp.get("content_sha256") == K.CORPUS_SHA256
    print(f"[c3/p2/{adapter}] setup OK sha256={fp.get('content_sha256')} "
          f"anchor_ok={anchor_ok}")
    if not anchor_ok and not args.skip_ingest:
        # Before ingest the index may be empty and report no/placeholder hash; the
        # anchor is re-checked against the source CSV the client was built with.
        print(f"[c3/p2/{adapter}] note: pre-ingest fingerprint != anchor "
              f"(expected until 10M is ingested)")

    # --- ingest ---
    ingest_s, ing_cost, ing = 0.0, 0.0, {}
    if not args.skip_ingest:
        t_ing = time.time()
        ing, ing_cost = ingest(adapter, client, upload=not args.no_upload)
        ingest_s = time.time() - t_ing
        print(f"[c3/p2/{adapter}] ingest done in {ingest_s:.1f}s cost=${ing_cost:.5f}")
        if ing_cost > INGEST_STOP_USD:
            print(f"STOP: ingest cost ${ing_cost:.4f} exceeds ${INGEST_STOP_USD}.")
            return 3
        # re-fingerprint after ingest
        fp = client.dataset_fingerprint(K.DATASET)
        anchor_ok = fp.get("content_sha256") == K.CORPUS_SHA256
        print(f"[c3/p2/{adapter}] post-ingest sha256={fp.get('content_sha256')} "
              f"anchor_ok={anchor_ok}")

    # LanceDB gate: one long no-retry client so a slow-but-fine invoke is neither
    # cut early nor duplicated by a retry (harness/adapters/lancedb_lambda.py hardening).
    if adapter == "lancedb":
        client.configure_client(read_timeout_s=900, max_retries=0)

    # --- probe one query first (catches a 10M OOM / timeout before the full gate) ---
    t_probe = time.time()
    phits, ptim = client.query(gate_q[0], top_k=C.TOP_K)
    probe_s = time.time() - t_probe
    print(f"[c3/p2/{adapter}] probe query: {len(phits)} hits in {probe_s:.2f}s "
          f"server_ms={ptim.get('server_ms')} cold={ptim.get('cold')}")
    if args.probe_only:
        print("[c3/p2] --probe-only: stopping after the probe.")
        return 0

    # --- recall gate (chunked) ---
    results, query_s, n_chunks = run_gate(adapter, client, gate_q, chunk)
    recall, per_query = K.recall_at_k(results, gate_t, C.TOP_K)
    n_perfect = sum(1 for h in per_query if h == C.TOP_K)
    non_empty = sum(1 for r in results if r)
    threshold = K.RECALL_STOP[adapter]
    print(f"[c3/p2/{adapter}] recall@10={recall:.2f}% (bar {threshold}%) "
          f"perfect={n_perfect}/{args.recall_n} non_empty={non_empty}/{args.recall_n} "
          f"query_s={query_s:.1f} chunks={n_chunks}")

    passed = recall >= threshold and non_empty == args.recall_n and anchor_ok
    summary = {
        "phase": "c3_phase2",
        "adapter": adapter,
        "started_utc": started,
        "account": acct,
        "dataset": K.DATASET,
        "corpus_sha256": fp.get("content_sha256"),
        "corpus_sha256_anchor_ok": anchor_ok,
        "recall_n": args.recall_n,
        "recall_at_10_pct": round(recall, 4),
        "recall_threshold_pct": threshold,
        "perfect_10of10": n_perfect,
        "non_empty": non_empty,
        "probe_query_seconds": round(probe_s, 3),
        "gate_chunk_size": chunk,
        "gate_n_chunks": n_chunks,
        "ingest_seconds": round(ingest_s, 3),
        "ingest_cost_usd": round(ing_cost, 6),
        "ingest_timings": {k: v for k, v in ing.items()
                           if isinstance(v, (int, float, str, bool))},
        "recall_query_seconds": round(query_s, 3),
        "wall_clock_seconds": round(time.time() - t_start, 3),
        "params": K.BLOCKS_PARAMS if adapter == "blocks" else None,
        "query0_top10_returned": [int(i) for i, _ in results[0][:C.TOP_K]] if results else [],
        "query0_top10_truth": [int(x) for x in gate_t[0][:C.TOP_K]],
        "passed": bool(passed),
    }
    with open(out_path + ".tmp", "w") as f:
        json.dump(summary, f, indent=2, sort_keys=True)
    os.replace(out_path + ".tmp", out_path)
    print(f"[c3/p2/{adapter}] wrote {out_path}")
    # Raw evidence (2026-10-16): per-query ids/dists, ingest record, provenance.
    from harness.records import per_query_records, run_provenance, write_sidecar
    write_sidecar(out_path, "per_query", {
        "per_query": per_query_records(results, gate_t, C.TOP_K),
        "ingest": ing,
        "provenance": run_provenance(client, K.DATASET,
                                     patch_path=out_path[:-5] + ".code.patch"),
    })

    if not passed:
        reason = []
        if recall < threshold:
            reason.append(f"recall {recall:.2f}% < {threshold}%")
        if non_empty != args.recall_n:
            reason.append(f"non_empty {non_empty}/{args.recall_n}")
        if not anchor_ok:
            reason.append("corpus sha256 anchor mismatch")
        print(f"\n=== C3 PHASE 2 {adapter.upper()} STOP ===", "; ".join(reason))
        return 1
    print(f"\n=== C3 PHASE 2 {adapter.upper()} PASS ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
