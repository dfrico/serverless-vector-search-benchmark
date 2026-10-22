"""
Experiment C2, Phase 2 — cold-start measurement for ONE adapter.

Question: how large is the cold-start penalty on this architecture, and does it
change the cost/latency story from Experiment B (finding 20) and C1 (finding 22)?

One driver per adapter, fully serial within itself. Each adapter only ever
touches its own backend, so its idle windows are never disturbed by the other
two drivers. Run all three at once (see run_phase2.sh) to fit the wall-clock
budget; total wall is ~1 rep-cycle x reps, not the sum across adapters.

Design — one rep is a cold cycle that yields all three configs:
  1. abs_cold  : force a fresh container, then one query.
                 Blocks/LanceDB: bump a Lambda env var (replaces the execution
                 environment) + wait abs_wait s. S3 Vectors: no container to
                 evict (managed service), so just wait 5 min then query.
  2. idle_cold : wait idle_s (default 10 min) with no traffic to this adapter,
                 then one query. This measures whether a 10-min idle evicts the
                 warm container on its own (not forced).
  3. warm      : immediately after idle_cold, five more queries; discard the
                 first (it may still carry idle_cold's transition), keep four.

Reps default to 5. abs_cold n=5, idle_cold n=5, warm n=20 per adapter.
Different query vectors per sample (cycled) so nothing can be served from a
per-query cache.

Records per sample (JSONL, results/cold_<adapter>.jsonl): adapter, config, rep,
sample index, ISO timestamp, wall latency, billed cost, and the adapter cold
signal (Blocks map/reduce invocation latency; LanceDB cold flag; S3 Vectors
none). wall latency is the primary cold-start metric; billed cost mostly does
NOT carry container init (finding 22), so it should move far less than wall.
"""
from __future__ import annotations

import argparse
import functools
import json
import os
import sys
import time
from datetime import datetime, timezone

print = functools.partial(print, flush=True)  # noqa: A001

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

import c2_common as K  # noqa: E402
import eb_common as C  # noqa: E402


def _now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _sleep(label, seconds):
    print(f"[c2/p2] sleep {seconds:.0f}s ({label}) ...")
    time.sleep(seconds)


def sample(adapter, client, qv, config, rep, sidx, out_path):
    """Run one measured query and append its flat record. qv = (query_index, vector)."""
    rec = K.query_once(adapter, client, qv[1], query_index=qv[0])
    rec.update(adapter=adapter, config=config, rep=rep, sample=sidx,
               ts=_now())
    K.jsonl_append(out_path, rec)
    cold = rec.get("cold")
    inv = rec.get("map_invocation_max_s")
    extra = ""
    if cold is not None:
        extra = f" cold={cold}"
    elif inv is not None:
        extra = f" map_inv_max={inv:.2f}s"
    print(f"[c2/p2/{adapter}] {config} rep={rep} s={sidx} "
          f"wall={rec['wall_clock_s']:.3f}s billed=${rec['billed_cost_usd']:.7f}{extra}")
    return rec


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("adapter", choices=["blocks", "s3vectors", "lancedb"])
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--idle-s", type=int, default=600, dest="idle_s")
    ap.add_argument("--warm-n", type=int, default=5, dest="warm_n",
                    help="queries fired after idle_cold; first discarded")
    ap.add_argument("--blocks-lancedb-abs-wait", type=int, default=30,
                    dest="abs_wait_container")
    ap.add_argument("--s3v-abs-wait", type=int, default=300, dest="abs_wait_s3v")
    args = ap.parse_args()
    adapter = args.adapter

    os.makedirs(K.RESULTS_DIR, exist_ok=True)
    out_path = os.path.join(K.RESULTS_DIR, f"cold_{adapter}.jsonl")
    # fresh file for this run
    if os.path.exists(out_path):
        os.replace(out_path, out_path + ".prev")

    started = _now()
    t_start = time.time()
    print(f"[c2/p2/{adapter}] start={started} reps={args.reps} "
          f"idle_s={args.idle_s} warm_n={args.warm_n}")
    print(f"[c2/p2/{adapter}] spend estimate: ~{args.reps} cold cycles x a few "
          "queries; per-query billed is Blocks ~$0.0001, LanceDB ~$0.00014, "
          "S3V $0.0000025. Whole Phase 2 across 3 adapters ~$0.01. No op > $3.")

    queries = C.load_queries()
    client = K.build_and_setup(adapter, create_if_missing=False)
    # Manifest (2026-10-16): C2 had none. Provenance + run arguments.
    from harness.records import run_provenance
    with open(out_path.replace(".jsonl", ".manifest.json"), "w") as _mf:
        json.dump({"args": vars(args), "run_started_utc": time.time(),
                   "provenance": run_provenance(
                       client, C.DATASET,
                       patch_path=out_path.replace(".jsonl", ".code.patch"))},
                  _mf, indent=2, sort_keys=True, default=str)
    # Phase 2 queries are sparse, so throttling is unlikely; but the three drivers
    # run concurrently and a Blocks 16-worker burst could briefly collide with a
    # LanceDB query. The client's default adaptive retries (set in
    # harness/adapters/lancedb_lambda.py) stop a
    # rare throttle from aborting the run; its 120s read timeout covers a cold
    # 384MB table load. So no per-experiment boto3 monkeypatch is needed here.
    # (A retried query would inflate that one sample's wall latency; such outliers
    # are flagged in aggregation.)

    targets = K.cold_targets(adapter)
    print(f"[c2/p2/{adapter}] cold targets: {targets or 'none (managed service)'}")

    qi = 0  # query cursor (cycle through the 1000 vectors)

    def next_q():
        nonlocal qi
        i = qi % len(queries)
        qi += 1
        return i, queries[i]

    try:
        for rep in range(1, args.reps + 1):
            print(f"\n[c2/p2/{adapter}] === rep {rep}/{args.reps} ===")
            # A transient environment fault (e.g. a local clock skew after a
            # laptop sleep makes an AWS signature expire) must not kill the whole
            # run. Wrap each rep: on error, log and move to the next rep, which
            # forces a fresh container anyway. We do NOT retry a failed cold
            # query in place — a retry would hit a now-warm container and be
            # mislabeled cold, corrupting the signal. Losing one rep is cheaper.
            try:
                # 1) abs_cold — force a fresh container, then one query.
                if targets:
                    marker = f"{adapter}-r{rep}-{int(t_start)}"
                    print(f"[c2/p2/{adapter}] force_cold marker={marker}")
                    K.force_cold(targets, marker)
                    _sleep("abs_cold settle", args.abs_wait_container)
                else:
                    # S3 Vectors: no container. Wait 5 min so no warm path is implied.
                    _sleep("abs_cold (s3v, no container)", args.abs_wait_s3v)
                sample(adapter, client, next_q(), "abs_cold", rep, 0, out_path)

                # 2) idle_cold — wait idle_s with no traffic, then one query.
                _sleep("idle_cold", args.idle_s)
                sample(adapter, client, next_q(), "idle_cold", rep, 0, out_path)

                # 3) warm — warm_n queries right after idle_cold; discard the first.
                for j in range(args.warm_n):
                    qv = next_q()
                    rec = K.query_once(adapter, client, qv[1], query_index=qv[0])
                    if j == 0:
                        # The transition query is excluded from "warm" but kept
                        # under its own label (the aggregator ignores it).
                        rec.update(adapter=adapter, config="warm_transition",
                                   rep=rep, sample=j, ts=_now())
                        K.jsonl_append(out_path, rec)
                        continue
                    rec.update(adapter=adapter, config="warm", rep=rep,
                               sample=j, ts=_now())
                    K.jsonl_append(out_path, rec)
                    print(f"[c2/p2/{adapter}] warm rep={rep} s={j} "
                          f"wall={rec['wall_clock_s']:.3f}s "
                          f"billed=${rec['billed_cost_usd']:.7f}")
            except Exception as e:  # noqa: BLE001 — keep the run alive
                print(f"[c2/p2/{adapter}] rep {rep} ABORTED (transient): {e!r} "
                      "-> continue to next rep")
    finally:
        print(f"[c2/p2/{adapter}] total wall {time.time() - t_start:.0f}s "
              f"({(time.time() - t_start) / 60:.1f} min)")

    print(f"\n=== C2 PHASE 2 {adapter.upper()} DONE ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
