"""
Experiment C2 — cold-start comparison across Blocks, S3 Vectors, LanceDB. Shared
helpers.

Goal: measure the cold-start latency penalty on each of the three serverless
vector-search architectures at 1M scale, and see if it changes the cost/latency
ranking from Experiment B (finding 20) and C1 (finding 22).

We reuse Experiment B's loaders, recall, corpus anchor, pricing, and adapter
constructors from eb_common, and C1's Blocks tuning sweet spot + billed cost
model. This module is import-safe in the consolidated harness/.venv: every
adapter-specific import is lazy, inside a function.

Cost model (per finding 22 / context/methodology-decisions.md): report the
*billed* per-query charge, not wall. For a cold query the container-init time is
mostly NOT in the billed handler duration, so the cold penalty shows up in
wall-clock latency, not in billed cost. We record both; wall latency is the
primary cold-start metric.
"""
from __future__ import annotations

import json
import os
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
WORKSPACE = os.path.dirname(os.path.dirname(_HERE))   # experiments/experiment_c2 -> repo root
RESULTS_DIR = os.path.join(_HERE, "results")

# Reuse Experiment B + C1 helpers and the adapter clients. Put the repo root (for
# the `harness` package), the sibling experiment_b/experiment_c1 dirs (for
# eb_common / c1_common), and the external Blocks repo on path.
for _p in (WORKSPACE,
           os.path.join(WORKSPACE, "experiments", "experiment_b"),
           os.path.join(WORKSPACE, "experiments", "experiment_c1"),
           os.path.join(WORKSPACE, "references", "blocks-db-wip")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import eb_common as C  # noqa: E402

# LanceDB Lambda memory (arm64), used for its billed cost. Matches eb_aggregate.
LANCEDB_MEM_GB = 2.048
BLOCKS_MEM_GB = 3.008

# Blocks tuning sweet spot from finding 22: blocks_per_task=8 (2 map tasks) is the
# cheapest full-recall cell; n_probe=7 is the smallest n_probe that still gives
# 100% recall (Axis A).
BLOCKS_PARAMS = dict(n_probe=7, num_blocks=16, partitions_per_block=10,
                     blocks_per_task=8, num_workers=16)

# Recall preconditions for C2 (from the brief; C2 does not relitigate recall).
RECALL_STOP = {"blocks": 99.0, "s3vectors": 90.0, "lancedb": 99.0}

COLD_MARKER_KEY = "LAST_COLD_MARKER"


def acct():
    return C.account_id()


# --------------------------------------------------------------------------
# client build + setup (per adapter)
# --------------------------------------------------------------------------
def build_and_setup(adapter, *, create_if_missing):
    """Construct the adapter client and run setup. For Blocks, pin the C1 sweet
    spot params after setup (setup would otherwise hydrate old config.json). No
    warming query is issued here for any adapter, so a following query is honest.
    (LanceDB setup does send one `ping` invoke to learn its version; that warms
    the container, but the 10-minute idle wait before the first measured query
    lets AWS reap it. abs_cold forces a fresh container regardless.)
    """
    a = acct()
    client = C.build_client(adapter, a)
    if adapter == "blocks":
        client.setup(C.DATASET, dimensions=C.DIMENSIONS, top_k_default=C.TOP_K,
                     create_if_missing=create_if_missing)
        import c1_common
        c1_common.pin_params(client, **BLOCKS_PARAMS)
    else:
        client.setup(C.DATASET, dimensions=C.DIMENSIONS, top_k_default=C.TOP_K,
                     create_if_missing=create_if_missing)
    return client


# --------------------------------------------------------------------------
# per-query cost + timing extraction (per adapter)
# --------------------------------------------------------------------------
def query_record(adapter, timings):
    """Turn one query's timings dict into a flat record: wall latency, billed
    cost, and the adapter-specific cold signal."""
    req = C.LAMBDA_REQ_PER_M / 1e6
    wall = float(timings.get("wall_clock_s", 0.0))
    rec = {"wall_clock_s": wall}
    if adapter == "blocks":
        import c1_common
        (wall_cost, billed_cost, n_map, n_reduce, billed_map_s, billed_reduce_s,
         map_exec, red_exec) = c1_common.query_costs(timings)
        map_inv = timings.get("0_map_invocation_blocks", []) or []
        red_inv = timings.get("0_reduce_invocation_blocks", []) or []
        rec.update(
            billed_cost_usd=billed_cost,
            wall_cost_usd=wall_cost,
            n_map=n_map,
            n_reduce=n_reduce,
            map_execution_s=map_exec,
            reduce_execution_s=red_exec,
            # invocation latency = job-submit -> worker-func-start. A cold worker
            # spends its container init here, so max invocation latency is the
            # Blocks cold-start signal (no explicit per-task cold flag exists).
            map_invocation_max_s=max(map_inv) if map_inv else None,
            reduce_invocation_max_s=max(red_inv) if red_inv else None,
        )
    elif adapter == "lancedb":
        server_ms = float(timings.get("server_ms", 0.0))
        billed = (server_ms / 1000.0) * LANCEDB_MEM_GB * C.LAMBDA_ARM_GBS + req
        rec.update(
            billed_cost_usd=billed,
            server_ms=server_ms,
            cold=bool(timings.get("cold", False)),
        )
    elif adapter == "s3vectors":
        # Managed service: no container, one QueryVectors request fee, no server
        # timing. wall is client-side RTT.
        rec.update(billed_cost_usd=C.S3V_QUERY_PER_M / 1e6)
    return rec


def query_once(adapter, client, vector, *, top_k=None):
    """Issue one query and return its flat record."""
    hits, timings = client.query(vector, top_k=top_k or C.TOP_K)
    rec = query_record(adapter, timings)
    rec["n_hits"] = len(hits) if hits else 0
    return rec


# --------------------------------------------------------------------------
# cold forcing (env-var update invalidates warm Lambda containers)
# --------------------------------------------------------------------------
def _lambda_client():
    import boto3
    return boto3.client("lambda", region_name="us-east-1")


def discover_blocks_worker_lambdas():
    """Lithops worker Lambda names carry a hash suffix, so discover them at
    runtime (same approach as eb_cleanup.py) rather than hardcoding."""
    lam = _lambda_client()
    names = []
    paginator = lam.get_paginator("list_functions")
    for page in paginator.paginate():
        for fn in page["Functions"]:
            n = fn["FunctionName"]
            if "lithops-worker" in n or "lithops_worker" in n:
                names.append(n)
    return sorted(names)


def _wait_update_done(lam, name, timeout_s=90):
    """Block until a function config update finishes (LastUpdateStatus)."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        cfg = lam.get_function_configuration(FunctionName=name)
        if cfg.get("LastUpdateStatus") == "Successful":
            return True
        if cfg.get("LastUpdateStatus") == "Failed":
            raise RuntimeError(f"{name} update failed: {cfg.get('LastUpdateStatusReason')}")
        time.sleep(2)
    return False


def force_cold(function_names, marker):
    """Bump an env var on each function so AWS replaces the execution
    environment; the next invoke is cold. Returns after all updates settle."""
    lam = _lambda_client()
    for name in function_names:
        cfg = lam.get_function_configuration(FunctionName=name)
        env = dict((cfg.get("Environment") or {}).get("Variables") or {})
        env[COLD_MARKER_KEY] = str(marker)
        lam.update_function_configuration(
            FunctionName=name, Environment={"Variables": env})
    for name in function_names:
        _wait_update_done(lam, name)


def restore_env(function_names):
    """Remove the cold marker var, leaving each function's env as we found it."""
    lam = _lambda_client()
    for name in function_names:
        cfg = lam.get_function_configuration(FunctionName=name)
        env = dict((cfg.get("Environment") or {}).get("Variables") or {})
        if COLD_MARKER_KEY in env:
            env.pop(COLD_MARKER_KEY)
            lam.update_function_configuration(
                FunctionName=name, Environment={"Variables": env})
            _wait_update_done(lam, name)


def cold_targets(adapter):
    """The Lambda function name(s) whose containers must be invalidated to force
    a cold query for this adapter. S3 Vectors has none (managed service)."""
    if adapter == "blocks":
        return discover_blocks_worker_lambdas()
    if adapter == "lancedb":
        return ["thesis-lancedb-lambda"]
    return []


# --------------------------------------------------------------------------
# results IO
# --------------------------------------------------------------------------
def jsonl_append(path, obj):
    with open(path, "a") as f:
        f.write(json.dumps(obj) + "\n")
    # flush by closing; keep it simple and durable across the long run
