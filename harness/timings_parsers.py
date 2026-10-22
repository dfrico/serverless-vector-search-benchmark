"""
Per-(adapter, op) timings-parser registry for the benchmark harness (day 5).

`OpResult.adapter_timings` is HETEROGENEOUS by adapter and by op (finding
07.5 / 10): every op carries the common `{op, wall_clock_s}` envelope, but the
rest of the keys are backend-specific (Blocks queries carry an 11-key
`0_*_blocks` map/reduce phase dict; other adapters will carry their own shapes).
The aggregator must not hard-code any of that.

This module is the plugin surface: a registry mapping `(adapter, op)` ->
`parser_fn(adapter_timings: dict) -> dict`, where the returned dict is a
NORMALIZED phase breakdown in seconds (keys end `_s`). Register one parser per
backend/op that has a meaningful breakdown; anything unregistered falls through
`parse_timings` unchanged and the aggregator marks it "unparsed".

Register a new adapter's parser with `@register("adapter", "op")` — no change to
the sink or aggregator is needed.
"""
from __future__ import annotations

from typing import Callable, Dict

# (adapter_name, op_name) -> parser_fn
_REGISTRY: Dict[tuple, Callable[[dict], dict]] = {}


def register(adapter: str, op: str):
    """Decorator: bind a parser_fn to an (adapter, op) key."""
    def _wrap(fn: Callable[[dict], dict]) -> Callable[[dict], dict]:
        _REGISTRY[(adapter, op)] = fn
        return fn
    return _wrap


def has_parser(adapter: str, op: str) -> bool:
    return (adapter, op) in _REGISTRY


def parse_timings(adapter: str, op: str, adapter_timings: dict) -> dict:
    """
    Return a normalized phase breakdown for `(adapter, op)`, or the raw dict
    unchanged if no parser is registered. A registered parser that hits an
    unexpected shape returns {} rather than raising, so one malformed record
    never sinks a whole aggregation.
    """
    fn = _REGISTRY.get((adapter, op))
    if fn is None:
        return dict(adapter_timings)
    try:
        return fn(adapter_timings)
    except Exception:  # noqa: BLE001 — a bad record must not crash aggregation
        return {}


# --------------------------------------------------------------------------- #
# Blocks query / query_batch
# --------------------------------------------------------------------------- #
# The Blocks orchestrator (references/blocks-db-wip/vectordb/orchestration/
# orchestrator.py) emits keys `{id_query}_{phase}_blocks`. `id_query` is 0 for a
# single query_batch call; we sum across any id_query prefixes to stay general.
# Scalar phases are orchestrator wall-clocks; `*_map_blocks` is a per-worker list
# of [total, d_queries, [downloads], [reads], [searches], reduce] (from
# implementations/blocks/querying.py:map_function), which we flatten to expose
# the worker-side download / faiss-read / search split finding 10 asks for.

def _sum_nested(x) -> float:
    """Recursively sum floats out of arbitrarily nested lists; ignore non-numbers."""
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, (list, tuple)):
        return sum(_sum_nested(e) for e in x)
    return 0.0


def _scalar_phase(t: dict, phase: str) -> float:
    """Sum the scalar value of `{id}_{phase}_blocks` across all id_query prefixes."""
    total = 0.0
    for key, val in t.items():
        if key.endswith(f"_{phase}_blocks") and isinstance(val, (int, float)):
            total += float(val)
    return total


def _parse_blocks_query(t: dict) -> dict:
    """Normalize Blocks' 0_*_blocks phase dict into named `_s` wall-clocks."""
    # orchestrator-side phase wall-clocks
    out = {
        "stage_map_data_s": _scalar_phase(t, "create_map_data"),
        "map_execution_s": _scalar_phase(t, "map_execution"),
        "create_reduce_data_s": _scalar_phase(t, "create_reduce_data"),
        "reduce_iterdata_s": _scalar_phase(t, "reduce_iterdata"),
        "reduce_execution_s": _scalar_phase(t, "reduce_execution"),
        "divide_reduce_s": _scalar_phase(t, "divide_reduce"),
        "total_querying_s": _scalar_phase(t, "total_querying"),
    }
    # worker-side split, flattened out of the per-worker `*_map_blocks` lists:
    # each worker entry = [total, d_queries, [downloads], [reads], [searches], reduce]
    dl = rd = srch = 0.0
    for key, val in t.items():
        if key.endswith("_map_blocks") and isinstance(val, list):
            for worker in val:
                if isinstance(worker, (list, tuple)) and len(worker) >= 5:
                    dl += _sum_nested(worker[2])
                    rd += _sum_nested(worker[3])
                    srch += _sum_nested(worker[4])
    out["worker_download_s"] = dl
    out["worker_faiss_read_s"] = rd
    out["worker_search_s"] = srch
    return out


register("blocks", "query")(_parse_blocks_query)
register("blocks", "query_batch")(_parse_blocks_query)


def _parse_lancedb_microvm(t: dict) -> dict:
    """LanceDB-on-MicroVM: time inside the server (the LanceDB search) and the rest of the round trip
    (TLS-terminating managed endpoint, auth proxy, network), which is a property of the MicroVM service
    (about 50 ms at 1M from the EC2 client; finding 30)."""
    wall = float(t.get("wall_clock_s", 0.0))
    server = float(t.get("server_ms", 0.0)) / 1000.0
    return {"server_s": server, "endpoint_overhead_s": max(0.0, wall - server)}


register("lancedb-microvm", "query")(_parse_lancedb_microvm)
register("lancedb-microvm", "query_batch")(_parse_lancedb_microvm)
