"""
Run aggregator for the serverless vector-DB benchmark harness (day 5).

Reads one JSONL results file + its manifest (produced by sink.py) and computes
the headline metrics the thesis reports: per-op counts / fail rate, latency
percentiles from `OpResult.latency_s`, realised throughput, the configured-vs-
realised rate gap, and an error taxonomy. Optionally attaches a normalized
timing-phase breakdown via the per-(adapter, op) parser registry.

Backend-neutral: no adapter imports. Latency percentiles come from `latency_s`
(the harness-side ground truth — monotonic submit->ack, includes orchestration
overhead, per finding 10). The opaque `adapter_timings` dict is only touched
through the parser registry, never interpreted directly here.
"""
from __future__ import annotations

import json
import os
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from harness.timings_parsers import parse_timings, has_parser


def _percentile(sorted_vals: List[float], q: float) -> float:
    """
    Linear-interpolation percentile (q in [0,1]) over an already-sorted list.
    Matches numpy's default 'linear' method so results line up with any numpy-
    based cross-check. Assumes a non-empty list.
    """
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    pos = q * (len(sorted_vals) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = pos - lo
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * frac


@dataclass
class LatencyStats:
    """Latency distribution (seconds) for one op group's ok records."""
    n: int
    min: float
    p50: float
    p95: float
    p99: float
    max: float
    mean: float

    @classmethod
    def from_latencies(cls, latencies: List[float]) -> "LatencyStats":
        s = sorted(latencies)
        return cls(
            n=len(s), min=s[0], max=s[-1], mean=sum(s) / len(s),
            p50=_percentile(s, 0.50), p95=_percentile(s, 0.95), p99=_percentile(s, 0.99),
        )


@dataclass
class OpStats:
    """Per-op rollup: counts, latency, and (optional) normalized timing phases."""
    op: str
    adapter: str
    ok: int
    fail: int
    latency: Optional[LatencyStats]        # None if no ok records
    phase_breakdown: Optional[Dict[str, float]]  # mean of normalized phases; None if unparsed
    phases_parsed: bool                    # True if a parser produced the breakdown

    @property
    def total(self) -> int:
        return self.ok + self.fail

    @property
    def fail_rate(self) -> float:
        return self.fail / self.total if self.total else 0.0


@dataclass
class RunSummary:
    """Full aggregate of one run. `to_dict()` for JSON, `format()` for terminal."""
    manifest: dict
    n_records: int
    wall_clock_s: float                    # max(ack_ts) - min(submit_ts) over all ops
    realised_throughput: float             # ok_count / wall_clock_s
    nominal_ops_per_second: Optional[float]  # from config, if the pattern uses it
    ok_total: int
    fail_total: int
    per_op: List[OpStats]
    error_taxonomy: List[tuple]            # [(error_str, count)], most common first

    def to_dict(self) -> dict:
        return {
            "manifest": self.manifest,
            "n_records": self.n_records,
            "wall_clock_s": self.wall_clock_s,
            "realised_throughput": self.realised_throughput,
            "nominal_ops_per_second": self.nominal_ops_per_second,
            "ok_total": self.ok_total,
            "fail_total": self.fail_total,
            "per_op": [
                {
                    "op": o.op, "adapter": o.adapter, "ok": o.ok, "fail": o.fail,
                    "total": o.total, "fail_rate": o.fail_rate,
                    "latency": (o.latency.__dict__ if o.latency else None),
                    "phase_breakdown": o.phase_breakdown,
                    "phases_parsed": o.phases_parsed,
                }
                for o in self.per_op
            ],
            "error_taxonomy": [{"error": e, "count": c} for e, c in self.error_taxonomy],
        }

    def format(self) -> str:
        m = self.manifest
        lines = []
        lines.append("=" * 66)
        lines.append(f"RUN SUMMARY — {m.get('adapter', '?')} / {m.get('config', {}).get('pattern', '?')} "
                     f"/ {m.get('dataset_name', '?')}")
        lines.append(f"  git {str(m.get('git_sha', '?'))[:12]}  ts {m.get('timestamp_iso', '?')}  "
                     f"host {m.get('hostname', '?')}  py {m.get('python_version', '?')}")
        lines.append(f"  records={self.n_records}  ok={self.ok_total}  fail={self.fail_total}  "
                     f"wall={self.wall_clock_s:.2f}s  eff_concurrency={m.get('effective_concurrency', '?')}")
        lines.append("-" * 66)

        # throughput: configured vs realised
        nom = self.nominal_ops_per_second
        if nom is not None:
            ratio = (self.realised_throughput / nom) if nom else float("nan")
            lines.append(f"  throughput: realised={self.realised_throughput:.3f} ops/s  "
                         f"nominal={nom:.3f} ops/s  realised/nominal={ratio:.1%}")
        else:
            lines.append(f"  throughput: realised={self.realised_throughput:.3f} ops/s  "
                         f"nominal=n/a (pattern has no fixed arrival rate)")
        lines.append("-" * 66)

        # per-op
        for o in self.per_op:
            lines.append(f"  [{o.adapter}/{o.op}]  ok={o.ok} fail={o.fail} fail_rate={o.fail_rate:.1%}")
            if o.latency:
                L = o.latency
                lines.append(f"      latency_s  min={L.min:.4f}  p50={L.p50:.4f}  "
                             f"p95={L.p95:.4f}  p99={L.p99:.4f}  max={L.max:.4f}  mean={L.mean:.4f}")
            if o.phases_parsed and o.phase_breakdown:
                phases = "  ".join(f"{k}={v:.4f}" for k, v in o.phase_breakdown.items())
                lines.append(f"      phases(mean)  {phases}")
            else:
                lines.append("      phases(mean)  unparsed (no (adapter,op) parser registered)")

        # errors
        if self.error_taxonomy:
            lines.append("-" * 66)
            lines.append("  errors (top):")
            for err, cnt in self.error_taxonomy:
                lines.append(f"      {cnt:>4}x  {err}")
        lines.append("=" * 66)
        return "\n".join(lines)


class RunAggregator:
    """Read a JSONL results file + manifest and compute a `RunSummary`."""

    def __init__(self, jsonl_path: str, manifest_path: Optional[str] = None):
        self.jsonl_path = jsonl_path
        # Convention: run_X.jsonl <-> run_X.manifest.json
        self.manifest_path = manifest_path or (jsonl_path[:-len(".jsonl")] + ".manifest.json"
                                               if jsonl_path.endswith(".jsonl")
                                               else jsonl_path + ".manifest.json")

    def _load(self):
        with open(self.manifest_path) as f:
            manifest = json.load(f)
        records = []
        with open(self.jsonl_path) as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        return manifest, records

    def aggregate(self, *, error_top_n: int = 5) -> RunSummary:
        manifest, records = self._load()

        # wall clock + realised throughput over the whole run
        submits = [r["submit_ts"] for r in records]
        acks = [r["ack_ts"] for r in records]
        wall = (max(acks) - min(submits)) if records else 0.0
        ok_total = sum(1 for r in records if r["ok"])
        fail_total = len(records) - ok_total
        realised = (ok_total / wall) if wall > 0 else 0.0

        # nominal rate is only meaningful for a fixed-arrival-rate pattern (constant)
        cfg = manifest.get("config", {})
        nominal = cfg.get("ops_per_second") if cfg.get("pattern") == "constant" else None

        # group by (adapter, op)
        groups: Dict[tuple, List[dict]] = defaultdict(list)
        for r in records:
            groups[(r["adapter"], r["op"])].append(r)

        per_op: List[OpStats] = []
        for (adapter, op), recs in sorted(groups.items()):
            ok_recs = [r for r in recs if r["ok"]]
            lat = LatencyStats.from_latencies([r["latency_s"] for r in ok_recs]) if ok_recs else None

            # normalized phase breakdown via the parser registry (ok ops only —
            # failed ops carry an empty adapter_timings)
            phase_dicts = []
            parsed_flag = has_parser(adapter, op)
            if parsed_flag:
                for r in ok_recs:
                    parsed = parse_timings(adapter, op, r.get("adapter_timings", {}))
                    if parsed:
                        phase_dicts.append(parsed)
            phase_breakdown = None
            if phase_dicts:
                keys = set().union(*(d.keys() for d in phase_dicts))
                phase_breakdown = {
                    k: sum(d.get(k, 0.0) for d in phase_dicts) / len(phase_dicts)
                    for k in sorted(keys)
                }

            per_op.append(OpStats(
                op=op, adapter=adapter,
                ok=len(ok_recs), fail=len(recs) - len(ok_recs),
                latency=lat, phase_breakdown=phase_breakdown,
                phases_parsed=bool(phase_breakdown),
            ))

        errors = Counter(r["error"] for r in records if not r["ok"] and r.get("error"))
        taxonomy = errors.most_common(error_top_n)

        return RunSummary(
            manifest=manifest, n_records=len(records), wall_clock_s=wall,
            realised_throughput=realised, nominal_ops_per_second=nominal,
            ok_total=ok_total, fail_total=fail_total,
            per_op=per_op, error_taxonomy=taxonomy,
        )


if __name__ == "__main__":
    # CLI: python aggregate.py <results.jsonl> [manifest.json]
    if len(sys.argv) < 2:
        raise SystemExit("usage: python aggregate.py <results.jsonl> [manifest.json]")
    agg = RunAggregator(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else None)
    print(agg.aggregate().format())
