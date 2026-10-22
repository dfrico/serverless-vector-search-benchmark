"""One source for every number in the thesis (fix list F42).

Reads the result files of the reported run (tag 2026-10-ec2, plus the 20 Oct polling check) and writes
report/numbers.tex: one \\defnum{key}{value} per quantity, used in the text as \\V{key}. An undefined key
stops the LaTeX build, so a typo cannot print an empty number. Ratios are computed here, never by hand.
It then greps report/thesis.tex for values of the discarded August run (F41 D.3) and prints any hit.

    python3 scripts/make_numbers.py            # run before every build
"""
from __future__ import annotations

import collections
import glob
import json
import math
import os
import re
import statistics
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
TAG = "2026-10-ec2"
EXP = os.path.join(ROOT, "experiments")
ANA = os.path.join(ROOT, "scripts", "analysis", f"results_{TAG}")
OUT = os.path.join(ROOT, "report", "numbers.tex")

V: dict[str, str] = {}
SRC: dict[str, str] = {}


def put(key, value, src):
    if key in V:
        sys.exit(f"duplicate key {key}")
    V[key], SRC[key] = value, src


def load(*parts):
    return json.load(open(os.path.join(ROOT, *parts)))


# ----- formatting (style rules in CLAUDE.md) -------------------------------------------------------------

def thou(s):
    """Thousands separator for LaTeX on the integer part."""
    neg = s.startswith("-")
    s = s.lstrip("-")
    ip, _, fp = s.partition(".")
    if len(ip) > 3:
        ip = re.sub(r"(\d)(?=(\d{3})+$)", r"\1{,}", ip)
    return ("-" if neg else "") + ip + ("." + fp if fp else "")


def sig(x, n=3):
    """n significant figures, fixed notation (latency in s, $/query)."""
    if x == 0:
        return "0"
    d = n - 1 - int(math.floor(math.log10(abs(x))))
    r = round(x, d)
    if r != 0 and int(math.floor(math.log10(abs(r)))) != int(math.floor(math.log10(abs(x)))):
        d -= 1  # rounding carried into a new digit (9.9957 -> 10.0, not 10.00)
        r = round(x, d)
    if d <= 0:
        return thou(str(int(r)))
    return thou(f"{r:.{d}f}")


def fix(x, d):
    return thou(f"{x:.{d}f}")


def pct(x):
    return f"{x:.2f}"


def ratio(x):
    return thou(str(int(round(x)))) if x >= 10 else f"{x:.1f}"


def integer(x):
    return thou(str(int(round(x))))


# ----- sources -------------------------------------------------------------------------------------------

lat = load("scripts", "analysis", f"results_{TAG}", "latency_summary.json")["oct"]
cost = {(c["exp"], c["cell"]): c for c in load("scripts", "analysis", f"results_{TAG}", "cost_levels.json")["cells"]}
prices = load("scripts", "analysis", f"results_{TAG}", "cost_levels.json")["meta"]["prices"]
LAT_SRC = f"scripts/analysis/results_{TAG}/latency_summary.json"
COST_SRC = f"scripts/analysis/results_{TAG}/cost_levels.json"
MAP_FN, RED_FN, LDB_FN = "11170a3116", "600f82b870", "edb-lambda"


def per_inv_s(c, fn):
    g = next(v for k, v in c["per_group"].items() if k.endswith(fn))
    return g["billed_ms"] / g["n"] / 1000.0, g


def put_cell(prefix, exp, cell, sweep=True):
    lk = f"{exp}/{cell}"
    L = lat[lk]
    put(f"{prefix}.n", integer(L["n"]), LAT_SRC)
    put(f"{prefix}.p50", sig(L["p50"]), LAT_SRC)
    put(f"{prefix}.p95", sig(L["p95"]), LAT_SRC)
    put(f"{prefix}.p50lo", sig(L["p50_ci"][0]), LAT_SRC)
    put(f"{prefix}.p50hi", sig(L["p50_ci"][1]), LAT_SRC)
    put(f"{prefix}.p95lo", sig(L["p95_ci"][0]), LAT_SRC)
    put(f"{prefix}.p95hi", sig(L["p95_ci"][1]), LAT_SRC)
    if sweep and L.get("ops_s") == L.get("ops_s"):  # not NaN
        put(f"{prefix}.ops", sig(L["ops_s"]), LAT_SRC)
    c = cost[(exp, cell)]
    put(f"{prefix}.l1", sig(c["l1_usd_q"]), COST_SRC)
    put(f"{prefix}.l2", sig(c["l2_usd_q"]), COST_SRC)
    put(f"{prefix}.l1k", sig(c["l1_usd_q"] * 1000), COST_SRC)  # $ per 1,000 queries
    put(f"{prefix}.l2k", sig(c["l2_usd_q"] * 1000), COST_SRC)
    put(f"{prefix}.gbs", fix(c["gb_s_q"], 1), COST_SRC)
    put(f"{prefix}.inv", integer(c["inv_q"]), COST_SRC)
    put(f"{prefix}.get", integer(c["s3_get_q"]), COST_SRC)
    put(f"{prefix}.putlist", integer(c["s3_put_list_q"]), COST_SRC)
    put(f"{prefix}.s3usd", sig(c["s3_usd_q"]), COST_SRC)
    put(f"{prefix}.ninit", integer(c["n_init"]), COST_SRC)
    return L, c


# ----- B (1M three-way) and C3 (10M) ---------------------------------------------------------------------

for exp, cells in (("b", [("blocks", (1, 4, 10)), ("lancedb", (1, 4, 10)), ("s3vectors", (1, 4, 10))]),
                   ("c3", [("blocks", (1,)), ("lancedb", (1, 4, 10)), ("s3vectors", (1, 4))])):
    for sysname, concs in cells:
        for k in concs:
            put_cell(f"{exp}.{sysname}.c{k}", exp, f"{sysname}_c{k}")

# S3 Vectors price components per query (marginal bands, 384 bytes per vector; cost_levels.py)
for exp in ("b", "c3"):
    c = cost[(exp, "s3vectors_c1")]
    put(f"{exp}.s3v.req", sig(c["s3v_request_usd_q"]), COST_SRC)
    put(f"{exp}.s3v.data", sig(c["s3v_data_usd_q"]), COST_SRC)
put("price.s3v.req", "2.50", "aws_s3_pricing")
put("price.x86", "0.0000166667", "aws_lambda_pricing")
put("price.arm", "0.0000133334", "aws_lambda_pricing")
put("price.req", "0.20", "aws_lambda_pricing")
put("price.s3get", fix(prices["s3_get"] * 1000, 4), "aws_s3_pricing")
put("price.s3put", fix(prices["s3_put"] * 1000, 3), "aws_s3_pricing")

# ranges across concurrency, used in prose
def rng(exp, sysname, concs, field, f=sig):
    vals = [cost[(exp, f"{sysname}_c{k}")][field] for k in concs]
    return f(min(vals)), f(max(vals))


for exp, sysname, concs in (("b", "lancedb", (1, 4, 10)), ("b", "blocks", (1, 4, 10)),
                            ("c3", "lancedb", (1, 4, 10))):
    for lvl, field in (("l1", "l1_usd_q"), ("l2", "l2_usd_q")):
        lo, hi = rng(exp, sysname, concs, field)
        put(f"{exp}.{sysname}.{lvl}min", lo, COST_SRC)
        put(f"{exp}.{sysname}.{lvl}max", hi, COST_SRC)
        lo, hi = rng(exp, sysname, concs, field, lambda x: sig(x * 1000))
        put(f"{exp}.{sysname}.{lvl}kmin", lo, COST_SRC)
        put(f"{exp}.{sysname}.{lvl}kmax", hi, COST_SRC)
    lo, hi = rng(exp, sysname, concs, "s3_get_q", integer)
    put(f"{exp}.{sysname}.getmin", lo, COST_SRC)
    put(f"{exp}.{sysname}.getmax", hi, COST_SRC)


def mean_cost(exp, sysname, concs, field):
    return statistics.mean(cost[(exp, f"{sysname}_c{k}")][field] for k in concs)


# Ratios, each from one run (cells of the same experiment), averaged over the concurrency levels
b_l1 = {s: mean_cost("b", s, (1, 4, 10), "l1_usd_q") for s in ("blocks", "lancedb")}
b_l2 = {s: mean_cost("b", s, (1, 4, 10), "l2_usd_q") for s in ("blocks", "lancedb", "s3vectors")}
c3_l1 = {"blocks": cost[("c3", "blocks_c1")]["l1_usd_q"], "lancedb": mean_cost("c3", "lancedb", (1, 4, 10), "l1_usd_q")}
c3_l2 = {"blocks": cost[("c3", "blocks_c1")]["l2_usd_q"], "lancedb": mean_cost("c3", "lancedb", (1, 4, 10), "l2_usd_q"),
         "s3vectors": mean_cost("c3", "s3vectors", (1, 4), "l2_usd_q")}
R = "computed in make_numbers.py from " + COST_SRC
put("ratio.b.l1.blocks_lancedb", ratio(b_l1["blocks"] / b_l1["lancedb"]), R)
put("ratio.b.l2.blocks_lancedb", ratio(b_l2["blocks"] / b_l2["lancedb"]), R)
put("ratio.b.l2.lancedb_s3v", ratio(b_l2["lancedb"] / b_l2["s3vectors"]), R)
put("ratio.b.l2.blocks_s3v", ratio(b_l2["blocks"] / b_l2["s3vectors"]), R)
put("ratio.c3.l1.blocks_lancedb", ratio(c3_l1["blocks"] / c3_l1["lancedb"]), R)
c3_lan_l2 = [cost[("c3", f"lancedb_c{k}")]["l2_usd_q"] for k in (1, 4, 10)]
put("ratio.c3.l2.lancedb_blocks.min", ratio(min(c3_lan_l2) / c3_l2["blocks"]), R)
put("ratio.c3.l2.lancedb_blocks.max", ratio(max(c3_lan_l2) / c3_l2["blocks"]), R)
put("ratio.c3.l2.lancedb_s3v", ratio(c3_l2["lancedb"] / c3_l2["s3vectors"]), R)
put("ratio.c3.l2.blocks_s3v", ratio(c3_l2["blocks"] / c3_l2["s3vectors"]), R)
put("ratio.c3.l1.blocks_b", ratio(c3_l1["blocks"] / cost[("c1", "c_bpt8")]["l1_usd_q"]), R)  # 10M vs 1M at bpt 8
put("ratio.c3.gbs.blocks_b", ratio(cost[("c3", "blocks_c1")]["gb_s_q"] / cost[("c1", "c_bpt8")]["gb_s_q"]), R)
put("ratio.s3v.c3_b", ratio(c3_l2["s3vectors"] / b_l2["s3vectors"]), R)
put("ratio.lancedb.c3_b.p50", ratio(lat["c3/lancedb_c1"]["p50"] / lat["b/lancedb_c1"]["p50"]), LAT_SRC)

# LanceDB in-function estimate (server_ms x 2.048 GB, aggregator) against Level 1, all B and C3 cells
diffs = []
for exp, f in (("b", "comparison_table.json"), ("c3", "comparison_table_10m.json")):
    for c in load("experiments", f"experiment_{exp}", f"results_{TAG}", f)["cells"]:
        if c["adapter"] == "lancedb":
            l1 = cost[(exp, f"lancedb_c{c['configured_concurrency']}")]["l1_usd_q"]
            diffs.append(abs(c["cost_per_query_usd"] / l1 - 1))
put("lancedb.inf_vs_l1.maxpct", integer(100 * max(diffs) + 0.4999), "comparison_table*.json vs " + COST_SRC)

# LanceDB S3 share of Level 2 at 10M
put("c3.lancedb.s3share", integer(100 * statistics.mean(
    cost[("c3", f"lancedb_c{k}")]["s3_usd_q"] / cost[("c3", f"lancedb_c{k}")]["l2_usd_q"] for k in (1, 4, 10))), R)
put("b.lancedb.s3share", integer(100 * statistics.mean(
    cost[("b", f"lancedb_c{k}")]["s3_usd_q"] / cost[("b", f"lancedb_c{k}")]["l2_usd_q"] for k in (1, 4, 10))), R)
put("b.blocks.s3share", integer(100 * statistics.mean(
    cost[("b", f"blocks_c{k}")]["s3_usd_q"] / cost[("b", f"blocks_c{k}")]["l2_usd_q"] for k in (1, 4, 10))), R)

# Blocks per-task billed durations (CloudWatch REPORT lines)
for exp, cell, key in (("b", "blocks_c1", "b"), ("c3", "blocks_c1", "c3"), ("c1", "base", "c1.base"),
                       ("c1", "c_bpt1", "c1.c_bpt1"), ("c1", "c_bpt2", "c1.c_bpt2"), ("c1", "c_bpt4", "c1.c_bpt4"),
                       ("c1", "c_bpt8", "c1.c_bpt8")):
    m, _ = per_inv_s(cost[(exp, cell)], MAP_FN)
    r, _ = per_inv_s(cost[(exp, cell)], RED_FN)
    put(f"task.{key}.map", fix(m, 2), COST_SRC)
    put(f"task.{key}.reduce", fix(r, 2), COST_SRC)
_, g = per_inv_s(cost[("c3", "blocks_c1")], MAP_FN)
put("task.c3.mapinit", integer(g["n_init"]), COST_SRC)

# Inside a Blocks map task (blocks-db-wip querying.py map_function timers): total, query fetch, per-block
# download (sequential), per-block faiss.read_index, per-block search. Medians over tasks; search per query.
def map_timers(records):
    tasks, per_query_search = [], []
    for r in records:
        mb = r["timings"].get("0_map_blocks") or []
        per_query_search.append(sum(sum(t[4]) for t in mb))
        tasks += [(t[0], sum(t[2]), sum(t[3]), sum(t[4])) for t in mb]
    med = lambda i: statistics.median(t[i] for t in tasks)
    return med(0), med(1), med(2), med(3), statistics.median(per_query_search)


for key, recs in (
        ("c1.base", [x for x in json.load(open(os.path.join(EXP, "experiment_c1", f"results_{TAG}", "measure_base.raw.json")))["records"] if x["phase"] == "measured"]),
        ("c1.c_bpt8", [x for x in json.load(open(os.path.join(EXP, "experiment_c1", f"results_{TAG}", "measure_c_bpt8.raw.json")))["records"] if x["phase"] == "measured"]),
        ("c1.c_bpt1", [x for x in json.load(open(os.path.join(EXP, "experiment_c1", f"results_{TAG}", "measure_c_bpt1.raw.json")))["records"] if x["phase"] == "measured"])):
    tot, dl, rd, se, q = map_timers(recs)
    src = f"experiment_c1/results_{TAG}/measure_{key[3:]}.raw.json (map timers)"
    put(f"inside.{key}.task", fix(tot, 2), src)
    put(f"inside.{key}.download", fix(dl, 2), src)
    put(f"inside.{key}.read", integer(rd * 1000), src + ", ms")
    put(f"inside.{key}.search", integer(se * 1000), src + ", ms")
    put(f"inside.{key}.searchq", integer(q * 1000), src + ", ms per query")

ov = [float(V[f"task.{k}.map"]) - float(V[f"inside.{k}.task"]) for k in ("c1.base", "c1.c_bpt8", "c1.c_bpt1")]
put("inside.overhead.min", fix(min(ov), 2), "task.*.map minus inside.*.task")
put("inside.overhead.max", fix(max(ov), 2), "same")

# ----- recall --------------------------------------------------------------------------------------------

for s in ("blocks", "lancedb", "s3vectors"):
    r = load("experiments", "experiment_b", f"results_{TAG}", f"recall_{s}.json")
    put(f"recall.b.{s}", pct(r["recall_at_10_pct"]), f"experiment_b/results_{TAG}/recall_{s}.json")
    put(f"recall.b.{s}.perfect", integer(r["per_query_hits"]["perfect_10of10"]), "same")
    put(f"recall.b.{s}.min", integer(r["per_query_hits"]["min"]), "same")
    put(f"recall.b.{s}.threshold", fix(r["recall_threshold_pct"], 0), "same")
    r = load("experiments", "experiment_c3", f"results_{TAG}", f"phase2_{s}.json")
    put(f"recall.c3.{s}", fix(r["recall_at_10_pct"], 1), f"experiment_c3/results_{TAG}/phase2_{s}.json")
    put(f"recall.c3.{s}.n", integer(r["recall_n"]), "same")
put("recall.b.s3vectors.missed", integer(round(10000 * (1 - float(V["recall.b.s3vectors"]) / 100))),
    "derived from recall.b.s3vectors over 1,000 queries")
# LanceDB 1M table re-ingested 17 Oct for the c10 cell (no separate gate): recall of that cell's queries
gt1m = [[int(x) for x in l.split(",")[:10]] for l in open(os.path.join(ROOT, "datasets", "deep_1m", "true_neighbours_deep_1m_1000q.csv"))]
recs = [json.loads(l) for l in open(os.path.join(EXP, "experiment_b", f"results_{TAG}", "sweep_lancedb_c10.jsonl"))]
recs = [r for r in recs if r.get("op") == "query" and r.get("ok")]
put("recall.b.lancedb.c10cell", pct(100 * sum(len(set(r["result_ids"]) & set(gt1m[r["query_index"]])) for r in recs) / (10 * len(recs))),
    f"experiment_b/results_{TAG}/sweep_lancedb_c10.jsonl vs datasets/deep_1m ground truth")
put("recall.b.lancedb.c10cell.distinct", integer(len({r["query_index"] for r in recs})), "same")
c2tab = load("experiments", "experiment_c2", f"results_{TAG}", "cold_start_table.json")
for s in ("blocks", "lancedb", "s3vectors"):
    r = load("experiments", "experiment_c2", f"results_{TAG}", f"phase1_{s}.json")
    put(f"recall.c2.{s}", fix(r["recall_at_10_pct"], 1), f"experiment_c2/results_{TAG}/phase1_{s}.json")
    put(f"recall.c2.{s}.threshold", fix(r.get("recall_threshold_pct", r.get("bar_pct", 0)), 0), "same")

# ----- C1 tuning -----------------------------------------------------------------------------------------

axis = load("experiments", "experiment_c1", f"results_{TAG}", "axis_tables.json")
for cell in ("base", "a_np1", "a_np3", "a_np5", "a_np7", "b_nw4", "b_nw8", "b_nw32",
             "c_bpt1", "c_bpt2", "c_bpt4", "c_bpt8", "c_bpt1_cap10", "c_bpt2_cap10"):
    put_cell(f"c1.{cell}", "c1", cell)
    m = load("experiments", "experiment_c1", f"results_{TAG}", f"measure_{cell}.json")
    src = f"experiment_c1/results_{TAG}/measure_{cell}.json"
    put(f"c1.{cell}.recall", fix(100.0 * m["perfect_10of10"] / m["n"], 1) if "recall_at_10_pct" not in m
        else fix(m["recall_at_10_pct"], 1), src)
    put(f"c1.{cell}.inf", sig(m["cost_billed_per_query_usd"]), src + " (in-function estimate)")
    put(f"c1.{cell}.wall", sig(m["cost_wall_per_query_usd"]), src + " (wall-clock estimate)")
    put(f"c1.{cell}.maptasks", integer(m["avg_n_map_tasks"]), src)
    if "ingest_index_s" in m:
        put(f"c1.{cell}.build", fix(m["ingest_index_s"], 1), src)
p1 = load("experiments", "experiment_c1", f"results_{TAG}", "phase1_baseline.json")
put("c1.base.build", fix(p1["ingest_index_s"], 1), f"experiment_c1/results_{TAG}/phase1_baseline.json")
C1 = {k: cost[("c1", k)]["l1_usd_q"] for k in ("base", "c_bpt1", "c_bpt2", "c_bpt4", "c_bpt8")}
INF = {k: load("experiments", "experiment_c1", f"results_{TAG}", f"measure_{k}.json")["cost_billed_per_query_usd"] for k in C1}
WALL = {k: load("experiments", "experiment_c1", f"results_{TAG}", f"measure_{k}.json")["cost_wall_per_query_usd"] for k in C1}
put("ratio.c1.l1.base_bpt8", ratio(C1["base"] / C1["c_bpt8"]), R)
put("ratio.c1.l1.bpt1_bpt8", ratio(C1["c_bpt1"] / C1["c_bpt8"]), R)
put("ratio.c1.l1.bpt1_base", ratio(C1["c_bpt1"] / C1["base"]), R)
put("ratio.c1.p50.base_bpt8", ratio(lat["c1/base"]["p50"] / lat["c1/c_bpt8"]["p50"]), LAT_SRC)
put("ratio.c1.l1_inf.base", fix(C1["base"] / INF["base"], 2), R)
put("ratio.c1.l1_inf.bpt1", fix(C1["c_bpt1"] / INF["c_bpt1"], 1), R)
put("ratio.c1.l1_inf.bpt8", fix(C1["c_bpt8"] / INF["c_bpt8"], 1), R)
put("ratio.c1.wall_l1.base", fix(WALL["base"] / C1["base"], 1), R)
put("ratio.c1.l1_wall.bpt1", fix(C1["c_bpt1"] / WALL["c_bpt1"], 1), R)
put("ratio.c1.wall_l1.bpt8", fix(WALL["c_bpt8"] / C1["c_bpt8"], 2), R)
c1_l2_bpt8 = cost[("c1", "c_bpt8")]["l2_usd_q"]
c1_l2_base = cost[("c1", "base")]["l2_usd_q"]
put("ratio.c1.l2.base_s3v", ratio(c1_l2_base / b_l2["s3vectors"]), R)
put("ratio.c1.l2.bpt8_s3v", ratio(c1_l2_bpt8 / b_l2["s3vectors"]), R)
cap = max(abs(cost[("c1", f"c_bpt{k}_cap10")]["l1_usd_q"] / cost[("c1", f"c_bpt{k}")]["l1_usd_q"] - 1) for k in (1, 2))
put("c1.cap10.maxdiff", integer(100 * cap + 0.4999), R)


# Latency grid: share of Blocks queries within 0.25 s of an even number of seconds (Lithops 2-s polling)
def grid_count(lats):
    return sum(1 for x in lats if abs(x - 2 * round(x / 2)) < 0.25)


c1_lats = []
for f in sorted(glob.glob(os.path.join(EXP, "experiment_c1", f"results_{TAG}", "measure_*.raw.json"))):
    recs = [x for x in json.load(open(f))["records"] if x["phase"] == "measured"]
    c1_lats += [x["wall_ack_utc"] - x["wall_submit_utc"] for x in recs]
put("grid.c1.on", integer(grid_count(c1_lats)), f"experiment_c1/results_{TAG}/measure_*.raw.json")
put("grid.c1.n", integer(len(c1_lats)), "same")
c3b = [json.loads(l) for l in open(os.path.join(EXP, "experiment_c3", f"results_{TAG}", "sweep_blocks_c1.jsonl"))]
c3b = [r["latency_s"] for r in c3b if r.get("op") == "query" and r.get("ok")]
put("grid.c3.six", integer(sum(1 for x in c3b if abs(x - 6) < 0.25)), f"experiment_c3/results_{TAG}/sweep_blocks_c1.jsonl")
put("grid.c3.n", integer(len(c3b)), "same")

# Polling check, 20 Oct (results_2026-10-20-poll): same-day controls and monitoring_interval 0.5 s
POLL = os.path.join(EXP, "experiment_c1", "results_2026-10-20-poll")
for f, key in (("measure_base", "base.p2"), ("measure_c_bpt8_poll2", "bpt8.p2"),
               ("measure_base_poll05", "base.p05"), ("measure_c_bpt8_poll05", "bpt8.p05")):
    recs = [x for x in json.load(open(os.path.join(POLL, f + ".raw.json")))["records"] if x["phase"] != "warmup"]
    ls = sorted(x["wall_ack_utc"] - x["wall_submit_utc"] for x in recs)
    src = f"experiment_c1/results_2026-10-20-poll/{f}.raw.json"
    put(f"poll.{key}.p50", sig(statistics.median(ls)), src)
    put(f"poll.{key}.n", integer(len(ls)), src)
    put(f"poll.{key}.grid", integer(grid_count(ls)), src)
    put(f"poll.{key}.min", sig(ls[0]), src)
    put(f"poll.{key}.max", sig(ls[-1]), src)
    if key == "bpt8.p05":
        put("poll.bpt8.p05.fast", integer(sum(1 for x in ls if x < 5.5)), src)
        put("poll.bpt8.p05.fastmax", sig(max(x for x in ls if x < 5.5)), src)
pb = load("experiments", "experiment_c1", "results_2026-10-20-poll", "phase1_baseline.json")
put("poll.recall", fix(pb["recall_at_10_pct"], 1), "experiment_c1/results_2026-10-20-poll/phase1_baseline.json")

# ----- C2 cold start -------------------------------------------------------------------------------------

for s in ("blocks", "lancedb", "s3vectors"):
    for st in ("abs_cold", "idle_cold", "warm_transition", "warm"):
        put_cell(f"c2.{s}.{st.replace('_', '')}", "c2", f"{s}_{st}", sweep=False)
    pen = lat[f"c2/{s}_idle_cold"]["p50"] - lat[f"c2/{s}_warm"]["p50"]
    put(f"c2.{s}.penalty", ("+" if pen >= 0 else "") + sig(pen, 2 if abs(pen) < 1 else 3), LAT_SRC)
    put(f"ratio.c2.l1.{s}.abs_warm", fix(cost[("c2", f"{s}_abs_cold")]["l1_usd_q"] / cost[("c2", f"{s}_warm")]["l1_usd_q"], 1)
        if s != "s3vectors" else "1.0", R)
for s, fn in (("lancedb", LDB_FN), ("blocks", MAP_FN)):
    _, g = per_inv_s(cost[("c2", f"{s}_abs_cold")], fn)
    put(f"c2.{s}.init_s", fix(g["init_ms"] / max(g["n_init"], 1) / 1000, 2), COST_SRC)
    put(f"c2.{s}.coldbilled_s", fix(g["billed_ms"] / g["n"] / 1000, 1), COST_SRC)
    put(f"c2.{s}.coldhandler_s", fix((g["billed_ms"] - g["init_ms"]) / g["n"] / 1000, 1), COST_SRC)
# Blocks' warm median lies between two polling clusters, so its penalty is given as a range over the CI
_ic = lat["c2/blocks_idle_cold"]["p50"]
put("c2.blocks.penalty.lo", fix(_ic - lat["c2/blocks_warm"]["p50_ci"][1], 0), LAT_SRC)
put("c2.blocks.penalty.hi", fix(_ic - lat["c2/blocks_warm"]["p50_ci"][0], 0), LAT_SRC)
_, g = per_inv_s(cost[("c2", "lancedb_warm")], LDB_FN)
put("c2.lancedb.warmbilled_s", fix(g["billed_ms"] / g["n"] / 1000, 1), COST_SRC)
idle_map = next(v for k, v in cost[("c2", "blocks_idle_cold")]["per_group"].items() if k.endswith(MAP_FN))
put("c2.blocks.idle.mapinit", integer(idle_map["n_init"]), COST_SRC)
put("c2.blocks.idle.mapn", integer(idle_map["n"]), COST_SRC)
put("c2.s3v.warm_over_b", fix(lat["c2/s3vectors_warm"]["p50"] / lat["b/s3vectors_c1"]["p50"], 1), LAT_SRC)

# ----- ingest --------------------------------------------------------------------------------------------

rb = load("experiments", "experiment_b", f"results_{TAG}", "recall_blocks.json")
put("ingest.1m.blocks", fix(rb["ingest_seconds"], 1), f"experiment_b/results_{TAG}/recall_blocks.json")
put("ingest.1m.blocks.upload", fix(rb["ingest_timings"]["bulk_insert_upload_s"], 1), "same")
put("ingest.1m.blocks.build", fix(rb["ingest_timings"]["bulk_insert_index_s"], 1), "same")
rl = load("experiments", "experiment_b", f"results_{TAG}", "recall_lancedb.json")
put("ingest.1m.lancedb", integer(rl["ingest_seconds"]), f"experiment_b/results_{TAG}/recall_lancedb.json")
rs = load("experiments", "experiment_b", f"results_{TAG}", "recall_s3vectors.json")
put("ingest.1m.s3vectors", integer(rs["ingest_seconds"]), f"experiment_b/results_{TAG}/recall_s3vectors.json")
put("ingest.1m.s3vectors.rate", integer(1e6 / rs["ingest_seconds"]), "same")
p2b = load("experiments", "experiment_c3", f"results_{TAG}", "phase2_blocks.json")
put("ingest.10m.blocks", fix(p2b["ingest_seconds"], 1), f"experiment_c3/results_{TAG}/phase2_blocks.json")
put("ingest.10m.blocks.upload", fix(p2b["ingest_timings"]["upload_s"], 1), "same")
put("ingest.10m.blocks.build", fix(p2b["ingest_timings"]["index_s"], 1), "same")
put("ingest.10m.blocks.cost", sig(p2b["ingest_cost_usd"], 2), "same (in-function estimate of the build)")
rl10 = load("experiments", "experiment_c3", f"results_{TAG}", "reingest_lancedb_10m.json")
put("ingest.10m.lancedb", integer(rl10["ingest_seconds"]), f"experiment_c3/results_{TAG}/reingest_lancedb_10m.json")
put("ingest.10m.lancedb.h", fix(rl10["ingest_seconds"] / 3600, 2), "same")
put("ingest.10m.lancedb.cost", sig(rl10["ingest_cost_usd"], 2), "same")
sr = load("experiments", "experiment_c3", f"results_{TAG}", "s3v_ingest_rate.json")
put("ingest.10m.s3vectors", integer(sr["ingest_seconds"]), f"experiment_c3/results_{TAG}/s3v_ingest_rate.json")
put("ingest.10m.s3vectors.h", fix(sr["ingest_seconds"] / 3600, 2), "same")
put("ingest.10m.s3vectors.rate", integer(sr["avg_rate_vec_s"]), "same")
rates = [w["rate_vec_s"] for w in sr["rate_per_30min"]]
put("ingest.10m.s3vectors.ratemin", integer(min(rates)), "same")
put("ingest.10m.s3vectors.ratemax", integer(max(rates)), "same")
# PUT is billed on logical GiB uploaded: vector data plus key (no metadata); 500 vectors per request
# (~195 KB) stays above the 128 KB per-PUT minimum. est_put_cost_usd in the file used 384 B and decimal GB.
_kb = sum((min(sr["n_vectors"], 10 ** d) - (10 ** (d - 1) if d > 1 else 0)) * d for d in range(1, 9)) / sr["n_vectors"]
assert sr["put_batch_size"] * (384 + _kb) > 128 * 1024
put("ingest.10m.s3vectors.cost", sig(sr["n_vectors"] * (384 + _kb) / 2 ** 30 * 0.20, 2),
    "same (n_vectors x (384 B + avg key) / GiB x $0.20)")
put("ingest.100m.s3vectors.h", fix(1e8 / sr["avg_rate_vec_s"] / 3600, 1), "same, projected at the measured rate")
put("ingest.ratio.lancedb", fix(rl10["ingest_seconds"] / rl["ingest_seconds"], 1), "ratio of the two ingest files")
put("ingest.ratio.s3vectors", fix(sr["ingest_seconds"] / rs["ingest_seconds"], 1), "ratio of the two ingest files")
put("ingest.1m.blocks.cost", sig(p1["ingest_cost_usd"], 2), f"experiment_c1/results_{TAG}/phase1_baseline.json")
for s in ("lancedb",):
    pass

# No break-even: the Lambda ingest costs are in-function compute only (no S3 request counts for ingest)

# D11 check against the bill: on 20 Oct replicate 1 was the only S3 Vectors traffic. Predict the processed GB per
# price band from the queries we logged and compare with Cost Explorer's usage quantities for that day.
def _avg_key(n):
    return sum((min(n, 10 ** d) - (10 ** (d - 1) if d > 1 else 0)) * d for d in range(1, len(str(n)) + 1)) / n
def _nq(exp):
    d = os.path.join(ROOT, "experiments", exp, "results_2026-10-rep1")
    n = sum(sum(1 for l in open(f) if '"op": "query"' in l) for f in glob.glob(os.path.join(d, "sweep_s3vectors_*.jsonl")))
    n += sum(json.load(open(f))["n"] for f in glob.glob(os.path.join(d, "sweep_s3vectors_*.warmup.json")))
    n += sum(len(json.load(open(f))["per_query"]) for f in glob.glob(os.path.join(d, "*s3vectors.per_query.json")))
    return n
_n1, _n10 = _nq("experiment_b"), _nq("experiment_c3")
_v1, _v10 = 384 + _avg_key(10 ** 6), 384 + _avg_key(10 ** 7)
_pred = ((_n1 * _v1 + _n10 * _v10) * 1e5 / 2 ** 30, (_n1 * 9e5 * _v1 + _n10 * 9.9e6 * _v10) / 2 ** 30)
_ce = {r["usage_type"]: r["quantity"] for r in load("scripts", "microvm", "results", "ce_2026-10-20.json")["rows"]}
_obs = (_ce["Vectors-Query-ProcessedBytes-Tier1"], _ce["Vectors-Query-ProcessedBytes-Tier2"])
_CE = "scripts/microvm/results/ce_2026-10-20.json + experiment_{b,c3}/results_2026-10-rep1"
put("d11.nq", integer(_n1 + _n10), _CE)
put("d11.maxdiff", fix(max(abs(p_ / o_ - 1) for p_, o_ in zip(_pred, _obs)) * 100, 1), _CE)
put("d11.olddiff", fix(min(((_n1 + _n10) * 1e5 * 384 / 1e9) / _obs[0], ((_n1 * 9e5 + _n10 * 9.9e6) * 384 / 1e9) / _obs[1]) * 100 - 100, 1), _CE)
put("ingest.ratio.blocks.upload", fix(p2b["ingest_timings"]["upload_s"] / rb["ingest_timings"]["bulk_insert_upload_s"], 0), "ratio of the two ingest files")

# LanceDB fragments: one per 2,000-vector insert batch (ingest records)
lb = load("experiments", "experiment_b", f"results_{TAG}", "recall_lancedb.ingest.json")["ingest_timings"]
put("lancedb.batch", integer(lb["insert_batch_size"]), f"experiment_b/results_{TAG}/recall_lancedb.ingest.json")
put("lancedb.frag.1m", integer(lb["n_invocations"]), "same (one fragment per insert invocation)")
put("lancedb.frag.10m", integer(rl10["ingest_timings"]["n_invocations"]), f"experiment_c3/results_{TAG}/reingest_lancedb_10m.json")

# ----- post-ingest checks (1M October indexes; 10M from the September checks on the retained indexes) ------

PI = os.path.join("scripts", "fixes", f"results_{TAG}", "deep_1m_2026-10-ec2")
for s in ("blocks", "lancedb", "s3vectors"):
    d = load(PI, f"postingest_{s}.json")
    sq = d["self_query"]
    put(f"pi.1m.{s}.self", f"{sq['top1_exact']} of {sq['n']}", f"{PI}/postingest_{s}.json")
    if "values" in d:
        put(f"pi.1m.{s}.values", integer(d["values"]["n"]), "same")
for s in ("blocks", "lancedb", "s3vectors"):
    d = load("scripts", "fixes", "results", f"postingest_{s}.json")
    sq = d["self_query"]
    put(f"pi.10m.{s}.self", f"{sq['top1_exact']} of {sq['n']}", f"scripts/fixes/results/postingest_{s}.json")
    if "values" in d:
        put(f"pi.10m.{s}.values", integer(d["values"]["n"]), "same")

# ----- S3 Vectors repeated query vectors (F43) -----------------------------------------------------------

for k in (1, 4, 10):
    recs = [json.loads(l) for l in open(os.path.join(EXP, "experiment_b", f"results_{TAG}", f"sweep_s3vectors_c{k}.jsonl"))]
    recs = [r for r in recs if r.get("op") == "query" and r.get("ok")]
    seen, first, rep = set(), [], []
    for r in sorted(recs, key=lambda r: r["submit_ts"]):
        (rep if r["query_index"] in seen else first).append(r["latency_s"])
        seen.add(r["query_index"])
    src = f"experiment_b/results_{TAG}/sweep_s3vectors_c{k}.jsonl"
    put(f"s3v.b.c{k}.distinct", integer(len(seen)), src)
    put(f"s3v.b.c{k}.repeatpct", integer(100 * len(rep) / len(recs)), src)
    put(f"s3v.b.c{k}.p50first", sig(statistics.median(first)), src)
    put(f"s3v.b.c{k}.p50repeat", sig(statistics.median(rep)), src)
    q = len(recs) // 4
    ordered = [r["latency_s"] for r in sorted(recs, key=lambda r: r["submit_ts"])]
    put(f"s3v.b.c{k}.p50q1", sig(statistics.median(ordered[:q])), src)
    put(f"s3v.b.c{k}.p50q4", sig(statistics.median(ordered[-q:])), src)
put("ratio.s3v.b.c1_c4", fix(lat["b/s3vectors_c1"]["p50"] / lat["b/s3vectors_c4"]["p50"], 1), LAT_SRC)

# ----- figure coordinates (log10 scale, 1 unit = 1 decade, origin at 0.03 s) -----------------------------

def ycoord(x):
    return f"{1.6 * (math.log10(x) - math.log10(0.03)):.2f}"


for key, lk in (("s3v.1m", "b/s3vectors_c1"), ("s3v.10m", "c3/s3vectors_c1"), ("lancedb.1m", "b/lancedb_c1"),
                ("lancedb.10m", "c3/lancedb_c1"), ("blocks.1m.base", "b/blocks_c1"), ("blocks.1m.bpt8", "c1/c_bpt8"),
                ("blocks.10m", "c3/blocks_c1")):
    put(f"fig.{key}.y", ycoord(lat[lk]["p50"]), LAT_SRC)

# ----- write ---------------------------------------------------------------------------------------------

with open(OUT, "w") as f:
    f.write("% Generated by scripts/make_numbers.py from the result files of run " + TAG + ". Do not edit.\n")
    f.write("\\makeatletter\n")
    f.write("\\newcommand{\\defnum}[2]{\\expandafter\\def\\csname num@#1\\endcsname{#2}}\n")
    f.write("\\newcommand{\\V}[1]{\\ifcsname num@#1\\endcsname\\csname num@#1\\endcsname"
            "\\else\\PackageError{numbers}{Undefined number #1}{Run scripts/make_numbers.py}\\fi}\n")
    f.write("\\makeatother\n")
    for k in sorted(V):
        f.write(f"\\defnum{{{k}}}{{{V[k]}}}% {SRC[k]}\n")
print(f"wrote {OUT}: {len(V)} numbers")

# ----- stale-value check (F41 D.3): August values and retired phrases ------------------------------------

STALE = ["13.7", "13.66", "13.1 ", "13.12", "13.4", "14 times", "190", "74 times", "74.4", "50 times", "50.4",
         "5.5 hours", "5 hours 35", "366", "1045", "99.85", "97.30", "10.52", "10.5 seconds", "523", "0.000186",
         "0.000134", "0.000126", "0.0000025 ", "560 times", "4,513", "4{,}513", "20{,}111", "1{,}360", "4{,}114",
         "honest", "billed model", "billed cost", "near-deterministic", "corpus-invariant", "about 2.5 times",
         "hundreds of seconds", "laptop", "0.459", "0.349", "0.251", "0.173", "0.171", "slow-start",
         "account limit of 10", "at a limit of 10", "client placement adds little", "recovers its extra ingest",
         "latency-sensitive"]
tex = open(os.path.join(ROOT, "report", "thesis.tex")).read().splitlines()
hits = [(i + 1, s, l.strip()[:110]) for i, l in enumerate(tex) for s in STALE if s in l]
if hits:
    print(f"{len(hits)} stale-value hits in thesis.tex:")
    for i, s, l in hits:
        print(f"  l.{i} [{s}] {l}")
else:
    print("no stale-value hits")
