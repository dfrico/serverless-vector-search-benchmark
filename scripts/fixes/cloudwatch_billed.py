"""Compare the thesis' task-compute ("billed") model with what Lambda actually billed.

Uses CloudWatch Logs Insights over the Lambda REPORT lines (@billedDuration, @initDuration,
@maxMemoryUsed) for the time windows of the C1 cells (1M Blocks tuning) and the C3 sweep
(10M). Read-only; Logs Insights costs $0.005/GB scanned (pennies here).

Output: scripts/fixes/results/cloudwatch_billed.json
If a log group no longer has the data (retention), the entry says so.
"""
import datetime as dt, json, glob, os, time
import boto3

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
OUT = os.path.join(HERE, "results"); os.makedirs(OUT, exist_ok=True)
logs = boto3.client("logs", region_name="us-east-1")

GROUPS = {
    "blocks_map": "/aws/lambda/lithops-worker-rb26-363-11170a3116",
    "blocks_reduce": "/aws/lambda/lithops-worker-rb26-363-600f82b870",
    "lancedb": "/aws/lambda/thesis-lancedb-lambda",
}
QUERY = ('filter @type = "REPORT" | stats count(*) as n, avg(@billedDuration) as avg_billed_ms, '
         'sum(@billedDuration) as sum_billed_ms, avg(@duration) as avg_dur_ms, '
         'count(@initDuration) as n_init, avg(@initDuration) as avg_init_ms, '
         'max(@maxMemoryUsed) as max_mem_bytes')


def insights(group, start, end):
    try:
        qid = logs.start_query(logGroupName=group, startTime=int(start), endTime=int(end),
                               queryString=QUERY)["queryId"]
    except logs.exceptions.ResourceNotFoundException:
        return {"error": "log group not found"}
    while True:
        r = logs.get_query_results(queryId=qid)
        if r["status"] in ("Complete", "Failed", "Cancelled", "Timeout"):
            break
        time.sleep(1)
    rows = [{f["field"]: f["value"] for f in row} for row in r.get("results", [])]
    return rows[0] if rows else {"n": "0"}


def ts(s):
    return dt.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc).timestamp()


out = {"c1": {}, "c3": {}}
# ---- C1 cells: window = started_utc .. started + sum(latencies) + warmups + margin
for f in sorted(glob.glob(os.path.join(ROOT, "experiments/experiment_c1/results/measure_*.json"))):
    d = json.load(open(f))
    if "started_utc" not in d:
        continue
    lat = d.get("raw_latencies_s") or []
    start = ts(d["started_utc"])
    end = start + sum(lat) + 3 * (sum(lat) / max(len(lat), 1)) + 60
    cell = {"n_queries": d.get("n"), "warmup": d.get("warmup", 0),
            "model_billed_map_s_per_query": d.get("avg_billed_map_s"),
            "model_cost_per_query": d.get("cost_billed_per_query_usd"),
            "window_utc": [d["started_utc"], dt.datetime.utcfromtimestamp(end).isoformat() + "Z"]}
    for g in ("blocks_map", "blocks_reduce"):
        cell[g] = insights(GROUPS[g], start, end)
    # per-query actual billed seconds (map+reduce), comparable to the model
    try:
        nq = (d.get("n") or 0) + (d.get("warmup") or 0)
        s_ms = sum(float(cell[g].get("sum_billed_ms", 0)) for g in ("blocks_map", "blocks_reduce"))
        cell["actual_billed_s_per_query"] = s_ms / 1000 / nq
        cell["actual_cost_per_query"] = (s_ms / 1000 / nq) * 3.008 * 0.0000166667 + \
            (sum(float(cell[g].get("n", 0)) for g in ("blocks_map", "blocks_reduce")) / nq) * 0.2e-6
    except Exception as e:  # noqa: BLE001
        cell["calc_error"] = str(e)
    out["c1"][os.path.basename(f)] = cell
    print(os.path.basename(f), json.dumps(cell)[:400])

# ---- C3 sweep cells (10M): window from manifest timestamp + sum of latencies
for m in sorted(glob.glob(os.path.join(ROOT, "experiments/experiment_c3/results/sweep_*.manifest.json"))):
    man = json.load(open(m)); jl = m.replace(".manifest.json", ".jsonl")
    if man["adapter"].startswith("s3v"):
        continue
    lat = [json.loads(l)["latency_s"] for l in open(jl) if l.strip()]
    start = man["timestamp"] - 30; end = man["timestamp"] + sum(lat) + 120
    groups = ["blocks_map", "blocks_reduce"] if man["adapter"] == "blocks" else ["lancedb"]
    cell = {"n_queries": len(lat), "window_utc": man["timestamp_iso"]}
    for g in groups:
        cell[g] = insights(GROUPS[g], start, end)
    out["c3"][os.path.basename(jl)] = cell
    print(os.path.basename(jl), json.dumps(cell)[:400])

json.dump(out, open(os.path.join(OUT, "cloudwatch_billed.json"), "w"), indent=1)
print("->", os.path.join(OUT, "cloudwatch_billed.json"))
