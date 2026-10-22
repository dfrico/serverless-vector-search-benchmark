"""Turn cloudwatch_billed.json into per-query 'actual Lambda charge' vs the thesis models.

Per-query sums from the query windows are unreliable (windows cut off or overlap
neighbouring cells: e.g. baseline shows 43 reduce invocations for 53 queries), so we use
AWS's per-invocation average @billedDuration (which includes INIT for container images)
x the number of tasks each query runs. Memory: map 3.008 GB, reduce 2.048 GB (Table 3.1).
"""
import json, os, glob
HERE = os.path.dirname(os.path.abspath(__file__)); ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
X86, ARM, REQ = 0.0000166667, 0.0000133334, 0.2e-6
cw = json.load(open(os.path.join(HERE, "results", "cloudwatch_billed.json")))
TASKS = {"measure_base.json": 1, "measure_c_bpt8.json": 2, "measure_c_bpt4.json": 4,
         "measure_c_bpt2.json": 8, "measure_c_bpt1.json": 16}
rows = {}
for f, n_map in TASKS.items():
    c = cw["c1"][f]; m = json.load(open(os.path.join(ROOT, "experiments/experiment_c1/results", f)))
    map_s = float(c["blocks_map"]["avg_billed_ms"]) / 1000
    red_s = float(c["blocks_reduce"]["avg_billed_ms"]) / 1000
    model_task_s = m["avg_billed_map_s"] / n_map
    actual = n_map * map_s * 3.008 * X86 + red_s * 2.048 * X86 + (n_map + 1) * REQ
    rows[f] = dict(map_tasks=n_map, aws_map_s_per_invocation=round(map_s, 3),
                   model_map_s_per_task=round(model_task_s, 3),
                   wrapper_overhead_s_per_map_task=round(map_s - model_task_s, 3),
                   aws_reduce_s=round(red_s, 3),
                   model_cost=m["cost_billed_per_query_usd"], wall_cost=m["cost_wall_per_query_usd"],
                   actual_cost=round(actual, 7),
                   actual_over_model=round(actual / m["cost_billed_per_query_usd"], 2),
                   wall_over_actual=round(m["cost_wall_per_query_usd"] / actual, 2),
                   init_share=f'{c["blocks_map"].get("n_init","0")}/{c["blocks_map"]["n"]} map invocations had INIT, avg {c["blocks_map"].get("avg_init_ms","-")} ms',
                   max_mem_gb=round(int(c["blocks_map"]["max_mem_bytes"]) / 1e9, 2))
# 10M Blocks (20 map tasks)
c = cw["c3"]["sweep_blocks_c1.jsonl"]
map_s = float(c["blocks_map"]["avg_billed_ms"]) / 1000; red_s = float(c["blocks_reduce"]["avg_billed_ms"]) / 1000
actual10 = 20 * map_s * 3.008 * X86 + red_s * 2.048 * X86 + 21 * REQ
rows["10M_blocks"] = dict(map_tasks=20, aws_map_s_per_invocation=round(map_s, 3),
                          invocations_per_query=round(int(c["blocks_map"]["n"]) / 63, 2),
                          model_cost=1.4754e-3, actual_cost=round(actual10, 7),
                          actual_over_model=round(actual10 / 1.4754e-3, 2))
# 10M LanceDB: compare AWS billed per invocation with the handler time (server_ms) used by the model
srv = []
for jl in glob.glob(os.path.join(ROOT, "experiments/experiment_c3/results/sweep_lancedb_c*.jsonl")):
    for line in open(jl):
        r = json.loads(line); t = r.get("adapter_timings") or {}
        if r.get("ok") and "server_ms" in t: srv.append(t["server_ms"] / 1000)
c = cw["c3"]["sweep_lancedb_c1.jsonl"]["lancedb"]
aws_s = float(c["avg_billed_ms"]) / 1000
rows["10M_lancedb"] = dict(aws_billed_s_per_invocation=round(aws_s, 2),
                           handler_server_s_mean=round(sum(srv) / len(srv), 2),
                           model_cost=1.3994e-3, actual_cost=round(aws_s * 2.048 * ARM + REQ, 7),
                           actual_over_model=round((aws_s * 2.048 * ARM + REQ) / 1.3994e-3, 2),
                           init=f'{c["n_init"]} INITs, avg {float(c["avg_init_ms"])/1000:.2f} s (billed)',
                           max_mem_gb=round(int(c["max_mem_bytes"]) / 1e9, 2))
json.dump(rows, open(os.path.join(HERE, "results", "cloudwatch_analysis.json"), "w"), indent=1)
print(json.dumps(rows, indent=1))
