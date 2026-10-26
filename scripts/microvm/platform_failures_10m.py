"""Every 10M MicroVM launch and resume, 21-26 Oct (incl. the 26 Oct E2 re-run, results_2026-10-mvm6), and the ones AWS ended on its own (findings 31-33).

Offline: reads our result records only. Two events have no record file and are listed by hand below with their source.
Launch = one run-microvm call; it either reached RUNNING or was TERMINATED before. Resume = a suspend followed by a
resume hook (idle suspends from suspend_census_10m.json, plus the two explicit suspends of the 21 Oct gates).

Writes scripts/microvm/results/platform_failures_10m.json.
"""
from __future__ import annotations

import glob
import json
import os
import re

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
OUT = os.path.join(ROOT, "scripts", "microvm", "results", "platform_failures_10m.json")
C2 = "experiments/experiment_c2/results_2026-10-mvm"

# No record file: probe b's driver looped on the TERMINATED state and was killed (finding 32, observed 24 Oct);
# the E1 v6 c32 cell's first launch was retried by the sweep, which keeps only the second MicroVM (get_microvm, 26 Oct).
MANUAL = [
    {"date": "2026-10-24", "run": "hook probe plainb", "image": "thesis-lancedb-mvm-deep10m-trace", "ok": False,
     "reason": "Internal service error.", "source": "finding 32 (session readout 24 Oct; driver had no record)"},
    {"date": "2026-10-25", "run": "E1 v6 c32, attempt 1", "image": "thesis-lancedb-mvm-deep10m-v6", "ok": False,
     "microvm_id": "microvm-6b48c685-40e4-3fd8-bdf4-3e357c3a313d", "reason": "Internal service error.",
     "source": "get_microvm readout 26 Oct (sweep retried; not in the cell's lifecycle file)"},
]


def rel(p):
    return os.path.relpath(p, ROOT)


def main():
    launches = []

    def add(date, run, image, ok, src, reason=None, mid=None):
        launches.append({"date": date, "run": run, "image": image, "ok": ok, "reason": reason, "microvm_id": mid,
                         "source": src})

    for f in sorted(glob.glob(os.path.join(ROOT, "scripts/microvm/results/gate_deep_10m_2026102*.json"))):
        d = json.load(open(f))
        add("2026-10-21", "gate", d["backend_version"]["image_arn"].rsplit(":", 1)[1], True, rel(f))
    for tag, date in (("2026-10-mvm", "2026-10-22"), ("2026-10-mvm6", "2026-10-25")):
        for f in sorted(glob.glob(os.path.join(ROOT, f"experiments/experiment_c3/results_{tag}/sweep_lancedb_microvm_c*.lifecycle.json"))):
            d = json.load(open(f))
            m = json.load(open(f.replace(".lifecycle.json", ".manifest.json")))
            image = json.dumps(m).split("microvm-image:")[1].split('"')[0]
            add(date, "E1 " + os.path.basename(f).split("_")[-1].split(".")[0], image, d["state"] in ("TERMINATED", "RUNNING"),
                rel(f), mid=d["microvm_id"])
    # 22 Oct first attempt: three launches answered, the fourth was TERMINATED before RUNNING (reason not recorded).
    log = open(os.path.join(ROOT, C2, "failed_attempt1/phase3_attempt1.log")).read()
    part = log.split("e2_microvm.py --dataset deep_10m")[1]  # between the 10M run's START and EXIT lines
    for _ in re.findall(r"^launch d=\d+ k=\d+: first [\d.]+s", part, re.M):
        add("2026-10-22", "E2 attempt 1", "thesis-lancedb-mvm-deep10m", True, f"{C2}/failed_attempt1/phase3_attempt1.log")
    for mid in re.findall(r"MicroVM (microvm-[0-9a-f-]+) is TERMINATED while waiting for RUNNING", part):
        add("2026-10-22", "E2 attempt 1", "thesis-lancedb-mvm-deep10m", False, f"{C2}/failed_attempt1/phase3_attempt1.log",
            reason="not recorded (aged out of the API)", mid=mid)
    for f in sorted(glob.glob(os.path.join(ROOT, C2, "e2_lancedb_microvm_deep_10m*.manifest.json"))
                    + glob.glob(os.path.join(ROOT, C2 + "6", "e2_lancedb_microvm_deep_10m*.manifest.json"))):
        d = json.load(open(f))
        date = d["started_utc"][:10]
        for L in d["launches"]:
            add(date, "E2 launch", L["backend_version"]["image_arn"].rsplit(":", 1)[1], True, rel(f),
                mid=L["backend_version"]["microvm_id"])
        for L in d["failed_launches"]:
            add(date, "E2 launch", "thesis-lancedb-mvm-deep10m", False, rel(f), reason=L["state_reason"],
                mid=L["microvm_id"])
        if d.get("resume_transitions"):
            t = d["resume_transitions"]
            img = json.dumps(d).split("microvm-image:")[1].split('"')[0] if "microvm-image:" in json.dumps(d) else "thesis-lancedb-mvm-deep10m"
            add(date, "E2 resume run", img, True, rel(f), mid=t["microvm_id"])
    for f in sorted(glob.glob(os.path.join(ROOT, "scripts/microvm/results/hook_probe/hook_probe_deep_10m_*.json"))):
        d = json.load(open(f))
        if d.get("microvm_id"):
            add(d["started_utc"][:10], f"hook probe {d['tag']}", d["image"], True, rel(f), mid=d["microvm_id"])
    for e in MANUAL:
        add(e["date"], e["run"], e["image"], e["ok"], e["source"], reason=e["reason"], mid=e.get("microvm_id"))

    census = json.load(open(os.path.join(ROOT, "scripts/microvm/results/suspend_census_10m.json")))
    gates = 2  # explicit suspend + resume in each 21 Oct gate (gate_deep_10m_*.json: suspend_s, first_after_resume)
    resumes = sum(r["suspends_ok"] for r in census["rows"]) + gates
    lost_at_resume = sum(r["internal_error_termination"] for r in census["rows"])
    hook_timeouts = sum(r["suspend_failed"] for r in census["rows"])
    failed = [L for L in launches if not L["ok"]]
    summary = {
        "launches": len(launches),
        "launches_terminated_before_running": len(failed),
        "launches_internal_error": sum(1 for L in failed if (L["reason"] or "").startswith("Internal service error")),
        "launches_reason_unrecorded": sum(1 for L in failed if not (L["reason"] or "").startswith("Internal service error")),
        "resumes": resumes,
        "resumes_internal_error": lost_at_resume,
        "suspend_hook_timeouts_21oct_build": hook_timeouts,
        "note": "resumes = idle suspends followed by a resume hook (census) + 2 explicit gate suspends; the lost "
                "resume's hook was answered, so it is counted among the resumes",
    }
    json.dump({"summary": summary, "launches": launches}, open(OUT, "w"), indent=1)
    print(json.dumps(summary, indent=1))
    for L in failed:
        print(L["date"], L["run"], L["image"], L["reason"])
    print("->", rel(OUT))


if __name__ == "__main__":
    main()
