"""Give the next C1 cell fresh Lithops worker containers (2026-10-18).

The harness creates a new Lithops executor per query, and the Lithops worker unpacks the job's
module (~0.6 MB) under /tmp/lithops-root/modules/<job> without removing it. A warm container
that has served a few hundred queries therefore fills most of the 512 MB /tmp, and a cell whose
map task downloads all 16 blocks (~400 MB) then fails with ENOSPC (18 Oct, cell a_np7, after 352
invocations in one container). Bumping an env var makes AWS replace the execution environments,
so each cell starts with an empty /tmp. Blocks code and function configuration are unchanged: the
env var is restored right after, using the C2 cold-start helpers. Each C1 cell discards its own
warm-up queries, so the cold first call is not measured.

    ./ec_blocks.sh c1_fresh_workers.py
"""
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "experiment_c2"))
import c2_common as C2  # noqa: E402

names = C2.discover_blocks_worker_lambdas()
t0 = time.time()
C2.force_cold(names, marker=f"c1-fresh-{int(t0)}")
C2.restore_env(names)
print(f"fresh workers: {', '.join(names)} ({time.time() - t0:.1f}s)", flush=True)
