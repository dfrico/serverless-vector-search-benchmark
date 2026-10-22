"""
Workload generator for the serverless vector-DB benchmark harness.

Decides WHAT operations to send and WHEN. Backend-neutral: it drives any
`BenchmarkVectorClient` (harness/client.py) and consults
`client.capabilities()` to decide which workloads are valid — it never imports a
concrete adapter.

Single-process asyncio. The client methods are synchronous and BLOCKING (a Blocks
query runs a whole Lithops map/reduce job), so each op is dispatched to a worker
thread via `asyncio.to_thread`; `max_in_flight` (an asyncio.Semaphore) caps how
many run concurrently and provides back-pressure.

Three patterns (see context/2026-05-10_Prompt.md "Updated experiment shape"):
  - constant : fixed arrival rate, fixed concurrency.
  - burst    : N ops as fast as concurrency allows, then idle, repeat.
  - mixed    : interleaved insert/query streams at a configurable ratio.

Operations in scope for day 3: `query` (one vector at a time) and `bulk_insert`
(a small batch). `query_batch`, `delete`, `put_pending` are out of scope.

Closed-loop arrival model: the producer acquires a concurrency slot BEFORE
issuing the next op, so when the backend can't keep up with the nominal rate the
producer is throttled (fewer ops issued) rather than piling up an unbounded
backlog. This bounds wall-clock to ~`duration` + one op tail and makes the
achievable op count = throughput-limited, which is the honest behaviour to
measure. Consequence: on a slow backend the *nominal* `ops_per_second` is an
upper bound, not a guarantee — realised throughput can be far lower.

Reproducibility / RNG contract (day 6): the generator owns a single
`random.Random(config.seed)` instance, `self._rng`. ANY sampled choice the
generator makes — now or in future (Zipfian query distribution, jittered arrival
times, insert-batch shuffling, sampled query selection) — MUST draw from
`self._rng`, NEVER the global `random` module, so a run is fully reproducible
from `(config, seed)` alone. `seed=None` means "no explicit seed": sampling
falls back to deterministic cursor order (the historical day-3 behaviour), so
existing seed-less runs are unchanged. The seed is recorded in the manifest.
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import csv
import os
import random
import tempfile
import time
from dataclasses import dataclass, field
from typing import AsyncIterator, Dict, List, Optional, Sequence, Tuple

# Interface type (base contract). Absolute package import; the entry-point
# script puts the repo root on sys.path (see harness/README.md).
from harness.client import BenchmarkVectorClient


@dataclass
class OpResult:
    """One completed operation. The generator yields these to an in-memory
    collector; the day-5 sink persists them to JSONL."""
    op: str                                  # "query" | "bulk_insert"
    adapter: str                             # client.name of the backend that ran it ("blocks")
    submit_ts: float                         # monotonic seconds (slot acquired -> dispatch)
    ack_ts: float                            # monotonic seconds (op returned)
    latency_s: float                         # ack_ts - submit_ts
    ok: bool
    error: Optional[str]
    adapter_timings: dict                    # opaque timings dict from the client
    payload_meta: dict                       # {"top_k": 10} | {"n_vectors": 100, "build_index": False}
    # Added 2026-10-16 for traceability (all optional, so older JSONL still parses):
    wall_submit_utc: Optional[float] = None  # epoch seconds at dispatch (aligns with CloudWatch)
    wall_ack_utc: Optional[float] = None     # epoch seconds when the op returned
    query_index: Optional[int] = None        # row of the query pool used (query ops)
    result_ids: Optional[list] = None        # returned top-k ids, in rank order (query ops)
    result_dists: Optional[list] = None      # matching distances (adapter-native units)


@dataclass
class WorkloadConfig:
    """Parameters across the three patterns (only the relevant subset is used per pattern)."""
    pattern: str                             # "constant" | "burst" | "mixed"
    duration_s: float = 5.0                  # constant / mixed
    ops_per_second: float = 5.0              # constant
    max_in_flight: int = 2
    top_k: int = 10
    # burst
    burst_size: int = 10
    idle_s: float = 2.0
    cycles: int = 3
    # mixed
    insert_ratio: float = 0.1                # fraction of ops that are inserts
    insert_batch_size: int = 100
    insert_build_index: bool = False         # day-3 fast insert: upload only (see findings/10)
    # reproducibility
    seed: Optional[int] = None               # RNG seed; None = deterministic cursor order (see below)


class WorkloadGenerator:
    """
    Drives a `BenchmarkVectorClient` through a workload pattern, yielding one
    `OpResult` per completed op as they finish.

    Args:
        client: query client (any BenchmarkVectorClient). Must be `setup()` done.
        query_pool: list of query vectors; the generator cycles through it.
        insert_client: client to receive inserts. Defaults to `client`. The smoke
            test points this at a SEPARATE ephemeral dataset so inserts don't
            mutate the queried index (see findings/10 — Blocks' only indexed
            insert is a destructive ~34s rebuild; the fast path uploads to its own
            dataset).
        insert_pool: list of (id, vector) pairs to draw inserts from (held-out IDs
            not in the queried index). Required for mixed if inserts will run.
    """

    def __init__(
        self,
        client: BenchmarkVectorClient,
        query_pool: Sequence[Sequence[float]],
        *,
        insert_client: Optional[BenchmarkVectorClient] = None,
        insert_pool: Optional[Sequence[Tuple[int, Sequence[float]]]] = None,
    ):
        if not query_pool:
            raise ValueError("query_pool must be non-empty")
        self.client = client
        self.query_pool = list(query_pool)
        self.insert_client = insert_client if insert_client is not None else client
        self.insert_pool = list(insert_pool) if insert_pool else []
        self._qi = 0       # query pool cursor
        self._last_query_index: Optional[int] = None
        self._ii = 0       # insert pool cursor
        # Seeded RNG for all sampled choices; bound at run() from config.seed.
        # None until then / when no seed is set (deterministic cursor order).
        self._rng: Optional[random.Random] = None

    # ----- capability gating -------------------------------------------------

    def _can_insert(self) -> bool:
        """
        True iff the insert client can ingest mid-run. Per the interface, an
        adapter that can ingest declares a non-empty `ingest_modes`. We also
        require that at least one of the two ingest entry points is actually
        callable for the declared mode.
        """
        caps = self.insert_client.capabilities()
        return bool(caps.ingest_modes) and bool(self.insert_pool)

    def effective_concurrency(self, config: WorkloadConfig) -> int:
        """
        Public: the concurrency this generator WILL use for `config`, capped at
        the backend(s)' `max_safe_concurrency`. Infers whether the insert client's
        cap applies from the pattern (only "mixed" runs an insert stream), so the
        sink can read the true effective value straight from the generator instead
        of recomputing it (removes the finding-12 item-4 workaround). Pure/no
        side-effects beyond the cap-applied log line in `_effective_concurrency`.
        """
        return self._effective_concurrency(config, include_insert=(config.pattern == "mixed"))

    def _effective_concurrency(self, config: WorkloadConfig, *, include_insert: bool) -> int:
        """
        Cap the configured `max_in_flight` at what the backend(s) can safely run
        concurrently (`capabilities().max_safe_concurrency`). A backend that is
        concurrency-unsafe (e.g. Blocks-localhost, which collides on a shared
        /tmp/index_0.ann — finding 10) declares 1, so we serialise it rather than
        issuing colliding requests.

        For the mixed pattern the insert stream may hit a different client, so we
        take the min of both caps when `include_insert` and the insert client
        differs. Logs a single line when the cap actually lowers the configured
        value, so the operator can see it was applied.
        """
        cap = self.client.capabilities().max_safe_concurrency
        if include_insert and self.insert_client is not self.client:
            cap = min(cap, self.insert_client.capabilities().max_safe_concurrency)
        effective = min(config.max_in_flight, cap)
        if effective < config.max_in_flight:
            print(f"[workload] concurrency capped: max_in_flight={config.max_in_flight} "
                  f"-> effective={effective} (backend max_safe_concurrency={cap}); "
                  f"pattern={config.pattern}")
        return effective

    # ----- single-op execution (runs in a worker thread) ---------------------

    def _next_query_vec(self) -> List[float]:
        # Seeded: sample uniformly from the pool via the run's RNG (reproducible
        # from the seed). Unseeded: deterministic cursor order (historical day-3
        # behaviour). Either way one vector per call.
        # randrange(n) draws the same sequence as choice(pool) for a given seed
        # (both call _randbelow(len)), so recording the index keeps old runs comparable.
        if self._rng is not None:
            i = self._rng.randrange(len(self.query_pool))
        else:
            i = self._qi % len(self.query_pool)
            self._qi += 1
        self._last_query_index = i
        return list(self.query_pool[i])

    def _next_insert_batch(self, n: int) -> List[Tuple[int, List[float]]]:
        batch = []
        for _ in range(n):
            vid, vec = self.insert_pool[self._ii % len(self.insert_pool)]
            batch.append((int(vid), list(vec)))
            self._ii += 1
        return batch

    def _do_query_blocking(self, vec: List[float], top_k: int):
        return self.client.query(vec, top_k=top_k)  # -> (hits, timings)

    def _do_insert_blocking(self, batch: List[Tuple[int, List[float]]], build_index: bool):
        """
        Backend-neutral ingest: prefer the in-memory iterable path; if the adapter
        is file-native ("path" mode) serialise the batch to a temp CSV and use
        bulk_insert_from_path. Returns (None, timings).
        """
        caps = self.insert_client.capabilities()
        if "iterable" in caps.ingest_modes:
            return self.insert_client.bulk_insert(iter(batch), build_index=build_index)
        if "path" in caps.ingest_modes:
            tmp = tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False, newline="")
            try:
                w = csv.writer(tmp)
                for vid, vec in batch:
                    w.writerow([vid, " ".join(str(x) for x in vec)])
                tmp.close()
                return self.insert_client.bulk_insert_from_path(tmp.name, build_index=build_index)
            finally:
                os.unlink(tmp.name)
        raise RuntimeError("insert client declares no usable ingest mode")

    async def _run_one(self, op: str, *, top_k: int, batch_size: int, build_index: bool) -> OpResult:
        submit_ts = time.monotonic()
        wall_submit = time.time()
        ok, error, timings, payload_meta = True, None, {}, {}
        query_index = result_ids = result_dists = None
        # Stamp the adapter that actually runs this op: queries hit self.client,
        # inserts hit self.insert_client (may be a different backend/dataset).
        adapter = self.insert_client.name if op == "bulk_insert" else self.client.name
        try:
            if op == "query":
                vec = self._next_query_vec()
                query_index = self._last_query_index
                payload_meta = {"top_k": top_k}
                hits, timings = await asyncio.to_thread(self._do_query_blocking, vec, top_k)
                result_ids = [int(h[0]) for h in hits]
                result_dists = [float(h[1]) for h in hits]
            elif op == "bulk_insert":
                batch = self._next_insert_batch(batch_size)
                payload_meta = {"n_vectors": len(batch), "build_index": build_index}
                _none, timings = await asyncio.to_thread(self._do_insert_blocking, batch, build_index)
            else:  # pragma: no cover
                raise ValueError(f"unknown op {op!r}")
        except Exception as e:  # noqa: BLE001 — record, don't crash the workload
            ok, error, timings = False, f"{type(e).__name__}: {e}", {}
        ack_ts = time.monotonic()
        return OpResult(
            op=op, adapter=adapter, submit_ts=submit_ts, ack_ts=ack_ts,
            latency_s=ack_ts - submit_ts,
            ok=ok, error=error, adapter_timings=timings or {}, payload_meta=payload_meta,
            wall_submit_utc=wall_submit, wall_ack_utc=time.time(),
            query_index=query_index, result_ids=result_ids, result_dists=result_dists,
        )

    # ----- pattern driver ----------------------------------------------------

    async def run(self, config: WorkloadConfig) -> AsyncIterator[OpResult]:
        """Yield OpResults as ops complete, per `config.pattern`."""
        # Bind the run's RNG from the seed (None -> deterministic cursor order).
        # Fresh instance per run() so repeated runs with the same seed match.
        self._rng = random.Random(config.seed) if config.seed is not None else None
        # Ops run via asyncio.to_thread, i.e. the loop's default executor, whose
        # size is min(32, cpu_count + 4). On a 2-vCPU client that is 6, which
        # silently capped c=10 cells at 6 in flight (EC2 B LanceDB c10, 2026-10-16).
        # Size it so the semaphore, not the thread pool, sets the concurrency.
        need = self._effective_concurrency(config, include_insert=(config.pattern == "mixed"))
        workers = max(need + 2, min(32, (os.cpu_count() or 1) + 4))
        asyncio.get_running_loop().set_default_executor(
            concurrent.futures.ThreadPoolExecutor(max_workers=workers))
        if config.pattern == "constant":
            async for r in self._drive_constant(config):
                yield r
        elif config.pattern == "burst":
            async for r in self._drive_burst(config):
                yield r
        elif config.pattern == "mixed":
            async for r in self._drive_mixed(config):
                yield r
        else:
            raise ValueError(f"unknown pattern {config.pattern!r}")

    async def _drain(self, sem: asyncio.Semaphore, q: "asyncio.Queue[OpResult]",
                     producer: "asyncio.Task", tasks: List["asyncio.Task"]) -> AsyncIterator[OpResult]:
        """Yield from the result queue until the producer and all op-tasks finish."""
        while True:
            if producer.done() and all(t.done() for t in tasks) and q.empty():
                break
            try:
                yield await asyncio.wait_for(q.get(), timeout=0.1)
            except asyncio.TimeoutError:
                continue
        while not q.empty():
            yield q.get_nowait()
        # surface a producer crash (not an op failure — those are captured in OpResult)
        if producer.exception():
            raise producer.exception()

    def _slot_task(self, sem: asyncio.Semaphore, q: "asyncio.Queue[OpResult]",
                   op: str, *, top_k: int, batch_size: int, build_index: bool) -> "asyncio.Task":
        """Create a task that holds a concurrency slot for the op's whole lifetime."""
        async def _job():
            try:
                res = await self._run_one(op, top_k=top_k, batch_size=batch_size, build_index=build_index)
                await q.put(res)
            finally:
                sem.release()
        return asyncio.create_task(_job())

    async def _drive_constant(self, c: WorkloadConfig) -> AsyncIterator[OpResult]:
        sem = asyncio.Semaphore(self._effective_concurrency(c, include_insert=False))
        q: "asyncio.Queue[OpResult]" = asyncio.Queue()
        tasks: List[asyncio.Task] = []
        interval = 1.0 / c.ops_per_second if c.ops_per_second > 0 else 0.0
        start = time.monotonic()

        async def produce():
            i = 0
            while time.monotonic() - start < c.duration_s:
                # closed-loop: block here when max_in_flight is saturated
                await sem.acquire()
                if time.monotonic() - start >= c.duration_s:
                    sem.release()
                    break
                tasks.append(self._slot_task(sem, q, "query", top_k=c.top_k,
                                             batch_size=0, build_index=False))
                i += 1
                # pace to the nominal rate (sleep only if we're ahead of schedule)
                ahead = (start + i * interval) - time.monotonic()
                if ahead > 0:
                    await asyncio.sleep(ahead)

        producer = asyncio.create_task(produce())
        async for r in self._drain(sem, q, producer, tasks):
            yield r

    async def _drive_burst(self, c: WorkloadConfig) -> AsyncIterator[OpResult]:
        sem = asyncio.Semaphore(self._effective_concurrency(c, include_insert=False))
        q: "asyncio.Queue[OpResult]" = asyncio.Queue()
        tasks: List[asyncio.Task] = []

        async def produce():
            for cycle in range(c.cycles):
                # fire the whole burst as fast as concurrency allows (no rate cap)
                for _ in range(c.burst_size):
                    await sem.acquire()
                    tasks.append(self._slot_task(sem, q, "query", top_k=c.top_k,
                                                 batch_size=0, build_index=False))
                # idle between bursts (not after the last)
                if cycle < c.cycles - 1:
                    await asyncio.sleep(c.idle_s)

        producer = asyncio.create_task(produce())
        async for r in self._drain(sem, q, producer, tasks):
            yield r

    async def _drive_mixed(self, c: WorkloadConfig) -> AsyncIterator[OpResult]:
        sem = asyncio.Semaphore(self._effective_concurrency(c, include_insert=True))
        q: "asyncio.Queue[OpResult]" = asyncio.Queue()
        tasks: List[asyncio.Task] = []
        inserts_enabled = self._can_insert()
        start = time.monotonic()

        async def produce():
            issued = 0
            issued_inserts = 0
            while time.monotonic() - start < c.duration_s:
                await sem.acquire()
                if time.monotonic() - start >= c.duration_s:
                    sem.release()
                    break
                # Deterministic interleave so the issued insert ratio tracks the
                # target tightly even at small N: issue an insert whenever doing so
                # keeps issued_inserts closest to insert_ratio * issued_total.
                want_insert = (
                    inserts_enabled
                    and round((issued + 1) * c.insert_ratio) > issued_inserts
                )
                if want_insert:
                    tasks.append(self._slot_task(sem, q, "bulk_insert", top_k=c.top_k,
                                                 batch_size=c.insert_batch_size,
                                                 build_index=c.insert_build_index))
                    issued_inserts += 1
                else:
                    tasks.append(self._slot_task(sem, q, "query", top_k=c.top_k,
                                                 batch_size=0, build_index=False))
                issued += 1

        producer = asyncio.create_task(produce())
        if not inserts_enabled:
            # Document, don't error: mixed degenerates to queries-only when the
            # backend can't ingest mid-run (empty ingest_modes or no insert pool).
            print("[workload] mixed: insert stream DISABLED "
                  f"(ingest_modes={self.insert_client.capabilities().ingest_modes}, "
                  f"insert_pool={len(self.insert_pool)}); running queries only.")
        async for r in self._drain(sem, q, producer, tasks):
            yield r
