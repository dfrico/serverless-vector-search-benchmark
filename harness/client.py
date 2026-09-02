"""
Abstract benchmark client interface for the serverless vector-DB thesis harness.

This is the *harness-facing* contract every system-under-test adapter implements
(Blocks, S3 Vectors, LanceDB-on-Lambda). It is deliberately WIDER than Blocks's
own `VectorDBClient` (references/blocks-db-wip/vectordb/client.py): it adds an
explicit `setup`/`teardown` lifecycle, a backend-neutral `bulk_insert`, a
first-class `delete`, and a Blocks-specific `put_pending` that is a no-op on
systems without an event-driven pending path. It is NARROWER in that it drops
Blocks's S3/DynamoDB plumbing conveniences (list_datasets, tracking, config I/O)
which are implementation details, not benchmark operations.

Naming: parameters are named for the concept, not the FAISS/Blocks term, so the
same word means the same thing across adapters:
  - `top_k`              number of neighbours to return  (Blocks: k_search/k_result, FAISS: k)
  - `partitions_per_block` IVF nlist, clusters within one block (Blocks/FAISS: `k` — overloaded there)
  - `num_blocks`          number of index blocks/partitions (Blocks: num_index)
  - `blocks_per_task`     blocks queried per map worker    (Blocks: query_batch_size)

Design notes grounded in the day-1 bring-up (findings/06_blocks_local.md):
  - Blocks splits a *control plane* (index discovery / dataset / pending state,
    all boto3 -> S3 + DynamoDB) from a *data plane* (Lithops map/reduce -> object
    store). Adapters own where each lands; the harness only sees these methods.
  - Every query returns a `(results, timings)` pair. `results` is a list (one
    entry per query vector) of ranked `(vector_id, distance)` tuples. `timings`
    is a per-call dict the metrics layer parses. It carries a COMMON MINIMAL
    ENVELOPE — `op: str` and `wall_clock_s: float` on every op — plus any number
    of backend-specific keys. The envelope lets the metrics layer group and
    aggregate without special-casing each adapter; the extra keys (e.g. Blocks'
    `0_*_blocks` map/reduce phase breakdown) are opaque and MUST NOT be
    interpreted by the workload generator.
  - All distances are L2 unless an adapter documents otherwise. No normalization.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

# A single ranked hit: (vector_id, distance). Lower distance = closer (L2).
Hit = Tuple[int, float]
# Results for a batch: one ranked list of hits per query vector, in input order.
QueryResults = List[List[Hit]]
# Per-call timing/metrics dict emitted by the adapter for the metrics layer.
# MINIMUM ENVELOPE: every adapter op MUST include `op: str` and
# `wall_clock_s: float`. Any further keys are backend-specific and opaque to the
# workload generator (the metrics layer parses them per-(adapter, op)).
Timings = Dict[str, object]
Vector = Sequence[float]


@dataclass(frozen=True)
class ClientCapabilities:
    """
    Static description of what a backend adapter can do, returned by
    `BenchmarkVectorClient.capabilities()`.

    The workload generator MUST consult capabilities before:
      - issuing any pending/freshness operation (check `supports_pending`),
      - comparing vector IDs across systems or against ground truth
        (check `id_stability` — a "reassigned" backend needs an ID-mapping layer),
      - interpreting a `Hit`'s distance (check `metric` — "l2" is ascending
        distance; "cosine" may be similarity and require conversion),
      - choosing how to ingest a corpus (check `ingest_modes`).

    Fields:
        supports_pending: True if the backend has a distinct pending/event-driven
            path (Blocks). False on backends where all writes go straight to the
            indexed corpus (S3 Vectors, LanceDB-on-Lambda). If False, `put_pending`
            is the inherited no-op and pending-aware queries are unavailable.
        metric: Distance/similarity family of the `Hit` distances, e.g. "l2"
            (ascending distance, lower = closer) or "cosine". The harness uses
            this to decide whether results are directly comparable across systems.
        id_stability: Whether vector IDs round-trip unchanged. "preserved" = the
            ID returned by a query equals the ID supplied at ingest. "reassigned"
            = the backend may renumber IDs (e.g. Blocks' auto-indexer on the
            pending->indexed path), so recall against fixed ground truth needs an
            ID map.
        ingest_modes: Which `bulk_insert*` entry points the adapter implements,
            a subset of {"iterable", "path"}. "iterable" = `bulk_insert` accepts
            an in-memory stream of (id, vector). "path" = `bulk_insert_from_path`
            accepts a CSV path. Adapters declare only what they actually support.
        max_safe_concurrency: The largest `max_in_flight` value under which this
            backend still behaves correctly. Adapters set this from KNOWN backend
            properties — e.g. worker temp-file namespacing, or a managed service's
            request-rate ceiling — not from measurement. The workload generator
            caps effective concurrency at this value (it takes
            `min(config.max_in_flight, max_safe_concurrency)`), so a backend that
            is concurrency-unsafe declares `1` and the generator serialises it
            rather than issuing colliding requests. Defaults to 1 (the always-safe
            floor); an adapter with no known concurrency limit sets a large value.
    """
    supports_pending: bool
    metric: str
    id_stability: str
    ingest_modes: Set[str] = field(default_factory=set)
    max_safe_concurrency: int = 1


class BenchmarkVectorClient(ABC):
    """
    The contract every system-under-test adapter implements for the harness.

    One instance is bound to one logical dataset namespace on one backend. The
    workload generator drives instances purely through the methods below; it
    never touches backend SDKs directly. Implementations decide synchronicity
    internally, but all methods are defined as synchronous from the caller's
    perspective (the workload generator supplies its own concurrency).
    """

    # ----- identity ----------------------------------------------------------

    @property
    @abstractmethod
    def name(self) -> str:
        """
        Short, stable adapter identifier (e.g. "blocks", "s3vectors", "lance").

        Used as the FIRST half of the `(adapter, op)` key the metrics layer's
        timings-parser registry is keyed on, and stamped onto every `OpResult`
        the workload generator emits so the sink/aggregator can attribute a
        record to its backend WITHOUT importing the concrete adapter. Must be
        lowercase, stable across runs (it is a persisted key), and unique per
        backend family. Distinct from the dataset namespace bound in `setup`.
        """
        raise NotImplementedError

    @abstractmethod
    def backend_version(self) -> Dict[str, object]:
        """
        Version identifiers for the underlying system-under-test, recorded in the
        run manifest so a result is tied to the exact backend build that produced
        it (a reproducibility requirement — backend behaviour can change between
        versions).

        Shape is backend-specific but SHOULD name the source and a version/commit,
        e.g. Blocks returns `{"repo", "commit_sha", "commit_date"}` read from its
        vendored clone; a managed service returns its API/SDK version and region.
        A field that cannot be determined at runtime is reported as `"unknown"`
        (with a logged warning) rather than omitted or faked — an honest "unknown"
        is a valid manifest value; a missing key is not.
        """
        raise NotImplementedError

    @abstractmethod
    def dataset_fingerprint(self, dataset_name: str) -> Dict[str, object]:
        """
        Content fingerprint of the corpus this run targets, recorded in the
        manifest so results are tied to exact corpus bytes (the datasets are
        git-ignored and reassembled per machine, so filename alone is not enough).

        SHOULD return `{"content_sha256", "line_count", "byte_size", "source_path"}`.
        When the adapter has no local corpus file to hash (e.g. the dataset is
        already resident in the object store), the hash fields are `None` — the
        shape is still recorded, the hash is not fabricated.
        """
        raise NotImplementedError

    # ----- capabilities ------------------------------------------------------

    @abstractmethod
    def capabilities(self) -> ClientCapabilities:
        """
        Return this adapter's static capability description.

        Callable before `setup` (it describes the adapter/backend, not a specific
        run), though an adapter whose capabilities depend on its local-vs-cloud
        configuration may finalise them in `setup` and return the resolved values
        here. The workload generator consults this to route pending operations,
        compare IDs, and interpret distances — see `ClientCapabilities`.
        """
        raise NotImplementedError

    # ----- lifecycle ---------------------------------------------------------

    @abstractmethod
    def setup(
        self,
        dataset_name: str,
        *,
        dimensions: int,
        top_k_default: int = 10,
        num_blocks: Optional[int] = None,
        partitions_per_block: Optional[int] = None,
        n_probe: Optional[int] = None,
        blocks_per_task: Optional[int] = None,
        create_if_missing: bool = True,
    ) -> None:
        """
        Prepare the backend so the dataset is queryable, and pin the index
        parameters for this run.

        Concretely an adapter does whatever its backend needs to reach a state
        where `query`/`query_batch` will succeed: verifying or provisioning the
        index, loading/writing index config, establishing clients/credentials.
        It does NOT ingest the dataset's vectors — that is `bulk_insert`.

        Args:
            dataset_name: Logical dataset/namespace this client is bound to.
            dimensions: Vector dimensionality (Blocks: `features`). Fixed for the run.
            top_k_default: Neighbours returned when a query omits `top_k`.
            num_blocks: Number of index blocks/partitions to build the index with
                (Blocks: `num_index`). Adapters that don't partition ignore it.
                NOTE for Blocks: the localhost indexing path floors blocks-per-worker
                via `num_index // num_workers`, so very small values degenerate —
                keep `num_blocks >= num_workers`.
            partitions_per_block: IVF nlist — clusters trained within each block
                (Blocks/FAISS: the overloaded `k`). None = adapter default.
            n_probe: IVF probe count at search time. None = adapter default.
            blocks_per_task: How many blocks one map worker handles
                (Blocks: `query_batch_size`); controls map fan-out. None = default.
            create_if_missing: If True, build/provision the index when absent;
                if False, raise when no usable index exists.

        Raises:
            RuntimeError: backend unreachable, or index missing with
                create_if_missing=False.
        """
        raise NotImplementedError

    @abstractmethod
    def teardown(self, *, drop_data: bool = False) -> None:
        """
        Release per-run resources held by this client.

        Always closes clients / thread pools / executors. If `drop_data` is True
        it also deletes this run's persisted artifacts (index blocks, config,
        pending files, tracking state, and — for ephemeral benchmark datasets —
        the dataset vectors). Defaults to False so a run's data survives for
        inspection unless the harness explicitly asks to clean up.

        Must be idempotent: safe to call after a partial `setup`.
        """
        raise NotImplementedError

    # ----- ingest ------------------------------------------------------------

    @abstractmethod
    def bulk_insert(
        self,
        vectors: Iterable[Tuple[int, Vector]],
        *,
        build_index: bool = True,
    ) -> Tuple[None, Timings]:
        """
        Ingest an in-memory stream of (id, vector) pairs as the dataset's
        baseline, durable, *indexed* corpus. This is the canonical bulk-load
        path, distinct from the incremental `put_pending` path.

        `vectors` is an Iterable (not a materialised Sequence) so adapters can
        stream large corpora without holding them all in memory; the adapter
        materialises into whatever shape its backend wants (Blocks → a CSV
        object; S3 Vectors → record batches). An adapter declares "iterable" in
        `capabilities().ingest_modes` iff it implements this method usefully.

        Args:
            vectors: Iterable of (vector_id, vector) pairs. IDs are caller-assigned
                ints. Whether IDs survive a round-trip is reported by
                `capabilities().id_stability` ("preserved" vs "reassigned").
            build_index: If True, the corpus is queryable via the indexed path
                when this returns. If False, vectors are staged/uploaded but the
                index build is deferred. CAVEAT: on backends whose deferred build
                is gated on a cloud service (Blocks' auto-indexer = DynamoDB), a
                `build_index=False` corpus may NOT be locally completable — the
                later build needs that service. Adapters document this; the
                interface does not promise deferred indexing is always local.

        Returns:
            (None, timings) — `None` is a placeholder for a future ingest summary;
            `timings` is the opaque per-op dict for the metrics layer.
        """
        raise NotImplementedError

    def bulk_insert_from_path(
        self,
        path: str,
        *,
        id_column: str = "id",
        build_index: bool = True,
    ) -> Tuple[None, Timings]:
        """
        Ingest a corpus directly from a CSV file on disk — the file-native
        bulk-load path for backends that partition an object rather than an
        in-memory list (Blocks reads `vectors_{dataset}.csv` from the object
        store and a Lithops job partitions that object).

        OPTIONAL. The DEFAULT IMPLEMENTATION RAISES NotImplementedError. An
        adapter that supports it MUST also declare "path" in
        `capabilities().ingest_modes`; the workload generator checks that before
        calling this instead of `bulk_insert`.

        Args:
            path: Local path to the corpus CSV. Row shape `id,<space floats>`
                unless the adapter documents otherwise.
            id_column: Name/identity of the ID column (for adapters that read a
                header); ignored by adapters with a fixed column layout.
            build_index: As in `bulk_insert` (same deferred-build caveat).

        Returns:
            (None, timings).

        Raises:
            NotImplementedError: if the adapter does not support path ingest.
        """
        raise NotImplementedError(
            "This adapter does not support path-based ingest; "
            "check capabilities().ingest_modes before calling."
        )

    def put_pending(
        self,
        vectors: Sequence[Tuple[int, Vector]],
    ) -> Timings:
        """
        Append (id, vector) pairs to the incremental "pending" path: searchable
        immediately via brute force, indexed only once the backend's threshold
        triggers a block build. This models the event-driven freshness behaviour
        that is the thesis's interactivity workload.

        Blocks-specific. The DEFAULT IMPLEMENTATION IS A NO-OP that returns an
        empty Timings dict, so adapters for systems without a pending/freshness
        distinction (S3 Vectors, LanceDB-on-Lambda) inherit it unchanged. The
        workload generator must treat an empty return as "this backend has no
        separate pending path" — NOT as a failure.

        Args:
            vectors: (vector_id, vector) pairs to stage as pending.

        Returns:
            Timings for the put (e.g. upload time); empty dict on the no-op default.
        """
        return {}

    # ----- query -------------------------------------------------------------

    @abstractmethod
    def query(
        self,
        vector: Vector,
        *,
        top_k: Optional[int] = None,
    ) -> Tuple[List[Hit], Timings]:
        """
        Find the nearest neighbours of a single query vector against the indexed
        corpus.

        Pending/freshness-aware queries are NOT a per-call flag here: whether a
        backend even has a pending path is a static property
        (`capabilities().supports_pending`), and pending-aware querying will be a
        separate routed path the workload generator owns. This method always
        searches the indexed corpus only.

        Args:
            vector: The query vector (length must equal `dimensions`).
            top_k: Neighbours to return; None uses `top_k_default` from setup.

        Returns:
            (hits, timings) where `hits` is a ranked list of (vector_id, distance),
            ascending by distance (for an "l2" backend), length <= top_k.
        """
        raise NotImplementedError

    @abstractmethod
    def query_batch(
        self,
        vectors: Sequence[Vector],
        *,
        top_k: Optional[int] = None,
    ) -> Tuple[QueryResults, Timings]:
        """
        Nearest-neighbour search for many query vectors in one call, against the
        indexed corpus.

        "Batch" means multiple query vectors per call — NOT asynchronous
        submission. From the caller's view this blocks until all results are in;
        the adapter may fan out internally (Blocks raises map/reduce workers).
        The workload generator owns inter-request concurrency and arrival timing;
        it should not assume this call is itself concurrent. As with `query`,
        pending awareness is governed by capabilities, not a per-call flag.

        Args:
            vectors: Query vectors, each of length `dimensions`.
            top_k: Neighbours per query; None uses `top_k_default`.

        Returns:
            (results, timings) where `results[i]` is the ranked hit list for
            `vectors[i]`, in input order. `timings` is one dict covering the
            whole batch.
        """
        raise NotImplementedError

    # ----- mutate ------------------------------------------------------------

    @abstractmethod
    def delete(
        self,
        vector_ids: Sequence[int],
        *,
        reindex: bool = True,
    ) -> Timings:
        """
        Remove vectors by ID from the dataset.

        Blocks's model is delete-and-rebuild: the main CSV is rewritten without
        the deleted IDs (remaining IDs are NOT renumbered), optionally followed
        by a full reindex. There is no first-class "update" verb anywhere in the
        Blocks API — callers model an update as delete + (bulk_insert | put_pending).

        Args:
            vector_ids: IDs to delete. IDs absent from the dataset are ignored.
            reindex: If True, rebuild the index after deletion so the indexed
                path stops returning the removed vectors; if False, the index is
                left stale until a later explicit rebuild.

        Returns:
            Timings for the delete (+ reindex if performed).
        """
        raise NotImplementedError
