"""Pipeline-level benchmarks — end-to-end hot path throughput.

Measures the full normalize → ledger → identity → interpret → publish
path that users experience. This is the number that matters most.

Substantiates claims:
  - S3.1: Near-linear worker scaling (throughput at 1, 2, 4, 8 workers)
  - S3.2: PyO3 boundary not a bottleneck

Usage:
    make release && make bench
"""

import tempfile

import pytest

from benchmarks.datagen import BENCH_DEFINITIONS, SyntheticGenerator
from defacto import Defacto


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


PG_CONNINFO = "postgresql://test:test@localhost:5432/defacto_test"


def _make_defacto(backend: str = "sqlite", workers: int = 1) -> Defacto:
    """Create a Defacto instance for benchmarking.

    Args:
        backend: 'sqlite' for in-process (computational ceiling) or
            'postgres' for out-of-process (production reality).
        workers: Rayon thread pool size.
    """
    if backend == "postgres":
        from tests.conftest import clean_postgres
        clean_postgres()
        return Defacto(
            BENCH_DEFINITIONS, database=PG_CONNINFO, workers=workers,
        )
    db_path = tempfile.mktemp(suffix=".db")
    return Defacto(
        BENCH_DEFINITIONS, database=db_path, workers=workers,
    )


def _backend_params():
    """Parametrize across backends: sqlite always, postgres if available."""
    return [
        pytest.param("sqlite", id="sqlite"),
        pytest.param("postgres", id="postgres", marks=pytest.mark.postgres),
    ]


# ---------------------------------------------------------------------------
# Ingest throughput — events/sec through the full hot path
# ---------------------------------------------------------------------------


class TestIngestThroughput:
    """End-to-end ingest throughput at varying batch sizes.

    Each benchmark creates a fresh Defacto instance, ingests a batch of
    events with process=True (inline processing), and measures wall time.
    Parametrized across SQLite and Postgres backends for direct comparison.
    """

    @pytest.mark.parametrize("backend", _backend_params())
    @pytest.mark.parametrize("batch_size", [100, 500])
    def test_ingest(self, benchmark, medium_events, backend, batch_size):
        """Events/sec through process_batch at varying batch sizes."""
        events = medium_events[:1000]

        def setup():
            m = _make_defacto(backend=backend)
            return (m, events), {}

        def run(m, events):
            for i in range(0, len(events), batch_size):
                batch = events[i : i + batch_size]
                m.ingest("bench", batch, process=True)

        benchmark.pedantic(run, setup=setup, rounds=5, warmup_rounds=1)


# ---------------------------------------------------------------------------
# Build throughput — events/sec during rebuild
# ---------------------------------------------------------------------------


class TestBuildThroughput:
    """Build throughput: how fast can we rebuild entity state from the ledger."""

    @pytest.mark.parametrize("backend", _backend_params())
    def test_build_full(self, benchmark, medium_events, backend):
        """Full rebuild throughput — replay all events from ledger."""
        events = medium_events[:5000]

        def setup():
            m = _make_defacto(backend=backend)
            m.ingest("bench", events)
            return (m,), {}

        def run(m):
            m.build(full=True)

        benchmark.pedantic(run, setup=setup, rounds=5, warmup_rounds=1)

    @pytest.mark.parametrize("backend", _backend_params())
    def test_build_incremental(self, benchmark, medium_events, backend):
        """Incremental build — process only new events from cursor."""
        base_events = medium_events[:3000]
        new_events = medium_events[3000:5000]

        def setup():
            m = _make_defacto(backend=backend)
            m.ingest("bench", base_events)
            m.build()
            m.ingest("bench", new_events)
            return (m,), {}

        def run(m):
            m.build()

        benchmark.pedantic(run, setup=setup, rounds=5, warmup_rounds=1)


# ---------------------------------------------------------------------------
# Worker scaling — throughput vs worker count
# ---------------------------------------------------------------------------


class TestWorkerScaling:
    """Throughput at varying Rayon worker counts.

    Substantiates claim S3.1: near-linear scaling with workers.
    Uses large event counts (50K) so Rust computation dominates I/O.
    At small counts (5K), I/O dominates and workers show no benefit.

    Each benchmark uses the same events and definitions — only the
    worker count changes. Pedantic mode ensures a fresh instance per
    round so DashMap starts empty each time.
    """

    @pytest.mark.parametrize("backend", _backend_params())
    @pytest.mark.parametrize("workers", [1, 2, 4, 8])
    def test_build_full_scaling(self, benchmark, medium_events, backend, workers):
        """Full rebuild throughput at 1, 2, 4, 8 workers with 10K events."""
        events = medium_events[:10000]

        def setup():
            m = _make_defacto(backend=backend, workers=workers)
            m.ingest("bench", events)
            return (m,), {}

        def run(m):
            m.build(full=True)
            m.close()

        benchmark.pedantic(run, setup=setup, rounds=3, warmup_rounds=1)


# ---------------------------------------------------------------------------
# Timing profile at varying scale — where does time go?
# ---------------------------------------------------------------------------


class TestTimingProfile:
    """Timing breakdown at different event counts.

    Shows how the performance profile shifts as batch size increases:
    at small N, I/O dominates. At large N, Rust computation dominates.
    The crossover point is where workers start helping.

    Uses process=True with batch_size=N so each run is a single
    process_batch call — no loop overhead, clean timing breakdown.
    """

    @pytest.mark.parametrize("backend", _backend_params())
    @pytest.mark.parametrize("n_events", [500, 2000, 10000])
    def test_profile(self, benchmark, large_events, backend, n_events):
        """Single-batch timing profile at varying event counts."""
        events = large_events[:n_events]

        def setup():
            m = _make_defacto(backend=backend)
            # batch_size=n_events ensures one process_batch call
            m._batch_size = n_events
            return (m, events), {}

        def run(m, events):
            m.ingest("bench", events, process=True)

        benchmark.pedantic(run, setup=setup, rounds=5, warmup_rounds=1)


# ---------------------------------------------------------------------------
# Entity cardinality — varying events-per-entity ratio
# ---------------------------------------------------------------------------


class TestEntityCardinality:
    """Throughput at varying entity densities.

    Same total events, different number of entities. Dense (few entities,
    many events each) has high cache hit rate and stresses interpretation.
    Sparse (many entities, few events each) has low cache hit rate and
    stresses identity creation.

    Answers: how much does cache hit rate affect pipeline throughput?
    """

    @pytest.mark.parametrize("backend", _backend_params())
    @pytest.mark.parametrize("n_entities,label", [
        (100, "dense"),     # 50 events/entity — warm cache
        (1000, "moderate"), # 5 events/entity — mixed
        (5000, "sparse"),   # 1 event/entity — cold cache
    ])
    def test_cardinality(self, benchmark, backend, label, n_entities):
        """Build throughput at varying events-per-entity ratios."""
        events = SyntheticGenerator(
            BENCH_DEFINITIONS,
            n_events=5000, n_entities=n_entities, seed=42,
        ).generate()

        def setup():
            m = _make_defacto(backend=backend)
            m.ingest("bench", events)
            return (m,), {}

        def run(m):
            m.build(full=True)
            m.close()

        benchmark.pedantic(run, setup=setup, rounds=5, warmup_rounds=1)
