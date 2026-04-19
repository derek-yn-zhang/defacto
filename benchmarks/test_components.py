"""Component-level benchmarks — isolated backend throughput.

Each benchmark measures a single backend operation in isolation,
without the overhead of the full pipeline. This identifies which
component is the bottleneck.

Substantiates claims:
  - M6.1: batch I/O efficiency
  - S6.1: I/O not the bottleneck at moderate scale
  - S2.3: cache hit rate effect on throughput
  - S3.2: PyO3 boundary overhead

Usage:
    make release && make bench
"""

import tempfile

import pytest

from benchmarks.datagen import BENCH_DEFINITIONS, SyntheticGenerator
from defacto._core import DefactoCore
from defacto.backends._ledger import SqliteLedger
from defacto.backends._identity import SqliteIdentity
from defacto._identity import IdentityResolver


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _generate_ledger_rows(core, events):
    """Normalize events to get ledger rows for direct backend testing."""
    result = core.normalize("bench", events)
    return result["ledger_rows"]


# ---------------------------------------------------------------------------
# Rust core — normalization and interpretation from Python
# ---------------------------------------------------------------------------


class TestRustCoreThroughput:
    """Measures Rust computation called from Python (includes PyO3 overhead).

    Compare these numbers against the Rust criterion benchmarks in
    crates/defacto-engine/benches/core.rs to isolate PyO3 boundary cost.
    """

    @pytest.mark.parametrize("n_events", [100, 1000, 5000])
    def test_normalize(self, benchmark, n_events):
        """Normalization throughput — Python → Rust → Python round trip."""
        events = SyntheticGenerator(
            BENCH_DEFINITIONS, n_events=n_events, n_entities=n_events // 5,
        ).generate()

        core = DefactoCore(workers=1)
        core.compile("bench_v1", BENCH_DEFINITIONS)
        core.activate("bench_v1")

        benchmark(core.normalize, "bench", events)
        core.close()

    @pytest.mark.parametrize("n_events", [100, 1000, 5000])
    def test_interpret(self, benchmark, n_events):
        """Interpretation throughput — entity mapping → snapshots."""
        events = SyntheticGenerator(
            BENCH_DEFINITIONS, n_events=n_events, n_entities=n_events // 5,
        ).generate()

        core = DefactoCore(workers=1)
        core.compile("bench_v1", BENCH_DEFINITIONS)
        core.activate("bench_v1")

        # Normalize first (populates internal event buffer)
        result = core.normalize("bench", events)

        # Build a simple entity mapping (event_idx → entity_id)
        entity_mapping = [
            (i, f"entity_{i % (n_events // 5)}", "customer")
            for i in range(len(result["ledger_rows"]))
        ]

        def run():
            # Re-normalize to refill the event buffer (interpret consumes it)
            core.normalize("bench", events)
            core.interpret(entity_mapping)

        benchmark(run)
        core.close()


# ---------------------------------------------------------------------------
# Ledger backend — append throughput
# ---------------------------------------------------------------------------


class TestLedgerThroughput:
    """Isolated ledger append — how fast can we write to the ledger."""

    @pytest.mark.parametrize("n_events", [100, 500, 1000])
    def test_append(self, benchmark, n_events):
        """Ledger append throughput at varying batch sizes."""
        events = SyntheticGenerator(
            BENCH_DEFINITIONS, n_events=n_events, n_entities=n_events // 5,
        ).generate()

        core = DefactoCore(workers=1)
        core.compile("bench_v1", BENCH_DEFINITIONS)
        core.activate("bench_v1")
        rows = _generate_ledger_rows(core, events)
        core.close()

        def setup():
            ledger = SqliteLedger(tempfile.mktemp(suffix=".db"))
            return (ledger, rows), {}

        def run(ledger, rows):
            ledger.append_batch(rows)

        benchmark.pedantic(run, setup=setup, rounds=10, warmup_rounds=2)


# ---------------------------------------------------------------------------
# Identity resolution — cache hit rate impact
# ---------------------------------------------------------------------------


class TestIdentityThroughput:
    """Identity resolution at varying cache states.

    Substantiates claim S2.3: cache hit rate dominates identity throughput.
    """

    def test_all_new(self, benchmark):
        """Cold cache — every hint is new. Maximum backend load."""
        events = SyntheticGenerator(
            BENCH_DEFINITIONS, n_events=500, n_entities=500,
        ).generate()

        core = DefactoCore(workers=1)
        core.compile("bench_v1", BENCH_DEFINITIONS)
        core.activate("bench_v1")
        result = core.normalize("bench", events)
        hints = [row.get("resolution_hints", {}) for row in result["ledger_rows"]]

        def setup():
            # Fresh core + backend each round — empty cache
            c = DefactoCore(workers=1)
            c.compile("bench_v1", BENCH_DEFINITIONS)
            c.activate("bench_v1")
            backend = SqliteIdentity(tempfile.mktemp(suffix=".db"))
            resolver = IdentityResolver(c, backend)
            return (resolver, hints), {}

        def run(resolver, hints):
            resolver.resolve_batch(hints)

        benchmark.pedantic(run, setup=setup, rounds=10, warmup_rounds=2)
        core.close()

    def test_all_cached(self, benchmark):
        """Warm cache — every hint is cached. Minimal backend load."""
        events = SyntheticGenerator(
            BENCH_DEFINITIONS, n_events=500, n_entities=100,
        ).generate()

        core = DefactoCore(workers=1)
        core.compile("bench_v1", BENCH_DEFINITIONS)
        core.activate("bench_v1")
        result = core.normalize("bench", events)
        hints = [row.get("resolution_hints", {}) for row in result["ledger_rows"]]

        # Pre-populate cache by resolving once
        backend = SqliteIdentity(tempfile.mktemp(suffix=".db"))
        resolver = IdentityResolver(core, backend)
        resolver.resolve_batch(hints)

        # Now benchmark with warm cache
        benchmark(resolver.resolve_batch, hints)
        core.close()


# ---------------------------------------------------------------------------
# Publisher — InlinePublisher vs KafkaPublisher
# ---------------------------------------------------------------------------


class TestPublisherThroughput:
    """Measures publishing overhead: InlinePublisher (direct state history
    write) vs KafkaPublisher produce (just the Kafka side, not consumer).

    The real comparison is InlinePublisher vs KafkaPublisher + Consumer
    end-to-end, but consumer benchmarks require a full e2e setup.
    This isolates the publisher side: how much does Kafka produce cost
    compared to a direct SQLite write?
    """

    def _make_snapshots(self, n):
        """Generate n entity snapshots for publishing."""
        return [
            {
                "entity_id": f"ent_{i}",
                "entity_type": "customer",
                "state": "active",
                "properties": {"email": f"user{i}@test.com", "plan": "pro", "mrr": 99.0},
                "relationships": [],
                "last_event_time": "2024-01-15T10:00:00Z",
                "state_entered_time": "2024-01-15T10:00:00Z",
                "created_time": "2024-01-15T10:00:00Z",
                "valid_from": "2024-01-15T10:00:00Z",
            }
            for i in range(n)
        ]

    @pytest.mark.parametrize("n_snapshots", [10, 100, 500])
    def test_inline_publisher(self, benchmark, n_snapshots):
        """InlinePublisher — direct write to SQLite state history.

        This is the full path for dev mode: publish = state history write.
        """
        from defacto._publisher import InlinePublisher
        from defacto.backends._state_history import SqliteStateHistory
        from benchmarks.datagen import BENCH_DEFINITIONS

        snapshots = self._make_snapshots(n_snapshots)

        def setup():
            sh = SqliteStateHistory(tempfile.mktemp(suffix=".db"))
            sh.ensure_tables(BENCH_DEFINITIONS["entities"])
            pub = InlinePublisher(sh)
            return (pub, snapshots), {}

        def run(pub, snaps):
            pub.publish(snaps, [])

        benchmark.pedantic(run, setup=setup, rounds=10, warmup_rounds=2)

    @pytest.mark.parametrize("n_snapshots", [10, 100, 500])
    @pytest.mark.kafka
    def test_kafka_producer_only(self, benchmark, n_snapshots):
        """KafkaPublisher produce + flush — just the Kafka side.

        Does not include consumer processing. Measures the overhead
        of serializing + producing to Kafka vs direct state history write.
        For full e2e comparison (producer + consumer + state history write),
        see test_production_e2e.py.
        """
        from defacto._publisher import KafkaPublisher

        snapshots = self._make_snapshots(n_snapshots)

        pub = KafkaPublisher({
            "bootstrap.servers": "localhost:9092",
            "topic": "defacto-bench-effects",
        })

        def run():
            pub.publish(snapshots, [])
            pub._producer.flush(timeout=5)

        benchmark(run)
        pub.close()
