"""Scale benchmarks — shards, memory, cold reads, compression.

Require infrastructure (docker compose up -d postgres) and take longer
to run. Not included in the default `make bench` — run explicitly.

Usage:
    docker compose up -d postgres
    make release
    PYTHONPATH=python .venv/bin/python -m pytest benchmarks/test_scale.py \
        --benchmark-enable -v
"""

import os
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor

import psutil
import pytest

from benchmarks.datagen import BENCH_DEFINITIONS, SyntheticGenerator
from defacto import Defacto
from tests.conftest import PG_CONNINFO, clean_postgres

pytestmark = pytest.mark.postgres


# ---------------------------------------------------------------------------
# Shard scaling — N shards ≈ N× throughput
# ---------------------------------------------------------------------------


class TestShardScaling:
    """Concurrent shard builds against shared Postgres.

    Substantiates claim S7.1: N shards ≈ N× interpretation throughput.
    Each shard is an independent Defacto process with its own Postgres
    connection, building its subset of entities from the shared ledger.

    Setup (excluded from timing):
      - Ingest 10K events with unsharded instance (populates ledger)
      - Create N shard instances (each opens own connection)

    Measurement (timed):
      - All shards build(full=True) concurrently via ThreadPoolExecutor
      - Wall-clock from first submit to last completion
    """

    @pytest.mark.parametrize("n_shards", [1, 2, 4])
    def test_shard_scaling(self, benchmark, n_shards):
        """Combined build throughput at 1, 2, 4 shards."""
        events = SyntheticGenerator(
            BENCH_DEFINITIONS, n_events=10_000, n_entities=2_000, seed=42,
        ).generate()

        def setup():
            clean_postgres()
            # Ingest with unsharded instance (shared ledger)
            m = Defacto(BENCH_DEFINITIONS, database=PG_CONNINFO)
            m.ingest("bench", events)
            m.build()
            m.close()

            # Create shard instances (connections opened, no build yet)
            shards = []
            for i in range(n_shards):
                s = Defacto(
                    BENCH_DEFINITIONS, database=PG_CONNINFO,
                    shard_id=i, total_shards=n_shards,
                )
                shards.append(s)
            return (shards,), {}

        def run(shards):
            if len(shards) == 1:
                shards[0].build(full=True)
                shards[0].close()
            else:
                with ThreadPoolExecutor(max_workers=len(shards)) as pool:
                    futures = [
                        pool.submit(lambda s: (s.build(full=True), s.close()), s)
                        for s in shards
                    ]
                    for f in futures:
                        f.result()

        benchmark.pedantic(run, setup=setup, rounds=3, warmup_rounds=1)


class TestParallelIngestAndBuild:
    """Split ingest → parallel build — the real sharded deployment model.

    Each shard ingests a portion of events (load balanced) into the shared
    ledger, then builds only its owned entities. Measures total wall time
    for the complete pipeline: parallel ingest + parallel build.

    This is the benchmark that matters for production throughput claims.
    """

    @pytest.mark.parametrize("n_shards", [1, 2, 4])
    def test_parallel_ingest_and_build(self, benchmark, n_shards):
        """End-to-end: split ingest + parallel build."""
        n_events = 50_000
        events = SyntheticGenerator(
            BENCH_DEFINITIONS, n_events=n_events, n_entities=5_000, seed=42,
        ).generate()

        def setup():
            clean_postgres()
            # Create schema + tables once (migration step, not measured)
            m = Defacto(BENCH_DEFINITIONS, database=PG_CONNINFO)
            m.close()

            # Split events into chunks for each shard
            chunk_size = len(events) // n_shards
            chunks = [
                events[i * chunk_size : (i + 1) * chunk_size]
                for i in range(n_shards)
            ]
            chunks[-1] = events[(n_shards - 1) * chunk_size :]

            # Create shard instances (connections open, schema exists)
            shards = []
            for i in range(n_shards):
                s = Defacto(
                    BENCH_DEFINITIONS, database=PG_CONNINFO,
                    shard_id=i, total_shards=n_shards,
                )
                shards.append(s)
            return (shards, chunks), {}

        def run(shards, chunks):
            if len(shards) == 1:
                shards[0].ingest("bench", chunks[0])
                shards[0].build()
                shards[0].close()
            else:
                # Parallel ingest — each shard ingests its chunk
                with ThreadPoolExecutor(max_workers=len(shards)) as pool:
                    futures = [
                        pool.submit(lambda s, c: s.ingest("bench", c), s, c)
                        for s, c in zip(shards, chunks)
                    ]
                    for f in futures:
                        f.result()

                # Parallel build — each shard builds its owned entities
                with ThreadPoolExecutor(max_workers=len(shards)) as pool:
                    futures = [
                        pool.submit(lambda s: (s.build(), s.close()), s)
                        for s in shards
                    ]
                    for f in futures:
                        f.result()

        benchmark.pedantic(run, setup=setup, rounds=3, warmup_rounds=1)


# ---------------------------------------------------------------------------
# Memory per entity — RSS measurement
# ---------------------------------------------------------------------------


class TestMemoryPerEntity:
    """Measure RSS per entity in DashMap via differential measurement.

    Substantiates claim S4.2: ~500 bytes per entity.

    Measures RSS at two entity counts and takes the delta to cancel
    out fixed overhead (Python runtime, Defacto instance, SQLite, etc.).
    Uses Postgres to avoid SQLite lock issues at scale.

    bytes_per_entity = (RSS_large - RSS_small) / (n_large - n_small)
    """

    def test_memory_differential(self):
        """RSS per entity via differential measurement.

        Uses SQLite (no Postgres cursor issues) with uniform distribution
        (ensures all target entities get events). Measures at two sizes
        and computes per-entity cost from the delta.
        """
        import gc
        process = psutil.Process(os.getpid())

        results = {}
        for n_entities in [1_000, 5_000]:
            events = SyntheticGenerator(
                BENCH_DEFINITIONS,
                n_events=n_entities * 2,  # 2 events/entity ensures creation + state change
                n_entities=n_entities,
                distribution="uniform",  # uniform so all entities get events
                seed=42,
            ).generate()

            gc.collect()
            db_path = tempfile.mktemp(suffix=".db")
            m = Defacto(
                BENCH_DEFINITIONS, database=db_path,
                workers=1, batch_size=500,
            )
            # process=True processes inline — avoids build_full's
            # replay cursor which conflicts with SQLite writes
            m.ingest("bench", events, process=True)

            gc.collect()
            rss = process.memory_info().rss
            count = m._core.entity_count()
            results[count] = rss
            m.close()

            print(f"\n  {count:,} entities: RSS = {rss / 1024 / 1024:.1f} MB")

        # Differential: cancel fixed overhead
        counts = sorted(results.keys())
        if len(counts) >= 2:
            n_small, n_large = counts[0], counts[-1]
            delta = results[n_large] - results[n_small]
            per_entity = delta / (n_large - n_small)
            print(f"\n  Differential: {delta / 1024:.0f} KB / "
                  f"{n_large - n_small:,} entities = "
                  f"~{per_entity:.0f} bytes/entity")


# ---------------------------------------------------------------------------
# Cold read throughput — Delta Lake sequential scan
# ---------------------------------------------------------------------------


class TestColdReadThroughput:
    """Events/sec reading from Delta Lake cold storage.

    Substantiates claim S6.3: cold read throughput.

    Setup: TieredLedger with N events flushed to cold, hot pruned.
    Measurement: time to replay all events from cold.
    """

    @pytest.mark.parametrize("n_events", [10_000, 50_000])
    def test_cold_read(self, benchmark, tmp_path, n_events):
        """Cold read throughput at varying event counts."""
        from defacto.backends._ledger import PostgresLedger, TieredLedger

        events = SyntheticGenerator(
            BENCH_DEFINITIONS, n_events=n_events, n_entities=n_events // 5,
            seed=42,
        ).generate()

        # Generate ledger rows via DefactoCore
        from defacto._core import DefactoCore
        core = DefactoCore(workers=1)
        core.compile("bench_v1", BENCH_DEFINITIONS)
        core.activate("bench_v1")
        result = core.normalize("bench", events)
        ledger_rows = result["ledger_rows"]
        core.close()

        def setup():
            clean_postgres()
            cold_path = str(tmp_path / f"cold_{n_events}")
            ledger = TieredLedger(PostgresLedger(PG_CONNINFO), cold_path)
            ledger.append_batch(ledger_rows)
            ledger.flush()
            ledger.prune()
            return (ledger,), {}

        def run(ledger):
            count = sum(1 for _ in ledger.replay(from_sequence=0))
            assert count > 0

        benchmark.pedantic(run, setup=setup, rounds=5, warmup_rounds=1)


# ---------------------------------------------------------------------------
# Compression ratio — Parquet vs Postgres
# ---------------------------------------------------------------------------


class TestCompressionRatio:
    """Size comparison: Delta Lake (Parquet) vs Postgres row storage.

    Substantiates claim S6.2: 5-10x compression ratio.

    Not a timing benchmark — measures storage size.
    """

    def test_compression_ratio(self, tmp_path):
        """Compare Postgres table size vs Delta Lake file size."""
        from defacto.backends._ledger import PostgresLedger, TieredLedger

        n_events = 10_000
        events = SyntheticGenerator(
            BENCH_DEFINITIONS, n_events=n_events, n_entities=2_000, seed=42,
        ).generate()

        # Generate ledger rows
        from defacto._core import DefactoCore
        core = DefactoCore(workers=1)
        core.compile("bench_v1", BENCH_DEFINITIONS)
        core.activate("bench_v1")
        result = core.normalize("bench", events)
        ledger_rows = result["ledger_rows"]
        core.close()

        clean_postgres()
        cold_path = str(tmp_path / "cold_compress")
        ledger = TieredLedger(PostgresLedger(PG_CONNINFO), cold_path)
        ledger.append_batch(ledger_rows)
        ledger.flush()

        # Postgres size
        import psycopg
        conn = psycopg.connect(PG_CONNINFO)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_total_relation_size('defacto.ledger'),"
                " pg_relation_size('defacto.ledger')"
            )
            pg_total, pg_data = cur.fetchone()
        conn.close()

        # Delta Lake size (all files including _delta_log)
        delta_bytes = 0
        for dirpath, _, filenames in os.walk(cold_path):
            for f in filenames:
                delta_bytes += os.path.getsize(os.path.join(dirpath, f))

        ratio_total = pg_total / max(delta_bytes, 1)
        ratio_data = pg_data / max(delta_bytes, 1)

        print(f"\n  {n_events:,} events:")
        print(f"  Postgres total: {pg_total / 1024:.0f} KB "
              f"(data: {pg_data / 1024:.0f} KB)")
        print(f"  Delta Lake:     {delta_bytes / 1024:.0f} KB")
        print(f"  Compression:    {ratio_total:.1f}x (total) / "
              f"{ratio_data:.1f}x (data only)")

        # Sanity check — Delta should be smaller
        assert delta_bytes < pg_total

        ledger.close()
