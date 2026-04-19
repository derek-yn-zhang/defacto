"""Integration tests for TieredLedger — hot Postgres + cold Delta Lake.

Tests the full tiered lifecycle: append to hot, flush to cold, stitch
reads across tiers, prune hot, dedup after pruning, and cross-tier
operations (replay_for_entities, update_normalized).

Requires Postgres (docker compose up -d postgres). Uses local filesystem
for Delta Lake cold storage via tmp_path.
"""

import json

import pytest

from defacto.backends._ledger import PostgresLedger, TieredLedger

pytestmark = pytest.mark.postgres

CONNINFO = "postgresql://test:test@localhost:5432/defacto_test"


def make_event(
    event_id="evt_001",
    event_type="signup",
    timestamp="2024-01-15T10:00:00Z",
    source="web",
    data=None,
    raw=None,
    resolution_hints=None,
):
    """Build a ledger event dict with sensible defaults."""
    return {
        "event_id": event_id,
        "event_type": event_type,
        "timestamp": timestamp,
        "source": source,
        "data": data or {"email": "alice@example.com"},
        "raw": raw or {"email": "alice@example.com", "type": "signup"},
        "resolution_hints": resolution_hints or {"customer": {"email": "alice@example.com"}},
    }


@pytest.fixture
def tiered(tmp_path):
    """Create a TieredLedger with Postgres hot + local Delta cold."""
    cold_path = str(tmp_path / "cold_ledger")
    ledger = TieredLedger(PostgresLedger(CONNINFO), cold_path)
    yield ledger
    ledger.close()


# ---------------------------------------------------------------------------
# Constructor and permanent tables
# ---------------------------------------------------------------------------


class TestTieredConstruction:
    """Constructor creates permanent tables and coordination state."""

    def test_event_ids_table_exists(self, tiered):
        """event_ids table is created in the defacto schema."""
        with tiered._hot.connection.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.tables"
                " WHERE table_schema = 'defacto' AND table_name = 'event_ids'"
            )
            assert cur.fetchone()[0] == 1

    def test_tiered_state_table_exists(self, tiered):
        """tiered_state table is created with initial values."""
        assert tiered._last_flushed_seq == 0

    def test_event_entities_fk_dropped(self, tiered):
        """event_entities FK constraint is dropped for pruning support."""
        with tiered._hot.connection.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.table_constraints"
                " WHERE table_schema = 'defacto'"
                " AND table_name = 'event_entities'"
                " AND constraint_type = 'FOREIGN KEY'"
            )
            assert cur.fetchone()[0] == 0


# ---------------------------------------------------------------------------
# Hot path — append_batch with event_ids
# ---------------------------------------------------------------------------


class TestHotPath:
    """Ingest writes to both ledger and event_ids."""

    def test_append_writes_to_event_ids(self, tiered):
        """append_batch records event_ids for permanent dedup."""
        seqs = tiered.append_batch([make_event()])
        assert len(seqs) == 1

        with tiered._hot.connection.cursor() as cur:
            cur.execute("SELECT event_id, sequence FROM defacto.event_ids")
            rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "evt_001"
        assert rows[0][1] == seqs[0]

    def test_cursor_returns_latest(self, tiered):
        """cursor() returns hot cursor (latest sequence)."""
        tiered.append_batch([make_event()])
        assert tiered.cursor() == 1

    def test_dedup_via_hot(self, tiered):
        """Duplicate event_ids are caught by hot ledger."""
        tiered.append_batch([make_event()])
        seqs = tiered.append_batch([make_event()])  # same event_id
        assert len(seqs) == 1  # existing sequence returned, not new


# ---------------------------------------------------------------------------
# Flush — hot to cold
# ---------------------------------------------------------------------------


class TestFlush:
    """Export events from hot Postgres to cold Delta Lake."""

    def test_flush_exports_events(self, tiered):
        """flush() writes events to Delta Lake."""
        tiered.append_batch([
            make_event("evt_001", timestamp="2024-01-15T10:00:00Z"),
            make_event("evt_002", timestamp="2024-01-15T10:01:00Z"),
        ])
        count = tiered.flush()
        assert count == 2
        assert tiered._last_flushed_seq == 2

    def test_flush_updates_state(self, tiered):
        """last_flushed_sequence is updated in Postgres."""
        tiered.append_batch([make_event()])
        tiered.flush()
        # Read directly from Postgres
        stored = tiered._read_tiered_state("last_flushed_sequence")
        assert stored == 1

    def test_flush_no_new_events(self, tiered):
        """flush() with no new events returns 0."""
        assert tiered.flush() == 0

    def test_flush_incremental(self, tiered):
        """Second flush only exports new events."""
        tiered.append_batch([make_event("evt_001")])
        tiered.flush()

        tiered.append_batch([make_event("evt_002")])
        count = tiered.flush()
        assert count == 1  # only the new event
        assert tiered._last_flushed_seq == 2

    def test_cold_readable_after_flush(self, tiered):
        """Events are readable from cold after flush."""
        tiered.append_batch([make_event()])
        tiered.flush()

        cold_events = list(tiered._read_cold())
        assert len(cold_events) == 1
        assert cold_events[0]["event_id"] == "evt_001"


# ---------------------------------------------------------------------------
# Replay stitching
# ---------------------------------------------------------------------------


class TestReplayStitch:
    """replay() stitches cold and hot transparently."""

    def test_replay_all_in_hot(self, tiered):
        """replay(from_sequence=0) with no cold data reads from hot."""
        tiered.append_batch([make_event()])
        events = list(tiered.replay(from_sequence=0))
        assert len(events) == 1

    def test_replay_after_flush(self, tiered):
        """replay(from_sequence=0) after flush reads from cold."""
        tiered.append_batch([make_event()])
        tiered.flush()
        events = list(tiered.replay(from_sequence=0))
        assert len(events) == 1

    def test_replay_stitches_cold_and_hot(self, tiered):
        """replay(from_sequence=0) stitches cold + hot tail."""
        tiered.append_batch([make_event("evt_001", timestamp="2024-01-15T10:00:00Z")])
        tiered.flush()
        tiered.append_batch([make_event("evt_002", timestamp="2024-01-15T10:01:00Z")])
        # evt_001 is in cold, evt_002 is in hot (unflushed)
        events = list(tiered.replay(from_sequence=0))
        assert len(events) == 2
        assert events[0]["event_id"] == "evt_001"
        assert events[1]["event_id"] == "evt_002"

    def test_replay_incremental_hot_only(self, tiered):
        """replay(from_sequence=N) where N > last_flushed reads hot only."""
        tiered.append_batch([make_event("evt_001")])
        tiered.flush()
        tiered.append_batch([make_event("evt_002")])

        events = list(tiered.replay(from_sequence=2))
        assert len(events) == 1
        assert events[0]["event_id"] == "evt_002"


# ---------------------------------------------------------------------------
# Prune
# ---------------------------------------------------------------------------


class TestPrune:
    """Prune removes ledger rows but keeps event_ids and event_entities."""

    def test_prune_removes_ledger_rows(self, tiered):
        """prune() deletes old events from hot ledger."""
        tiered.append_batch([make_event("evt_001"), make_event("evt_002")])
        tiered.flush()
        count = tiered.prune()
        assert count == 2

        # Ledger is empty
        with tiered._hot.connection.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM defacto.ledger")
            assert cur.fetchone()[0] == 0

    def test_prune_keeps_event_ids(self, tiered):
        """event_ids survive pruning for dedup."""
        tiered.append_batch([make_event("evt_001")])
        tiered.flush()
        tiered.prune()

        with tiered._hot.connection.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM defacto.event_ids")
            assert cur.fetchone()[0] == 1

    def test_dedup_after_prune(self, tiered):
        """Duplicate events are caught even after pruning.

        After flush + prune, the event is gone from hot but event_ids
        still has it. append_batch pre-filters against event_ids to
        prevent re-insertion of pruned events.
        """
        tiered.append_batch([make_event("evt_001")])
        tiered.flush()
        tiered.prune()

        # Same event again — should be caught by event_ids pre-filter
        seqs = tiered.append_batch([make_event("evt_001")])
        assert len(seqs) == 1  # returns existing sequence

        # Hot ledger should NOT have a duplicate row
        with tiered._hot.connection.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM defacto.ledger WHERE event_id = 'evt_001'")
            assert cur.fetchone()[0] == 0  # still pruned, not re-inserted

    def test_prune_refuses_beyond_flushed(self, tiered):
        """prune() won't delete events that haven't been flushed."""
        tiered.append_batch([make_event()])
        # Not flushed — prune should do nothing
        count = tiered.prune()
        assert count == 0

    def test_replay_after_prune_reads_cold(self, tiered):
        """After prune, replay(from_sequence=0) reads from cold."""
        tiered.append_batch([make_event("evt_001")])
        tiered.flush()
        tiered.prune()

        events = list(tiered.replay(from_sequence=0))
        assert len(events) == 1
        assert events[0]["event_id"] == "evt_001"


# ---------------------------------------------------------------------------
# replay_for_entities across tiers
# ---------------------------------------------------------------------------


class TestReplayForEntities:
    """Entity-scoped replay stitches across tiers."""

    def test_all_in_hot(self, tiered):
        """replay_for_entities when all events are in hot."""
        seqs = tiered.append_batch([make_event()])
        tiered.write_event_entities([(seqs[0], "customer_001", "customer")])
        events = list(tiered.replay_for_entities(["customer_001"]))
        assert len(events) == 1

    def test_all_in_cold(self, tiered):
        """replay_for_entities when all events are in cold (pruned from hot)."""
        seqs = tiered.append_batch([make_event()])
        tiered.write_event_entities([(seqs[0], "customer_001", "customer")])
        tiered.flush()
        tiered.prune()

        events = list(tiered.replay_for_entities(["customer_001"]))
        assert len(events) == 1
        assert events[0]["event_id"] == "evt_001"

    def test_split_across_tiers(self, tiered):
        """replay_for_entities when events span hot and cold."""
        seq1 = tiered.append_batch([
            make_event("evt_001", timestamp="2024-01-15T10:00:00Z"),
        ])
        tiered.write_event_entities([(seq1[0], "customer_001", "customer")])
        tiered.flush()
        tiered.prune()

        seq2 = tiered.append_batch([
            make_event("evt_002", timestamp="2024-01-15T10:01:00Z"),
        ])
        tiered.write_event_entities([(seq2[0], "customer_001", "customer")])

        # evt_001 in cold, evt_002 in hot
        events = list(tiered.replay_for_entities(["customer_001"]))
        assert len(events) == 2
        assert events[0]["event_id"] == "evt_001"
        assert events[1]["event_id"] == "evt_002"


# ---------------------------------------------------------------------------
# Count
# ---------------------------------------------------------------------------


class TestCount:
    """count() uses event_ids for total, hot for filtered."""

    def test_count_total(self, tiered):
        """count() returns total from event_ids (survives pruning)."""
        tiered.append_batch([make_event("evt_001"), make_event("evt_002")])
        tiered.flush()
        tiered.prune()
        assert tiered.count() == 2  # event_ids still has both

    def test_count_by_source(self, tiered):
        """count(source=) queries hot only (approximate after prune)."""
        tiered.append_batch([make_event()])
        assert tiered.count(source="web") == 1


# ---------------------------------------------------------------------------
# Redact across tiers
# ---------------------------------------------------------------------------


class TestRedact:
    """Erase redacts sensitive fields in both hot and cold."""

    def test_redact_hot_only(self, tiered):
        """Redact works when events are only in hot."""
        seqs = tiered.append_batch([make_event()])
        tiered.write_event_entities([(seqs[0], "customer_001", "customer")])
        count = tiered.redact("customer_001", ["email"])
        assert count >= 1

        events = list(tiered.replay(from_sequence=0))
        data = events[0]["data"]
        assert data.get("email") == "[REDACTED]"

    def test_redact_cold(self, tiered):
        """Redact updates events in cold after flush + prune."""
        seqs = tiered.append_batch([make_event()])
        tiered.write_event_entities([(seqs[0], "customer_001", "customer")])
        tiered.flush()
        tiered.prune()

        count = tiered.redact("customer_001", ["email"])
        assert count >= 1

        # Read from cold — should be redacted
        events = list(tiered.replay(from_sequence=0))
        assert len(events) == 1
        assert events[0]["data"]["email"] == "[REDACTED]"
        assert events[0]["raw"]["email"] == "[REDACTED]"


# ---------------------------------------------------------------------------
# Crash recovery — reconcile after interrupted flush
# ---------------------------------------------------------------------------


class TestCrashRecovery:
    """Crash recovery via _reconcile_flush_state.

    If a process crashes after writing to Delta but before updating
    last_flushed_sequence in Postgres, the cold ledger has more events
    than Postgres knows about. On restart, _reconcile_flush_state detects
    the discrepancy and updates Postgres to match.

    Substantiates claim F6.5 (TieredLedger crash recovery).
    """

    def test_reconcile_after_interrupted_flush(self, tmp_path):
        """Simulate a crash between Delta write and Postgres state update.

        1. Create TieredLedger, append events, flush normally
        2. Append more events, flush them to Delta manually (bypass state update)
        3. Re-create TieredLedger — reconcile should fix the state
        4. Verify replay returns all events in correct order
        """
        cold_path = str(tmp_path / "cold_ledger")

        # Phase 1: normal operation — append and flush
        ledger = TieredLedger(PostgresLedger(CONNINFO), cold_path)
        seqs1 = ledger.append_batch([
            make_event(event_id="evt_001", timestamp="2024-01-01T10:00:00Z"),
            make_event(event_id="evt_002", timestamp="2024-01-01T11:00:00Z"),
        ])
        ledger.flush()
        flushed_seq_after_normal = ledger._last_flushed_seq
        assert flushed_seq_after_normal == max(seqs1)

        # Phase 2: append more events
        seqs2 = ledger.append_batch([
            make_event(event_id="evt_003", timestamp="2024-01-01T12:00:00Z"),
            make_event(event_id="evt_004", timestamp="2024-01-01T13:00:00Z"),
        ])

        # Simulate crash: write to Delta manually but DON'T update tiered_state
        import pyarrow as pa
        from defacto.backends._ledger import TieredLedger as TL
        with ledger._hot.connection.cursor() as cur:
            cur.execute(
                "SELECT sequence, event_id, event_type, timestamp, source,"
                " data::text, raw::text, resolution_hints::text, duplicate_of"
                " FROM defacto.ledger WHERE sequence > %s ORDER BY sequence",
                (flushed_seq_after_normal,),
            )
            rows = cur.fetchall()

        # Write to Delta (simulating the flush's Delta write step)
        import deltalake
        schema = pa.schema([
            ("sequence", pa.int64()),
            ("event_id", pa.string()),
            ("event_type", pa.string()),
            ("timestamp", pa.string()),
            ("source", pa.string()),
            ("data", pa.string()),
            ("raw", pa.string()),
            ("resolution_hints", pa.string()),
            ("duplicate_of", pa.int64()),
        ])
        arrays = [
            pa.array([r[0] for r in rows], type=pa.int64()),
            pa.array([r[1] for r in rows]),
            pa.array([r[2] for r in rows]),
            pa.array([str(r[3]) for r in rows]),
            pa.array([r[4] for r in rows]),
            pa.array([r[5] for r in rows]),
            pa.array([r[6] for r in rows]),
            pa.array([r[7] for r in rows]),
            pa.array([r[8] for r in rows], type=pa.int64()),
        ]
        table = pa.table(
            {name: arr for name, arr in zip(schema.names, arrays)},
            schema=schema,
        )
        deltalake.write_deltalake(cold_path, table, mode="append")

        # At this point: Delta has events 1-4, Postgres thinks only 1-2 are flushed.
        # Close without updating state (simulates crash).
        old_flushed = ledger._last_flushed_seq
        assert old_flushed == max(seqs1)  # Still thinks only first flush happened
        ledger.close()

        # Phase 3: Re-create — _reconcile_flush_state should fix it
        ledger2 = TieredLedger(PostgresLedger(CONNINFO), cold_path)
        assert ledger2._last_flushed_seq == max(seqs2)  # Reconciled!

        # Phase 4: Replay should return all 4 events in order
        events = list(ledger2.replay(from_sequence=0))
        event_ids = [e["event_id"] for e in events]
        assert "evt_001" in event_ids
        assert "evt_002" in event_ids
        assert "evt_003" in event_ids
        assert "evt_004" in event_ids
        # Verify ordering
        assert event_ids == sorted(event_ids, key=lambda x: x)
        ledger2.close()
