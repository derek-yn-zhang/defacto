"""Tests for SQLite backends — ledger, identity, and state history.

Unit tests only — SQLite is embedded, no infrastructure needed. Each test
creates a fresh :memory: database with no state leakage between tests.
"""

import json

import pytest

from defacto.backends import SqliteIdentity, SqliteLedger, SqliteStateHistory


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_event(
    event_id: str = "evt_001",
    event_type: str = "signup",
    timestamp: str = "2024-01-15T10:00:00Z",
    source: str = "web",
    data: dict | None = None,
    raw: dict | None = None,
    resolution_hints: dict | None = None,
) -> dict:
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


CUSTOMER_DEF = {
    "properties": {
        "name": {"type": "string"},
        "mrr": {"type": "float"},
        "signup_count": {"type": "int"},
        "is_active": {"type": "bool"},
        "tags": {"type": "array"},
    },
}


# ===========================================================================
# SqliteLedger
# ===========================================================================


class TestSqliteLedgerAppend:
    """Append batch operations."""

    def test_append_returns_sequences(self):
        ledger = SqliteLedger(":memory:")
        events = [make_event("e1"), make_event("e2"), make_event("e3")]
        seqs = ledger.append_batch(events)
        assert len(seqs) == 3
        assert seqs == sorted(seqs)

    def test_append_empty_batch(self):
        ledger = SqliteLedger(":memory:")
        assert ledger.append_batch([]) == []

    def test_append_dedup_by_event_id(self):
        ledger = SqliteLedger(":memory:")
        seqs_first = ledger.append_batch([make_event("e1")])
        # Same event_id again — should not create a new row
        seqs = ledger.append_batch([make_event("e1"), make_event("e2")])
        assert len(seqs) == 2  # both returned (existing + new)
        # e1's sequence is unchanged from first insert
        assert seqs[0] == seqs_first[0]
        # Only 2 distinct events in the ledger
        assert len(list(ledger.replay())) == 2

    def test_append_preserves_json_fields(self):
        ledger = SqliteLedger(":memory:")
        data = {"email": "bob@test.com", "plan": "pro"}
        raw = {"e": "bob@test.com", "p": "pro", "extra": True}
        hints = {"customer": ["bob@test.com"]}
        ledger.append_batch([make_event("e1", data=data, raw=raw, resolution_hints=hints)])

        events = list(ledger.replay())
        assert events[0]["data"] == data
        assert events[0]["raw"] == raw
        assert events[0]["resolution_hints"] == hints


class TestSqliteLedgerReplay:
    """Replay and cursor operations."""

    def test_replay_all(self):
        ledger = SqliteLedger(":memory:")
        ledger.append_batch([make_event("e1"), make_event("e2"), make_event("e3")])
        events = list(ledger.replay())
        assert len(events) == 3
        assert [e["event_id"] for e in events] == ["e1", "e2", "e3"]

    def test_replay_from_sequence(self):
        ledger = SqliteLedger(":memory:")
        seqs = ledger.append_batch([make_event("e1"), make_event("e2"), make_event("e3")])
        events = list(ledger.replay(from_sequence=seqs[1]))
        assert len(events) == 2
        assert events[0]["event_id"] == "e2"

    def test_replay_empty_ledger(self):
        ledger = SqliteLedger(":memory:")
        assert list(ledger.replay()) == []

    def test_cursor_empty(self):
        ledger = SqliteLedger(":memory:")
        assert ledger.cursor() == 0

    def test_cursor_after_append(self):
        ledger = SqliteLedger(":memory:")
        ledger.append_batch([make_event("e1"), make_event("e2")])
        assert ledger.cursor() == 2


class TestSqliteLedgerEventEntities:
    """Event-entity mapping and entity-scoped replay."""

    def test_write_and_replay_for_entities(self):
        ledger = SqliteLedger(":memory:")
        seqs = ledger.append_batch([make_event("e1"), make_event("e2"), make_event("e3")])
        ledger.write_event_entities([
            (seqs[0], "cust_001", "customer"),
            (seqs[1], "cust_001", "customer"),
            (seqs[2], "cust_002", "customer"),
        ])
        # Replay for cust_001 only
        events = list(ledger.replay_for_entities(["cust_001"]))
        assert len(events) == 2
        assert {e["event_id"] for e in events} == {"e1", "e2"}

    def test_replay_for_entities_deduplicates(self):
        """One event mapped to two requested entities appears once."""
        ledger = SqliteLedger(":memory:")
        seqs = ledger.append_batch([make_event("e1")])
        ledger.write_event_entities([
            (seqs[0], "cust_001", "customer"),
            (seqs[0], "cust_002", "customer"),
        ])
        events = list(ledger.replay_for_entities(["cust_001", "cust_002"]))
        assert len(events) == 1

    def test_replay_for_entities_empty_list(self):
        ledger = SqliteLedger(":memory:")
        assert list(ledger.replay_for_entities([])) == []

    def test_write_event_entities_empty(self):
        ledger = SqliteLedger(":memory:")
        ledger.write_event_entities([])  # should not raise


class TestSqliteLedgerRedact:
    """Server-side JSON redaction."""

    def test_redact_sensitive_fields(self):
        ledger = SqliteLedger(":memory:")
        seqs = ledger.append_batch([
            make_event("e1", data={"email": "a@b.com", "plan": "pro"},
                       raw={"email": "a@b.com", "plan": "pro"}),
        ])
        ledger.write_event_entities([(seqs[0], "cust_001", "customer")])
        ledger.redact("cust_001", ["email"])

        events = list(ledger.replay())
        assert events[0]["data"]["email"] == "[REDACTED]"
        assert events[0]["raw"]["email"] == "[REDACTED]"
        assert events[0]["data"]["plan"] == "pro"  # untouched

    def test_redact_multiple_fields(self):
        ledger = SqliteLedger(":memory:")
        seqs = ledger.append_batch([
            make_event("e1", data={"email": "a@b.com", "phone": "555", "plan": "pro"},
                       raw={"email": "a@b.com", "phone": "555"}),
        ])
        ledger.write_event_entities([(seqs[0], "cust_001", "customer")])
        ledger.redact("cust_001", ["email", "phone"])

        events = list(ledger.replay())
        assert events[0]["data"]["email"] == "[REDACTED]"
        assert events[0]["data"]["phone"] == "[REDACTED]"
        assert events[0]["data"]["plan"] == "pro"

    def test_redact_no_fields_is_noop(self):
        ledger = SqliteLedger(":memory:")
        ledger.append_batch([make_event("e1")])
        ledger.redact("cust_001", [])  # should not raise


# ===========================================================================
# SqliteIdentity
# ===========================================================================


class TestSqliteLedgerEdgeCases:
    """Edge cases for ledger operations."""

    def test_replay_past_end_returns_empty(self):
        ledger = SqliteLedger(":memory:")
        ledger.append_batch([make_event("e1")])
        events = list(ledger.replay(from_sequence=999999))
        assert events == []

    def test_redact_nonexistent_entity(self):
        ledger = SqliteLedger(":memory:")
        ledger.append_batch([make_event("e1")])
        ledger.redact("nonexistent", ["email"])  # should not raise

    def test_write_event_entities_duplicate_ignored(self):
        ledger = SqliteLedger(":memory:")
        seqs = ledger.append_batch([make_event("e1")])
        ledger.write_event_entities([(seqs[0], "c_001", "customer")])
        ledger.write_event_entities([(seqs[0], "c_001", "customer")])  # duplicate
        rows = ledger._conn.execute("SELECT COUNT(*) FROM event_entities").fetchone()
        assert rows[0] == 1

    def test_append_batch_unicode_data(self):
        ledger = SqliteLedger(":memory:")
        data = {"name": "日本語テスト", "emoji": "🎉"}
        seqs = ledger.append_batch([make_event("e1", data=data, raw=data)])
        events = list(ledger.replay())
        assert events[0]["data"]["name"] == "日本語テスト"
        assert events[0]["data"]["emoji"] == "🎉"


class TestSqliteLedgerUpdateNormalized:
    """Tests for update_normalized — used by FULL_RENORMALIZE pre-pass."""

    def test_update_changes_data_and_hints(self):
        """Update replaces data, event_id, event_type, resolution_hints."""
        ledger = SqliteLedger(":memory:")
        seqs = ledger.append_batch([make_event("e1")])

        new_row = {
            "data": {"email": "ALICE@EXAMPLE.COM"},
            "event_id": "e_new_hash",
            "event_type": "customer_signup",
            "resolution_hints": {"customer": {"email": "ALICE@EXAMPLE.COM"}},
        }
        ledger.update_normalized([(seqs[0], new_row)])

        events = list(ledger.replay())
        assert events[0]["data"]["email"] == "ALICE@EXAMPLE.COM"
        assert events[0]["event_id"] == "e_new_hash"
        assert events[0]["event_type"] == "customer_signup"
        assert events[0]["resolution_hints"]["customer"] == {"email": "ALICE@EXAMPLE.COM"}

    def test_update_preserves_raw(self):
        """Raw column is untouched — it's the immutable source of truth."""
        ledger = SqliteLedger(":memory:")
        original_raw = {"email": "alice@example.com", "type": "signup"}
        seqs = ledger.append_batch([make_event("e1", raw=original_raw)])

        new_row = {
            "data": {"email": "ALICE@EXAMPLE.COM"},
            "event_id": "e_new",
            "event_type": "signup",
            "resolution_hints": {},
        }
        ledger.update_normalized([(seqs[0], new_row)])

        events = list(ledger.replay())
        assert events[0]["raw"] == original_raw

    def test_update_empty_list_is_noop(self):
        ledger = SqliteLedger(":memory:")
        ledger.update_normalized([])  # should not raise

    def test_update_multiple_rows(self):
        """Batch update across multiple sequences."""
        ledger = SqliteLedger(":memory:")
        seqs = ledger.append_batch([
            make_event("e1", timestamp="2024-01-01T00:00:00Z"),
            make_event("e2", timestamp="2024-01-02T00:00:00Z"),
        ])

        updates = [
            (seqs[0], {"data": {"v": 1}, "event_id": "new_1", "event_type": "a", "resolution_hints": {}}),
            (seqs[1], {"data": {"v": 2}, "event_id": "new_2", "event_type": "b", "resolution_hints": {}}),
        ]
        ledger.update_normalized(updates)

        events = list(ledger.replay())
        assert events[0]["data"]["v"] == 1
        assert events[1]["data"]["v"] == 2


class TestSqliteLedgerClearEventEntities:
    """Tests for clear_event_entities — used before RENORMALIZE/IDENTITY_RESET."""

    def test_clear_deletes_all_mappings(self):
        ledger = SqliteLedger(":memory:")
        seqs = ledger.append_batch([make_event("e1"), make_event("e2")])
        ledger.write_event_entities([
            (seqs[0], "c_001", "customer"),
            (seqs[1], "c_002", "customer"),
        ])

        ledger.clear_event_entities()

        rows = ledger._conn.execute("SELECT COUNT(*) FROM event_entities").fetchone()
        assert rows[0] == 0

    def test_clear_empty_is_noop(self):
        ledger = SqliteLedger(":memory:")
        ledger.clear_event_entities()  # should not raise


class TestSqliteLedgerReplayOrder:
    """Replay orders by timestamp, not sequence — correct for rebuilds."""

    def test_replay_returns_timestamp_order(self):
        """Events with out-of-order sequences are returned by timestamp."""
        ledger = SqliteLedger(":memory:")
        # Insert in sequence order: e1(t=10:00), e2(t=09:00), e3(t=11:00)
        # e2 is a "late arrival" — higher sequence but earlier timestamp
        ledger.append_batch([
            make_event("e1", timestamp="2024-01-15T10:00:00Z"),
            make_event("e2", timestamp="2024-01-15T09:00:00Z"),
            make_event("e3", timestamp="2024-01-15T11:00:00Z"),
        ])

        events = list(ledger.replay())
        timestamps = [e["timestamp"] for e in events]
        assert timestamps == [
            "2024-01-15T09:00:00Z",
            "2024-01-15T10:00:00Z",
            "2024-01-15T11:00:00Z",
        ]

    def test_replay_for_entities_returns_timestamp_order(self):
        ledger = SqliteLedger(":memory:")
        seqs = ledger.append_batch([
            make_event("e1", timestamp="2024-01-15T10:00:00Z"),
            make_event("e2", timestamp="2024-01-15T09:00:00Z"),
        ])
        ledger.write_event_entities([
            (seqs[0], "c_001", "customer"),
            (seqs[1], "c_001", "customer"),
        ])

        events = list(ledger.replay_for_entities(["c_001"]))
        assert events[0]["timestamp"] == "2024-01-15T09:00:00Z"
        assert events[1]["timestamp"] == "2024-01-15T10:00:00Z"


class TestSqliteIdentityResolveAndCreate:
    """Identity resolution — the hot path."""

    def test_create_new_hints(self):
        identity = SqliteIdentity(":memory:")
        result = identity.resolve_and_create([
            ("customer", "email", "alice@test.com", "cust_001"),
            ("customer", "email", "bob@test.com", "cust_002"),
        ])
        assert len(result) == 2
        result_map = {(r[0], r[1]): r[2] for r in result}
        assert result_map[("customer", "alice@test.com")] == "cust_001"
        assert result_map[("customer", "bob@test.com")] == "cust_002"

    def test_resolve_existing_hint(self):
        """Existing hint returns original entity_id, ignores candidate."""
        identity = SqliteIdentity(":memory:")
        identity.resolve_and_create([("customer", "email", "alice@test.com", "cust_001")])
        # Same (entity_type, hint_value), different candidate — should return original
        result = identity.resolve_and_create([("customer", "email", "alice@test.com", "cust_999")])
        assert result[0][2] == "cust_001"

    def test_mixed_new_and_existing(self):
        identity = SqliteIdentity(":memory:")
        identity.resolve_and_create([("customer", "email", "alice@test.com", "cust_001")])
        result = identity.resolve_and_create([
            ("customer", "email", "alice@test.com", "cust_999"),  # existing
            ("customer", "email", "bob@test.com", "cust_002"),    # new
        ])
        result_map = {(r[0], r[1]): r[2] for r in result}
        assert result_map[("customer", "alice@test.com")] == "cust_001"
        assert result_map[("customer", "bob@test.com")] == "cust_002"

    def test_empty_hints(self):
        identity = SqliteIdentity(":memory:")
        assert identity.resolve_and_create([]) == []

    def test_same_hint_different_entity_types(self):
        """Same hint value for different entity types resolves independently."""
        identity = SqliteIdentity(":memory:")
        identity.resolve_and_create([
            ("customer", "email", "alice@test.com", "cust_001"),
            ("fraud_case", "email", "alice@test.com", "fraud_001"),
        ])
        assert identity.lookup("customer", "alice@test.com") == "cust_001"
        assert identity.lookup("fraud_case", "alice@test.com") == "fraud_001"


class TestSqliteIdentityEdgeCases:
    """Edge cases for identity operations."""

    def test_merge_nonexistent_entity_is_noop(self):
        identity = SqliteIdentity(":memory:")
        identity.merge("nonexistent", "also_nonexistent")  # should not raise

    def test_self_merge_is_noop(self):
        identity = SqliteIdentity(":memory:")
        identity.resolve_and_create([("customer", "email", "a@b.com", "c_001")])
        identity.merge("c_001", "c_001")  # should not raise
        assert identity.lookup("customer", "a@b.com") == "c_001"

    def test_unicode_hint_values(self):
        identity = SqliteIdentity(":memory:")
        identity.resolve_and_create([
            ("customer", "name", "日本語", "c_001"),
        ])
        assert identity.lookup("customer", "日本語") == "c_001"

    def test_empty_string_hint_value(self):
        identity = SqliteIdentity(":memory:")
        result = identity.resolve_and_create([("customer", "email", "", "c_001")])
        assert len(result) == 1
        assert identity.lookup("customer", "") == "c_001"


class TestSqliteIdentityOperations:
    """Merge, delete, lookup, warmup, reset."""

    def test_merge_reassigns_hints(self):
        identity = SqliteIdentity(":memory:")
        identity.resolve_and_create([
            ("customer", "email", "alice@test.com", "cust_001"),
            ("customer", "phone", "555-1234", "cust_001"),
            ("customer", "email", "bob@test.com", "cust_002"),
        ])
        identity.merge("cust_001", "cust_002")

        # All hints now point to cust_002
        assert identity.lookup("customer", "alice@test.com") == "cust_002"
        assert identity.lookup("customer", "555-1234") == "cust_002"
        assert identity.lookup("customer", "bob@test.com") == "cust_002"

    def test_delete_removes_all_hints(self):
        identity = SqliteIdentity(":memory:")
        identity.resolve_and_create([
            ("customer", "email", "alice@test.com", "cust_001"),
            ("customer", "phone", "555-1234", "cust_001"),
        ])
        identity.delete("cust_001")
        assert identity.lookup("customer", "alice@test.com") is None
        assert identity.lookup("customer", "555-1234") is None

    def test_lookup_existing(self):
        identity = SqliteIdentity(":memory:")
        identity.resolve_and_create([("customer", "email", "alice@test.com", "cust_001")])
        assert identity.lookup("customer", "alice@test.com") == "cust_001"

    def test_lookup_missing(self):
        identity = SqliteIdentity(":memory:")
        assert identity.lookup("customer", "nobody@test.com") is None

    def test_warmup_returns_all_mappings(self):
        identity = SqliteIdentity(":memory:")
        identity.resolve_and_create([
            ("customer", "email", "alice@test.com", "cust_001"),
            ("customer", "email", "bob@test.com", "cust_002"),
        ])
        mappings = identity.warmup()
        assert len(mappings) == 2
        mapping_map = {(m[0], m[1]): m[2] for m in mappings}
        assert mapping_map[("customer", "alice@test.com")] == "cust_001"
        assert mapping_map[("customer", "bob@test.com")] == "cust_002"

    def test_warmup_empty(self):
        identity = SqliteIdentity(":memory:")
        assert identity.warmup() == []

    def test_reset_clears_all(self):
        identity = SqliteIdentity(":memory:")
        identity.resolve_and_create([
            ("customer", "email", "alice@test.com", "cust_001"),
            ("customer", "email", "bob@test.com", "cust_002"),
        ])
        identity.reset()
        assert identity.warmup() == []
        assert identity.lookup("customer", "alice@test.com") is None


# ===========================================================================
# SqliteStateHistory
# ===========================================================================


class TestSqliteStateHistoryEnsureTables:
    """Dynamic table creation from entity definitions."""

    def test_creates_table_with_property_columns(self):
        sh = SqliteStateHistory(":memory:")
        sh.ensure_tables({"customer": CUSTOMER_DEF})
        # Verify table exists by inserting a row
        sh._conn.execute(
            "INSERT INTO customer_history"
            " (customer_id, customer_state, name, mrr, signup_count, is_active, tags, valid_from)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("c_001", "active", "Alice", 99.0, 1, 1, json.dumps(["vip"]), "2024-01-15T10:00:00Z"),
        )
        row = sh._conn.execute("SELECT * FROM customer_history").fetchone()
        assert row is not None

    def test_idempotent_create(self):
        sh = SqliteStateHistory(":memory:")
        sh.ensure_tables({"customer": CUSTOMER_DEF})
        sh.ensure_tables({"customer": CUSTOMER_DEF})  # should not raise

    def test_multiple_entity_types(self):
        sh = SqliteStateHistory(":memory:")
        sh.ensure_tables({
            "customer": CUSTOMER_DEF,
            "account": {"properties": {"plan": {"type": "string"}}},
        })
        # Both tables exist
        tables = sh._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%_history'"
        ).fetchall()
        table_names = {t[0] for t in tables}
        assert "customer_history" in table_names
        assert "account_history" in table_names


class TestSqliteStateHistoryWriteBatch:
    """Snapshot and tombstone writes."""

    def _setup(self):
        """Create a state history with customer table."""
        sh = SqliteStateHistory(":memory:")
        sh.ensure_tables({"customer": CUSTOMER_DEF})
        return sh

    def test_write_snapshot(self):
        sh = self._setup()
        sh.write_batch(
            snapshots=[{
                "entity_id": "c_001",
                "entity_type": "customer",
                "state": "active",
                "properties": {"name": "Alice", "mrr": 99.0, "signup_count": 1, "is_active": True, "tags": ["vip"]},
                "valid_from": "2024-01-15T10:00:00Z",
            }],
            tombstones=[],
        )
        row = sh._conn.execute(
            "SELECT customer_id, customer_state, name, mrr, valid_from, valid_to"
            " FROM customer_history"
        ).fetchone()
        assert row[0] == "c_001"
        assert row[1] == "active"
        assert row[2] == "Alice"
        assert row[3] == 99.0
        assert row[5] is None  # valid_to is NULL (current version)

    def test_write_closes_previous_version(self):
        sh = self._setup()
        # First version
        sh.write_batch(
            snapshots=[{
                "entity_id": "c_001", "entity_type": "customer", "state": "active",
                "properties": {"name": "Alice", "mrr": 29.0, "signup_count": 1, "is_active": True, "tags": []},
                "valid_from": "2024-01-15T10:00:00Z",
            }],
            tombstones=[],
        )
        # Second version — should close the first
        sh.write_batch(
            snapshots=[{
                "entity_id": "c_001", "entity_type": "customer", "state": "active",
                "properties": {"name": "Alice", "mrr": 99.0, "signup_count": 1, "is_active": True, "tags": []},
                "valid_from": "2024-01-20T10:00:00Z",
            }],
            tombstones=[],
        )
        rows = sh._conn.execute(
            "SELECT valid_from, valid_to FROM customer_history WHERE customer_id = 'c_001' ORDER BY valid_from"
        ).fetchall()
        assert len(rows) == 2
        assert rows[0][1] == "2024-01-20T10:00:00Z"  # first version closed
        assert rows[1][1] is None  # second version is current

    def test_write_tombstone(self):
        sh = self._setup()
        sh.write_batch(
            snapshots=[{
                "entity_id": "c_001", "entity_type": "customer", "state": "active",
                "properties": {"name": "Alice", "mrr": 99.0, "signup_count": 1, "is_active": True, "tags": []},
                "valid_from": "2024-01-15T10:00:00Z",
            }],
            tombstones=[],
        )
        # Tombstone — entity merged
        sh.write_batch(
            snapshots=[],
            tombstones=[{
                "entity_id": "c_001", "entity_type": "customer",
                "merged_into": "c_002", "timestamp": "2024-01-20T10:00:00Z",
            }],
        )
        row = sh._conn.execute(
            "SELECT valid_to, merged_into FROM customer_history WHERE customer_id = 'c_001'"
        ).fetchone()
        assert row[0] == "2024-01-20T10:00:00Z"
        assert row[1] == "c_002"

    def test_idempotent_write(self):
        """Writing the same snapshot twice doesn't create duplicates."""
        sh = self._setup()
        snap = {
            "entity_id": "c_001", "entity_type": "customer", "state": "active",
            "properties": {"name": "Alice", "mrr": 99.0, "signup_count": 1, "is_active": True, "tags": []},
            "valid_from": "2024-01-15T10:00:00Z",
        }
        sh.write_batch(snapshots=[snap], tombstones=[])
        sh.write_batch(snapshots=[snap], tombstones=[])
        count = sh._conn.execute("SELECT COUNT(*) FROM customer_history").fetchone()[0]
        assert count == 1

    def test_write_multiple_entity_types(self):
        sh = SqliteStateHistory(":memory:")
        sh.ensure_tables({
            "customer": CUSTOMER_DEF,
            "account": {"properties": {"plan": {"type": "string"}}},
        })
        sh.write_batch(
            snapshots=[
                {
                    "entity_id": "c_001", "entity_type": "customer", "state": "active",
                    "properties": {"name": "Alice", "mrr": 99.0, "signup_count": 1, "is_active": True, "tags": []},
                    "valid_from": "2024-01-15T10:00:00Z",
                },
                {
                    "entity_id": "a_001", "entity_type": "account", "state": "trial",
                    "properties": {"plan": "free"},
                    "valid_from": "2024-01-15T10:00:00Z",
                },
            ],
            tombstones=[],
        )
        c_count = sh._conn.execute("SELECT COUNT(*) FROM customer_history").fetchone()[0]
        a_count = sh._conn.execute("SELECT COUNT(*) FROM account_history").fetchone()[0]
        assert c_count == 1
        assert a_count == 1

    def test_empty_batch(self):
        sh = self._setup()
        sh.write_batch(snapshots=[], tombstones=[])  # should not raise


class TestSqliteStateHistoryEdgeCases:
    """Edge cases for state history writes."""

    def test_snapshot_with_missing_properties(self):
        """Snapshot that doesn't include all entity properties."""
        sh = SqliteStateHistory(":memory:")
        sh.ensure_tables({"customer": CUSTOMER_DEF})
        sh.write_batch(
            snapshots=[{
                "entity_id": "c_001", "entity_type": "customer",
                "state": "active",
                "properties": {"name": "Alice"},  # missing mrr, signup_count, etc.
                "valid_from": "2024-01-15T10:00:00Z",
            }],
            tombstones=[],
        )
        row = sh._conn.execute("SELECT name, mrr FROM customer_history").fetchone()
        assert row[0] == "Alice"
        assert row[1] is None  # missing properties are NULL

    def test_snapshot_with_array_property(self):
        """Array properties are stored as JSON strings."""
        sh = SqliteStateHistory(":memory:")
        sh.ensure_tables({"customer": CUSTOMER_DEF})
        sh.write_batch(
            snapshots=[{
                "entity_id": "c_001", "entity_type": "customer",
                "state": "active",
                "properties": {"name": "Alice", "mrr": 99.0, "signup_count": 1,
                               "is_active": True, "tags": ["vip", "beta"]},
                "valid_from": "2024-01-15T10:00:00Z",
            }],
            tombstones=[],
        )
        row = sh._conn.execute("SELECT tags FROM customer_history").fetchone()
        assert json.loads(row[0]) == ["vip", "beta"]

    def test_tombstone_without_timestamp_key(self):
        """Tombstone uses valid_from fallback if timestamp is missing."""
        sh = SqliteStateHistory(":memory:")
        sh.ensure_tables({"customer": CUSTOMER_DEF})
        sh.write_batch(
            snapshots=[{
                "entity_id": "c_001", "entity_type": "customer", "state": "active",
                "properties": {"name": "Alice", "mrr": 99.0, "signup_count": 1,
                               "is_active": True, "tags": []},
                "valid_from": "2024-01-15T10:00:00Z",
            }],
            tombstones=[],
        )
        # Tombstone with valid_from instead of timestamp
        sh.write_batch(
            snapshots=[],
            tombstones=[{
                "entity_id": "c_001", "entity_type": "customer",
                "merged_into": "c_002", "valid_from": "2024-01-20T10:00:00Z",
            }],
        )
        row = sh._conn.execute("SELECT valid_to FROM customer_history").fetchone()
        assert row[0] == "2024-01-20T10:00:00Z"

    def test_ensure_tables_unknown_property_type(self):
        """Unknown property type falls back to TEXT via DDLGenerator."""
        sh = SqliteStateHistory(":memory:")
        sh.ensure_tables({"thing": {"properties": {"data": {"type": "custom_type"}}}})
        # Table should exist with TEXT column
        sh._conn.execute("INSERT INTO thing_history (thing_id, thing_state, data, valid_from) VALUES ('t1', 'a', 'hello', '2024-01-15')")
        row = sh._conn.execute("SELECT data FROM thing_history").fetchone()
        assert row[0] == "hello"


class TestSqliteStateHistoryDelete:
    """Entity deletion for right to erasure."""

    def test_delete_entity(self):
        sh = SqliteStateHistory(":memory:")
        sh.ensure_tables({"customer": CUSTOMER_DEF})
        sh.write_batch(
            snapshots=[{
                "entity_id": "c_001", "entity_type": "customer", "state": "active",
                "properties": {"name": "Alice", "mrr": 99.0, "signup_count": 1, "is_active": True, "tags": []},
                "valid_from": "2024-01-15T10:00:00Z",
            }],
            tombstones=[],
        )
        sh.delete_entity("c_001")
        count = sh._conn.execute("SELECT COUNT(*) FROM customer_history").fetchone()[0]
        assert count == 0

    def test_delete_nonexistent_entity(self):
        sh = SqliteStateHistory(":memory:")
        sh.ensure_tables({"customer": CUSTOMER_DEF})
        sh.delete_entity("nobody")  # should not raise


# ===========================================================================
# DuckDB State History
# ===========================================================================


class TestDuckDBStateHistory:
    """DuckDB state history — in-process columnar analytics backend."""

    def test_ensure_tables_creates_schema(self):
        from defacto.backends import DuckDBStateHistory

        sh = DuckDBStateHistory(":memory:", "v1")
        sh.ensure_tables({"customer": CUSTOMER_DEF})
        result = sh._conn.execute(
            "SELECT table_name FROM information_schema.tables"
            " WHERE table_schema = 'defacto_v1'"
        ).fetchall()
        table_names = [r[0] for r in result]
        assert "customer_history" in table_names
        sh.close()

    def test_write_and_read(self):
        from defacto.backends import DuckDBStateHistory

        sh = DuckDBStateHistory(":memory:", "v1")
        sh.ensure_tables({"customer": CUSTOMER_DEF})
        sh.write_batch(
            snapshots=[{
                "entity_id": "c_001", "entity_type": "customer",
                "state": "active",
                "properties": {"name": "Alice", "mrr": 99.0,
                               "signup_count": 1, "is_active": True, "tags": []},
                "valid_from": "2024-01-15T10:00:00Z",
            }],
            tombstones=[],
        )
        rows = sh._conn.execute(
            "SELECT customer_id, customer_state, name, mrr"
            " FROM defacto_v1.customer_history"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "c_001"
        assert rows[0][1] == "active"
        assert rows[0][2] == "Alice"
        assert rows[0][3] == 99.0
        sh.close()

    def test_write_idempotent(self):
        from defacto.backends import DuckDBStateHistory

        sh = DuckDBStateHistory(":memory:", "v1")
        sh.ensure_tables({"customer": CUSTOMER_DEF})
        snap = {
            "entity_id": "c_001", "entity_type": "customer",
            "state": "active",
            "properties": {"name": "Alice", "mrr": 99.0,
                           "signup_count": 1, "is_active": True, "tags": []},
            "valid_from": "2024-01-15T10:00:00Z",
        }
        sh.write_batch(snapshots=[snap], tombstones=[])
        sh.write_batch(snapshots=[snap], tombstones=[])  # duplicate
        count = sh._conn.execute(
            "SELECT COUNT(*) FROM defacto_v1.customer_history"
        ).fetchone()[0]
        assert count == 1  # idempotent
        sh.close()

    def test_close_previous_version(self):
        from defacto.backends import DuckDBStateHistory

        sh = DuckDBStateHistory(":memory:", "v1")
        sh.ensure_tables({"customer": CUSTOMER_DEF})
        sh.write_batch(
            snapshots=[{
                "entity_id": "c_001", "entity_type": "customer",
                "state": "trial",
                "properties": {"name": "Alice", "mrr": 0,
                               "signup_count": 1, "is_active": True, "tags": []},
                "valid_from": "2024-01-15T10:00:00Z",
            }],
            tombstones=[],
        )
        sh.write_batch(
            snapshots=[{
                "entity_id": "c_001", "entity_type": "customer",
                "state": "active",
                "properties": {"name": "Alice", "mrr": 99.0,
                               "signup_count": 1, "is_active": True, "tags": []},
                "valid_from": "2024-01-16T10:00:00Z",
            }],
            tombstones=[],
        )
        rows = sh._conn.execute(
            "SELECT valid_from, valid_to, customer_state"
            " FROM defacto_v1.customer_history ORDER BY valid_from"
        ).fetchall()
        assert len(rows) == 2
        assert rows[0][2] == "trial"
        assert rows[0][1] is not None  # closed
        assert rows[1][2] == "active"
        assert rows[1][1] is None  # current
        sh.close()
