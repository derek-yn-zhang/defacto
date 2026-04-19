"""LedgerBackend contract tests.

Every LedgerBackend implementation must satisfy these correctness properties.
Tests are parametrized across SQLite, Postgres, and TieredLedger via the
`ledger` fixture in conftest.py.

Each test exercises one method or property from the LedgerBackend ABC.
If a test fails for one backend but passes for another, that backend has
a contract violation.
"""

import json

from tests.correctness.conftest import make_event


# ---------------------------------------------------------------------------
# append_batch
# ---------------------------------------------------------------------------


class TestAppendBatch:
    """append_batch: append events, return sequences, dedup by event_id."""

    def test_returns_sequences(self, ledger):
        """Appended events get monotonically increasing sequence numbers."""
        seqs = ledger.append_batch([make_event()])
        assert len(seqs) == 1
        assert seqs[0] > 0

    def test_multiple_events(self, ledger):
        """Multiple events in one batch get sequential sequences."""
        seqs = ledger.append_batch([
            make_event(event_id="a", timestamp="2024-01-01T10:00:00Z"),
            make_event(event_id="b", timestamp="2024-01-01T11:00:00Z"),
            make_event(event_id="c", timestamp="2024-01-01T12:00:00Z"),
        ])
        assert len(seqs) == 3
        assert seqs[0] < seqs[1] < seqs[2]

    def test_dedup_by_event_id(self, ledger):
        """Duplicate event_ids produce only one row in the ledger."""
        ledger.append_batch([make_event(event_id="dup")])
        ledger.append_batch([make_event(event_id="dup")])
        # Regardless of what append_batch returns, the ledger has exactly one event
        events = list(ledger.replay(from_sequence=0))
        assert len(events) == 1
        assert events[0]["event_id"] == "dup"

    def test_empty_batch(self, ledger):
        """Empty batch returns empty list, no error."""
        seqs = ledger.append_batch([])
        assert seqs == []


# ---------------------------------------------------------------------------
# cursor
# ---------------------------------------------------------------------------


class TestCursor:
    """cursor: returns the highest assigned sequence number."""

    def test_empty_ledger(self, ledger):
        """Empty ledger has cursor 0."""
        assert ledger.cursor() == 0

    def test_after_append(self, ledger):
        """Cursor advances to the max sequence after append."""
        seqs = ledger.append_batch([
            make_event(event_id="a"),
            make_event(event_id="b"),
        ])
        assert ledger.cursor() == max(seqs)


# ---------------------------------------------------------------------------
# replay
# ---------------------------------------------------------------------------


class TestReplay:
    """replay: iterate events in timestamp order."""

    def test_replay_all(self, ledger):
        """replay(from_sequence=0) returns all events."""
        ledger.append_batch([
            make_event(event_id="a", timestamp="2024-01-01T10:00:00Z"),
            make_event(event_id="b", timestamp="2024-01-01T11:00:00Z"),
        ])
        events = list(ledger.replay(from_sequence=0))
        assert len(events) == 2

    def test_replay_from_sequence(self, ledger):
        """replay(from_sequence=N) returns events at or after sequence N.

        from_sequence is inclusive (WHERE sequence >= N). This is the
        contract used by incremental builds — the cursor points to the
        last processed event, so replay starts from cursor + 1.
        """
        seqs = ledger.append_batch([
            make_event(event_id="a", timestamp="2024-01-01T10:00:00Z"),
            make_event(event_id="b", timestamp="2024-01-01T11:00:00Z"),
        ])
        # from_sequence = max(seqs) → only the last event (inclusive)
        events = list(ledger.replay(from_sequence=max(seqs)))
        assert len(events) == 1
        assert events[0]["event_id"] == "b"

        # from_sequence past the end → no events
        events = list(ledger.replay(from_sequence=max(seqs) + 1))
        assert len(events) == 0

    def test_replay_timestamp_order(self, ledger):
        """Events are replayed in timestamp order regardless of insert order."""
        ledger.append_batch([
            make_event(event_id="late", timestamp="2024-01-01T15:00:00Z"),
            make_event(event_id="early", timestamp="2024-01-01T08:00:00Z"),
            make_event(event_id="mid", timestamp="2024-01-01T12:00:00Z"),
        ])
        events = list(ledger.replay(from_sequence=0))
        ids = [e["event_id"] for e in events]
        assert ids == ["early", "mid", "late"]

    def test_replay_filters_duplicates(self, ledger):
        """replay() excludes events with duplicate_of set (default behavior).

        This requires update_normalized to set duplicate_of, so we test
        the interaction between the two methods.
        """
        seqs = ledger.append_batch([
            make_event(event_id="canonical", timestamp="2024-01-01T10:00:00Z"),
            make_event(event_id="dup", timestamp="2024-01-01T11:00:00Z"),
        ])
        # Mark second event as duplicate of first
        ledger.update_normalized([
            (seqs[1], {
                "data": {"email": "alice@example.com"},
                "event_id": "dup",
                "event_type": "signup",
                "resolution_hints": json.dumps({"customer": {"email": "alice@example.com"}}),
                "duplicate_of": seqs[0],
            }),
        ])
        events = list(ledger.replay(from_sequence=0))
        assert len(events) == 1
        assert events[0]["event_id"] == "canonical"

    def test_replay_empty_ledger(self, ledger):
        """Replaying an empty ledger yields nothing."""
        events = list(ledger.replay(from_sequence=0))
        assert events == []


# ---------------------------------------------------------------------------
# event_entities
# ---------------------------------------------------------------------------


class TestEventEntities:
    """write_event_entities + replay_for_entities: entity-scoped replay."""

    def test_write_and_replay_round_trip(self, ledger):
        """Events for a specific entity can be replayed via event_entities."""
        seqs = ledger.append_batch([
            make_event(event_id="a", timestamp="2024-01-01T10:00:00Z"),
            make_event(event_id="b", timestamp="2024-01-01T11:00:00Z"),
            make_event(event_id="c", timestamp="2024-01-01T12:00:00Z"),
        ])
        # a and c belong to alice, b belongs to bob
        ledger.write_event_entities([
            (seqs[0], "alice", "customer"),
            (seqs[1], "bob", "customer"),
            (seqs[2], "alice", "customer"),
        ])
        events = list(ledger.replay_for_entities(["alice"]))
        ids = [e["event_id"] for e in events]
        assert "a" in ids
        assert "c" in ids
        assert "b" not in ids

    def test_replay_for_entities_dedup(self, ledger):
        """If an event affects multiple requested entities, it appears once."""
        seqs = ledger.append_batch([make_event(event_id="shared")])
        ledger.write_event_entities([
            (seqs[0], "alice", "customer"),
            (seqs[0], "bob", "customer"),
        ])
        events = list(ledger.replay_for_entities(["alice", "bob"]))
        assert len(events) == 1

    def test_replay_for_entities_empty_list(self, ledger):
        """Empty entity list returns no events."""
        ledger.append_batch([make_event()])
        events = list(ledger.replay_for_entities([]))
        assert events == []

    def test_clear_event_entities(self, ledger):
        """clear_event_entities removes all mappings."""
        seqs = ledger.append_batch([make_event()])
        ledger.write_event_entities([(seqs[0], "alice", "customer")])
        ledger.clear_event_entities()
        events = list(ledger.replay_for_entities(["alice"]))
        assert events == []


# ---------------------------------------------------------------------------
# redact
# ---------------------------------------------------------------------------


class TestRedact:
    """redact: replace sensitive fields with [REDACTED]."""

    def test_redacts_data_and_raw(self, ledger):
        """Redact replaces fields in both data and raw columns."""
        seqs = ledger.append_batch([make_event(
            data={"email": "alice@test.com", "plan": "pro"},
            raw={"email": "alice@test.com", "plan": "pro", "type": "signup"},
        )])
        ledger.write_event_entities([(seqs[0], "alice", "customer")])
        count = ledger.redact("alice", ["email"])
        assert count >= 1

        events = list(ledger.replay(from_sequence=0))
        assert events[0]["data"]["email"] == "[REDACTED]"
        assert events[0]["raw"]["email"] == "[REDACTED]"
        # Non-redacted fields preserved
        assert events[0]["data"]["plan"] == "pro"


# ---------------------------------------------------------------------------
# update_normalized
# ---------------------------------------------------------------------------


class TestUpdateNormalized:
    """update_normalized: rewrite derived fields during renormalization."""

    def test_updates_derived_fields(self, ledger):
        """update_normalized changes data, event_id, event_type, hints."""
        seqs = ledger.append_batch([make_event(
            event_id="old_id",
            event_type="old_type",
            data={"old": "data"},
        )])
        ledger.update_normalized([
            (seqs[0], {
                "data": {"new": "data"},
                "event_id": "new_id",
                "event_type": "new_type",
                "resolution_hints": json.dumps({"customer": {"phone": "555"}}),
                "duplicate_of": None,
            }),
        ])
        events = list(ledger.replay(from_sequence=0))
        assert events[0]["event_id"] == "new_id"
        assert events[0]["event_type"] == "new_type"
        assert events[0]["data"] == {"new": "data"}


# ---------------------------------------------------------------------------
# count and events_for
# ---------------------------------------------------------------------------


class TestInspection:
    """count and events_for: ledger inspection methods."""

    def test_count_total(self, ledger):
        """count() returns total events."""
        ledger.append_batch([
            make_event(event_id="a"),
            make_event(event_id="b"),
        ])
        assert ledger.count() == 2

    def test_count_by_source(self, ledger):
        """count(source=) filters by source."""
        ledger.append_batch([
            make_event(event_id="a", source="web"),
            make_event(event_id="b", source="api"),
        ])
        assert ledger.count(source="web") == 1
        assert ledger.count(source="api") == 1

    def test_events_for_entity(self, ledger):
        """events_for returns event summaries for a specific entity."""
        seqs = ledger.append_batch([
            make_event(event_id="a", timestamp="2024-01-01T10:00:00Z"),
            make_event(event_id="b", timestamp="2024-01-01T11:00:00Z"),
        ])
        ledger.write_event_entities([
            (seqs[0], "alice", "customer"),
            (seqs[1], "bob", "customer"),
        ])
        events = ledger.events_for("alice")
        assert len(events) == 1
        assert events[0]["event_id"] == "a"

    def test_lookup_entity_type(self, ledger):
        """lookup_entity_type finds the entity type from event_entities."""
        seqs = ledger.append_batch([make_event()])
        ledger.write_event_entities([(seqs[0], "alice", "customer")])
        assert ledger.lookup_entity_type("alice") == "customer"
        assert ledger.lookup_entity_type("unknown") is None


# ---------------------------------------------------------------------------
# JSON preservation
# ---------------------------------------------------------------------------


class TestJsonPreservation:
    """Data, raw, and resolution_hints survive the append→replay round trip."""

    def test_complex_json_round_trip(self, ledger):
        """Nested JSON with various types is preserved exactly."""
        complex_data = {
            "string": "hello",
            "number": 42,
            "float": 3.14,
            "boolean": True,
            "null_val": None,
            "nested": {"a": [1, 2, 3]},
        }
        complex_raw = {"original": "payload", "items": [{"id": 1}, {"id": 2}]}
        complex_hints = {"customer": {"email": "alice@example.com"}}

        ledger.append_batch([make_event(
            data=complex_data,
            raw=complex_raw,
            resolution_hints=complex_hints,
        )])
        events = list(ledger.replay(from_sequence=0))
        assert events[0]["data"] == complex_data
        assert events[0]["raw"] == complex_raw
        assert events[0]["resolution_hints"] == complex_hints


# ---------------------------------------------------------------------------
# Transaction protocol
# ---------------------------------------------------------------------------


class TestReplayForShard:
    """replay_for_shard: shard-aware replay via event_entities filtering."""

    def _shard_for(self, entity_id: str, total_shards: int) -> int:
        """Match the engine's shard assignment logic (4-byte hash)."""
        import hashlib
        h = int.from_bytes(hashlib.sha256(entity_id.encode()).digest()[:4])
        return h % total_shards

    def test_shards_partition_events(self, ledger):
        """Two shards cover all events with no overlap."""
        seqs = ledger.append_batch([
            make_event(event_id=f"e{i}", timestamp=f"2024-01-01T{10+i}:00:00Z")
            for i in range(6)
        ])
        # Assign each event to a distinct entity
        entities = [f"entity_{i}" for i in range(6)]
        ledger.write_event_entities([
            (seqs[i], entities[i], "customer") for i in range(6)
        ])

        shard_0 = list(ledger.replay_for_shard(0, 2))
        shard_1 = list(ledger.replay_for_shard(1, 2))

        # Union covers all events
        all_ids = {e["event_id"] for e in shard_0} | {e["event_id"] for e in shard_1}
        assert all_ids == {f"e{i}" for i in range(6)}

        # No overlap
        ids_0 = {e["event_id"] for e in shard_0}
        ids_1 = {e["event_id"] for e in shard_1}
        assert ids_0 & ids_1 == set()

    def test_shard_assignment_matches_python(self, ledger):
        """SQL/Python shard filter produces the same assignment as _shard_for_entity."""
        seqs = ledger.append_batch([
            make_event(event_id=f"e{i}", timestamp=f"2024-01-01T{10+i}:00:00Z")
            for i in range(10)
        ])
        entities = [f"ent_{i}" for i in range(10)]
        ledger.write_event_entities([
            (seqs[i], entities[i], "customer") for i in range(10)
        ])

        for shard_id in range(3):
            events = list(ledger.replay_for_shard(shard_id, 3))
            # Every returned event should map to an entity owned by this shard
            for event in events:
                seq = event["sequence"]
                # Find entity for this sequence
                idx = seqs.index(seq)
                entity_id = entities[idx]
                assert self._shard_for(entity_id, 3) == shard_id

    def test_from_sequence_filters(self, ledger):
        """from_sequence limits replay to events at or after that sequence."""
        seqs = ledger.append_batch([
            make_event(event_id=f"e{i}", timestamp=f"2024-01-01T{10+i}:00:00Z")
            for i in range(6)
        ])
        entities = [f"ent_{i}" for i in range(6)]
        ledger.write_event_entities([
            (seqs[i], entities[i], "customer") for i in range(6)
        ])

        midpoint = seqs[3]
        for shard_id in range(2):
            events = list(ledger.replay_for_shard(shard_id, 2, from_sequence=midpoint))
            for event in events:
                assert event["sequence"] >= midpoint

    def test_empty_event_entities(self, ledger):
        """replay_for_shard returns nothing when event_entities is empty."""
        ledger.append_batch([make_event()])
        events = list(ledger.replay_for_shard(0, 2))
        assert events == []

    def test_has_event_entities(self, ledger):
        """has_event_entities reflects whether mappings exist."""
        assert not ledger.has_event_entities()
        seqs = ledger.append_batch([make_event()])
        ledger.write_event_entities([(seqs[0], "alice", "customer")])
        assert ledger.has_event_entities()


class TestMergeLog:
    """merge_log: write, find, delete merge relationships."""

    def test_write_and_find(self, ledger):
        """Written merge is findable via find_merges_into."""
        ledger.write_merge_log("loser", "winner", "2024-01-15T10:00:00Z")
        merged = ledger.find_merges_into("winner")
        assert merged == ["loser"]

    def test_find_empty(self, ledger):
        """No merges returns empty list."""
        assert ledger.find_merges_into("nonexistent") == []

    def test_multiple_merges_into_same_winner(self, ledger):
        """Multiple losers merged into the same winner."""
        ledger.write_merge_log("loser_a", "winner", "2024-01-15T10:00:00Z")
        ledger.write_merge_log("loser_b", "winner", "2024-01-15T11:00:00Z")
        merged = ledger.find_merges_into("winner")
        assert set(merged) == {"loser_a", "loser_b"}

    def test_write_idempotent(self, ledger):
        """Writing the same merge twice doesn't create duplicates."""
        ledger.write_merge_log("loser", "winner", "2024-01-15T10:00:00Z")
        ledger.write_merge_log("loser", "winner", "2024-01-15T10:00:00Z")
        merged = ledger.find_merges_into("winner")
        assert merged == ["loser"]

    def test_delete_single(self, ledger):
        """delete_merge_log removes entries for an entity."""
        ledger.write_merge_log("loser", "winner", "2024-01-15T10:00:00Z")
        ledger.delete_merge_log("loser")
        assert ledger.find_merges_into("winner") == []

    def test_delete_batch(self, ledger):
        """delete_merge_log_batch removes entries for multiple entities."""
        ledger.write_merge_log("a", "winner", "2024-01-15T10:00:00Z")
        ledger.write_merge_log("b", "winner", "2024-01-15T11:00:00Z")
        ledger.delete_merge_log_batch(["a", "b"])
        assert ledger.find_merges_into("winner") == []

    def test_delete_batch_empty(self, ledger):
        """Empty batch delete is a no-op."""
        ledger.delete_merge_log_batch([])  # should not raise

    def test_cascade_chain(self, ledger):
        """Merge cascade: A→B→C. find_merges_into(C) returns B.
        Walking the chain: find_merges_into(B) returns A."""
        ledger.write_merge_log("a", "b", "2024-01-15T10:00:00Z")
        ledger.write_merge_log("b", "c", "2024-01-15T11:00:00Z")
        assert ledger.find_merges_into("c") == ["b"]
        assert ledger.find_merges_into("b") == ["a"]


class TestDeleteEventsFor:
    """delete_events_for: remove events and event_entities for an entity."""

    def test_deletes_events(self, ledger):
        """Events for the entity are removed from the ledger."""
        seqs = ledger.append_batch([
            make_event(event_id="e1"),
            make_event(event_id="e2"),
        ])
        ledger.write_event_entities([
            (seqs[0], "ent_1", "customer"),
            (seqs[1], "ent_1", "customer"),
        ])
        count = ledger.delete_events_for("ent_1")
        assert count == 2
        assert list(ledger.replay()) == []

    def test_delete_unknown_entity(self, ledger):
        """Deleting events for a nonexistent entity returns 0."""
        assert ledger.delete_events_for("nonexistent") == 0


class TestLookupEntityType:
    """lookup_entity_type: find entity type from event_entities."""

    def test_finds_type(self, ledger):
        """Returns entity type for a known entity."""
        seqs = ledger.append_batch([make_event()])
        ledger.write_event_entities([(seqs[0], "ent_1", "customer")])
        assert ledger.lookup_entity_type("ent_1") == "customer"

    def test_unknown_entity(self, ledger):
        """Returns None for an unknown entity."""
        assert ledger.lookup_entity_type("nonexistent") is None


class TestTransactions:
    """begin/commit: explicit transaction boundaries."""

    def test_begin_commit_no_error(self, ledger):
        """begin() + commit() should not raise for any backend."""
        ledger.begin()
        ledger.commit()
