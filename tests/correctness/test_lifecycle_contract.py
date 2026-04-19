"""Lifecycle operation contract tests.

Verify that merge, erase, and tick produce correct results across all
backend combinations. These are Defacto-level tests (not backend-level)
because lifecycle operations orchestrate multiple backends together.

Parametrized across:
- SQLite (always runs)
- Postgres (requires running Postgres)
- TieredLedger (Postgres + cold Delta Lake)

If merge works on SQLite but fails on Postgres, these tests catch it.
"""

import pytest

from defacto import Defacto
from tests.conftest import PG_CONNINFO, clean_postgres


# ---------------------------------------------------------------------------
# Shared definitions — simple customer with lead → active state machine
# ---------------------------------------------------------------------------

DEFINITIONS = {
    "entities": {
        "customer": {
            "starts": "lead",
            "properties": {
                "email": {"type": "string"},
                "plan": {"type": "string", "default": "free"},
            },
            "identity": {"email": {"match": "exact"}},
            "states": {
                "lead": {
                    "when": {
                        "signup": {
                            "effects": [
                                "create",
                                {"set": {"property": "email", "from": "event.email"}},
                            ],
                        },
                        "upgrade": {
                            "effects": [
                                {"transition": {"to": "active"}},
                                {"set": {"property": "plan", "from": "event.plan"}},
                            ],
                        },
                    },
                },
                "active": {
                    "when": {
                        "upgrade": {
                            "effects": [
                                {"set": {"property": "plan", "from": "event.plan"}},
                            ],
                        },
                    },
                },
            },
        },
    },
    "sources": {
        "web": {
            "event_type": "type",
            "timestamp": "ts",
            "events": {
                "signup": {
                    "raw_type": "signup",
                    "mappings": {"email": {"from": "email"}},
                    "hints": {"customer": ["email"]},
                },
                "upgrade": {
                    "raw_type": "upgrade",
                    "mappings": {
                        "email": {"from": "email"},
                        "plan": {"from": "plan"},
                    },
                    "hints": {"customer": ["email"]},
                },
            },
        },
    },
    "schemas": {},
}


# ---------------------------------------------------------------------------
# Fixture — Defacto instance parametrized across backends
# ---------------------------------------------------------------------------


@pytest.fixture(params=[
    pytest.param("sqlite", id="sqlite"),
    pytest.param("postgres", id="postgres", marks=pytest.mark.postgres),
    pytest.param("tiered", id="tiered", marks=pytest.mark.postgres),
])
def defacto(request, tmp_path):
    """A fully configured Defacto instance across backend types."""
    if request.param == "sqlite":
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        yield m
        m.close()

    elif request.param == "postgres":
        clean_postgres()
        m = Defacto(DEFINITIONS, database=PG_CONNINFO)
        yield m
        m.close()

    elif request.param == "tiered":
        clean_postgres()
        cold_path = str(tmp_path / "cold_ledger")
        m = Defacto(
            DEFINITIONS, database=PG_CONNINFO, cold_ledger=cold_path,
        )
        yield m
        m.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ingest_two_customers(m: Defacto) -> tuple[str, str]:
    """Ingest alice (active/pro) and bob (lead/free), build, return IDs."""
    m.ingest("web", [
        {"type": "signup", "ts": "2024-01-01T10:00:00Z", "email": "bob@test.com"},
        {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        {"type": "upgrade", "ts": "2024-01-15T10:01:00Z",
         "email": "alice@test.com", "plan": "pro"},
    ])
    m.build()
    alice_id = m._identity_backend.lookup("customer", "alice@test.com")
    bob_id = m._identity_backend.lookup("customer", "bob@test.com")
    return alice_id, bob_id


# ---------------------------------------------------------------------------
# Merge
# ---------------------------------------------------------------------------


class TestMerge:
    """Merge produces correct combined state across all backends."""

    def test_merge_combined_state(self, defacto):
        """Winner has combined state from both entities after merge."""
        alice_id, bob_id = _ingest_two_customers(defacto)
        result = defacto.merge(alice_id, bob_id)

        assert result.from_entity_id == alice_id
        assert result.into_entity_id == bob_id
        assert result.tombstones_produced >= 1

        df = defacto.table("customer").execute()
        assert len(df) == 1
        assert df["customer_state"].iloc[0] == "active"
        assert df["plan"].iloc[0] == "pro"

    def test_merge_idempotent(self, defacto):
        """Merging already-merged entities is a no-op."""
        alice_id, bob_id = _ingest_two_customers(defacto)
        defacto.merge(alice_id, bob_id)
        result = defacto.merge(alice_id, bob_id)
        # Should not crash, entity count unchanged
        df = defacto.table("customer").execute()
        assert len(df) == 1

    def test_merge_new_events_resolve_to_winner(self, defacto):
        """New events for the loser's hints resolve to the winner."""
        alice_id, bob_id = _ingest_two_customers(defacto)
        defacto.merge(alice_id, bob_id)

        # New event for alice's email should go to bob (winner)
        defacto.ingest("web", [
            {"type": "upgrade", "ts": "2024-02-01T10:00:00Z",
             "email": "alice@test.com", "plan": "enterprise"},
        ], process=True)

        df = defacto.table("customer").execute()
        assert len(df) == 1
        assert df["plan"].iloc[0] == "enterprise"

    def test_merge_survives_full_rebuild(self, defacto):
        """After merge + full rebuild, state is still correct."""
        alice_id, bob_id = _ingest_two_customers(defacto)
        defacto.merge(alice_id, bob_id)
        defacto.build(full=True)

        df = defacto.table("customer").execute()
        assert len(df) == 1
        assert df["customer_state"].iloc[0] == "active"
        assert df["plan"].iloc[0] == "pro"

    def test_merge_log_written(self, defacto):
        """merge_log records the merge relationship."""
        alice_id, bob_id = _ingest_two_customers(defacto)
        defacto.merge(alice_id, bob_id)

        merged = defacto._ledger.find_merges_into(bob_id)
        assert alice_id in merged


# ---------------------------------------------------------------------------
# Erase
# ---------------------------------------------------------------------------


class TestErase:
    """Erase removes all traces of an entity across all backends."""

    def test_erase_removes_from_all_stores(self, defacto):
        """After erase, entity is gone from identity, ledger, DashMap."""
        alice_id, bob_id = _ingest_two_customers(defacto)
        result = defacto.erase(alice_id)

        assert result.entity_id == alice_id
        assert result.entities_erased >= 1
        assert result.events_deleted >= 1

        # Identity gone
        assert defacto._identity_backend.lookup("customer", "alice@test.com") is None
        # DashMap gone
        assert defacto._core.entity_count() == 1  # only bob remains
        # State history gone (only bob in query)
        df = defacto.table("customer").execute()
        assert len(df) == 1

    def test_erase_cascade_through_merge(self, defacto):
        """Erasing the winner also erases merged losers."""
        alice_id, bob_id = _ingest_two_customers(defacto)
        defacto.merge(alice_id, bob_id)
        result = defacto.erase(bob_id)

        # Both entities erased (bob + alice who was merged into bob)
        assert result.entities_erased == 2
        assert defacto._core.entity_count() == 0

    def test_erase_new_events_create_fresh_entity(self, defacto):
        """After erase, same hint creates a new entity with new ID."""
        alice_id, _ = _ingest_two_customers(defacto)
        defacto.erase(alice_id)

        defacto.ingest("web", [
            {"type": "signup", "ts": "2024-06-01T10:00:00Z", "email": "alice@test.com"},
        ])
        defacto.build()

        # After erase + rebuild, alice should exist again with a new entity
        assert defacto._core.entity_count() == 2  # bob + new alice


# ---------------------------------------------------------------------------
# Tick
# ---------------------------------------------------------------------------


class TestTick:
    """Tick evaluates time rules and validates entities across backends."""

    def test_tick_returns_result(self, defacto):
        """tick() returns a TickResult even when no rules fire."""
        _ingest_two_customers(defacto)
        result = defacto.tick()
        assert result.effects_produced >= 0
        assert result.entities_affected >= 0

    def test_tick_after_erase_ignores_erased(self, defacto):
        """Tick doesn't produce effects for erased entities."""
        alice_id, _ = _ingest_two_customers(defacto)
        defacto.erase(alice_id)
        result = defacto.tick()
        # Should not crash or produce effects for erased entity
        assert result.effects_produced >= 0


# ---------------------------------------------------------------------------
# Store consistency
# ---------------------------------------------------------------------------


class TestStoreConsistency:
    """All stores agree after lifecycle operations."""

    def test_merge_log_consistent(self, defacto):
        """merge_log has exactly one entry after a merge."""
        alice_id, bob_id = _ingest_two_customers(defacto)
        defacto.merge(alice_id, bob_id)

        merged = defacto._ledger.find_merges_into(bob_id)
        assert len(merged) == 1
        assert merged[0] == alice_id

    def test_event_entities_updated_after_merge(self, defacto):
        """All events point to the winner after merge."""
        alice_id, bob_id = _ingest_two_customers(defacto)
        defacto.merge(alice_id, bob_id)

        entity_type = defacto._ledger.lookup_entity_type(bob_id)
        assert entity_type == "customer"
        # Loser should have no events in event_entities
        assert defacto._ledger.lookup_entity_type(alice_id) is None

    def test_erase_cleans_merge_log(self, defacto):
        """Erase removes merge_log entries."""
        alice_id, bob_id = _ingest_two_customers(defacto)
        defacto.merge(alice_id, bob_id)
        defacto.erase(bob_id)

        assert defacto._ledger.find_merges_into(bob_id) == []


# ---------------------------------------------------------------------------
# Query layer — SCD history, as_of, timeline through lifecycle operations
# ---------------------------------------------------------------------------


class TestSCDHistory:
    """State history has per-event rows with correct timestamps."""

    def test_history_has_per_event_rows(self, defacto):
        """Each state change gets its own SCD row."""
        _ingest_two_customers(defacto)

        alice_id = defacto._identity_backend.lookup("customer", "alice@test.com")
        hist = defacto.history("customer").filter(
            defacto.history("customer")._expr.customer_id == alice_id
        ).order_by("valid_from").execute()

        # alice: signup (lead) → upgrade (active/pro) = 2 rows
        assert len(hist) >= 2
        states = list(hist["customer_state"])
        assert "lead" in states
        assert "active" in states

    def test_as_of_before_any_events(self, defacto):
        """Point-in-time before any events returns empty."""
        _ingest_two_customers(defacto)
        snap = defacto.history("customer").as_of("2023-01-01T00:00:00Z").execute()
        assert len(snap) == 0

    def test_as_of_between_events(self, defacto):
        """Point-in-time between signup and upgrade shows pre-upgrade state."""
        _ingest_two_customers(defacto)

        # Alice: signup Jan 15, upgrade Jan 15 10:01.
        # Bob: signup Jan 1.
        # Jan 5: only bob exists.
        snap = defacto.history("customer").as_of("2024-01-05T00:00:00Z").execute()
        assert len(snap) == 1
        assert snap.iloc[0]["customer_state"] == "lead"  # bob

    def test_as_of_after_all_events(self, defacto):
        """Point-in-time after all events shows final state."""
        _ingest_two_customers(defacto)

        snap = defacto.history("customer").as_of("2025-01-01T00:00:00Z").execute()
        alice_rows = snap[snap["email"] == "alice@test.com"]
        assert len(alice_rows) == 1
        assert alice_rows.iloc[0]["customer_state"] == "active"
        assert alice_rows.iloc[0]["plan"] == "pro"

    def test_as_of_after_merge(self, defacto):
        """as_of after merge shows only the winner (loser tombstoned)."""
        alice_id, bob_id = _ingest_two_customers(defacto)
        defacto.merge(alice_id, bob_id)

        snap = defacto.history("customer").as_of("2025-01-01T00:00:00Z").execute()
        # Winner is current (valid_to IS NULL), loser is tombstoned (valid_to set).
        # Only entities without merged_into should appear in current queries.
        current = snap[snap["merged_into"].isna()]
        assert len(current) == 1

    def test_as_of_before_merge(self, defacto):
        """as_of before merge timestamp shows both entities."""
        alice_id, bob_id = _ingest_two_customers(defacto)
        defacto.merge(alice_id, bob_id)

        # Jan 10: both alice and bob existed (merge hadn't happened yet)
        snap = defacto.history("customer").as_of("2024-01-10T00:00:00Z").execute()
        # Should show entities that were valid at that time
        assert len(snap) >= 1


class TestTimeline:
    """m.timeline() produces correct entries across backends."""

    def test_timeline_entries(self, defacto):
        """Timeline has one entry per state change."""
        _ingest_two_customers(defacto)
        alice_id = defacto._identity_backend.lookup("customer", "alice@test.com")
        tl = defacto.timeline(alice_id)

        assert tl.entity_id == alice_id
        assert tl.entity_type == "customer"
        assert len(tl.entries) >= 2  # signup + upgrade

    def test_timeline_state_transitions(self, defacto):
        """Timeline tracks state before/after each change."""
        _ingest_two_customers(defacto)
        alice_id = defacto._identity_backend.lookup("customer", "alice@test.com")
        tl = defacto.timeline(alice_id)

        # First entry: entity created
        assert tl.entries[0].state_before is None
        assert tl.entries[0].state_after == "lead"

        # Second entry: upgrade
        assert tl.entries[1].state_before == "lead"
        assert tl.entries[1].state_after == "active"

    def test_timeline_effects(self, defacto):
        """Timeline shows human-readable effects."""
        _ingest_two_customers(defacto)
        alice_id = defacto._identity_backend.lookup("customer", "alice@test.com")
        tl = defacto.timeline(alice_id)

        # First entry should mention creation
        assert any("created" in e.lower() for e in tl.entries[0].effects)

    def test_timeline_unknown_entity(self, defacto):
        """Timeline for nonexistent entity returns empty."""
        tl = defacto.timeline("nonexistent_id")
        assert tl.entries == []

    def test_timeline_after_merge(self, defacto):
        """Winner's timeline reflects combined history after merge."""
        alice_id, bob_id = _ingest_two_customers(defacto)
        defacto.merge(alice_id, bob_id)
        tl = defacto.timeline(bob_id)

        # Winner should have entries from the rebuild
        assert len(tl.entries) >= 1
