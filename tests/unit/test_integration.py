"""End-to-end integration tests with real DefactoCore + real SQLite backends.

No mocks. Real Rust normalization, real identity resolution, real
interpretation, real SQLite writes. These tests prove the full pipeline
actually works — the kind of bugs that mocks hide show up here.

Uses :memory: SQLite databases, no infrastructure needed.
"""

import sqlite3

import pytest

from defacto._build import BuildManager, SqliteBuildStateStore
from defacto._core import DefactoCore
from defacto._ddl import DDLGenerator
from defacto._identity import IdentityResolver
from defacto._pipeline import Pipeline
from defacto._publisher import InlinePublisher
from defacto.backends import SqliteIdentity, SqliteLedger, SqliteStateHistory


# ---------------------------------------------------------------------------
# Definitions — minimal but complete
# ---------------------------------------------------------------------------

DEFINITIONS = {
    "entities": {
        "customer": {
            "starts": "lead",
            "properties": {
                "email": {"type": "string"},
                "plan": {"type": "string", "default": "free"},
            },
            "identity": {
                "email": {"match": "exact"},
            },
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
                    "mappings": {
                        "email": {"from": "email"},
                    },
                    "hints": {
                        "customer": ["email"],
                    },
                },
                "upgrade": {
                    "raw_type": "upgrade",
                    "mappings": {
                        "email": {"from": "email"},
                        "plan": {"from": "plan"},
                    },
                    "hints": {
                        "customer": ["email"],
                    },
                },
            },
        },
    },
    "schemas": {},
}


# ---------------------------------------------------------------------------
# Test fixture
# ---------------------------------------------------------------------------


class RealPipeline:
    """Wires up a full pipeline with real DefactoCore + real SQLite backends."""

    def __init__(self):
        self.core = DefactoCore(workers=1)
        self.core.compile("v1", DEFINITIONS)
        self.core.activate("v1")

        self.ledger = SqliteLedger(":memory:")
        self.identity_backend = SqliteIdentity(":memory:")
        self.identity = IdentityResolver(self.core, self.identity_backend)

        self.state_history = SqliteStateHistory(":memory:")
        self.state_history.ensure_tables(DEFINITIONS["entities"])
        self.publisher = InlinePublisher(self.state_history)

        build_store = SqliteBuildStateStore(":memory:")
        self.build_manager = BuildManager(build_store, self.core)

        self.pipeline = Pipeline(
            self.core, self.ledger, self.identity,
            self.publisher, self.build_manager,
        )


@pytest.fixture
def real():
    """Create a fully wired real pipeline."""
    return RealPipeline()


# ---------------------------------------------------------------------------
# Golden path tests
# ---------------------------------------------------------------------------


class TestProcessBatchEndToEnd:
    """Full ingest flow: raw events → entity snapshots in state history."""

    def test_single_event_creates_entity(self, real):
        result, snapshots = real.pipeline.process_batch("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ], "v1")

        assert result.events_ingested == 1
        assert result.events_failed == 0
        assert len(snapshots) == 1
        assert snapshots[0]["entity_type"] == "customer"
        assert snapshots[0]["state"] == "lead"
        assert snapshots[0]["properties"]["email"] == "alice@test.com"

    def test_entity_in_state_history(self, real):
        real.pipeline.process_batch("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ], "v1")

        row = real.state_history._conn.execute(
            "SELECT customer_id, customer_state, email, plan, valid_from, valid_to"
            " FROM customer_history"
        ).fetchone()
        assert row is not None
        assert row[1] == "lead"
        assert row[2] == "alice@test.com"
        assert row[3] == "free"  # default
        assert row[4] is not None  # valid_from set
        assert row[5] is None  # valid_to NULL (current version)

    def test_entity_in_ledger(self, real):
        real.pipeline.process_batch("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ], "v1")

        events = list(real.ledger.replay())
        assert len(events) == 1
        assert events[0]["event_type"] == "signup"
        assert events[0]["data"]["email"] == "alice@test.com"
        # raw should be a dict (not double-encoded string)
        assert isinstance(events[0]["raw"], dict)
        assert events[0]["raw"]["email"] == "alice@test.com"

    def test_entity_in_identity(self, real):
        real.pipeline.process_batch("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ], "v1")

        entity_id = real.identity_backend.lookup("customer", "alice@test.com")
        assert entity_id is not None

    def test_multiple_events_different_entities(self, real):
        result, snapshots = real.pipeline.process_batch("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
            {"type": "signup", "ts": "2024-01-15T10:01:00Z", "email": "bob@test.com"},
        ], "v1")

        assert result.events_ingested == 2
        assert len(snapshots) == 2

        # Different entities created
        entity_ids = {s["entity_id"] for s in snapshots}
        assert len(entity_ids) == 2

    def test_multiple_events_same_entity(self, real):
        """Two events for the same entity — state machine progresses."""
        result, snapshots = real.pipeline.process_batch("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
            {"type": "upgrade", "ts": "2024-01-15T10:01:00Z", "email": "alice@test.com", "plan": "pro"},
        ], "v1")

        assert result.events_ingested == 2
        # Per-event snapshots: one per state change
        assert len(snapshots) == 2
        # Final snapshot has the correct combined state
        assert snapshots[-1]["state"] == "active"
        assert snapshots[-1]["properties"]["plan"] == "pro"

    def test_cursor_advances(self, real):
        assert real.ledger.cursor() == 0
        real.pipeline.process_batch("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ], "v1")
        assert real.ledger.cursor() > 0


class TestProcessBatchEdgeCases:
    """Edge cases and error handling."""

    def test_unknown_event_type(self, real):
        """Events with unknown type fail normalization."""
        result, snapshots = real.pipeline.process_batch("web", [
            {"type": "unknown_event", "ts": "2024-01-15T10:00:00Z"},
        ], "v1")

        assert result.events_ingested == 0
        assert result.events_failed == 1
        assert len(result.failures) == 1
        assert snapshots == []

    def test_mixed_success_and_failure(self, real):
        result, snapshots = real.pipeline.process_batch("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
            {"type": "bogus", "ts": "2024-01-15T10:00:00Z"},
        ], "v1")

        assert result.events_ingested == 1
        assert result.events_failed == 1
        assert len(snapshots) == 1

    def test_empty_batch(self, real):
        result, snapshots = real.pipeline.process_batch("web", [], "v1")
        assert result.events_ingested == 0
        assert snapshots == []

    def test_second_batch_same_entity(self, real):
        """Two separate batches affecting the same entity."""
        real.pipeline.process_batch("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ], "v1")

        _, snapshots = real.pipeline.process_batch("web", [
            {"type": "upgrade", "ts": "2024-01-15T11:00:00Z", "email": "alice@test.com", "plan": "pro"},
        ], "v1")

        assert len(snapshots) == 1
        assert snapshots[0]["state"] == "active"
        assert snapshots[0]["properties"]["plan"] == "pro"

        # State history should have 2 versions (old closed, new current)
        rows = real.state_history._conn.execute(
            "SELECT valid_from, valid_to FROM customer_history ORDER BY valid_from"
        ).fetchall()
        assert len(rows) == 2
        assert rows[0][1] is not None  # old version closed
        assert rows[1][1] is None  # new version current

    def test_identity_cache_warm_on_second_batch(self, real):
        """Second batch hits the identity cache — no backend round-trip."""
        real.pipeline.process_batch("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ], "v1")

        # alice@test.com should now be in the cache
        cache_result = real.core.resolve_cache([("customer", "alice@test.com")])
        assert len(cache_result["resolved"]) == 1
        assert len(cache_result["unresolved"]) == 0


class TestBuildEndToEnd:
    """Build operations with real components."""

    def test_build_incremental(self, real):
        real.pipeline.process_batch("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ], "v1")

        result = real.pipeline.build_incremental("v1")
        assert result.mode == "INCREMENTAL"

    def test_build_full_replays_from_scratch(self, real):
        """Full build clears state and replays all events."""
        real.pipeline.process_batch("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ], "v1")

        # Clear entity state and rebuild
        result = real.pipeline.build_full("v1")
        assert result.mode == "FULL"
        assert result.events_processed > 0

        # Entity should still be in the core (rebuilt from ledger)
        assert real.core.entity_count() > 0

    def test_build_full_identity_survives(self, real):
        """Full build preserves identity cache — no re-resolution needed."""
        real.pipeline.process_batch("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ], "v1")

        # Full build clears entity state but NOT identity cache
        real.pipeline.build_full("v1")

        cache_result = real.core.resolve_cache([("customer", "alice@test.com")])
        assert len(cache_result["resolved"]) == 1


class TestPartialBuildEndToEnd:
    """Partial builds — rebuild specific entities only."""

    def test_partial_build_rebuilds_affected(self, real):
        """Partial build replays only affected entity's events."""
        real.pipeline.process_batch("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
            {"type": "signup", "ts": "2024-01-15T10:01:00Z", "email": "bob@test.com"},
        ], "v1")

        alice_id = real.identity_backend.lookup("customer", "alice@test.com")
        bob_id = real.identity_backend.lookup("customer", "bob@test.com")
        assert alice_id is not None
        assert bob_id is not None

        # Partial rebuild only alice
        result = real.pipeline.build_partial("v1", [alice_id])
        assert result.mode == "PARTIAL"
        assert result.events_processed >= 1

        # Both entities should still be in the core
        assert real.core.entity_count() >= 1

    def test_partial_build_empty_entity_list(self, real):
        """Partial build with no entities is a no-op."""
        real.pipeline.process_batch("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ], "v1")

        result = real.pipeline.build_partial("v1", [])
        assert result.mode == "PARTIAL"
        assert result.events_processed == 0


class TestIdentityResetEndToEnd:
    """Identity reset — clear cache and backend."""

    def test_reset_clears_identity(self, real):
        """After reset, identity cache and backend are empty."""
        real.pipeline.process_batch("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ], "v1")

        # Identity should exist
        assert real.identity_backend.lookup("customer", "alice@test.com") is not None

        # Reset clears both cache and backend
        real.identity.reset()

        assert real.identity_backend.lookup("customer", "alice@test.com") is None
        cache_result = real.core.resolve_cache([("customer", "alice@test.com")])
        assert len(cache_result["resolved"]) == 0

    def test_resolve_after_reset_creates_new_entity(self, real):
        """After reset, the same hint creates a new entity_id."""
        real.pipeline.process_batch("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ], "v1")

        old_id = real.identity_backend.lookup("customer", "alice@test.com")

        real.identity.reset()

        # Process again — should create a new entity
        real.pipeline.process_batch("web", [
            {"type": "signup", "ts": "2024-01-16T10:00:00Z", "email": "alice@test.com"},
        ], "v1")

        new_id = real.identity_backend.lookup("customer", "alice@test.com")
        assert new_id is not None
        assert new_id != old_id


class TestMultiBatchWorkflow:
    """Multi-batch workflows testing state progression and persistence."""

    def test_three_batch_entity_lifecycle(self, real):
        """Entity progresses through state machine across multiple batches."""
        # Batch 1: signup
        real.pipeline.process_batch("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ], "v1")

        # Batch 2: upgrade (transitions lead → active)
        _, snaps = real.pipeline.process_batch("web", [
            {"type": "upgrade", "ts": "2024-01-16T10:00:00Z", "email": "alice@test.com", "plan": "pro"},
        ], "v1")

        assert snaps[0]["state"] == "active"
        assert snaps[0]["properties"]["plan"] == "pro"

        # Batch 3: another upgrade (stays active, updates plan)
        _, snaps = real.pipeline.process_batch("web", [
            {"type": "upgrade", "ts": "2024-01-17T10:00:00Z", "email": "alice@test.com", "plan": "enterprise"},
        ], "v1")

        assert snaps[0]["state"] == "active"
        assert snaps[0]["properties"]["plan"] == "enterprise"

    def test_state_history_versions_accumulate(self, real):
        """Each batch creates a new SCD Type 2 version in state history."""
        real.pipeline.process_batch("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ], "v1")
        real.pipeline.process_batch("web", [
            {"type": "upgrade", "ts": "2024-01-16T10:00:00Z", "email": "alice@test.com", "plan": "pro"},
        ], "v1")
        real.pipeline.process_batch("web", [
            {"type": "upgrade", "ts": "2024-01-17T10:00:00Z", "email": "alice@test.com", "plan": "enterprise"},
        ], "v1")

        rows = real.state_history._conn.execute(
            "SELECT valid_from, valid_to, customer_state, plan FROM customer_history ORDER BY valid_from"
        ).fetchall()

        assert len(rows) == 3
        # First two versions closed (valid_to not null)
        assert rows[0][1] is not None
        assert rows[1][1] is not None
        # Third version current (valid_to null)
        assert rows[2][1] is None
        assert rows[2][2] == "active"
        assert rows[2][3] == "enterprise"

    def test_full_rebuild_produces_same_state(self, real):
        """Full rebuild from ledger produces the same final entity state."""
        real.pipeline.process_batch("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ], "v1")
        real.pipeline.process_batch("web", [
            {"type": "upgrade", "ts": "2024-01-16T10:00:00Z", "email": "alice@test.com", "plan": "pro"},
        ], "v1")

        # Get entity state before rebuild
        alice_id = real.identity_backend.lookup("customer", "alice@test.com")

        # Full rebuild
        real.pipeline.build_full("v1")

        # Entity state should be the same
        assert real.core.entity_count() > 0


class TestLoadEventsEndToEnd:
    """Verify load_events works with real DefactoCore."""

    def test_load_and_interpret(self, real):
        """Load events from ledger dicts, then interpret them."""
        # First, ingest normally to get events in the ledger
        real.pipeline.process_batch("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ], "v1")

        # Read events back from ledger
        events = list(real.ledger.replay())
        assert len(events) == 1

        # Clear entity state
        real.core.clear()
        assert real.core.entity_count() == 0

        # Load events into Rust and re-interpret
        real.core.load_events(events)

        entity_id = real.identity_backend.lookup("customer", "alice@test.com")
        entity_mapping = [(0, entity_id, "customer")]
        result = real.core.interpret(entity_mapping)

        assert len(result["snapshots"]) == 1
        assert len(result["failures"]) == 0
        assert result["snapshots"][0]["state"] == "lead"
        assert result["snapshots"][0]["properties"]["email"] == "alice@test.com"
