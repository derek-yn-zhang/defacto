"""Tests for the Defacto class — the top-level user-facing entry point.

Integration tests with real DefactoCore + real SQLite. Written BEFORE the
implementation (tests-first) to catch data format mismatches at boundaries.

Uses tmp_path for filesystem tests and real YAML definitions.
"""

import os
import sqlite3

import pytest

from defacto import Defacto, validate_definitions
from defacto.definitions import Definitions
from defacto.errors import DefinitionError
from defacto.results import BuildResult, BuildStatus, IngestResult, TickResult


# ---------------------------------------------------------------------------
# Shared definitions — same as test_integration.py, proven to work with Rust
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

CUSTOMER_YAML = """\
customer:
  starts: lead
  identity:
    email:
      match: exact
  properties:
    email:
      type: string
    plan:
      type: string
      default: free
  states:
    lead:
      when:
        signup:
          effects:
            - create
            - set:
                property: email
                from: event.email
        upgrade:
          effects:
            - transition:
                to: active
            - set:
                property: plan
                from: event.plan
    active:
      when:
        upgrade:
          effects:
            - set:
                property: plan
                from: event.plan
"""

SOURCE_YAML = """\
web:
  event_type: type
  timestamp: ts
  events:
    signup:
      raw_type: signup
      mappings:
        email:
          from: email
      hints:
        customer:
          - email
    upgrade:
      raw_type: upgrade
      mappings:
        email:
          from: email
        plan:
          from: plan
      hints:
        customer:
          - email
"""


@pytest.fixture
def project_dir(tmp_path):
    """Create a project directory with YAML definitions."""
    entities_dir = tmp_path / "entities"
    sources_dir = tmp_path / "sources"
    entities_dir.mkdir()
    sources_dir.mkdir()
    (entities_dir / "customer.yaml").write_text(CUSTOMER_YAML)
    (sources_dir / "web.yaml").write_text(SOURCE_YAML)
    return str(tmp_path)


# ===========================================================================
# from_directory
# ===========================================================================


class TestFromDirectory:
    """Create a Defacto instance from a project directory."""

    def test_creates_instance(self, project_dir):
        m = Defacto(project_dir)
        assert isinstance(m, Defacto)
        m.close()

    def test_creates_working_directory(self, project_dir):
        m = Defacto(project_dir)
        assert os.path.isdir(os.path.join(project_dir, ".defacto"))
        assert os.path.isfile(os.path.join(project_dir, ".defacto", "defacto.db"))
        m.close()

    def test_reopening_same_directory(self, project_dir):
        """Close and reopen — state persists across sessions."""
        m1 = Defacto(project_dir)
        m1.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ])
        m1.close()

        m2 = Defacto(project_dir)
        status = m2.build_status()
        assert status.total_events > 0
        m2.close()


# ===========================================================================
# from_config
# ===========================================================================


class TestFromConfig:
    """Create a Defacto instance from config dicts."""

    def test_with_dict(self, tmp_path):
        m = Defacto(DEFINITIONS, database=str(tmp_path / "test.db"))
        assert isinstance(m, Defacto)
        m.close()

    def test_with_definitions_object(self, tmp_path):
        defs = Definitions.from_dict(DEFINITIONS)
        m = Defacto(defs, database=str(tmp_path / "test.db"))
        assert isinstance(m, Defacto)
        m.close()

    def test_database_none_uses_temp(self):
        m = Defacto(DEFINITIONS)
        assert isinstance(m, Defacto)
        m.close()


# ===========================================================================
# ingest — batch append-only (process=None)
# ===========================================================================


class TestIngestAppendOnly:
    """Ingest without process= — normalize + append to ledger only."""

    def test_events_in_ledger(self, project_dir):
        m = Defacto(project_dir)
        result = m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ])
        assert isinstance(result, IngestResult)
        assert result.events_ingested == 1
        assert result.events_failed == 0

        # Events are in ledger
        status = m.build_status()
        assert status.total_events > 0
        m.close()

    def test_no_entity_state_without_process(self, project_dir):
        """Without process=, events are in ledger but no entity state built."""
        m = Defacto(project_dir)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ])
        assert m._core.entity_count() == 0
        m.close()

    def test_large_batch_chunked(self, project_dir):
        m = Defacto(project_dir, batch_size=10)
        events = [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": f"user{i}@test.com"}
            for i in range(25)
        ]
        result = m.ingest("web", events)
        assert result.events_ingested == 25
        m.close()

    def test_normalization_failure(self, project_dir):
        m = Defacto(project_dir)
        result = m.ingest("web", [
            {"type": "bogus_event", "ts": "2024-01-15T10:00:00Z"},
        ])
        assert result.events_failed == 1
        assert result.events_ingested == 0
        assert len(result.failures) == 1
        m.close()


# ===========================================================================
# ingest — stream-first (process=True)
# ===========================================================================


class TestIngestStreamFirst:
    """Ingest with process= — buffer + process at batch_size."""

    def test_single_event_buffered(self, project_dir):
        """One event with batch_size=100 — buffered, not processed yet."""
        m = Defacto(project_dir, batch_size=100)
        result = m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ], process=True)
        # Buffered, reported as ingested but not yet processed
        assert result.events_ingested == 1
        assert len(m._pending) == 1
        m.close()

    def test_buffer_flushes_at_batch_size(self):
        m = Defacto(DEFINITIONS, batch_size=3)
        # First two events: buffered
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "a@test.com"},
        ], process=True)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:01:00Z", "email": "b@test.com"},
        ], process=True)
        assert len(m._pending) == 2

        # Third event: triggers flush
        result = m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:02:00Z", "email": "c@test.com"},
        ], process=True)
        assert result.events_ingested == 3
        assert len(m._pending) == 0
        assert m._core.entity_count() == 3
        m.close()

    def test_large_batch_with_process(self):
        """Sending more than batch_size events with process=True."""
        m = Defacto(DEFINITIONS, batch_size=3)
        events = [
            {"type": "signup", "ts": f"2024-01-15T10:0{i}:00Z", "email": f"u{i}@test.com"}
            for i in range(7)
        ]
        result = m.ingest("web", events, process=True)
        # 7 events, batch_size=3: processes 2 full batches (6 events), 1 left in buffer
        assert result.events_ingested == 6
        assert len(m._pending) == 1
        m.close()


# ===========================================================================
# build
# ===========================================================================


class TestBuild:
    """Build entity state from the ledger."""

    def test_build_after_ingest(self, project_dir):
        """Ingest without process, then build."""
        m = Defacto(project_dir)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ])
        result = m.build()
        assert isinstance(result, BuildResult)
        assert result.events_processed >= 1
        assert m._core.entity_count() > 0
        m.close()

    def test_build_full(self, project_dir):
        m = Defacto(project_dir)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ])
        result = m.build(full=True)
        assert result.mode == "FULL"
        assert result.events_processed >= 1
        m.close()

    def test_build_skip_when_caught_up(self, project_dir):
        m = Defacto(project_dir)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ])
        m.build()  # processes events
        result = m.build()  # nothing new
        assert result.mode == "SKIP"
        m.close()

    def test_build_flushes_pending_first(self):
        """build() should flush any buffered events before building."""
        m = Defacto(DEFINITIONS, batch_size=100)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ], process=True)
        assert len(m._pending) == 1  # buffered

        m.build()
        assert len(m._pending) == 0  # flushed
        # Event was processed during pre-build flush — entity exists
        df = m.table("customer").execute()
        assert len(df) == 1
        m.close()

    def test_build_incremental(self, project_dir):
        """Second build after new events is INCREMENTAL."""
        m = Defacto(project_dir)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ])
        m.build()

        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T11:00:00Z", "email": "bob@test.com"},
        ])
        result = m.build()
        assert result.mode == "INCREMENTAL"
        assert result.events_processed >= 1
        assert m._core.entity_count() == 2
        m.close()

    def test_build_watermark_advances(self, project_dir):
        """Watermark tracks the max event timestamp across builds."""
        m = Defacto(project_dir)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ])
        m.build()

        wm1 = m._build_manager.get_watermark(m._active_version)
        assert wm1 != ""

        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T12:00:00Z", "email": "bob@test.com"},
        ])
        m.build()

        wm2 = m._build_manager.get_watermark(m._active_version)
        assert wm2 > wm1
        m.close()

    def test_build_full_with_identity_reset(self, tmp_path):
        """Changing identity config triggers FULL_WITH_IDENTITY_RESET.

        Identity is cleared and re-resolved from scratch. Entity IDs change
        because the identity backend was reset (new UUIDs assigned).
        """
        db_path = str(tmp_path / "defacto.db")

        # First session: ingest + build with original definitions
        m1 = Defacto(DEFINITIONS, database=db_path)
        m1.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ])
        m1.build()
        old_entity_id = m1._identity_backend.lookup("customer", "alice@test.com")
        assert old_entity_id is not None
        m1.close()

        # Second session: change identity config (add normalize expression)
        defs_v2 = {**DEFINITIONS}
        defs_v2["entities"] = {
            "customer": {
                **DEFINITIONS["entities"]["customer"],
                "identity": {
                    "email": {"match": "exact", "normalize": "lowercase(value)"},
                },
            },
        }

        m2 = Defacto(defs_v2, database=db_path)
        result = m2.build()

        assert result.mode == "FULL_WITH_IDENTITY_RESET"
        assert result.events_processed >= 1

        # Identity was reset — entity gets a new UUID
        new_entity_id = m2._identity_backend.lookup("customer", "alice@test.com")
        assert new_entity_id is not None
        assert new_entity_id != old_entity_id
        assert m2._core.entity_count() == 1
        m2.close()

    def test_build_full_renormalize(self, tmp_path):
        """Changing source config triggers FULL_RENORMALIZE.

        Ledger data is updated with re-normalized output, then a full
        rebuild produces correct entity state from the new normalization.
        """
        db_path = str(tmp_path / "defacto.db")

        # First session: ingest + build
        m1 = Defacto(DEFINITIONS, database=db_path)
        m1.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ])
        m1.build()
        assert m1._core.entity_count() == 1
        m1.close()

        # Second session: change source mapping (add a computed field)
        defs_v2 = {**DEFINITIONS}
        defs_v2["sources"] = {
            "web": {
                "event_type": "type",
                "timestamp": "ts",
                "events": {
                    "signup": {
                        "raw_type": "signup",
                        "mappings": {
                            "email": {"compute": "lowercase(event.email)"},
                        },
                        "hints": {"customer": ["email"]},
                    },
                    "upgrade": DEFINITIONS["sources"]["web"]["events"]["upgrade"],
                },
            },
        }

        m2 = Defacto(defs_v2, database=db_path)
        result = m2.build()

        assert result.mode == "FULL_RENORMALIZE"
        assert result.events_processed >= 1
        assert m2._core.entity_count() == 1
        m2.close()

    def test_renormalize_dedup(self, tmp_path):
        """Changing event_id_fields detects and filters duplicate events.

        Two upgrade events with different plans have different event_ids
        by default (event_type + all data fields hashed). When event_id_fields
        narrows to just ["email"], they become duplicates (same type + same
        email). The ledger keeps both rows but marks one as duplicate_of.
        """
        import copy
        db_path = str(tmp_path / "defacto.db")

        # Session 1: signup then two upgrades with different plans.
        # Default event_id = hash(event_type + all data), so different
        # plan values produce different IDs.
        m1 = Defacto(DEFINITIONS, database=db_path)
        m1.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
            {"type": "upgrade", "ts": "2024-01-15T10:01:00Z", "email": "alice@test.com", "plan": "pro"},
            {"type": "upgrade", "ts": "2024-01-15T10:02:00Z", "email": "alice@test.com", "plan": "enterprise"},
        ])
        m1.build()
        assert m1._ledger.cursor() == 3
        m1.close()

        # Session 2: change source to use event_id = ["email"] — now both
        # upgrades produce the same event_id (same type + same email).
        defs_v2 = copy.deepcopy(DEFINITIONS)
        defs_v2["sources"]["web"]["event_id"] = ["email"]

        m2 = Defacto(defs_v2, database=db_path)
        result = m2.build()
        assert result.mode == "FULL_RENORMALIZE"

        # Check that one upgrade is marked as duplicate
        rows = list(m2._ledger._conn.execute(
            "SELECT sequence, event_type, duplicate_of FROM ledger ORDER BY sequence"
        ))
        assert len(rows) == 3
        canonical = [r for r in rows if r[2] is None]
        duplicates = [r for r in rows if r[2] is not None]
        # signup is unique, one upgrade is canonical, other is duplicate
        assert len(canonical) == 2
        assert len(duplicates) == 1

        # Replay should skip the duplicate
        replayed = list(m2._ledger.replay(from_sequence=0))
        assert len(replayed) == 2  # signup + one upgrade
        m2.close()

    def test_renormalize_dedup_reversible(self, tmp_path):
        """Dedup is reversible — re-renormalizing with original fields restores events.

        After reverting event_id_fields and triggering a renormalize,
        previously duplicate events become distinct again. duplicate_of
        is cleared because renormalize is always a clean slate.
        """
        import copy
        db_path = str(tmp_path / "defacto.db")

        # Session 1: signup + two upgrades with different plans.
        # Default event_id = hash(type + all data) → all 3 are distinct.
        m1 = Defacto(DEFINITIONS, database=db_path)
        m1.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
            {"type": "upgrade", "ts": "2024-01-15T10:01:00Z", "email": "alice@test.com", "plan": "pro"},
            {"type": "upgrade", "ts": "2024-01-15T10:02:00Z", "email": "alice@test.com", "plan": "enterprise"},
        ])
        m1.build()
        assert m1._ledger.cursor() == 3
        m1.close()

        # Session 2: narrow event_id to just email → upgrades become duplicates
        defs_narrow = copy.deepcopy(DEFINITIONS)
        defs_narrow["sources"]["web"]["event_id"] = ["email"]
        m2 = Defacto(defs_narrow, database=db_path)
        m2.build()
        replayed_narrow = list(m2._ledger.replay(from_sequence=0))
        assert len(replayed_narrow) == 2  # signup + one upgrade (other is dup)
        m2.close()

        # Session 3: revert to default event_id and force renormalize.
        # from_raw=True forces FULL_RENORMALIZE regardless of hashes,
        # which clears duplicate_of and recomputes with original fields.
        m3 = Defacto(DEFINITIONS, database=db_path)
        m3.build(from_raw=True)
        replayed_reverted = list(m3._ledger.replay(from_sequence=0))
        assert len(replayed_reverted) == 3  # all events restored
        m3.close()

    def test_build_incremental_late_arrival(self, tmp_path):
        """Late arrivals trigger a partial rebuild of affected entities.

        Ingest events, build, then ingest an event with an older timestamp.
        The second build detects the late arrival and rebuilds affected entities.
        """
        db_path = str(tmp_path / "defacto.db")

        m = Defacto(DEFINITIONS, database=db_path)

        # First batch: alice signs up at t=10:00
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ])
        m.build()
        assert m._core.entity_count() == 1

        # Second batch: bob signs up at t=09:00 (older than watermark)
        # Different entity, different data → distinct event_id
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T09:00:00Z", "email": "bob@test.com"},
        ])
        result = m.build()

        assert result.mode == "INCREMENTAL"
        assert m._core.entity_count() == 2
        # bob is a late arrival (his timestamp is before the watermark)
        # but he's a new entity, so the partial rebuild creates him correctly
        assert result.late_arrivals >= 1
        m.close()

    def test_build_late_arrival_same_entity(self, tmp_path):
        """Late arrival for the SAME entity triggers partial rebuild.

        Alice signs up, upgrades to pro, then a late-arriving upgrade to
        enterprise (timestamped between signup and pro) arrives. The
        partial rebuild replays all events in chronological order, so
        alice ends up on pro (the latest upgrade wins).
        """
        db_path = str(tmp_path / "defacto.db")

        m = Defacto(DEFINITIONS, database=db_path)

        # First: signup alice at t=10:00, upgrade to pro at t=12:00
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
            {"type": "upgrade", "ts": "2024-01-15T12:00:00Z", "email": "alice@test.com", "plan": "pro"},
        ])
        m.build()

        # Late arrival: upgrade to enterprise at t=11:00 (between signup and pro)
        m.ingest("web", [
            {"type": "upgrade", "ts": "2024-01-15T11:00:00Z", "email": "alice@test.com", "plan": "enterprise"},
        ])
        result = m.build()

        assert result.mode == "INCREMENTAL"
        assert result.late_arrivals >= 1
        # Entity was rebuilt with all 3 events in timestamp order:
        # signup(10:00) → upgrade enterprise(11:00) → upgrade pro(12:00)
        assert m._core.entity_count() == 1
        m.close()

    def test_replay_timestamp_order(self, tmp_path):
        """Full rebuild processes events in chronological order."""
        db_path = str(tmp_path / "defacto.db")

        m = Defacto(DEFINITIONS, database=db_path)

        # Ingest out of chronological order (by sequence):
        # signup at t=11:00, then signup at t=10:00 (late)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T11:00:00Z", "email": "alice@test.com"},
        ])
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "bob@test.com"},
        ])

        # Full build replays in timestamp order (bob first, then alice)
        result = m.build(full=True)
        assert result.mode == "FULL"
        assert result.events_processed == 2
        assert m._core.entity_count() == 2
        m.close()

    def test_build_from_raw(self, tmp_path):
        """from_raw=True forces FULL_RENORMALIZE mode."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ])
        m.build()

        result = m.build(from_raw=True)
        assert result.mode == "FULL_RENORMALIZE"
        assert result.events_processed >= 1
        m.close()


# ===========================================================================
# tick
# ===========================================================================


class TestTick:
    """Time rule evaluation."""

    def test_tick_no_time_rules(self, project_dir):
        m = Defacto(project_dir)
        result = m.tick()
        assert isinstance(result, TickResult)
        assert result.effects_produced == 0
        m.close()

    def test_full_rebuild_ticks_overdue_entities(self, tmp_path):
        """After a full rebuild, entities with overdue time rules get ticked.

        An entity in 'active' state with a 1-day inactivity rule should
        transition to 'churned' during the post-rebuild tick if its last
        event was more than a day ago.
        """
        import copy
        defs_with_time_rules = copy.deepcopy(DEFINITIONS)
        # Add 'churned' state and inactivity rule on 'active'
        defs_with_time_rules["entities"]["customer"]["states"]["churned"] = {}
        defs_with_time_rules["entities"]["customer"]["states"]["active"]["after"] = [
            {"type": "inactivity", "threshold": "1d",
             "effects": [{"transition": {"to": "churned"}}]},
        ]

        db_path = str(tmp_path / "defacto.db")
        m = Defacto(defs_with_time_rules, database=db_path)

        # Ingest an event from 30 days ago — entity becomes active
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-01T10:00:00Z", "email": "alice@test.com"},
            {"type": "upgrade", "ts": "2024-01-01T10:01:00Z", "email": "alice@test.com", "plan": "pro"},
        ])
        m.build()

        # Force a full rebuild — the post-rebuild tick should fire the
        # inactivity rule since the last event was 30+ days ago.
        result = m.build(full=True)
        assert result.mode == "FULL"
        # The tick should have produced at least one effect (the transition)
        assert result.effects_produced >= 1
        m.close()

    def test_incremental_build_ticks_overdue_entities(self, tmp_path):
        """build_incremental also evaluates time rules after replay.

        Without this, time rules only fire on full builds or manual
        m.tick() calls — broken for the normal ingest→build loop.

        Setup: entity with inactivity rule on both active and churned
        states. First build churns alice (old events). Second build is
        incremental (bob's signup) — the post-incremental tick fires
        alice's churned→lead transition.
        """
        import copy
        defs = copy.deepcopy(DEFINITIONS)
        # Inactivity on active → churned, and on churned → lead (cycle)
        defs["entities"]["customer"]["states"]["churned"] = {
            "after": [
                {"type": "inactivity", "threshold": "1d",
                 "effects": [{"transition": {"to": "lead"}}]},
            ],
        }
        defs["entities"]["customer"]["states"]["active"]["after"] = [
            {"type": "inactivity", "threshold": "1d",
             "effects": [{"transition": {"to": "churned"}}]},
        ]

        db_path = str(tmp_path / "defacto.db")
        m = Defacto(defs, database=db_path)

        # Alice's events from 30 days ago — first build churns her
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-01T10:00:00Z", "email": "alice@test.com"},
            {"type": "upgrade", "ts": "2024-01-01T10:01:00Z",
             "email": "alice@test.com", "plan": "pro"},
        ])
        r1 = m.build()
        # First build ticks: alice active→churned (1 effect)
        assert r1.effects_produced >= 1

        # Incremental build with bob — alice is churned with old events,
        # churned→lead inactivity rule should fire during post-build tick
        m.ingest("web", [
            {"type": "signup", "ts": "2024-02-15T10:00:00Z", "email": "bob@test.com"},
        ])
        r2 = m.build()
        assert r2.mode == "INCREMENTAL"
        # Post-incremental tick fires alice's churned→lead rule
        assert r2.effects_produced >= 1
        m.close()


class TestTickNoDoubleEffects:
    """Tick sweep doesn't duplicate effects for entities that received events."""

    def test_no_duplicate_tick_after_pre_event_check(self, tmp_path):
        """Entity that transitioned via pre-event check is NOT re-ticked.

        Alice becomes active with 1-day inactivity rule. 30 days later,
        she receives an event. Pre-event check fires active→churned.
        The post-build tick sweep should NOT fire churned again (no
        time rule on churned state = no double transition).
        """
        import copy
        defs = copy.deepcopy(DEFINITIONS)
        defs["entities"]["customer"]["states"]["churned"] = {}
        defs["entities"]["customer"]["states"]["active"]["after"] = [
            {"type": "inactivity", "threshold": "1d",
             "effects": [{"transition": {"to": "churned"}}]},
        ]

        db_path = str(tmp_path / "defacto.db")
        m = Defacto(defs, database=db_path)

        # Alice active 30 days ago
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-01T10:00:00Z", "email": "alice@test.com"},
            {"type": "upgrade", "ts": "2024-01-01T10:01:00Z",
             "email": "alice@test.com", "plan": "pro"},
        ])
        m.build()  # first build ticks: active → churned

        # New event for alice 30 days later — pre-event check already
        # handled inactivity during the first build. Alice is churned.
        # Incremental build should not produce additional effects for alice
        # (churned has no time rules).
        m.ingest("web", [
            {"type": "signup", "ts": "2024-02-15T10:00:00Z", "email": "bob@test.com"},
        ])
        result = m.build()
        assert result.mode == "INCREMENTAL"

        # Check alice is still churned, not double-transitioned
        import sqlite3
        conn = sqlite3.connect(db_path)
        rows = conn.execute(
            "SELECT customer_state FROM customer_history WHERE valid_to IS NULL"
        ).fetchall()
        states = [r[0] for r in rows]
        conn.close()

        # churned count should be exactly 1 (alice), not 2
        assert states.count("churned") <= 1
        m.close()


class TestEraseTickInteraction:
    """Erased entity cannot be ticked on the same instance."""

    def test_tick_after_erase_ignores_erased(self, tmp_path):
        """After erase, m.tick() does not produce effects for the entity."""
        import copy
        defs = copy.deepcopy(DEFINITIONS)
        defs["entities"]["customer"]["states"]["churned"] = {}
        defs["entities"]["customer"]["states"]["active"]["after"] = [
            {"type": "inactivity", "threshold": "1d",
             "effects": [{"transition": {"to": "churned"}}]},
        ]

        db_path = str(tmp_path / "defacto.db")
        m = Defacto(defs, database=db_path)

        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-01T10:00:00Z", "email": "alice@test.com"},
            {"type": "upgrade", "ts": "2024-01-01T10:01:00Z",
             "email": "alice@test.com", "plan": "pro"},
        ])
        m.build()

        alice_id = m._identity_backend.lookup("customer", "alice@test.com")
        m.erase(alice_id)

        # Tick after erase — alice is not in DashMap, tick should produce nothing
        result = m.tick()
        assert result.effects_produced == 0
        m.close()


class TestMergeRebuildsWinner:
    """Merge replays all events under the winner — correct combined state."""

    def test_merge_replays_loser_events_under_winner(self, tmp_path):
        """After merge, winner has the combined state from both entities' events."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)

        # Alice upgrades to pro, bob stays on free
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-01T10:00:00Z", "email": "bob@test.com"},
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
            {"type": "upgrade", "ts": "2024-01-15T10:01:00Z",
             "email": "alice@test.com", "plan": "pro"},
        ])
        m.build()

        alice_id = m._identity_backend.lookup("customer", "alice@test.com")
        bob_id = m._identity_backend.lookup("customer", "bob@test.com")

        # Merge alice (active, plan=pro) into bob (lead, plan=free)
        m.merge(alice_id, bob_id)

        # Bob should now have the combined state: all events replayed
        # under bob's ID. Alice's upgrade event transitions bob to active
        # and sets plan=pro.
        df = m.table("customer").execute()
        assert len(df) == 1
        assert df["plan"].iloc[0] == "pro"
        m.close()


class TestMergeTimingFields:
    """Merge updates the winner's timing fields from the loser."""

    def test_merge_preserves_most_recent_event_time(self, tmp_path):
        """Winner gets the loser's last_event_time if it's more recent."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)

        # Bob's event is older, alice's is newer
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-01T10:00:00Z", "email": "bob@test.com"},
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ])
        m.build()

        alice_id = m._identity_backend.lookup("customer", "alice@test.com")
        bob_id = m._identity_backend.lookup("customer", "bob@test.com")

        # Merge alice (newer) into bob (older)
        m.merge(alice_id, bob_id)

        # Bob's state history should have alice's more recent timestamp
        import sqlite3
        conn = sqlite3.connect(db_path)
        row = conn.execute(
            "SELECT last_event_time FROM customer_history"
            " WHERE valid_to IS NULL"
        ).fetchone()
        conn.close()

        assert row is not None
        # Should reflect alice's more recent event time, not bob's older one
        assert "2024-01-15" in row[0]
        m.close()


# ===========================================================================
# erase
# ===========================================================================


class TestErase:
    """Right to erasure — cross-system delete."""

    def test_erase_entity(self, project_dir):
        m = Defacto(project_dir)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ])
        m.build()

        entity_id = m._identity_backend.lookup("customer", "alice@test.com")
        assert entity_id is not None

        m.erase(entity_id)

        # Identity mapping gone
        assert m._identity_backend.lookup("customer", "alice@test.com") is None
        # Entity gone from core
        assert m._core.entity_count() == 0
        m.close()

    def test_erase_survives_rebuild(self, project_dir):
        """Erased entity does not reappear after a FULL rebuild."""
        m = Defacto(project_dir)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
            {"type": "signup", "ts": "2024-01-15T10:01:00Z", "email": "bob@test.com"},
        ])
        m.build()
        assert m._core.entity_count() == 2

        alice_id = m._identity_backend.lookup("customer", "alice@test.com")
        m.erase(alice_id)
        assert m._core.entity_count() == 1

        # Force a full rebuild — erased entity should NOT come back
        m._core.clear()
        m._pipeline.build_full(m._active_version)
        assert m._core.entity_count() == 1  # only bob
        m.close()

    def test_erase_events_gone_from_ledger(self, project_dir):
        """Erased entity's events are deleted from the ledger."""
        import sqlite3
        m = Defacto(project_dir)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ])
        m.build()

        alice_id = m._identity_backend.lookup("customer", "alice@test.com")
        m.erase(alice_id)

        # Events for alice are gone from the ledger
        db_path = m._db_path
        conn = sqlite3.connect(db_path)
        count = conn.execute("SELECT COUNT(*) FROM ledger").fetchone()[0]
        conn.close()
        assert count == 0  # only event was alice's signup, now deleted
        m.close()

    def test_erase_new_events_create_fresh_entity(self, project_dir):
        """After erase, new events for the same person create a new entity.

        The erase deleted the old entity and its identity mappings.
        New events with the same hints resolve to a fresh entity_id —
        the old entity is gone, but new data is accepted.
        """
        m = Defacto(project_dir)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ])
        m.build()

        old_alice_id = m._identity_backend.lookup("customer", "alice@test.com")
        m.erase(old_alice_id)
        assert m._core.entity_count() == 0

        # New events for alice — creates a fresh entity
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T11:00:00Z", "email": "alice@test.com"},
        ])
        m.build()
        assert m._core.entity_count() == 1

        # Fresh entity has a different ID
        new_alice_id = m._identity_backend.lookup("customer", "alice@test.com")
        assert new_alice_id != old_alice_id
        m.close()

    def test_erase_merge_winner(self, project_dir):
        """Erase the winner of a merge — both entities disappear."""
        m = Defacto(project_dir)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
            {"type": "signup", "ts": "2024-01-15T10:01:00Z", "email": "bob@test.com"},
        ])
        m.build()

        alice_id = m._identity_backend.lookup("customer", "alice@test.com")
        bob_id = m._identity_backend.lookup("customer", "bob@test.com")
        m.merge(alice_id, bob_id)
        assert m._core.entity_count() == 1

        # Erase the winner (bob, who absorbed alice)
        m.erase(bob_id)
        assert m._core.entity_count() == 0

        # Rebuild — both should stay gone
        m._core.clear()
        m._pipeline.build_full(m._active_version)
        assert m._core.entity_count() == 0
        m.close()

    def test_erase_merge_loser(self, project_dir):
        """Erase the loser of a merge — already tombstoned, no error."""
        m = Defacto(project_dir)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
            {"type": "signup", "ts": "2024-01-15T10:01:00Z", "email": "bob@test.com"},
        ])
        m.build()

        alice_id = m._identity_backend.lookup("customer", "alice@test.com")
        bob_id = m._identity_backend.lookup("customer", "bob@test.com")
        m.merge(alice_id, bob_id)

        # Erase the loser (alice, already merged into bob) — should not error
        m.erase(alice_id)
        # Bob still exists
        assert m._core.entity_count() == 1
        m.close()

    def test_erase_then_merge(self, project_dir):
        """Erase entity A, then try to merge A into B — merge is a no-op."""
        m = Defacto(project_dir)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
            {"type": "signup", "ts": "2024-01-15T10:01:00Z", "email": "bob@test.com"},
        ])
        m.build()

        alice_id = m._identity_backend.lookup("customer", "alice@test.com")
        bob_id = m._identity_backend.lookup("customer", "bob@test.com")

        m.erase(alice_id)
        assert m._core.entity_count() == 1

        # Merge erased entity into bob — alice is already gone
        m.merge(alice_id, bob_id)
        assert m._core.entity_count() == 1
        m.close()

    def test_cold_start_with_erased_entity(self, tmp_path):
        """Erased entity doesn't appear in cold start recovery."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
            {"type": "signup", "ts": "2024-01-15T10:01:00Z", "email": "bob@test.com"},
        ])
        m.build()
        assert m._core.entity_count() == 2

        alice_id = m._identity_backend.lookup("customer", "alice@test.com")
        m.erase(alice_id)
        m.close()

        # Reopen — cold start loads from state history
        m2 = Defacto(DEFINITIONS, database=db_path)
        # Alice was erased — state history rows deleted — not loaded
        assert m2._core.entity_count() == 1
        m2.close()


# ===========================================================================
# close
# ===========================================================================


class TestClose:
    """Lifecycle management."""

    def test_close_flushes_pending(self):
        m = Defacto(DEFINITIONS, batch_size=100)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ], process=True)
        assert len(m._pending) == 1
        m.close()
        assert len(m._pending) == 0

    def test_close_idempotent(self):
        m = Defacto(DEFINITIONS)
        m.close()
        m.close()  # should not raise

    def test_context_manager(self, project_dir):
        with Defacto(project_dir) as m:
            m.ingest("web", [
                {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
            ])
        # m.close() called by __exit__


# ===========================================================================
# build_status
# ===========================================================================


class TestBuildStatus:
    """Build state inspection."""

    def test_initial_status(self, project_dir):
        m = Defacto(project_dir)
        status = m.build_status()
        assert isinstance(status, BuildStatus)
        assert status.cursor == 0
        assert status.total_events == 0
        m.close()

    def test_status_after_ingest(self, project_dir):
        m = Defacto(project_dir)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ])
        status = m.build_status()
        assert status.total_events > 0
        assert status.pending_events > 0
        m.close()

    def test_status_after_build(self, project_dir):
        m = Defacto(project_dir)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ])
        m.build()
        status = m.build_status()
        assert status.pending_events == 0
        m.close()


# ===========================================================================
# Sub-objects — m.ledger.*, m.identity.*
# ===========================================================================


class TestLedgerInspection:
    """m.ledger.* — read-only access to the event ledger."""

    def test_count_all(self, project_dir):
        """count() returns total events in ledger."""
        m = Defacto(project_dir)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "a@test.com"},
            {"type": "signup", "ts": "2024-01-15T10:01:00Z", "email": "b@test.com"},
        ])
        m.build()
        assert m.ledger.count() == 2
        m.close()

    def test_count_by_source(self, project_dir):
        """count(source=) filters by source name."""
        m = Defacto(project_dir)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "a@test.com"},
        ])
        m.build()
        assert m.ledger.count(source="web") == 1
        assert m.ledger.count(source="nonexistent") == 0
        m.close()

    def test_events_for(self, project_dir):
        """events_for() returns event summaries for an entity."""
        m = Defacto(project_dir)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "a@test.com"},
        ])
        m.build()
        entity_id = m._identity_backend.lookup("customer", "a@test.com")
        events = m.ledger.events_for(entity_id)
        assert len(events) == 1
        assert events[0]["event_type"] == "signup"
        assert "sequence" in events[0]
        assert "event_id" in events[0]
        assert "timestamp" in events[0]
        m.close()


class TestIdentityInspection:
    """m.identity.* — read-only access to identity mappings."""

    def test_lookup(self, project_dir):
        """lookup() finds entity by hint value."""
        m = Defacto(project_dir)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ])
        m.build()
        entity_id = m.identity.lookup("alice@test.com")
        assert entity_id is not None
        assert m.identity.lookup("unknown@test.com") is None
        m.close()

    def test_hints(self, project_dir):
        """hints() returns all hints linked to an entity."""
        m = Defacto(project_dir)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ])
        m.build()
        entity_id = m.identity.lookup("alice@test.com")
        hints = m.identity.hints(entity_id)
        assert "email" in hints
        assert hints["email"] == ["alice@test.com"]
        m.close()

    def test_hints_multiple_fields(self, tmp_path):
        """hints() preserves all hint fields when an entity has multiple."""
        import copy
        defs = copy.deepcopy(DEFINITIONS)
        # Add phone as a second identity field
        defs["entities"]["customer"]["identity"]["phone"] = {"match": "exact"}
        # Add phone to source mappings and hints
        defs["sources"]["web"]["events"]["signup"]["mappings"]["phone"] = {"from": "phone"}
        defs["sources"]["web"]["events"]["signup"]["hints"]["customer"].append("phone")

        db_path = str(tmp_path / "defacto.db")
        m = Defacto(defs, database=db_path)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z",
             "email": "alice@test.com", "phone": "555-1234"},
        ])
        m.build()

        entity_id = m.identity.lookup("alice@test.com")
        hints = m.identity.hints(entity_id)
        assert hints["email"] == ["alice@test.com"]
        assert hints["phone"] == ["555-1234"]
        m.close()


# ===========================================================================
# End-to-end workflow
# ===========================================================================


class TestContentHashVersioning:
    """Content-hash versioning: same definitions produce same version name.

    Substantiates claims F8.1 (deterministic content hash) and M8.1
    (identical definitions skip re-registration).
    """

    def test_same_definitions_produce_same_hash(self, tmp_path):
        """Two from_config calls with identical definitions get the same version."""
        db_path = str(tmp_path / "defacto.db")
        m1 = Defacto(DEFINITIONS, database=db_path)
        v1 = m1._active_version
        m1.close()

        m2 = Defacto(DEFINITIONS, database=db_path)
        v2 = m2._active_version
        m2.close()

        assert v1 == v2
        assert len(v1) == 8  # 8-char hex

    def test_different_definitions_produce_different_hash(self, tmp_path):
        """Changing definitions produces a different version hash."""
        import copy
        db_path = str(tmp_path / "defacto.db")

        m1 = Defacto(DEFINITIONS, database=db_path)
        v1 = m1._active_version
        m1.close()

        modified = copy.deepcopy(DEFINITIONS)
        modified["entities"]["customer"]["properties"]["name"] = {"type": "string"}

        m2 = Defacto(modified, database=db_path)
        v2 = m2._active_version
        m2.close()

        assert v1 != v2

    def test_reopen_skips_registration(self, tmp_path):
        """Re-opening with the same definitions doesn't create a duplicate version."""
        db_path = str(tmp_path / "defacto.db")

        m1 = Defacto(DEFINITIONS, database=db_path)
        v1 = m1._active_version
        m1.close()

        # Re-open — same definitions, same database
        m2 = Defacto(DEFINITIONS, database=db_path)

        # Only one version should exist
        versions = m2.definitions.versions()
        version_names = [v["version"] for v in versions]
        assert version_names.count(v1) == 1
        assert len(versions) == 1
        m2.close()

    def test_from_directory_deterministic(self, project_dir):
        """from_directory produces the same version for unchanged YAML files."""
        m1 = Defacto(project_dir)
        v1 = m1._active_version
        m1.close()

        m2 = Defacto(project_dir)
        v2 = m2._active_version
        m2.close()

        assert v1 == v2


class TestBatchSorting:
    """Batch sorting prevents false late arrivals.

    process_batch sorts events by timestamp so that the watermark (based
    on the last sorted element) accurately reflects the max timestamp.
    Substantiates the batch sorting mechanism.
    """

    def test_watermark_is_max_timestamp_regardless_of_input_order(self, tmp_path):
        """Events ingested out of order still produce a watermark at the max timestamp."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)

        # Ingest events deliberately out of timestamp order
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T15:00:00Z", "email": "carol@test.com"},
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
            {"type": "signup", "ts": "2024-01-15T12:00:00Z", "email": "bob@test.com"},
        ])
        m.build()

        wm = m._build_manager.get_watermark(m._active_version)
        # Watermark must be the max timestamp (carol's), not the last input (bob's)
        assert "15:00:00" in wm
        m.close()

    def test_no_false_late_arrival_from_unordered_batch(self, tmp_path):
        """A second batch with timestamp between min and max of the first batch
        should be detected as a late arrival (timestamp < watermark).
        Without sorting, watermark could be wrong and miss this."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path, batch_size=10)

        # Batch 1: events at t=10:00 and t=15:00 (out of order)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T15:00:00Z", "email": "carol@test.com"},
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ])
        m.build()

        # Batch 2: event at t=12:00 — between the two, but after the watermark
        # should be correctly identified as a late arrival (12:00 < 15:00 watermark)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T12:00:00Z", "email": "bob@test.com"},
        ])
        result = m.build()
        assert result.mode == "INCREMENTAL"
        assert result.late_arrivals >= 1
        m.close()


class TestPreEventTimeRuleCheck:
    """Pre-event time rule evaluation during interpretation.

    Before interpreting any event for an entity, the interpreter checks
    if time rules have fired since the entity's last event. This ensures
    correct state regardless of tick frequency.

    Substantiates claim S3.3 (correct state regardless of tick frequency).
    """

    def test_time_rule_fires_before_event_is_interpreted(self, tmp_path):
        """An inactivity rule fires before a late event is processed.

        Setup: entity enters 'active' state with 1-day inactivity rule.
        Then a second event arrives 2 days later. The interpreter should:
        1. Check time rules → inactivity threshold crossed → transition to 'inactive'
        2. Process the event in 'inactive' state (no handler → no-op)

        If the pre-event check didn't work, the event would process in
        'active' state and set plan='pro'.
        """
        import copy
        defs = copy.deepcopy(DEFINITIONS)

        # Add 'inactive' state with NO upgrade handler — upgrade is a no-op here
        defs["entities"]["customer"]["states"]["inactive"] = {
            "when": {
                "reactivate": {
                    "effects": [{"transition": {"to": "active"}}],
                },
            },
        }
        # Add inactivity rule on 'active' → transitions to 'inactive' after 1 day
        defs["entities"]["customer"]["states"]["active"]["after"] = [
            {"type": "inactivity", "threshold": "1d",
             "effects": [{"transition": {"to": "inactive"}}]},
        ]
        # Add a handler on lead for signup that transitions to active
        defs["entities"]["customer"]["states"]["lead"]["when"]["signup"]["effects"] = [
            "create",
            {"set": {"property": "email", "from": "event.email"}},
            {"transition": {"to": "active"}},
        ]

        db_path = str(tmp_path / "defacto.db")
        m = Defacto(defs, database=db_path, batch_size=1)

        # Event 1: signup creates entity in 'active' state (batch_size=1 → immediate)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-01T10:00:00Z", "email": "alice@test.com"},
        ], process=True)

        # Event 2: upgrade arrives 2 days later (past 1d inactivity threshold)
        # Processed in a separate batch (batch_size=1).
        # If pre-event check works: active→inactive (time rule), then upgrade
        # hits inactive state → no handler → no-op → plan stays 'free'
        # If pre-event check fails: upgrade processes in active state → plan='pro'
        m.ingest("web", [
            {"type": "upgrade", "ts": "2024-01-03T10:00:00Z",
             "email": "alice@test.com", "plan": "pro"},
        ], process=True)

        # Check: entity should be in 'inactive' state (time rule fired)
        # and plan should be 'free' (upgrade was no-op in inactive state)
        df = m.table("customer").execute()
        assert df["customer_state"].iloc[0] == "inactive"
        assert df["plan"].iloc[0] == "free"
        m.close()


class TestCrashRecovery:
    """Cold start recovery from state history.

    Entity state lives in DashMap (in-memory) and is lost on restart.
    On cold start, entity state is restored from state history — cost is
    proportional to entities (fast) not events (slow). Works for both
    clean restarts and crash recovery (dirty flag).

    Substantiates claim S5.3 (crash recovery produces correct state).
    """

    def test_dirty_flag_triggers_full_rebuild(self, tmp_path):
        """Simulated crash: set dirty flag, reopen, verify recovery.

        Cold start loads entity state from state history, clears the
        dirty flag, then runs INCREMENTAL if there's a cursor gap.
        The subsequent explicit build() is INCREMENTAL (only new events).
        """
        db_path = str(tmp_path / "defacto.db")

        # Normal operation: ingest and build
        m = Defacto(DEFINITIONS, database=db_path)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ])
        result = m.build()
        assert result.events_processed == 1

        # Simulate crash: set dirty flag directly in SQLite
        version_key = m._active_version
        m._build_manager._store.set_dirty(version_key)
        m.close()

        # Reopen — cold start detection rebuilds during initialization.
        # Alice should be restored in DashMap before we even call build().
        m2 = Defacto(DEFINITIONS, database=db_path)
        assert m2._core.entity_count() == 1  # alice restored at init

        # New events build incrementally (cold start already handled the rebuild)
        m2.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T11:00:00Z", "email": "bob@test.com"},
        ])
        result = m2.build()
        assert result.mode == "INCREMENTAL"
        assert m2._core.entity_count() == 2
        m2.close()

    def test_clean_restart_restores_entity_state(self, tmp_path):
        """After clean restart, entity state is loaded from state history.

        Entity state lives in DashMap (in-memory) and is lost on restart.
        Cold start recovery loads from state history (proportional to
        entities) instead of replaying the full ledger (proportional to
        events).
        """
        db_path = str(tmp_path / "defacto.db")

        m = Defacto(DEFINITIONS, database=db_path)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ])
        m.build()
        assert m._core.entity_count() == 1
        m.close()

        # Reopen — cold start detection should rebuild alice from ledger
        m2 = Defacto(DEFINITIONS, database=db_path)
        assert m2._core.entity_count() == 1  # alice restored!

        # New events for alice should interpret correctly (not as fresh entity)
        m2.ingest("web", [
            {"type": "upgrade", "ts": "2024-01-15T11:00:00Z",
             "email": "alice@test.com", "plan": "pro"},
        ])
        result = m2.build()
        assert result.mode == "INCREMENTAL"

        # Alice should have plan=pro (upgrade applied to existing entity)
        df = m2.table("customer").execute()
        assert df["plan"].iloc[0] == "pro"
        m2.close()

    def test_first_boot_no_unnecessary_rebuild(self, tmp_path):
        """First boot (no events ever) should NOT trigger cold start rebuild."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)

        # No events, no builds — entity_count is 0, cursor is 0
        assert m._core.entity_count() == 0
        # Should not have done any build (SKIP, not FULL)
        m.close()

    def test_timing_fields_in_state_history(self, tmp_path):
        """Timing fields (last_event_time, state_entered_time, created_time)
        flow through snapshots into state history tables."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ])
        m.build()

        # Read directly from SQLite — timing columns should exist and be populated
        import sqlite3
        conn = sqlite3.connect(db_path)
        row = conn.execute(
            "SELECT last_event_time, state_entered_time, created_time"
            " FROM customer_history WHERE valid_to IS NULL"
        ).fetchone()
        conn.close()

        assert row is not None
        assert row[0] is not None  # last_event_time
        assert row[1] is not None  # state_entered_time
        assert row[2] is not None  # created_time
        # All should contain the event timestamp
        assert "2024-01-15" in row[0]
        assert "2024-01-15" in row[1]
        assert "2024-01-15" in row[2]
        m.close()

    def test_cold_start_preserves_time_rules(self, tmp_path):
        """Cold start recovery restores timing fields so time rules work.

        After restart, entities loaded from state history should have
        correct timing metadata. A tick() call should evaluate time rules
        against the restored timestamps.
        """
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ])
        m.build()
        m.close()

        # Reopen — cold start restores from state history
        m2 = Defacto(DEFINITIONS, database=db_path)
        assert m2._core.entity_count() == 1

        # Tick should not crash — timing fields restored correctly
        tick_result = m2.tick()
        assert tick_result is not None
        m2.close()

    def test_cold_start_incremental_gap(self, tmp_path):
        """Cold start with events added while process was down.

        Simulates: process A builds, shuts down. External process adds
        events to ledger. Process A restarts — should load from state
        history then INCREMENTAL to process the gap.
        """
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ])
        m.build()

        # Add events directly to the ledger (simulating external ingest)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T11:00:00Z", "email": "bob@test.com"},
        ])
        # Don't build — leave events unprocessed in the ledger
        m.close()

        # Reopen — should load alice from state history, then INCREMENTAL
        # picks up bob from the ledger gap
        m2 = Defacto(DEFINITIONS, database=db_path)
        assert m2._core.entity_count() == 2  # alice (state history) + bob (incremental)
        m2.close()


class TestMergeAPI:
    """External merge events via m.merge()."""

    def test_merge_tombstones_loser(self, tmp_path):
        """Loser entity is tombstoned, winner survives."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
            {"type": "signup", "ts": "2024-01-15T10:01:00Z", "email": "bob@test.com"},
        ])
        m.build()
        assert m._core.entity_count() == 2

        # Get entity IDs
        alice_id = m._identity_backend.lookup("customer", "alice@test.com")
        bob_id = m._identity_backend.lookup("customer", "bob@test.com")

        # Merge alice into bob
        m.merge(alice_id, bob_id)

        # Alice is gone, bob survives
        assert m._core.entity_count() == 1
        m.close()

    def test_merge_survives_rebuild(self, tmp_path):
        """Merge persists through a FULL rebuild."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
            {"type": "signup", "ts": "2024-01-15T10:01:00Z", "email": "bob@test.com"},
        ])
        m.build()

        alice_id = m._identity_backend.lookup("customer", "alice@test.com")
        bob_id = m._identity_backend.lookup("customer", "bob@test.com")
        m.merge(alice_id, bob_id)
        m.close()

        # Reopen — cold start recovery, then rebuild
        m2 = Defacto(DEFINITIONS, database=db_path)
        # Merge should have survived — only 1 entity
        assert m2._core.entity_count() == 1
        m2.close()

    def test_merge_in_ledger(self, tmp_path):
        """Merge event is recorded in the ledger."""
        import sqlite3
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
            {"type": "signup", "ts": "2024-01-15T10:01:00Z", "email": "bob@test.com"},
        ])
        m.build()

        alice_id = m._identity_backend.lookup("customer", "alice@test.com")
        bob_id = m._identity_backend.lookup("customer", "bob@test.com")
        m.merge(alice_id, bob_id, reason="manual_review")

        # Check the ledger directly
        conn = sqlite3.connect(db_path)
        row = conn.execute(
            "SELECT event_type, data FROM ledger WHERE event_type = '_defacto_merge'"
        ).fetchone()
        conn.close()

        assert row is not None
        assert row[0] == "_defacto_merge"
        import json
        data = json.loads(row[1])
        assert data["from_entity_id"] == alice_id
        assert data["into_entity_id"] == bob_id
        assert data["reason"] == "manual_review"
        m.close()

    def test_merge_idempotent(self, tmp_path):
        """Merging already-merged entities is a no-op."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
            {"type": "signup", "ts": "2024-01-15T10:01:00Z", "email": "bob@test.com"},
        ])
        m.build()

        alice_id = m._identity_backend.lookup("customer", "alice@test.com")
        bob_id = m._identity_backend.lookup("customer", "bob@test.com")

        m.merge(alice_id, bob_id)
        # Second merge — should not error
        m.merge(alice_id, bob_id)
        assert m._core.entity_count() == 1
        m.close()

    def test_merge_new_events_resolve_to_winner(self, tmp_path):
        """After merge, new events for the loser resolve to the winner."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
            {"type": "signup", "ts": "2024-01-15T10:01:00Z", "email": "bob@test.com"},
        ])
        m.build()

        alice_id = m._identity_backend.lookup("customer", "alice@test.com")
        bob_id = m._identity_backend.lookup("customer", "bob@test.com")
        m.merge(alice_id, bob_id)

        # New event for alice should resolve to bob
        m.ingest("web", [
            {"type": "upgrade", "ts": "2024-01-15T11:00:00Z",
             "email": "alice@test.com", "plan": "pro"},
        ])
        m.build()

        # Still 1 entity, with the upgrade applied
        assert m._core.entity_count() == 1
        m.close()

    def test_merge_new_events_produce_correct_state(self, tmp_path):
        """After merge, new events produce the correct combined state."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-01T10:00:00Z", "email": "bob@test.com"},
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
            {"type": "upgrade", "ts": "2024-01-15T10:01:00Z",
             "email": "alice@test.com", "plan": "pro"},
        ])
        m.build()

        alice_id = m._identity_backend.lookup("customer", "alice@test.com")
        bob_id = m._identity_backend.lookup("customer", "bob@test.com")
        m.merge(alice_id, bob_id)

        # New upgrade — should apply to the winner (bob) who is now active
        m.ingest("web", [
            {"type": "upgrade", "ts": "2024-02-01T10:00:00Z",
             "email": "alice@test.com", "plan": "enterprise"},
        ])
        m.build()

        df = m.table("customer").execute()
        assert len(df) == 1
        assert df["plan"].iloc[0] == "enterprise"
        assert df["customer_state"].iloc[0] == "active"
        m.close()

    def test_merge_full_rebuild_produces_correct_state(self, tmp_path):
        """Full rebuild after merge produces the same combined state."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-01T10:00:00Z", "email": "bob@test.com"},
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
            {"type": "upgrade", "ts": "2024-01-15T10:01:00Z",
             "email": "alice@test.com", "plan": "pro"},
        ])
        m.build()

        alice_id = m._identity_backend.lookup("customer", "alice@test.com")
        bob_id = m._identity_backend.lookup("customer", "bob@test.com")
        m.merge(alice_id, bob_id)

        # Add a post-merge event
        m.ingest("web", [
            {"type": "upgrade", "ts": "2024-02-01T10:00:00Z",
             "email": "alice@test.com", "plan": "enterprise"},
        ])
        m.build()

        # Full rebuild — should produce identical state
        m._core.clear()
        m._pipeline.build_full(m._active_version)

        df = m.table("customer").execute()
        assert len(df) == 1
        assert df["plan"].iloc[0] == "enterprise"
        assert df["customer_state"].iloc[0] == "active"
        m.close()


class TestLifecycleProtocol:
    """Verify the unified lifecycle protocol: one merge path, one tick path,
    one rebuild path, erase reads from merge_log not state_history."""

    def test_merge_produces_correct_combined_state(self, tmp_path):
        """m.merge() produces correct combined state via _rebuild_entities."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-01T10:00:00Z", "email": "bob@test.com"},
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
            {"type": "upgrade", "ts": "2024-01-15T10:01:00Z",
             "email": "alice@test.com", "plan": "pro"},
        ])
        m.build()

        alice_id = m._identity_backend.lookup("customer", "alice@test.com")
        bob_id = m._identity_backend.lookup("customer", "bob@test.com")

        # Alice is active/pro, Bob is lead/free. Merge alice INTO bob.
        # Winner should have combined state: active/pro.
        m.merge(alice_id, bob_id)

        df = m.table("customer").execute()
        assert len(df) == 1
        assert df["customer_state"].iloc[0] == "active"
        assert df["plan"].iloc[0] == "pro"
        m.close()

    def test_merge_state_history_is_clean(self, tmp_path):
        """After merge, winner's state history has no stale rows."""
        import sqlite3
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-01T10:00:00Z", "email": "bob@test.com"},
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
            {"type": "upgrade", "ts": "2024-01-15T10:01:00Z",
             "email": "alice@test.com", "plan": "pro"},
        ])
        m.build()

        alice_id = m._identity_backend.lookup("customer", "alice@test.com")
        bob_id = m._identity_backend.lookup("customer", "bob@test.com")
        m.merge(alice_id, bob_id)

        conn = sqlite3.connect(db_path)
        # Winner should have exactly 1 current row (valid_to IS NULL)
        rows = conn.execute(
            "SELECT * FROM customer_history WHERE customer_id = ? AND valid_to IS NULL",
            (bob_id,),
        ).fetchall()
        assert len(rows) == 1

        # Loser should have 0 current rows (tombstoned)
        loser_rows = conn.execute(
            "SELECT * FROM customer_history WHERE customer_id = ? AND valid_to IS NULL",
            (alice_id,),
        ).fetchall()
        assert len(loser_rows) == 0
        conn.close()
        m.close()

    def test_build_full_merge_post_pass(self, tmp_path):
        """Full rebuild with merge event produces correct state via post-pass."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-01T10:00:00Z", "email": "bob@test.com"},
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
            {"type": "upgrade", "ts": "2024-01-15T10:01:00Z",
             "email": "alice@test.com", "plan": "pro"},
        ])
        m.build()

        alice_id = m._identity_backend.lookup("customer", "alice@test.com")
        bob_id = m._identity_backend.lookup("customer", "bob@test.com")
        m.merge(alice_id, bob_id)

        m.ingest("web", [
            {"type": "upgrade", "ts": "2024-02-01T10:00:00Z",
             "email": "alice@test.com", "plan": "enterprise"},
        ])
        m.build()

        # Full rebuild — merge post-pass should produce correct state
        m._core.clear()
        m._pipeline.build_full(m._active_version)

        df = m.table("customer").execute()
        assert len(df) == 1
        assert df["plan"].iloc[0] == "enterprise"
        assert df["customer_state"].iloc[0] == "active"
        m.close()

    def test_erase_cascade_via_merge_log(self, tmp_path):
        """Erase cascades through merge_log, not state_history."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
            {"type": "signup", "ts": "2024-01-15T10:01:00Z", "email": "bob@test.com"},
        ])
        m.build()

        alice_id = m._identity_backend.lookup("customer", "alice@test.com")
        bob_id = m._identity_backend.lookup("customer", "bob@test.com")
        m.merge(alice_id, bob_id)

        # Verify merge_log has the entry
        merges = m._ledger.find_merges_into(bob_id)
        assert alice_id in merges

        # Erase the winner — should cascade to the loser
        m.erase(bob_id)
        assert m._core.entity_count() == 0

        # merge_log should be cleaned up
        merges_after = m._ledger.find_merges_into(bob_id)
        assert len(merges_after) == 0
        m.close()

    def test_merge_then_incremental_build(self, tmp_path):
        """Incremental build after merge handles new events correctly."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-01T10:00:00Z", "email": "bob@test.com"},
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
            {"type": "upgrade", "ts": "2024-01-15T10:01:00Z",
             "email": "alice@test.com", "plan": "pro"},
        ])
        m.build()

        alice_id = m._identity_backend.lookup("customer", "alice@test.com")
        bob_id = m._identity_backend.lookup("customer", "bob@test.com")
        m.merge(alice_id, bob_id)

        # New events for both hints should resolve to winner
        m.ingest("web", [
            {"type": "upgrade", "ts": "2024-02-01T10:00:00Z",
             "email": "alice@test.com", "plan": "enterprise"},
            {"type": "upgrade", "ts": "2024-03-01T10:00:00Z",
             "email": "bob@test.com", "plan": "ultimate"},
        ])
        m.build()

        df = m.table("customer").execute()
        assert len(df) == 1
        assert df["plan"].iloc[0] == "ultimate"
        m.close()

    def test_merge_across_shards_winner_correct(self, tmp_path):
        """Sharded merge: winner shard has correct combined state."""
        db_path = str(tmp_path / "shared.db")

        # Unsharded: ingest, build, merge
        m = Defacto(DEFINITIONS, database=db_path)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-01T10:00:00Z", "email": "bob@test.com"},
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
            {"type": "upgrade", "ts": "2024-01-15T10:01:00Z",
             "email": "alice@test.com", "plan": "pro"},
        ])
        m.build()
        alice_id = m._identity_backend.lookup("customer", "alice@test.com")
        bob_id = m._identity_backend.lookup("customer", "bob@test.com")
        m.merge(alice_id, bob_id)
        m.close()

        # Both shards should have 1 entity total (winner on one shard)
        total = 0
        for shard in range(2):
            s = Defacto(
                DEFINITIONS, database=db_path,
                shard_id=shard, total_shards=2,
            )
            s.build()
            total += s._core.entity_count()
            s.close()
        assert total == 1


class TestIdentityResetMergeSurvival:
    """Verify external merges survive IDENTITY_RESET builds."""

    def test_merge_survives_identity_reset(self, tmp_path):
        """External merge is re-executed with new entity_ids after reset."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-01T10:00:00Z", "email": "bob@test.com"},
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
            {"type": "upgrade", "ts": "2024-01-15T10:01:00Z",
             "email": "alice@test.com", "plan": "pro"},
        ])
        m.build()

        alice_id = m._identity_backend.lookup("customer", "alice@test.com")
        bob_id = m._identity_backend.lookup("customer", "bob@test.com")
        m.merge(alice_id, bob_id)

        # IDENTITY_RESET — new entity_ids, merge event has old IDs.
        # Stored hints should allow re-resolution.
        m.build(from_raw=True)

        # After reset: both hints should resolve to the same (new) entity
        new_alice = m._identity_backend.lookup("customer", "alice@test.com")
        new_bob = m._identity_backend.lookup("customer", "bob@test.com")
        assert new_alice == new_bob, "Hints should resolve to same entity after reset"

        # Should have correct combined state
        assert m._core.entity_count() == 1
        df = m.table("customer").execute()
        assert len(df) == 1
        assert df["customer_state"].iloc[0] == "active"
        assert df["plan"].iloc[0] == "pro"
        m.close()

    def test_identity_reset_tombstones_old_ids(self, tmp_path):
        """IDENTITY_RESET produces tombstones for old entity_ids."""
        import sqlite3
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ])
        m.build()

        old_id = m._identity_backend.lookup("customer", "alice@test.com")

        # IDENTITY_RESET
        m.build(from_raw=True)

        new_id = m._identity_backend.lookup("customer", "alice@test.com")
        assert old_id != new_id, "Entity should get a new ID after reset"

        # Old ID should have a tombstone (valid_to set)
        conn = sqlite3.connect(db_path)
        old_rows = conn.execute(
            "SELECT valid_to FROM customer_history WHERE customer_id = ?",
            (old_id,),
        ).fetchall()
        assert all(row[0] is not None for row in old_rows), (
            "Old entity_id should be tombstoned after IDENTITY_RESET"
        )

        # New ID should have a current row
        new_rows = conn.execute(
            "SELECT valid_to FROM customer_history WHERE customer_id = ? AND valid_to IS NULL",
            (new_id,),
        ).fetchall()
        assert len(new_rows) == 1
        conn.close()
        m.close()


class TestStoreConsistency:
    """Verify every store is in the correct state after lifecycle operations.

    Checks all 7 stores: DashMap, identity cache, identity table,
    event_entities, merge_log, state history, build_state.
    """

    def _store_snapshot(self, m, db_path):
        """Capture the state of all stores for verification."""
        import sqlite3
        conn = sqlite3.connect(db_path)
        return {
            "entity_count": m._core.entity_count(),
            "identity": conn.execute(
                "SELECT entity_type, hint_value, entity_id FROM identity"
            ).fetchall(),
            "event_entities": conn.execute(
                "SELECT sequence, entity_id, entity_type FROM event_entities"
                " ORDER BY sequence"
            ).fetchall(),
            "merge_log": conn.execute(
                "SELECT from_entity_id, into_entity_id FROM merge_log"
            ).fetchall(),
            "state_history_current": conn.execute(
                "SELECT customer_id, customer_state, plan, email"
                " FROM customer_history WHERE valid_to IS NULL"
                " ORDER BY customer_id"
            ).fetchall(),
            "state_history_tombstones": conn.execute(
                "SELECT customer_id, merged_into"
                " FROM customer_history WHERE merged_into IS NOT NULL"
            ).fetchall(),
            "conn": conn,
        }

    def test_merge_all_stores_consistent(self, tmp_path):
        """After merge, every store reflects the merge correctly."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-01T10:00:00Z", "email": "bob@test.com"},
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
            {"type": "upgrade", "ts": "2024-01-15T10:01:00Z",
             "email": "alice@test.com", "plan": "pro"},
        ])
        m.build()

        alice_id = m._identity_backend.lookup("customer", "alice@test.com")
        bob_id = m._identity_backend.lookup("customer", "bob@test.com")
        m.merge(alice_id, bob_id)

        s = self._store_snapshot(m, db_path)

        # DashMap: 1 entity (winner only)
        assert s["entity_count"] == 1

        # Identity: both hints point to winner
        identity_map = {row[1]: row[2] for row in s["identity"]}
        assert identity_map["alice@test.com"] == bob_id
        assert identity_map["bob@test.com"] == bob_id

        # Event_entities: all events mapped to winner
        ee_entities = {row[1] for row in s["event_entities"]}
        assert ee_entities == {bob_id}

        # Merge_log: records the merge
        assert (alice_id, bob_id) in s["merge_log"]

        # State history: winner current with correct state, loser tombstoned
        current = {row[0]: row for row in s["state_history_current"]}
        assert bob_id in current
        assert current[bob_id][1] == "active"  # state
        assert current[bob_id][2] == "pro"     # plan
        assert alice_id not in current

        tombstones = {row[0]: row[1] for row in s["state_history_tombstones"]}
        assert tombstones.get(alice_id) == bob_id

        s["conn"].close()
        m.close()

    def test_erase_all_stores_clean(self, tmp_path):
        """After erase, every trace of the entity is gone."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
            {"type": "signup", "ts": "2024-01-15T10:01:00Z", "email": "bob@test.com"},
        ])
        m.build()

        alice_id = m._identity_backend.lookup("customer", "alice@test.com")
        bob_id = m._identity_backend.lookup("customer", "bob@test.com")
        m.merge(alice_id, bob_id)
        m.erase(bob_id)

        s = self._store_snapshot(m, db_path)

        # DashMap: empty
        assert s["entity_count"] == 0

        # Identity: no hints for either entity
        assert len(s["identity"]) == 0

        # Event_entities: no events for either entity
        assert len(s["event_entities"]) == 0

        # Merge_log: cleaned up
        assert len(s["merge_log"]) == 0

        # State history: no rows for either entity
        assert len(s["state_history_current"]) == 0

        s["conn"].close()
        m.close()

    def test_merge_then_full_rebuild_stores_consistent(self, tmp_path):
        """After merge + full rebuild, all stores match direct merge state."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-01T10:00:00Z", "email": "bob@test.com"},
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
            {"type": "upgrade", "ts": "2024-01-15T10:01:00Z",
             "email": "alice@test.com", "plan": "pro"},
        ])
        m.build()

        alice_id = m._identity_backend.lookup("customer", "alice@test.com")
        bob_id = m._identity_backend.lookup("customer", "bob@test.com")
        m.merge(alice_id, bob_id)

        # Full rebuild
        m._core.clear()
        m._pipeline.build_full(m._active_version)

        s = self._store_snapshot(m, db_path)

        # DashMap: 1 entity
        assert s["entity_count"] == 1

        # Identity: both hints → winner
        identity_map = {row[1]: row[2] for row in s["identity"]}
        assert identity_map["alice@test.com"] == bob_id
        assert identity_map["bob@test.com"] == bob_id

        # Event_entities: all mapped to winner
        ee_entities = {row[1] for row in s["event_entities"]}
        assert ee_entities == {bob_id}

        # State history: winner current with correct combined state
        current = {row[0]: row for row in s["state_history_current"]}
        assert bob_id in current
        assert current[bob_id][1] == "active"
        assert current[bob_id][2] == "pro"

        s["conn"].close()
        m.close()

    def test_cursor_not_regressed_after_rebuild(self, tmp_path):
        """Post-pass rebuild doesn't regress the cursor."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-01T10:00:00Z", "email": "bob@test.com"},
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ])
        m.build()

        alice_id = m._identity_backend.lookup("customer", "alice@test.com")
        bob_id = m._identity_backend.lookup("customer", "bob@test.com")
        m.merge(alice_id, bob_id)

        # Add more events
        m.ingest("web", [
            {"type": "upgrade", "ts": "2024-02-01T10:00:00Z",
             "email": "alice@test.com", "plan": "pro"},
        ])
        m.build()

        # Cursor should be at the head of the ledger
        cursor = m._build_manager.get_status(m._active_version).cursor
        ledger_head = m._ledger.cursor()
        assert cursor == ledger_head

        # Another build should be SKIP (nothing to do)
        result = m.build()
        assert result.mode == "SKIP"
        m.close()


class TestExpressionExtensions:
    """End-to-end tests for split, join, and bracket indexing in definitions."""

    DEFS_WITH_SPLIT = {
        "entities": {
            "customer": {
                "starts": "active",
                "properties": {
                    "email": {"type": "string"},
                    "domain": {"type": "string"},
                },
                "identity": {
                    "email": {"match": "exact"},
                },
                "states": {
                    "active": {
                        "when": {
                            "signup": {
                                "effects": [
                                    "create",
                                    {"set": {"property": "email", "from": "event.email"}},
                                    {"set": {"property": "domain", "compute": 'split(event.email, "@")[1]'}},
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
                },
            },
        },
    }

    def test_split_and_index_in_definition(self, tmp_path):
        """split(event.email, "@")[1] extracts the domain in a computed property."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(self.DEFS_WITH_SPLIT, database=db_path)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ])
        m.build()

        df = m.table("customer").execute()
        assert len(df) == 1
        assert df["domain"].iloc[0] == "test.com"
        m.close()

    def test_split_various_emails(self, tmp_path):
        """split+index works across different email formats."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(self.DEFS_WITH_SPLIT, database=db_path)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
            {"type": "signup", "ts": "2024-01-15T10:01:00Z", "email": "bob@company.co.uk"},
        ])
        m.build()

        df = m.table("customer").execute()
        domains = set(df["domain"])
        assert "test.com" in domains
        assert "company.co.uk" in domains
        m.close()


class TestGraphAndConnection:
    """Graph auto-materialization and connection access."""

    def test_connection_returns_raw_db(self, tmp_path):
        """m.connection exposes the state history database connection."""
        import sqlite3
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        assert isinstance(m.connection, sqlite3.Connection)
        m.close()

    def test_graph_auto_materializes_networkx(self, tmp_path):
        """m.graph auto-creates NetworkX backend from current entity state."""
        pytest.importorskip("networkx")
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ])
        m.build()

        # m.graph triggers NetworkX materialization
        from defacto.query._graph import NetworkXGraphBackend
        backend = m.graph
        assert isinstance(backend, NetworkXGraphBackend)

        # Analytics work
        df = backend.centrality()
        assert len(df) >= 1

        # Connection returns the DiGraph
        import networkx as nx
        assert isinstance(backend.connection, nx.DiGraph)
        m.close()

    def test_core_ops_use_cte_before_graph_access(self, tmp_path):
        """Before m.graph is accessed, the backend is CTE."""
        pytest.importorskip("networkx")
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ])
        m.build()

        # Before any graph access, backend is CTE
        from defacto.query._graph import CteGraphBackend
        assert isinstance(m._graph_backend, CteGraphBackend)

        # Core op on undefined relationship gives clear error
        with pytest.raises(ValueError, match="'placed_order' is not defined"):
            m.traverse("cust_001", "placed_order")

        # After m.graph access, backend upgrades to NetworkX
        from defacto.query._graph import NetworkXGraphBackend
        _ = m.graph
        assert isinstance(m._graph_backend, NetworkXGraphBackend)
        m.close()


class TestMultiWorkerCorrectness:
    """Multi-worker interpretation produces correct results.

    Verifies that workers > 1 (Rayon thread pool) produces the same
    entity state as workers=1. This tests the full Python → Rust → Python
    path with parallel interpretation.
    """

    def test_workers_4_matches_workers_1(self, tmp_path):
        """4 workers produce identical entity state to 1 worker."""
        events = [
            {"type": "signup", "ts": f"2024-{1 + i // 28:02d}-{1 + i % 28:02d}T10:00:00Z",
             "email": f"user{i}@test.com"}
            for i in range(20)
        ] + [
            {"type": "upgrade", "ts": f"2024-{1 + i // 28:02d}-{1 + i % 28:02d}T11:00:00Z",
             "email": f"user{i}@test.com", "plan": "pro"}
            for i in range(10)
        ]

        # Single worker — baseline
        db1 = str(tmp_path / "w1.db")
        m1 = Defacto(DEFINITIONS, database=db1, workers=1)
        m1.ingest("web", events)
        r1 = m1.build()
        count_1 = m1._core.entity_count()
        df_1 = m1.table("customer").execute().sort_values("customer_id")
        m1.close()

        # Four workers — should produce identical results
        db4 = str(tmp_path / "w4.db")
        m4 = Defacto(DEFINITIONS, database=db4, workers=4)
        m4.ingest("web", events)
        r4 = m4.build()
        count_4 = m4._core.entity_count()
        df_4 = m4.table("customer").execute().sort_values("customer_id")
        m4.close()

        # Same number of events processed and entities created
        assert r1.events_processed == r4.events_processed
        assert count_1 == count_4 == 20

        # Compare by email (identity hint) not entity_id (generated UUID).
        # Each instance generates its own UUIDs, but the same emails
        # should produce the same states and properties.
        states_1 = dict(zip(df_1["email"], df_1["customer_state"]))
        states_4 = dict(zip(df_4["email"], df_4["customer_state"]))
        assert states_1 == states_4

        plans_1 = dict(zip(df_1["email"], df_1["plan"]))
        plans_4 = dict(zip(df_4["email"], df_4["plan"]))
        assert plans_1 == plans_4

        # First 10 users upgraded to active with plan=pro
        for i in range(10):
            email = f"user{i}@test.com"
            assert states_1[email] == "active", f"{email} should be active"
            assert plans_1[email] == "pro", f"{email} should have plan=pro"

        # Last 10 users still in lead with plan=free
        for i in range(10, 20):
            email = f"user{i}@test.com"
            assert states_1[email] == "lead", f"{email} should be lead"
            assert plans_1[email] == "free", f"{email} should have plan=free"

    def test_workers_4_stream_first(self, tmp_path):
        """Multi-worker correctness with process=True (streaming path)."""
        db1 = str(tmp_path / "w1.db")
        m1 = Defacto(DEFINITIONS, database=db1, workers=1, batch_size=5)
        for i in range(15):
            m1.ingest("web", [
                {"type": "signup", "ts": f"2024-01-{15 + i}T10:00:00Z",
                 "email": f"user{i}@test.com"},
            ], process=True)
        m1.close()

        db4 = str(tmp_path / "w4.db")
        m4 = Defacto(DEFINITIONS, database=db4, workers=4, batch_size=5)
        for i in range(15):
            m4.ingest("web", [
                {"type": "signup", "ts": f"2024-01-{15 + i}T10:00:00Z",
                 "email": f"user{i}@test.com"},
            ], process=True)
        m4.close()

        # Reopen to query (close flushes remaining)
        m1r = Defacto(DEFINITIONS, database=db1, workers=1)
        m4r = Defacto(DEFINITIONS, database=db4, workers=4)
        assert m1r._core.entity_count() == m4r._core.entity_count() == 15
        m1r.close()
        m4r.close()


class TestAdversarialInputs:
    """Edge cases in data crossing the Python ↔ Rust ↔ database boundary.

    These test the full pipeline round-trip: raw event → PyO3 conversion →
    Rust normalization → ledger (SQLite JSON) → replay → interpret →
    snapshot → state history. If any boundary mishandles the data,
    the final query result will be wrong.
    """

    def test_unicode_in_event_data(self, tmp_path):
        """Unicode (emoji, CJK, accented chars) survives the full pipeline."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z",
             "email": "ünïcödé@tëst.com"},
        ])
        m.build()

        # Identity resolution should handle unicode hint
        entity_id = m._identity_backend.lookup("customer", "ünïcödé@tëst.com")
        assert entity_id is not None

        # Property should preserve unicode
        df = m.table("customer").execute()
        assert df["email"].iloc[0] == "ünïcödé@tëst.com"
        m.close()

    def test_emoji_in_event_data(self, tmp_path):
        """Emoji survive the full pipeline including ledger storage."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z",
             "email": "rocket🚀@test.com"},
        ])
        m.build()

        df = m.table("customer").execute()
        assert "🚀" in df["email"].iloc[0]
        m.close()

    def test_null_property_value(self, tmp_path):
        """None/null in event data doesn't crash the pipeline."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z",
             "email": "alice@test.com"},
            # Upgrade with plan=None — should this use the default or set null?
            {"type": "upgrade", "ts": "2024-01-15T11:00:00Z",
             "email": "alice@test.com", "plan": None},
        ])
        result = m.build()
        # Should not crash — either the null is set or the default applies
        assert result.events_processed >= 1
        m.close()

    def test_nested_json_in_raw_event(self, tmp_path):
        """Deeply nested JSON in raw events survives ledger round-trip."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)

        nested_event = {
            "type": "signup",
            "ts": "2024-01-15T10:00:00Z",
            "email": "alice@test.com",
            "metadata": {
                "browser": {"name": "Chrome", "version": 120},
                "location": {"country": "US", "city": "SF"},
                "tags": ["new", "web", "organic"],
            },
        }
        m.ingest("web", [nested_event])
        m.build()

        # Raw input should preserve nested structure in ledger
        events = m.ledger.events_for(
            m._identity_backend.lookup("customer", "alice@test.com")
        )
        assert len(events) >= 1

        # Replay the raw event from ledger — nested JSON should survive
        ledger_events = list(m._ledger.replay(from_sequence=0))
        raw = ledger_events[0]["raw"]
        assert raw["metadata"]["browser"]["name"] == "Chrome"
        assert raw["metadata"]["tags"] == ["new", "web", "organic"]
        m.close()

    def test_empty_string_values(self, tmp_path):
        """Empty strings are preserved, not converted to null."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": ""},
        ])
        result = m.build()
        # Empty email is a valid input — should process without error
        assert result.events_processed >= 1
        m.close()

    def test_large_event_payload(self, tmp_path):
        """Large event payloads cross the PyO3 boundary without issues."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)

        # ~100KB payload
        large_value = "x" * 100_000
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z",
             "email": "alice@test.com", "notes": large_value},
        ])
        result = m.build()
        assert result.events_processed == 1

        # Verify the large value survived the round-trip
        ledger_events = list(m._ledger.replay(from_sequence=0))
        assert len(ledger_events[0]["raw"]["notes"]) == 100_000
        m.close()

    def test_special_characters_in_values(self, tmp_path):
        """SQL-sensitive characters don't cause injection or corruption."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z",
             "email": "alice'; DROP TABLE ledger;--@test.com"},
        ])
        result = m.build()
        assert result.events_processed == 1

        # Verify the value is stored literally, no injection
        df = m.table("customer").execute()
        assert "DROP TABLE" in df["email"].iloc[0]
        m.close()

    def test_boolean_and_numeric_types_preserved(self, tmp_path):
        """Python bool, int, float survive round-trip through raw column."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)

        event = {
            "type": "signup",
            "ts": "2024-01-15T10:00:00Z",
            "email": "alice@test.com",
            "active": True,
            "count": 42,
            "score": 3.14,
        }
        m.ingest("web", [event])
        m.build()

        ledger_events = list(m._ledger.replay(from_sequence=0))
        raw = ledger_events[0]["raw"]
        assert raw["active"] is True
        assert raw["count"] == 42
        assert abs(raw["score"] - 3.14) < 0.001
        m.close()

    def test_many_events_single_batch(self, tmp_path):
        """Large batch (1000 events) processes without issues."""
        from datetime import datetime, timedelta, timezone

        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        base = datetime(2024, 1, 1, tzinfo=timezone.utc)
        events = [
            {"type": "signup",
             "ts": (base + timedelta(hours=i)).strftime("%Y-%m-%dT%H:%M:%SZ"),
             "email": f"user{i}@test.com"}
            for i in range(1000)
        ]
        m.ingest("web", events)
        result = m.build()
        assert result.events_processed == 1000
        assert m._core.entity_count() == 1000
        m.close()


class TestEndToEnd:
    """Full workflows combining multiple operations."""

    def test_ingest_build_query_lifecycle(self, project_dir):
        """Ingest → build → verify entity state."""
        m = Defacto(project_dir)

        # Ingest events
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
            {"type": "signup", "ts": "2024-01-15T10:01:00Z", "email": "bob@test.com"},
        ])

        # Build entity state
        result = m.build()
        assert result.events_processed == 2
        assert m._core.entity_count() == 2

        m.close()

    def test_multi_batch_ingest_then_build(self, project_dir):
        """Multiple ingest calls, then one build."""
        m = Defacto(project_dir)

        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ])
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T11:00:00Z", "email": "bob@test.com"},
        ])
        m.ingest("web", [
            {"type": "upgrade", "ts": "2024-01-15T12:00:00Z", "email": "alice@test.com", "plan": "pro"},
        ])

        result = m.build()
        assert result.events_processed == 3
        m.close()

    def test_stream_first_pattern(self):
        """Stream-first: ingest with process=True, events processed inline."""
        m = Defacto(DEFINITIONS, batch_size=2)

        # First event: buffered
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ], process=True)

        # Second event: triggers flush, both processed
        result = m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:01:00Z", "email": "bob@test.com"},
        ], process=True)

        assert result.events_ingested == 2
        assert m._core.entity_count() == 2
        m.close()
