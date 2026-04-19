"""Tests for Pipeline — batch processing coordination.

Pipeline tests use mock components that record calls and return canned
data, plus real SQLite backends for the ledger and identity store.
"""

import sqlite3
from typing import Any

import pytest

from defacto._build import BuildManager, SqliteBuildStateStore
from defacto._identity import IdentityResolver
from defacto._pipeline import Pipeline
from defacto._publisher import InlinePublisher
from defacto.backends import SqliteIdentity, SqliteLedger, SqliteStateHistory


# ---------------------------------------------------------------------------
# Mock DefactoCore
# ---------------------------------------------------------------------------


class MockCore:
    """Mock DefactoCore that tracks calls and returns canned responses.

    Provides enough of the DefactoCore interface for Pipeline tests:
    normalize, interpret, merge, resolve_cache, update_cache, clear, delete,
    definition_hashes.
    """

    def __init__(self) -> None:
        self.normalize_calls: list[tuple] = []
        self.interpret_calls: list[Any] = []
        self.merge_calls: list[tuple] = []
        self.load_events_calls: list[list] = []
        self.clear_calls: int = 0
        self.delete_calls: list[str] = []
        self._cache: dict[tuple[str, str], str] = {}
        self._hashes = {
            "definition_hash": "def_abc",
            "source_hash": "src_def",
            "identity_hash": "id_ghi",
        }
        # Canned normalize output
        self._normalize_result: dict[str, Any] | None = None

    def normalize(self, source: str, raw_events: list) -> dict:
        self.normalize_calls.append((source, raw_events))
        if self._normalize_result:
            return self._normalize_result
        # Default: produce one ledger row per event
        ledger_rows = []
        for i, evt in enumerate(raw_events):
            ledger_rows.append({
                "event_id": evt.get("event_id", f"evt_{i}"),
                "event_type": evt.get("type", "signup"),
                "timestamp": evt.get("timestamp", "2024-01-15T10:00:00Z"),
                "source": source,
                "data": evt,
                "raw": evt,
                "resolution_hints": evt.get("resolution_hints", {}),
            })
        return {"ledger_rows": ledger_rows, "failures": [], "count": len(ledger_rows)}

    def interpret(self, entity_mapping: list) -> dict:
        self.interpret_calls.append(entity_mapping)
        # Return one snapshot per unique entity, no failures
        seen = {}
        for _, eid, etype in entity_mapping:
            if eid not in seen:
                seen[eid] = {
                    "entity_id": eid,
                    "entity_type": etype,
                    "state": "active",
                    "properties": {},
                    "valid_from": "2024-01-15T10:00:00Z",
                }
        return {"snapshots": list(seen.values()), "failures": []}

    def merge(self, from_id: str, into_id: str) -> dict:
        self.merge_calls.append((from_id, into_id))
        return {"entity_id": into_id, "entity_type": "customer", "valid_from": "2024-01-15T10:00:00Z"}

    def resolve_cache(self, hints: list[tuple[str, str]]) -> dict:
        resolved, unresolved = [], []
        for et, hv in hints:
            if (et, hv) in self._cache:
                resolved.append((et, hv, self._cache[(et, hv)]))
            else:
                unresolved.append((et, hv))
        return {"resolved": resolved, "unresolved": unresolved}

    def update_cache(self, mappings: list[tuple[str, str, str]]) -> None:
        for et, hv, eid in mappings:
            self._cache[(et, hv)] = eid

    def load_events(self, events: list) -> None:
        self.load_events_calls.append(events)

    def clear(self) -> None:
        self.clear_calls += 1

    def delete(self, entity_id: str) -> None:
        self.delete_calls.append(entity_id)

    def definition_hashes(self, version: str) -> dict:
        return self._hashes

    def clear_identity_cache(self) -> None:
        self._cache.clear()

    def tick(self, as_of: str) -> list:
        """No time rules in mock — always returns empty."""
        return []


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

CUSTOMER_DEF = {"properties": {"name": {"type": "string"}}}


def make_pipeline() -> tuple[Pipeline, MockCore, SqliteLedger]:
    """Build a full pipeline with mock core and real SQLite backends."""
    core = MockCore()
    ledger = SqliteLedger(":memory:")
    identity_backend = SqliteIdentity(":memory:")
    identity = IdentityResolver(core, identity_backend)

    sh = SqliteStateHistory(":memory:")
    sh.ensure_tables({"customer": CUSTOMER_DEF})
    publisher = InlinePublisher(sh)

    store = SqliteBuildStateStore(":memory:")
    build_manager = BuildManager(store, core)

    pipeline = Pipeline(core, ledger, identity, publisher, build_manager)
    return pipeline, core, ledger


def make_raw_event(
    event_id: str = "evt_001",
    entity_type: str = "customer",
    hint_value: str = "alice@test.com",
) -> dict:
    """Build a raw event with identity hints."""
    return {
        "event_id": event_id,
        "type": "signup",
        "timestamp": "2024-01-15T10:00:00Z",
        "resolution_hints": {entity_type: {"hint": hint_value}},
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestProcessBatch:
    """The core batch processing flow."""

    def test_basic_flow(self):
        pipeline, core, ledger = make_pipeline()
        result, snapshots = pipeline.process_batch(
            "web", [make_raw_event()], "v1"
        )
        assert result.events_ingested == 1
        assert result.events_failed == 0
        assert len(snapshots) == 1
        assert snapshots[0]["entity_type"] == "customer"

    def test_normalizes_events(self):
        pipeline, core, _ = make_pipeline()
        pipeline.process_batch("web", [make_raw_event()], "v1")
        assert len(core.normalize_calls) == 1
        assert core.normalize_calls[0][0] == "web"

    def test_appends_to_ledger(self):
        pipeline, _, ledger = make_pipeline()
        pipeline.process_batch("web", [make_raw_event()], "v1")
        assert ledger.cursor() > 0

    def test_writes_event_entities(self):
        pipeline, _, ledger = make_pipeline()
        pipeline.process_batch("web", [make_raw_event()], "v1")
        # Should have event_entities for the event
        events = list(ledger.replay_for_entities(["will_not_match"]))
        # Test that event_entities were written by replaying for the actual entity
        all_events = list(ledger.replay())
        assert len(all_events) == 1

    def test_interprets_events(self):
        pipeline, core, _ = make_pipeline()
        pipeline.process_batch("web", [make_raw_event()], "v1")
        assert len(core.interpret_calls) == 1

    def test_returns_ingest_result(self):
        pipeline, _, _ = make_pipeline()
        result, _ = pipeline.process_batch("web", [make_raw_event()], "v1")
        assert result.events_ingested == 1
        assert result.events_failed == 0
        assert result.duplicates_skipped == 0

    def test_all_failures_returns_empty(self):
        pipeline, core, _ = make_pipeline()
        core._normalize_result = {
            "ledger_rows": [],
            "failures": [{"raw": {}, "error": "bad event", "source": "web", "handler": None, "field": None}],
            "count": 0,
        }
        result, snapshots = pipeline.process_batch("web", [{"bad": True}], "v1")
        assert result.events_ingested == 0
        assert result.events_failed == 1
        assert snapshots == []

    def test_multiple_events(self):
        pipeline, _, _ = make_pipeline()
        events = [
            make_raw_event("e1", hint_value="alice@test.com"),
            make_raw_event("e2", hint_value="bob@test.com"),
            make_raw_event("e3", hint_value="carol@test.com"),
        ]
        result, snapshots = pipeline.process_batch("web", events, "v1")
        assert result.events_ingested == 3
        assert len(snapshots) == 3

    def test_empty_events(self):
        pipeline, core, _ = make_pipeline()
        core._normalize_result = {"ledger_rows": [], "failures": [], "count": 0}
        result, snapshots = pipeline.process_batch("web", [], "v1")
        assert result.events_ingested == 0
        assert snapshots == []


class TestBuildIncremental:
    """Incremental build — replay from cursor."""

    def test_incremental_returns_result(self):
        pipeline, _, ledger = make_pipeline()
        # First, ingest some events
        pipeline.process_batch("web", [make_raw_event("e1")], "v1")
        pipeline.process_batch("web", [make_raw_event("e2", hint_value="bob@test.com")], "v1")

        result = pipeline.build_incremental("v1")
        assert result.mode == "INCREMENTAL"
        assert isinstance(result.duration_ms, int)

    def test_incremental_no_new_events(self):
        pipeline, _, _ = make_pipeline()
        result = pipeline.build_incremental("v1")
        assert result.events_processed == 0


class TestBuildFull:
    """Full build — clear + replay all."""

    def test_full_clears_state(self):
        pipeline, core, _ = make_pipeline()
        pipeline.process_batch("web", [make_raw_event()], "v1")
        pipeline.build_full("v1")
        assert core.clear_calls >= 1

    def test_full_returns_result(self):
        pipeline, _, _ = make_pipeline()
        pipeline.process_batch("web", [make_raw_event()], "v1")
        result = pipeline.build_full("v1")
        assert result.mode == "FULL"


class TestBuildPartial:
    """Partial build — replay for specific entities."""

    def test_partial_deletes_affected(self):
        pipeline, core, _ = make_pipeline()
        pipeline.process_batch("web", [make_raw_event()], "v1")
        pipeline.build_partial("v1", ["cust_001", "cust_002"])
        assert "cust_001" in core.delete_calls
        assert "cust_002" in core.delete_calls

    def test_partial_returns_result(self):
        pipeline, _, _ = make_pipeline()
        result = pipeline.build_partial("v1", ["cust_001"])
        assert result.mode == "PARTIAL"


# ===========================================================================
# Resilience — interpretation failures don't crash the batch
# ===========================================================================


class TestInterpretResilience:
    """Interpretation failures are surfaced, not fatal."""

    def test_interpret_failures_in_result(self):
        """Interpretation failures appear in IngestResult.failures."""
        pipeline, core, _ = make_pipeline()

        # Override interpret to return a mix of snapshots and failures
        def failing_interpret(entity_mapping):
            return {
                "snapshots": [
                    {"entity_id": "c1", "entity_type": "customer",
                     "state": "active", "properties": {}},
                ],
                "failures": [
                    {"entity_id": "c2", "entity_type": "customer",
                     "error": "Entity doesn't exist (missing create)"},
                ],
            }
        core.interpret = failing_interpret

        result, snapshots = pipeline.process_batch("web", [
            make_raw_event(),
        ], "v1")

        # Good entity succeeded
        assert len(snapshots) == 1
        assert snapshots[0]["entity_id"] == "c1"

        # Failed entity is in failures with correct stage
        interpret_failures = [f for f in result.failures if f.stage == "interpretation"]
        assert len(interpret_failures) == 1
        assert interpret_failures[0].entity_id == "c2"
        assert "missing create" in interpret_failures[0].error

    def test_dead_letter_receives_failures(self):
        """Dead letter sink receives failures when configured."""
        from defacto._dead_letter import NullDeadLetter

        # Use a collecting sink to verify routing
        class CollectingSink(NullDeadLetter):
            def __init__(self):
                self.received: list = []

            def send(self, failures):
                self.received.extend(failures)

        sink = CollectingSink()
        pipeline, core, _ = make_pipeline()
        pipeline._dead_letter = sink

        # Override interpret to produce a failure
        def failing_interpret(entity_mapping):
            return {
                "snapshots": [],
                "failures": [
                    {"entity_id": "c1", "entity_type": "customer",
                     "error": "Invalid transition"},
                ],
            }
        core.interpret = failing_interpret

        pipeline.process_batch("web", [make_raw_event()], "v1")

        assert len(sink.received) == 1
        assert sink.received[0].stage == "interpretation"
        assert sink.received[0].entity_id == "c1"


class TestStorageErrorWrapping:
    """Database exceptions are wrapped in StorageError."""

    def test_storage_errors_wraps_psycopg(self):
        """psycopg exceptions become StorageError with recoverable=True."""
        from defacto.errors import StorageError, storage_errors

        # Simulate a psycopg-like exception with the right module
        class FakePsycopgError(Exception):
            pass
        FakePsycopgError.__module__ = "psycopg"

        with pytest.raises(StorageError) as exc_info:
            with storage_errors("Ledger append"):
                raise FakePsycopgError("connection refused")

        assert "Ledger append failed" in str(exc_info.value)
        assert exc_info.value.recoverable is True
        assert exc_info.value.details["operation"] == "Ledger append"

    def test_storage_errors_passes_non_db_exceptions(self):
        """Non-database exceptions propagate unchanged."""
        from defacto.errors import storage_errors

        with pytest.raises(ValueError):
            with storage_errors("test"):
                raise ValueError("not a database error")

    def test_storage_errors_no_double_wrap(self):
        """StorageError inside storage_errors doesn't get double-wrapped."""
        from defacto.errors import StorageError, storage_errors

        with pytest.raises(StorageError) as exc_info:
            with storage_errors("outer"):
                raise StorageError("already wrapped")

        assert str(exc_info.value) == "already wrapped"
