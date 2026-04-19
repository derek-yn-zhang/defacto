"""Integration tests for Postgres backends.

Tests all Postgres backend implementations against a real Postgres 16
instance (docker compose up -d postgres). Each test gets a clean
database via the clean_schemas fixture in conftest.py.

These tests verify Postgres-specific behavior: JSONB auto-deserialization,
TIMESTAMPTZ handling, ON CONFLICT dedup, per-version schemas, and the
full Defacto write path through Postgres.
"""

import pytest

pytestmark = pytest.mark.postgres


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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


CUSTOMER_DEF = {
    "properties": {
        "name": {"type": "string"},
        "mrr": {"type": "float"},
    },
}


# ===========================================================================
# PostgresLedger
# ===========================================================================


class TestPostgresLedgerAppend:
    """Append and dedup via ON CONFLICT."""

    def test_append_returns_sequences(self, pg_conninfo):
        from defacto.backends import PostgresLedger

        ledger = PostgresLedger(pg_conninfo)
        seqs = ledger.append_batch([make_event("e1"), make_event("e2")])
        assert len(seqs) == 2
        assert seqs[0] < seqs[1]
        ledger.close()

    def test_duplicate_event_id_skipped(self, pg_conninfo):
        from defacto.backends import PostgresLedger

        ledger = PostgresLedger(pg_conninfo)
        ledger.append_batch([make_event("e1")])
        seqs = ledger.append_batch([make_event("e1")])
        assert len(seqs) == 1  # returns existing sequence
        assert len(list(ledger.replay())) == 1  # only one row
        ledger.close()

    def test_empty_batch(self, pg_conninfo):
        from defacto.backends import PostgresLedger

        ledger = PostgresLedger(pg_conninfo)
        assert ledger.append_batch([]) == []
        ledger.close()


class TestPostgresLedgerReplay:
    """Replay with JSONB auto-deserialization and timestamp ordering."""

    def test_replay_returns_dicts(self, pg_conninfo):
        """JSONB columns come back as Python dicts, not strings."""
        from defacto.backends import PostgresLedger

        ledger = PostgresLedger(pg_conninfo)
        ledger.append_batch([make_event("e1", data={"email": "alice@test.com"})])
        events = list(ledger.replay())
        assert len(events) == 1
        assert isinstance(events[0]["data"], dict)
        assert events[0]["data"]["email"] == "alice@test.com"
        assert isinstance(events[0]["raw"], dict)
        assert isinstance(events[0]["resolution_hints"], dict)
        ledger.close()

    def test_replay_timestamp_order(self, pg_conninfo):
        """Events returned in timestamp order, not insertion order."""
        from defacto.backends import PostgresLedger

        ledger = PostgresLedger(pg_conninfo)
        ledger.append_batch([
            make_event("e1", timestamp="2024-01-15T10:00:00Z"),
            make_event("e2", timestamp="2024-01-15T09:00:00Z"),
            make_event("e3", timestamp="2024-01-15T11:00:00Z"),
        ])
        events = list(ledger.replay())
        timestamps = [e["event_id"] for e in events]
        assert timestamps == ["e2", "e1", "e3"]
        ledger.close()

    def test_replay_for_entities(self, pg_conninfo):
        from defacto.backends import PostgresLedger

        ledger = PostgresLedger(pg_conninfo)
        seqs = ledger.append_batch([make_event("e1"), make_event("e2")])
        ledger.write_event_entities([
            (seqs[0], "c_001", "customer"),
            (seqs[1], "c_002", "customer"),
        ])
        events = list(ledger.replay_for_entities(["c_001"]))
        assert len(events) == 1
        assert events[0]["event_id"] == "e1"
        ledger.close()


class TestPostgresLedgerRedact:
    """Server-side JSONB redaction."""

    def test_redact_sensitive_fields(self, pg_conninfo):
        from defacto.backends import PostgresLedger

        ledger = PostgresLedger(pg_conninfo)
        seqs = ledger.append_batch([
            make_event("e1", data={"email": "a@b.com", "plan": "pro"},
                       raw={"email": "a@b.com", "plan": "pro"}),
        ])
        ledger.write_event_entities([(seqs[0], "c_001", "customer")])
        count = ledger.redact("c_001", ["email"])

        assert count == 1
        events = list(ledger.replay())
        assert events[0]["data"]["email"] == "[REDACTED]"
        assert events[0]["raw"]["email"] == "[REDACTED]"
        assert events[0]["data"]["plan"] == "pro"
        ledger.close()


class TestPostgresLedgerUpdateNormalized:
    """Ledger update for FULL_RENORMALIZE."""

    def test_update_changes_data_preserves_raw(self, pg_conninfo):
        from defacto.backends import PostgresLedger

        ledger = PostgresLedger(pg_conninfo)
        original_raw = {"email": "alice@test.com", "type": "signup"}
        seqs = ledger.append_batch([make_event("e1", raw=original_raw)])

        new_row = {
            "data": {"email": "ALICE@TEST.COM"},
            "event_id": "e_new_hash",
            "event_type": "customer_signup",
            "resolution_hints": {"customer": {"email": "ALICE@TEST.COM"}},
        }
        ledger.update_normalized([(seqs[0], new_row)])

        events = list(ledger.replay())
        assert events[0]["data"]["email"] == "ALICE@TEST.COM"
        assert events[0]["event_id"] == "e_new_hash"
        assert events[0]["raw"] == original_raw
        ledger.close()


class TestPostgresLedgerClearEventEntities:
    """TRUNCATE for RENORMALIZE/IDENTITY_RESET."""

    def test_truncate_clears_all(self, pg_conninfo):
        from defacto.backends import PostgresLedger

        ledger = PostgresLedger(pg_conninfo)
        seqs = ledger.append_batch([make_event("e1")])
        ledger.write_event_entities([(seqs[0], "c_001", "customer")])
        ledger.clear_event_entities()

        assert list(ledger.replay_for_entities(["c_001"])) == []
        ledger.close()


# ===========================================================================
# PostgresIdentity
# ===========================================================================


class TestPostgresIdentity:
    """Identity resolution with CTE upsert."""

    def test_create_and_resolve(self, pg_conninfo):
        from defacto.backends import PostgresIdentity

        identity = PostgresIdentity(pg_conninfo)
        result = identity.resolve_and_create([
            ("customer", "email", "alice@test.com", "cust_001"),
            ("customer", "email", "bob@test.com", "cust_002"),
        ])
        assert len(result) == 2
        result_map = {(r[0], r[1]): r[2] for r in result}
        assert result_map[("customer", "alice@test.com")] == "cust_001"
        assert result_map[("customer", "bob@test.com")] == "cust_002"
        identity.close()

    def test_existing_hint_preserved(self, pg_conninfo):
        """Existing hint returns original entity_id, ignores candidate."""
        from defacto.backends import PostgresIdentity

        identity = PostgresIdentity(pg_conninfo)
        identity.resolve_and_create([("customer", "email", "alice@test.com", "cust_001")])
        result = identity.resolve_and_create([("customer", "email", "alice@test.com", "cust_999")])
        assert result[0][2] == "cust_001"  # original, not cust_999
        identity.close()

    def test_entity_type_scoping(self, pg_conninfo):
        """Same hint value for different entity types resolves independently."""
        from defacto.backends import PostgresIdentity

        identity = PostgresIdentity(pg_conninfo)
        identity.resolve_and_create([
            ("customer", "email", "alice@test.com", "cust_001"),
            ("user", "email", "alice@test.com", "user_001"),
        ])
        assert identity.lookup("customer", "alice@test.com") == "cust_001"
        assert identity.lookup("user", "alice@test.com") == "user_001"
        identity.close()

    def test_merge(self, pg_conninfo):
        from defacto.backends import PostgresIdentity

        identity = PostgresIdentity(pg_conninfo)
        identity.resolve_and_create([
            ("customer", "email", "alice@test.com", "cust_001"),
            ("customer", "phone", "555-1234", "cust_002"),
        ])
        identity.merge("cust_002", "cust_001")
        assert identity.lookup("customer", "555-1234") == "cust_001"
        identity.close()

    def test_warmup(self, pg_conninfo):
        from defacto.backends import PostgresIdentity

        identity = PostgresIdentity(pg_conninfo)
        identity.resolve_and_create([
            ("customer", "email", "alice@test.com", "cust_001"),
        ])
        mappings = identity.warmup()
        assert len(mappings) == 1
        assert mappings[0] == ("customer", "alice@test.com", "cust_001")
        identity.close()

    def test_delete(self, pg_conninfo):
        from defacto.backends import PostgresIdentity

        identity = PostgresIdentity(pg_conninfo)
        identity.resolve_and_create([
            ("customer", "email", "alice@test.com", "cust_001"),
            ("customer", "phone", "555-1234", "cust_001"),
        ])
        count = identity.delete("cust_001")
        assert count == 2
        assert identity.lookup("customer", "alice@test.com") is None
        identity.close()

    def test_reset(self, pg_conninfo):
        from defacto.backends import PostgresIdentity

        identity = PostgresIdentity(pg_conninfo)
        identity.resolve_and_create([
            ("customer", "email", "alice@test.com", "cust_001"),
        ])
        identity.reset()
        assert identity.warmup() == []
        identity.close()


# ===========================================================================
# PostgresStateHistory
# ===========================================================================


class TestPostgresStateHistory:
    """SCD Type 2 tables with per-version schemas."""

    def test_ensure_tables_creates_schema(self, pg_conninfo):
        """Per-version schema (defacto_dev) is created."""
        import psycopg
        from defacto.backends import PostgresStateHistory

        sh = PostgresStateHistory(pg_conninfo, version="dev")
        sh.ensure_tables({"customer": CUSTOMER_DEF})

        conn = psycopg.connect(pg_conninfo)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables"
                " WHERE table_schema = 'defacto_dev'"
            )
            tables = [row[0] for row in cur.fetchall()]
        conn.close()

        assert "customer_history" in tables
        sh.close()

    def test_write_and_delete(self, pg_conninfo):
        from defacto.backends import PostgresStateHistory

        sh = PostgresStateHistory(pg_conninfo, version="dev")
        sh.ensure_tables({"customer": CUSTOMER_DEF})

        sh.write_batch(
            snapshots=[{
                "entity_id": "c_001",
                "entity_type": "customer",
                "state": "active",
                "properties": {"name": "Alice", "mrr": 99.0},
                "valid_from": "2024-01-15T10:00:00Z",
            }],
            tombstones=[],
        )

        count = sh.delete_entity("c_001")
        assert count == 1
        sh.close()

    def test_write_idempotent(self, pg_conninfo):
        """Duplicate (entity_id, valid_from) is silently skipped."""
        from defacto.backends import PostgresStateHistory

        sh = PostgresStateHistory(pg_conninfo, version="dev")
        sh.ensure_tables({"customer": CUSTOMER_DEF})

        snap = {
            "entity_id": "c_001",
            "entity_type": "customer",
            "state": "active",
            "properties": {"name": "Alice", "mrr": 99.0},
            "valid_from": "2024-01-15T10:00:00Z",
        }
        sh.write_batch(snapshots=[snap], tombstones=[])
        sh.write_batch(snapshots=[snap], tombstones=[])  # duplicate — no error
        sh.close()


# ===========================================================================
# End-to-end: Defacto with Postgres
# ===========================================================================


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


class TestDefactoPostgres:
    """Full write path through Postgres backends."""

    def test_ingest_and_build(self, pg_conninfo):
        """Ingest events → build → verify entity state through Postgres."""
        from defacto import Defacto

        m = Defacto(DEFINITIONS, database=pg_conninfo)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
            {"type": "signup", "ts": "2024-01-15T10:01:00Z", "email": "bob@test.com"},
        ])
        result = m.build()

        assert result.events_processed == 2
        assert m._core.entity_count() == 2
        m.close()

    def test_build_incremental(self, pg_conninfo):
        """Two ingests, two builds — second is INCREMENTAL."""
        from defacto import Defacto

        m = Defacto(DEFINITIONS, database=pg_conninfo)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ])
        m.build()

        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T11:00:00Z", "email": "bob@test.com"},
        ])
        result = m.build()

        assert result.mode == "INCREMENTAL"
        assert m._core.entity_count() == 2
        m.close()

    def test_erase(self, pg_conninfo):
        """Erase entity across all Postgres backends."""
        from defacto import Defacto

        m = Defacto(DEFINITIONS, database=pg_conninfo)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ])
        m.build()

        entity_id = m._identity_backend.lookup("customer", "alice@test.com")
        m.erase(entity_id)

        assert m._identity_backend.lookup("customer", "alice@test.com") is None
        assert m._core.entity_count() == 0
        m.close()

    def test_erase_survives_rebuild(self, pg_conninfo):
        """Erased entity does not reappear after a FULL rebuild on Postgres."""
        from defacto import Defacto

        m = Defacto(DEFINITIONS, database=pg_conninfo)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
            {"type": "signup", "ts": "2024-01-15T10:01:00Z", "email": "bob@test.com"},
        ])
        m.build()
        assert m._core.entity_count() == 2

        alice_id = m._identity_backend.lookup("customer", "alice@test.com")
        m.erase(alice_id)
        assert m._core.entity_count() == 1

        # Full rebuild — erased entity stays gone
        m._core.clear()
        m._pipeline.build_full(m._active_version)
        assert m._core.entity_count() == 1
        m.close()

    def test_erase_cascade_through_merge(self, pg_conninfo):
        """Erasing the merge winner cascades to the loser on Postgres."""
        from defacto import Defacto

        m = Defacto(DEFINITIONS, database=pg_conninfo)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
            {"type": "signup", "ts": "2024-01-15T10:01:00Z", "email": "bob@test.com"},
        ])
        m.build()

        alice_id = m._identity_backend.lookup("customer", "alice@test.com")
        bob_id = m._identity_backend.lookup("customer", "bob@test.com")
        m.merge(alice_id, bob_id)
        m.erase(bob_id)

        assert m._core.entity_count() == 0

        # Rebuild — both stay gone
        m._core.clear()
        m._pipeline.build_full(m._active_version)
        assert m._core.entity_count() == 0
        m.close()

    def test_merge_survives_rebuild(self, pg_conninfo):
        """External merge persists through a FULL rebuild on Postgres."""
        from defacto import Defacto

        m = Defacto(DEFINITIONS, database=pg_conninfo)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
            {"type": "signup", "ts": "2024-01-15T10:01:00Z", "email": "bob@test.com"},
        ])
        m.build()
        assert m._core.entity_count() == 2

        alice_id = m._identity_backend.lookup("customer", "alice@test.com")
        bob_id = m._identity_backend.lookup("customer", "bob@test.com")
        m.merge(alice_id, bob_id)
        assert m._core.entity_count() == 1

        # Full rebuild — merge survives
        m._core.clear()
        m._pipeline.build_full(m._active_version)
        assert m._core.entity_count() == 1
        m.close()

    def test_merge_new_events_resolve_to_winner(self, pg_conninfo):
        """After merge on Postgres, new events for loser resolve to winner."""
        from defacto import Defacto

        m = Defacto(DEFINITIONS, database=pg_conninfo)
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
        assert m._core.entity_count() == 1
        m.close()

    def test_concurrent_shard_build(self, pg_conninfo):
        """Two shards building concurrently against shared Postgres.

        Unlike the SQLite version in test_sharding.py (which serializes
        writes via WAL), this exercises true concurrent Postgres I/O:
        parallel identity resolution, parallel ledger reads, parallel
        build state updates.
        """
        from concurrent.futures import ThreadPoolExecutor
        from defacto import Defacto

        # Ingest first (unsharded — populates shared ledger)
        m = Defacto(DEFINITIONS, database=pg_conninfo)
        m.ingest("web", [
            {"type": "signup", "ts": f"2024-01-15T{10 + i}:00:00Z",
             "email": f"user{i}@test.com"}
            for i in range(20)
        ])
        m.build()
        full_count = m._core.entity_count()
        version = m._active_version
        m.close()

        results = {}

        def build_shard(shard_id):
            ms = Defacto(
                DEFINITIONS, database=pg_conninfo,
                shard_id=shard_id, total_shards=2,
            )
            ms.build()
            count = ms._core.entity_count()
            ms.close()
            return count

        with ThreadPoolExecutor(max_workers=2) as pool:
            f0 = pool.submit(build_shard, 0)
            f1 = pool.submit(build_shard, 1)
            results[0] = f0.result()
            results[1] = f1.result()

        # Combined output equals unsharded
        assert results[0] + results[1] == full_count
        assert results[0] > 0
        assert results[1] > 0

    def test_concurrent_identity_resolution(self, pg_conninfo):
        """Two threads resolving overlapping hints concurrently.

        Tests the key-level atomicity claim: INSERT ON CONFLICT handles
        concurrent registration without serialization or corruption.
        Both threads try to create entities for overlapping email hints.
        After both complete, each email should map to exactly one entity.
        """
        from concurrent.futures import ThreadPoolExecutor
        from defacto.backends import PostgresIdentity

        # Create schema first (avoid CREATE SCHEMA race between threads)
        setup = PostgresIdentity(pg_conninfo)
        setup.close()

        # Each thread resolves 50 hints, with 20 overlapping
        shared_hints = [f"shared{i}@test.com" for i in range(20)]
        thread_a_only = [f"thread_a_{i}@test.com" for i in range(30)]
        thread_b_only = [f"thread_b_{i}@test.com" for i in range(30)]

        def resolve_batch(hints, prefix):
            """Each thread opens its own connection — true concurrent I/O."""
            identity = PostgresIdentity(pg_conninfo)
            result = identity.resolve_and_create([
                ("customer", "email", h, f"{prefix}_{h}")
                for h in hints
            ])
            identity.close()
            return result

        with ThreadPoolExecutor(max_workers=2) as pool:
            fa = pool.submit(resolve_batch, shared_hints + thread_a_only, "a")
            fb = pool.submit(resolve_batch, shared_hints + thread_b_only, "b")
            result_a = fa.result()
            result_b = fb.result()

        # Both threads got results for all their hints
        assert len(result_a) == 50
        assert len(result_b) == 50

        # For shared hints, both threads resolved to the SAME entity_id
        # (whichever thread's INSERT won, the other got the existing mapping)
        a_map = {r[1]: r[2] for r in result_a}
        b_map = {r[1]: r[2] for r in result_b}
        for hint in shared_hints:
            assert a_map[hint] == b_map[hint], (
                f"Shared hint {hint} resolved to different entities: "
                f"{a_map[hint]} vs {b_map[hint]}"
            )

        # Verify via a fresh connection — each shared hint has exactly one entity
        verify = PostgresIdentity(pg_conninfo)
        for hint in shared_hints:
            entity_id = verify.lookup("customer", hint)
            assert entity_id is not None
            assert entity_id == a_map[hint]
        verify.close()

    def test_version_activation_visible_across_connections(self, pg_conninfo):
        """Version activation is visible to other connections immediately.

        Tests the claim that activate() updates the database and any
        connection to the same database sees the change. Uses the
        DefinitionsStore directly to read active version without creating
        a full Defacto instance (which would re-activate its own version).
        """
        import copy
        from defacto import Defacto
        from defacto.backends._definition_store import PostgresDefinitionsStore

        # Create instance with initial definitions
        m = Defacto(DEFINITIONS, database=pg_conninfo)
        v1 = m._active_version

        # Register and activate a new version
        modified = copy.deepcopy(DEFINITIONS)
        modified["entities"]["customer"]["properties"]["name"] = {"type": "string"}
        m.definitions.register("v2", modified)
        m.definitions.activate("v2")

        # Read active version from a completely separate connection
        reader = PostgresDefinitionsStore(pg_conninfo)
        active = reader.active_version()
        reader.close()

        assert active == "v2"
        assert active != v1

        m.close()
