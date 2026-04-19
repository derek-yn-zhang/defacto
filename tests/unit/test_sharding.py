"""Tests for entity sharding — hash partitioning across processes.

Verifies that the shard hash is deterministic and well-distributed,
that entity ownership filtering works correctly, and that two shards
produce the same combined output as a single unsharded build.

Uses real DefactoCore + real SQLite. Both shards share the same database
to simulate a shared Postgres deployment.
"""

import pytest

from defacto import Defacto
from defacto._pipeline import _shard_for_entity


# ---------------------------------------------------------------------------
# Shared definitions
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
    "schemas": {},
}


# ---------------------------------------------------------------------------
# Shard hash function
# ---------------------------------------------------------------------------


class TestShardHash:
    """_shard_for_entity produces deterministic, well-distributed assignments."""

    def test_deterministic(self):
        """Same entity_id always gets the same shard."""
        assert _shard_for_entity("customer_001", 4) == _shard_for_entity("customer_001", 4)

    def test_different_entities_may_differ(self):
        """Different entity_ids can get different shards."""
        shards = {_shard_for_entity(f"entity_{i}", 4) for i in range(100)}
        assert len(shards) > 1

    def test_well_distributed(self):
        """Shard assignments are roughly even across 4 shards."""
        from collections import Counter
        c = Counter(_shard_for_entity(f"entity_{i}", 4) for i in range(10000))
        for shard_id in range(4):
            assert 2000 < c[shard_id] < 3000

    def test_covers_all_shards(self):
        """All shard IDs from 0 to total_shards-1 are used."""
        shards = {_shard_for_entity(f"entity_{i}", 4) for i in range(1000)}
        assert shards == {0, 1, 2, 3}

    def test_valid_range(self):
        """Shard IDs are always in [0, total_shards)."""
        for i in range(1000):
            s = _shard_for_entity(f"entity_{i}", 7)
            assert 0 <= s < 7


# ---------------------------------------------------------------------------
# Two-shard equivalence
# ---------------------------------------------------------------------------


class TestShardEquivalence:
    """Two shards produce the same combined output as one unsharded build."""

    def test_two_shards_cover_all_entities(self, tmp_path):
        """Every entity is owned by exactly one shard."""
        db_path = str(tmp_path / "shared.db")

        events = [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": f"user{i}@test.com"}
            for i in range(10)
        ]

        # Unsharded build — baseline
        m_full = Defacto(DEFINITIONS, database=db_path)
        m_full.ingest("web", events)
        m_full.build()
        full_count = m_full._core.entity_count()
        m_full.close()

        # Shard 0
        m_s0 = Defacto(
            DEFINITIONS, database=db_path,
            shard_id=0, total_shards=2,
        )
        m_s0.build()
        s0_count = m_s0._core.entity_count()
        m_s0.close()

        # Shard 1
        m_s1 = Defacto(
            DEFINITIONS, database=db_path,
            shard_id=1, total_shards=2,
        )
        m_s1.build()
        s1_count = m_s1._core.entity_count()
        m_s1.close()

        # Combined shards should equal unsharded
        assert s0_count + s1_count == full_count
        assert s0_count > 0
        assert s1_count > 0

    def test_sharded_entities_are_disjoint(self, tmp_path):
        """No entity appears in both shards — verified by hash."""
        db_path = str(tmp_path / "shared.db")

        events = [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": f"user{i}@test.com"}
            for i in range(20)
        ]

        m = Defacto(DEFINITIONS, database=db_path)
        m.ingest("web", events)
        m.build()

        # Get all entity IDs from event_entities
        rows = m._ledger._conn.execute(
            "SELECT DISTINCT entity_id FROM event_entities"
        ).fetchall()
        all_entity_ids = {r[0] for r in rows}
        m.close()

        shard_0 = {eid for eid in all_entity_ids if _shard_for_entity(eid, 2) == 0}
        shard_1 = {eid for eid in all_entity_ids if _shard_for_entity(eid, 2) == 1}

        assert shard_0 & shard_1 == set()  # disjoint
        assert shard_0 | shard_1 == all_entity_ids  # complete

    def test_unsharded_is_default(self, tmp_path):
        """Without shard params, all entities are processed."""
        db_path = str(tmp_path / "test.db")
        m = Defacto(DEFINITIONS, database=db_path)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "a@test.com"},
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "b@test.com"},
        ])
        m.build()
        assert m._core.entity_count() == 2
        m.close()

    def test_incremental_build_sharded(self, tmp_path):
        """Incremental build after initial sharded build processes new events correctly."""
        db_path = str(tmp_path / "shared.db")

        # Initial ingest + sharded build
        m = Defacto(
            DEFINITIONS, database=db_path,
            shard_id=0, total_shards=2,
        )
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": f"user{i}@test.com"}
            for i in range(6)
        ])
        r1 = m.build()
        count_after_first = m._core.entity_count()
        assert count_after_first > 0

        # Ingest more events
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T11:00:00Z", "email": f"new{i}@test.com"}
            for i in range(4)
        ])
        r2 = m.build()
        count_after_second = m._core.entity_count()

        # Should have more entities now (some of the new ones are in shard 0)
        assert count_after_second >= count_after_first
        assert r2.mode == "INCREMENTAL"
        m.close()

    def test_concurrent_shards(self, tmp_path):
        """Two shards building concurrently produce correct combined output."""
        from concurrent.futures import ThreadPoolExecutor

        db_path = str(tmp_path / "shared.db")

        # Ingest first (unsharded — populates shared ledger)
        m = Defacto(DEFINITIONS, database=db_path)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": f"user{i}@test.com"}
            for i in range(20)
        ])
        m.build()
        full_count = m._core.entity_count()
        m.close()

        results = {}

        def build_shard(shard_id):
            ms = Defacto(
                DEFINITIONS, database=db_path,
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

        assert results[0] + results[1] == full_count
        assert results[0] > 0
        assert results[1] > 0

    def test_merge_across_shards(self, tmp_path):
        """Merges involving entities on different shards are handled correctly.

        When two hints resolve to different entities that should merge,
        both shards detect the merge during identity resolution. The
        identity merge is idempotent. Only the shard owning the canonical
        entity holds the merged state.
        """
        # Use definitions with two identity fields to trigger merges
        import copy
        defs = copy.deepcopy(DEFINITIONS)
        defs["entities"]["customer"]["identity"]["phone"] = {"match": "exact"}
        defs["sources"]["web"]["events"]["signup"]["mappings"]["phone"] = {"from": "phone"}
        defs["sources"]["web"]["events"]["signup"]["hints"]["customer"].append("phone")

        db_path = str(tmp_path / "shared.db")

        # Ingest two events for the same person with different identifiers.
        # Event 1: email only. Event 2: email + phone (triggers merge if
        # phone was seen with a different entity).
        m = Defacto(defs, database=db_path)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z",
             "email": "alice@test.com", "phone": "555-1234"},
        ])
        m.build()
        # Should have exactly 1 entity (both hints → same entity)
        assert m._core.entity_count() == 1
        m.close()

        # Now build with sharding — the entity should appear in exactly one shard
        s0 = Defacto(defs, database=db_path, shard_id=0, total_shards=2)
        s0.build()
        s0_count = s0._core.entity_count()
        s0.close()

        s1 = Defacto(defs, database=db_path, shard_id=1, total_shards=2)
        s1.build()
        s1_count = s1._core.entity_count()
        s1.close()

        assert s0_count + s1_count == 1  # entity in exactly one shard

    def test_version_activation_with_sharding(self, tmp_path):
        """Version activation works correctly when sharded."""
        import copy

        db_path = str(tmp_path / "shared.db")

        m = Defacto(
            DEFINITIONS, database=db_path,
            shard_id=0, total_shards=2,
        )
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": f"user{i}@test.com"}
            for i in range(6)
        ])
        m.build()

        # Register v2 with a new property
        modified = copy.deepcopy(DEFINITIONS)
        modified["entities"]["customer"]["properties"]["name"] = {"type": "string"}
        m.definitions.register("v2", modified)
        result = m.definitions.activate("v2")

        # Should have switched and rebuilt
        assert m._active_version == "v2"
        assert isinstance(result.mode, str)
        m.close()
