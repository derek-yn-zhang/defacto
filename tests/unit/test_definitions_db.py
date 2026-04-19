"""Tests for DefinitionsStore — definition version CRUD."""

import sqlite3

import pytest

from defacto.backends._definition_store import SqliteDefinitionsStore
from defacto.errors import ConfigError, DefinitionError, NotFoundError


def make_store() -> SqliteDefinitionsStore:
    """Create a SqliteDefinitionsStore with an in-memory database."""
    return SqliteDefinitionsStore(":memory:")


SAMPLE_DEFS = {
    "entities": {"customer": {"starts": "lead", "states": {"lead": {}}}},
    "sources": {"web": {"events": {}}},
    "schemas": {},
}

SAMPLE_HASHES = {
    "definition_hash": "def123",
    "source_hash": "src456",
    "identity_hash": "id789",
}


class TestDefinitionsStoreRegister:
    """Registering definition versions."""

    def test_register_and_get(self):
        store = make_store()
        store.register("v1", SAMPLE_DEFS, SAMPLE_HASHES)
        result = store.get("v1")
        assert result == SAMPLE_DEFS

    def test_register_duplicate_raises(self):
        store = make_store()
        store.register("v1", SAMPLE_DEFS, SAMPLE_HASHES)
        with pytest.raises(DefinitionError, match="already exists"):
            store.register("v1", SAMPLE_DEFS, SAMPLE_HASHES)

    def test_register_preserves_complex_definitions(self):
        store = make_store()
        complex_defs = {
            "entities": {
                "customer": {
                    "starts": "lead",
                    "properties": {"mrr": {"type": "float", "default": 0.0}},
                    "states": {"lead": {"when": {"signup": {"effects": ["create"]}}}},
                },
            },
            "sources": {"web": {"events": {"signup": {"fields": {"email": {"from": "email"}}}}}},
            "schemas": {"signup": {"fields": {"email": {"type": "string", "required": True}}}},
        }
        store.register("v1", complex_defs, SAMPLE_HASHES)
        assert store.get("v1") == complex_defs

    def test_table_created_on_init(self):
        store = SqliteDefinitionsStore(":memory:")
        tables = store._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        assert ("definition_versions",) in tables


class TestDefinitionsStoreActivate:
    """Version activation."""

    def test_activate_and_active_version(self):
        store = make_store()
        store.register("v1", SAMPLE_DEFS, SAMPLE_HASHES)
        store.activate("v1")
        assert store.active_version() == "v1"

    def test_activate_deactivates_previous(self):
        store = make_store()
        store.register("v1", SAMPLE_DEFS, SAMPLE_HASHES)
        store.register("v2", SAMPLE_DEFS, SAMPLE_HASHES)
        store.activate("v1")
        store.activate("v2")
        assert store.active_version() == "v2"

    def test_activate_nonexistent_raises(self):
        store = make_store()
        with pytest.raises(NotFoundError, match="not found"):
            store.activate("v999")

    def test_active_version_none_raises(self):
        store = make_store()
        with pytest.raises(ConfigError, match="No active"):
            store.active_version()

    def test_activate_idempotent(self):
        store = make_store()
        store.register("v1", SAMPLE_DEFS, SAMPLE_HASHES)
        store.activate("v1")
        store.activate("v1")  # should not raise
        assert store.active_version() == "v1"


class TestDefinitionsStoreQuery:
    """Reading definitions, versions, and hashes."""

    def test_get_nonexistent_raises(self):
        store = make_store()
        with pytest.raises(NotFoundError):
            store.get("v999")

    def test_versions_empty(self):
        store = make_store()
        assert store.versions() == []

    def test_versions_lists_all(self):
        store = make_store()
        store.register("v1", SAMPLE_DEFS, SAMPLE_HASHES)
        store.register("v2", SAMPLE_DEFS, SAMPLE_HASHES)
        versions = store.versions()
        assert len(versions) == 2
        assert {v["version"] for v in versions} == {"v1", "v2"}

    def test_versions_shows_active_flag(self):
        store = make_store()
        store.register("v1", SAMPLE_DEFS, SAMPLE_HASHES)
        store.register("v2", SAMPLE_DEFS, SAMPLE_HASHES)
        store.activate("v2")
        versions = store.versions()
        active_map = {v["version"]: v["active"] for v in versions}
        assert active_map["v1"] is False
        assert active_map["v2"] is True

    def test_hashes_round_trip(self):
        store = make_store()
        store.register("v1", SAMPLE_DEFS, SAMPLE_HASHES)
        assert store.hashes("v1") == SAMPLE_HASHES

    def test_hashes_nonexistent_raises(self):
        store = make_store()
        with pytest.raises(NotFoundError):
            store.hashes("v999")

    def test_multiple_versions_workflow(self):
        store = make_store()
        store.register("v1", SAMPLE_DEFS, SAMPLE_HASHES)
        store.activate("v1")
        store.register("v2", {"entities": {}}, {"definition_hash": "x", "source_hash": "y", "identity_hash": "z"})
        store.activate("v2")
        assert store.active_version() == "v2"
        assert store.get("v1") == SAMPLE_DEFS
        assert store.get("v2") == {"entities": {}}
