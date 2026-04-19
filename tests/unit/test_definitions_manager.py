"""Tests for definition management, versioning, and draft API.

Tests the full lifecycle: content-hash versioning, DefinitionsManager,
DefinitionsDraft, version activation, rollback, and consumer version
detection. Uses real DefactoCore + real SQLite — no mocks.
"""

import copy

import pytest

from defacto import Defacto
from defacto._defacto import _content_hash
from defacto.errors import DefinitionError
from defacto.results import BuildResult, RegisterResult, ValidationResult


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
# Content-hash versioning
# ---------------------------------------------------------------------------


class TestContentHash:
    """Content hash produces deterministic, dedup-friendly version names."""

    def test_same_definitions_same_hash(self):
        """Identical definitions produce the same hash."""
        h1 = _content_hash(DEFINITIONS)
        h2 = _content_hash(DEFINITIONS)
        assert h1 == h2

    def test_different_definitions_different_hash(self):
        """Changed definitions produce a different hash."""
        h1 = _content_hash(DEFINITIONS)
        modified = copy.deepcopy(DEFINITIONS)
        modified["entities"]["customer"]["properties"]["name"] = {"type": "string"}
        h2 = _content_hash(modified)
        assert h1 != h2

    def test_hash_is_8_chars(self):
        """Hash is truncated to 8 hex characters."""
        h = _content_hash(DEFINITIONS)
        assert len(h) == 8
        assert all(c in "0123456789abcdef" for c in h)

    def test_from_config_uses_content_hash(self, tmp_path):
        """from_config assigns a content-hash version, not 'dev'."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        assert m._active_version != "dev"
        # Version is an 8-char hex string (content hash of the round-tripped
        # definitions dict, which may differ from the raw input dict).
        assert len(m._active_version) == 8
        assert all(c in "0123456789abcdef" for c in m._active_version)
        m.close()

    def test_reopen_same_definitions_same_version(self, tmp_path):
        """Re-opening with the same definitions reuses the version."""
        db_path = str(tmp_path / "defacto.db")
        m1 = Defacto(DEFINITIONS, database=db_path)
        v1 = m1._active_version
        m1.close()

        m2 = Defacto(DEFINITIONS, database=db_path)
        assert m2._active_version == v1
        m2.close()

    def test_changed_definitions_new_version(self, tmp_path):
        """Changed definitions get a new version on reopen."""
        db_path = str(tmp_path / "defacto.db")
        m1 = Defacto(DEFINITIONS, database=db_path)
        v1 = m1._active_version
        m1.close()

        modified = copy.deepcopy(DEFINITIONS)
        modified["entities"]["customer"]["properties"]["name"] = {"type": "string"}
        m2 = Defacto(modified, database=db_path)
        assert m2._active_version != v1
        m2.close()

    def test_from_config_explicit_version(self, tmp_path):
        """from_config accepts an explicit version name for production."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path, version="v1")
        assert m._active_version == "v1"
        m.close()


# ---------------------------------------------------------------------------
# DefinitionsManager
# ---------------------------------------------------------------------------


class TestDefinitionsManager:
    """m.definitions.* — version listing, registration, activation."""

    def test_versions_lists_registered(self, tmp_path):
        """versions() returns all registered versions."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        versions = m.definitions.versions()
        assert len(versions) == 1
        assert versions[0]["version"] == m._active_version
        assert versions[0]["active"] is True
        m.close()

    def test_active_returns_current(self, tmp_path):
        """active() returns the currently active version name."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        assert m.definitions.active() == m._active_version
        m.close()

    def test_get_returns_definitions(self, tmp_path):
        """get() returns the definitions dict for the active version."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        defs = m.definitions.get()
        assert "entities" in defs
        assert "customer" in defs["entities"]
        m.close()

    def test_register_stores_version(self, tmp_path):
        """register() stores a new version without activating it."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        original_version = m._active_version

        modified = copy.deepcopy(DEFINITIONS)
        modified["entities"]["customer"]["properties"]["name"] = {"type": "string"}

        result = m.definitions.register("v2", modified)
        assert isinstance(result, RegisterResult)
        assert result.version == "v2"
        # Active version unchanged — register doesn't activate
        assert m._active_version == original_version
        # But v2 is now in the versions list
        versions = [v["version"] for v in m.definitions.versions()]
        assert "v2" in versions
        m.close()

    def test_register_predicts_build_mode(self, tmp_path):
        """register() predicts the build mode needed."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        m.build()

        # Change entity definitions only
        modified = copy.deepcopy(DEFINITIONS)
        modified["entities"]["customer"]["properties"]["name"] = {"type": "string"}
        result = m.definitions.register("v2", modified)
        assert result.build_mode == "FULL"
        m.close()

    def test_register_duplicate_raises(self, tmp_path):
        """register() raises if version already exists."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        with pytest.raises(DefinitionError, match="already exists"):
            m.definitions.register(m._active_version, DEFINITIONS)
        m.close()

    def test_activate_switches_version(self, tmp_path):
        """activate() compiles, builds, and switches to the new version."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "a@test.com"},
        ])
        m.build()
        original_version = m._active_version

        # Register and activate a new version with an added property
        modified = copy.deepcopy(DEFINITIONS)
        modified["entities"]["customer"]["properties"]["name"] = {"type": "string"}
        m.definitions.register("v2", modified)
        result = m.definitions.activate("v2")

        assert isinstance(result, BuildResult)
        assert m._active_version == "v2"
        assert m.definitions.active() == "v2"
        assert "name" in m._definitions_dict["entities"]["customer"]["properties"]
        m.close()

    def test_activate_invalidates_query(self, tmp_path):
        """activate() invalidates the lazy query coordinator."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        m.build()
        # Access query to initialize the lazy property
        _ = m._query

        modified = copy.deepcopy(DEFINITIONS)
        modified["entities"]["customer"]["properties"]["name"] = {"type": "string"}
        m.definitions.register("v2", modified)
        m.definitions.activate("v2")

        # Query should be re-created with new version
        assert m._Defacto__query is None  # was invalidated
        m.close()

    def test_rollback(self, tmp_path):
        """activate(old_version) rolls back to a previous version."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "a@test.com"},
        ])
        m.build()
        v1 = m._active_version

        # Switch to v2
        modified = copy.deepcopy(DEFINITIONS)
        modified["entities"]["customer"]["properties"]["name"] = {"type": "string"}
        m.definitions.register("v2", modified)
        m.definitions.activate("v2")
        assert m._active_version == "v2"

        # Roll back to v1
        result = m.definitions.activate(v1)
        assert m._active_version == v1
        assert isinstance(result, BuildResult)
        m.close()


# ---------------------------------------------------------------------------
# DefinitionsDraft
# ---------------------------------------------------------------------------


class TestDefinitionsDraft:
    """Draft API — incremental modifications with per-op validation."""

    @pytest.fixture
    def defacto(self, tmp_path):
        """Create a Defacto instance for draft testing."""
        db_path = str(tmp_path / "defacto.db")
        m = Defacto(DEFINITIONS, database=db_path)
        m.build()
        yield m
        m.close()

    def test_add_property(self, defacto):
        """add_property adds a property and validates."""
        draft = defacto.definitions.draft("v2")
        draft.add_property("customer", "name", type="string")
        defs = draft._definitions
        assert "name" in defs["entities"]["customer"]["properties"]

    def test_add_property_duplicate_raises(self, defacto):
        """add_property raises if property already exists."""
        draft = defacto.definitions.draft("v2")
        with pytest.raises(DefinitionError, match="already exists"):
            draft.add_property("customer", "email", type="string")

    def test_add_property_bad_entity_raises(self, defacto):
        """add_property raises if entity type doesn't exist."""
        draft = defacto.definitions.draft("v2")
        with pytest.raises(DefinitionError, match="not found"):
            draft.add_property("nonexistent", "name", type="string")

    def test_add_state(self, defacto):
        """add_state adds a state to the state machine."""
        draft = defacto.definitions.draft("v2")
        draft.add_state("customer", "suspended")
        assert "suspended" in draft._definitions["entities"]["customer"]["states"]

    def test_add_state_duplicate_raises(self, defacto):
        """add_state raises if state already exists."""
        draft = defacto.definitions.draft("v2")
        with pytest.raises(DefinitionError, match="already exists"):
            draft.add_state("customer", "lead")

    def test_add_transition(self, defacto):
        """add_transition adds a handler with a transition effect."""
        draft = defacto.definitions.draft("v2")
        draft.add_state("customer", "suspended")
        draft.add_transition("customer", "active", "suspend", to="suspended")
        handler = draft._definitions["entities"]["customer"]["states"]["active"]["when"]["suspend"]
        assert handler["effects"] == [{"transition": {"to": "suspended"}}]

    def test_add_transition_duplicate_raises(self, defacto):
        """add_transition raises if handler already exists for the event."""
        draft = defacto.definitions.draft("v2")
        with pytest.raises(DefinitionError, match="already exists"):
            draft.add_transition("customer", "lead", "signup", to="active")

    def test_add_handler(self, defacto):
        """add_handler adds a full handler definition."""
        draft = defacto.definitions.draft("v2")
        draft.add_state("customer", "suspended")
        draft.add_handler(
            "customer", "suspended", "reactivate",
            effects=[{"transition": {"to": "active"}}],
        )
        handler = draft._definitions["entities"]["customer"]["states"]["suspended"]["when"]["reactivate"]
        assert handler["effects"] == [{"transition": {"to": "active"}}]

    def test_add_handler_duplicate_raises(self, defacto):
        """add_handler raises if handler already exists."""
        draft = defacto.definitions.draft("v2")
        with pytest.raises(DefinitionError, match="already exists"):
            draft.add_handler(
                "customer", "lead", "signup",
                effects=[{"transition": {"to": "active"}}],
            )

    def test_update_identity(self, defacto):
        """update_identity replaces identity config."""
        draft = defacto.definitions.draft("v2")
        draft.update_identity("customer", email={"match": "exact", "normalize": "lowercase(value)"})
        identity = draft._definitions["entities"]["customer"]["identity"]
        assert "normalize" in identity["email"]

    def test_remove_property(self, defacto):
        """remove_property removes a property."""
        draft = defacto.definitions.draft("v2")
        draft.remove_property("customer", "plan")
        assert "plan" not in draft._definitions["entities"]["customer"]["properties"]

    def test_remove_property_missing_raises(self, defacto):
        """remove_property raises if property doesn't exist."""
        draft = defacto.definitions.draft("v2")
        with pytest.raises(DefinitionError, match="not found"):
            draft.remove_property("customer", "nonexistent")

    def test_remove_state(self, defacto):
        """remove_state removes a state."""
        draft = defacto.definitions.draft("v2")
        draft.remove_state("customer", "active")
        assert "active" not in draft._definitions["entities"]["customer"]["states"]

    def test_diff_shows_changes(self, defacto):
        """diff() reports added/removed/modified entities and properties."""
        draft = defacto.definitions.draft("v2")
        draft.add_property("customer", "name", type="string")
        result = draft.diff()
        assert "modified" in result
        assert "customer" in result["modified"]
        assert "name" in result["modified"]["customer"]["added_properties"]

    def test_impact_predicts_full(self, defacto):
        """impact() predicts FULL when entity definitions change."""
        draft = defacto.definitions.draft("v2")
        draft.add_property("customer", "name", type="string")
        result = draft.impact()
        assert result["build_mode"] == "FULL"

    def test_impact_predicts_identity_reset(self, defacto):
        """impact() predicts FULL_WITH_IDENTITY_RESET when identity changes."""
        draft = defacto.definitions.draft("v2")
        draft.update_identity("customer", email={"match": "exact", "normalize": "lowercase(value)"})
        result = draft.impact()
        assert result["build_mode"] == "FULL_WITH_IDENTITY_RESET"

    def test_validate_valid(self, defacto):
        """validate() returns valid for a good draft."""
        draft = defacto.definitions.draft("v2")
        draft.add_property("customer", "name", type="string")
        result = draft.validate()
        assert isinstance(result, ValidationResult)
        assert result.valid is True

    def test_register_stores_version(self, defacto):
        """register() validates and stores the draft as a new version."""
        draft = defacto.definitions.draft("v2")
        draft.add_property("customer", "name", type="string")
        result = draft.register()
        assert isinstance(result, RegisterResult)
        assert result.version == "v2"
        # Version is stored but not activated
        assert defacto._active_version != "v2"
        versions = [v["version"] for v in defacto.definitions.versions()]
        assert "v2" in versions

    def test_draft_does_not_mutate_active(self, defacto):
        """Mutations on a draft don't affect the active definitions."""
        original_props = set(defacto._definitions_dict["entities"]["customer"]["properties"])
        draft = defacto.definitions.draft("v2")
        draft.add_property("customer", "name", type="string")
        current_props = set(defacto._definitions_dict["entities"]["customer"]["properties"])
        assert current_props == original_props


# ---------------------------------------------------------------------------
# StateHistoryBackend.set_version
# ---------------------------------------------------------------------------


class TestSetVersion:
    """set_version() switches backend to a new version's tables."""

    def test_set_version_updates_version(self, tmp_path):
        """set_version updates the internal _version attribute."""
        from defacto.backends._state_history import SqliteStateHistory

        db_path = str(tmp_path / "test.db")
        sh = SqliteStateHistory(db_path, "v1")
        assert sh._version == "v1"

        entity_defs = {"customer": {"properties": {"email": {"type": "string"}}}}
        sh.set_version("v2", entity_defs)
        assert sh._version == "v2"
        sh.close()

    def test_set_version_clears_and_rebuilds_cache(self, tmp_path):
        """set_version clears old column caches and rebuilds from new defs."""
        from defacto.backends._state_history import SqliteStateHistory

        db_path = str(tmp_path / "test.db")
        sh = SqliteStateHistory(db_path)

        # First version has email
        sh.ensure_tables({"customer": {"properties": {"email": {"type": "string"}}}})
        assert "email" in sh._table_columns["customer"]

        # Switch to version with different properties
        sh.set_version("v2", {"customer": {"properties": {"name": {"type": "string"}}}})
        assert sh._table_columns["customer"] == ["name"]
        assert "email" not in sh._table_columns.get("customer", [])
        sh.close()

    def test_set_version_removes_old_entity_types(self, tmp_path):
        """set_version with different entity types clears old entries."""
        from defacto.backends._state_history import SqliteStateHistory

        db_path = str(tmp_path / "test.db")
        sh = SqliteStateHistory(db_path)

        sh.ensure_tables({
            "customer": {"properties": {"email": {"type": "string"}}},
            "order": {"properties": {"total": {"type": "number"}}},
        })
        assert "customer" in sh._table_columns
        assert "order" in sh._table_columns

        # Switch to version with only customer
        sh.set_version("v2", {"customer": {"properties": {"email": {"type": "string"}}}})
        assert "customer" in sh._table_columns
        assert "order" not in sh._table_columns
        sh.close()
