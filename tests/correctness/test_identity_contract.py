"""IdentityBackend contract tests.

Every IdentityBackend implementation must satisfy these correctness properties.
Tests are parametrized across SQLite and Postgres via the `identity` fixture
in conftest.py.

Each test exercises one method or property from the IdentityBackend ABC.
"""


# ---------------------------------------------------------------------------
# resolve_and_create
# ---------------------------------------------------------------------------


class TestResolveAndCreate:
    """resolve_and_create: create new mappings, return existing ones."""

    def test_creates_new_entity(self, identity):
        """New hint creates a mapping with the candidate entity_id."""
        result = identity.resolve_and_create([
            ("customer", "email", "alice@test.com", "cust_001"),
        ])
        assert len(result) == 1
        assert result[0] == ("customer", "alice@test.com", "cust_001")

    def test_returns_existing_entity(self, identity):
        """Known hint returns existing entity_id, ignores candidate."""
        identity.resolve_and_create([
            ("customer", "email", "alice@test.com", "cust_001"),
        ])
        result = identity.resolve_and_create([
            ("customer", "email", "alice@test.com", "cust_999"),
        ])
        # Should return cust_001 (existing), not cust_999 (candidate)
        assert result[0][2] == "cust_001"

    def test_scoped_by_entity_type(self, identity):
        """Same hint value for different entity types creates separate entities."""
        identity.resolve_and_create([
            ("customer", "email", "alice@test.com", "cust_001"),
            ("fraud_case", "email", "alice@test.com", "fraud_001"),
        ])
        # Both should exist independently
        assert identity.lookup("customer", "alice@test.com") == "cust_001"
        assert identity.lookup("fraud_case", "alice@test.com") == "fraud_001"

    def test_multiple_hints_one_call(self, identity):
        """Multiple hints resolved in a single call."""
        result = identity.resolve_and_create([
            ("customer", "email", "alice@test.com", "cust_001"),
            ("customer", "phone", "555-1234", "cust_002"),
            ("customer", "email", "bob@test.com", "cust_003"),
        ])
        assert len(result) == 3
        # Each hint has a mapping
        entity_ids = {r[2] for r in result}
        assert len(entity_ids) == 3


# ---------------------------------------------------------------------------
# merge
# ---------------------------------------------------------------------------


class TestMerge:
    """merge: reassign all hints from one entity to another."""

    def test_merge_reassigns_hints(self, identity):
        """After merge, loser's hints point to survivor."""
        identity.resolve_and_create([
            ("customer", "email", "alice@test.com", "cust_001"),
            ("customer", "phone", "555-1234", "cust_002"),
        ])
        identity.merge("cust_002", "cust_001")
        # Phone now points to cust_001
        assert identity.lookup("customer", "555-1234") == "cust_001"
        assert identity.lookup("customer", "alice@test.com") == "cust_001"

    def test_merge_idempotent(self, identity):
        """Merging the same pair twice doesn't error."""
        identity.resolve_and_create([
            ("customer", "email", "alice@test.com", "cust_001"),
            ("customer", "phone", "555-1234", "cust_002"),
        ])
        identity.merge("cust_002", "cust_001")
        identity.merge("cust_002", "cust_001")  # no-op, no error
        assert identity.lookup("customer", "555-1234") == "cust_001"


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------


class TestDelete:
    """delete: remove all mappings for an entity (right to erasure)."""

    def test_delete_removes_mappings(self, identity):
        """After delete, entity's hints resolve to None."""
        identity.resolve_and_create([
            ("customer", "email", "alice@test.com", "cust_001"),
            ("customer", "phone", "555-1234", "cust_001"),
        ])
        count = identity.delete("cust_001")
        assert count == 2
        assert identity.lookup("customer", "alice@test.com") is None
        assert identity.lookup("customer", "555-1234") is None

    def test_delete_returns_zero_for_unknown(self, identity):
        """Deleting a nonexistent entity returns 0."""
        assert identity.delete("unknown_entity") == 0


# ---------------------------------------------------------------------------
# lookup
# ---------------------------------------------------------------------------


class TestLookup:
    """lookup: find entity by (entity_type, hint_value)."""

    def test_lookup_found(self, identity):
        """Known hint returns entity_id."""
        identity.resolve_and_create([
            ("customer", "email", "alice@test.com", "cust_001"),
        ])
        assert identity.lookup("customer", "alice@test.com") == "cust_001"

    def test_lookup_not_found(self, identity):
        """Unknown hint returns None."""
        assert identity.lookup("customer", "unknown@test.com") is None

    def test_lookup_wrong_entity_type(self, identity):
        """Hint exists but for a different entity type → None."""
        identity.resolve_and_create([
            ("customer", "email", "alice@test.com", "cust_001"),
        ])
        assert identity.lookup("fraud_case", "alice@test.com") is None


# ---------------------------------------------------------------------------
# lookup_any and hints_for_entity
# ---------------------------------------------------------------------------


class TestInspection:
    """lookup_any and hints_for_entity: debugging/inspection methods."""

    def test_lookup_any_across_types(self, identity):
        """lookup_any finds the entity regardless of entity_type."""
        identity.resolve_and_create([
            ("customer", "email", "alice@test.com", "cust_001"),
        ])
        assert identity.lookup_any("alice@test.com") == "cust_001"

    def test_lookup_any_not_found(self, identity):
        """lookup_any returns None for unknown hint."""
        assert identity.lookup_any("unknown") is None

    def test_hints_for_entity(self, identity):
        """hints_for_entity returns all hints linked to an entity."""
        identity.resolve_and_create([
            ("customer", "email", "alice@test.com", "cust_001"),
            ("customer", "phone", "555-1234", "cust_001"),
        ])
        hints = identity.hints_for_entity("cust_001")
        assert hints["email"] == ["alice@test.com"]
        assert hints["phone"] == ["555-1234"]

    def test_hints_for_unknown_entity(self, identity):
        """hints_for_entity returns empty dict for unknown entity."""
        assert identity.hints_for_entity("unknown") == {}


# ---------------------------------------------------------------------------
# warmup and reset
# ---------------------------------------------------------------------------


class TestWarmupReset:
    """warmup and reset: bulk operations for startup and identity reset."""

    def test_warmup_returns_all_mappings(self, identity):
        """warmup returns every (entity_type, hint_value, entity_id) triple."""
        identity.resolve_and_create([
            ("customer", "email", "alice@test.com", "cust_001"),
            ("customer", "phone", "555-1234", "cust_002"),
        ])
        mappings = identity.warmup()
        assert len(mappings) == 2
        # All mappings present
        hint_values = {m[1] for m in mappings}
        assert "alice@test.com" in hint_values
        assert "555-1234" in hint_values

    def test_warmup_empty(self, identity):
        """warmup on empty backend returns empty list."""
        assert identity.warmup() == []

    def test_reset_clears_everything(self, identity):
        """reset removes all identity data."""
        identity.resolve_and_create([
            ("customer", "email", "alice@test.com", "cust_001"),
        ])
        identity.reset()
        assert identity.lookup("customer", "alice@test.com") is None
        assert identity.warmup() == []
