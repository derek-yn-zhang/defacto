"""StateHistoryBackend contract tests.

Every StateHistoryBackend implementation must satisfy these correctness
properties. Tests are parametrized across SQLite and Postgres via the
`state_history` fixture in conftest.py.

Note: current_state() and history() return Ibis expressions, which require
a working Ibis connection. These are tested here via the raw connection
property instead, since the Ibis wiring is a query layer concern (tested
at the Defacto class level), not a backend contract concern.
"""


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _make_snapshot(
    entity_id="customer_001",
    entity_type="customer",
    state="active",
    valid_from="2024-01-15T10:00:00Z",
    **properties,
):
    """Build an entity snapshot dict for write_batch."""
    return {
        "entity_id": entity_id,
        "entity_type": entity_type,
        "state": state,
        "valid_from": valid_from,
        "properties": {"email": "alice@test.com", "plan": "free", **properties},
    }


def _make_tombstone(
    entity_id="customer_001",
    entity_type="customer",
    merged_into="customer_002",
    valid_from="2024-01-15T10:00:00Z",
):
    """Build a tombstone dict for write_batch."""
    return {
        "entity_id": entity_id,
        "entity_type": entity_type,
        "reason": "merge",
        "merged_into": merged_into,
        "valid_from": valid_from,
    }


def _resolve_table(state_history, entity_type="customer"):
    """Resolve the fully qualified table name for a state history backend.

    Backends store tables under different naming conventions:
      SQLite:   {entity_type}_history
      Postgres: defacto_{version}.{entity_type}_history

    Uses the backend's _version attribute to detect schema qualification.
    New backends should either set _version (for schema-qualified) or
    leave it absent (for unqualified).
    """
    version = getattr(state_history, '_version', None)
    if version:
        return f"defacto_{version}.{entity_type}_history"
    return f"{entity_type}_history"


def _query_rows(conn, sql):
    """Execute SQL and return list of dicts.

    Handles the dialect difference between SQLite (conn.execute returns
    cursor with .description), psycopg (needs cursor factory for dict
    rows), and DuckDB (result.fetchall with .description).
    """
    import sqlite3

    if isinstance(conn, sqlite3.Connection):
        cur = conn.execute(sql)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    # DuckDB — check before psycopg since both have .cursor()
    try:
        import duckdb
        if isinstance(conn, duckdb.DuckDBPyConnection):
            result = conn.execute(sql)
            cols = [d[0] for d in result.description]
            return [dict(zip(cols, row)) for row in result.fetchall()]
    except ImportError:
        pass

    import psycopg.rows
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(sql)
        return cur.fetchall()


def _query_history(state_history, entity_type="customer"):
    """Read all rows from the history table via raw SQL."""
    table = _resolve_table(state_history, entity_type)
    return _query_rows(
        state_history.connection,
        f"SELECT * FROM {table} ORDER BY valid_from",
    )


def _current_rows(state_history, entity_type="customer"):
    """Read current-state rows (valid_to IS NULL)."""
    return [r for r in _query_history(state_history, entity_type)
            if r.get("valid_to") is None]


# ---------------------------------------------------------------------------
# ensure_tables
# ---------------------------------------------------------------------------


class TestEnsureTables:
    """ensure_tables: create state history tables from entity definitions."""

    def test_creates_tables(self, state_history):
        """After ensure_tables, the history table exists and is queryable."""
        # The fixture already calls ensure_tables. Verify the table exists
        # by querying it (no rows yet).
        rows = _query_history(state_history)
        assert rows == []


# ---------------------------------------------------------------------------
# write_batch
# ---------------------------------------------------------------------------


class TestWriteBatch:
    """write_batch: SCD Type 2 writes with snapshot/tombstone handling."""

    def test_insert_new_version(self, state_history):
        """First snapshot creates a version row with valid_to = NULL."""
        state_history.write_batch([_make_snapshot()], [])
        rows = _current_rows(state_history)
        assert len(rows) == 1
        assert rows[0]["customer_state"] == "active"

    def test_closes_previous_version(self, state_history):
        """Second snapshot for same entity closes the first (sets valid_to)."""
        state_history.write_batch([_make_snapshot(
            valid_from="2024-01-15T10:00:00Z",
        )], [])
        state_history.write_batch([_make_snapshot(
            state="churned",
            valid_from="2024-01-15T12:00:00Z",
        )], [])

        # Current state: only the latest version
        current = _current_rows(state_history)
        assert len(current) == 1
        assert current[0]["customer_state"] == "churned"

        # History: both versions exist
        all_rows = _query_history(state_history)
        assert len(all_rows) == 2

    def test_idempotent_writes(self, state_history):
        """Writing the same snapshot twice leaves state unchanged.

        This is the Kafka replay scenario — the same message delivered
        twice must not corrupt state. The close operation must not close
        the existing row when the INSERT will be skipped by ON CONFLICT.
        """
        snapshot = _make_snapshot()
        state_history.write_batch([snapshot], [])
        state_history.write_batch([snapshot], [])

        rows = _current_rows(state_history)
        assert len(rows) == 1
        assert rows[0]["customer_state"] == "active"

    def test_tombstone_closes_entity(self, state_history):
        """Tombstone sets merged_into on the entity's current version."""
        state_history.write_batch([_make_snapshot(
            valid_from="2024-01-15T10:00:00Z",
        )], [])
        state_history.write_batch([], [_make_tombstone(
            valid_from="2024-01-15T12:00:00Z",
        )])

        # Entity should no longer appear in current state
        current = _current_rows(state_history)
        assert len(current) == 0

    def test_properties_preserved(self, state_history):
        """Entity properties survive the write→read round trip."""
        state_history.write_batch([_make_snapshot(plan="pro")], [])
        rows = _current_rows(state_history)
        assert rows[0]["plan"] == "pro"
        assert rows[0]["email"] == "alice@test.com"


# ---------------------------------------------------------------------------
# delete_entity
# ---------------------------------------------------------------------------


class TestDeleteEntity:
    """delete_entity: right to erasure on state history."""

    def test_delete_removes_all_versions(self, state_history):
        """delete_entity removes all history rows for an entity."""
        state_history.write_batch([_make_snapshot(
            valid_from="2024-01-15T10:00:00Z",
        )], [])
        state_history.write_batch([_make_snapshot(
            state="churned",
            valid_from="2024-01-15T12:00:00Z",
        )], [])

        count = state_history.delete_entity("customer_001")
        assert count >= 1

        all_rows = _query_history(state_history)
        assert len(all_rows) == 0

    def test_delete_unknown_entity(self, state_history):
        """Deleting an unknown entity returns 0."""
        assert state_history.delete_entity("nonexistent") == 0


# ---------------------------------------------------------------------------
# Timing fields
# ---------------------------------------------------------------------------


def _make_snapshot_with_timing(
    entity_id="customer_001",
    entity_type="customer",
    state="active",
    valid_from="2024-01-15T10:00:00Z",
    last_event_time="2024-01-15T10:00:00+00:00",
    state_entered_time="2024-01-15T10:00:00+00:00",
    created_time="2024-01-15T09:00:00+00:00",
    **properties,
):
    """Build a snapshot dict with timing fields."""
    return {
        "entity_id": entity_id,
        "entity_type": entity_type,
        "state": state,
        "valid_from": valid_from,
        "properties": {"email": "alice@test.com", "plan": "free", **properties},
        "last_event_time": last_event_time,
        "state_entered_time": state_entered_time,
        "created_time": created_time,
    }


class TestTimingFields:
    """Timing fields flow through write_batch and persist in state history."""

    def test_timing_fields_written(self, state_history):
        """Timing fields are stored as columns in the history table."""
        state_history.write_batch(
            [_make_snapshot_with_timing()], [],
        )
        rows = _current_rows(state_history)
        assert len(rows) == 1
        row = rows[0]
        assert row["last_event_time"] is not None
        assert row["state_entered_time"] is not None
        assert row["created_time"] is not None

    def test_timing_fields_nullable(self, state_history):
        """Timing fields are optional — None values are stored as NULL."""
        state_history.write_batch([{
            "entity_id": "customer_001",
            "entity_type": "customer",
            "state": "active",
            "valid_from": "2024-01-15T10:00:00Z",
            "properties": {"email": "alice@test.com", "plan": "free"},
            # No timing fields
        }], [])
        rows = _current_rows(state_history)
        assert len(rows) == 1
        assert rows[0]["last_event_time"] is None
        assert rows[0]["state_entered_time"] is None
        assert rows[0]["created_time"] is None

    def test_timing_fields_updated_on_new_version(self, state_history):
        """When a new snapshot closes the old one, timing fields reflect
        the new state."""
        state_history.write_batch(
            [_make_snapshot_with_timing(
                valid_from="2024-01-15T10:00:00Z",
                last_event_time="2024-01-15T10:00:00+00:00",
                state_entered_time="2024-01-15T10:00:00+00:00",
            )], [],
        )
        state_history.write_batch(
            [_make_snapshot_with_timing(
                state="churned",
                valid_from="2024-01-15T12:00:00Z",
                last_event_time="2024-01-15T12:00:00+00:00",
                state_entered_time="2024-01-15T12:00:00+00:00",
            )], [],
        )
        current = _current_rows(state_history)
        assert len(current) == 1
        # Timing fields should reflect the second snapshot
        row = current[0]
        assert "12:00" in str(row["last_event_time"])
        assert "12:00" in str(row["state_entered_time"])


# ---------------------------------------------------------------------------
# read_current_entities
# ---------------------------------------------------------------------------


class TestReadCurrentEntities:
    """read_current_entities: streaming cold start recovery."""

    def test_reads_current_entities(self, state_history):
        """Returns entities with valid_to IS NULL."""
        state_history.write_batch(
            [_make_snapshot_with_timing(entity_id="c_001")], [],
        )
        state_history.write_batch(
            [_make_snapshot_with_timing(entity_id="c_002", email="bob@test.com")], [],
        )

        entities = list(state_history.read_current_entities(
            "customer", ["email", "plan"],
        ))
        assert len(entities) == 2

        ids = {e["entity_id"] for e in entities}
        assert ids == {"c_001", "c_002"}

    def test_excludes_merged_entities(self, state_history):
        """Merged entities (merged_into IS NOT NULL) are excluded."""
        state_history.write_batch(
            [_make_snapshot_with_timing(
                entity_id="c_001",
                valid_from="2024-01-15T10:00:00Z",
            )], [],
        )
        # Merge c_001 into c_002
        state_history.write_batch([], [_make_tombstone(
            entity_id="c_001",
            merged_into="c_002",
            valid_from="2024-01-15T12:00:00Z",
        )])

        entities = list(state_history.read_current_entities(
            "customer", ["email", "plan"],
        ))
        assert len(entities) == 0

    def test_includes_properties(self, state_history):
        """Properties are returned as a dict in each entity."""
        state_history.write_batch(
            [_make_snapshot_with_timing(plan="pro")], [],
        )
        entities = list(state_history.read_current_entities(
            "customer", ["email", "plan"],
        ))
        assert len(entities) == 1
        assert entities[0]["properties"]["plan"] == "pro"
        assert entities[0]["properties"]["email"] == "alice@test.com"

    def test_includes_timing_fields(self, state_history):
        """Timing fields are included in the returned dicts."""
        state_history.write_batch(
            [_make_snapshot_with_timing()], [],
        )
        entities = list(state_history.read_current_entities(
            "customer", ["email", "plan"],
        ))
        assert len(entities) == 1
        e = entities[0]
        assert e["last_event_time"] is not None
        assert e["state_entered_time"] is not None
        assert e["created_time"] is not None

    def test_entity_type_and_state(self, state_history):
        """Each entity dict includes entity_type and state."""
        state_history.write_batch(
            [_make_snapshot_with_timing(state="churned")], [],
        )
        entities = list(state_history.read_current_entities(
            "customer", ["email", "plan"],
        ))
        assert entities[0]["entity_type"] == "customer"
        assert entities[0]["state"] == "churned"

    def test_empty_table(self, state_history):
        """Returns empty iterator when no entities exist."""
        entities = list(state_history.read_current_entities(
            "customer", ["email", "plan"],
        ))
        assert entities == []

    def test_only_latest_version(self, state_history):
        """Only returns the current version, not closed history rows."""
        state_history.write_batch(
            [_make_snapshot_with_timing(
                valid_from="2024-01-15T10:00:00Z",
            )], [],
        )
        state_history.write_batch(
            [_make_snapshot_with_timing(
                state="churned",
                valid_from="2024-01-15T12:00:00Z",
            )], [],
        )
        entities = list(state_history.read_current_entities(
            "customer", ["email", "plan"],
        ))
        assert len(entities) == 1
        assert entities[0]["state"] == "churned"
