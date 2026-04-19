"""Query layer contract tests.

Every state history backend must be queryable via DefactoQuery. Tests
are parametrized across SQLite, Postgres, and DuckDB. Writes data via
the state history backend, then queries it back via DefactoQuery.

This catches backend-specific issues like Ibis connection handling,
schema qualification, and column type mapping.
"""

import pytest

from defacto.query._query import DefactoQuery
from tests.correctness.conftest import STATE_HISTORY_ENTITY_DEFS


def _make_snapshot(
    entity_id="c_001",
    state="active",
    valid_from="2024-01-15T10:00:00Z",
    **props,
):
    """Build a snapshot dict."""
    return {
        "entity_id": entity_id,
        "entity_type": "customer",
        "state": state,
        "valid_from": valid_from,
        "properties": {"email": "alice@test.com", "plan": "free", **props},
    }


# ---------------------------------------------------------------------------
# Fixtures — state_history + DefactoQuery paired per backend
# ---------------------------------------------------------------------------


@pytest.fixture(params=[
    pytest.param("sqlite", id="sqlite"),
    pytest.param("postgres", id="postgres", marks=pytest.mark.postgres),
    pytest.param("duckdb", id="duckdb"),
])
def query_env(request, tmp_path):
    """Yield (state_history, defacto_query) paired for each backend.

    Writes go through state_history, reads go through DefactoQuery.
    """
    from tests.conftest import PG_CONNINFO, clean_postgres

    if request.param == "sqlite":
        from defacto.backends._state_history import SqliteStateHistory

        db_path = str(tmp_path / "query_test.db")
        sh = SqliteStateHistory(db_path)
        sh.ensure_tables(STATE_HISTORY_ENTITY_DEFS)
        q = DefactoQuery(db_path, {"entities": STATE_HISTORY_ENTITY_DEFS}, "")
        yield sh, q
        q.close()
        sh.close()

    elif request.param == "postgres":
        from defacto.backends._state_history import PostgresStateHistory

        clean_postgres()
        sh = PostgresStateHistory(PG_CONNINFO, "test_v1")
        sh.ensure_tables(STATE_HISTORY_ENTITY_DEFS)
        q = DefactoQuery(
            PG_CONNINFO,
            {"entities": STATE_HISTORY_ENTITY_DEFS},
            "test_v1",
        )
        yield sh, q
        q.close()
        sh.close()

    elif request.param == "duckdb":
        from defacto.backends._state_history import DuckDBStateHistory

        db_path = str(tmp_path / "query_test.duckdb")
        sh = DuckDBStateHistory(db_path, "test_v1")
        sh.ensure_tables(STATE_HISTORY_ENTITY_DEFS)
        q = DefactoQuery(
            f"duckdb:///{db_path}",
            {"entities": STATE_HISTORY_ENTITY_DEFS},
            "test_v1",
        )
        yield sh, q
        q.close()
        sh.close()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestTable:
    """table(): current state query."""

    def test_returns_current_entities(self, query_env):
        """table() returns only entities with valid_to IS NULL."""
        sh, q = query_env
        sh.write_batch([_make_snapshot()], [])

        df = q.table("customer").execute()
        assert len(df) == 1
        assert df["customer_state"].iloc[0] == "active"

    def test_filters_closed_versions(self, query_env):
        """Only current version returned, not closed ones."""
        sh, q = query_env
        sh.write_batch([_make_snapshot(
            valid_from="2024-01-15T10:00:00Z",
        )], [])
        sh.write_batch([_make_snapshot(
            state="churned",
            valid_from="2024-01-15T12:00:00Z",
        )], [])

        df = q.table("customer").execute()
        assert len(df) == 1
        assert df["customer_state"].iloc[0] == "churned"

    def test_properties_queryable(self, query_env):
        """Entity properties are accessible as columns."""
        sh, q = query_env
        sh.write_batch([_make_snapshot(plan="pro")], [])

        df = q.table("customer").execute()
        assert df["plan"].iloc[0] == "pro"
        assert df["email"].iloc[0] == "alice@test.com"

    def test_empty_table(self, query_env):
        """Empty table returns empty DataFrame."""
        _, q = query_env
        df = q.table("customer").execute()
        assert len(df) == 0


class TestHistory:
    """history(): full SCD Type 2 history."""

    def test_returns_all_versions(self, query_env):
        """history() returns both current and closed versions."""
        sh, q = query_env
        sh.write_batch([_make_snapshot(
            valid_from="2024-01-15T10:00:00Z",
        )], [])
        sh.write_batch([_make_snapshot(
            state="churned",
            valid_from="2024-01-15T12:00:00Z",
        )], [])

        df = q.history("customer").execute()
        assert len(df) == 2
        states = set(df["customer_state"])
        assert states == {"active", "churned"}
