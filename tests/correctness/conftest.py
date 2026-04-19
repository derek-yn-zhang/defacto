"""Parametrized backend fixtures for contract tests.

Each fixture creates a backend instance parametrized across all implementations.
SQLite variants always run (no infrastructure needed). Postgres variants are
marked with @pytest.mark.postgres and skip when Postgres isn't available.

Test output shows which backend is being tested:
    test_dedup[sqlite] PASSED
    test_dedup[postgres] PASSED
    test_dedup[tiered] PASSED
"""

import pytest

from defacto.backends._ledger import SqliteLedger
from defacto.backends._identity import SqliteIdentity
from defacto.backends._state_history import SqliteStateHistory, DuckDBStateHistory
from defacto._build import SqliteBuildStateStore
from tests.conftest import PG_CONNINFO, clean_postgres


# ---------------------------------------------------------------------------
# Ledger backends
# ---------------------------------------------------------------------------


@pytest.fixture(params=[
    pytest.param("sqlite", id="sqlite"),
    pytest.param("postgres", id="postgres", marks=pytest.mark.postgres),
    pytest.param("tiered", id="tiered", marks=pytest.mark.postgres),
])
def ledger(request, tmp_path):
    """A LedgerBackend instance — parametrized across all implementations."""
    if request.param == "sqlite":
        db = SqliteLedger(str(tmp_path / "ledger.db"))
        yield db
        db.close()

    elif request.param == "postgres":
        from defacto.backends._ledger import PostgresLedger
        clean_postgres()
        db = PostgresLedger(PG_CONNINFO)
        yield db
        db.close()

    elif request.param == "tiered":
        from defacto.backends._ledger import PostgresLedger, TieredLedger
        clean_postgres()
        cold_path = str(tmp_path / "cold_ledger")
        db = TieredLedger(PostgresLedger(PG_CONNINFO), cold_path)
        yield db
        db.close()


# ---------------------------------------------------------------------------
# Identity backends
# ---------------------------------------------------------------------------


@pytest.fixture(params=[
    pytest.param("sqlite", id="sqlite"),
    pytest.param("postgres", id="postgres", marks=pytest.mark.postgres),
])
def identity(request, tmp_path):
    """An IdentityBackend instance — parametrized across all implementations."""
    if request.param == "sqlite":
        db = SqliteIdentity(str(tmp_path / "identity.db"))
        yield db
        db.close()

    elif request.param == "postgres":
        from defacto.backends._identity import PostgresIdentity
        clean_postgres()
        db = PostgresIdentity(PG_CONNINFO)
        yield db
        db.close()


# ---------------------------------------------------------------------------
# Build state backends
# ---------------------------------------------------------------------------


@pytest.fixture(params=[
    pytest.param("sqlite", id="sqlite"),
    pytest.param("postgres", id="postgres", marks=pytest.mark.postgres),
])
def build_state(request, tmp_path):
    """A BuildStateStore instance — parametrized across all implementations."""
    if request.param == "sqlite":
        db = SqliteBuildStateStore(str(tmp_path / "build.db"))
        yield db
        db.close()

    elif request.param == "postgres":
        from defacto._build import PostgresBuildStateStore
        clean_postgres()
        db = PostgresBuildStateStore(PG_CONNINFO)
        yield db
        db.close()


# ---------------------------------------------------------------------------
# State history backends
# ---------------------------------------------------------------------------

# Entity definitions used by state history tests — need enough structure
# to generate DDL and write snapshots.
STATE_HISTORY_ENTITY_DEFS = {
    "customer": {
        "starts": "lead",
        "properties": {
            "email": {"type": "string"},
            "plan": {"type": "string", "default": "free"},
        },
        "states": {
            "lead": {},
            "active": {},
        },
    },
}


@pytest.fixture(params=[
    pytest.param("sqlite", id="sqlite"),
    pytest.param("postgres", id="postgres", marks=pytest.mark.postgres),
    pytest.param("duckdb", id="duckdb"),
])
def state_history(request, tmp_path):
    """A StateHistoryBackend instance — parametrized across all implementations."""
    if request.param == "sqlite":
        db = SqliteStateHistory(str(tmp_path / "history.db"))
        db.ensure_tables(STATE_HISTORY_ENTITY_DEFS)
        yield db
        db.close()

    elif request.param == "postgres":
        from defacto.backends._state_history import PostgresStateHistory
        clean_postgres()
        db = PostgresStateHistory(PG_CONNINFO, "test_v1")
        db.ensure_tables(STATE_HISTORY_ENTITY_DEFS)
        yield db
        db.close()

    elif request.param == "duckdb":
        db = DuckDBStateHistory(str(tmp_path / "history.duckdb"), "test_v1")
        db.ensure_tables(STATE_HISTORY_ENTITY_DEFS)
        yield db
        db.close()


# ---------------------------------------------------------------------------
# Shared test data builders
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
        "resolution_hints": resolution_hints or {
            "customer": {"email": "alice@example.com"},
        },
    }
