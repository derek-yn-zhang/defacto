"""Integration test fixtures for Postgres backends.

Provides a clean Postgres database for each test by dropping all defacto
schemas before each test runs. Tests use @pytest.mark.postgres — the
root conftest auto-skips if Postgres isn't available.
"""

import pytest

from tests.conftest import PG_CONNINFO, clean_postgres


@pytest.fixture
def pg_conninfo():
    """Postgres connection string for backend constructors."""
    return PG_CONNINFO


@pytest.fixture(autouse=True)
def clean_schemas():
    """Drop all defacto schemas before each test — clean slate."""
    clean_postgres()
