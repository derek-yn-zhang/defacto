"""Shared test fixtures and marker auto-skip logic."""

import pytest

# Connection string for all Postgres-dependent tests (integration, correctness).
# Matches docker-compose.yml: postgres:16, db=defacto_test, user=test, pass=test.
PG_CONNINFO = "postgresql://test:test@localhost:5432/defacto_test"


def clean_postgres():
    """Drop all defacto schemas — clean slate for Postgres tests.

    Shared by integration/ and correctness/ conftest fixtures. Terminates
    leftover connections and drops all defacto-prefixed schemas.
    """
    import psycopg
    conn = psycopg.connect(PG_CONNINFO, autocommit=True)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pg_terminate_backend(pid)"
            " FROM pg_stat_activity"
            " WHERE datname = 'defacto_test' AND pid <> pg_backend_pid()"
        )
        cur.execute(
            "SELECT schema_name FROM information_schema.schemata"
            " WHERE schema_name LIKE 'defacto%'"
        )
        schemas = [row[0] for row in cur.fetchall()]
        for schema in schemas:
            cur.execute(f"DROP SCHEMA {schema} CASCADE")
    conn.close()


def pytest_configure(config):
    """Register custom markers."""
    for marker in ["postgres", "neo4j", "minio", "kafka", "cloud"]:
        config.addinivalue_line("markers", f"{marker}: requires {marker} service")


def pytest_collection_modifyitems(config, items):
    """Auto-skip tests that require unavailable services."""
    skip_checks = {
        "postgres": _check_postgres,
        "neo4j": _check_neo4j,
        "minio": _check_minio,
        "kafka": _check_kafka,
        "cloud": _check_cloud,
    }

    for item in items:
        for marker_name, check_fn in skip_checks.items():
            if marker_name in item.keywords and not check_fn():
                item.add_marker(pytest.mark.skip(
                    reason=f"{marker_name} not available — run: docker compose up -d {marker_name}"
                ))


def _check_postgres() -> bool:
    try:
        import psycopg
        conn = psycopg.connect("postgresql://test:test@localhost:5432/defacto_test", connect_timeout=2)
        conn.close()
        return True
    except Exception:
        return False


def _check_neo4j() -> bool:
    try:
        from neo4j import GraphDatabase
        driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "defacto_test"))
        driver.verify_connectivity()
        driver.close()
        return True
    except Exception:
        return False


def _check_minio() -> bool:
    try:
        import boto3
        s3 = boto3.client("s3",
            endpoint_url="http://localhost:9000",
            aws_access_key_id="defacto",
            aws_secret_access_key="defactotest",
        )
        s3.list_buckets()
        return True
    except Exception:
        return False


def _check_kafka() -> bool:
    try:
        from confluent_kafka.admin import AdminClient
        admin = AdminClient({"bootstrap.servers": "localhost:9092"})
        admin.list_topics(timeout=2)
        return True
    except Exception:
        return False


def _check_cloud() -> bool:
    import os
    return os.environ.get("DEFACTO_CLOUD_TESTS") == "1"
