"""End-to-end production mode test: Defacto → Kafka → Consumer → Postgres.

The full production path:
1. Defacto.from_config with kafka config → uses KafkaPublisher
2. ingest + build → snapshots published to Kafka
3. DefactoConsumer reads from Kafka, writes to Postgres state history
4. Verify state history rows in Postgres

Requires both Kafka AND Postgres: docker compose up -d postgres kafka
Tests auto-skip if either service is unavailable.
"""

import uuid

import psycopg
import pytest

pytestmark = [pytest.mark.kafka, pytest.mark.postgres]

PG_CONNINFO = "postgresql://test:test@localhost:5432/defacto_test"
KAFKA_BOOTSTRAP = "localhost:9092"

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


@pytest.fixture(autouse=True)
def clean_schemas():
    """Drop all defacto schemas before each test."""
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
        for row in cur.fetchall():
            cur.execute(f"DROP SCHEMA {row[0]} CASCADE")
    conn.close()


class TestProductionEndToEnd:
    """Full production loop: ingest → Kafka → consumer → Postgres state history."""

    def test_ingest_publish_consume_verify(self):
        """Ingest events with KafkaPublisher, consume with DefactoConsumer,
        verify state history rows exist in Postgres."""
        from defacto import Defacto

        topic = f"defacto-e2e-{uuid.uuid4().hex[:8]}"

        # ── Producer side: Defacto with KafkaPublisher ──
        m = Defacto(
            DEFINITIONS,
            database=PG_CONNINFO,
            kafka={
                "bootstrap.servers": KAFKA_BOOTSTRAP,
                "topic": topic,
            },
        )

        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
            {"type": "signup", "ts": "2024-01-15T10:01:00Z", "email": "bob@test.com"},
        ])
        result = m.build()
        assert result.events_processed == 2

        # Flush Kafka producer to ensure all messages are sent
        m._publisher.close()

        # ── Consumer side: DefactoConsumer → Postgres state history ──
        consumer = Defacto.consumer(
            kafka={
                "bootstrap.servers": KAFKA_BOOTSTRAP,
                "topic": topic,
                "group.id": f"e2e-test-{uuid.uuid4().hex[:8]}",
                "session.timeout.ms": 6000,
                "heartbeat.interval.ms": 1000,
            },
            database=PG_CONNINFO,
            store=PG_CONNINFO,
            batch_timeout_ms=3000,
        )

        consume_result = consumer.run_once()
        assert consume_result.snapshots_processed == 2
        assert consume_result.batches_written == 1

        # ── Verify: state history rows in Postgres ──
        version = m._active_version
        conn = psycopg.connect(PG_CONNINFO)
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT customer_id, customer_state, email"
                f" FROM defacto_{version}.customer_history"
                f" WHERE valid_to IS NULL"
                f" ORDER BY email"
            )
            rows = cur.fetchall()

        assert len(rows) == 2
        assert rows[0][2] == "alice@test.com"
        assert rows[0][1] == "lead"
        assert rows[1][2] == "bob@test.com"
        assert rows[1][1] == "lead"

        conn.close()
        consumer.close()
        m.close()

    def test_ingest_upgrade_consume_scd(self):
        """Ingest signup + upgrade, consume, verify SCD Type 2 versioning."""
        from defacto import Defacto

        topic = f"defacto-e2e-{uuid.uuid4().hex[:8]}"

        m = Defacto(
            DEFINITIONS,
            database=PG_CONNINFO,
            kafka={
                "bootstrap.servers": KAFKA_BOOTSTRAP,
                "topic": topic,
            },
        )

        # First batch: signup
        m.ingest("web", [
            {"type": "signup", "ts": "2024-01-15T10:00:00Z", "email": "alice@test.com"},
        ])
        m.build()
        m._publisher._producer.flush(timeout=10)

        # Second batch: upgrade
        m.ingest("web", [
            {"type": "upgrade", "ts": "2024-01-16T10:00:00Z",
             "email": "alice@test.com", "plan": "pro"},
        ])
        m.build()
        m._publisher._producer.flush(timeout=10)

        # Consume — each run_once processes one batch at a time.
        # Two separate write_batch calls ensure SCD Type 2 versioning
        # works correctly (close old version, then insert new).
        consumer = Defacto.consumer(
            kafka={
                "bootstrap.servers": KAFKA_BOOTSTRAP,
                "topic": topic,
                "group.id": f"e2e-test-{uuid.uuid4().hex[:8]}",
                "session.timeout.ms": 6000,
                "heartbeat.interval.ms": 1000,
            },
            database=PG_CONNINFO,
            store=PG_CONNINFO,
            batch_size=100,
            batch_timeout_ms=3000,
        )

        total_snaps = 0
        for _ in range(5):
            r = consumer.run_once()
            total_snaps += r.snapshots_processed
            if total_snaps >= 2:
                break

        assert total_snaps >= 2

        # Verify SCD Type 2: should have 2 rows for alice
        # (lead version closed, active version current)
        version = m._active_version
        conn = psycopg.connect(PG_CONNINFO)
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT customer_state, valid_to IS NULL as is_current"
                f" FROM defacto_{version}.customer_history"
                f" ORDER BY valid_from"
            )
            rows = cur.fetchall()

        assert len(rows) == 2
        assert rows[0][0] == "lead"
        assert rows[0][1] is False  # closed
        assert rows[1][0] == "active"
        assert rows[1][1] is True  # current

        conn.close()
        consumer.close()
        m.close()
