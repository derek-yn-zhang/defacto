"""Integration tests for KafkaPublisher + DefactoConsumer.

Requires Kafka running: docker compose up -d kafka
Tests auto-skip via pytest.mark.kafka if Kafka isn't available.
"""

import json
import time
import uuid

import pytest

pytestmark = pytest.mark.kafka

KAFKA_BOOTSTRAP = "localhost:9092"


@pytest.fixture
def topic():
    """Unique topic per test to avoid cross-test interference."""
    return f"defacto-test-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def publisher(topic):
    """KafkaPublisher connected to the test topic."""
    from defacto._publisher import KafkaPublisher

    pub = KafkaPublisher({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "topic": topic,
    })
    yield pub
    pub.close()


@pytest.fixture
def consumer_raw(topic):
    """Raw confluent_kafka Consumer for verifying published messages."""
    from confluent_kafka import Consumer

    c = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": f"test-verify-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "session.timeout.ms": 6000,
        "heartbeat.interval.ms": 1000,
    })
    c.subscribe([topic])
    # Wait for partition assignment before returning
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        c.poll(timeout=0.2)
        if c.assignment():
            break
    yield c
    c.close()


def _drain(consumer, timeout=3.0, max_messages=100):
    """Poll all available messages from a consumer."""
    messages = []
    deadline = time.monotonic() + timeout
    while len(messages) < max_messages:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        msg = consumer.poll(timeout=remaining)
        if msg is None:
            break
        if msg.error():
            continue
        messages.append(msg)
    return messages


# ---------------------------------------------------------------------------
# KafkaPublisher tests
# ---------------------------------------------------------------------------


class TestKafkaPublisher:
    """KafkaPublisher produces messages with correct keys and headers."""

    def test_publish_snapshot(self, publisher, consumer_raw, topic):
        """Snapshot is published with entity_id key and 'snapshot' header."""
        publisher.publish(
            [{"entity_id": "cust_001", "entity_type": "customer",
              "state": "active", "properties": {"mrr": 99.0},
              "valid_from": "2026-04-15T10:00:00Z"}],
            [],
        )
        publisher._producer.flush(timeout=10)

        messages = _drain(consumer_raw)
        assert len(messages) == 1

        msg = messages[0]
        assert msg.key() == b"cust_001"

        headers = dict(msg.headers())
        assert headers["type"] == b"snapshot"

        payload = json.loads(msg.value())
        assert payload["entity_id"] == "cust_001"
        assert payload["state"] == "active"
        assert "type" not in payload  # no transport marker in payload

    def test_publish_tombstone(self, publisher, consumer_raw, topic):
        """Tombstone is published with entity_id key and 'tombstone' header."""
        publisher.publish(
            [],
            [{"entity_id": "cust_002", "entity_type": "customer",
              "merged_into": "cust_001",
              "timestamp": "2026-04-15T10:00:00Z"}],
        )
        publisher._producer.flush(timeout=10)

        messages = _drain(consumer_raw)
        assert len(messages) == 1

        msg = messages[0]
        assert msg.key() == b"cust_002"

        headers = dict(msg.headers())
        assert headers["type"] == b"tombstone"

        payload = json.loads(msg.value())
        assert payload["entity_id"] == "cust_002"
        assert payload["merged_into"] == "cust_001"
        assert "type" not in payload  # no transport marker in payload

    def test_publish_mixed_batch(self, publisher, consumer_raw, topic):
        """Snapshots and tombstones in one batch produce correct messages."""
        publisher.publish(
            [{"entity_id": "c1", "entity_type": "customer",
              "state": "active", "properties": {},
              "valid_from": "2026-04-15T10:00:00Z"},
             {"entity_id": "c2", "entity_type": "customer",
              "state": "trial", "properties": {},
              "valid_from": "2026-04-15T10:00:00Z"}],
            [{"entity_id": "c3", "entity_type": "customer",
              "merged_into": "c1",
              "timestamp": "2026-04-15T10:00:00Z"}],
        )
        publisher._producer.flush(timeout=10)

        messages = _drain(consumer_raw)
        assert len(messages) == 3

        types = [dict(m.headers())["type"] for m in messages]
        assert types.count(b"snapshot") == 2
        assert types.count(b"tombstone") == 1

    def test_publish_empty_batch(self, publisher):
        """Empty publish is a no-op."""
        publisher.publish([], [])
        # No assertion — just verifying no exception


# ---------------------------------------------------------------------------
# DefactoConsumer tests
# ---------------------------------------------------------------------------


class TestDefactoConsumer:
    """DefactoConsumer reads from Kafka and classifies messages by header."""

    @pytest.fixture
    def defacto_consumer(self, topic):
        """DefactoConsumer with an in-memory SQLite state history backend."""
        from confluent_kafka import Consumer

        from defacto.backends._definition_store import SqliteDefinitionsStore
        from defacto.backends._state_history import SqliteStateHistory

        # Minimal definitions for state history tables
        defs_db = SqliteDefinitionsStore(":memory:")
        defs_db.register("v1", {
            "entities": {
                "customer": {
                    "properties": {"mrr": {"type": "number"}},
                    "states": ["trial", "active"],
                },
            },
        }, {"definition_hash": "x", "source_hash": "x", "identity_hash": "x"})
        defs_db.activate("v1")

        state_history = SqliteStateHistory(":memory:", "v1")
        state_history.ensure_tables({
            "customer": {"properties": {"mrr": {"type": "number"}}},
        })

        kafka_consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": f"test-consumer-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "session.timeout.ms": 6000,
            "heartbeat.interval.ms": 1000,
        })

        from defacto._defacto import DefactoConsumer

        consumer = DefactoConsumer(
            consumer=kafka_consumer,
            topic=topic,
            definitions_db=defs_db,
            state_history=state_history,
            batch_size=100,
            batch_timeout_ms=500,
        )
        yield consumer
        try:
            consumer.close()
        except Exception:
            pass

    def test_consume_snapshots(self, publisher, defacto_consumer):
        """Consumer reads snapshots and returns correct counts."""
        publisher.publish(
            [{"entity_id": "cust_001", "entity_type": "customer",
              "state": "active", "properties": {"mrr": 99.0},
              "valid_from": "2026-04-15T10:00:00Z"}],
            [],
        )
        publisher._producer.flush(timeout=10)

        result = defacto_consumer.run_once()
        assert result.snapshots_processed == 1
        assert result.tombstones_processed == 0
        assert result.batches_written == 1

    def test_consume_tombstones(self, publisher, defacto_consumer):
        """Consumer reads tombstones and returns correct counts."""
        publisher.publish(
            [],
            [{"entity_id": "cust_002", "entity_type": "customer",
              "merged_into": "cust_001",
              "timestamp": "2026-04-15T10:00:00Z"}],
        )
        publisher._producer.flush(timeout=10)

        result = defacto_consumer.run_once()
        assert result.snapshots_processed == 0
        assert result.tombstones_processed == 1

    def test_consume_empty_returns_zero(self, defacto_consumer):
        """No messages available returns zero counts."""
        result = defacto_consumer.run_once()
        assert result.snapshots_processed == 0
        assert result.tombstones_processed == 0
        assert result.batches_written == 0

    def test_idempotent_replay(self, publisher, defacto_consumer):
        """Consuming the same snapshot twice is safe (idempotent writes)."""
        snap = {"entity_id": "cust_001", "entity_type": "customer",
                "state": "active", "properties": {"mrr": 99.0},
                "valid_from": "2026-04-15T10:00:00Z"}

        # Publish same snapshot twice
        publisher.publish([snap], [])
        publisher.publish([snap], [])
        publisher._producer.flush(timeout=10)

        # Consume both — write_batch handles idempotency
        result = defacto_consumer.run_once()
        assert result.snapshots_processed == 2
        assert result.batches_written == 1
