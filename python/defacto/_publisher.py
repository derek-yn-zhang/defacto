"""Effect publishing — delivers entity snapshots and tombstones downstream.

Two implementations:
- InlinePublisher: development, writes directly to state history backend
- KafkaPublisher: production, sends to Kafka effects topic with entity_id
  partition key, lz4 compression, and delivery callbacks for observability
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from defacto.backends import StateHistoryBackend


class Publisher(ABC):
    """Abstract base for effect publishing.

    Two modes exist because the hot path changes between development and production:
    - Development (InlinePublisher): no Kafka, writes state history directly to SQLite/Postgres
    - Production (KafkaPublisher): publishes to Kafka, separate consumer builds state history

    The pipeline doesn't know which mode it's in — it calls publish() and the
    implementation handles delivery.
    """

    @abstractmethod
    def publish(self, snapshots: list[dict[str, Any]], tombstones: list[dict[str, Any]]) -> None:
        """Publish entity snapshots and tombstones.

        Args:
            snapshots: Full entity state dicts from interpretation.
            tombstones: Entity removal dicts from merges or erasure.
        """

    def drain_errors(self) -> list[tuple[str, str]]:
        """Return and clear any delivery errors from the previous publish.

        Returns:
            List of (error_message, entity_id) tuples. Empty for backends
            that don't have async delivery (InlinePublisher).
        """
        return []

    def delete_entity(self, entity_id: str) -> None:
        """Delete all state history for an entity before a rebuild.

        Called by _rebuild_entities to clear stale state history before
        replaying events. The replay produces fresh, correct history
        via publish(). Without the delete, SCD write_batch would collide
        with existing rows (same valid_from → ON CONFLICT DO NOTHING).

        Default no-op — KafkaPublisher sends a delete message,
        InlinePublisher writes directly.
        """

    def delete_entity_batch(self, entity_ids: list[str]) -> None:
        """Delete state history for multiple entities. Used by erase cascade.

        Default loops; InlinePublisher overrides with batch operation.
        """
        for eid in entity_ids:
            self.delete_entity(eid)

    @abstractmethod
    def close(self) -> None:
        """Flush pending messages and release resources."""


class KafkaPublisher(Publisher):
    """Production publisher — sends to Kafka effects topic.

    Uses confluent-kafka Producer with lz4 compression and entity_id
    partition key. produce() is non-blocking (buffered internally by
    librdkafka). Messages are sent when linger.ms expires or the buffer
    fills. Delivery callbacks track failed messages for observability.

    Message type is encoded as a Kafka header ('type': 'snapshot' or
    'tombstone'), keeping the payload a clean dict that matches what
    write_batch() expects on the consumer side.
    """

    def __init__(self, kafka_config: dict[str, Any]) -> None:
        """Initialize Kafka producer.

        Args:
            kafka_config: Dict with 'bootstrap.servers', 'topic', and optional
                producer config. 'topic' is extracted; everything else is
                passed to confluent-kafka Producer.
        """
        try:
            from confluent_kafka import Producer
        except ModuleNotFoundError:
            raise ModuleNotFoundError(
                "KafkaPublisher requires confluent-kafka. "
                "Install it with: pip install defacto[kafka]"
            ) from None

        config = {**kafka_config}
        self._topic = config.pop("topic")
        config.setdefault("linger.ms", 5)
        config.setdefault("compression.type", "lz4")
        config.setdefault("acks", "all")
        self._producer = Producer(config)
        self._delivery_errors: list[tuple[str, str]] = []

    def publish(self, snapshots: list[dict[str, Any]], tombstones: list[dict[str, Any]]) -> None:
        """Produce snapshots and tombstones to the Kafka effects topic.

        Each message is keyed by entity_id for partition-level ordering.
        Non-blocking — messages are buffered by librdkafka and sent in
        the background. poll(0) triggers delivery callbacks from the
        previous batch.
        """
        self._producer.poll(0)

        for snap in snapshots:
            self._produce(snap["entity_id"], snap, "snapshot")
        for tomb in tombstones:
            self._produce(tomb["entity_id"], tomb, "tombstone")

    def _produce(self, key: str, value: dict[str, Any], msg_type: str) -> None:
        """Produce a single message with type header."""
        self._producer.produce(
            self._topic,
            key=key.encode("utf-8"),
            value=json.dumps(value).encode("utf-8"),
            headers={"type": msg_type},
            callback=self._on_delivery,
        )

    def _on_delivery(self, err: Any, msg: Any) -> None:
        """Delivery callback — collects errors for the next drain_errors() call."""
        if err is not None:
            self._delivery_errors.append((err.str(), msg.key().decode("utf-8")))

    def drain_errors(self) -> list[tuple[str, str]]:
        """Return and clear delivery errors from previous publishes.

        Called by the Pipeline after each publish() to collect failures.
        Errors are (error_message, entity_id) tuples.
        """
        errors = self._delivery_errors
        self._delivery_errors = []
        return errors

    def delete_entity(self, entity_id: str) -> None:
        """Produce a delete message for the entity.

        The consumer processes this before subsequent snapshots (same
        partition key = entity_id, Kafka guarantees ordering within
        partition). Consumer deletes all state history rows for the
        entity, then processes fresh snapshots from the rebuild.
        """
        self._produce(entity_id, {"entity_id": entity_id}, "delete")

    def close(self) -> None:
        """Flush all buffered messages and close the producer.

        Raises RuntimeError if messages remain unflushed after timeout.
        """
        remaining = self._producer.flush(timeout=30)
        if remaining > 0:
            raise RuntimeError(
                f"KafkaPublisher: {remaining} messages unflushed after 30s"
            )


class InlinePublisher(Publisher):
    """Development publisher — writes directly to state history backend.

    Used when kafka=None. State history is built inline during build(),
    no separate consumer process needed.
    """

    def __init__(self, state_history: StateHistoryBackend) -> None:
        """Initialize with the state history backend to write to directly.

        Args:
            state_history: The backend that owns the SCD Type 2 tables.
        """
        self._state_history = state_history

    def publish(self, snapshots: list[dict[str, Any]], tombstones: list[dict[str, Any]]) -> None:
        """Write directly to state history backend."""
        self._state_history.write_batch(snapshots, tombstones)

    def delete_entity(self, entity_id: str) -> None:
        """Delete state history rows for an entity before rebuild."""
        self._state_history.delete_entity(entity_id)

    def close(self) -> None:
        """No-op — state history backend manages its own lifecycle."""
