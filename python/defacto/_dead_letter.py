"""Dead letter sink for failed events at any pipeline stage.

Routes EventFailure objects to a durable destination for inspection,
debugging, and potential reprocessing. Every failure carries its stage
(normalization, interpretation, publishing, consumption), the input
that failed, and enough context to diagnose the issue.

Three implementations:
- NullDeadLetter: no-op, failures are only in result objects
- FileDeadLetter: append JSONL to a file (development)
- KafkaDeadLetter: publish to a Kafka dead letter topic (production)

Configured via the dead_letter parameter on Defacto:
  None → NullDeadLetter (failures only in IngestResult/BuildResult)
  {"type": "file", "path": "/var/log/defacto/dead-letter.jsonl"} → FileDeadLetter
  {"type": "kafka", "bootstrap.servers": "...", "topic": "..."} → KafkaDeadLetter
"""

from __future__ import annotations

import json
import logging
import os
from abc import ABC, abstractmethod
from dataclasses import asdict
from typing import TYPE_CHECKING, Any

logger = logging.getLogger("defacto.dead_letter")

if TYPE_CHECKING:
    from defacto.results import EventFailure


class DeadLetterSink(ABC):
    """Routes failed events to a dead letter destination.

    Receives EventFailure objects from the Pipeline (ingest path) or
    DefactoConsumer (consumer path). Implementations serialize and deliver
    to their destination. Fire-and-forget — delivery errors are logged
    but do not propagate back to the caller.
    """

    @staticmethod
    def create(config: dict[str, Any] | None) -> DeadLetterSink:
        """Factory — returns the appropriate sink based on config.

        Args:
            config: Dead letter config dict, or None for NullDeadLetter.
                {"type": "file", "path": "/var/log/defacto/dead-letter.jsonl"}
                {"type": "kafka", "bootstrap.servers": "...", "topic": "defacto-dlq"}

        Returns:
            A DeadLetterSink instance.

        Raises:
            ValueError: If config type is not recognized.
        """
        if config is None:
            return NullDeadLetter()

        dl_type = config.get("type", "")
        if dl_type == "file":
            return FileDeadLetter(config["path"])
        if dl_type == "kafka":
            return KafkaDeadLetter(config)
        raise ValueError(
            f"Unknown dead_letter type '{dl_type}'. Supported: 'file', 'kafka'"
        )

    @abstractmethod
    def send(self, failures: list[EventFailure]) -> None:
        """Send failed events to the dead letter destination.

        Fire-and-forget — delivery errors should be caught internally,
        not propagated. The pipeline must not crash because dead letter
        routing failed.

        Args:
            failures: EventFailure objects with raw input, error, stage,
                and entity context.
        """

    @abstractmethod
    def close(self) -> None:
        """Flush pending writes and release resources."""


class NullDeadLetter(DeadLetterSink):
    """No-op sink — failures are only in result objects.

    Used when dead_letter config is None. The caller can still inspect
    failures via IngestResult.failures or BuildResult — they're just
    not persisted anywhere.
    """

    def send(self, failures: list[EventFailure]) -> None:
        """No-op — failures are not routed anywhere."""

    def close(self) -> None:
        """No-op."""


class FileDeadLetter(DeadLetterSink):
    """Appends failed events as JSON lines to a file.

    Each line is a complete JSON object with all EventFailure fields.
    Creates the file and parent directories on first write. Flushes
    after each send to ensure failures are visible immediately.

    Good for development and single-process deployments where a Kafka
    dead letter topic isn't available.
    """

    def __init__(self, path: str) -> None:
        """Initialize with output file path.

        Args:
            path: Path to the dead letter JSONL file. Created on first
                write. Parent directories created automatically.
        """
        self._path = path
        self._file = None

    def send(self, failures: list[EventFailure]) -> None:
        """Append failures as JSON lines, one per line."""
        if not failures:
            return
        try:
            if self._file is None:
                os.makedirs(os.path.dirname(self._path) or ".", exist_ok=True)
                self._file = open(self._path, "a")
            for failure in failures:
                self._file.write(json.dumps(asdict(failure)) + "\n")
            self._file.flush()
        except OSError as exc:
            logger.error("dead letter file write failed", extra={
                "error": str(exc),
                "path": self._path,
                "failure_count": len(failures),
            })

    def close(self) -> None:
        """Close the file handle."""
        if self._file is not None:
            self._file.close()
            self._file = None


class KafkaDeadLetter(DeadLetterSink):
    """Publishes failed events to a Kafka dead letter topic.

    Each failure is serialized as JSON and published as a Kafka message.
    Uses the same confluent-kafka Producer patterns as KafkaPublisher:
    lz4 compression, entity_id as partition key (when available),
    stage as a Kafka header for filtering.
    """

    def __init__(self, kafka_config: dict[str, Any]) -> None:
        """Initialize with Kafka config.

        Args:
            kafka_config: Dict with 'bootstrap.servers', 'topic', and
                optional producer config. 'type' and 'topic' are
                extracted; everything else is passed to the Producer.
        """
        try:
            from confluent_kafka import Producer
        except ModuleNotFoundError:
            raise ModuleNotFoundError(
                "KafkaDeadLetter requires confluent-kafka. "
                "Install it with: pip install defacto[kafka]"
            ) from None

        config = {**kafka_config}
        config.pop("type", None)
        self._topic = config.pop("topic")
        config.setdefault("linger.ms", 5)
        config.setdefault("compression.type", "lz4")
        self._producer = Producer(config)

    def send(self, failures: list[EventFailure]) -> None:
        """Publish failures to the dead letter Kafka topic.

        Each failure is one message. Key is entity_id (if available)
        or source. Header carries the stage for downstream filtering.
        """
        if not failures:
            return
        try:
            for failure in failures:
                key = (failure.entity_id or failure.source or "unknown").encode("utf-8")
                value = json.dumps(asdict(failure)).encode("utf-8")
                self._producer.produce(
                    self._topic,
                    key=key,
                    value=value,
                    headers={"stage": failure.stage},
                )
            self._producer.poll(0)
        except Exception as exc:
            logger.error("dead letter Kafka produce failed", extra={
                "error": str(exc),
                "topic": self._topic,
                "failure_count": len(failures),
            })

    def close(self) -> None:
        """Flush all buffered messages and close the producer."""
        try:
            self._producer.flush(timeout=10)
        except Exception as exc:
            logger.error("dead letter Kafka flush failed", extra={
                "error": str(exc),
            })
