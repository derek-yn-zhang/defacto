"""Tests for internal orchestration components — publisher and dead letter.

KafkaPublisher and KafkaDeadLetter construct real confluent-kafka Producer
objects but don't require a running broker for unit tests (the producer
buffers internally without connecting).
"""

import importlib.util
import json
import os
import tempfile

import pytest

from defacto.results import EventFailure


# ---------------------------------------------------------------------------
# Publisher
# ---------------------------------------------------------------------------


class TestPublisherAbstract:
    """Publisher is abstract — can't be instantiated directly."""

    def test_publisher_is_abstract(self):
        from defacto._publisher import Publisher

        with pytest.raises(TypeError):
            Publisher()  # type: ignore


_has_kafka = bool(importlib.util.find_spec("confluent_kafka"))


@pytest.mark.skipif(not _has_kafka, reason="confluent-kafka not installed")
class TestKafkaPublisher:
    """KafkaPublisher unit tests — construct producer, verify config handling."""

    def test_construction(self):
        from defacto._publisher import KafkaPublisher

        pub = KafkaPublisher({"bootstrap.servers": "localhost:9092", "topic": "test"})
        assert pub._topic == "test"

    def test_topic_extracted_from_config(self):
        from defacto._publisher import KafkaPublisher

        pub = KafkaPublisher({"bootstrap.servers": "localhost:9092", "topic": "my-effects"})
        assert pub._topic == "my-effects"

    def test_drain_errors_empty_initially(self):
        from defacto._publisher import KafkaPublisher

        pub = KafkaPublisher({"bootstrap.servers": "localhost:9092", "topic": "test"})
        assert pub.drain_errors() == []

    def test_inline_publisher_drain_errors_always_empty(self):
        """InlinePublisher inherits the ABC default — always empty."""
        from unittest.mock import MagicMock

        from defacto._publisher import InlinePublisher

        pub = InlinePublisher(MagicMock())
        assert pub.drain_errors() == []


# ---------------------------------------------------------------------------
# Dead Letter Sink
# ---------------------------------------------------------------------------


class TestDeadLetterSinkAbstract:
    """DeadLetterSink is abstract — can't be instantiated directly."""

    def test_dead_letter_sink_is_abstract(self):
        from defacto._dead_letter import DeadLetterSink

        with pytest.raises(TypeError):
            DeadLetterSink()  # type: ignore

    def test_factory_returns_null_for_none(self):
        from defacto._dead_letter import DeadLetterSink, NullDeadLetter

        sink = DeadLetterSink.create(None)
        assert isinstance(sink, NullDeadLetter)

    def test_factory_returns_file_for_file_config(self):
        from defacto._dead_letter import DeadLetterSink, FileDeadLetter

        sink = DeadLetterSink.create({"type": "file", "path": "/tmp/dl.jsonl"})
        assert isinstance(sink, FileDeadLetter)

    def test_factory_raises_for_unknown_type(self):
        from defacto._dead_letter import DeadLetterSink

        with pytest.raises(ValueError, match="Unknown dead_letter type"):
            DeadLetterSink.create({"type": "redis"})


class TestNullDeadLetter:
    """NullDeadLetter is a no-op — send and close do nothing."""

    def test_send_is_noop(self):
        from defacto._dead_letter import NullDeadLetter

        sink = NullDeadLetter()
        failure = EventFailure(raw={"x": 1}, error="bad", stage="normalization")
        sink.send([failure])  # should not raise

    def test_close_is_noop(self):
        from defacto._dead_letter import NullDeadLetter

        NullDeadLetter().close()


class TestFileDeadLetter:
    """FileDeadLetter writes JSONL to a file."""

    def test_writes_jsonl(self):
        from defacto._dead_letter import FileDeadLetter

        with tempfile.NamedTemporaryFile(mode="r", suffix=".jsonl", delete=False) as f:
            path = f.name

        try:
            sink = FileDeadLetter(path)
            sink.send([
                EventFailure(raw={"email": "bad"}, error="missing field", stage="normalization", source="web"),
                EventFailure(raw={}, error="entity missing", stage="interpretation", entity_id="c_001"),
            ])
            sink.close()

            with open(path) as f:
                lines = f.readlines()
            assert len(lines) == 2

            first = json.loads(lines[0])
            assert first["stage"] == "normalization"
            assert first["error"] == "missing field"
            assert first["raw"] == {"email": "bad"}

            second = json.loads(lines[1])
            assert second["stage"] == "interpretation"
            assert second["entity_id"] == "c_001"
        finally:
            os.unlink(path)

    def test_creates_parent_directories(self):
        from defacto._dead_letter import FileDeadLetter

        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "sub", "dir", "dl.jsonl")
            sink = FileDeadLetter(path)
            sink.send([EventFailure(raw={}, error="test", stage="normalization")])
            sink.close()

            assert os.path.exists(path)
            with open(path) as f:
                assert len(f.readlines()) == 1

    def test_empty_send_is_noop(self):
        from defacto._dead_letter import FileDeadLetter

        sink = FileDeadLetter("/tmp/should-not-be-created.jsonl")
        sink.send([])
        sink.close()
        assert not os.path.exists("/tmp/should-not-be-created.jsonl")


@pytest.mark.skipif(not _has_kafka, reason="confluent-kafka not installed")
class TestKafkaDeadLetter:
    """KafkaDeadLetter constructs a producer — no broker needed for unit tests."""

    def test_construction(self):
        from defacto._dead_letter import KafkaDeadLetter

        sink = KafkaDeadLetter({
            "type": "kafka",
            "bootstrap.servers": "localhost:9092",
            "topic": "defacto-dlq",
        })
        assert sink._topic == "defacto-dlq"

    def test_empty_send_is_noop(self):
        from defacto._dead_letter import KafkaDeadLetter

        sink = KafkaDeadLetter({
            "bootstrap.servers": "localhost:9092",
            "topic": "defacto-dlq",
        })
        sink.send([])  # should not raise


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------


class TestConfigureLogging:
    """configure_logging sets up handlers correctly."""

    def test_console_mode(self):
        import logging
        from defacto._logging import configure_logging

        # Remove any existing handlers from prior test runs
        root = logging.getLogger("defacto")
        root.handlers = [h for h in root.handlers if isinstance(h, logging.NullHandler)]

        configure_logging(level=logging.WARNING, log_format="console")
        assert root.level == logging.WARNING
        # Should have NullHandler + StreamHandler
        non_null = [h for h in root.handlers if not isinstance(h, logging.NullHandler)]
        assert len(non_null) == 1
        assert isinstance(non_null[0], logging.StreamHandler)

        # Cleanup
        root.handlers = [logging.NullHandler()]
        root.setLevel(logging.WARNING)

    def test_json_mode(self):
        import logging
        from defacto._logging import configure_logging, _JSONFormatter

        root = logging.getLogger("defacto")
        root.handlers = [h for h in root.handlers if isinstance(h, logging.NullHandler)]

        configure_logging(level=logging.DEBUG, log_format="json")
        non_null = [h for h in root.handlers if not isinstance(h, logging.NullHandler)]
        assert len(non_null) == 1
        assert isinstance(non_null[0].formatter, _JSONFormatter)

        # Cleanup
        root.handlers = [logging.NullHandler()]
        root.setLevel(logging.WARNING)

    def test_json_formatter_includes_extra(self):
        """JSON formatter outputs extra fields as top-level JSON keys."""
        import json
        import logging
        from defacto._logging import _JSONFormatter

        formatter = _JSONFormatter()
        record = logging.LogRecord(
            name="defacto.pipeline", level=logging.INFO,
            pathname="", lineno=0, msg="build completed",
            args=(), exc_info=None,
        )
        record.operation = "build"
        record.mode = "INCREMENTAL"
        record.events_processed = 5000

        output = formatter.format(record)
        parsed = json.loads(output)
        assert parsed["message"] == "build completed"
        assert parsed["logger"] == "defacto.pipeline"
        assert parsed["level"] == "INFO"
        assert parsed["operation"] == "build"
        assert parsed["mode"] == "INCREMENTAL"
        assert parsed["events_processed"] == 5000
