"""Tests for InlinePublisher — development-mode publisher."""

import json

from defacto._publisher import InlinePublisher
from defacto.backends import SqliteStateHistory


CUSTOMER_DEF = {
    "properties": {
        "name": {"type": "string"},
        "mrr": {"type": "float"},
    },
}


class TestInlinePublisher:
    """InlinePublisher delegates directly to state history backend."""

    def _setup(self):
        """Create an InlinePublisher with a real SqliteStateHistory."""
        sh = SqliteStateHistory(":memory:")
        sh.ensure_tables({"customer": CUSTOMER_DEF})
        return InlinePublisher(sh), sh

    def test_publish_writes_to_backend(self):
        pub, sh = self._setup()
        pub.publish(
            snapshots=[{
                "entity_id": "c_001", "entity_type": "customer",
                "state": "active", "properties": {"name": "Alice", "mrr": 99.0},
                "valid_from": "2024-01-15T10:00:00Z",
            }],
            tombstones=[],
        )
        row = sh._conn.execute("SELECT customer_id, customer_state FROM customer_history").fetchone()
        assert row == ("c_001", "active")

    def test_publish_empty_batches(self):
        pub, _ = self._setup()
        pub.publish(snapshots=[], tombstones=[])  # should not raise

    def test_close_is_noop(self):
        pub, _ = self._setup()
        result = pub.close()
        assert result is None

    def test_publish_tombstone(self):
        pub, sh = self._setup()
        # First write a snapshot
        pub.publish(
            snapshots=[{
                "entity_id": "c_001", "entity_type": "customer",
                "state": "active", "properties": {"name": "Alice", "mrr": 99.0},
                "valid_from": "2024-01-15T10:00:00Z",
            }],
            tombstones=[],
        )
        # Then tombstone it
        pub.publish(
            snapshots=[],
            tombstones=[{
                "entity_id": "c_001", "entity_type": "customer",
                "merged_into": "c_002", "timestamp": "2024-01-20T10:00:00Z",
            }],
        )
        row = sh._conn.execute(
            "SELECT valid_to, merged_into FROM customer_history WHERE customer_id = 'c_001'"
        ).fetchone()
        assert row[0] == "2024-01-20T10:00:00Z"
        assert row[1] == "c_002"
