"""Tests for BuildManager — build mode detection and state management."""

import sqlite3

import pytest

from defacto._build import BuildManager, SqliteBuildStateStore
from defacto.results import BuildStatus


class MockCore:
    """Mock DefactoCore that returns configurable definition hashes."""

    def __init__(self, hashes: dict[str, str] | None = None) -> None:
        self._hashes = hashes or {
            "definition_hash": "def_abc",
            "source_hash": "src_def",
            "identity_hash": "id_ghi",
        }

    def definition_hashes(self, version: str) -> dict[str, str]:
        return self._hashes


def make_manager(
    hashes: dict[str, str] | None = None,
) -> tuple[BuildManager, sqlite3.Connection]:
    """Create a BuildManager with an in-memory database."""
    core = MockCore(hashes)
    store = SqliteBuildStateStore(":memory:")
    return BuildManager(store, core), store._conn


class TestBuildManagerDetectMode:
    """Build mode detection via hash comparison."""

    def test_first_build_returns_renormalize(self):
        """No existing build state — all hashes mismatch, source wins cascade."""
        bm, _ = make_manager()
        # Empty stored hashes don't match any current hash. The cascade
        # checks source_hash first (most expensive), so first build
        # triggers FULL_RENORMALIZE. This is correct — a first build
        # from scratch needs to normalize everything anyway.
        assert bm.detect_mode("v1", ledger_cursor=0) == "FULL_RENORMALIZE"

    def test_skip_when_nothing_changed(self):
        bm, _ = make_manager()
        # First detect_mode triggers FULL_RENORMALIZE (empty stored hashes)
        bm.detect_mode("v1", ledger_cursor=0)
        # Simulate successful build: clear_dirty stores hashes
        bm.clear_dirty("v1", "FULL_RENORMALIZE")
        # Second call: hashes match, cursor at 0, ledger at 0
        assert bm.detect_mode("v1", ledger_cursor=0) == "SKIP"

    def test_incremental_when_cursor_behind(self):
        bm, _ = make_manager()
        bm.detect_mode("v1", ledger_cursor=0)
        bm.clear_dirty("v1", "FULL")  # stores hashes
        bm.advance_cursor("v1", 0)
        assert bm.detect_mode("v1", ledger_cursor=10) == "INCREMENTAL"

    def test_dirty_returns_full(self):
        bm, _ = make_manager()
        bm.detect_mode("v1", ledger_cursor=0)
        bm.clear_dirty("v1", "FULL")  # stores hashes
        bm.set_dirty("v1")
        assert bm.detect_mode("v1", ledger_cursor=0) == "FULL"

    def test_source_hash_changed_returns_renormalize(self):
        bm, _ = make_manager()
        bm.detect_mode("v1", ledger_cursor=0)
        bm.clear_dirty("v1", "FULL")  # stores initial hashes
        # Change core hashes to simulate source change
        bm._core._hashes["source_hash"] = "src_CHANGED"
        assert bm.detect_mode("v1", ledger_cursor=0) == "FULL_RENORMALIZE"

    def test_identity_hash_changed_returns_identity_reset(self):
        bm, _ = make_manager()
        bm.detect_mode("v1", ledger_cursor=0)
        bm.clear_dirty("v1", "FULL")
        bm._core._hashes["identity_hash"] = "id_CHANGED"
        assert bm.detect_mode("v1", ledger_cursor=0) == "FULL_WITH_IDENTITY_RESET"

    def test_definition_hash_changed_returns_full(self):
        bm, _ = make_manager()
        bm.detect_mode("v1", ledger_cursor=0)
        bm.clear_dirty("v1", "FULL")
        bm._core._hashes["definition_hash"] = "def_CHANGED"
        assert bm.detect_mode("v1", ledger_cursor=0) == "FULL"

    def test_cascade_source_wins_over_identity(self):
        """Source hash change is more expensive than identity — it wins."""
        bm, _ = make_manager()
        bm.detect_mode("v1", ledger_cursor=0)
        bm.clear_dirty("v1", "FULL")
        bm._core._hashes["source_hash"] = "src_CHANGED"
        bm._core._hashes["identity_hash"] = "id_CHANGED"
        assert bm.detect_mode("v1", ledger_cursor=0) == "FULL_RENORMALIZE"

    def test_dirty_with_hash_change_picks_expensive(self):
        """If dirty AND source hash changed, source wins (most expensive)."""
        bm, _ = make_manager()
        bm.detect_mode("v1", ledger_cursor=0)
        bm.clear_dirty("v1", "FULL")
        bm.set_dirty("v1")
        bm._core._hashes["source_hash"] = "src_CHANGED"
        assert bm.detect_mode("v1", ledger_cursor=0) == "FULL_RENORMALIZE"

    def test_hashes_updated_on_clear_dirty_not_detect(self):
        """detect_mode does NOT update stored hashes — clear_dirty does."""
        bm, conn = make_manager()
        bm.detect_mode("v1", ledger_cursor=0)
        # Hashes should still be empty (not updated by detect_mode)
        row = conn.execute(
            "SELECT definition_hash FROM build_state WHERE version = 'v1'"
        ).fetchone()
        assert row[0] == ""
        # After clear_dirty, hashes are stored
        bm.clear_dirty("v1", "FULL")
        row = conn.execute(
            "SELECT definition_hash, source_hash, identity_hash FROM build_state WHERE version = 'v1'"
        ).fetchone()
        assert row == ("def_abc", "src_def", "id_ghi")


class TestBuildManagerState:
    """Cursor, watermark, dirty flag management."""

    def test_table_created_on_init(self):
        _, conn = make_manager()
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        assert ("build_state",) in tables

    def test_advance_cursor(self):
        bm, _ = make_manager()
        bm.advance_cursor("v1", 42)
        status = bm.get_status("v1")
        assert status.cursor == 42

    def test_update_watermark(self):
        from datetime import datetime, timezone
        bm, conn = make_manager()
        bm.update_watermark("v1", datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc))
        row = conn.execute(
            "SELECT watermark FROM build_state WHERE version = 'v1'"
        ).fetchone()
        assert row[0] == "2024-01-15T10:00:00+00:00"

    def test_set_and_clear_dirty(self):
        bm, _ = make_manager()
        bm.set_dirty("v1")
        assert bm.get_status("v1").dirty is True
        bm.clear_dirty("v1", "FULL")
        status = bm.get_status("v1")
        assert status.dirty is False
        assert status.last_build_mode == "FULL"
        assert status.last_build_time is not None

    def test_get_status_returns_build_status(self):
        bm, _ = make_manager()
        status = bm.get_status("v1")
        assert isinstance(status, BuildStatus)
        assert status.cursor == 0
        assert status.dirty is False

    def test_get_status_first_build(self):
        bm, _ = make_manager()
        status = bm.get_status("v1")
        assert status.cursor == 0
        assert status.last_build_time is None
        assert status.last_build_mode is None
        assert status.definition_hash == ""

    def test_ensure_row_idempotent(self):
        bm, _ = make_manager()
        bm._store._ensure_row("v1")
        bm._store._ensure_row("v1")  # should not raise

    def test_advance_cursor_creates_row(self):
        bm, _ = make_manager()
        bm.advance_cursor("v1", 5)  # creates row via _ensure_row
        assert bm.get_status("v1").cursor == 5

    def test_watermark_monotonic_advances(self):
        """Watermark advances when given a later timestamp."""
        from datetime import datetime, timezone
        bm, _ = make_manager()
        t1 = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
        t2 = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        bm.update_watermark("v1", t1)
        assert bm.get_watermark("v1") == t1.isoformat()
        bm.update_watermark("v1", t2)
        assert bm.get_watermark("v1") == t2.isoformat()

    def test_watermark_monotonic_no_regression(self):
        """Watermark does not decrease when given an earlier timestamp."""
        from datetime import datetime, timezone
        bm, _ = make_manager()
        t_later = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        t_earlier = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
        bm.update_watermark("v1", t_later)
        bm.update_watermark("v1", t_earlier)
        assert bm.get_watermark("v1") == t_later.isoformat()

    def test_get_watermark_empty(self):
        """Fresh version has no watermark — returns empty string."""
        bm, _ = make_manager()
        assert bm.get_watermark("v1") == ""

    def test_get_watermark_after_update(self):
        from datetime import datetime, timezone
        bm, _ = make_manager()
        ts = datetime(2024, 6, 1, 8, 30, 0, tzinfo=timezone.utc)
        bm.update_watermark("v1", ts)
        assert bm.get_watermark("v1") == ts.isoformat()
