"""BuildStateStore contract tests.

Every BuildStateStore implementation must satisfy these correctness properties.
Tests are parametrized across SQLite and Postgres via the `build_state` fixture
in conftest.py.
"""


# ---------------------------------------------------------------------------
# get_state and defaults
# ---------------------------------------------------------------------------


class TestGetState:
    """get_state: read build state for a version."""

    def test_new_version_defaults(self, build_state):
        """New version has cursor=0, empty hashes, dirty=False."""
        state = build_state.get_state("v1")
        cursor, def_hash, src_hash, id_hash, dirty = state
        assert cursor == 0
        assert def_hash == ""
        assert src_hash == ""
        assert id_hash == ""
        assert not dirty


# ---------------------------------------------------------------------------
# cursor
# ---------------------------------------------------------------------------


class TestCursor:
    """update_cursor: track the last processed sequence."""

    def test_update_cursor(self, build_state):
        """Cursor updates are readable."""
        build_state.update_cursor("v1", 42)
        state = build_state.get_state("v1")
        assert state[0] == 42

    def test_cursor_advances(self, build_state):
        """Successive updates advance the cursor."""
        build_state.update_cursor("v1", 10)
        build_state.update_cursor("v1", 20)
        state = build_state.get_state("v1")
        assert state[0] == 20


# ---------------------------------------------------------------------------
# watermark
# ---------------------------------------------------------------------------


class TestWatermark:
    """update_watermark / get_watermark: monotonic timestamp tracking."""

    def test_watermark_initially_empty(self, build_state):
        """New version has empty watermark."""
        assert build_state.get_watermark("v1") == ""

    def test_watermark_advances(self, build_state):
        """Setting a watermark makes it readable."""
        build_state.update_watermark("v1", "2024-01-15T10:00:00Z")
        wm = build_state.get_watermark("v1")
        assert "2024-01-15" in wm

    def test_watermark_monotonic_advances(self, build_state):
        """Later timestamp advances the watermark."""
        build_state.update_watermark("v1", "2024-01-15T10:00:00Z")
        build_state.update_watermark("v1", "2024-01-15T15:00:00Z")
        wm = build_state.get_watermark("v1")
        assert "15:00:00" in wm

    def test_watermark_monotonic_no_regression(self, build_state):
        """Earlier timestamp does NOT decrease the watermark."""
        build_state.update_watermark("v1", "2024-01-15T15:00:00Z")
        build_state.update_watermark("v1", "2024-01-15T10:00:00Z")
        wm = build_state.get_watermark("v1")
        assert "15:00:00" in wm


# ---------------------------------------------------------------------------
# dirty flag
# ---------------------------------------------------------------------------


class TestDirtyFlag:
    """set_dirty / clear_dirty: crash recovery protocol."""

    def test_set_dirty(self, build_state):
        """set_dirty marks the version as in-progress."""
        build_state.set_dirty("v1")
        state = build_state.get_state("v1")
        assert bool(state[4]) is True

    def test_clear_dirty(self, build_state):
        """clear_dirty marks the version as complete and stores metadata."""
        hashes = {
            "definition_hash": "def_abc",
            "source_hash": "src_def",
            "identity_hash": "id_ghi",
        }
        build_state.set_dirty("v1")
        build_state.clear_dirty("v1", "FULL", hashes, "2024-01-15T10:00:00Z")
        state = build_state.get_state("v1")
        assert not bool(state[4])  # dirty = False
        assert state[1] == "def_abc"  # definition_hash stored
        assert state[2] == "src_def"  # source_hash stored
        assert state[3] == "id_ghi"  # identity_hash stored

    def test_dirty_round_trip(self, build_state):
        """set → clear → get is clean, clear → set → get is dirty."""
        hashes = {
            "definition_hash": "a",
            "source_hash": "b",
            "identity_hash": "c",
        }
        build_state.set_dirty("v1")
        build_state.clear_dirty("v1", "FULL", hashes, "2024-01-15T10:00:00Z")
        assert not bool(build_state.get_state("v1")[4])

        build_state.set_dirty("v1")
        assert bool(build_state.get_state("v1")[4])


# ---------------------------------------------------------------------------
# get_status_row
# ---------------------------------------------------------------------------


class TestStatusRow:
    """get_status_row: read full build history."""

    def test_status_includes_build_metadata(self, build_state):
        """clear_dirty stores last_build_time and last_build_mode."""
        hashes = {
            "definition_hash": "a",
            "source_hash": "b",
            "identity_hash": "c",
        }
        build_state.set_dirty("v1")
        build_state.clear_dirty("v1", "FULL", hashes, "2024-01-15T10:00:00Z")
        row = build_state.get_status_row("v1")
        # (cursor, def_hash, src_hash, id_hash, dirty, last_time, last_mode)
        assert row[6] == "FULL"  # last_build_mode
        assert "2024-01-15" in str(row[5])  # last_build_time
