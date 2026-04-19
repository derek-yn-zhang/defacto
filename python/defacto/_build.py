"""Build mode detection and build state management.

Tracks the build cursor, definition hashes, watermark, and dirty flag
in the database. Detects what changed and determines the minimum build
mode needed to bring entity state up to date.

BuildManager owns the business logic (cascade detection, hash comparison).
BuildStateStore is the database interface — SQLite and Postgres implement
it with their own SQL dialects.

PARTIAL detection (late arrivals) is NOT handled here — that requires
ledger access and is the Pipeline's responsibility. BuildManager only
does hash-based and cursor-based detection.
"""

from __future__ import annotations

import sqlite3
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

from defacto.errors import storage_errors
from defacto.results import BuildStatus


class BuildStateStore(ABC):
    """Database interface for build state CRUD.

    Stores version, cursor, watermark, definition hashes, dirty flag,
    and build history. Each backend (SQLite, Postgres) implements this
    with its own SQL dialect and column types.
    """

    @abstractmethod
    def create_tables(self) -> None:
        """Create the build_state table if it doesn't exist."""

    @abstractmethod
    def get_state(self, version: str) -> tuple:
        """Read (cursor, def_hash, src_hash, id_hash, dirty) for a version."""

    @abstractmethod
    def update_cursor(self, version: str, cursor: int) -> None:
        """Set the cursor position after successful processing."""

    @abstractmethod
    def update_watermark(self, version: str, ts: str) -> None:
        """Advance the watermark (monotonic — only if ts > current)."""

    @abstractmethod
    def get_watermark(self, version: str) -> str:
        """Read current watermark as ISO 8601 string, or '' if unset."""

    @abstractmethod
    def set_dirty(self, version: str) -> None:
        """Mark build as in-progress."""

    @abstractmethod
    def clear_dirty(
        self, version: str, mode: str, hashes: dict[str, str], now: str
    ) -> None:
        """Mark build as complete. Store hashes, mode, and timestamp."""

    @abstractmethod
    def get_status_row(self, version: str) -> tuple:
        """Read (cursor, def_hash, src_hash, id_hash, dirty, last_time, last_mode)."""

    def begin(self) -> None:
        """Begin an explicit transaction — subsequent writes don't commit
        until commit() is called. Default no-op for auto-commit backends."""

    def commit(self) -> None:
        """Commit the current explicit transaction. Default no-op."""

    @abstractmethod
    def close(self) -> None:
        """Close the database connection and release resources."""


class SqliteBuildStateStore(BuildStateStore):
    """SQLite build state storage — development, single-process."""

    def __init__(
        self, db_path: str, *, connection: sqlite3.Connection | None = None,
    ) -> None:
        self._owns_connection = connection is None
        if connection is not None:
            self._conn = connection
        else:
            self._conn = sqlite3.connect(db_path)
        self.create_tables()

    def create_tables(self) -> None:
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS build_state (
                version          TEXT PRIMARY KEY,
                cursor           INTEGER NOT NULL DEFAULT 0,
                watermark        TEXT,
                definition_hash  TEXT NOT NULL DEFAULT '',
                source_hash      TEXT NOT NULL DEFAULT '',
                identity_hash    TEXT NOT NULL DEFAULT '',
                dirty            INTEGER NOT NULL DEFAULT 0,
                last_build_time  TEXT,
                last_build_mode  TEXT
            )
        """)
        self._conn.commit()

    def _ensure_row(self, version: str) -> None:
        """Create a row for this version if it doesn't exist."""
        with storage_errors("Build state ensure row"):
            self._conn.execute(
                "INSERT OR IGNORE INTO build_state (version) VALUES (?)",
                (version,),
            )
            self._conn.commit()

    def get_state(self, version: str) -> tuple:
        self._ensure_row(version)
        with storage_errors("Build state read"):
            return self._conn.execute(
                "SELECT cursor, definition_hash, source_hash, identity_hash, dirty"
                " FROM build_state WHERE version = ?",
                (version,),
            ).fetchone()

    def update_cursor(self, version: str, cursor: int) -> None:
        self._ensure_row(version)
        with storage_errors("Build state update cursor"), self._conn:
            self._conn.execute(
                "UPDATE build_state SET cursor = ? WHERE version = ?",
                (cursor, version),
            )

    def update_watermark(self, version: str, ts: str) -> None:
        self._ensure_row(version)
        with storage_errors("Build state update watermark"), self._conn:
            self._conn.execute(
                "UPDATE build_state SET watermark = ?"
                " WHERE version = ? AND (watermark IS NULL OR watermark < ?)",
                (ts, version, ts),
            )

    def get_watermark(self, version: str) -> str:
        self._ensure_row(version)
        with storage_errors("Build state read watermark"):
            row = self._conn.execute(
                "SELECT watermark FROM build_state WHERE version = ?",
                (version,),
            ).fetchone()
            return row[0] or ""

    def set_dirty(self, version: str) -> None:
        self._ensure_row(version)
        with storage_errors("Build state set dirty"), self._conn:
            self._conn.execute(
                "UPDATE build_state SET dirty = 1 WHERE version = ?",
                (version,),
            )

    def clear_dirty(
        self, version: str, mode: str, hashes: dict[str, str], now: str
    ) -> None:
        with storage_errors("Build state clear dirty"), self._conn:
            self._conn.execute(
                "UPDATE build_state"
                " SET dirty = 0, last_build_time = ?, last_build_mode = ?,"
                " definition_hash = ?, source_hash = ?, identity_hash = ?"
                " WHERE version = ?",
                (
                    now, mode,
                    hashes["definition_hash"],
                    hashes["source_hash"],
                    hashes["identity_hash"],
                    version,
                ),
            )

    def get_status_row(self, version: str) -> tuple:
        self._ensure_row(version)
        with storage_errors("Build state read status"):
            return self._conn.execute(
                "SELECT cursor, definition_hash, source_hash, identity_hash,"
                " dirty, last_build_time, last_build_mode"
                " FROM build_state WHERE version = ?",
                (version,),
            ).fetchone()

    def close(self) -> None:
        if self._owns_connection:
            self._conn.close()


class PostgresBuildStateStore(BuildStateStore):
    """Postgres build state storage — production.

    Tables in shared `defacto` schema. Uses TIMESTAMPTZ for watermark,
    BOOLEAN for dirty flag, BIGINT for cursor.

    Supports shared connections: when constructed with a `connection`
    parameter (from the ledger), build state writes are committed as
    part of the ledger's transaction. This eliminates separate commits
    on the hot path — cursor and watermark updates ride along with the
    ledger's commit atomically.
    """

    def __init__(
        self,
        db_path: str,
        *,
        namespace: str = "defacto",
        connection: Any | None = None,
        in_transaction_fn: Any | None = None,
    ) -> None:
        """Initialize with a Postgres connection string or shared connection.

        Args:
            db_path: Postgres connection string.
            namespace: Schema prefix for tables. Default 'defacto'.
            connection: Optional shared connection (from ledger). When
                provided, this store does not own or close the connection.
            in_transaction_fn: Callable returning True when the shared
                connection is in an explicit transaction. Auto-commit
                is suppressed when this returns True — the ledger's
                commit() handles it.
        """
        self._owns_connection = connection is None
        self._ns = namespace
        if connection is not None:
            self._conn = connection
        else:
            import psycopg

            self._conn = psycopg.connect(db_path)
            self._conn.execute("SET timezone = 'UTC'")
        self._in_transaction_fn = in_transaction_fn or (lambda: False)
        self._ensured: set[str] = set()
        self.create_tables()

    def create_tables(self) -> None:
        """Create the build_state table if it doesn't exist."""
        ns = self._ns
        with storage_errors("Build state create tables"), self._conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {ns}")
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {ns}.build_state (
                    version          TEXT    PRIMARY KEY,
                    cursor           BIGINT  NOT NULL DEFAULT 0,
                    watermark        TIMESTAMPTZ,
                    definition_hash  TEXT    NOT NULL DEFAULT '',
                    source_hash      TEXT    NOT NULL DEFAULT '',
                    identity_hash    TEXT    NOT NULL DEFAULT '',
                    dirty            BOOLEAN NOT NULL DEFAULT FALSE,
                    last_build_time  TIMESTAMPTZ,
                    last_build_mode  TEXT
                )
            """)
        self._auto_commit()

    def _ensure_row(self, version: str) -> None:
        """Create a row for this version if it doesn't exist."""
        with storage_errors("Build state ensure row"), self._conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO {self._ns}.build_state (version)"
                " VALUES (%s) ON CONFLICT (version) DO NOTHING",
                (version,),
            )
        self._auto_commit()

    def _ensure_once(self, version: str) -> None:
        """Ensure the version row exists, at most once per version per session."""
        if version not in self._ensured:
            self._ensure_row(version)
            self._ensured.add(version)

    def get_state(self, version: str) -> tuple:
        self._ensure_once(version)
        with storage_errors("Build state read"), self._conn.cursor() as cur:
            cur.execute(
                "SELECT cursor, definition_hash, source_hash, identity_hash, dirty"
                f" FROM {self._ns}.build_state WHERE version = %s",
                (version,),
                prepare=True,
            )
            return cur.fetchone()

    def update_cursor(self, version: str, cursor: int) -> None:
        self._ensure_once(version)
        with storage_errors("Build state update cursor"), self._conn.cursor() as cur:
            cur.execute(
                f"UPDATE {self._ns}.build_state SET cursor = %s WHERE version = %s",
                (cursor, version),
                prepare=True,
            )
        self._auto_commit()

    def update_watermark(self, version: str, ts: str) -> None:
        self._ensure_once(version)
        with storage_errors("Build state update watermark"), self._conn.cursor() as cur:
            cur.execute(
                f"UPDATE {self._ns}.build_state SET watermark = %s::timestamptz"
                " WHERE version = %s AND (watermark IS NULL OR watermark < %s::timestamptz)",
                (ts, version, ts),
            )
        self._auto_commit()

    def get_watermark(self, version: str) -> str:
        self._ensure_once(version)
        with storage_errors("Build state read watermark"), self._conn.cursor() as cur:
            cur.execute(
                f"SELECT watermark::text FROM {self._ns}.build_state WHERE version = %s",
                (version,),
            )
            row = cur.fetchone()
            return row[0] or ""

    def set_dirty(self, version: str) -> None:
        self._ensure_once(version)
        with storage_errors("Build state set dirty"), self._conn.cursor() as cur:
            cur.execute(
                f"UPDATE {self._ns}.build_state SET dirty = TRUE WHERE version = %s",
                (version,),
            )
        self._auto_commit()

    def clear_dirty(
        self, version: str, mode: str, hashes: dict[str, str], now: str
    ) -> None:
        self._ensure_once(version)
        with storage_errors("Build state clear dirty"), self._conn.cursor() as cur:
            cur.execute(
                f"UPDATE {self._ns}.build_state"
                " SET dirty = FALSE, last_build_time = %s::timestamptz,"
                " last_build_mode = %s,"
                " definition_hash = %s, source_hash = %s, identity_hash = %s"
                " WHERE version = %s",
                (
                    now, mode,
                    hashes["definition_hash"],
                    hashes["source_hash"],
                    hashes["identity_hash"],
                    version,
                ),
            )
        self._auto_commit()

    def get_status_row(self, version: str) -> tuple:
        self._ensure_once(version)
        with storage_errors("Build state read status"), self._conn.cursor() as cur:
            cur.execute(
                "SELECT cursor, definition_hash, source_hash, identity_hash,"
                " dirty, last_build_time::text, last_build_mode"
                f" FROM {self._ns}.build_state WHERE version = %s",
                (version,),
            )
            return cur.fetchone()

    # ── Transaction control ──

    def begin(self) -> None:
        """No-op — transaction boundaries are owned by the ledger."""

    def commit(self) -> None:
        """Commit only if this store owns its connection."""
        if self._owns_connection:
            self._conn.commit()

    def _auto_commit(self) -> None:
        """Commit unless the shared connection is in an explicit transaction.

        When the ledger calls begin(), in_transaction_fn returns True and
        auto_commit is suppressed — writes are committed by ledger.commit().
        When not in a transaction (builds, standalone use), commits normally.
        """
        if self._in_transaction_fn():
            return
        self._conn.commit()

    def close(self) -> None:
        """Close only if this store owns its connection."""
        if self._owns_connection:
            self._conn.close()


class BuildManager:
    """Detects required build mode and manages build state.

    Owns the business logic: hash cascade for mode detection, watermark
    monotonicity, crash recovery via dirty flag. Delegates database
    operations to a BuildStateStore.
    """

    def __init__(
        self, store: BuildStateStore, core: Any,
        shard_id: int | None = None, total_shards: int | None = None,
    ) -> None:
        """Initialize with a state store and DefactoCore reference.

        Args:
            store: Database backend for build state CRUD.
            core: DefactoCore instance for reading definition hashes.
            shard_id: Shard index for per-shard build state. None = unsharded.
            total_shards: Total number of shards. None = unsharded.
        """
        self._store = store
        self._core = core
        self._shard_id = shard_id
        self._total_shards = total_shards

    def _state_key(self, version: str) -> str:
        """Build state row key — includes shard_id if sharded.

        Unsharded: returns version as-is.
        Sharded: returns '{version}_s{shard_id}' so each shard tracks
        its own cursor, watermark, and dirty flag independently.
        """
        if self._total_shards is None:
            return version
        return f"{version}_s{self._shard_id}"

    def detect_mode(self, version: str, ledger_cursor: int) -> str:
        """Compare current definition hashes against stored hashes.

        Returns one of: SKIP, INCREMENTAL, FULL, FULL_WITH_IDENTITY_RESET,
        FULL_RENORMALIZE. PARTIAL detection is the Pipeline's responsibility
        (requires inspecting event timestamps against the watermark).

        Cascade (most expensive wins):
        FULL_RENORMALIZE > FULL_WITH_IDENTITY_RESET > FULL > INCREMENTAL > SKIP

        Args:
            version: Definition version name.
            ledger_cursor: Current highest sequence in the ledger.
        """
        key = self._state_key(version)
        row = self._store.get_state(key)
        stored_cursor = row[0]
        stored_def_hash = row[1]
        stored_src_hash = row[2]
        stored_id_hash = row[3]
        dirty = bool(row[4])

        # Plain version for definition hashes — definitions are shared
        # across shards, only build progress is per-shard.
        current = self._core.definition_hashes(version)

        # Determine mode via cascade (most expensive wins).
        # Hash checks come first so we pick the right mode even during
        # crash recovery (dirty=True). If definitions changed AND we
        # crashed, we need FULL_RENORMALIZE, not just FULL.
        if current["source_hash"] != stored_src_hash:
            return "FULL_RENORMALIZE"
        if current["identity_hash"] != stored_id_hash:
            return "FULL_WITH_IDENTITY_RESET"
        if current["definition_hash"] != stored_def_hash:
            return "FULL"
        if dirty:
            return "FULL"
        if ledger_cursor > stored_cursor:
            return "INCREMENTAL"
        return "SKIP"

    def advance_cursor(self, version: str, cursor: int) -> None:
        """Update the cursor after successful batch processing."""
        self._store.update_cursor(self._state_key(version), cursor)

    def update_watermark(self, version: str, max_timestamp: datetime) -> None:
        """Advance the watermark — the max timestamp of all processed events.

        Monotonic: only advances, never decreases. If max_timestamp is older
        than the current watermark, this is a no-op.
        """
        self._store.update_watermark(self._state_key(version), max_timestamp.isoformat())

    def get_watermark(self, version: str) -> str:
        """Current watermark (max event timestamp processed), or '' if unset.

        Returns ISO 8601 string for direct comparison with event timestamps.
        Empty string compares less than any valid timestamp, so late arrival
        detection correctly skips when no watermark has been set.
        """
        return self._store.get_watermark(self._state_key(version))

    def set_dirty(self, version: str) -> None:
        """Mark build as in-progress. If we crash, next startup detects this."""
        self._store.set_dirty(self._state_key(version))

    def has_empty_hashes(self, version: str) -> bool:
        """Check if this version has never been built (all hashes are empty).

        Used to distinguish first-ever build (empty hashes → seed them)
        from genuine definition changes (stored hashes differ → rebuild).
        """
        key = self._state_key(version)
        row = self._store.get_state(key)
        return not row[1] and not row[2] and not row[3]

    def seed_hashes(self, version: str) -> None:
        """Write current definition hashes to build_state without recording a build.

        Used when build_state has empty hashes (first-ever build) which
        would trigger FULL_RENORMALIZE. Seeding writes the correct
        hashes so detect_mode returns FULL instead of RENORMALIZE.

        Unlike clear_dirty, this preserves last_build_time and
        last_build_mode as NULL — no build has actually occurred.
        """
        key = self._state_key(version)
        hashes = self._core.definition_hashes(version)
        self._store.clear_dirty(key, None, hashes, None)

    def clear_dirty(self, version: str, build_mode: str) -> None:
        """Mark build as complete. Records build mode, timestamp, and current hashes.

        Hashes are updated here (not in detect_mode) so that if a build
        crashes, the next startup's detect_mode still sees the hash mismatch
        and picks the correct rebuild mode.
        """
        now = datetime.now(timezone.utc).isoformat()
        hashes = self._core.definition_hashes(version)
        self._store.clear_dirty(self._state_key(version), build_mode, hashes, now)

    def get_status(self, version: str) -> BuildStatus:
        """Read current build state for a version.

        Note: total_events and pending_events are set to 0 here — the
        Defacto class fills them from ledger.cursor() since BuildManager
        doesn't own the ledger.
        """
        row = self._store.get_status_row(self._state_key(version))
        return BuildStatus(
            cursor=row[0],
            total_events=0,
            pending_events=0,
            last_build_time=row[5],
            last_build_mode=row[6],
            definition_hash=row[1],
            source_hash=row[2],
            identity_hash=row[3],
            dirty=bool(row[4]),
        )

    def close(self) -> None:
        """Close the underlying state store connection."""
        self._store.close()
