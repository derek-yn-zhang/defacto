"""Ledger backend interface and implementations.

The ledger is the single source of truth — an append-only, immutable log
of every event. Each event stores both raw input (original source data)
and normalized data (what the framework produced).

The event_entities mapping (which entities each event affected) is also
part of the ledger, written at ingest time after identity resolution.

Implementations:
    SqliteLedger  — development, single-process
    PostgresLedger — production
    TieredLedger — stitches hot (Postgres) + cold (S3/Parquet) for large-scale
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import Any

from defacto.errors import StorageError, storage_errors


class LedgerBackend(ABC):
    """Abstract interface for the event ledger.

    The ledger supports five operations: append events, replay events
    (sequential or entity-scoped), check the cursor position, and
    redact sensitive fields (right to erasure).
    """

    @abstractmethod
    def append_batch(self, events: list[dict[str, Any]]) -> list[int]:
        """Append a batch of normalized events to the ledger.

        Each event dict must contain: event_id, event_type, timestamp,
        source, data, raw, resolution_hints. Events with duplicate
        event_ids are silently skipped (UNIQUE constraint).

        Args:
            events: Normalized events with raw input preserved.

        Returns:
            Assigned sequence numbers for each successfully appended event.
            Duplicates get no sequence (filtered out).
        """

    @abstractmethod
    def write_event_entities(self, mappings: list[tuple[int, str, str]]) -> None:
        """Write event-to-entity mappings after identity resolution.

        Each mapping is (sequence, entity_id, entity_type). Written immediately
        after identity resolution so partial rebuilds and merge handling can
        efficiently find events for specific entities.

        Args:
            mappings: List of (sequence, entity_id, entity_type) tuples.
        """

    @abstractmethod
    def replay(
        self, from_sequence: int = 0, *, include_duplicates: bool = False,
    ) -> Iterator[dict[str, Any]]:
        """Replay events from the ledger starting at a sequence number.

        Used by builds to process events. Incremental builds start from
        the cursor. Full builds start from 0. By default, events marked
        as duplicates (duplicate_of IS NOT NULL) are filtered out.

        Args:
            from_sequence: Starting sequence number (inclusive). Default 0.
            include_duplicates: If True, include events marked as duplicates.
                Used by renormalize_ledger which needs to re-process all
                events to recompute duplicate_of from scratch.

        Yields:
            Event dicts in timestamp order (with sequence as tiebreaker).
        """

    @abstractmethod
    def replay_for_entities(self, entity_ids: list[str]) -> Iterator[dict[str, Any]]:
        """Replay events that affected specific entities.

        Uses the event_entities mapping for efficient lookup — no full
        ledger scan needed. Used by partial rebuilds and merge handling.

        Args:
            entity_ids: Entity IDs to replay events for.

        Yields:
            Event dicts in sequence order, deduplicated across entities.
        """

    @abstractmethod
    def replay_for_shard(
        self, shard_id: int, total_shards: int, from_sequence: int = 0,
    ) -> Iterator[dict[str, Any]]:
        """Replay events for entities owned by a specific shard.

        Uses event_entities to determine which events belong to entities
        assigned to this shard via SHA-256 hash partitioning. Only valid
        when event_entities is populated (FULL and INCREMENTAL builds,
        not RENORMALIZE or IDENTITY_RESET).

        Args:
            shard_id: This shard's index (0-based).
            total_shards: Total number of shards.
            from_sequence: Starting sequence number (inclusive). Default 0.

        Yields:
            Event dicts in timestamp order (with sequence as tiebreaker).
        """

    @abstractmethod
    def cursor(self) -> int:
        """Get the current ledger cursor (highest sequence number).

        Returns:
            The highest assigned sequence number, or 0 if the ledger is empty.
        """

    @abstractmethod
    def redact(self, entity_id: str, sensitive_fields: list[str]) -> int:
        """Redact sensitive fields in ledger events for an entity.

        Part of the right-to-erasure flow. Finds all events that affected
        this entity (via event_entities) and replaces sensitive field values
        in both raw and normalized data.

        Args:
            entity_id: Entity whose events should be redacted.
            sensitive_fields: Property names to redact.

        Returns:
            Number of events redacted.
        """

    @abstractmethod
    def update_normalized(self, updates: list[tuple[int, dict]]) -> None:
        """Update normalized data for existing ledger rows.

        Used by FULL_RENORMALIZE to write re-normalized output back to the
        ledger. Updates data, event_id, event_type, resolution_hints, and
        duplicate_of columns in one atomic operation per row. The raw
        column is unchanged — it's the source of truth that
        renormalization reads from.

        The duplicate_of field is set by Rust during renormalization:
        NULL for canonical events, canonical_sequence for duplicates.
        Every renormalize is a clean slate — all rows get a fresh
        duplicate_of value, making dedup fully reversible.

        Args:
            updates: List of (sequence, new_ledger_row_dict) pairs.
                Each dict must have: data, event_id, event_type,
                resolution_hints, duplicate_of.
        """

    @abstractmethod
    def clear_event_entities(self) -> None:
        """Delete all event-entity mappings.

        Used before FULL_RENORMALIZE and FULL_WITH_IDENTITY_RESET builds.
        When resolution hints change (renormalize) or entity IDs are
        regenerated (identity reset), existing mappings become stale.
        The rebuild rewrites them from scratch.
        """

    @abstractmethod
    def merge_event_entities(self, loser_id: str, winner_id: str) -> None:
        """Update event_entities after an entity merge.

        Reassigns all event_entities rows from loser_id to winner_id
        and recomputes shard_hash. Keeps event_entities current so
        replay_for_shard and replay_for_entities return correct results
        for merged entities.
        """

    def has_event_entities(self) -> bool:
        """Check if event_entities has any rows.

        Used as a precondition for replay_for_shard — shard-aware replay
        requires event_entities to be populated from a prior build or
        from ingest with process=True. Returns False on first build
        with append-only ingest.
        """
        return False  # Overridden by backends with event_entities

    def begin(self) -> None:
        """Begin an explicit transaction — subsequent writes don't commit
        until commit() is called. Default no-op for auto-commit backends."""

    def commit(self) -> None:
        """Commit the current explicit transaction. Default no-op."""

    @abstractmethod
    def count(self, source: str | None = None) -> int:
        """Count events in the ledger, optionally filtered by source.

        Args:
            source: Filter by source name. None = count all events.

        Returns:
            Number of events.
        """

    @abstractmethod
    def events_for(self, entity_id: str) -> list[dict[str, Any]]:
        """Get event summaries that affected a specific entity.

        Uses the event_entities mapping for efficient lookup — no full
        ledger scan. Returns lightweight summaries for debugging, not
        full event payloads.

        Args:
            entity_id: Entity ID to look up events for.

        Returns:
            List of dicts with sequence, event_id, event_type, timestamp.
        """

    @abstractmethod
    def delete_events_for(self, entity_id: str) -> int:
        """Delete all ledger events and event_entities for an entity.

        Used by erase to permanently remove an entity's events from
        the ledger. The events are gone — replays will not see them.

        Args:
            entity_id: Entity whose events should be deleted.

        Returns:
            Number of ledger rows deleted.
        """

    def existing_entity_ids(self, entity_ids: list[str]) -> set[str]:
        """Check which entity IDs still have events in event_entities.

        Used by tick validation to detect entities erased or merged
        away by another shard. One indexed query instead of N lookups.

        Args:
            entity_ids: Entity IDs to check.

        Returns:
            Set of entity IDs that still exist in event_entities.
        """
        # Default: fall back to individual lookups
        return {
            eid for eid in entity_ids
            if self.lookup_entity_type(eid) is not None
        }

    def all_entity_mappings(self) -> dict[str, str]:
        """Return all (entity_id → entity_type) from event_entities.

        Used before IDENTITY_RESET to capture old entity_ids so
        tombstones can be produced for IDs that aren't recreated.

        Returns:
            Dict mapping entity_id to entity_type.
        """
        return {}  # Overridden by backends with event_entities

    # ── Merge log ──

    @abstractmethod
    def write_merge_log(
        self, from_entity_id: str, into_entity_id: str, timestamp: str,
    ) -> None:
        """Record a merge in the merge log.

        The merge log is the inline source of truth for which entities
        were merged into which. Used by erase for cascade and by
        inspection. Written by _execute_merges alongside identity and
        event_entities updates.

        Args:
            from_entity_id: Entity that was absorbed (the loser).
            into_entity_id: Entity that survived (the winner).
            timestamp: ISO 8601 timestamp of the merge.
        """

    @abstractmethod
    def find_merges_into(self, entity_id: str) -> list[str]:
        """Find all entities that were merged into the given entity.

        Used by erase to cascade deletion through merge chains.
        Walks the merge log, not state history — inline store,
        always consistent regardless of publisher lag.

        Args:
            entity_id: The surviving entity to check.

        Returns:
            List of entity IDs that were merged into entity_id.
        """

    @abstractmethod
    def find_merge_losers(self, entity_ids: list[str]) -> set[str]:
        """Check which entity IDs are merge losers in the merge log.

        Used by identity resolution to validate cache hits against
        cross-shard merges. A cache-hit entity_id that appears as
        from_entity_id in the merge log is stale — the entity was
        merged away, but this shard's cache hasn't been updated.

        One indexed query against the merge_log primary key. Returns
        instantly when no merges exist (empty table).

        Args:
            entity_ids: Entity IDs to check (typically from cache hits).

        Returns:
            Subset of entity_ids that are merge losers.
        """

    @abstractmethod
    def delete_merge_log(self, entity_id: str) -> None:
        """Delete merge log entries involving an entity (right to erasure).

        Removes all rows where entity_id appears as either from or into.

        Args:
            entity_id: Entity being erased.
        """

    def delete_merge_log_batch(self, entity_ids: list[str]) -> None:
        """Delete merge log entries for multiple entities in one operation.

        Used by erase cascade to avoid N+1 deletes. Default loops;
        backends override with IN (...) / ANY(...) for efficiency.
        """
        for eid in entity_ids:
            self.delete_merge_log(eid)

    @abstractmethod
    def close(self) -> None:
        """Close the database connection and release resources."""

    @abstractmethod
    def lookup_entity_type(self, entity_id: str) -> str | None:
        """Find the entity type for an entity from event_entities.

        Used by the erase flow to determine which entity definition
        applies (for sensitive field lookup).

        Args:
            entity_id: Entity to look up.

        Returns:
            Entity type name, or None if the entity has no events.
        """

    @property
    @abstractmethod
    def connection(self) -> Any:
        """The underlying database connection.

        Exposed for use by TieredLedger (metadata tables on the hot
        tier's database) and build_state connection sharing.
        """


class SqliteLedger(LedgerBackend):
    """SQLite ledger backend — development and single-process use.

    Stores events and event-entity mappings in a single SQLite database.
    Uses WAL mode for concurrent read access during writes, INSERT OR IGNORE
    for idempotent appends, and server-side json_set for redaction.
    """

    def __init__(self, db_path: str) -> None:
        """Initialize with a database path and create tables.

        Args:
            db_path: Path to the SQLite database file. Use ":memory:" for tests.
        """
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._create_tables()

    def _create_tables(self) -> None:
        """Create ledger and event_entities tables if they don't exist."""
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS ledger (
                sequence          INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id          TEXT    NOT NULL,
                event_type        TEXT    NOT NULL,
                timestamp         TEXT    NOT NULL,
                source            TEXT    NOT NULL,
                data              TEXT    NOT NULL,
                raw               TEXT    NOT NULL,
                resolution_hints  TEXT    NOT NULL,
                duplicate_of      INTEGER
            );

            CREATE UNIQUE INDEX IF NOT EXISTS idx_ledger_event_id_unique
                ON ledger(event_id) WHERE duplicate_of IS NULL;

            CREATE TABLE IF NOT EXISTS event_entities (
                sequence    INTEGER NOT NULL REFERENCES ledger(sequence),
                entity_id   TEXT    NOT NULL,
                entity_type TEXT    NOT NULL,
                shard_hash  INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (sequence, entity_id)
            );

            CREATE INDEX IF NOT EXISTS idx_event_entities_entity
                ON event_entities(entity_id);

            CREATE INDEX IF NOT EXISTS idx_ledger_timestamp
                ON ledger(timestamp, sequence);

            CREATE TABLE IF NOT EXISTS merge_log (
                from_entity_id TEXT NOT NULL,
                into_entity_id TEXT NOT NULL,
                timestamp      TEXT NOT NULL,
                PRIMARY KEY (from_entity_id, into_entity_id)
            );

            CREATE INDEX IF NOT EXISTS idx_merge_log_into
                ON merge_log(into_entity_id);
        """)

    def append_batch(self, events: list[dict[str, Any]]) -> list[int]:
        """Append events in a single transaction using INSERT OR IGNORE.

        Duplicates (by event_id) are silently skipped. Returns sequence
        numbers for all events that were successfully inserted or already
        existed — the caller needs sequences to write event_entities.
        """
        if not events:
            return []

        rows = [
            (
                e["event_id"],
                e["event_type"],
                e["timestamp"],
                e["source"],
                json.dumps(e["data"]),
                json.dumps(e["raw"]),
                json.dumps(e["resolution_hints"]),
            )
            for e in events
        ]

        with storage_errors("Ledger append"), self._conn:
            self._conn.executemany(
                "INSERT OR IGNORE INTO ledger"
                " (event_id, event_type, timestamp, source, data, raw, resolution_hints)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                rows,
            )

            # Fetch sequences for all event_ids (inserted or pre-existing)
            event_ids = [e["event_id"] for e in events]
            placeholders = ",".join("?" * len(event_ids))
            cursor = self._conn.execute(
                f"SELECT event_id, sequence FROM ledger"
                f" WHERE event_id IN ({placeholders})",
                event_ids,
            )
            id_to_seq = dict(cursor.fetchall())

        # Return sequences in input order
        return [id_to_seq[e["event_id"]] for e in events if e["event_id"] in id_to_seq]

    def write_event_entities(self, mappings: list[tuple[int, str, str]]) -> None:
        """Batch INSERT event-entity mappings with pre-computed shard hash."""
        if not mappings:
            return

        rows = [
            (seq, eid, etype,
             int.from_bytes(hashlib.sha256(eid.encode()).digest()[:4]))
            for seq, eid, etype in mappings
        ]
        with storage_errors("Event entities write"), self._conn:
            self._conn.executemany(
                "INSERT OR IGNORE INTO event_entities"
                " (sequence, entity_id, entity_type, shard_hash)"
                " VALUES (?, ?, ?, ?)",
                rows,
            )

    def replay(
        self, from_sequence: int = 0, *, include_duplicates: bool = False,
    ) -> Iterator[dict[str, Any]]:
        """Stream events from the ledger using a lazy cursor.

        Ordered by timestamp (with sequence as tiebreaker) so rebuilds
        process events in chronological order. SQLite's cursor iterates
        row-by-row internally, so this uses bounded memory regardless
        of ledger size.
        """
        dup_filter = "" if include_duplicates else " AND duplicate_of IS NULL"
        with storage_errors("Ledger replay"):
            cursor = self._conn.execute(
                "SELECT sequence, event_id, event_type, timestamp, source,"
                " data, raw, resolution_hints"
                f" FROM ledger WHERE sequence >= ?{dup_filter}"
                " ORDER BY timestamp, sequence",
                (from_sequence,),
            )

            for row in cursor:
                yield {
                    "sequence": row[0],
                    "event_id": row[1],
                    "event_type": row[2],
                    "timestamp": row[3],
                    "source": row[4],
                    "data": json.loads(row[5]),
                    "raw": json.loads(row[6]),
                    "resolution_hints": json.loads(row[7]),
                }

    def replay_for_entities(self, entity_ids: list[str]) -> Iterator[dict[str, Any]]:
        """Replay events for specific entities via event_entities join.

        Uses DISTINCT to deduplicate when one event maps to multiple
        requested entities. Ordered by timestamp (with sequence as tiebreaker)
        so partial rebuilds process events in chronological order.
        """
        if not entity_ids:
            return

        placeholders = ",".join("?" * len(entity_ids))
        with storage_errors("Ledger replay for entities"):
            cursor = self._conn.execute(
                f"SELECT DISTINCT l.sequence, l.event_id, l.event_type, l.timestamp,"
                f" l.source, l.data, l.raw, l.resolution_hints"
                f" FROM ledger l"
                f" JOIN event_entities ee ON l.sequence = ee.sequence"
                f" WHERE ee.entity_id IN ({placeholders})"
                f" AND l.duplicate_of IS NULL"
                f" ORDER BY l.timestamp, l.sequence",
                entity_ids,
            )

            for row in cursor:
                yield {
                    "sequence": row[0],
                    "event_id": row[1],
                    "event_type": row[2],
                    "timestamp": row[3],
                    "source": row[4],
                    "data": json.loads(row[5]),
                    "raw": json.loads(row[6]),
                    "resolution_hints": json.loads(row[7]),
                }

    def replay_for_shard(
        self, shard_id: int, total_shards: int, from_sequence: int = 0,
    ) -> Iterator[dict[str, Any]]:
        """Replay events for entities owned by a shard.

        Filters via pre-computed shard_hash column and includes the
        pre-resolved entity_id and entity_type from event_entities.
        This lets the build skip identity resolution entirely — the
        entity mapping rides along with the replay for free.
        """
        with storage_errors("Ledger replay for shard"):
            cursor = self._conn.execute(
                "SELECT l.sequence, l.event_id, l.event_type,"
                " l.timestamp, l.source, l.data, l.raw, l.resolution_hints,"
                " ee.entity_id, ee.entity_type"
                " FROM ledger l"
                " JOIN event_entities ee ON l.sequence = ee.sequence"
                " WHERE ee.shard_hash % ? = ?"
                " AND l.sequence >= ?"
                " AND l.duplicate_of IS NULL"
                " ORDER BY l.timestamp, l.sequence",
                (total_shards, shard_id, from_sequence),
            )

            for row in cursor:
                yield {
                    "sequence": row[0],
                    "event_id": row[1],
                    "event_type": row[2],
                    "timestamp": row[3],
                    "source": row[4],
                    "data": json.loads(row[5]),
                    "raw": json.loads(row[6]),
                    "resolution_hints": json.loads(row[7]),
                    "entity_id": row[8],
                    "entity_type": row[9],
                }

    def has_event_entities(self) -> bool:
        """Check if event_entities has any rows."""
        with storage_errors("Event entities check"):
            row = self._conn.execute("SELECT 1 FROM event_entities LIMIT 1").fetchone()
            return row is not None

    def cursor(self) -> int:
        """Return the highest sequence number, or 0 if empty."""
        with storage_errors("Ledger cursor"):
            row = self._conn.execute("SELECT MAX(sequence) FROM ledger").fetchone()
            return row[0] if row[0] is not None else 0

    def redact(self, entity_id: str, sensitive_fields: list[str]) -> int:
        """Redact sensitive fields using server-side json_set.

        One UPDATE statement — SQLite's json_set modifies the JSON in-place
        without Python-side parsing. Scoped to events that affected the
        entity via event_entities join. Returns count of events redacted.
        """
        if not sensitive_fields:
            return 0

        # Build json_set arguments: '$.field1', '[REDACTED]', ...
        # json_set treats SQL TEXT values as JSON strings automatically,
        # so '[REDACTED]' becomes the JSON string "[REDACTED]".
        data_set_args = ", ".join(
            f"'$.{field}', '[REDACTED]'" for field in sensitive_fields
        )
        raw_set_args = data_set_args  # same fields in both columns

        with storage_errors("Redact events"), self._conn:
            cursor = self._conn.execute(
                f"UPDATE ledger SET"
                f" data = json_set(data, {data_set_args}),"
                f" raw = json_set(raw, {raw_set_args})"
                f" WHERE sequence IN ("
                f"   SELECT sequence FROM event_entities WHERE entity_id = ?"
                f" )",
                (entity_id,),
            )
            return cursor.rowcount

    def update_normalized(self, updates: list[tuple[int, dict]]) -> None:
        """Batch UPDATE ledger rows with re-normalized data.

        Updates data, event_id, event_type, resolution_hints, and
        duplicate_of columns by sequence (primary key). The raw column
        is untouched — it's the immutable source of truth that
        renormalization reads from. duplicate_of is set by Rust: NULL
        for canonical events, canonical_sequence for duplicates.
        """
        if not updates:
            return

        with storage_errors("Ledger update normalized"), self._conn:
            self._conn.executemany(
                "UPDATE ledger SET data = ?, event_id = ?, event_type = ?,"
                " resolution_hints = ?, duplicate_of = ? WHERE sequence = ?",
                [
                    (
                        json.dumps(row["data"]),
                        row["event_id"],
                        row["event_type"],
                        json.dumps(row["resolution_hints"]),
                        row.get("duplicate_of"),
                        seq,
                    )
                    for seq, row in updates
                ],
            )

    def clear_event_entities(self) -> None:
        """Delete all event-entity mappings.

        Called before FULL_RENORMALIZE and FULL_WITH_IDENTITY_RESET builds
        so the rebuild can rewrite mappings from scratch with correct
        resolution hints and entity IDs.
        """
        with storage_errors("Clear event entities"), self._conn:
            self._conn.execute("DELETE FROM event_entities")

    def merge_event_entities(self, loser_id: str, winner_id: str) -> None:
        """Reassign event_entities from loser to winner after a merge.

        Deletes loser rows that conflict with existing winner rows (same
        sequence) before updating, to avoid UNIQUE constraint violations
        when both entities share events from the same batch.
        """
        winner_hash = int.from_bytes(
            hashlib.sha256(winner_id.encode()).digest()[:4]
        )
        with storage_errors("Merge event entities"), self._conn:
            # Remove loser rows where the winner already has an entry
            # for the same sequence (both entities in the same event batch)
            self._conn.execute(
                "DELETE FROM event_entities"
                " WHERE entity_id = ? AND sequence IN ("
                "   SELECT sequence FROM event_entities WHERE entity_id = ?"
                " )",
                (loser_id, winner_id),
            )
            self._conn.execute(
                "UPDATE event_entities SET entity_id = ?, shard_hash = ?"
                " WHERE entity_id = ?",
                (winner_id, winner_hash, loser_id),
            )

    def count(self, source: str | None = None) -> int:
        """Count events, optionally filtered by source."""
        with storage_errors("Ledger count"):
            if source is None:
                row = self._conn.execute("SELECT COUNT(*) FROM ledger").fetchone()
            else:
                row = self._conn.execute(
                    "SELECT COUNT(*) FROM ledger WHERE source = ?", (source,),
                ).fetchone()
            return row[0] if row else 0

    def events_for(self, entity_id: str) -> list[dict[str, Any]]:
        """Get event summaries for an entity via event_entities join."""
        with storage_errors("Ledger events for entity"):
            rows = self._conn.execute(
                "SELECT l.sequence, l.event_id, l.event_type, l.timestamp"
                " FROM ledger l"
                " JOIN event_entities ee ON l.sequence = ee.sequence"
                " WHERE ee.entity_id = ?"
                " ORDER BY l.timestamp, l.sequence",
                (entity_id,),
            ).fetchall()
            return [
                {"sequence": r[0], "event_id": r[1], "event_type": r[2], "timestamp": r[3]}
                for r in rows
            ]

    def existing_entity_ids(self, entity_ids: list[str]) -> set[str]:
        """Batch check which entities still have events."""
        if not entity_ids:
            return set()
        placeholders = ",".join("?" * len(entity_ids))
        with storage_errors("Ledger existing entity IDs"):
            rows = self._conn.execute(
                f"SELECT DISTINCT entity_id FROM event_entities"
                f" WHERE entity_id IN ({placeholders})",
                entity_ids,
            ).fetchall()
            return {r[0] for r in rows}

    def all_entity_mappings(self) -> dict[str, str]:
        """All entity_id → entity_type from event_entities."""
        with storage_errors("Ledger all entity mappings"):
            rows = self._conn.execute(
                "SELECT DISTINCT entity_id, entity_type FROM event_entities",
            ).fetchall()
            return {r[0]: r[1] for r in rows}

    def delete_events_for(self, entity_id: str) -> int:
        """Delete event_entities for an entity, then orphaned ledger rows.

        Multi-entity events (hints for customer + order) produce
        event_entities rows for each entity. Only delete the ledger row
        when no entity references it anymore.
        """
        with storage_errors("Ledger delete events"), self._conn:
            seqs = self._conn.execute(
                "SELECT sequence FROM event_entities WHERE entity_id = ?",
                (entity_id,),
            ).fetchall()
            if not seqs:
                return 0
            seq_list = [s[0] for s in seqs]

            # Remove this entity's event_entities rows
            self._conn.execute(
                "DELETE FROM event_entities WHERE entity_id = ?",
                (entity_id,),
            )

            # Only delete ledger rows with zero remaining references
            placeholders = ",".join("?" * len(seq_list))
            cur = self._conn.execute(
                f"DELETE FROM ledger WHERE sequence IN ({placeholders})"
                f" AND sequence NOT IN ("
                f"SELECT sequence FROM event_entities"
                f" WHERE sequence IN ({placeholders}))",
                seq_list + seq_list,
            )
            return cur.rowcount

    def lookup_entity_type(self, entity_id: str) -> str | None:
        """Find entity type from event_entities."""
        with storage_errors("Ledger lookup entity type"):
            row = self._conn.execute(
                "SELECT entity_type FROM event_entities WHERE entity_id = ? LIMIT 1",
                (entity_id,),
            ).fetchone()
            return row[0] if row else None

    # ── Merge log ──

    def write_merge_log(
        self, from_entity_id: str, into_entity_id: str, timestamp: str,
    ) -> None:
        """Record a merge in the merge log."""
        with storage_errors("Merge log write"), self._conn:
            self._conn.execute(
                "INSERT OR IGNORE INTO merge_log"
                " (from_entity_id, into_entity_id, timestamp)"
                " VALUES (?, ?, ?)",
                (from_entity_id, into_entity_id, timestamp),
            )

    def find_merges_into(self, entity_id: str) -> list[str]:
        """Find entities that were merged into entity_id."""
        with storage_errors("Merge log read"):
            rows = self._conn.execute(
                "SELECT from_entity_id FROM merge_log"
                " WHERE into_entity_id = ?",
                (entity_id,),
            ).fetchall()
            return [r[0] for r in rows]

    def find_merge_losers(self, entity_ids: list[str]) -> set[str]:
        """Check which entity IDs are merge losers."""
        if not entity_ids:
            return set()
        placeholders = ",".join("?" * len(entity_ids))
        with storage_errors("Merge log loser check"):
            rows = self._conn.execute(
                f"SELECT DISTINCT from_entity_id FROM merge_log"
                f" WHERE from_entity_id IN ({placeholders})",
                entity_ids,
            ).fetchall()
            return {r[0] for r in rows}

    def delete_merge_log(self, entity_id: str) -> None:
        """Delete merge log entries involving entity_id."""
        with storage_errors("Merge log delete"), self._conn:
            self._conn.execute(
                "DELETE FROM merge_log"
                " WHERE from_entity_id = ? OR into_entity_id = ?",
                (entity_id, entity_id),
            )

    def delete_merge_log_batch(self, entity_ids: list[str]) -> None:
        """Delete merge log entries for multiple entities in one query."""
        if not entity_ids:
            return
        placeholders = ",".join("?" * len(entity_ids))
        with storage_errors("Merge log batch delete"), self._conn:
            self._conn.execute(
                f"DELETE FROM merge_log"
                f" WHERE from_entity_id IN ({placeholders})"
                f" OR into_entity_id IN ({placeholders})",
                entity_ids + entity_ids,
            )

    # ── Transaction control ──

    # begin() and commit() are inherited as no-ops from LedgerBackend.
    # SQLite allows one writer at a time — explicit BEGIN would block
    # other backends sharing this connection unnecessarily.

    @property
    def connection(self) -> Any:
        """The underlying sqlite3 connection."""
        return self._conn

    def close(self) -> None:
        """Close the database connection."""
        self._conn.close()


class PostgresLedger(LedgerBackend):
    """Postgres ledger backend — production.

    Uses JSONB for data/raw/resolution_hints (native indexing, server-side
    queries). TIMESTAMPTZ for timestamps. Tables in shared `defacto` schema.
    Bulk writes via unnest arrays with ON CONFLICT for idempotent appends —
    zero DDL overhead, one round trip per operation regardless of batch size.
    """

    def __init__(self, db_path: str, *, namespace: str = "defacto") -> None:
        """Initialize with a Postgres connection string.

        Creates the schema and ledger/event_entities tables if they
        don't exist.

        Args:
            db_path: Postgres connection string (e.g., "postgresql://user:pass@host/db").
            namespace: Schema prefix for tables. Default 'defacto'.
        """
        import psycopg
        from psycopg.types.json import Jsonb

        self._conn = psycopg.connect(db_path)
        self._conn.execute("SET timezone = 'UTC'")
        self._Jsonb = Jsonb
        self._in_transaction = False
        self._ns = namespace
        self._create_tables()

    def _create_tables(self) -> None:
        """Create the namespace schema and ledger tables if they don't exist."""
        ns = self._ns
        with self._conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {ns}")
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {ns}.ledger (
                    sequence          BIGSERIAL   PRIMARY KEY,
                    event_id          TEXT        NOT NULL,
                    event_type        TEXT        NOT NULL,
                    timestamp         TIMESTAMPTZ NOT NULL,
                    source            TEXT        NOT NULL,
                    data              JSONB       NOT NULL,
                    raw               JSONB       NOT NULL,
                    resolution_hints  JSONB       NOT NULL,
                    duplicate_of      BIGINT
                )
            """)
            cur.execute(f"""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_ledger_event_id_unique
                    ON {ns}.ledger(event_id) WHERE duplicate_of IS NULL
            """)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {ns}.event_entities (
                    sequence    BIGINT  NOT NULL REFERENCES {ns}.ledger(sequence),
                    entity_id   TEXT    NOT NULL,
                    entity_type TEXT    NOT NULL,
                    shard_hash  BIGINT  NOT NULL DEFAULT 0,
                    PRIMARY KEY (sequence, entity_id)
                )
            """)
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_event_entities_entity
                    ON {ns}.event_entities(entity_id)
            """)
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_ledger_timestamp
                    ON {ns}.ledger(timestamp, sequence)
            """)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {ns}.merge_log (
                    from_entity_id TEXT        NOT NULL,
                    into_entity_id TEXT        NOT NULL,
                    timestamp      TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY (from_entity_id, into_entity_id)
                )
            """)
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_merge_log_into
                    ON {ns}.merge_log(into_entity_id)
            """)
        self._conn.commit()

    def append_batch(self, events: list[dict[str, Any]]) -> list[int]:
        """Append events via unnest arrays with ON CONFLICT dedup.

        Pipeline mode: INSERT + SELECT in one network round trip.
        Retries on deadlock (concurrent B-tree index contention).
        """
        if not events:
            return []

        Jsonb = self._Jsonb
        event_ids = [e["event_id"] for e in events]
        event_types = [e["event_type"] for e in events]
        timestamps = [e["timestamp"] for e in events]
        sources = [e["source"] for e in events]
        data_arr = [Jsonb(e["data"]) for e in events]
        raw_arr = [Jsonb(e["raw"]) for e in events]
        hints_arr = [Jsonb(e["resolution_hints"]) for e in events]

        for attempt in range(3):
            try:
                with storage_errors("Ledger append"), self._conn.pipeline():
                    with self._conn.cursor() as cur:
                        cur.execute(
                            f"INSERT INTO {self._ns}.ledger"
                            " (event_id, event_type, timestamp, source,"
                            " data, raw, resolution_hints)"
                            " SELECT * FROM unnest("
                            "   %s::text[], %s::text[], %s::timestamptz[],"
                            "   %s::text[], %s::jsonb[], %s::jsonb[],"
                            "   %s::jsonb[]"
                            " ) ON CONFLICT (event_id)"
                            " WHERE duplicate_of IS NULL DO NOTHING",
                            (event_ids, event_types, timestamps, sources,
                             data_arr, raw_arr, hints_arr),
                        )
                        cur.execute(
                            f"SELECT event_id, sequence FROM {self._ns}.ledger"
                            " WHERE event_id = ANY(%s)",
                            (event_ids,),
                        )
                        id_to_seq = dict(cur.fetchall())

                self._auto_commit()
                return [id_to_seq[eid] for eid in event_ids
                        if eid in id_to_seq]

            except StorageError as e:
                if "deadlock" in str(e).lower() and attempt < 2:
                    self._conn.rollback()
                    continue
                raise

    def write_event_entities(self, mappings: list[tuple[int, str, str]]) -> None:
        """Write event-entity mappings with pre-computed shard hash.

        One INSERT round trip via unnest regardless of batch size.
        Idempotent — duplicates from prior builds or process_batch
        ingests are skipped.
        """
        if not mappings:
            return

        sequences = [m[0] for m in mappings]
        entity_ids = [m[1] for m in mappings]
        entity_types = [m[2] for m in mappings]
        shard_hashes = [
            int.from_bytes(hashlib.sha256(eid.encode()).digest()[:4])
            for eid in entity_ids
        ]

        with storage_errors("Event entities write"), self._conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO {self._ns}.event_entities"
                " (sequence, entity_id, entity_type, shard_hash)"
                " SELECT * FROM unnest(%s::bigint[], %s::text[],"
                " %s::text[], %s::bigint[])"
                " ON CONFLICT (sequence, entity_id) DO NOTHING",
                (sequences, entity_ids, entity_types, shard_hashes),
            )

        self._auto_commit()

    def replay(
        self, from_sequence: int = 0, *, include_duplicates: bool = False,
    ) -> Iterator[dict[str, Any]]:
        """Stream events using a server-side cursor for bounded memory.

        Ordered by timestamp (with sequence as tiebreaker) for correct
        chronological processing during rebuilds. JSONB columns are
        automatically deserialized to Python dicts by psycopg.
        """
        dup_filter = "" if include_duplicates else " AND duplicate_of IS NULL"
        # Server-side cursor for streaming without loading all rows
        with storage_errors("Ledger replay"), self._conn.cursor(name="replay_cursor") as cur:
            cur.itersize = 1000
            cur.execute(
                "SELECT sequence, event_id, event_type,"
                " to_char(timestamp, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') as timestamp,"
                " source, data, raw, resolution_hints"
                f" FROM {self._ns}.ledger WHERE sequence >= %s{dup_filter}"
                " ORDER BY timestamp, sequence",
                (from_sequence,),
            )
            for row in cur:
                yield {
                    "sequence": row[0],
                    "event_id": row[1],
                    "event_type": row[2],
                    "timestamp": row[3],
                    "source": row[4],
                    "data": row[5],      # JSONB → dict automatically
                    "raw": row[6],
                    "resolution_hints": row[7],
                }

    def replay_for_entities(self, entity_ids: list[str]) -> Iterator[dict[str, Any]]:
        """Replay events for specific entities via event_entities join.

        Uses a server-side cursor. Ordered by timestamp for correct
        chronological processing during partial rebuilds.
        """
        if not entity_ids:
            return

        with storage_errors("Ledger replay for entities"), self._conn.cursor(name="entity_replay_cursor") as cur:
            cur.itersize = 1000
            # Postgres requires ORDER BY columns in SELECT list with DISTINCT.
            # Use a subquery to deduplicate, then order the outer query.
            cur.execute(
                "SELECT sequence, event_id, event_type,"
                " to_char(timestamp, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') as timestamp,"
                " source, data, raw, resolution_hints"
                f" FROM {self._ns}.ledger"
                " WHERE sequence IN ("
                f"   SELECT DISTINCT ee.sequence FROM {self._ns}.event_entities ee"
                "   WHERE ee.entity_id = ANY(%s)"
                " )"
                " AND duplicate_of IS NULL"
                " ORDER BY timestamp, sequence",
                (entity_ids,),
            )
            for row in cur:
                yield {
                    "sequence": row[0],
                    "event_id": row[1],
                    "event_type": row[2],
                    "timestamp": row[3],
                    "source": row[4],
                    "data": row[5],
                    "raw": row[6],
                    "resolution_hints": row[7],
                }

    def replay_for_shard(
        self, shard_id: int, total_shards: int, from_sequence: int = 0,
    ) -> Iterator[dict[str, Any]]:
        """Replay events for entities owned by a shard.

        JOINs event_entities for the shard filter and includes the
        pre-resolved entity_id and entity_type — the build skips
        identity resolution entirely since the mapping is attached.
        """
        with storage_errors("Ledger replay for shard"), self._conn.cursor(name="shard_replay_cursor") as cur:
            cur.itersize = 1000
            cur.execute(
                "SELECT l.sequence, l.event_id, l.event_type,"
                " to_char(l.timestamp, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'),"
                " l.source, l.data, l.raw, l.resolution_hints,"
                " ee.entity_id, ee.entity_type"
                f" FROM {self._ns}.ledger l"
                f" JOIN {self._ns}.event_entities ee ON l.sequence = ee.sequence"
                " WHERE ee.shard_hash %% %s = %s"
                " AND l.sequence >= %s"
                " AND l.duplicate_of IS NULL"
                " ORDER BY l.timestamp, l.sequence",
                (total_shards, shard_id, from_sequence),
            )
            for row in cur:
                yield {
                    "sequence": row[0],
                    "event_id": row[1],
                    "event_type": row[2],
                    "timestamp": row[3],
                    "source": row[4],
                    "data": row[5],
                    "raw": row[6],
                    "resolution_hints": row[7],
                    "entity_id": row[8],
                    "entity_type": row[9],
                }

    def has_event_entities(self) -> bool:
        """Check if event_entities has any rows."""
        with storage_errors("Event entities check"), self._conn.cursor() as cur:
            cur.execute(f"SELECT 1 FROM {self._ns}.event_entities LIMIT 1")
            return cur.fetchone() is not None

    def cursor(self) -> int:
        """Return the highest sequence number, or 0 if empty."""
        with storage_errors("Ledger cursor"), self._conn.cursor() as cur:
            cur.execute(f"SELECT MAX(sequence) FROM {self._ns}.ledger")
            row = cur.fetchone()
            return row[0] if row[0] is not None else 0

    def redact(self, entity_id: str, sensitive_fields: list[str]) -> int:
        """Redact sensitive fields using Postgres jsonb_set.

        Each field is replaced with "[REDACTED]" in both data and raw
        columns. Scoped to events via event_entities join.
        """
        if not sensitive_fields:
            return 0

        # Build chained jsonb_set calls: jsonb_set(jsonb_set(col, ...), ...)
        data_expr = "data"
        raw_expr = "raw"
        for field in sensitive_fields:
            path = "'{" + field + "}'"
            data_expr = f"jsonb_set({data_expr}, {path}, '\"[REDACTED]\"'::jsonb)"
            raw_expr = f"jsonb_set({raw_expr}, {path}, '\"[REDACTED]\"'::jsonb)"

        with storage_errors("Redact events"), self._conn.cursor() as cur:
            cur.execute(
                f"UPDATE {self._ns}.ledger SET data = {data_expr}, raw = {raw_expr}"
                f" WHERE sequence IN ("
                f"   SELECT sequence FROM {self._ns}.event_entities WHERE entity_id = %s"
                f" )",
                (entity_id,),
            )
            count = cur.rowcount

        self._auto_commit()
        return count

    def update_normalized(self, updates: list[tuple[int, dict]]) -> None:
        """Batch UPDATE ledger rows with re-normalized data via unnest.

        One UPDATE FROM unnest round trip regardless of batch size.
        Updates data, event_id, event_type, resolution_hints, and
        duplicate_of columns. The raw column is unchanged — it's the
        immutable source of truth that renormalization reads from.
        duplicate_of is set by Rust: NULL for canonical events,
        canonical_sequence for duplicates.
        """
        if not updates:
            return


        Jsonb = self._Jsonb
        sequences = [seq for seq, _ in updates]
        event_ids = [row["event_id"] for _, row in updates]
        event_types = [row["event_type"] for _, row in updates]
        data_arr = [Jsonb(row["data"]) for _, row in updates]
        hints_arr = [Jsonb(row["resolution_hints"]) for _, row in updates]
        dup_arr = [row.get("duplicate_of") for _, row in updates]

        with storage_errors("Ledger update normalized"), self._conn.cursor() as cur:
            cur.execute(
                f"UPDATE {self._ns}.ledger SET"
                " event_id = s.event_id, event_type = s.event_type,"
                " data = s.data, resolution_hints = s.resolution_hints,"
                " duplicate_of = s.duplicate_of"
                " FROM unnest("
                "   %s::bigint[], %s::text[], %s::text[],"
                "   %s::jsonb[], %s::jsonb[], %s::bigint[]"
                " ) AS s(sequence, event_id, event_type, data, resolution_hints, duplicate_of)"
                f" WHERE {self._ns}.ledger.sequence = s.sequence",
                (sequences, event_ids, event_types, data_arr, hints_arr, dup_arr),
            )

        self._auto_commit()

    def clear_event_entities(self) -> None:
        """Truncate all event-entity mappings.

        TRUNCATE is faster than DELETE on Postgres (no per-row WAL).
        """
        with storage_errors("Clear event entities"), self._conn.cursor() as cur:
            cur.execute(f"TRUNCATE {self._ns}.event_entities")
        self._auto_commit()

    def merge_event_entities(self, loser_id: str, winner_id: str) -> None:
        """Reassign event_entities from loser to winner after a merge.

        Deletes loser rows that conflict with existing winner rows (same
        sequence) before updating, to avoid UNIQUE constraint violations
        when both entities share events from the same batch.
        """
        winner_hash = int.from_bytes(
            hashlib.sha256(winner_id.encode()).digest()[:4]
        )
        with storage_errors("Merge event entities"), self._conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {self._ns}.event_entities"
                " WHERE entity_id = %s AND sequence IN ("
                f"   SELECT sequence FROM {self._ns}.event_entities"
                "   WHERE entity_id = %s"
                " )",
                (loser_id, winner_id),
            )
            cur.execute(
                f"UPDATE {self._ns}.event_entities"
                " SET entity_id = %s, shard_hash = %s"
                " WHERE entity_id = %s",
                (winner_id, winner_hash, loser_id),
            )
        self._auto_commit()

    def count(self, source: str | None = None) -> int:
        """Count events, optionally filtered by source."""
        with storage_errors("Ledger count"), self._conn.cursor() as cur:
            if source is None:
                cur.execute(f"SELECT COUNT(*) FROM {self._ns}.ledger")
            else:
                cur.execute(
                    f"SELECT COUNT(*) FROM {self._ns}.ledger WHERE source = %s",
                    (source,),
                )
            row = cur.fetchone()
        return row[0] if row else 0

    def events_for(self, entity_id: str) -> list[dict[str, Any]]:
        """Get event summaries for an entity via event_entities join."""
        with storage_errors("Ledger events for entity"), self._conn.cursor() as cur:
            cur.execute(
                "SELECT l.sequence, l.event_id, l.event_type,"
                " to_char(l.timestamp, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')"
                f" FROM {self._ns}.ledger l"
                f" JOIN {self._ns}.event_entities ee ON l.sequence = ee.sequence"
                " WHERE ee.entity_id = %s"
                " ORDER BY l.timestamp, l.sequence",
                (entity_id,),
            )
            rows = cur.fetchall()
        return [
            {"sequence": r[0], "event_id": r[1], "event_type": r[2], "timestamp": r[3]}
            for r in rows
        ]

    def existing_entity_ids(self, entity_ids: list[str]) -> set[str]:
        """Batch check which entities still have events."""
        if not entity_ids:
            return set()
        with storage_errors("Ledger existing entity IDs"), self._conn.cursor() as cur:
            cur.execute(
                f"SELECT DISTINCT entity_id FROM {self._ns}.event_entities"
                " WHERE entity_id = ANY(%s)",
                (entity_ids,),
            )
            return {r[0] for r in cur.fetchall()}

    def all_entity_mappings(self) -> dict[str, str]:
        """All entity_id → entity_type from event_entities."""
        with storage_errors("Ledger all entity mappings"), self._conn.cursor() as cur:
            cur.execute(
                f"SELECT DISTINCT entity_id, entity_type"
                f" FROM {self._ns}.event_entities",
            )
            return {r[0]: r[1] for r in cur.fetchall()}

    def delete_events_for(self, entity_id: str) -> int:
        """Delete event_entities for an entity, then orphaned ledger rows.

        Multi-entity events (hints for customer + order) produce
        event_entities rows for each entity. Only delete the ledger row
        when no entity references it anymore.
        """
        with storage_errors("Ledger delete events"), self._conn.cursor() as cur:
            cur.execute(
                f"SELECT sequence FROM {self._ns}.event_entities"
                f" WHERE entity_id = %s",
                (entity_id,),
            )
            seqs = [r[0] for r in cur.fetchall()]
            if not seqs:
                return 0

            # Remove this entity's event_entities rows
            cur.execute(
                f"DELETE FROM {self._ns}.event_entities WHERE entity_id = %s",
                (entity_id,),
            )

            # Only delete ledger rows with zero remaining references
            cur.execute(
                f"DELETE FROM {self._ns}.ledger"
                f" WHERE sequence = ANY(%s)"
                f" AND sequence NOT IN ("
                f"SELECT sequence FROM {self._ns}.event_entities"
                f" WHERE sequence = ANY(%s))",
                (seqs, seqs),
            )
            count = cur.rowcount
        self._auto_commit()
        return count

    def lookup_entity_type(self, entity_id: str) -> str | None:
        """Find entity type from event_entities."""
        with storage_errors("Ledger lookup entity type"), self._conn.cursor() as cur:
            cur.execute(
                f"SELECT entity_type FROM {self._ns}.event_entities"
                " WHERE entity_id = %s LIMIT 1",
                (entity_id,),
            )
            row = cur.fetchone()
        return row[0] if row else None

    # ── Merge log ──

    def write_merge_log(
        self, from_entity_id: str, into_entity_id: str, timestamp: str,
    ) -> None:
        """Record a merge in the merge log."""
        with storage_errors("Merge log write"), self._conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO {self._ns}.merge_log"
                " (from_entity_id, into_entity_id, timestamp)"
                " VALUES (%s, %s, %s)"
                " ON CONFLICT DO NOTHING",
                (from_entity_id, into_entity_id, timestamp),
            )
        self._auto_commit()

    def find_merges_into(self, entity_id: str) -> list[str]:
        """Find entities that were merged into entity_id."""
        with storage_errors("Merge log read"), self._conn.cursor() as cur:
            cur.execute(
                f"SELECT from_entity_id FROM {self._ns}.merge_log"
                " WHERE into_entity_id = %s",
                (entity_id,),
            )
            return [r[0] for r in cur.fetchall()]

    def find_merge_losers(self, entity_ids: list[str]) -> set[str]:
        """Check which entity IDs are merge losers."""
        if not entity_ids:
            return set()
        with storage_errors("Merge log loser check"), self._conn.cursor() as cur:
            cur.execute(
                f"SELECT DISTINCT from_entity_id FROM {self._ns}.merge_log"
                " WHERE from_entity_id = ANY(%s)",
                (entity_ids,),
            )
            return {r[0] for r in cur.fetchall()}

    def delete_merge_log(self, entity_id: str) -> None:
        """Delete merge log entries involving entity_id."""
        with storage_errors("Merge log delete"), self._conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {self._ns}.merge_log"
                " WHERE from_entity_id = %s OR into_entity_id = %s",
                (entity_id, entity_id),
            )
        self._auto_commit()

    def delete_merge_log_batch(self, entity_ids: list[str]) -> None:
        """Delete merge log entries for multiple entities in one query."""
        if not entity_ids:
            return
        with storage_errors("Merge log batch delete"), self._conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {self._ns}.merge_log"
                " WHERE from_entity_id = ANY(%s) OR into_entity_id = ANY(%s)",
                (entity_ids, entity_ids),
            )
        self._auto_commit()

    # ── Transaction control ──

    def begin(self) -> None:
        """Start an explicit transaction — defer commits until commit()."""
        self._in_transaction = True

    def commit(self) -> None:
        """Commit the current transaction."""
        self._conn.commit()
        self._in_transaction = False

    def _auto_commit(self) -> None:
        """Commit if not inside an explicit transaction."""
        if not self._in_transaction:
            self._conn.commit()

    @property
    def connection(self) -> Any:
        """The underlying psycopg connection."""
        return self._conn

    def close(self) -> None:
        """Close the database connection."""
        self._conn.close()


def _delta_ledger_schema():
    """PyArrow schema for cold ledger Delta table.

    Explicit schema prevents type inference issues (e.g., duplicate_of
    inferred as Null when all values are None). Shared by flush, redact,
    and update_normalized.
    """
    import pyarrow as pa
    return pa.schema([
        ("sequence", pa.int64()),
        ("event_id", pa.string()),
        ("event_type", pa.string()),
        ("timestamp", pa.string()),
        ("source", pa.string()),
        ("data", pa.string()),
        ("raw", pa.string()),
        ("resolution_hints", pa.string()),
        ("duplicate_of", pa.int64()),
    ])


class TieredLedger(LedgerBackend):
    """Tiered ledger — hot backend + cold Delta Lake.

    Wraps any LedgerBackend (hot) and a Delta Lake table (cold). The
    pipeline doesn't know it's tiered — all routing is internal. Hot
    handles real-time operations (ingest, incremental builds, dedup).
    Cold stores the complete archive for full rebuilds.

    Permanent structures in hot (never pruned):
    - event_ids: dedup index that survives ledger pruning
    - event_entities: entity-event mapping for partial rebuilds
    - tiered_state: coordination metadata (last_flushed_sequence)

    The ledger table itself is prunable — old events are flushed to
    cold then pruned from hot to keep the hot tier bounded.
    """

    def __init__(self, hot: LedgerBackend, cold_path: str) -> None:
        """Initialize with a hot ledger backend and cold storage path.

        Args:
            hot: Any LedgerBackend implementation for hot storage.
            cold_path: Local path or S3 URL for cold Delta Lake table.
        """
        import sqlite3
        self._hot = hot
        self._cold_path = cold_path
        self._is_sqlite = isinstance(hot.connection, sqlite3.Connection)
        self._schema = "" if self._is_sqlite else f"{getattr(hot, '_ns', 'defacto')}."
        self._create_tiered_tables()
        self._last_flushed_seq = self._read_tiered_state("last_flushed_sequence")
        self._reconcile_flush_state()

    def _create_tiered_tables(self) -> None:
        """Create tiered-specific tables and drop the event_entities FK.

        event_ids: permanent dedup index (survives ledger pruning).
        tiered_state: coordination metadata.
        FK drop: event_entities rows outlive their ledger rows after pruning.
        """
        s = self._schema
        conn = self._hot.connection
        with storage_errors("Tiered ledger create tables"):
            if self._is_sqlite:
                conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {s}event_ids (
                        event_id TEXT PRIMARY KEY,
                        sequence INTEGER NOT NULL
                    )
                """)
                conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {s}tiered_state (
                        key   TEXT PRIMARY KEY,
                        value TEXT NOT NULL
                    )
                """)
                conn.execute(f"""
                    INSERT OR IGNORE INTO {s}tiered_state (key, value)
                    VALUES ('last_flushed_sequence', '0')
                """)
                conn.commit()
            else:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        CREATE TABLE IF NOT EXISTS {s}event_ids (
                            event_id TEXT PRIMARY KEY,
                            sequence BIGINT NOT NULL
                        )
                    """)
                    cur.execute(f"""
                        CREATE TABLE IF NOT EXISTS {s}tiered_state (
                            key   TEXT PRIMARY KEY,
                            value TEXT NOT NULL
                        )
                    """)
                    cur.execute(f"""
                        INSERT INTO {s}tiered_state (key, value)
                        VALUES ('last_flushed_sequence', '0')
                        ON CONFLICT (key) DO NOTHING
                    """)
                    # Drop FK on event_entities — pruned ledger rows would
                    # violate it. SQLite doesn't enforce FK constraints the
                    # same way, so this is Postgres-only.
                    cur.execute(f"""
                        ALTER TABLE {s}event_entities
                        DROP CONSTRAINT IF EXISTS event_entities_sequence_fkey
                    """)
                conn.commit()

    def _read_tiered_state(self, key: str) -> int:
        """Read an integer value from the tiered_state table."""
        s = self._schema
        conn = self._hot.connection
        with storage_errors("Tiered state read"):
            if self._is_sqlite:
                row = conn.execute(
                    f"SELECT value FROM {s}tiered_state WHERE key = ?", (key,),
                ).fetchone()
            else:
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT value FROM {s}tiered_state WHERE key = %s",
                        (key,),
                    )
                    row = cur.fetchone()
            return int(row[0]) if row else 0

    def _update_tiered_state(self, key: str, value: str) -> None:
        """Update a value in the tiered_state table."""
        s = self._schema
        conn = self._hot.connection
        with storage_errors("Tiered state update"):
            if self._is_sqlite:
                conn.execute(
                    f"UPDATE {s}tiered_state SET value = ? WHERE key = ?",
                    (value, key),
                )
            else:
                with conn.cursor() as cur:
                    cur.execute(
                        f"UPDATE {s}tiered_state SET value = %s WHERE key = %s",
                        (value, key),
                    )
            conn.commit()

    def _reconcile_flush_state(self) -> None:
        """Recover from a crash during flush.

        If the process died after writing to Delta but before updating
        last_flushed_sequence in Postgres, cold has sequences beyond
        what Postgres knows about. Check and reconcile.
        """
        if not os.path.exists(self._cold_path):
            return
        try:
            import deltalake
            dt = deltalake.DeltaTable(self._cold_path)
            dataset = dt.to_pyarrow_dataset()
            table = dataset.to_table(columns=["sequence"])
            if len(table) == 0:
                return
            max_seq = max(table.column("sequence").to_pylist())
            if max_seq > self._last_flushed_seq:
                self._update_tiered_state("last_flushed_sequence", str(max_seq))
                self._last_flushed_seq = max_seq
        except FileNotFoundError:
            pass  # No cold table yet — first run
        except deltalake.DeltaError:
            pass  # Cold table doesn't exist as Delta yet — first flush will create it

    # ── Hot vs cold detection ──

    def _sequences_in_hot(self, sequences: list[int]) -> set[int]:
        """Determine which sequences are in the hot tier.

        Uses the flush boundary — sequences above last_flushed_seq are
        in hot, the rest are in cold. No database query needed.
        """
        return {s for s in sequences if s > self._last_flushed_seq}

    def _read_hot_events(self, sequences: list[int]) -> list[dict[str, Any]]:
        """Read events from hot Postgres by sequence numbers.

        Direct read bypassing the replay methods — avoids re-querying
        event_entities when the caller already has the sequence list.
        """
        if not sequences:
            return []
        s = self._schema
        conn = self._hot.connection
        with storage_errors("Tiered ledger read hot events"):
            if self._is_sqlite:
                placeholders = ",".join("?" * len(sequences))
                rows = conn.execute(
                    f"SELECT sequence, event_id, event_type, timestamp,"
                    f" source, data, raw, resolution_hints"
                    f" FROM {s}ledger"
                    f" WHERE sequence IN ({placeholders})"
                    f" AND duplicate_of IS NULL"
                    f" ORDER BY timestamp, sequence",
                    sequences,
                ).fetchall()
            else:
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT sequence, event_id, event_type,"
                        f" to_char(timestamp, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'),"
                        f" source, data, raw, resolution_hints"
                        f" FROM {s}ledger"
                        f" WHERE sequence = ANY(%s)"
                        f" AND duplicate_of IS NULL"
                        f" ORDER BY timestamp, sequence",
                        (sequences,),
                    )
                    rows = cur.fetchall()
        return [
            {
                "sequence": row[0],
                "event_id": row[1],
                "event_type": row[2],
                "timestamp": row[3] if isinstance(row[3], str)
                    else row[3].isoformat() if row[3] else None,
                "source": row[4],
                "data": row[5],
                "raw": row[6],
                "resolution_hints": row[7],
            }
            for row in rows
        ]

    # ── Cold reads ──

    def _read_cold(
        self,
        sequences: list[int] | None = None,
        *,
        include_duplicates: bool = False,
    ) -> Iterator[dict[str, Any]]:
        """Read events from cold Delta Lake, streaming in sequence order.

        Uses Delta's file-level min/max stats to sort fragments by
        min(sequence) — pure metadata, no data reads for ordering.
        Each fragment is internally sorted (flushed with ORDER BY
        sequence) and fragments have non-overlapping sequence ranges
        (each flush appends higher sequences), so reading fragments
        in min-sequence order yields globally sorted output.

        If sequences is provided: targeted read with data skipping,
        materialized and sorted (bounded by the number of targeted events).
        """
        if not os.path.exists(self._cold_path):
            return

        import deltalake
        import pyarrow.dataset as ds

        dt = deltalake.DeltaTable(self._cold_path)

        # Build filter expression
        filter_expr = None
        if sequences is not None:
            filter_expr = ds.field("sequence").isin(sequences)
        if not include_duplicates:
            dup_filter = ds.field("duplicate_of").is_null()
            filter_expr = dup_filter if filter_expr is None else (filter_expr & dup_filter)

        if sequences is not None:
            # Targeted read — small result set, safe to materialize and sort
            dataset = dt.to_pyarrow_dataset()
            table = dataset.to_table(filter=filter_expr)
            table = table.sort_by([("timestamp", "ascending"), ("sequence", "ascending")])
            for row in table.to_pylist():
                yield self._cold_row_to_dict(row)
        else:
            # Full scan — stream fragments in sequence order. Each
            # flush produces a file with non-overlapping, monotonically
            # increasing sequences. Sort fragments by min(sequence)
            # from file-level stats, then sort each fragment by
            # (timestamp, sequence) for correct chronological replay.
            # Memory = one fragment at a time.
            dataset = dt.to_pyarrow_dataset()
            fragments = list(dataset.get_fragments())

            # Get min sequence per fragment (one-column read per file)
            frag_order = []
            for frag in fragments:
                seq_col = frag.to_table(columns=["sequence"]).column("sequence")
                frag_order.append((min(seq_col.to_pylist()), frag))
            frag_order.sort(key=lambda x: x[0])

            for _, frag in frag_order:
                table = frag.to_table(filter=filter_expr)
                table = table.sort_by([
                    ("timestamp", "ascending"),
                    ("sequence", "ascending"),
                ])
                for row in table.to_pylist():
                    yield self._cold_row_to_dict(row)

    @staticmethod
    def _cold_row_to_dict(row: dict) -> dict[str, Any]:
        """Convert a PyArrow row dict to the event dict format.

        Delta/Parquet stores JSON columns as strings. Parse them back
        to dicts for consistency with the hot path.
        """
        return {
            "sequence": row["sequence"],
            "event_id": row["event_id"],
            "event_type": row["event_type"],
            "timestamp": str(row["timestamp"]) if row["timestamp"] else "",
            "source": row["source"],
            "data": json.loads(row["data"]) if isinstance(row["data"], str) else row["data"],
            "raw": json.loads(row["raw"]) if isinstance(row["raw"], str) else row["raw"],
            "resolution_hints": (
                json.loads(row["resolution_hints"])
                if isinstance(row["resolution_hints"], str)
                else row["resolution_hints"]
            ),
        }

    # ── LedgerBackend methods ──

    def append_batch(self, events: list[dict[str, Any]]) -> list[int]:
        """Append events to hot tier and record in event_ids.

        Pre-filters against event_ids to catch duplicates of pruned
        events that the hot ledger's UNIQUE constraint wouldn't catch.
        Then inserts survivors into both hot ledger and event_ids.
        """
        if not events:
            return []

        s = self._schema
        conn = self._hot.connection
        candidate_ids = [e["event_id"] for e in events]

        with storage_errors("Tiered ledger append"):
            # Pre-filter: check event_ids for duplicates of pruned events.
            if self._is_sqlite:
                placeholders = ",".join("?" * len(candidate_ids))
                rows = conn.execute(
                    f"SELECT event_id FROM {s}event_ids"
                    f" WHERE event_id IN ({placeholders})",
                    candidate_ids,
                ).fetchall()
                existing = {row[0] for row in rows}
            else:
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT event_id FROM {s}event_ids"
                        f" WHERE event_id = ANY(%s)",
                        (candidate_ids,),
                    )
                    existing = {row[0] for row in cur.fetchall()}

            new_events = [e for e in events if e["event_id"] not in existing]
            if not new_events:
                # All events already seen — return existing sequences
                if self._is_sqlite:
                    rows = conn.execute(
                        f"SELECT event_id, sequence FROM {s}event_ids"
                        f" WHERE event_id IN ({placeholders})",
                        candidate_ids,
                    ).fetchall()
                    id_to_seq = dict(rows)
                else:
                    with conn.cursor() as cur:
                        cur.execute(
                            f"SELECT event_id, sequence FROM {s}event_ids"
                            f" WHERE event_id = ANY(%s)",
                            (candidate_ids,),
                        )
                        id_to_seq = dict(cur.fetchall())
                return [id_to_seq[eid] for eid in candidate_ids if eid in id_to_seq]

            sequences = self._hot.append_batch(new_events)
            if sequences:
                new_ids = [e["event_id"] for e in new_events]
                if self._is_sqlite:
                    conn.executemany(
                        f"INSERT OR IGNORE INTO {s}event_ids"
                        f" (event_id, sequence) VALUES (?, ?)",
                        list(zip(new_ids, sequences)),
                    )
                else:
                    with conn.cursor() as cur:
                        cur.execute(
                            f"INSERT INTO {s}event_ids (event_id, sequence)"
                            f" SELECT * FROM unnest(%s::text[], %s::bigint[])"
                            f" ON CONFLICT (event_id) DO NOTHING",
                            (new_ids, sequences),
                        )
                self._hot.commit()
            return sequences

    def write_event_entities(self, mappings: list[tuple[int, str, str]]) -> None:
        """Delegate to hot — event_entities is permanent in hot."""
        self._hot.write_event_entities(mappings)

    def replay(
        self, from_sequence: int = 0, *, include_duplicates: bool = False,
    ) -> Iterator[dict[str, Any]]:
        """Replay events, stitching cold and hot as needed.

        from_sequence=0: full rebuild — auto-flush, yield cold then hot tail.
        from_sequence > last_flushed_seq: incremental — hot only.
        from_sequence in between: stitch — cold from that point, then hot.
        """
        if from_sequence <= self._last_flushed_seq:
            if from_sequence == 0:
                # Full rebuild — auto-flush to ensure cold is complete
                self.flush()
            # Read from cold (from the requested sequence onward)
            yield from self._read_cold(include_duplicates=include_duplicates)
            # Read hot tail (events since last flush)
            yield from self._hot.replay(
                from_sequence=self._last_flushed_seq + 1,
                include_duplicates=include_duplicates,
            )
        else:
            # Incremental — entirely in hot
            yield from self._hot.replay(
                from_sequence=from_sequence,
                include_duplicates=include_duplicates,
            )

    def replay_for_entities(self, entity_ids: list[str]) -> Iterator[dict[str, Any]]:
        """Replay events for specific entities, stitching across tiers.

        Looks up all sequences in event_entities, determines which are
        in hot vs cold, reads from each tier, merges by timestamp.
        """
        if not entity_ids:
            return

        # Get all sequences for these entities from event_entities (permanent)
        s = self._schema
        conn = self._hot.connection
        with storage_errors("Tiered ledger replay for entities"):
            if self._is_sqlite:
                placeholders = ",".join("?" * len(entity_ids))
                rows = conn.execute(
                    f"SELECT DISTINCT sequence FROM {s}event_entities"
                    f" WHERE entity_id IN ({placeholders})",
                    entity_ids,
                ).fetchall()
            else:
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT DISTINCT sequence FROM {s}event_entities"
                        f" WHERE entity_id = ANY(%s)",
                        (entity_ids,),
                    )
                    rows = cur.fetchall()
        all_seqs = [row[0] for row in rows]

        if not all_seqs:
            return

        # Determine which sequences are in hot
        hot_seqs = self._sequences_in_hot(all_seqs)
        cold_seqs = [s for s in all_seqs if s not in hot_seqs and s <= self._last_flushed_seq]

        # Read from both tiers
        cold_events = list(self._read_cold(sequences=cold_seqs)) if cold_seqs else []
        hot_events = list(self._hot.replay_for_entities(entity_ids))

        # Merge by timestamp + sequence. Both sources already filter
        # duplicate_of IS NULL — cold via PyArrow filter, hot via SQL.
        all_events = cold_events + hot_events
        all_events.sort(key=lambda e: (e.get("timestamp", ""), e.get("sequence", 0)))
        yield from all_events

    def replay_for_shard(
        self, shard_id: int, total_shards: int, from_sequence: int = 0,
    ) -> Iterator[dict[str, Any]]:
        """Replay events for a shard, stitching across tiers.

        Queries event_entities for owned sequences with entity_id mapping,
        splits into hot vs cold, reads from each tier, attaches entity_id
        to each event so the build can skip identity resolution.
        """
        # Get sequences AND entity mapping in one query
        s = self._schema
        conn = self._hot.connection
        with storage_errors("Tiered ledger replay for shard"):
            if self._is_sqlite:
                rows = conn.execute(
                    f"SELECT sequence, entity_id, entity_type"
                    f" FROM {s}event_entities"
                    f" WHERE shard_hash % ? = ?"
                    f" AND sequence >= ?",
                    (total_shards, shard_id, from_sequence),
                ).fetchall()
            else:
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT sequence, entity_id, entity_type"
                        f" FROM {s}event_entities"
                        f" WHERE shard_hash %% %s = %s"
                        f" AND sequence >= %s",
                        (total_shards, shard_id, from_sequence),
                    )
                    rows = cur.fetchall()
        seq_map = {row[0]: (row[1], row[2]) for row in rows}

        if not seq_map:
            return

        all_seqs = list(seq_map.keys())

        # Split sequences into hot vs cold
        hot_seqs = self._sequences_in_hot(all_seqs)
        cold_seqs = [
            s for s in all_seqs
            if s not in hot_seqs and s <= self._last_flushed_seq
        ]

        # Read from both tiers
        cold_events = list(self._read_cold(sequences=cold_seqs)) if cold_seqs else []
        hot_events = self._read_hot_events(list(hot_seqs))

        # Merge by timestamp + sequence, attach entity_id
        all_events = cold_events + hot_events
        all_events.sort(
            key=lambda e: (e.get("timestamp", ""), e.get("sequence", 0))
        )
        for event in all_events:
            mapping = seq_map.get(event["sequence"])
            if mapping:
                event["entity_id"] = mapping[0]
                event["entity_type"] = mapping[1]
            yield event

    def has_event_entities(self) -> bool:
        """Delegate to hot — event_entities lives in Postgres."""
        return self._hot.has_event_entities()

    def cursor(self) -> int:
        """Hot always has the latest sequence."""
        return self._hot.cursor()

    def redact(self, entity_id: str, sensitive_fields: list[str]) -> int:
        """Redact sensitive fields in both hot and cold tiers.

        Hot: delegates to PostgresLedger (SQL UPDATE).
        Cold: finds affected sequences via event_entities, reads those
        events from Delta, redacts sensitive fields in data and raw,
        writes back via Delta merge.
        """
        hot_count = self._hot.redact(entity_id, sensitive_fields)

        if not os.path.exists(self._cold_path) or not sensitive_fields:
            return hot_count

        # Find sequences for this entity that are in cold
        s = self._schema
        conn = self._hot.connection
        if self._is_sqlite:
            rows = conn.execute(
                f"SELECT DISTINCT sequence FROM {s}event_entities"
                f" WHERE entity_id = ?",
                (entity_id,),
            ).fetchall()
        else:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT DISTINCT sequence FROM {s}event_entities"
                    f" WHERE entity_id = %s",
                    (entity_id,),
                )
                rows = cur.fetchall()
        all_seqs = [row[0] for row in rows]

        cold_seqs = [s for s in all_seqs if s not in self._sequences_in_hot(all_seqs) and s <= self._last_flushed_seq]
        if not cold_seqs:
            return hot_count

        import deltalake
        import pyarrow as pa

        dt = deltalake.DeltaTable(self._cold_path)
        dataset = dt.to_pyarrow_dataset()
        table = dataset.to_table(
            filter=pa.compute.field("sequence").isin(cold_seqs),
        )
        rows = table.to_pylist()
        for row in rows:
            # Redact sensitive fields in data and raw (both are JSON strings)
            data = json.loads(row["data"]) if isinstance(row["data"], str) else row["data"]
            raw = json.loads(row["raw"]) if isinstance(row["raw"], str) else row["raw"]
            for field in sensitive_fields:
                if field in data:
                    data[field] = "[REDACTED]"
                if field in raw:
                    raw[field] = "[REDACTED]"
            row["data"] = json.dumps(data)
            row["raw"] = json.dumps(raw)

        if rows:
            updated_table = pa.Table.from_pylist(rows, schema=_delta_ledger_schema())
            (
                dt.merge(
                    updated_table,
                    predicate="s.sequence = t.sequence",
                    source_alias="s",
                    target_alias="t",
                )
                .when_matched_update_all()
                .execute()
            )

        return hot_count + len(rows)

    def update_normalized(self, updates: list[tuple[int, dict]]) -> None:
        """Update normalized data, splitting between hot and cold.

        Sequences in hot: delegate to PostgresLedger.
        Sequences only in cold: update via Delta Lake merge.
        """
        if not updates:
            return

        # Split by what's in hot
        all_seqs = [seq for seq, _ in updates]
        hot_seqs = self._sequences_in_hot(all_seqs)

        hot_updates = [(seq, row) for seq, row in updates if seq in hot_seqs]
        cold_updates = [(seq, row) for seq, row in updates if seq not in hot_seqs]

        if hot_updates:
            self._hot.update_normalized(hot_updates)

        if cold_updates:
            self._update_cold_normalized(cold_updates)

    def _update_cold_normalized(self, updates: list[tuple[int, dict]]) -> None:
        """Update normalized data for events in cold Delta Lake.

        Reads affected rows, applies updates, writes back via Delta
        merge. Used by update_normalized when some sequences have been
        pruned from hot and only exist in cold.
        """
        if not os.path.exists(self._cold_path):
            return

        import deltalake
        import pyarrow as pa

        dt = deltalake.DeltaTable(self._cold_path)
        cold_seqs = [seq for seq, _ in updates]
        cold_map = {seq: row for seq, row in updates}

        dataset = dt.to_pyarrow_dataset()
        table = dataset.to_table(
            filter=pa.compute.field("sequence").isin(cold_seqs),
        )
        rows = table.to_pylist()
        for row in rows:
            update = cold_map.get(row["sequence"])
            if update:
                row["event_id"] = update["event_id"]
                row["event_type"] = update["event_type"]
                row["data"] = json.dumps(update["data"])
                row["resolution_hints"] = json.dumps(update["resolution_hints"])
                row["duplicate_of"] = update.get("duplicate_of")

        if rows:
            updated_table = pa.Table.from_pylist(rows, schema=_delta_ledger_schema())
            (
                dt.merge(
                    updated_table,
                    predicate="s.sequence = t.sequence",
                    source_alias="s",
                    target_alias="t",
                )
                .when_matched_update_all()
                .execute()
            )

    def clear_event_entities(self) -> None:
        """Delegate to hot — event_entities lives in hot."""
        self._hot.clear_event_entities()

    def merge_event_entities(self, loser_id: str, winner_id: str) -> None:
        """Delegate to hot — event_entities lives in hot."""
        self._hot.merge_event_entities(loser_id, winner_id)

    def count(self, source: str | None = None) -> int:
        """Count events. Total from event_ids, filtered from hot only."""
        if source is None:
            s = self._schema
            conn = self._hot.connection
            with storage_errors("Tiered ledger count"):
                if self._is_sqlite:
                    row = conn.execute(
                        f"SELECT COUNT(*) FROM {s}event_ids"
                    ).fetchone()
                else:
                    with conn.cursor() as cur:
                        cur.execute(f"SELECT COUNT(*) FROM {s}event_ids")
                        row = cur.fetchone()
                return row[0] if row else 0
        return self._hot.count(source)

    def events_for(self, entity_id: str) -> list[dict[str, Any]]:
        """Event summaries, stitching across tiers if needed."""
        # Get sequences from event_entities
        s = self._schema
        conn = self._hot.connection
        with storage_errors("Tiered ledger events for entity"):
            if self._is_sqlite:
                rows = conn.execute(
                    f"SELECT DISTINCT sequence FROM {s}event_entities"
                    f" WHERE entity_id = ?",
                    (entity_id,),
                ).fetchall()
            else:
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT DISTINCT sequence FROM {s}event_entities"
                        f" WHERE entity_id = %s",
                        (entity_id,),
                    )
                    rows = cur.fetchall()
        all_seqs = [row[0] for row in rows]

        if not all_seqs:
            return []

        hot_seqs = self._sequences_in_hot(all_seqs)
        cold_seqs = [s for s in all_seqs if s not in hot_seqs and s <= self._last_flushed_seq]

        results = []

        # Hot events
        if hot_seqs:
            results.extend(self._hot.events_for(entity_id))

        # Cold events (summaries only — sequence, event_id, event_type, timestamp)
        if cold_seqs:
            for event in self._read_cold(sequences=cold_seqs):
                results.append({
                    "sequence": event["sequence"],
                    "event_id": event["event_id"],
                    "event_type": event["event_type"],
                    "timestamp": event["timestamp"],
                })

        results.sort(key=lambda e: (e.get("timestamp", ""), e.get("sequence", 0)))
        return results

    def delete_events_for(self, entity_id: str) -> int:
        """Delete event_entities for an entity, then orphaned ledger rows.

        Multi-entity events produce event_entities rows for each entity.
        Only delete ledger rows (and event_ids, cold storage) when no
        entity references them anymore.
        """
        # Get sequences before hot deletes event_entities
        s = self._schema
        conn = self._hot.connection
        with storage_errors("Tiered ledger delete events"):
            if self._is_sqlite:
                rows = conn.execute(
                    f"SELECT DISTINCT sequence FROM {s}event_entities"
                    f" WHERE entity_id = ?",
                    (entity_id,),
                ).fetchall()
            else:
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT DISTINCT sequence FROM {s}event_entities"
                        f" WHERE entity_id = %s",
                        (entity_id,),
                    )
                    rows = cur.fetchall()
        all_seqs = [row[0] for row in rows]
        if not all_seqs:
            return 0

        # Hot delete: removes event_entities + orphaned ledger rows
        count = self._hot.delete_events_for(entity_id)

        # Find which sequences are truly orphaned (no remaining references)
        with storage_errors("Tiered ledger find orphans"):
            if self._is_sqlite:
                placeholders = ",".join("?" * len(all_seqs))
                remaining = conn.execute(
                    f"SELECT DISTINCT sequence FROM {s}event_entities"
                    f" WHERE sequence IN ({placeholders})",
                    all_seqs,
                ).fetchall()
            else:
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT DISTINCT sequence FROM {s}event_entities"
                        f" WHERE sequence = ANY(%s)",
                        (all_seqs,),
                    )
                    remaining = cur.fetchall()
        remaining_seqs = {r[0] for r in remaining}
        orphaned = [seq for seq in all_seqs if seq not in remaining_seqs]

        if not orphaned:
            return count

        # Delete orphaned event_ids (permanent dedup index)
        with storage_errors("Tiered ledger delete event_ids"):
            if self._is_sqlite:
                placeholders = ",".join("?" * len(orphaned))
                conn.execute(
                    f"DELETE FROM {s}event_ids"
                    f" WHERE sequence IN ({placeholders})",
                    orphaned,
                )
                conn.commit()
            else:
                with conn.cursor() as cur:
                    cur.execute(
                        f"DELETE FROM {s}event_ids"
                        f" WHERE sequence = ANY(%s)",
                        (orphaned,),
                    )
                conn.commit()

        # Delete orphaned cold sequences from Delta Lake
        cold_orphaned = [seq for seq in orphaned if seq <= self._last_flushed_seq]
        if cold_orphaned and os.path.exists(self._cold_path):
            import deltalake

            dt = deltalake.DeltaTable(self._cold_path)
            seq_list = ", ".join(str(s) for s in cold_orphaned)
            dt.delete(f"sequence IN ({seq_list})")
            count += len(cold_orphaned)

        return count

    def lookup_entity_type(self, entity_id: str) -> str | None:
        """Delegate to hot — event_entities is permanent."""
        return self._hot.lookup_entity_type(entity_id)

    def all_entity_mappings(self) -> dict[str, str]:
        """Delegate to hot — event_entities is permanent."""
        return self._hot.all_entity_mappings()

    # ── Merge log (delegate to hot) ──

    def write_merge_log(
        self, from_entity_id: str, into_entity_id: str, timestamp: str,
    ) -> None:
        """Delegate to hot — merge_log is permanent."""
        self._hot.write_merge_log(from_entity_id, into_entity_id, timestamp)

    def find_merges_into(self, entity_id: str) -> list[str]:
        """Delegate to hot — merge_log is permanent."""
        return self._hot.find_merges_into(entity_id)

    def find_merge_losers(self, entity_ids: list[str]) -> set[str]:
        """Delegate to hot — merge_log is permanent."""
        return self._hot.find_merge_losers(entity_ids)

    def delete_merge_log(self, entity_id: str) -> None:
        """Delegate to hot — merge_log is permanent."""
        self._hot.delete_merge_log(entity_id)

    def begin(self) -> None:
        """Delegate to hot."""
        self._hot.begin()

    def commit(self) -> None:
        """Delegate to hot."""
        self._hot.commit()

    @property
    def connection(self) -> Any:
        """The hot tier's database connection."""
        return self._hot.connection

    def close(self) -> None:
        """Close hot connection."""
        self._hot.close()

    # ── Tiered-specific operations (not on ABC) ──

    def flush(self) -> int:
        """Export new events from hot to cold Delta Lake.

        Reads directly from the hot ledger table (not via replay()) to
        get all columns including duplicate_of. Writes to Delta as
        PyArrow. Updates coordination state atomically.

        Returns:
            Number of events flushed.
        """
        _FLUSH_COLS = (
            "sequence", "event_id", "event_type", "timestamp",
            "source", "data", "raw", "resolution_hints", "duplicate_of",
        )
        s = self._schema
        conn = self._hot.connection
        with storage_errors("Tiered ledger flush read"):
            if self._is_sqlite:
                rows = conn.execute(
                    f"SELECT sequence, event_id, event_type, timestamp,"
                    f" source, data, raw, resolution_hints, duplicate_of"
                    f" FROM {s}ledger"
                    f" WHERE sequence > ? ORDER BY sequence",
                    (self._last_flushed_seq,),
                ).fetchall()
                events = []
                for r in rows:
                    d = dict(zip(_FLUSH_COLS, r))
                    # SQLite stores JSON as text — stringify if needed
                    for k in ("data", "raw", "resolution_hints"):
                        if d[k] is not None and not isinstance(d[k], str):
                            d[k] = json.dumps(d[k])
                    events.append(d)
            else:
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT sequence, event_id, event_type,"
                        f" to_char(timestamp, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'),"
                        f" source, data::text, raw::text,"
                        f" resolution_hints::text, duplicate_of"
                        f" FROM {s}ledger"
                        f" WHERE sequence > %s ORDER BY sequence",
                        (self._last_flushed_seq,),
                    )
                    rows = cur.fetchall()
                events = [dict(zip(_FLUSH_COLS, r)) for r in rows]

        if not events:
            return 0

        import deltalake
        import pyarrow as pa

        table = pa.Table.from_pylist(events, schema=_delta_ledger_schema())
        deltalake.write_deltalake(self._cold_path, table, mode="append")

        max_seq = max(e["sequence"] for e in events)
        self._update_tiered_state("last_flushed_sequence", str(max_seq))
        self._last_flushed_seq = max_seq

        return len(events)

    def prune(self, before_sequence: int | None = None) -> int:
        """Delete old ledger rows from hot tier.

        Keeps event_ids and event_entities (permanent). Only prunes
        rows that have been flushed to cold — refuses to prune beyond
        last_flushed_sequence for safety.

        Args:
            before_sequence: Prune rows with sequence ≤ this value.
                Default: last_flushed_sequence.

        Returns:
            Number of ledger rows pruned.
        """
        cutoff = before_sequence or self._last_flushed_seq
        if cutoff > self._last_flushed_seq:
            cutoff = self._last_flushed_seq  # Safety: never prune beyond flushed

        if cutoff <= 0:
            return 0

        s = self._schema
        conn = self._hot.connection
        with storage_errors("Tiered ledger prune"):
            if self._is_sqlite:
                cur = conn.execute(
                    f"DELETE FROM {s}ledger WHERE sequence <= ?",
                    (cutoff,),
                )
                count = cur.rowcount
                conn.commit()
            else:
                with conn.cursor() as cur:
                    cur.execute(
                        f"DELETE FROM {s}ledger WHERE sequence <= %s",
                        (cutoff,),
                    )
                    count = cur.rowcount
                conn.commit()
        return count
