"""Identity backend interface and implementations.

The identity backend stores hint-to-entity mappings, handles entity creation,
linking, and merging. It's the transactional core of identity resolution.

V2 uses key-level atomicity (INSERT ON CONFLICT) instead of SERIALIZABLE
transactions. This enables concurrent identity resolution across multiple
processes without batch-level blocking.

Implementations:
    SqliteIdentity  — development, single-process
    PostgresIdentity — production, concurrent-safe
    (future: RedisIdentity for extreme scale)
"""

from __future__ import annotations

import sqlite3
from abc import ABC, abstractmethod

from defacto.errors import storage_errors


class IdentityBackend(ABC):
    """Abstract interface for identity storage.

    Identity mappings are scoped by entity type — the same hint value for
    different entity types resolves independently. The composite key is
    (entity_type, hint_value), not just hint_value.

    All identity operations go through this interface. The framework
    provides SQLite and Postgres implementations. Custom backends can
    be added for extreme scale (Redis, dedicated identity services).
    """

    @abstractmethod
    def resolve_and_create(
        self, hints: list[tuple[str, str, str, str]]
    ) -> list[tuple[str, str, str]]:
        """Resolve existing identity mappings and create new ones in one call.

        Takes (entity_type, hint_type, hint_value, candidate_entity_id) tuples.
        For each hint:
          - If (entity_type, hint_value) already maps to an entity: return existing
          - If new: create the mapping with candidate_entity_id

        Returns (entity_type, hint_value, entity_id) for ALL hints.

        One round-trip regardless of backend:
          Postgres: CTE with INSERT ON CONFLICT DO NOTHING + SELECT
          SQLite: INSERT OR IGNORE + SELECT in one transaction
          Redis: pipeline SETNX + MGET

        Args:
            hints: List of (entity_type, hint_type, hint_value, candidate_entity_id).

        Returns:
            List of (entity_type, hint_value, entity_id) for all hints.
        """

    @abstractmethod
    def merge(
        self, from_entity_id: str, into_entity_id: str,
    ) -> list[tuple[str, str]]:
        """Merge one entity into another.

        Updates all hint mappings from the absorbed entity to point to
        the canonical entity. Returns the moved hints so the caller can
        update the in-memory identity cache.

        The single UPDATE is atomic and correct for single-process use.
        For multi-process deployments, concurrent merges can produce
        incorrect results (merge chains break when two processes merge
        overlapping entity sets simultaneously). This requires
        serialization via FOR UPDATE, advisory locks, or a merge queue.

        Args:
            from_entity_id: Entity being absorbed.
            into_entity_id: Entity absorbing (the canonical entity).

        Returns:
            List of (entity_type, hint_value) for hints that were moved.
        """

    @abstractmethod
    def delete(self, entity_id: str) -> int:
        """Delete all identity mappings for an entity (right to erasure).

        Args:
            entity_id: Entity to delete from the identity store.

        Returns:
            Number of mappings deleted.
        """

    def delete_batch(self, entity_ids: list[str]) -> int:
        """Delete identity mappings for multiple entities in one operation.

        Used by erase cascade to avoid N+1 deletes. Default implementation
        loops; backends override with WHERE entity_id IN (...) for efficiency.

        Returns:
            Total number of mappings deleted.
        """
        return sum(self.delete(eid) for eid in entity_ids)

    @abstractmethod
    def lookup(self, entity_type: str, hint_value: str) -> str | None:
        """Look up which entity a hint value maps to, scoped by entity type.

        Args:
            entity_type: The entity type to scope the lookup.
            hint_value: The hint value to look up.

        Returns:
            Canonical entity ID, or None if the hint is unknown.
        """

    @abstractmethod
    def warmup(self) -> list[tuple[str, str, str]]:
        """Load all identity mappings for cache warming on startup.

        Returns:
            List of (entity_type, hint_value, entity_id) triples.
        """

    @abstractmethod
    def reset(self) -> None:
        """Delete all identity data. Used during FULL_WITH_IDENTITY_RESET builds."""

    @abstractmethod
    def lookup_any(self, hint_value: str) -> str | None:
        """Look up which entity a hint value maps to, across all entity types.

        For debugging/inspection — searches all entity types. For pipeline
        use, prefer lookup() which is scoped by entity type.

        Args:
            hint_value: The hint value (e.g., 'alice@gmail.com').

        Returns:
            Canonical entity ID, or None if the hint is unknown.
        """

    @abstractmethod
    def hints_for_entity(self, entity_id: str) -> dict[str, list[str]]:
        """Get all hints linked to an entity.

        Args:
            entity_id: Entity to look up hints for.

        Returns:
            Dict of hint_type → list of hint values. Always a list,
            even for single values, so callers don't need isinstance checks.
            Multiple hints of the same type occur after merges (e.g., two
            entities with different emails merged into one).
        """

    @abstractmethod
    def close(self) -> None:
        """Close the database connection and release resources."""


class SqliteIdentity(IdentityBackend):
    """SQLite identity backend — development and single-process use.

    Identity mappings are scoped by entity type via a composite primary key
    (entity_type, hint_value). Uses INSERT OR IGNORE for idempotent creation
    and simple UPDATE for merges.
    """

    def __init__(self, db_path: str) -> None:
        """Initialize with a database path and create the identity table.

        Args:
            db_path: Path to the SQLite database file. Use ":memory:" for tests.
        """
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._create_tables()

    def _create_tables(self) -> None:
        """Create the identity table if it doesn't exist."""
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS identity (
                entity_type TEXT NOT NULL,
                hint_type   TEXT NOT NULL,
                hint_value  TEXT NOT NULL,
                entity_id   TEXT NOT NULL,
                created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                PRIMARY KEY (entity_type, hint_value)
            );

            CREATE INDEX IF NOT EXISTS idx_identity_entity
                ON identity(entity_id);
        """)

    def resolve_and_create(
        self, hints: list[tuple[str, str, str, str]]
    ) -> list[tuple[str, str, str]]:
        """INSERT OR IGNORE + SELECT in one transaction.

        New hints get created with the candidate entity_id. Existing hints
        keep their current entity_id. Returns the resolved mapping for all
        hints. Uses a temp table join for the SELECT — same pattern as
        Postgres unnest, no batch size limits, uses the primary key index.
        """
        if not hints:
            return []

        with storage_errors("Identity resolve"), self._conn:
            self._conn.executemany(
                "INSERT OR IGNORE INTO identity"
                " (entity_type, hint_type, hint_value, entity_id)"
                " VALUES (?, ?, ?, ?)",
                hints,
            )

            # Temp table join — single indexed query, no size limits
            pairs = [(h[0], h[2]) for h in hints]
            self._conn.execute(
                "CREATE TEMP TABLE _resolve_lookup"
                " (entity_type TEXT, hint_value TEXT)"
            )
            self._conn.executemany(
                "INSERT INTO _resolve_lookup VALUES (?, ?)", pairs
            )
            rows = self._conn.execute(
                "SELECT i.entity_type, i.hint_value, i.entity_id"
                " FROM identity i"
                " JOIN _resolve_lookup l"
                "   ON i.entity_type = l.entity_type"
                "  AND i.hint_value = l.hint_value",
            ).fetchall()
            self._conn.execute("DROP TABLE _resolve_lookup")

        return rows

    def merge(
        self, from_entity_id: str, into_entity_id: str,
    ) -> list[tuple[str, str]]:
        """Reassign all hints from one entity to another."""
        with storage_errors("Identity merge"), self._conn:
            moved = self._conn.execute(
                "SELECT entity_type, hint_value FROM identity"
                " WHERE entity_id = ?",
                (from_entity_id,),
            ).fetchall()
            self._conn.execute(
                "UPDATE identity SET entity_id = ? WHERE entity_id = ?",
                (into_entity_id, from_entity_id),
            )
            return [(r[0], r[1]) for r in moved]

    def delete(self, entity_id: str) -> int:
        """Delete all hint mappings for an entity. Returns count deleted."""
        with storage_errors("Identity delete"), self._conn:
            cursor = self._conn.execute(
                "DELETE FROM identity WHERE entity_id = ?",
                (entity_id,),
            )
            return cursor.rowcount

    def delete_batch(self, entity_ids: list[str]) -> int:
        """Delete hint mappings for multiple entities in one query."""
        if not entity_ids:
            return 0
        placeholders = ",".join("?" * len(entity_ids))
        with storage_errors("Identity batch delete"), self._conn:
            cursor = self._conn.execute(
                f"DELETE FROM identity WHERE entity_id IN ({placeholders})",
                entity_ids,
            )
            return cursor.rowcount

    def lookup(self, entity_type: str, hint_value: str) -> str | None:
        """Single hint lookup scoped by entity type."""
        with storage_errors("Identity lookup"):
            row = self._conn.execute(
                "SELECT entity_id FROM identity"
                " WHERE entity_type = ? AND hint_value = ?",
                (entity_type, hint_value),
            ).fetchone()
            return row[0] if row else None

    def warmup(self) -> list[tuple[str, str, str]]:
        """Load all mappings for cache warming on startup."""
        with storage_errors("Identity warmup"):
            return self._conn.execute(
                "SELECT entity_type, hint_value, entity_id FROM identity"
            ).fetchall()

    def reset(self) -> None:
        """Delete all identity data."""
        with storage_errors("Identity reset"), self._conn:
            self._conn.execute("DELETE FROM identity")

    def lookup_any(self, hint_value: str) -> str | None:
        """Look up entity by hint value across all entity types."""
        with storage_errors("Identity lookup"):
            row = self._conn.execute(
                "SELECT entity_id FROM identity WHERE hint_value = ? LIMIT 1",
                (hint_value,),
            ).fetchone()
            return row[0] if row else None

    def hints_for_entity(self, entity_id: str) -> dict[str, list[str]]:
        """Get all hints linked to an entity."""
        with storage_errors("Identity hints for entity"):
            rows = self._conn.execute(
                "SELECT hint_type, hint_value FROM identity WHERE entity_id = ?",
                (entity_id,),
            ).fetchall()
            result: dict[str, list[str]] = {}
            for hint_type, hint_value in rows:
                result.setdefault(hint_type, []).append(hint_value)
            return result

    def close(self) -> None:
        """Close the database connection."""
        self._conn.close()


class PostgresIdentity(IdentityBackend):
    """Postgres identity backend — production.

    Uses key-level atomicity: INSERT ON CONFLICT for hint registration.
    Composite key (entity_type, hint_value) for entity-type-scoped resolution.
    Tables in shared `defacto` schema. Merges use FOR UPDATE with deterministic
    lock ordering for concurrent process safety.
    """

    def __init__(self, db_path: str, *, namespace: str = "defacto") -> None:
        """Initialize with a Postgres connection string.

        Args:
            db_path: Postgres connection string.
            namespace: Schema prefix for tables. Default 'defacto'.
        """
        import psycopg

        self._conn = psycopg.connect(db_path)
        self._conn.execute("SET timezone = 'UTC'")
        self._in_transaction = False
        self._ns = namespace
        self._create_tables()

    def _create_tables(self) -> None:
        """Create the namespace schema and identity table if they don't exist."""
        ns = self._ns
        with self._conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {ns}")
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {ns}.identity (
                    entity_type TEXT        NOT NULL,
                    hint_type   TEXT        NOT NULL,
                    hint_value  TEXT        NOT NULL,
                    entity_id   TEXT        NOT NULL,
                    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (entity_type, hint_value)
                )
            """)
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_identity_entity
                    ON {ns}.identity(entity_id)
            """)
        self._conn.commit()

    def resolve_and_create(
        self, hints: list[tuple[str, str, str, str]]
    ) -> list[tuple[str, str, str]]:
        """Insert new hints and resolve all to entity IDs.

        Two statements in pipeline mode (one network round trip):
        1. INSERT ON CONFLICT DO NOTHING — create new mappings
        2. SELECT via JOIN — resolve all hints (new + existing)

        The SELECT runs with a fresh READ COMMITTED snapshot, so it sees
        concurrent commits from other processes. This is correct under
        concurrent access — unlike a single CTE where the SELECT shares
        the INSERT's snapshot and can miss concurrent inserts.

        No dead tuples, no extra row locking — DO NOTHING skips conflicts
        without side effects. The SELECT reads committed state.
        """
        if not hints:
            return []

        entity_types = [h[0] for h in hints]
        hint_types = [h[1] for h in hints]
        hint_values = [h[2] for h in hints]
        candidate_ids = [h[3] for h in hints]

        with storage_errors("Identity resolve"), self._conn.pipeline():
            with self._conn.cursor() as cur:
                # Statement 1: create new mappings (DO NOTHING for existing)
                cur.execute(
                    f"INSERT INTO {self._ns}.identity"
                    "  (entity_type, hint_type, hint_value, entity_id)"
                    " SELECT * FROM unnest("
                    "   %s::text[], %s::text[], %s::text[], %s::text[]"
                    " ) ON CONFLICT (entity_type, hint_value) DO NOTHING",
                    (entity_types, hint_types, hint_values, candidate_ids),
                )

                # Statement 2: resolve all hints (fresh snapshot)
                cur.execute(
                    "SELECT i.entity_type, i.hint_value, i.entity_id"
                    f" FROM {self._ns}.identity i"
                    " JOIN unnest(%s::text[], %s::text[])"
                    "   AS input(entity_type, hint_value)"
                    "   ON i.entity_type = input.entity_type"
                    "  AND i.hint_value = input.hint_value",
                    (entity_types, hint_values),
                )
                rows = cur.fetchall()

        self._auto_commit()
        return rows

    def merge(
        self, from_entity_id: str, into_entity_id: str,
    ) -> list[tuple[str, str]]:
        """Reassign all hints from one entity to another.

        Uses FOR UPDATE to lock affected rows, preventing concurrent merge
        chain breakage. Deterministic lock ordering (lower entity_id first)
        prevents deadlocks when two processes merge overlapping entities.
        """
        # Lock in deterministic order to prevent deadlocks
        first, second = sorted([from_entity_id, into_entity_id])
        with storage_errors("Identity merge"), self._conn.cursor() as cur:
            cur.execute(
                f"SELECT entity_id FROM {self._ns}.identity"
                " WHERE entity_id IN (%s, %s)"
                " ORDER BY entity_id FOR UPDATE",
                (first, second),
                prepare=True,
            )
            # Capture which hints are moving (for cache update)
            cur.execute(
                f"SELECT entity_type, hint_value FROM {self._ns}.identity"
                " WHERE entity_id = %s",
                (from_entity_id,),
            )
            moved = [(r[0], r[1]) for r in cur.fetchall()]
            cur.execute(
                f"UPDATE {self._ns}.identity SET entity_id = %s WHERE entity_id = %s",
                (into_entity_id, from_entity_id),
                prepare=True,
            )
        self._auto_commit()
        return moved

    def delete(self, entity_id: str) -> int:
        """Delete all hint mappings for an entity. Returns count deleted."""
        with storage_errors("Identity delete"), self._conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {self._ns}.identity WHERE entity_id = %s",
                (entity_id,),
            )
            count = cur.rowcount
        self._auto_commit()
        return count

    def delete_batch(self, entity_ids: list[str]) -> int:
        """Delete hint mappings for multiple entities in one query."""
        if not entity_ids:
            return 0
        with storage_errors("Identity batch delete"), self._conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {self._ns}.identity WHERE entity_id = ANY(%s)",
                (entity_ids,),
            )
            count = cur.rowcount
        self._auto_commit()
        return count

    def lookup(self, entity_type: str, hint_value: str) -> str | None:
        """Single hint lookup scoped by entity type."""
        with storage_errors("Identity lookup"), self._conn.cursor() as cur:
            cur.execute(
                f"SELECT entity_id FROM {self._ns}.identity"
                " WHERE entity_type = %s AND hint_value = %s",
                (entity_type, hint_value),
                prepare=True,
            )
            row = cur.fetchone()
        return row[0] if row else None

    def warmup(self) -> list[tuple[str, str, str]]:
        """Load all mappings for cache warming on startup."""
        with storage_errors("Identity warmup"), self._conn.cursor() as cur:
            cur.execute(
                f"SELECT entity_type, hint_value, entity_id FROM {self._ns}.identity"
            )
            return cur.fetchall()

    def reset(self) -> None:
        """Delete all identity data."""
        with storage_errors("Identity reset"), self._conn.cursor() as cur:
            cur.execute(f"TRUNCATE {self._ns}.identity")
        self._auto_commit()

    def lookup_any(self, hint_value: str) -> str | None:
        """Look up entity by hint value across all entity types."""
        with storage_errors("Identity lookup"), self._conn.cursor() as cur:
            cur.execute(
                f"SELECT entity_id FROM {self._ns}.identity"
                " WHERE hint_value = %s LIMIT 1",
                (hint_value,),
            )
            row = cur.fetchone()
        return row[0] if row else None

    def hints_for_entity(self, entity_id: str) -> dict[str, list[str]]:
        """Get all hints linked to an entity."""
        with storage_errors("Identity hints for entity"), self._conn.cursor() as cur:
            cur.execute(
                f"SELECT hint_type, hint_value FROM {self._ns}.identity"
                " WHERE entity_id = %s",
                (entity_id,),
            )
            rows = cur.fetchall()
        result: dict[str, list[str]] = {}
        for hint_type, hint_value in rows:
            result.setdefault(hint_type, []).append(hint_value)
        return result

    # ── Transaction control ──

    def _auto_commit(self) -> None:
        """Commit if not inside an explicit transaction."""
        if not self._in_transaction:
            self._conn.commit()

    def close(self) -> None:
        """Close the database connection."""
        self._conn.close()
