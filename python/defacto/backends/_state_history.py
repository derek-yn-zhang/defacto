"""State history backend interface and implementations.

The state history stores SCD Type 2 tables — entity state versioned with
valid_from/valid_to timestamps. It replaces both the v1 projection (current
state) and journal (change history) with a single queryable artifact.

The state history backend handles DDL (creating tables from definitions),
writing snapshots (closing old versions, inserting new versions), and
providing query access via Ibis.

Implementations:
    SqliteStateHistory   — development, inline writes during build
    PostgresStateHistory — production, used by consumer or inline
    DuckDBStateHistory   — local analytics, columnar storage
    (future: BigQueryStateHistory, SnowflakeStateHistory)
"""

from __future__ import annotations

import json
import sqlite3
from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import datetime

from defacto.errors import storage_errors
from typing import Any, Iterator


def _split_generations(snaps: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    """Split snapshots into generations for correct SCD Type 2 ordering.

    Each generation has at most one snapshot per entity_id. If an entity
    has multiple snapshots in the batch (e.g., signup then upgrade), they're
    distributed across generations in valid_from order so each close+insert
    pass sees the previous version already written.

    For the common case (each entity appears once), there's one generation.

    Shared across all state history backends — the intra-batch ordering
    problem is the same regardless of database.
    """
    sorted_snaps = sorted(snaps, key=lambda s: s.get("valid_from", ""))
    generations: list[list[dict[str, Any]]] = []
    for snap in sorted_snaps:
        eid = snap["entity_id"]
        placed = False
        for gen in generations:
            if not any(s["entity_id"] == eid for s in gen):
                gen.append(snap)
                placed = True
                break
        if not placed:
            generations.append([snap])
    return generations


def _split_generations_rel(
    rels: list[dict[str, Any]],
) -> list[list[dict[str, Any]]]:
    """Split relationship snapshots into generations for correct SCD ordering.

    Same concept as _split_generations but keyed on (source_id, target_id)
    instead of entity_id — each generation has at most one row per
    source-target pair.
    """
    sorted_rels = sorted(rels, key=lambda r: r.get("valid_from", ""))
    generations: list[list[dict[str, Any]]] = []
    for rel in sorted_rels:
        key = (rel["source_id"], rel["target_id"])
        placed = False
        for gen in generations:
            if not any(
                (r["source_id"], r["target_id"]) == key for r in gen
            ):
                gen.append(rel)
                placed = True
                break
        if not placed:
            generations.append([rel])
    return generations


class StateHistoryBackend(ABC):
    """Abstract interface for state history storage.

    State history tables have the same columns as entity properties plus
    entity_id, state, valid_from, valid_to, and merged_into. One table
    per entity type, one table per relationship type.
    """

    @abstractmethod
    def ensure_tables(self, entity_definitions: dict[str, Any]) -> None:
        """Create or update state history tables from entity definitions.

        Generates DDL (CREATE TABLE IF NOT EXISTS) with columns matching
        entity properties + system columns (entity_id, state, valid_from,
        valid_to, merged_into).

        Args:
            entity_definitions: Dict of entity type name → Entity or definition dict.
        """

    @abstractmethod
    def write_batch(
        self,
        snapshots: list[dict[str, Any]],
        tombstones: list[dict[str, Any]],
    ) -> None:
        """Write a batch of entity snapshots and tombstones.

        For each snapshot: close the current version (set valid_to) and
        insert a new version row. For each tombstone: close the entity's
        current version with merged_into set.

        Writes are idempotent — (entity_id, valid_from) is the natural key.
        Duplicate writes (from Kafka replay) are safely skipped.

        Args:
            snapshots: Entity snapshot dicts with entity_id, entity_type,
                state, properties, and version_timestamp.
            tombstones: Tombstone dicts with entity_id, entity_type,
                reason ('merge' or 'erase'), and optionally merged_into.
        """

    def entity_columns(self) -> dict[str, list[str]]:
        """Return entity type → property column names mapping.

        Public accessor for the column metadata populated by ensure_tables.
        Used by cold start recovery to know which entity types to stream
        and which columns to read.

        Returns:
            Dict mapping entity_type to list of property column names.
        """
        return dict(getattr(self, "_table_columns", {}))

    @abstractmethod
    def delete_entity(self, entity_id: str) -> int:
        """Delete all state history rows for an entity (right to erasure).

        Args:
            entity_id: Entity to delete from state history.

        Returns:
            Number of rows deleted.
        """

    @abstractmethod
    def current_state(self, entity_type: str) -> Any:
        """Get current state for all entities of a type.

        Returns an Ibis table expression equivalent to:
            SELECT * FROM {entity_type}_history WHERE valid_to IS NULL

        Args:
            entity_type: Entity type name.

        Returns:
            Ibis table expression.
        """

    @abstractmethod
    def history(self, entity_type: str) -> Any:
        """Get full SCD Type 2 history for an entity type.

        Returns an Ibis table expression for the complete history table
        including all versions, merged entities, and time bounds.

        Args:
            entity_type: Entity type name.

        Returns:
            Ibis table expression.
        """

    @abstractmethod
    def read_current_entities(
        self, entity_type: str, prop_names: list[str],
    ) -> Iterator[dict[str, Any]]:
        """Stream current entity state for cold start recovery.

        Yields dicts with entity_id, entity_type, state, properties (dict),
        and timing fields (last_event_time, state_entered_time, created_time).
        Only yields entities that are current (valid_to IS NULL) and not
        merged (merged_into IS NULL).

        Uses server-side cursors (Postgres) or fetchmany (SQLite/DuckDB)
        to stream results without materializing the full result set.

        Args:
            entity_type: Entity type name.
            prop_names: Property column names to read.

        Yields:
            Entity state dicts suitable for DefactoCore.load_entities().
        """

    @abstractmethod
    def as_of(self, entity_type: str, timestamp: datetime) -> Any:
        """Get entity state at a specific point in time.

        Returns an Ibis table expression equivalent to:
            SELECT * FROM {entity_type}_history
            WHERE valid_from <= timestamp
            AND (valid_to > timestamp OR valid_to IS NULL)

        Args:
            entity_type: Entity type name.
            timestamp: Point in time to query.

        Returns:
            Ibis table expression.
        """

    @property
    @abstractmethod
    def connection(self) -> Any:
        """The underlying database connection.

        Exposed for read-only use by the graph query layer (CteGraphBackend)
        which needs raw SQL access for recursive CTEs.
        """

    def set_version(self, version: str, entity_definitions: dict[str, Any]) -> None:
        """Switch to a new definition version.

        Updates the version scope and ensures tables exist for the new
        definitions. The database connection is reused — only the target
        schema/tables change. Column caches are cleared and rebuilt by
        ensure_tables().

        Args:
            version: New definition version name.
            entity_definitions: Entity definitions dict for the new version.
        """
        self._version = version
        self.ensure_tables(entity_definitions)

    @abstractmethod
    def close(self) -> None:
        """Close the database connection and release resources."""


class SqliteStateHistory(StateHistoryBackend):
    """SQLite state history — development, inline writes during build.

    Creates one SCD Type 2 table per entity type with dynamic property columns.
    Uses batch UPDATE + INSERT grouped by entity type for efficient writes.
    DDL generation is delegated to DDLGenerator for consistent type mappings.
    """

    def __init__(self, db_path: str, version: str = "") -> None:
        """Initialize with a database path.

        Tables are created lazily via ensure_tables() — we don't know the
        entity definitions at construction time.

        Args:
            db_path: Path to the SQLite database file. Use ":memory:" for tests.
            version: Ignored for SQLite (no per-version schemas). Accepted
                for constructor uniformity across backends.
        """
        self._conn = sqlite3.connect(db_path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._version = version
        # Track which tables exist and their property columns, so write_batch
        # knows which columns to insert without re-reading definitions.
        # Each entry maps entity_type → list of property column names.
        self._table_columns: dict[str, list[str]] = {}
        # Relationship table columns keyed by relationship type
        self._rel_columns: dict[str, list[str]] = {}

    def ensure_tables(self, entity_definitions: dict[str, Any]) -> None:
        """Create SCD Type 2 tables and relationship history tables.

        Delegates DDL generation to DDLGenerator for consistent type mappings
        across backends. Clears and rebuilds column caches so they reflect
        exactly the current definitions (not accumulated from prior versions).
        """
        from defacto._ddl import DDLGenerator

        self._table_columns = {}
        self._rel_columns = {}

        ddl = DDLGenerator()
        statements = ddl.generate(
            {"entities": entity_definitions}, version="", backend="sqlite"
        )
        with storage_errors("State history ensure tables"):
            for stmt in statements:
                self._conn.execute(stmt)

            # SQLite has no per-version schemas — tables are shared across
            # versions. When a new version adds properties, the existing
            # table won't have those columns (CREATE TABLE IF NOT EXISTS
            # is a no-op for existing tables). Add missing columns.
            for entity_type, entity_def in entity_definitions.items():
                table = f"{entity_type}_history"
                existing = {
                    row[1] for row in self._conn.execute(f"PRAGMA table_info({table})")
                }
                for prop_name in entity_def.get("properties", {}):
                    if prop_name not in existing:
                        self._conn.execute(
                            f"ALTER TABLE {table} ADD COLUMN {prop_name} TEXT"
                        )

        # Cache property column names for entity write_batch
        seen_rels: set[str] = set()
        for entity_type, entity_def in entity_definitions.items():
            properties = entity_def.get("properties", {})
            self._table_columns[entity_type] = list(properties.keys())

            # Cache relationship property columns
            for rel in entity_def.get("relationships", []):
                rel_type = rel["type"]
                if rel_type not in seen_rels:
                    seen_rels.add(rel_type)
                    rel_props = rel.get("properties", {})
                    self._rel_columns[rel_type] = list(rel_props.keys())

    def write_batch(
        self,
        snapshots: list[dict[str, Any]],
        tombstones: list[dict[str, Any]],
    ) -> None:
        """Write snapshots and tombstones grouped by entity type.

        For each snapshot: close the current version (set valid_to) then insert
        a new version row. For each tombstone: close the current version with
        merged_into set. All operations in one transaction, batched by entity
        type using executemany.
        """
        with storage_errors("State history write"), self._conn:
            # ── Snapshots ──
            if snapshots:
                by_type: dict[str, list[dict[str, Any]]] = defaultdict(list)
                for snap in snapshots:
                    by_type[snap["entity_type"]].append(snap)

                for entity_type, type_snaps in by_type.items():
                    table = f"{entity_type}_history"
                    id_col = f"{entity_type}_id"
                    state_col = f"{entity_type}_state"
                    prop_names = self._table_columns.get(entity_type, [])

                    # Split into generations for correct SCD ordering
                    # when the same entity appears multiple times in a batch.
                    timing_cols = [
                        "last_event_time", "state_entered_time", "created_time",
                    ]
                    for gen in _split_generations(type_snaps):
                        # Close current versions — only close rows that are
                        # strictly older than the incoming snapshot. This
                        # prevents idempotent replays from corrupting state:
                        # replaying the same (entity_id, valid_from) pair
                        # must not close the existing row since the INSERT
                        # will be skipped by ON CONFLICT.
                        close_params = [
                            (s["valid_from"], s["entity_id"], s["valid_from"])
                            for s in gen
                        ]
                        self._conn.executemany(
                            f"UPDATE {table} SET valid_to = ?"
                            f" WHERE {id_col} = ? AND valid_to IS NULL"
                            f" AND valid_from < ?",
                            close_params,
                        )

                        # Insert new versions
                        col_names = [
                            id_col, state_col, *prop_names,
                            "valid_from", *timing_cols,
                        ]
                        placeholders = ", ".join("?" * len(col_names))
                        col_sql = ", ".join(col_names)

                        insert_rows = []
                        for snap in gen:
                            props = snap.get("properties", {})
                            prop_values = [
                                json.dumps(props[p]) if isinstance(props.get(p), (list, dict)) else props.get(p)
                                for p in prop_names
                            ]
                            insert_rows.append((
                                snap["entity_id"],
                                snap["state"],
                                *prop_values,
                                snap["valid_from"],
                                *(snap.get(c) for c in timing_cols),
                            ))

                        self._conn.executemany(
                            f"INSERT OR IGNORE INTO {table} ({col_sql})"
                            f" VALUES ({placeholders})",
                            insert_rows,
                        )

            # ── Relationships ──
            if snapshots and self._rel_columns:
                self._write_relationships(snapshots)

            # ── Tombstones ──
            if tombstones:
                tomb_by_type: dict[str, list[dict[str, Any]]] = defaultdict(list)
                for tomb in tombstones:
                    tomb_by_type[tomb["entity_type"]].append(tomb)

                for entity_type, type_tombs in tomb_by_type.items():
                    table = f"{entity_type}_history"
                    id_col = f"{entity_type}_id"
                    tomb_params = [
                        (
                            t.get("timestamp", t.get("valid_from")),
                            t.get("merged_into"),
                            t["entity_id"],
                        )
                        for t in type_tombs
                    ]
                    self._conn.executemany(
                        f"UPDATE {table} SET valid_to = ?, merged_into = ?"
                        f" WHERE {id_col} = ? AND valid_to IS NULL",
                        tomb_params,
                    )

    def _write_relationships(self, snapshots: list[dict[str, Any]]) -> None:
        """Write relationship snapshots from entity snapshots.

        Extracts relationships from each snapshot and writes them to
        relationship history tables using the same close+insert SCD pattern.
        """
        # Build relationship rows: source_id is the entity, target is from the relationship
        rel_by_type: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for snap in snapshots:
            for rel in snap.get("relationships", []):
                rel_type = rel["relationship_type"]
                if rel_type in self._rel_columns:
                    rel_by_type[rel_type].append({
                        "source_id": snap["entity_id"],
                        "target_id": rel["target_entity_id"],
                        "relationship_type": rel_type,
                        "properties": rel.get("properties", {}),
                        "valid_from": snap["valid_from"],
                    })

        for rel_type, rel_snaps in rel_by_type.items():
            table = f"rel_{rel_type}_history"
            prop_names = self._rel_columns[rel_type]

            for gen in _split_generations_rel(rel_snaps):
                # Close current versions
                close_params = [
                    (r["valid_from"], r["source_id"], r["target_id"])
                    for r in gen
                ]
                self._conn.executemany(
                    f"UPDATE {table} SET valid_to = ?"
                    f" WHERE source_id = ? AND target_id = ?"
                    f" AND valid_to IS NULL",
                    close_params,
                )

                # Insert new versions
                col_names = [
                    "source_id", "target_id", "relationship_type",
                    *prop_names, "valid_from",
                ]
                placeholders = ", ".join("?" * len(col_names))
                col_sql = ", ".join(col_names)

                insert_rows = []
                for r in gen:
                    props = r["properties"]
                    prop_values = [
                        json.dumps(props.get(p))
                        if isinstance(props.get(p), (list, dict))
                        else props.get(p)
                        for p in prop_names
                    ]
                    insert_rows.append((
                        r["source_id"],
                        r["target_id"],
                        r["relationship_type"],
                        *prop_values,
                        r["valid_from"],
                    ))

                self._conn.executemany(
                    f"INSERT OR IGNORE INTO {table} ({col_sql})"
                    f" VALUES ({placeholders})",
                    insert_rows,
                )

    def read_current_entities(
        self, entity_type: str, prop_names: list[str],
    ) -> Iterator[dict[str, Any]]:
        """Stream current entities from SQLite using fetchmany."""
        table = f"{entity_type}_history"
        id_col = f"{entity_type}_id"
        state_col = f"{entity_type}_state"
        timing_cols = ["last_event_time", "state_entered_time", "created_time"]
        select_cols = [id_col, state_col, *prop_names, *timing_cols]
        col_sql = ", ".join(select_cols)

        with storage_errors("State history read current entities"):
            cursor = self._conn.execute(
                f"SELECT {col_sql} FROM {table}"
                f" WHERE valid_to IS NULL AND merged_into IS NULL",
            )
            while True:
                rows = cursor.fetchmany(5000)
                if not rows:
                    break
                for row in rows:
                    props = {}
                    for i, pname in enumerate(prop_names):
                        val = row[2 + i]
                        if isinstance(val, str):
                            try:
                                parsed = json.loads(val)
                                if isinstance(parsed, (list, dict)):
                                    val = parsed
                            except (json.JSONDecodeError, ValueError):
                                pass
                        props[pname] = val
                    offset = 2 + len(prop_names)
                    yield {
                        "entity_id": row[0],
                        "entity_type": entity_type,
                        "state": row[1],
                        "properties": props,
                        "last_event_time": row[offset],
                        "state_entered_time": row[offset + 1],
                        "created_time": row[offset + 2],
                    }

    def delete_entity(self, entity_id: str) -> int:
        """Delete all history rows for an entity across all known tables."""
        total = 0
        with storage_errors("State history delete entity"), self._conn:
            for entity_type in self._table_columns:
                id_col = f"{entity_type}_id"
                cursor = self._conn.execute(
                    f"DELETE FROM {entity_type}_history WHERE {id_col} = ?",
                    (entity_id,),
                )
                total += cursor.rowcount
            # Also delete from relationship tables
            for rel_type in self._rel_columns:
                cursor = self._conn.execute(
                    f"DELETE FROM rel_{rel_type}_history"
                    f" WHERE source_id = ? OR target_id = ?",
                    (entity_id, entity_id),
                )
                total += cursor.rowcount
        return total

    def current_state(self, entity_type: str) -> Any:
        """Deferred to query layer design session."""
        raise NotImplementedError

    def history(self, entity_type: str) -> Any:
        """Deferred to query layer design session."""
        raise NotImplementedError

    def as_of(self, entity_type: str, timestamp: datetime) -> Any:
        """Deferred to query layer design session."""
        raise NotImplementedError

    @property
    def connection(self) -> Any:
        """The underlying sqlite3 connection."""
        return self._conn

    def close(self) -> None:
        """Close the database connection."""
        self._conn.close()


class PostgresStateHistory(StateHistoryBackend):
    """Postgres state history — production.

    Per-version schemas (defacto_v1, defacto_v2). All bulk writes via unnest
    arrays inside pipeline mode — one network round trip for all entity
    types, one commit. Prepared statements cache query plans across batches.
    Generation splitting ensures correct SCD Type 2 ordering when the same
    entity has multiple snapshots in one batch.
    """

    def __init__(
        self, db_path: str, version: str = "", *, namespace: str = "defacto",
    ) -> None:
        """Initialize with a Postgres connection string and version.

        The version determines the schema name ({namespace}_{version}).
        Tables are created lazily via ensure_tables().

        Args:
            db_path: Postgres connection string.
            version: Definition version (used for schema name).
            namespace: Schema prefix. Default 'defacto'.
        """
        import psycopg
        from psycopg.types.json import Jsonb

        self._conn = psycopg.connect(db_path)
        self._conn.execute("SET timezone = 'UTC'")
        self._version = version
        self._ns = namespace
        self._Jsonb = Jsonb
        self._table_columns: dict[str, list[str]] = {}
        self._table_types: dict[str, list[str]] = {}
        self._rel_columns: dict[str, list[str]] = {}
        self._rel_types: dict[str, list[str]] = {}

    def ensure_tables(self, entity_definitions: dict[str, Any]) -> None:
        """Create per-version schema, SCD Type 2 tables, and relationship tables.

        Delegates DDL generation to DDLGenerator for consistent type
        mappings. Clears and rebuilds all column/type caches so they
        reflect exactly the current definitions.
        """
        from defacto._ddl import DDLGenerator, TYPE_MAPS

        self._table_columns = {}
        self._table_types = {}
        self._rel_columns = {}
        self._rel_types = {}

        ddl = DDLGenerator()
        statements = ddl.generate(
            {"entities": entity_definitions},
            version=self._version,
            backend="postgres",
            namespace=self._ns,
        )
        with storage_errors("State history ensure tables"), self._conn.cursor() as cur:
            for stmt in statements:
                cur.execute(stmt)
        self._conn.commit()

        pg_types = TYPE_MAPS["postgres"]
        seen_rels: set[str] = set()
        for entity_type, entity_def in entity_definitions.items():
            properties = entity_def.get("properties", {})
            self._table_columns[entity_type] = list(properties.keys())
            self._table_types[entity_type] = [
                pg_types.get(prop_def.get("type", "string"), "TEXT")
                for prop_def in properties.values()
            ]

            # Cache relationship property columns and types
            for rel in entity_def.get("relationships", []):
                rel_type = rel["type"]
                if rel_type not in seen_rels:
                    seen_rels.add(rel_type)
                    rel_props = rel.get("properties", {})
                    self._rel_columns[rel_type] = list(rel_props.keys())
                    self._rel_types[rel_type] = [
                        pg_types.get(p.get("type", "string"), "TEXT")
                        for p in rel_props.values()
                    ]

    def write_batch(
        self,
        snapshots: list[dict[str, Any]],
        tombstones: list[dict[str, Any]],
    ) -> None:
        """Write snapshots and tombstones via unnest in pipeline mode.

        All operations are buffered inside a pipeline context — one
        network round trip for the entire batch. Prepared statements
        cache query plans across batches. One commit at the end.

        When multiple snapshots for the same entity appear in one batch,
        they're sorted by valid_from and split into generations — each
        generation has at most one snapshot per entity. Generations are
        processed sequentially (close then insert) so SCD Type 2
        versioning is correct regardless of batch composition.
        """

        Jsonb = self._Jsonb

        with storage_errors("State history write"), self._conn.pipeline():
            with self._conn.cursor() as cur:
                # ── Snapshots: close old versions + insert new ──
                if snapshots:
                    by_type: dict[str, list[dict[str, Any]]] = defaultdict(list)
                    for snap in snapshots:
                        by_type[snap["entity_type"]].append(snap)

                    for entity_type, type_snaps in by_type.items():
                        table = f"{self._ns}_{self._version}.{entity_type}_history"
                        prop_names = self._table_columns.get(entity_type, [])
                        prop_types = self._table_types.get(entity_type, [])

                        generations = _split_generations(type_snaps)

                        for gen in generations:
                            self._write_generation(
                                cur, table, entity_type, gen,
                                prop_names, prop_types, Jsonb,
                            )

                # ── Relationships ──
                if snapshots and self._rel_columns:
                    self._write_relationships(cur, snapshots)

                # ── Tombstones: close merged entity versions ──
                if tombstones:
                    tomb_by_type: dict[str, list[dict[str, Any]]] = defaultdict(list)
                    for tomb in tombstones:
                        tomb_by_type[tomb["entity_type"]].append(tomb)

                    for entity_type, type_tombs in tomb_by_type.items():
                        table = f"{self._ns}_{self._version}.{entity_type}_history"
                        id_col = f"{entity_type}_id"
                        entity_ids = [t["entity_id"] for t in type_tombs]
                        timestamps = [
                            t.get("timestamp", t.get("valid_from"))
                            for t in type_tombs
                        ]
                        merged_intos = [t.get("merged_into") for t in type_tombs]

                        cur.execute(
                            f"UPDATE {table} SET valid_to = s.ts,"
                            f" merged_into = s.merged_into"
                            f" FROM unnest(%s::text[], %s::timestamptz[], %s::text[])"
                            f"   AS s({id_col}, ts, merged_into)"
                            f" WHERE {table}.{id_col} = s.{id_col}"
                            f"   AND {table}.valid_to IS NULL",
                            (entity_ids, timestamps, merged_intos),
                            prepare=True,
                        )

        self._conn.commit()

    def delete_entity(self, entity_id: str) -> int:
        """Delete all history rows for an entity across all known tables."""
        total = 0
        with storage_errors("State history delete entity"), self._conn.cursor() as cur:
            for entity_type in self._table_columns:
                table = f"{self._ns}_{self._version}.{entity_type}_history"
                id_col = f"{entity_type}_id"
                cur.execute(
                    f"DELETE FROM {table} WHERE {id_col} = %s",
                    (entity_id,),
                )
                total += cur.rowcount
            for rel_type in self._rel_columns:
                table = f"{self._ns}_{self._version}.rel_{rel_type}_history"
                cur.execute(
                    f"DELETE FROM {table}"
                    f" WHERE source_id = %s OR target_id = %s",
                    (entity_id, entity_id),
                )
                total += cur.rowcount
        self._conn.commit()
        return total

    def read_current_entities(
        self, entity_type: str, prop_names: list[str],
    ) -> Iterator[dict[str, Any]]:
        """Stream current entities from Postgres using a server-side cursor."""
        table = f"{self._ns}_{self._version}.{entity_type}_history"
        id_col = f"{entity_type}_id"
        state_col = f"{entity_type}_state"
        timing_cols = ["last_event_time", "state_entered_time", "created_time"]
        select_cols = [id_col, state_col, *prop_names, *timing_cols]
        col_sql = ", ".join(select_cols)

        with storage_errors("State history read current entities"):
            with self._conn.cursor(name="cold_start_read") as cur:
                cur.itersize = 5000
                cur.execute(
                    f"SELECT {col_sql} FROM {table}"
                    f" WHERE valid_to IS NULL AND merged_into IS NULL",
                )
                for row in cur:
                    props = {}
                    for i, pname in enumerate(prop_names):
                        val = row[2 + i]
                        props[pname] = val
                    offset = 2 + len(prop_names)
                    yield {
                        "entity_id": row[0],
                        "entity_type": entity_type,
                        "state": row[1],
                        "properties": props,
                        "last_event_time": (
                            row[offset].isoformat() if row[offset] else None
                        ),
                        "state_entered_time": (
                            row[offset + 1].isoformat()
                            if row[offset + 1] else None
                        ),
                        "created_time": (
                            row[offset + 2].isoformat()
                            if row[offset + 2] else None
                        ),
                    }

    def current_state(self, entity_type: str) -> Any:
        """Deferred to query layer design session."""
        raise NotImplementedError

    def history(self, entity_type: str) -> Any:
        """Deferred to query layer design session."""
        raise NotImplementedError

    def as_of(self, entity_type: str, timestamp: datetime) -> Any:
        """Deferred to query layer design session."""
        raise NotImplementedError

    # ── Private helpers ──

    def _write_relationships(
        self, cur: Any, snapshots: list[dict[str, Any]]
    ) -> None:
        """Write relationship snapshots via unnest arrays.

        Same pattern as entity writes — close current versions, insert new
        ones. Uses unnest for bulk operations inside the pipeline context.
        """
        Jsonb = self._Jsonb

        # Build relationship rows from entity snapshots
        rel_by_type: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for snap in snapshots:
            for rel in snap.get("relationships", []):
                rel_type = rel["relationship_type"]
                if rel_type in self._rel_columns:
                    rel_by_type[rel_type].append({
                        "source_id": snap["entity_id"],
                        "target_id": rel["target_entity_id"],
                        "relationship_type": rel_type,
                        "properties": rel.get("properties", {}),
                        "valid_from": snap["valid_from"],
                    })

        for rel_type, rel_snaps in rel_by_type.items():
            table = f"{self._ns}_{self._version}.rel_{rel_type}_history"
            prop_names = self._rel_columns[rel_type]
            prop_types = self._rel_types[rel_type]

            for gen in _split_generations_rel(rel_snaps):
                # Close current versions
                src_ids = [r["source_id"] for r in gen]
                tgt_ids = [r["target_id"] for r in gen]
                valid_froms = [r["valid_from"] for r in gen]
                cur.execute(
                    f"UPDATE {table} SET valid_to = s.valid_from"
                    f" FROM unnest(%s::text[], %s::text[], %s::timestamptz[])"
                    f"   AS s(source_id, target_id, valid_from)"
                    f" WHERE {table}.source_id = s.source_id"
                    f"   AND {table}.target_id = s.target_id"
                    f"   AND {table}.valid_to IS NULL",
                    (src_ids, tgt_ids, valid_froms),
                    prepare=True,
                )

                # Insert new versions
                col_names = [
                    "source_id", "target_id", "relationship_type",
                    *prop_names, "valid_from",
                ]
                col_sql = ", ".join(col_names)
                all_types = ["text", "text", "text", *prop_types, "timestamptz"]
                unnest_casts = ", ".join(f"%s::{t}[]" for t in all_types)

                type_arr = [r["relationship_type"] for r in gen]
                prop_arrays = []
                for i, pname in enumerate(prop_names):
                    ptype = prop_types[i] if i < len(prop_types) else "TEXT"
                    arr = []
                    for r in gen:
                        val = r["properties"].get(pname)
                        if ptype == "JSONB" and isinstance(val, (list, dict)):
                            arr.append(Jsonb(val))
                        else:
                            arr.append(val)
                    prop_arrays.append(arr)

                params = (src_ids, tgt_ids, type_arr, *prop_arrays, valid_froms)
                cur.execute(
                    f"INSERT INTO {table} ({col_sql})"
                    f" SELECT * FROM unnest({unnest_casts})"
                    f" ON CONFLICT (source_id, target_id, valid_from) DO NOTHING",
                    params,
                    prepare=True,
                )

    def _write_generation(
        self,
        cur: Any,
        table: str,
        entity_type: str,
        snaps: list[dict[str, Any]],
        prop_names: list[str],
        prop_types: list[str],
        Jsonb: type,
    ) -> None:
        """Write one generation: close current versions, insert new ones."""
        id_col = f"{entity_type}_id"
        state_col = f"{entity_type}_state"

        # Close current versions — only close rows strictly older than
        # the incoming snapshot. Prevents idempotent replays (same
        # entity_id + valid_from) from closing the existing row when
        # the INSERT will be skipped by ON CONFLICT.
        entity_ids = [s["entity_id"] for s in snaps]
        valid_froms = [s["valid_from"] for s in snaps]
        cur.execute(
            f"UPDATE {table} SET valid_to = s.valid_from"
            f" FROM unnest(%s::text[], %s::timestamptz[])"
            f"   AS s({id_col}, valid_from)"
            f" WHERE {table}.{id_col} = s.{id_col}"
            f"   AND {table}.valid_to IS NULL"
            f"   AND {table}.valid_from < s.valid_from",
            (entity_ids, valid_froms),
            prepare=True,
        )

        # Insert new versions
        timing_cols = ["last_event_time", "state_entered_time", "created_time"]
        col_names = [
            id_col, state_col, *prop_names,
            "valid_from", *timing_cols,
        ]
        col_sql = ", ".join(col_names)
        all_types = [
            "text", "text", *prop_types,
            "timestamptz", "timestamptz", "timestamptz", "timestamptz",
        ]
        unnest_casts = ", ".join(f"%s::{t}[]" for t in all_types)

        id_arr = [s["entity_id"] for s in snaps]
        state_arr = [s["state"] for s in snaps]
        prop_arrays = []
        for i, pname in enumerate(prop_names):
            ptype = prop_types[i] if i < len(prop_types) else "TEXT"
            arr = []
            for snap in snaps:
                val = snap.get("properties", {}).get(pname)
                if ptype == "JSONB" and isinstance(val, (list, dict)):
                    arr.append(Jsonb(val))
                else:
                    arr.append(val)
            prop_arrays.append(arr)
        vf_arr = [s["valid_from"] for s in snaps]
        timing_arrays = [
            [s.get(c) for s in snaps] for c in timing_cols
        ]

        params = (id_arr, state_arr, *prop_arrays, vf_arr, *timing_arrays)
        cur.execute(
            f"INSERT INTO {table} ({col_sql})"
            f" SELECT * FROM unnest({unnest_casts})"
            f" ON CONFLICT ({id_col}, valid_from) DO NOTHING",
            params,
            prepare=True,
        )

    @property
    def connection(self) -> Any:
        """The underlying psycopg connection."""
        return self._conn

    def close(self) -> None:
        """Close the database connection."""
        self._conn.close()


class DuckDBStateHistory(StateHistoryBackend):
    """DuckDB state history — local columnar analytics.

    In-process columnar database, good for local analytics without
    a Postgres server. Supports schemas, batch INSERT, and efficient
    analytical queries. Used as a consumer store or for development
    with analytical workloads. Generation splitting ensures correct
    SCD Type 2 ordering when the same entity has multiple snapshots
    in one batch.
    """

    def __init__(
        self, db_path: str, version: str = "", *, namespace: str = "defacto",
    ) -> None:
        """Initialize with a DuckDB database path and version.

        Args:
            db_path: Path to the DuckDB file, or ':memory:' for in-memory.
            version: Definition version (used for schema name).
            namespace: Schema prefix. Default 'defacto'.
        """
        import duckdb

        self._conn = duckdb.connect(db_path)
        self._version = version
        self._ns = namespace
        self._table_columns: dict[str, list[str]] = {}
        self._rel_columns: dict[str, list[str]] = {}

    def ensure_tables(self, entity_definitions: dict[str, Any]) -> None:
        """Create per-version schema, SCD Type 2 tables, and relationship tables.

        Delegates DDL generation to DDLGenerator with the duckdb dialect.
        Clears and rebuilds column caches so they reflect exactly the
        current definitions.
        """
        from defacto._ddl import DDLGenerator

        self._table_columns = {}
        self._rel_columns = {}

        ddl = DDLGenerator()
        statements = ddl.generate(
            {"entities": entity_definitions},
            version=self._version,
            backend="duckdb",
            namespace=self._ns,
        )
        with storage_errors("State history ensure tables"):
            for stmt in statements:
                self._conn.execute(stmt)

        seen_rels: set[str] = set()
        for entity_type, entity_def in entity_definitions.items():
            properties = entity_def.get("properties", {})
            self._table_columns[entity_type] = list(properties.keys())

            for rel in entity_def.get("relationships", []):
                rel_type = rel["type"]
                if rel_type not in seen_rels:
                    seen_rels.add(rel_type)
                    rel_props = rel.get("properties", {})
                    self._rel_columns[rel_type] = list(rel_props.keys())

    def write_batch(
        self,
        snapshots: list[dict[str, Any]],
        tombstones: list[dict[str, Any]],
    ) -> None:
        """Write snapshots and tombstones grouped by entity type.

        Uses executemany for batch operations. DuckDB handles batch
        INSERT efficiently via its columnar engine. INSERT OR IGNORE
        for idempotent writes.
        """
        with storage_errors("State history write"):
            # ── Snapshots: close old versions + insert new ──
            if snapshots:
                by_type: dict[str, list[dict[str, Any]]] = defaultdict(list)
                for snap in snapshots:
                    by_type[snap["entity_type"]].append(snap)

                for entity_type, type_snaps in by_type.items():
                    table = f"{self._ns}_{self._version}.{entity_type}_history"
                    id_col = f"{entity_type}_id"
                    state_col = f"{entity_type}_state"
                    prop_names = self._table_columns.get(entity_type, [])

                    timing_cols = [
                        "last_event_time", "state_entered_time", "created_time",
                    ]
                    for gen in _split_generations(type_snaps):
                        close_params = [
                            (s["valid_from"], s["entity_id"], s["valid_from"])
                            for s in gen
                        ]
                        self._conn.executemany(
                            f"UPDATE {table} SET valid_to = ?"
                            f" WHERE {id_col} = ? AND valid_to IS NULL"
                            f" AND valid_from < ?",
                            close_params,
                        )

                        col_names = [
                            id_col, state_col, *prop_names,
                            "valid_from", *timing_cols,
                        ]
                        placeholders = ", ".join("?" for _ in col_names)
                        col_sql = ", ".join(col_names)

                        insert_params = []
                        for snap in gen:
                            props = snap.get("properties", {})
                            prop_values = [
                                json.dumps(props.get(p))
                                if isinstance(props.get(p), (list, dict))
                                else props.get(p)
                                for p in prop_names
                            ]
                            insert_params.append((
                                snap["entity_id"],
                                snap["state"],
                                *prop_values,
                                snap["valid_from"],
                                *(snap.get(c) for c in timing_cols),
                            ))

                        self._conn.executemany(
                            f"INSERT OR IGNORE INTO {table} ({col_sql})"
                            f" VALUES ({placeholders})",
                            insert_params,
                        )

            # ── Relationships ──
            if snapshots and self._rel_columns:
                self._write_relationships(snapshots)

            # ── Tombstones: close merged entity versions ──
            if tombstones:
                tomb_by_type: dict[str, list[dict[str, Any]]] = defaultdict(list)
                for tomb in tombstones:
                    tomb_by_type[tomb["entity_type"]].append(tomb)

                for entity_type, type_tombs in tomb_by_type.items():
                    table = f"{self._ns}_{self._version}.{entity_type}_history"
                    id_col = f"{entity_type}_id"
                    tomb_params = [
                        (
                            t.get("timestamp", t.get("valid_from")),
                            t.get("merged_into"),
                            t["entity_id"],
                        )
                        for t in type_tombs
                    ]
                    self._conn.executemany(
                        f"UPDATE {table} SET valid_to = ?, merged_into = ?"
                        f" WHERE {id_col} = ? AND valid_to IS NULL",
                        tomb_params,
                    )

    def _write_relationships(self, snapshots: list[dict[str, Any]]) -> None:
        """Write relationship snapshots from entity snapshots.

        Same pattern as SQLite — executemany with close+insert per generation.
        """
        rel_by_type: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for snap in snapshots:
            for rel in snap.get("relationships", []):
                rel_type = rel["relationship_type"]
                if rel_type in self._rel_columns:
                    rel_by_type[rel_type].append({
                        "source_id": snap["entity_id"],
                        "target_id": rel["target_entity_id"],
                        "relationship_type": rel_type,
                        "properties": rel.get("properties", {}),
                        "valid_from": snap["valid_from"],
                    })

        for rel_type, rel_snaps in rel_by_type.items():
            table = f"{self._ns}_{self._version}.rel_{rel_type}_history"
            prop_names = self._rel_columns[rel_type]

            for gen in _split_generations_rel(rel_snaps):
                # Close current versions
                close_params = [
                    (r["valid_from"], r["source_id"], r["target_id"])
                    for r in gen
                ]
                self._conn.executemany(
                    f"UPDATE {table} SET valid_to = ?"
                    f" WHERE source_id = ? AND target_id = ?"
                    f" AND valid_to IS NULL",
                    close_params,
                )

                # Insert new versions
                col_names = [
                    "source_id", "target_id", "relationship_type",
                    *prop_names, "valid_from",
                ]
                placeholders = ", ".join("?" for _ in col_names)
                col_sql = ", ".join(col_names)

                insert_rows = []
                for r in gen:
                    props = r["properties"]
                    prop_values = [
                        json.dumps(props.get(p))
                        if isinstance(props.get(p), (list, dict))
                        else props.get(p)
                        for p in prop_names
                    ]
                    insert_rows.append((
                        r["source_id"],
                        r["target_id"],
                        r["relationship_type"],
                        *prop_values,
                        r["valid_from"],
                    ))

                self._conn.executemany(
                    f"INSERT OR IGNORE INTO {table} ({col_sql})"
                    f" VALUES ({placeholders})",
                    insert_rows,
                )

    def delete_entity(self, entity_id: str) -> int:
        """Delete all history rows for an entity across all known tables."""
        total = 0
        with storage_errors("State history delete entity"):
            for entity_type in self._table_columns:
                table = f"{self._ns}_{self._version}.{entity_type}_history"
                id_col = f"{entity_type}_id"
                result = self._conn.execute(
                    f"SELECT count(*) FROM {table} WHERE {id_col} = ?",
                    (entity_id,),
                )
                count = result.fetchone()[0]
                self._conn.execute(
                    f"DELETE FROM {table} WHERE {id_col} = ?",
                    (entity_id,),
                )
                total += count
            for rel_type in self._rel_columns:
                table = f"{self._ns}_{self._version}.rel_{rel_type}_history"
                result = self._conn.execute(
                    f"SELECT count(*) FROM {table}"
                    f" WHERE source_id = ? OR target_id = ?",
                    (entity_id, entity_id),
                )
                count = result.fetchone()[0]
                self._conn.execute(
                    f"DELETE FROM {table}"
                    f" WHERE source_id = ? OR target_id = ?",
                    (entity_id, entity_id),
                )
                total += count
        return total

    def read_current_entities(
        self, entity_type: str, prop_names: list[str],
    ) -> Iterator[dict[str, Any]]:
        """Stream current entities from DuckDB using fetchmany."""
        table = f"{self._ns}_{self._version}.{entity_type}_history"
        id_col = f"{entity_type}_id"
        state_col = f"{entity_type}_state"
        timing_cols = ["last_event_time", "state_entered_time", "created_time"]
        select_cols = [id_col, state_col, *prop_names, *timing_cols]
        col_sql = ", ".join(select_cols)

        with storage_errors("State history read current entities"):
            result = self._conn.execute(
                f"SELECT {col_sql} FROM {table}"
                f" WHERE valid_to IS NULL AND merged_into IS NULL",
            )
            while True:
                rows = result.fetchmany(5000)
                if not rows:
                    break
                for row in rows:
                    props = {}
                    for i, pname in enumerate(prop_names):
                        val = row[2 + i]
                        props[pname] = val
                    offset = 2 + len(prop_names)
                    yield {
                        "entity_id": row[0],
                        "entity_type": entity_type,
                        "state": row[1],
                        "properties": props,
                        "last_event_time": (
                            row[offset].isoformat() if row[offset] else None
                        ),
                        "state_entered_time": (
                            row[offset + 1].isoformat()
                            if row[offset + 1] else None
                        ),
                        "created_time": (
                            row[offset + 2].isoformat()
                            if row[offset + 2] else None
                        ),
                    }

    def current_state(self, entity_type: str) -> Any:
        """Deferred to query layer design session."""
        raise NotImplementedError

    def history(self, entity_type: str) -> Any:
        """Deferred to query layer design session."""
        raise NotImplementedError

    def as_of(self, entity_type: str, timestamp: datetime) -> Any:
        """Deferred to query layer design session."""
        raise NotImplementedError

    @property
    def connection(self) -> Any:
        """The underlying duckdb connection."""
        return self._conn

    def close(self) -> None:
        """Close the database connection."""
        self._conn.close()
