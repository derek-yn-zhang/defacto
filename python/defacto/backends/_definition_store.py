"""Definition version storage in the database.

Definitions are stored in the definition_versions table alongside the
ledger and identity. This is the coordination point for multi-process
deployments — all processes sharing the same database see the same
definitions.

DefinitionsStore is the abstract interface. SqliteDefinitionsStore and
PostgresDefinitionsStore implement it with their own SQL dialects.
"""

from __future__ import annotations

import json
import sqlite3
from abc import ABC, abstractmethod
from typing import Any

from defacto.errors import ConfigError, DefinitionError, NotFoundError

# Postgres error import deferred to PostgresDefinitionsStore.__init__
# to avoid import when psycopg isn't installed (optional dependency).


class DefinitionsStore(ABC):
    """Abstract interface for definition version storage.

    This is the coordination point for multi-process deployments: all Defacto
    instances and consumers sharing the same database read definitions from
    this table. Version activation is a database UPDATE — every process sees
    the change on its next read. Exactly one version is active at any time.
    """

    @abstractmethod
    def register(self, version: str, definitions: dict[str, Any], hashes: dict[str, str]) -> None:
        """Store a version. Does NOT activate.

        Raises:
            DefinitionError: If version already exists.
        """

    @abstractmethod
    def activate(self, version: str) -> None:
        """Set active version. Deactivates all others.

        Raises:
            NotFoundError: If version doesn't exist.
        """

    @abstractmethod
    def active_version(self) -> str:
        """Read the currently active version name.

        Raises:
            ConfigError: If no version is active.
        """

    @abstractmethod
    def get(self, version: str) -> dict[str, Any]:
        """Read definitions dict for a version.

        Raises:
            NotFoundError: If version doesn't exist.
        """

    @abstractmethod
    def versions(self) -> list[dict[str, Any]]:
        """List all registered versions with metadata."""

    @abstractmethod
    def hashes(self, version: str) -> dict[str, str]:
        """Read definition_hash, source_hash, identity_hash for a version.

        Raises:
            NotFoundError: If version doesn't exist.
        """

    @abstractmethod
    def close(self) -> None:
        """Close the database connection and release resources."""


class SqliteDefinitionsStore(DefinitionsStore):
    """SQLite definitions storage — development, single-process."""

    def __init__(self, db_path: str) -> None:
        """Initialize with a database path and create the table."""
        self._conn = sqlite3.connect(db_path)
        self._create_table()

    def _create_table(self) -> None:
        """Create the definition_versions table if it doesn't exist."""
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS definition_versions (
                version          TEXT PRIMARY KEY,
                definitions      TEXT NOT NULL,
                definition_hash  TEXT NOT NULL,
                source_hash      TEXT NOT NULL,
                identity_hash    TEXT NOT NULL,
                active           INTEGER NOT NULL DEFAULT 0,
                created_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            )
        """)
        self._conn.commit()

    def register(self, version: str, definitions: dict[str, Any], hashes: dict[str, str]) -> None:
        """Store a version. Does NOT activate.

        Raises:
            DefinitionError: If version already exists.
        """
        try:
            with self._conn:
                self._conn.execute(
                    "INSERT INTO definition_versions"
                    " (version, definitions, definition_hash, source_hash, identity_hash)"
                    " VALUES (?, ?, ?, ?, ?)",
                    (
                        version,
                        json.dumps(definitions),
                        hashes["definition_hash"],
                        hashes["source_hash"],
                        hashes["identity_hash"],
                    ),
                )
        except sqlite3.IntegrityError:
            raise DefinitionError(
                f"Version '{version}' already exists",
                details={"version": version},
            )

    def activate(self, version: str) -> None:
        """Set active version. Deactivates all others in one transaction."""
        with self._conn:
            self._conn.execute(
                "UPDATE definition_versions SET active = 0 WHERE active = 1"
            )
            cursor = self._conn.execute(
                "UPDATE definition_versions SET active = 1 WHERE version = ?",
                (version,),
            )
            if cursor.rowcount == 0:
                raise NotFoundError(
                    f"Version '{version}' not found",
                    details={"version": version},
                )

    def active_version(self) -> str:
        """Read the currently active version name."""
        row = self._conn.execute(
            "SELECT version FROM definition_versions WHERE active = 1"
        ).fetchone()
        if row is None:
            raise ConfigError("No active definition version")
        return row[0]

    def get(self, version: str) -> dict[str, Any]:
        """Read definitions dict for a version."""
        row = self._conn.execute(
            "SELECT definitions FROM definition_versions WHERE version = ?",
            (version,),
        ).fetchone()
        if row is None:
            raise NotFoundError(
                f"Version '{version}' not found",
                details={"version": version},
            )
        return json.loads(row[0])

    def versions(self) -> list[dict[str, Any]]:
        """List all registered versions with metadata."""
        rows = self._conn.execute(
            "SELECT version, active, created_at"
            " FROM definition_versions ORDER BY created_at"
        ).fetchall()
        return [
            {"version": r[0], "active": bool(r[1]), "created_at": r[2]}
            for r in rows
        ]

    def hashes(self, version: str) -> dict[str, str]:
        """Read definition_hash, source_hash, identity_hash for a version."""
        row = self._conn.execute(
            "SELECT definition_hash, source_hash, identity_hash"
            " FROM definition_versions WHERE version = ?",
            (version,),
        ).fetchone()
        if row is None:
            raise NotFoundError(
                f"Version '{version}' not found",
                details={"version": version},
            )
        return {
            "definition_hash": row[0],
            "source_hash": row[1],
            "identity_hash": row[2],
        }

    def close(self) -> None:
        self._conn.close()


class PostgresDefinitionsStore(DefinitionsStore):
    """Postgres definitions storage — production, multi-process.

    Tables in shared `defacto` schema. Uses JSONB for definitions,
    TIMESTAMPTZ for created_at.
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
        self._ns = namespace
        self._create_table()

    def _create_table(self) -> None:
        """Create the namespace schema and definition_versions table."""
        ns = self._ns
        with self._conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {ns}")
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {ns}.definition_versions (
                    version          TEXT PRIMARY KEY,
                    definitions      JSONB       NOT NULL,
                    definition_hash  TEXT        NOT NULL,
                    source_hash      TEXT        NOT NULL,
                    identity_hash    TEXT        NOT NULL,
                    active           BOOLEAN     NOT NULL DEFAULT FALSE,
                    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)
        self._conn.commit()

    def register(self, version: str, definitions: dict[str, Any], hashes: dict[str, str]) -> None:
        """Store a version. Does NOT activate.

        Raises:
            DefinitionError: If version already exists.
        """
        import psycopg
        from psycopg.types.json import Jsonb

        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    f"INSERT INTO {self._ns}.definition_versions"
                    " (version, definitions, definition_hash, source_hash, identity_hash)"
                    " VALUES (%s, %s, %s, %s, %s)",
                    (
                        version,
                        Jsonb(definitions),
                        hashes["definition_hash"],
                        hashes["source_hash"],
                        hashes["identity_hash"],
                    ),
                )
            self._conn.commit()
        except psycopg.errors.UniqueViolation:
            self._conn.rollback()
            raise DefinitionError(
                f"Version '{version}' already exists",
                details={"version": version},
            )

    def activate(self, version: str) -> None:
        """Set active version. Deactivates all others."""
        with self._conn.cursor() as cur:
            cur.execute(
                f"UPDATE {self._ns}.definition_versions SET active = FALSE WHERE active = TRUE"
            )
            cur.execute(
                f"UPDATE {self._ns}.definition_versions SET active = TRUE WHERE version = %s",
                (version,),
            )
            if cur.rowcount == 0:
                self._conn.rollback()
                raise NotFoundError(
                    f"Version '{version}' not found",
                    details={"version": version},
                )
        self._conn.commit()

    def active_version(self) -> str:
        """Read the currently active version name."""
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT version FROM {self._ns}.definition_versions WHERE active = TRUE"
            )
            row = cur.fetchone()
        if row is None:
            raise ConfigError("No active definition version")
        return row[0]

    def get(self, version: str) -> dict[str, Any]:
        """Read definitions dict for a version. JSONB auto-deserializes."""
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT definitions FROM {self._ns}.definition_versions WHERE version = %s",
                (version,),
            )
            row = cur.fetchone()
        if row is None:
            raise NotFoundError(
                f"Version '{version}' not found",
                details={"version": version},
            )
        return row[0]  # JSONB → dict automatically

    def versions(self) -> list[dict[str, Any]]:
        """List all registered versions with metadata."""
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT version, active, created_at::text"
                f" FROM {self._ns}.definition_versions ORDER BY created_at"
            )
            rows = cur.fetchall()
        return [
            {"version": r[0], "active": bool(r[1]), "created_at": r[2]}
            for r in rows
        ]

    def hashes(self, version: str) -> dict[str, str]:
        """Read definition_hash, source_hash, identity_hash for a version."""
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT definition_hash, source_hash, identity_hash"
                f" FROM {self._ns}.definition_versions WHERE version = %s",
                (version,),
            )
            row = cur.fetchone()
        if row is None:
            raise NotFoundError(
                f"Version '{version}' not found",
                details={"version": version},
            )
        return {
            "definition_hash": row[0],
            "source_hash": row[1],
            "identity_hash": row[2],
        }

    def close(self) -> None:
        self._conn.close()
