"""Defacto error hierarchy.

All framework errors inherit from DefactoError. Each error carries structured
context (details dict) and a recoverable flag so callers can decide whether
to retry or propagate.

Error categories:
    - ConfigError: bad configuration (URLs, parameters, missing settings)
    - DefinitionError: invalid YAML definitions (schema violations, bad references)
    - IngestError: normalization failure (missing fields, type coercion)
    - ValidationError: event data fails schema validation
    - BuildError: interpretation or build process failure
    - StorageError: database read/write failure (Postgres, SQLite)
    - IdentityError: identity resolution failure (merge conflict, corruption)
    - NotFoundError: entity or version doesn't exist
    - ConsumerError: Kafka consumer or state history write failure

Utilities:
    - storage_errors(operation): context manager that wraps raw database
      exceptions (psycopg, sqlite3, duckdb) in StorageError with context.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any

logger = logging.getLogger("defacto.storage")


class DefactoError(Exception):
    """Base exception for all Defacto framework errors.

    Every Defacto error carries:
        message: Human-readable description of what went wrong.
        details: Structured context (entity_id, event_id, field names, etc.)
            for programmatic inspection.
        recoverable: Whether retrying the operation might succeed. Data errors
            (bad input) are not recoverable. Infrastructure errors (DB down)
            may be.
    """

    def __init__(
        self,
        message: str,
        *,
        details: dict[str, Any] | None = None,
        recoverable: bool = False,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}
        self.recoverable = recoverable

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.message!r})"


class ConfigError(DefactoError):
    """Invalid configuration — bad URLs, missing parameters, unreachable services."""


class DefinitionError(DefactoError):
    """Invalid YAML definitions — missing fields, bad state references, relationship mismatches."""


class IngestError(DefactoError):
    """Normalization failure during ingest — missing required fields, type coercion errors.

    Raised when ALL events in a batch fail normalization, indicating a handler
    bug rather than bad data. Individual event failures are captured in
    IngestResult.failures, not raised as exceptions.
    """


class ValidationError(DefactoError):
    """Event data fails schema validation after normalization.

    Schema validation is advisory — events are still ingested. This error
    is raised only when validation is configured to be blocking.
    """


class BuildError(DefactoError):
    """Interpretation or build process failure — definition inconsistency, engine error."""


class StorageError(DefactoError):
    """Database read/write failure — Postgres down, SQLite locked, disk full.

    Storage errors are typically recoverable (infrastructure issue, not data issue).
    The framework does NOT retry automatically — the orchestration layer handles retry.
    """

    def __init__(
        self,
        message: str,
        *,
        details: dict[str, Any] | None = None,
    ) -> None:
        # Storage errors are recoverable by default — infrastructure, not data.
        super().__init__(message, details=details, recoverable=True)


class IdentityError(DefactoError):
    """Identity resolution failure — merge conflict, canonical chain corruption."""


class NotFoundError(DefactoError):
    """Entity, version, or resource not found."""


class QueryError(DefactoError):
    """Query validation failure — unknown table, unknown column, invalid reference.

    Raised before execution when the generated SQL references tables or
    columns that don't exist in the entity definitions. Provides clear
    error messages with available names so the user can fix typos.
    """


class ConsumerError(DefactoError):
    """Kafka consumer or state history write failure.

    May be recoverable (broker temporarily unavailable) or not (incompatible
    schema version).
    """


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


@contextmanager
def storage_errors(operation: str):
    """Wrap raw database exceptions in StorageError with operation context.

    Usage:
        with storage_errors("ledger append"):
            cur.execute(...)

    Catches psycopg.Error, sqlite3.Error, and duckdb.Error and re-raises
    as StorageError with a message like "Ledger append failed: connection refused".
    The original exception is chained via ``from exc`` for full tracebacks.
    Logs at ERROR level before re-raising so every storage failure is
    observable without additional logging at call sites.
    """
    try:
        yield
    except StorageError:
        raise  # already wrapped, don't double-wrap
    except Exception as exc:
        # Check if it's a database driver exception
        exc_module = type(exc).__module__ or ""
        if any(
            driver in exc_module
            for driver in ("psycopg", "sqlite3", "duckdb")
        ):
            logger.error("storage operation failed", extra={
                "operation": operation,
                "driver": exc_module,
                "error": str(exc),
            })
            raise StorageError(
                f"{operation} failed: {exc}",
                details={"operation": operation, "driver": exc_module},
            ) from exc
        raise  # not a database exception, let it propagate unchanged
