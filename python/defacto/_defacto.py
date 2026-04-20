"""Defacto — the main entry point and orchestration layer.

This module contains the Defacto class (the primary user-facing API),
along with supporting classes for definition management, inspection,
and the state history consumer.

Usage:
    from defacto import Defacto

    m = Defacto("project/")
    m.ingest("app", events, process=True)
    m.table("customer").execute()
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import tempfile
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Self

import logging

from defacto._build import BuildManager
from defacto._core import DefactoCore
from defacto._dead_letter import DeadLetterSink
from defacto.backends._definition_store import DefinitionsStore
from defacto._identity import IdentityResolver
from defacto._pipeline import Pipeline
from defacto._ddl import detect_backend
from defacto.query import DefactoQuery, DefactoTable, TableCollection
from defacto._publisher import InlinePublisher, KafkaPublisher
from defacto.backends import SqliteIdentity, SqliteLedger, SqliteStateHistory

logger = logging.getLogger("defacto.lifecycle")


# ---------------------------------------------------------------------------
# Backend registry — adding a new backend = implement the ABCs, define a
# BackendSet class, and add one entry to _BACKEND_REGISTRY.
# ---------------------------------------------------------------------------


class _SqliteBackends:
    """SQLite backend classes — development, single-process."""

    from defacto._build import SqliteBuildStateStore as build_state
    from defacto.backends._definition_store import SqliteDefinitionsStore as definitions

    ledger = SqliteLedger
    identity = SqliteIdentity
    state_history = SqliteStateHistory


class _PostgresBackends:
    """Postgres backend classes — production."""

    from defacto._build import PostgresBuildStateStore as build_state
    from defacto.backends._definition_store import PostgresDefinitionsStore as definitions
    from defacto.backends._identity import PostgresIdentity as identity
    from defacto.backends._ledger import PostgresLedger as ledger
    from defacto.backends._state_history import PostgresStateHistory as state_history


class _DuckDBBackends:
    """DuckDB backend classes — local columnar analytics.

    Only state_history is supported. DuckDB is used as a consumer store
    for analytical queries, not as a full backend for the Defacto engine.
    """

    from defacto.backends._state_history import DuckDBStateHistory as state_history


# URL scheme → backend set. All backend constructors take (db_path) or
# (db_path, version). Dispatched in _create_backends.
_BACKEND_REGISTRY: dict[str, type] = {
    "sqlite": _SqliteBackends,
    "postgresql": _PostgresBackends,
    "postgres": _PostgresBackends,
    "duckdb": _DuckDBBackends,
}


def _content_hash(defs_dict: dict[str, Any]) -> str:
    """Deterministic version name from definition content.

    Produces an 8-character hex string from the SHA-256 of the canonical
    JSON representation. Same definitions always produce the same hash,
    so re-opening an unchanged project directory doesn't create a new
    version.
    """
    canonical = json.dumps(defs_dict, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()[:8]


_LOG_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
}
_logging_configured = False


def _setup_logging(log_level: str, log_format: str) -> None:
    """Configure Defacto logging from entry point parameters.

    Idempotent — only configures on the first call. Subsequent calls
    (e.g., creating a second Defacto instance) are no-ops so handlers
    don't accumulate.
    """
    global _logging_configured
    if _logging_configured:
        return
    _logging_configured = True

    from defacto._logging import configure_logging

    level = _LOG_LEVELS.get(log_level.upper(), logging.INFO)
    configure_logging(level=level, log_format=log_format)


_COLD_START_BATCH = 10_000


def _cold_start_recover(
    core: Any,
    state_history: Any,
    identity: Any,
    build_manager: Any,
    pipeline: Any,
    version: str,
) -> None:
    """Restore entity state from state history instead of replaying the ledger.

    Handles both clean restarts and crash recovery. State history is always
    consistent through the last committed batch (writes are atomic per batch,
    cursor tracks committed position). Loading from state history + processing
    the gap from cursor is correct in both cases.

    If dirty flag is set (crash during previous build), clears it — loading
    from state history IS the recovery. detect_mode then decides what's needed
    based on hashes and cursor position, not the dirty flag.

    Peak Python memory is bounded by batch size (~10K entities) regardless
    of total entity count.
    """
    # Warm identity cache — needed for correct resolution of any new events
    identity.warmup()

    # Stream entities from state history → DashMap, one entity type at a time
    for entity_type, prop_names in state_history.entity_columns().items():
        batch: list[dict] = []
        for entity in state_history.read_current_entities(
            entity_type, prop_names,
        ):
            batch.append(entity)
            if len(batch) >= _COLD_START_BATCH:
                core.load_entities(batch)
                batch.clear()
        if batch:
            core.load_entities(batch)

    # Clear dirty flag if set — loading from state history is the recovery.
    # This lets detect_mode return the right mode based on hashes + cursor
    # instead of blindly returning FULL.
    build_manager.clear_dirty(version, "COLD_START")

    # Process any gap between stored cursor and ledger head
    mode = build_manager.detect_mode(version, pipeline._ledger.cursor())
    if mode == "INCREMENTAL":
        pipeline.build_incremental(version)
    elif mode != "SKIP":
        # Hash mismatch (RENORMALIZE, IDENTITY_RESET, FULL) — definitions
        # changed while the process was down. Full rebuild needed.
        pipeline.build_full(version, mode=mode)


def _create_backends(
    db_path: str, version: str, *,
    cold_ledger: str | None = None, namespace: str = "defacto",
) -> dict[str, Any]:
    """Create all backend instances for the given database type.

    Dispatches based on URL scheme: "postgresql://..." → Postgres,
    file path → SQLite. Adding a backend = implement ABCs, write a
    backend set class, add one entry to _BACKEND_REGISTRY.

    For Postgres, the ledger and build_state share a connection — both
    write to the namespace schema system tables. Sharing eliminates
    separate commits on the hot path: cursor and watermark updates
    ride along with the ledger's transaction atomically.

    When cold_ledger is provided, wraps the Postgres ledger in a
    TieredLedger that adds Delta Lake cold storage. Only valid with
    Postgres hot storage.

    Args:
        namespace: Schema prefix for Postgres tables. Default 'defacto'.
            Infrastructure tables go in {namespace}. (e.g., defacto.ledger).
            State history goes in {namespace}_{version}. schemas.
            Ignored for SQLite (no schema support).
    """
    scheme = db_path.split("://")[0] if "://" in db_path else "sqlite"
    backends = _BACKEND_REGISTRY.get(scheme)
    if backends is None:
        raise ValueError(
            f"Unsupported database scheme '{scheme}'. "
            f"Supported: {', '.join(_BACKEND_REGISTRY)}"
        )

    is_pg = scheme in ("postgresql", "postgres")

    if cold_ledger:
        from defacto.backends._ledger import TieredLedger
        hot_ledger = backends.ledger(db_path, namespace=namespace) if is_pg else backends.ledger(db_path)
        ledger = TieredLedger(hot_ledger, cold_ledger)
    else:
        hot_ledger = None
        ledger = backends.ledger(db_path, namespace=namespace) if is_pg else backends.ledger(db_path)

    # Share the ledger connection with build_state — both backends write
    # to system tables during builds, and sharing eliminates separate
    # commits on the hot path. For SQLite this also prevents "database
    # is locked" errors (one writer at a time, cursor + writes on the
    # same connection avoids cross-connection lock contention). Other
    # backends (identity, state_history, definitions) keep their own
    # connections — they don't conflict during builds and identity needs
    # a separate connection for parallel I/O in process_batch.
    hot = hot_ledger or ledger
    if is_pg:
        build_state = backends.build_state(
            db_path,
            namespace=namespace,
            connection=hot.connection,
            in_transaction_fn=lambda: hot._in_transaction,
        )
    elif scheme == "sqlite":
        build_state = backends.build_state(
            db_path, connection=hot.connection,
        )
    else:
        build_state = backends.build_state(db_path)

    return {
        "ledger": ledger,
        "identity": backends.identity(db_path, namespace=namespace) if is_pg else backends.identity(db_path),
        "state_history": (
            backends.state_history(db_path, version, namespace=namespace)
            if is_pg or scheme == "duckdb"
            else backends.state_history(db_path, version)
        ),
        "definitions": backends.definitions(db_path, namespace=namespace) if is_pg else backends.definitions(db_path),
        "build_state": build_state,
    }
from defacto.definitions import Definitions
from defacto.errors import ConfigError, DefinitionError, NotFoundError
from defacto.results import (
    BuildResult,
    BuildStatus,
    ConsumerResult,
    ConsumerStatus,
    EraseResult,
    EventFailure,
    IngestResult,
    MergeResult,
    RegisterResult,
    TickResult,
    Timeline,
    ValidationResult,
)


# ---------------------------------------------------------------------------
# Defacto — the primary API
# ---------------------------------------------------------------------------


class Defacto:
    """Defacto event-sourcing entity engine.

    Create an instance by passing definitions as a directory path, a dict,
    or a Definitions object. All configuration options are the same
    regardless of how definitions are provided.

    Usage:
        # From YAML directory (development)
        d = Defacto("project/")

        # From dict (programmatic)
        d = Defacto({"entities": {...}, "sources": {...}})

        # From Definitions object
        d = Defacto(definitions_obj)

        # With production config
        d = Defacto("project/", database="postgresql://...", kafka={...})

        # Ingest, build, query
        d.ingest("app", events)
        d.build()
        d.table("customer").execute()
    """


    # -- Public constructor --

    def __init__(
        self,
        config: str | dict[str, Any] | Definitions,
        *,
        database: str | None = None,
        version: str | None = None,
        batch_size: int = 100,
        workers: int = 1,
        kafka: dict[str, Any] | None = None,
        cold_ledger: str | None = None,
        dead_letter: dict[str, Any] | None = None,
        shard_id: int | None = None,
        total_shards: int | None = None,
        graph: str | None = None,
        namespace: str = "defacto",
        log_level: str = "INFO",
        log_format: str = "console",
    ) -> None:
        """Create a Defacto instance.

        Args:
            config: Entity definitions — one of:
                - str: path to a project directory containing YAML definitions
                - dict: definition dict with 'entities', 'sources', 'schemas'
                - Definitions: a pre-built Definitions object
            database: Database URL. None = SQLite (auto-created).
            version: Explicit version name. None = content hash.
            batch_size: Events per processing batch.
            workers: Parallel Rust computation threads.
            kafka: Kafka config for effects streaming. None = inline.
            cold_ledger: Cold storage URL for ledger archival.
            dead_letter: Dead letter config for failed events.
            shard_id: Shard index for multi-process deployments.
            total_shards: Total number of shards.
            graph: Graph backend URL. None = NetworkX, 'bolt://...' = Neo4j.
            namespace: Postgres schema prefix. Default 'defacto'.
            log_level: 'DEBUG', 'INFO', 'WARNING', or 'ERROR'.
            log_format: 'console' or 'json'.

        Raises:
            DefinitionError: If definitions are invalid.
            ConfigError: If database/Kafka config is invalid.
        """
        _setup_logging(log_level, log_format)

        # Resolve definitions from whatever form was provided
        if isinstance(config, str):
            # Directory path — read YAML definitions from disk
            defs = Definitions.from_directory(config)
            workdir = os.path.join(config, ".defacto")
            os.makedirs(workdir, exist_ok=True)
            db_path = database or os.path.join(workdir, "defacto.db")
        elif isinstance(config, Definitions):
            defs = config
            db_path = database or os.path.join(tempfile.mkdtemp(), "defacto.db")
        else:
            defs = Definitions.from_dict(config)
            db_path = database or os.path.join(tempfile.mkdtemp(), "defacto.db")

        # Validate — errors block, warnings log
        defs_dict = defs.to_dict()
        validation = defs.validate()
        if not validation.valid:
            raise DefinitionError(
                f"Invalid definitions: {'; '.join(validation.errors)}",
                details={"errors": validation.errors},
            )
        for w in validation.warnings:
            logger.warning("definition warning", extra={
                "operation": "validate", "warning": w,
            })

        v = version or _content_hash(defs_dict)
        built = self._build_instance(
            defs_dict, db_path, v,
            batch_size=batch_size, workers=workers, kafka=kafka,
            dead_letter=dead_letter, cold_ledger=cold_ledger,
            shard_id=shard_id, total_shards=total_shards,
            graph=graph, namespace=namespace,
        )
        # Copy all state from the built instance
        self.__dict__.update(built.__dict__)

    # -- Private component constructor --

    def _init_components(
        self,
        *,
        core: Any,
        ledger: Any,
        identity_backend: Any,
        identity: IdentityResolver,
        state_history: Any,
        publisher: Any,
        pipeline: Pipeline,
        build_manager: BuildManager,
        definitions_db: DefinitionsStore,
        dead_letter: Any,
        definitions_dict: dict[str, Any],
        active_version: str,
        batch_size: int,
        db_path: str = "",
        graph: str | None = None,
        namespace: str = "defacto",
    ) -> None:
        self._core = core
        self._ledger = ledger
        self._identity_backend = identity_backend
        self._identity = identity
        self._state_history = state_history
        self._publisher = publisher
        self._pipeline = pipeline
        self._build_manager = build_manager
        self._definitions_db = definitions_db
        self._dead_letter = dead_letter
        self._definitions_dict = definitions_dict
        self._active_version = active_version
        self._batch_size = batch_size
        self._db_path = db_path
        self._graph_url = graph
        self._namespace = namespace
        self._pending: list[tuple[str, dict[str, Any]]] = []
        self._closed = False
        self.__query: DefactoQuery | None = None
        self.__graph_backend: Any = None
        self.__definitions_manager: DefinitionsManager | None = None

    # -- Shared factory wiring --

    @classmethod
    def _build_instance(
        cls,
        defs_dict: dict[str, Any],
        db_path: str,
        version: str,
        *,
        batch_size: int,
        workers: int,
        kafka: dict[str, Any] | None,
        dead_letter: dict[str, Any] | None = None,
        cold_ledger: str | None = None,
        shard_id: int | None = None,
        total_shards: int | None = None,
        graph: str | None = None,
        namespace: str = "defacto",
    ) -> Self:
        """Wire up all internal components from definitions and a database path.

        Shared by from_directory and from_config — the only difference
        between them is how definitions and db_path are determined.
        """
        # Rust computation core
        core = DefactoCore(workers=workers)
        core.compile(version, defs_dict)
        core.activate(version)

        # Storage backends — dispatched by URL scheme (sqlite, postgresql, etc.)
        b = _create_backends(db_path, version, cold_ledger=cold_ledger, namespace=namespace)
        ledger = b["ledger"]
        identity_backend = b["identity"]
        state_history = b["state_history"]
        definitions_db = b["definitions"]

        state_history.ensure_tables(defs_dict.get("entities", {}))

        # Identity resolution — cache warming from backend.
        # Ledger reference enables merge_log validation of cache hits
        # (prevents stale cache from misrouting events after cross-shard merges).
        identity = IdentityResolver(core, identity_backend, ledger=ledger)
        identity.warmup()

        # Publisher — inline (dev) or Kafka (production)
        publisher = InlinePublisher(state_history) if kafka is None else KafkaPublisher(kafka)

        # Dead letter sink — routes failures to file, Kafka, or nowhere
        dead_letter_sink = DeadLetterSink.create(dead_letter)

        # Definition version storage — register + activate
        hashes = core.definition_hashes(version)
        try:
            definitions_db.register(version, defs_dict, hashes)

            # New version registered — seed its build_state with the
            # previous active version's hashes so detect_mode correctly
            # identifies what changed (e.g., only identity →
            # FULL_WITH_IDENTITY_RESET, not FULL_RENORMALIZE from
            # comparing against empty defaults). Only runs for truly new
            # versions — existing versions keep their own build_state.
            try:
                prev_version = definitions_db.active_version()
                prev_hashes = definitions_db.hashes(prev_version)
                build_state = b["build_state"]
                build_state.get_state(version)
                build_state.clear_dirty(version, "SKIP", prev_hashes, "")
            except (ConfigError, NotFoundError):
                pass  # No previous version (first startup)

        except DefinitionError:
            pass  # Already registered (re-opening same directory)
        definitions_db.activate(version)

        # Build state management (shard-aware — qualifies build_state keys internally)
        build_manager = BuildManager(
            b["build_state"], core,
            shard_id=shard_id, total_shards=total_shards,
        )

        # Seed build_state if this is the very first build — a fresh row
        # has NULL hashes which would trigger FULL_RENORMALIZE (because
        # current hashes != NULL). Seed with current hashes so
        # detect_mode returns FULL (rebuild entities) instead of
        # RENORMALIZE (rewrite ledger). Only seed when ALL hashes are
        # NULL — if any hash is set, the version has been built before
        # and a hash mismatch is a genuine definition change.
        if build_manager.has_empty_hashes(version):
            build_manager.seed_hashes(version)

        # Batch processing pipeline (shard-aware — filters entity_mapping before interpret)
        pipeline = Pipeline(
            core, ledger, identity, publisher, build_manager, dead_letter_sink,
            shard_id=shard_id, total_shards=total_shards,
        )

        # Cold start recovery — entity state lives in DashMap (in-memory)
        # and is lost on process restart. If this version has processed
        # events before (stored_cursor > 0) but DashMap is empty
        # (entity_count == 0), restore from state history instead of
        # replaying the full ledger. Cost is proportional to entities
        # (seconds) instead of events (hours at scale).
        #
        # Uses stored_cursor (from build_state), not ledger.cursor().
        # The ledger is shared across versions — a new version has
        # stored_cursor = 0 even though the ledger is full. We only
        # auto-recover for versions that have previously built, not
        # for new versions where the user's build() needs to run with
        # the correct mode (FULL_WITH_IDENTITY_RESET, etc.).
        stored_cursor = build_manager.get_status(version).cursor
        if stored_cursor > 0 and core.entity_count() == 0:
            _cold_start_recover(
                core, state_history, identity, build_manager,
                pipeline, version,
            )

        obj = object.__new__(cls)
        obj._init_components(
            core=core,
            ledger=ledger,
            identity_backend=identity_backend,
            identity=identity,
            state_history=state_history,
            publisher=publisher,
            pipeline=pipeline,
            build_manager=build_manager,
            definitions_db=definitions_db,
            dead_letter=dead_letter_sink,
            definitions_dict=defs_dict,
            active_version=version,
            batch_size=batch_size,
            db_path=db_path,
            graph=graph,
            namespace=namespace,
        )
        return obj

    # -- Constructors --

    @classmethod
    def from_directory(
        cls,
        path: str,
        *,
        database: str | None = None,
        batch_size: int = 100,
        workers: int = 1,
        kafka: dict[str, Any] | None = None,
        cold_ledger: str | None = None,
        dead_letter: dict[str, Any] | None = None,
        shard_id: int | None = None,
        total_shards: int | None = None,
        graph: str | None = None,
        namespace: str = "defacto",
        log_level: str = "INFO",
        log_format: str = "console",
    ) -> Self:
        """Create a Defacto instance from a project directory.

        Auto-discovers entity definitions, source handlers, and event schemas
        from the filesystem. Primary entry point for development.

        Args:
            path: Project directory containing entities/, sources/, events/ subdirs.
            database: Database URL. None = SQLite (default), or 'postgresql://...'.
            batch_size: Events accumulated before processing. Higher = more throughput.
            workers: Parallel Rust computation threads.
            kafka: Kafka config for effects streaming. None = inline processing.
            cold_ledger: Cold storage URL for ledger archival. None = hot only.
            dead_letter: Dead letter config for failed events. None = failures in result only.
            namespace: Postgres schema prefix. Default 'defacto'. Enables multiple
                independent environments on the same database.
            log_level: Logging level. 'DEBUG', 'INFO', 'WARNING', or 'ERROR'.
            log_format: Log output format. 'console' (scannable columns) or
                'json' (structured, one JSON object per line).

        Returns:
            A configured Defacto instance ready for ingest/build/query.

        Raises:
            DefinitionError: If YAML definitions are invalid.
            ConfigError: If database URL is unreachable or Kafka config is invalid.
        """
        _setup_logging(log_level, log_format)

        defs = Definitions.from_directory(path)
        defs_dict = defs.to_dict()
        # Validate — same checks as from_config. Errors block, warnings log.
        validation = defs.validate()
        if not validation.valid:
            raise DefinitionError(
                f"Invalid definitions: {'; '.join(validation.errors)}",
                details={"errors": validation.errors},
            )
        for w in validation.warnings:
            logger.warning("definition warning", extra={
                "operation": "validate", "warning": w,
            })

        # Working directory for framework state (.defacto/ alongside project)
        workdir = os.path.join(path, ".defacto")
        os.makedirs(workdir, exist_ok=True)
        db_path = database or os.path.join(workdir, "defacto.db")

        version = _content_hash(defs_dict)
        return cls._build_instance(
            defs_dict, db_path, version,
            batch_size=batch_size, workers=workers, kafka=kafka,
            dead_letter=dead_letter, cold_ledger=cold_ledger,
            shard_id=shard_id, total_shards=total_shards,
            graph=graph, namespace=namespace,
        )

    @classmethod
    def from_config(
        cls,
        config: dict[str, Any] | Definitions,
        *,
        database: str | None = None,
        version: str | None = None,
        batch_size: int = 100,
        workers: int = 1,
        kafka: dict[str, Any] | None = None,
        cold_ledger: str | None = None,
        dead_letter: dict[str, Any] | None = None,
        shard_id: int | None = None,
        total_shards: int | None = None,
        graph: str | None = None,
        namespace: str = "defacto",
        log_level: str = "INFO",
        log_format: str = "console",
    ) -> Self:
        """Create a Defacto instance from configuration dicts or objects.

        No filesystem dependency. Primary entry point for production and
        programmatic use.

        Args:
            config: Definition set — either a Definitions object or a dict
                with 'entities', 'sources', and optionally 'schemas' keys.
            database: Database URL. None = SQLite in temp dir, or a file path.
            version: Explicit version name. None = content hash (deterministic).
                Use explicit names in production for readable version history.
            batch_size: Events accumulated before processing.
            workers: Parallel Rust computation threads.
            kafka: Kafka config for effects streaming.
            cold_ledger: Cold storage URL for ledger archival.
            dead_letter: Dead letter config for failed events.
            namespace: Postgres schema prefix. Default 'defacto'.
            log_level: Logging level. 'DEBUG', 'INFO', 'WARNING', or 'ERROR'.
            log_format: Log output format. 'console' or 'json'.

        Returns:
            A configured Defacto instance.

        Raises:
            DefinitionError: If definitions are invalid.
            ConfigError: If configuration is invalid.
        """
        _setup_logging(log_level, log_format)

        if isinstance(config, Definitions):
            defs = config
        else:
            defs = Definitions.from_dict(config)
        validation = defs.validate()
        if not validation.valid:
            raise DefinitionError(
                f"Invalid definitions: {'; '.join(validation.errors)}",
                details={"errors": validation.errors},
            )
        for w in validation.warnings:
            logger.warning("definition warning", extra={
                "operation": "validate", "warning": w,
            })
        defs_dict = defs.to_dict()

        db_path = database or os.path.join(tempfile.mkdtemp(), "defacto.db")
        v = version or _content_hash(defs_dict)

        return cls._build_instance(
            defs_dict, db_path, v,
            batch_size=batch_size, workers=workers, kafka=kafka,
            dead_letter=dead_letter, cold_ledger=cold_ledger,
            shard_id=shard_id, total_shards=total_shards,
            graph=graph, namespace=namespace,
        )

    @classmethod
    def consumer(
        cls,
        *,
        kafka: dict[str, Any],
        database: str,
        store: str,
        batch_size: int = 1000,
        batch_timeout_ms: int = 5000,
        dead_letter: dict[str, Any] | None = None,
        log_level: str = "INFO",
        log_format: str = "console",
    ) -> DefactoConsumer:
        """Create a state history consumer.

        Reads entity snapshots from Kafka and writes SCD Type 2 tables
        to an analytical store. Reads definitions from the shared database.

        Args:
            kafka: Kafka consumer config with 'bootstrap.servers' and 'topic'.
                Optional: 'group.id' (default 'defacto-consumer').
            database: Database URL for reading definitions (the shared
                definition_versions table). Usually the same Postgres
                instance used by the Defacto producer.
            store: Analytical store URL for state history writes.
                Same URL schemes as database ('postgresql://...', etc.).
            batch_size: Max snapshots accumulated before writing a batch.
            batch_timeout_ms: Max wait before flushing a partial batch.
            log_level: Logging level. 'DEBUG', 'INFO', 'WARNING', or 'ERROR'.
            log_format: Log output format. 'console' or 'json'.

        Returns:
            A configured DefactoConsumer ready to run.
        """
        _setup_logging(log_level, log_format)

        try:
            from confluent_kafka import Consumer
        except ModuleNotFoundError:
            raise ModuleNotFoundError(
                "DefactoConsumer requires confluent-kafka. "
                "Install it with: pip install defacto[kafka]"
            ) from None

        # Definitions from the shared database
        scheme = database.split("://")[0] if "://" in database else "sqlite"
        defs_backend = _BACKEND_REGISTRY.get(scheme)
        if defs_backend is None:
            raise ValueError(
                f"Unsupported database scheme '{scheme}'. "
                f"Supported: {', '.join(_BACKEND_REGISTRY)}"
            )
        definitions_db = defs_backend.definitions(database)
        active_version = definitions_db.active_version()
        defs_dict = definitions_db.get(active_version)

        # State history backend for the analytical store
        store_scheme = store.split("://")[0] if "://" in store else "sqlite"
        store_backends = _BACKEND_REGISTRY.get(store_scheme)
        if store_backends is None:
            raise ValueError(
                f"Unsupported store scheme '{store_scheme}'. "
                f"Supported: {', '.join(_BACKEND_REGISTRY)}"
            )
        state_history = store_backends.state_history(store, active_version)
        state_history.ensure_tables(defs_dict.get("entities", {}))

        # Kafka consumer — manual offset commit for exactly-once writes
        config = {**kafka}
        topic = config.pop("topic")
        config.setdefault("group.id", "defacto-consumer")
        config.setdefault("auto.offset.reset", "earliest")
        config["enable.auto.commit"] = False
        kafka_consumer = Consumer(config)

        # Dead letter sink for consumption failures
        dead_letter_sink = DeadLetterSink.create(dead_letter)

        return DefactoConsumer(
            consumer=kafka_consumer,
            topic=topic,
            definitions_db=definitions_db,
            state_history=state_history,
            dead_letter=dead_letter_sink,
            batch_size=batch_size,
            batch_timeout_ms=batch_timeout_ms,
        )

    # -- Core operations --

    def ingest(
        self,
        source: str,
        events: list[dict[str, Any]],
        *,
        process: str | bool | None = None,
    ) -> IngestResult:
        """Normalize raw input and append to the ledger.

        Optionally process events immediately (stream-first pattern).
        Good events proceed; failed events are captured in result.failures.

        Args:
            source: Registered source name — determines normalization handlers.
            events: Raw input records from the source.
            process: If True, process using the active version. If a string,
                process using that specific version. If None, just append
                to ledger (batch pattern — call build() later).

        Returns:
            IngestResult with ingested/failed/duplicate counts and any failures.

        Raises:
            IngestError: If ALL events fail normalization (handler bug).
            StorageError: If the database is unreachable.
        """
        if process is None:
            return self._ingest_append_only(source, events)

        # Stream-first: buffer events, flush at batch_size
        version = process if isinstance(process, str) else self._active_version
        self._pending.extend((source, e) for e in events)
        return self._flush_pending(version, buffered=len(events))

    def build(
        self,
        version: str | None = None,
        *,
        full: bool = False,
        from_raw: bool = False,
    ) -> BuildResult:
        """Process events from the ledger into entity state.

        Unified build flow:
        1. Flush pending buffer
        2. Detect mode (hashes + cursor + dirty + overrides)
        3. Pre-pass: if RENORMALIZE → re-normalize from raw, update ledger
        4. Setup: clear entity state (and identity if needed)
        5. Replay: stream events, resolve, interpret, publish
        6. Post-pass: if INCREMENTAL and late arrivals → partial rebuild
        7. Finalize: clear dirty, store hashes, update watermark

        Args:
            version: Definition version to build. Default: active version.
            full: Force a full rebuild (replay all events). Identity preserved.
            from_raw: Force re-normalization from raw input. The most expensive
                mode — re-normalizes, re-resolves identity, re-interprets.

        Returns:
            BuildResult with mode, event/effect counts, and timing.
        """
        version = version or self._active_version

        # Flush any pending buffered events first
        if self._pending:
            self._flush_pending_all(version)

        # Determine mode: explicit overrides first, then auto-detect
        if from_raw:
            mode = "FULL_RENORMALIZE"
        elif full:
            mode = "FULL"
        else:
            mode = self._build_manager.detect_mode(version, self._ledger.cursor())

        if mode == "SKIP":
            return BuildResult(
                mode="SKIP", events_processed=0, effects_produced=0,
                entities_created=0, entities_updated=0, merges_detected=0,
                late_arrivals=0, duration_ms=0,
            )

        # Pre-pass: renormalize ledger if source mappings changed
        if mode == "FULL_RENORMALIZE":
            self._pipeline.renormalize_ledger(version)
            self._identity.reset()

        # Setup: identity reset if identity config changed.
        # Capture old entity_ids before clearing — every old ID is stale
        # after the reset (new UUIDs generated). Tombstones produced after
        # the build so consumers can close those rows.
        old_entities: dict[str, str] = {}
        if mode in ("FULL_WITH_IDENTITY_RESET", "FULL_RENORMALIZE"):
            old_entities = self._ledger.all_entity_mappings()
        if mode == "FULL_WITH_IDENTITY_RESET":
            self._identity.reset()

        # Replay: full or incremental
        if mode in ("FULL", "FULL_RENORMALIZE", "FULL_WITH_IDENTITY_RESET"):
            result = self._pipeline.build_full(version, mode=mode)
        else:
            # INCREMENTAL — includes late arrival post-pass internally
            return self._pipeline.build_incremental(version)

        # Tombstone old entity_ids that were replaced during the reset.
        # Every old ID is stale — the build created new UUIDs for all.
        if old_entities:
            now = datetime.now(timezone.utc).isoformat()
            tombstones = [
                {
                    "entity_id": eid,
                    "entity_type": etype,
                    "timestamp": now,
                }
                for eid, etype in old_entities.items()
            ]
            self._publisher.publish([], tombstones)
            self._publisher.drain_errors()

        return result

    def tick(
        self,
        version: str | None = None,
        *,
        as_of: datetime | None = None,
    ) -> TickResult:
        """Evaluate time rules for all entities.

        Checks inactivity, expiration, and state_duration rules. Produces
        effects (typically state transitions) for entities that meet
        their time thresholds.

        Uses _tick_and_validate — the same path as post-build ticks.
        Validates each entity still exists in the shared ledger and
        cleans orphans from DashMap (entities erased or merged away
        on another shard).

        Args:
            version: Definition version. Default: active version.
            as_of: Timestamp to evaluate against. Default: current UTC time.

        Returns:
            TickResult with effects produced and entities affected.
        """
        as_of_str = as_of.isoformat() if as_of else None
        tick_snapshots = self._pipeline._tick_and_validate(as_of=as_of_str)
        if tick_snapshots:
            self._publisher.publish(tick_snapshots, [])
            # Drain delivery errors so they don't accumulate silently.
            self._publisher.drain_errors()

        result = TickResult(
            effects_produced=len(tick_snapshots),
            entities_affected=len(tick_snapshots),
            transitions=len(tick_snapshots),
        )
        if tick_snapshots:
            logger.info("tick completed", extra={
                "operation": "tick",
                "effects_produced": len(tick_snapshots),
                "entities_affected": len(tick_snapshots),
            })
        return result

    def redact(self, entity_id: str) -> int:
        """Redact sensitive fields for an entity's events.

        Replaces fields marked sensitive in the entity definition with
        '[REDACTED]' in both data and raw columns. The entity continues
        to exist with sanitized data — replays produce entities with
        redacted values. Use for GDPR right to rectification.

        Args:
            entity_id: Entity whose events should be redacted.

        Returns:
            Number of events redacted.
        """
        entity_type = self._lookup_entity_type(entity_id)
        if not entity_type:
            return 0
        sensitive_fields = self._get_sensitive_fields(entity_type)
        if not sensitive_fields:
            return 0
        return self._ledger.redact(entity_id, sensitive_fields)

    def erase(self, entity_id: str) -> EraseResult:
        """Right to erasure — permanently erase an entity.

        Deletes everything: identity mappings, events from the ledger,
        event_entities, merge log, DashMap, state history.

        Cascades through merges via the merge_log (inline store, always
        consistent). After a merge, the loser's events are already
        associated with the winner in event_entities, so
        delete_events_for(winner) catches them.

        Ordering is deliberate for partial failure safety:
        1. Identity deleted first — new events for the same hints create
           fresh entities, not the erased one.
        2. Ledger events deleted — removes source data.
        3. Merge log cleaned — removes cascade metadata.
        4. DashMap cleared — removes in-memory state.
        5. State history via publisher — removes projection.
        If erase fails partway, re-calling it finishes the cleanup.

        Args:
            entity_id: Canonical entity ID to erase.

        Returns:
            EraseResult with deletion counts.
        """
        self._flush_pending_all(self._active_version)

        # Find merge cascade via merge_log — entities merged into this
        # one need their identity mappings and state history deleted too.
        to_erase = {entity_id}
        frontier = [entity_id]
        while frontier:
            current = frontier.pop()
            merged = self._ledger.find_merges_into(current)
            for mid in merged:
                if mid not in to_erase:
                    to_erase.add(mid)
                    frontier.append(mid)

        erase_ids = list(to_erase)

        # 1. Identity first — prevents new events from resolving to
        #    the erased entity. Safe to retry (idempotent DELETE).
        #    Capture hints BEFORE deleting so we can clear the Rust cache.
        cache_hints: list[tuple[str, str]] = []
        for eid in erase_ids:
            hints = self._identity_backend.hints_for_entity(eid)
            entity_type = self._ledger.lookup_entity_type(eid) or ""
            for _hint_type, hint_values in hints.items():
                for hv in hint_values:
                    cache_hints.append((entity_type, hv))
        self._identity_backend.delete_batch(erase_ids)
        if cache_hints:
            self._core.remove_from_cache(cache_hints)

        # 2. Ledger events (includes merged entities' events since
        #    merge_event_entities moved them to the winner's ID).
        events_deleted = self._ledger.delete_events_for(entity_id)

        # 3. Merge log cleanup (batch, one query).
        self._ledger.delete_merge_log_batch(erase_ids)

        # 4. DashMap (in-memory, always succeeds).
        for eid in erase_ids:
            self._core.delete(eid)

        # 5. State history via publisher (batch).
        self._publisher.delete_entity_batch(erase_ids)

        result = EraseResult(
            entity_id=entity_id,
            entities_erased=len(erase_ids),
            events_deleted=events_deleted,
            merge_log_cleaned=len(erase_ids),
        )
        logger.info("entity erased", extra={
            "operation": "erase",
            "entity_id": entity_id,
            "entities_erased": len(erase_ids),
            "events_deleted": events_deleted,
        })
        return result

    def merge(
        self, from_entity_id: str, into_entity_id: str,
        *, reason: str = "",
    ) -> MergeResult:
        """Merge two entities — external merge directive.

        Records a merge event in the ledger (audit trail, survives replays)
        then follows the standard merge protocol: _execute_merges for
        metadata + _rebuild_merge_winners for correct combined state.

        The merge is a discovery, not an event — it means "these were
        always the same entity." The winner's state is rebuilt from
        scratch by replaying all events under its entity_id.

        Idempotent — merging already-merged entities is a no-op.

        Args:
            from_entity_id: Entity to merge away (the loser).
            into_entity_id: Entity to merge into (the winner).
            reason: Optional reason for audit trail (e.g., 'manual_review',
                'splink_confidence_0.95').

        Returns:
            MergeResult with merge details.

        Raises:
            NotFoundError: If the surviving entity doesn't exist.
        """
        from defacto._pipeline import _DEFACTO_MERGE_EVENT_TYPE

        self._flush_pending_all(self._active_version)

        # Validate the surviving entity exists in event_entities.
        entity_type = self._ledger.lookup_entity_type(into_entity_id)
        if entity_type is None:
            raise NotFoundError(
                f"Surviving entity '{into_entity_id}' not found",
                details={"entity_id": into_entity_id},
            )

        # Capture hints BEFORE the merge moves them. These let the
        # merge survive IDENTITY_RESET where entity_ids change.
        from_hints = self._identity_backend.hints_for_entity(from_entity_id)
        into_hints = self._identity_backend.hints_for_entity(into_entity_id)

        # Write merge event to ledger + event_entities (winner's shard)
        now = datetime.now(timezone.utc).isoformat()
        merge_event = {
            "event_id": f"_defacto_merge:{from_entity_id}:{into_entity_id}:{now}",
            "event_type": _DEFACTO_MERGE_EVENT_TYPE,
            "timestamp": now,
            "source": "_defacto",
            "data": {
                "from_entity_id": from_entity_id,
                "into_entity_id": into_entity_id,
                "reason": reason,
                "from_hints": {entity_type: from_hints},
                "into_hints": {entity_type: into_hints},
            },
            "raw": {
                "from_entity_id": from_entity_id,
                "into_entity_id": into_entity_id,
                "reason": reason,
            },
            "resolution_hints": {},
        }

        self._ledger.begin()
        sequences = self._ledger.append_batch([merge_event])
        if sequences:
            self._ledger.write_event_entities([
                (sequences[0], into_entity_id, entity_type),
            ])

        # Merge protocol: metadata + rebuild.
        # _execute_merges handles identity, event_entities, merge_log,
        # DashMap timing, and tombstone production.
        tombstones, rebuild_pairs = self._pipeline._execute_merges(
            [(from_entity_id, into_entity_id)], now,
        )
        self._publisher.publish([], tombstones)
        self._publisher.drain_errors()

        # Rebuild winners for correct combined state.
        rebuild_failures = self._pipeline._rebuild_merge_winners(
            self._active_version, rebuild_pairs,
        )

        # Advance cursor + commit
        if sequences:
            self._build_manager.advance_cursor(
                self._active_version, max(sequences),
            )
        self._ledger.commit()

        result = MergeResult(
            from_entity_id=from_entity_id,
            into_entity_id=into_entity_id,
            events_reassigned=len(sequences) if sequences else 0,
            entities_rebuilt=len(rebuild_pairs),
            tombstones_produced=len(tombstones),
            failures=rebuild_failures,
        )
        logger.info("entity merged", extra={
            "operation": "merge",
            "from_entity_id": from_entity_id,
            "into_entity_id": into_entity_id,
            "entities_rebuilt": len(rebuild_pairs),
            "tombstones_produced": len(tombstones),
        })
        return result

    def close(self) -> None:
        """Flush pending work and release all connections.

        Processes any pending events, flushes Kafka producer if applicable,
        and closes all backend connections. Safe to call multiple times.
        """
        if self._closed:
            return
        self._closed = True

        # Flush ALL pending events, including partial batches
        if self._pending:
            self._flush_pending_all(self._active_version)

        self._publisher.close()
        if self._dead_letter:
            self._dead_letter.close()
        if self.__query is not None:
            self.__query.close()
        self._core.close()
        self._ledger.close()
        self._identity_backend.close()
        self._state_history.close()
        self._build_manager.close()
        self._definitions_db.close()

    # -- Version management --

    def _activate_version(
        self, version: str, definitions: dict[str, Any],
    ) -> BuildResult:
        """Switch to a new definition version — compile, ensure DDL, build, swap.

        This is the single orchestration point for all version changes:
        m.definitions.activate(), rollback, etc. The sequence is ordered
        so that if core.compile() fails (invalid definitions), nothing
        is mutated — the instance stays on the previous version.

        Args:
            version: Version name to activate.
            definitions: Full definitions dict for the version.

        Returns:
            BuildResult from the auto-build (or SKIP if nothing to do).
        """
        # 1. Validate + load into Rust. If this throws, nothing is mutated.
        self._core.compile(version, definitions)

        # 2. Switch Rust interpretation pointer.
        self._core.activate(version)

        # 3. Update state history table names, clear caches, ensure DDL.
        self._state_history.set_version(version, definitions.get("entities", {}))

        # 4. DB flag for other processes (consumer, other Defacto instances).
        self._definitions_db.activate(version)

        # 5-6. Update in-memory state.
        self._active_version = version
        self._definitions_dict = definitions

        # 7-8. Invalidate lazy properties (they store version immutably).
        self.__query = None
        self.__graph_backend = None

        # 9. Detect build mode and auto-build.
        mode = self._build_manager.detect_mode(version, self._ledger.cursor())
        if mode == "SKIP":
            return BuildResult(
                mode="SKIP", events_processed=0, effects_produced=0,
                entities_created=0, entities_updated=0, merges_detected=0,
                late_arrivals=0, duration_ms=0,
            )
        return self.build(version)

    # -- Private helpers --

    def _ingest_append_only(self, source: str, events: list[dict[str, Any]]) -> IngestResult:
        """Normalize + append to ledger without processing.

        Events are written to the durable ledger but not interpreted.
        Call build() to process them into entity state.
        """
        total_ingested = 0
        total_failed = 0
        all_failures: list[EventFailure] = []

        for i in range(0, len(events), self._batch_size):
            chunk = events[i : i + self._batch_size]
            result = self._core.normalize(source, chunk)
            if result["ledger_rows"]:
                self._ledger.append_batch(result["ledger_rows"])
            total_ingested += result["count"]
            total_failed += len(result["failures"])
            all_failures.extend(
                Pipeline._make_normalize_failure(f) for f in result["failures"]
            )

        if all_failures and self._dead_letter:
            self._dead_letter.send(all_failures)

        return IngestResult(
            events_ingested=total_ingested,
            events_failed=total_failed,
            duplicates_skipped=len(events) - total_ingested - total_failed,
            failures=all_failures,
        )

    def _flush_pending(self, version: str, buffered: int = 0) -> IngestResult:
        """Process complete batches from the pending buffer.

        Only flushes batches that fill batch_size. Leftover events stay
        in the buffer for the next ingest call. Returns combined results
        for all flushed batches, reporting buffered count if nothing was
        flushed yet.
        """
        if len(self._pending) < self._batch_size:
            return IngestResult(
                events_ingested=buffered, events_failed=0, duplicates_skipped=0,
            )

        results: list[IngestResult] = []
        while len(self._pending) >= self._batch_size:
            batch = self._pending[: self._batch_size]
            self._pending = self._pending[self._batch_size :]

            # Group by source — normalize is source-specific
            by_source: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for src, evt in batch:
                by_source[src].append(evt)

            for src, src_events in by_source.items():
                result, _ = self._pipeline.process_batch(src, src_events, version)
                results.append(result)

        return _combine_ingest_results(results)

    def _flush_pending_all(self, version: str) -> None:
        """Flush ALL pending events, including partial batches.

        Called by close() and build() — processes everything in the buffer
        regardless of whether it fills a complete batch.
        """
        if not self._pending:
            return

        by_source: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for src, evt in self._pending:
            by_source[src].append(evt)
        self._pending.clear()

        for src, src_events in by_source.items():
            for i in range(0, len(src_events), self._batch_size):
                chunk = src_events[i : i + self._batch_size]
                self._pipeline.process_batch(src, chunk, version)

    def _lookup_entity_type(self, entity_id: str) -> str | None:
        """Find entity type from event_entities in the ledger."""
        return self._ledger.lookup_entity_type(entity_id)

    def _get_sensitive_fields(self, entity_type: str) -> list[str]:
        """Get property names marked as sensitive from definitions."""
        entity_def = self._definitions_dict.get("entities", {}).get(entity_type, {})
        return [
            name
            for name, prop in entity_def.get("properties", {}).items()
            if prop.get("sensitive")
        ]

    def __enter__(self) -> Self:
        """Context manager entry."""
        return self

    def __exit__(self, *args: Any) -> None:
        """Context manager exit — calls close()."""
        self.close()

    # -- Query layer --

    @property
    def connection(self) -> Any:
        """The underlying state history database connection.

        For direct SQL access beyond the structured query API.
        Returns the raw connection (sqlite3, psycopg, duckdb).
        """
        return self._state_history.connection

    @property
    def _query(self) -> DefactoQuery:
        """Lazy query coordinator — created on first query access."""
        if self.__query is None:
            self.__query = DefactoQuery(
                self._db_path, self._definitions_dict, self._active_version,
                self._namespace,
            )
        return self.__query

    def table(self, entity_type: str) -> Any:
        """Current state of all entities of a type (active version).

        Flushes any pending events before querying to ensure results
        include recently ingested data.

        Args:
            entity_type: Entity type name (e.g., 'customer').

        Returns:
            DefactoTable wrapping an Ibis expression (execute() to materialize).
        """
        self._flush_pending_all(self._active_version)
        return self._query.table(entity_type)

    def tables(self, *entity_types: Any, relationships: bool = True) -> TableCollection:
        """Select multiple entity types as a collection (active version).

        Accepts any number of entity type names, a list, or no arguments
        (all types). Includes relationship tables by default.

        Args:
            *entity_types: Entity type names. If empty, selects all types.
            relationships: Include relationship tables. Default True.

        Returns:
            TableCollection for multi-table operations and exports.
        """
        self._flush_pending_all(self._active_version)
        return self._query.tables(*entity_types, relationships=relationships)

    def history(self, entity_type: str) -> Any:
        """Full SCD Type 2 history (active version).

        Returns all versions including closed and merged rows.
        Filter by {entity_type}_id for a specific entity's history.

        Args:
            entity_type: Entity type name (e.g., 'customer').

        Returns:
            DefactoTable wrapping an Ibis expression with SCD columns.
        """
        self._flush_pending_all(self._active_version)
        return self._query.history(entity_type)

    def query(self, sql: str) -> DefactoTable:
        """Execute raw SQL against the state history store.

        Validates the SQL against entity definitions before execution.

        Args:
            sql: Raw SQL query string.

        Returns:
            DefactoTable wrapping the result.
        """
        self._flush_pending_all(self._active_version)
        return self._query.query(sql)

    @property
    def graph(self) -> Any:
        """Graph analytics interface.

        Provides graph-native operations (communities, centrality,
        similarity, connected_components) plus direct access to the
        underlying graph via ``connection`` and ``query()``.

        The backend is determined by the ``graph=`` parameter:
        - ``None`` (default): auto-materializes NetworkX from current
          entity state on first access. Requires ``networkx`` installed.
        - ``"bolt://..."`` URL: connects to a Neo4j instance. Data must
          be exported via ``m.tables().to_neo4j()`` first.

        Core operations (traverse, path, neighbors, related) are
        available as top-level methods on Defacto regardless of backend.
        """
        from defacto.query._graph import CteGraphBackend
        backend = self._graph_backend
        if isinstance(backend, CteGraphBackend):
            backend = self._upgrade_to_networkx()
        return backend

    def version(self, v: str) -> DefactoQuery:
        """Create a version-scoped query interface.

        All queries through the returned object target a specific version's
        state history tables.

        Args:
            v: Definition version name.

        Returns:
            DefactoQuery scoped to version v.
        """
        self._flush_pending_all(self._active_version)
        return self._query.version(v)

    # -- Graph queries (active version) --

    @property
    def _graph_backend(self) -> Any:
        """Lazy graph backend — one backend for all graph operations.

        Starts as CTE (no extra dependencies). Upgrades to NetworkX or
        Neo4j on first analytics access (m.graph). Once upgraded, stays
        upgraded — all graph operations (core + analytics) use it.

        - No graph= URL: CTE initially, auto-upgrades to NetworkX
        - graph= bolt://... URL: Neo4j from the start
        """
        if self.__graph_backend is None:
            if self._graph_url:
                # Neo4j — configured at init, handles everything
                from defacto.query._graph import Neo4jGraphBackend
                from urllib.parse import urlparse
                parsed = urlparse(self._graph_url)
                user = parsed.username or "neo4j"
                password = parsed.password or ""
                clean_url = f"bolt://{parsed.hostname}:{parsed.port or 7687}"
                self.__graph_backend = Neo4jGraphBackend(
                    clean_url, auth=(user, password),
                )
            else:
                from defacto.query import CteGraphBackend
                self.__graph_backend = CteGraphBackend(
                    connection=self._state_history.connection,
                    version=self._active_version,
                    backend=detect_backend(self._db_path),
                    definitions=self._definitions_dict,
                    namespace=self._namespace,
                )
        return self.__graph_backend

    def _upgrade_to_networkx(self) -> Any:
        """Upgrade from CTE to NetworkX for analytics support.

        Auto-materializes a NetworkX graph from current entity state.
        Replaces the CTE backend — all subsequent graph operations
        (core + analytics) use NetworkX.
        """
        try:
            from defacto.query._graph import NetworkXGraphBackend
        except ImportError:
            raise ImportError(
                "Graph analytics require networkx. "
                "Install with: pip install 'defacto[networkx]'"
            )
        self._flush_pending_all(self._active_version)
        G = self.tables().to_networkx()
        self.__graph_backend = NetworkXGraphBackend(G)
        return self.__graph_backend

    def traverse(
        self, entity_id: str, relationship_type: str, *, depth: int = 1
    ) -> DefactoTable:
        """Follow relationship chains to a given depth (active version).

        Results are returned as a DefactoTable so they're chainable
        with filter/select/etc.

        Args:
            entity_id: Starting entity ID.
            relationship_type: Relationship type to follow.
            depth: How many hops to traverse.

        Returns:
            DefactoTable with columns: entity_id, depth.
        """
        self._flush_pending_all(self._active_version)
        df = self._graph_backend.traverse(
            entity_id, relationship_type, depth=depth,
        )
        return self._graph_backend._to_defacto_table(df, self._query)

    def path(self, entity_id_a: str, entity_id_b: str) -> list[str]:
        """Find shortest path between two entities (active version).

        Args:
            entity_id_a: Starting entity.
            entity_id_b: Target entity.

        Returns:
            Ordered list of entity IDs, or empty list if no path exists.
        """
        self._flush_pending_all(self._active_version)
        return self._graph_backend.path(entity_id_a, entity_id_b)

    def neighbors(self, entity_id: str) -> DefactoTable:
        """Get all directly connected entities (active version).

        Args:
            entity_id: Entity to find neighbors for.

        Returns:
            DefactoTable with columns: entity_id, relationship_type, direction.
        """
        self._flush_pending_all(self._active_version)
        df = self._graph_backend.neighbors(entity_id)
        return self._graph_backend._to_defacto_table(df, self._query)

    def related(self, entity_id: str, relationship_type: str) -> DefactoTable:
        """Get entities related through a relationship (active version).

        Returns target entities with their properties and any
        relationship edge properties.

        Args:
            entity_id: Source entity ID.
            relationship_type: Relationship type name.

        Returns:
            DefactoTable with target entity columns + relationship properties.
        """
        self._flush_pending_all(self._active_version)
        df = self._graph_backend.related(entity_id, relationship_type)
        return self._graph_backend._to_defacto_table(df, self._query)

    # -- Debugging & validation --

    def validate(self, version: str | None = None) -> ValidationResult:
        """Deep validation of definitions — errors + warnings.

        Errors were already caught at registration time. This method
        returns additional warnings: unreachable states, unused handlers,
        write-but-never-read properties, etc.

        Args:
            version: Definition version. Default: active version.

        Returns:
            ValidationResult with errors and warnings.
        """
        raise NotImplementedError

    def timeline(self, entity_id: str) -> Timeline:
        """Full timeline for a specific entity.

        Combines ledger events (event_id, event_type, timestamp) with
        SCD state history (state, properties) to show every state change
        in chronological order with human-readable effect descriptions.

        Args:
            entity_id: Canonical entity ID.

        Returns:
            Timeline with chronologically ordered entries.
        """
        from defacto.results import TimelineEntry

        self._flush_pending_all(self._active_version)

        entity_type = self._lookup_entity_type(entity_id)
        if not entity_type:
            return Timeline(entity_id=entity_id, entity_type="", entries=[])

        # Get event info from the ledger (event_id, event_type, timestamp)
        events = self._ledger.events_for(entity_id)
        event_by_ts: dict[str, dict] = {}
        for e in events:
            event_by_ts[e["timestamp"]] = e

        # Get SCD history ordered by valid_from
        id_col = f"{entity_type}_id"
        state_col = f"{entity_type}_state"
        hist_table = self.history(entity_type)
        hist = hist_table.filter(
            getattr(hist_table._expr, id_col) == entity_id
        ).order_by("valid_from").execute()

        # Build entries by diffing consecutive SCD rows
        entries: list[TimelineEntry] = []
        prev_state: str | None = None
        prev_props: dict = {}

        prop_names = list(
            self._definitions_dict.get("entities", {})
            .get(entity_type, {}).get("properties", {}).keys()
        )

        for _, row in hist.iterrows():
            ts_raw = row["valid_from"]
            ts = ts_raw if isinstance(ts_raw, str) else str(ts_raw)
            state_after = row[state_col]

            # Match to ledger event by timestamp
            event = event_by_ts.get(ts, {})

            # Compute effects by diffing properties
            effects: list[str] = []
            if prev_state is None:
                effects.append("Entity created")
            if prev_state is not None and state_after != prev_state:
                effects.append(f"State: {prev_state} → {state_after}")
            for prop in prop_names:
                old_val = prev_props.get(prop)
                new_val = row.get(prop)
                if new_val != old_val and new_val is not None:
                    if old_val is not None:
                        effects.append(f"{prop}: {old_val} → {new_val}")
                    else:
                        effects.append(f"{prop}: {new_val}")

            entries.append(TimelineEntry(
                timestamp=datetime.fromisoformat(ts) if isinstance(ts, str) else ts,
                event_type=event.get("event_type", ""),
                event_id=event.get("event_id", ""),
                effects=effects,
                state_before=prev_state,
                state_after=state_after,
            ))

            prev_state = state_after
            prev_props = {p: row.get(p) for p in prop_names}

        return Timeline(
            entity_id=entity_id,
            entity_type=entity_type,
            entries=entries,
        )

    def assert_entity(
        self,
        entity_id: str,
        *,
        state: str | None = None,
        **properties: Any,
    ) -> None:
        """Assert an entity's current state and properties.

        Convenience for testing scripts and CI pipelines. Queries the
        current state and raises AssertionError with a clear message
        if the entity doesn't exist, is in the wrong state, or has
        unexpected property values.

        Args:
            entity_id: Canonical entity ID to check.
            state: Expected state (e.g., 'active'). None skips state check.
            **properties: Expected property values as keyword arguments
                (e.g., plan='pro', mrr=99.0).

        Raises:
            AssertionError: If any expectation doesn't match.

        Example:
            m.assert_entity("cust_001", state="active", plan="pro", mrr=99.0)
        """
        self._flush_pending_all(self._active_version)

        entity_type = self._lookup_entity_type(entity_id)
        if not entity_type:
            raise AssertionError(f"{entity_id} not found in any entity type")

        id_col = f"{entity_type}_id"
        state_col = f"{entity_type}_state"
        tag = f"{entity_type} {entity_id}"

        df = self.table(entity_type).execute()
        if df.empty:
            raise AssertionError(f"{tag} has no rows in current state")

        matches = df[df[id_col] == entity_id]
        if matches.empty:
            raise AssertionError(
                f"{tag} not in current state (may have been erased or merged)"
            )

        row = matches.iloc[0]

        if state is not None:
            actual_state = row[state_col]
            if actual_state != state:
                raise AssertionError(
                    f"{tag} expected state '{state}', got '{actual_state}'"
                )

        for prop_name, expected in properties.items():
            if prop_name not in row.index:
                raise AssertionError(
                    f"{tag} property '{prop_name}' not found in columns "
                    f"{sorted(row.index.tolist())}"
                )
            actual = row[prop_name]
            if actual != expected:
                raise AssertionError(
                    f"{tag} {prop_name}: expected {expected!r}, got {actual!r}"
                )

    def build_status(self, version: str | None = None) -> BuildStatus:
        """Current build state for a definition version.

        Args:
            version: Definition version. Default: active version.

        Returns:
            BuildStatus with cursor, hashes, dirty flag, etc.
        """
        version = version or self._active_version
        status = self._build_manager.get_status(version)
        ledger_cursor = self._ledger.cursor()

        return BuildStatus(
            cursor=status.cursor,
            total_events=ledger_cursor,
            pending_events=ledger_cursor - status.cursor,
            last_build_time=status.last_build_time,
            last_build_mode=status.last_build_mode,
            definition_hash=status.definition_hash,
            source_hash=status.source_hash,
            identity_hash=status.identity_hash,
            dirty=status.dirty,
        )

    def diff(self, v1: str, v2: str) -> dict[str, Any]:
        """Compare entity state between two definition versions.

        Args:
            v1: First version name.
            v2: Second version name.

        Returns:
            Dict describing differences in entity state between versions.
        """
        raise NotImplementedError

    # -- Sub-managers (properties) --

    @property
    def definitions(self) -> DefinitionsManager:
        """Access definition management operations.

        Returns:
            DefinitionsManager for version/definition operations.
        """
        if self.__definitions_manager is None:
            self.__definitions_manager = DefinitionsManager(self)
        return self.__definitions_manager

    @property
    def ledger(self) -> LedgerInspection:
        """Access ledger inspection operations.

        Returns:
            LedgerInspection for querying the event ledger.
        """
        return LedgerInspection(self)

    @property
    def identity(self) -> IdentityInspection:
        """Access identity inspection operations.

        Returns:
            IdentityInspection for querying identity mappings.
        """
        return IdentityInspection(self)


def _combine_ingest_results(results: list[IngestResult]) -> IngestResult:
    """Merge multiple IngestResults from batch processing into one."""
    if not results:
        return IngestResult(events_ingested=0, events_failed=0, duplicates_skipped=0)

    # Sum timing across batches
    combined_timing: dict[str, float] = {}
    for r in results:
        for key, value in r.timing.items():
            combined_timing[key] = round(combined_timing.get(key, 0) + value, 2)

    return IngestResult(
        events_ingested=sum(r.events_ingested for r in results),
        events_failed=sum(r.events_failed for r in results),
        duplicates_skipped=sum(r.duplicates_skipped for r in results),
        failures=[f for r in results for f in r.failures],
        timing=combined_timing,
    )


# ---------------------------------------------------------------------------
# DefinitionsManager — m.definitions.*
# ---------------------------------------------------------------------------


class DefinitionsManager:
    """Definition version and management operations.

    Accessed via m.definitions. Provides version listing, activation,
    registration, and the draft API for incremental definition changes.

    Stateless — delegates everything to the parent Defacto instance's
    internal components (DefinitionsStore, DefactoCore, BuildManager).
    A single DefinitionsManager works across all version switches.
    """

    def __init__(self, defacto: Defacto) -> None:
        self._m = defacto

    def versions(self) -> list[dict[str, Any]]:
        """List all registered definition versions.

        Returns:
            List of dicts with 'version', 'active', 'created_at' keys.
        """
        return self._m._definitions_db.versions()

    def active(self) -> str:
        """Get the currently active definition version name.

        Returns:
            Active version name (e.g., 'a3f8c2d1').

        Raises:
            ConfigError: If no versions are registered.
        """
        return self._m._definitions_db.active_version()

    def activate(self, version: str) -> BuildResult:
        """Switch to a definition version — compile, build, swap.

        Loads definitions from the database, compiles in Rust, ensures
        DDL for the new version's tables, detects and runs the required
        build, then switches all internal state. Auto-build means the
        version is fully operational when this returns.

        Args:
            version: Version name to activate. Must be registered.

        Returns:
            BuildResult from the auto-build (or SKIP if nothing to do).

        Raises:
            NotFoundError: If version doesn't exist.
            DefinitionError: If definitions fail Rust compilation.
        """
        defs = self._m._definitions_db.get(version)
        return self._m._activate_version(version, defs)

    def get(self, version: str | None = None) -> dict[str, Any]:
        """Get the definitions dict for a version.

        Args:
            version: Version name. Default: active version.

        Returns:
            Definitions dict with 'entities', 'sources', 'schemas' keys.
        """
        v = version or self._m._active_version
        return self._m._definitions_db.get(v)

    def register(self, version: str, definitions: dict[str, Any] | Definitions) -> RegisterResult:
        """Register a complete definition set as a new version.

        Validates holistically via Rust compilation before storing.
        Does NOT activate — caller must explicitly call activate().

        Args:
            version: Version name (e.g., 'v2').
            definitions: Complete definition set — Definitions object or dict.

        Returns:
            RegisterResult with changes vs previous version, predicted build mode.

        Raises:
            DefinitionError: If validation fails. Version is NOT registered.
        """
        if isinstance(definitions, Definitions):
            defs_dict = definitions.to_dict()
        else:
            defs_dict = definitions

        # Validate via Rust compilation — if this throws, nothing is stored.
        self._m._core.compile(version, defs_dict)
        hashes = self._m._core.definition_hashes(version)

        # Store immutably in the database.
        self._m._definitions_db.register(version, defs_dict, hashes)

        # Predict build mode by comparing against active version's hashes.
        changes: dict[str, Any] = {}
        predicted_mode = "FULL"
        try:
            active_hashes = self._m._definitions_db.hashes(
                self._m._active_version,
            )
            if hashes["source_hash"] != active_hashes["source_hash"]:
                predicted_mode = "FULL_RENORMALIZE"
            elif hashes["identity_hash"] != active_hashes["identity_hash"]:
                predicted_mode = "FULL_WITH_IDENTITY_RESET"
            elif hashes["definition_hash"] != active_hashes["definition_hash"]:
                predicted_mode = "FULL"
            else:
                predicted_mode = "INCREMENTAL"
        except (ConfigError, NotFoundError):
            # No active version yet (first registration).
            predicted_mode = "FULL"

        return RegisterResult(
            version=version,
            changes=changes,
            build_mode=predicted_mode,
        )

    def draft(self, version: str, *, based_on: str | None = None) -> DefinitionsDraft:
        """Create a mutable draft for building a new version incrementally.

        Each modification is validated per-operation via Rust compilation.
        Use diff(), impact(), and validate() to review before committing
        with register().

        Args:
            version: Version name for the new version.
            based_on: Version to copy from. Default: active version.

        Returns:
            DefinitionsDraft for incremental modifications.
        """
        base = based_on or self._m._active_version
        base_defs = self._m._definitions_db.get(base)
        return DefinitionsDraft(version, base, copy.deepcopy(base_defs), self._m)


# ---------------------------------------------------------------------------
# DefinitionsDraft — m.definitions.draft("v2", based_on="v1")
# ---------------------------------------------------------------------------


class DefinitionsDraft:
    """A mutable draft for building a new definition version incrementally.

    Each modification is validated immediately via Rust compilation —
    invalid operations raise DefinitionError and are reverted. Use diff()
    and impact() to review changes before committing with register().

    Example:
        draft = m.definitions.draft("v2", based_on="v1")
        draft.add_property("customer", "churn_risk", type="number", default=0)
        print(draft.diff())       # what changed
        print(draft.impact())     # predicted build mode
        draft.register()          # validate + store
    """

    def __init__(
        self,
        version: str,
        base_version: str,
        definitions: dict[str, Any],
        defacto: Defacto,
    ) -> None:
        """Initialize with a deep copy of the base version's definitions.

        Args:
            version: Version name for the new version.
            base_version: Version this draft is based on.
            definitions: Deep copy of base definitions (mutable).
            defacto: Parent Defacto instance for validation and registration.
        """
        self._version = version
        self._base_version = base_version
        self._definitions = definitions
        self._m = defacto

    def _entity_defs(self, entity_type: str) -> dict[str, Any]:
        """Get entity definition dict, raising if entity type doesn't exist."""
        entities = self._definitions.get("entities", {})
        if entity_type not in entities:
            raise DefinitionError(
                f"Entity type '{entity_type}' not found",
                details={"entity_type": entity_type},
            )
        return entities[entity_type]

    def _validate(self) -> None:
        """Validate current draft state via Rust compilation.

        Raises DefinitionError with a clear message if invalid.
        """
        try:
            self._m._core.compile(self._version, self._definitions)
        except Exception as e:
            raise DefinitionError(
                f"Draft validation failed: {e}",
                details={"version": self._version},
            ) from e

    def add_property(
        self,
        entity_type: str,
        name: str,
        *,
        type: str,
        default: Any = None,
        sensitive: str | None = None,
        treatment: str | None = None,
        allowed: list[Any] | None = None,
        min: float | None = None,
        max: float | None = None,
        compute: str | None = None,
    ) -> None:
        """Add a property to an entity type.

        Validates immediately via Rust compilation. Reverts on failure.

        Raises:
            DefinitionError: If property name is reserved or already exists.
        """
        entity_def = self._entity_defs(entity_type)
        props = entity_def.setdefault("properties", {})
        if name in props:
            raise DefinitionError(
                f"Property '{name}' already exists on '{entity_type}'",
                details={"entity_type": entity_type, "property": name},
            )

        prop_def: dict[str, Any] = {"type": type}
        if default is not None:
            prop_def["default"] = default
        if sensitive is not None:
            prop_def["sensitive"] = sensitive
        if treatment is not None:
            prop_def["treatment"] = treatment
        if allowed is not None:
            prop_def["allowed"] = allowed
        if min is not None:
            prop_def["min"] = min
        if max is not None:
            prop_def["max"] = max
        if compute is not None:
            prop_def["compute"] = compute

        props[name] = prop_def
        try:
            self._validate()
        except DefinitionError:
            del props[name]
            raise

    def add_state(
        self,
        entity_type: str,
        name: str,
        *,
        on: dict[str, Any] | None = None,
        after: list[Any] | None = None,
    ) -> None:
        """Add a state to an entity's state machine.

        Raises:
            DefinitionError: If state name already exists.
        """
        entity_def = self._entity_defs(entity_type)
        states = entity_def.setdefault("states", {})
        if name in states:
            raise DefinitionError(
                f"State '{name}' already exists on '{entity_type}'",
                details={"entity_type": entity_type, "state": name},
            )

        state_def: dict[str, Any] = {}
        if on is not None:
            state_def["when"] = on
        if after is not None:
            state_def["after"] = after

        states[name] = state_def
        try:
            self._validate()
        except DefinitionError:
            del states[name]
            raise

    def add_transition(
        self, entity_type: str, state: str, event: str, *, to: str,
    ) -> None:
        """Add an event handler that transitions to another state.

        Raises:
            DefinitionError: If source or target state doesn't exist,
                or if a handler already exists for this event.
        """
        entity_def = self._entity_defs(entity_type)
        states = entity_def.get("states", {})
        if state not in states:
            raise DefinitionError(
                f"State '{state}' not found on '{entity_type}'",
                details={"entity_type": entity_type, "state": state},
            )
        if to not in states:
            raise DefinitionError(
                f"Target state '{to}' not found on '{entity_type}'",
                details={"entity_type": entity_type, "state": to},
            )

        when = states[state].setdefault("when", {})
        if event in when:
            raise DefinitionError(
                f"Handler for '{event}' already exists on state '{state}'",
                details={"entity_type": entity_type, "state": state, "event": event},
            )

        when[event] = {
            "effects": [{"transition": {"to": to}}],
        }
        try:
            self._validate()
        except DefinitionError:
            del when[event]
            raise

    def add_handler(
        self,
        entity_type: str,
        state: str,
        event: str,
        *,
        effects: list[Any],
        guard: str | None = None,
    ) -> None:
        """Add a full event handler to a state.

        Raises:
            DefinitionError: If state doesn't exist or handler already defined.
        """
        entity_def = self._entity_defs(entity_type)
        states = entity_def.get("states", {})
        if state not in states:
            raise DefinitionError(
                f"State '{state}' not found on '{entity_type}'",
                details={"entity_type": entity_type, "state": state},
            )

        when = states[state].setdefault("when", {})
        if event in when:
            raise DefinitionError(
                f"Handler for '{event}' already exists on state '{state}'",
                details={"entity_type": entity_type, "state": state, "event": event},
            )

        handler: dict[str, Any] = {"effects": effects}
        if guard is not None:
            handler["guard"] = guard

        when[event] = handler
        try:
            self._validate()
        except DefinitionError:
            del when[event]
            raise

    def update_identity(self, entity_type: str, **fields: Any) -> None:
        """Update identity configuration for an entity type.

        Raises:
            DefinitionError: If entity type doesn't exist.
        """
        entity_def = self._entity_defs(entity_type)
        prev = entity_def.get("identity")
        entity_def["identity"] = fields
        try:
            self._validate()
        except DefinitionError:
            if prev is None:
                entity_def.pop("identity", None)
            else:
                entity_def["identity"] = prev
            raise

    def remove_property(self, entity_type: str, name: str) -> None:
        """Remove a property from an entity type.

        Raises:
            DefinitionError: If property doesn't exist.
        """
        entity_def = self._entity_defs(entity_type)
        props = entity_def.get("properties", {})
        if name not in props:
            raise DefinitionError(
                f"Property '{name}' not found on '{entity_type}'",
                details={"entity_type": entity_type, "property": name},
            )

        prev = props.pop(name)
        try:
            self._validate()
        except DefinitionError:
            props[name] = prev
            raise

    def remove_state(self, entity_type: str, name: str) -> None:
        """Remove a state from an entity's state machine.

        Raises:
            DefinitionError: If state doesn't exist.
        """
        entity_def = self._entity_defs(entity_type)
        states = entity_def.get("states", {})
        if name not in states:
            raise DefinitionError(
                f"State '{name}' not found on '{entity_type}'",
                details={"entity_type": entity_type, "state": name},
            )

        prev = states.pop(name)
        try:
            self._validate()
        except DefinitionError:
            states[name] = prev
            raise

    def diff(self) -> dict[str, Any]:
        """Show what changed compared to the base version.

        Returns:
            Dict with 'added', 'removed', 'modified' keys describing
            changes to entities, properties, and states.
        """
        base_defs = self._m._definitions_db.get(self._base_version)
        base_entities = base_defs.get("entities", {})
        draft_entities = self._definitions.get("entities", {})

        added = {k: v for k, v in draft_entities.items() if k not in base_entities}
        removed = {k: v for k, v in base_entities.items() if k not in draft_entities}
        modified: dict[str, Any] = {}

        for entity_type in set(base_entities) & set(draft_entities):
            base_def = base_entities[entity_type]
            draft_def = draft_entities[entity_type]
            if base_def != draft_def:
                changes: dict[str, Any] = {}
                # Property changes
                base_props = set(base_def.get("properties", {}))
                draft_props = set(draft_def.get("properties", {}))
                if draft_props - base_props:
                    changes["added_properties"] = list(draft_props - base_props)
                if base_props - draft_props:
                    changes["removed_properties"] = list(base_props - draft_props)
                # State changes
                base_states = set(base_def.get("states", {}))
                draft_states = set(draft_def.get("states", {}))
                if draft_states - base_states:
                    changes["added_states"] = list(draft_states - base_states)
                if base_states - draft_states:
                    changes["removed_states"] = list(base_states - draft_states)
                # Identity changes
                if base_def.get("identity") != draft_def.get("identity"):
                    changes["identity_changed"] = True
                if changes:
                    modified[entity_type] = changes

        return {"added": added, "removed": removed, "modified": modified}

    def impact(self) -> dict[str, Any]:
        """Predict the build impact of these changes.

        Compiles the draft, computes hashes, and compares against the
        active version's stored hashes to predict the build mode needed.

        Returns:
            Dict with 'build_mode' and 'reason'.
        """
        self._m._core.compile(self._version, self._definitions)
        draft_hashes = self._m._core.definition_hashes(self._version)

        try:
            active_hashes = self._m._definitions_db.hashes(
                self._m._active_version,
            )
        except (ConfigError, NotFoundError):
            return {"build_mode": "FULL", "reason": "no active version to compare"}

        if draft_hashes["source_hash"] != active_hashes["source_hash"]:
            return {"build_mode": "FULL_RENORMALIZE", "reason": "source mappings changed"}
        if draft_hashes["identity_hash"] != active_hashes["identity_hash"]:
            return {
                "build_mode": "FULL_WITH_IDENTITY_RESET",
                "reason": "identity config changed",
            }
        if draft_hashes["definition_hash"] != active_hashes["definition_hash"]:
            return {"build_mode": "FULL", "reason": "entity definitions changed"}
        return {"build_mode": "INCREMENTAL", "reason": "no definition changes"}

    def validate(self) -> ValidationResult:
        """Run holistic validation on the draft.

        Returns:
            ValidationResult with errors (blocking) and warnings (informational).
        """
        try:
            self._m._core.compile(self._version, self._definitions)
            return ValidationResult(valid=True)
        except Exception as e:
            return ValidationResult(valid=False, errors=[str(e)])

    def register(self) -> RegisterResult:
        """Validate and register the draft as a new version.

        Does NOT activate — call m.definitions.activate() after reviewing.

        Returns:
            RegisterResult with changes and predicted build mode.

        Raises:
            DefinitionError: If holistic validation fails.
        """
        validation = self.validate()
        if not validation.valid:
            raise DefinitionError(
                f"Draft validation failed: {'; '.join(validation.errors)}",
                details={"errors": validation.errors},
            )
        return self._m.definitions.register(self._version, self._definitions)


# ---------------------------------------------------------------------------
# LedgerInspection — m.ledger.*
# ---------------------------------------------------------------------------


class LedgerInspection:
    """Ledger inspection operations.

    Accessed via m.ledger. Provides read-only access to the event ledger
    for debugging and operational queries. Delegates to the ledger backend.
    """

    def __init__(self, defacto: Defacto) -> None:
        self._m = defacto

    def count(self, *, source: str | None = None) -> int:
        """Count events in the ledger.

        Args:
            source: Filter by source name. None = count all events.

        Returns:
            Number of events.
        """
        return self._m._ledger.count(source)

    def events_for(self, entity_id: str) -> list[dict[str, Any]]:
        """Get events that affected a specific entity.

        Uses the event_entities mapping for efficient lookup — no full
        ledger scan. Returns lightweight summaries, not full event data.

        Args:
            entity_id: Entity ID to look up events for.

        Returns:
            List of event summaries with sequence, event_id, event_type,
            and timestamp.
        """
        return self._m._ledger.events_for(entity_id)

    def export(self, *, since_sequence: int, format: str, path: str) -> None:
        """Export ledger events to cold storage.

        Exports events + event_entities together for archival. Requires
        the tiered ledger infrastructure (Delta Lake on S3).

        Args:
            since_sequence: Export events after this sequence number.
            format: Output format ('parquet').
            path: Output path (local directory or S3 URL).
        """
        raise NotImplementedError("Ledger export requires tiered ledger (Phase 6)")


# ---------------------------------------------------------------------------
# IdentityInspection — m.identity.*
# ---------------------------------------------------------------------------


class IdentityInspection:
    """Identity inspection operations.

    Accessed via m.identity. Provides read-only access to identity mappings
    for debugging. Delegates to the identity backend.
    """

    def __init__(self, defacto: Defacto) -> None:
        self._m = defacto

    def lookup(self, hint_value: str) -> str | None:
        """Look up which entity a hint value maps to.

        Searches across all entity types. Returns the first match.

        Args:
            hint_value: The hint value (e.g., 'alice@gmail.com').

        Returns:
            Canonical entity ID, or None if the hint is unknown.
        """
        return self._m._identity_backend.lookup_any(hint_value)

    def hints(self, entity_id: str) -> dict[str, str]:
        """Get all hints linked to an entity.

        Args:
            entity_id: Entity to look up hints for.

        Returns:
            Dict of hint_type → hint_value for all linked hints.
        """
        return self._m._identity_backend.hints_for_entity(entity_id)

    def merges(self, entity_id: str) -> list[dict[str, Any]]:
        """Get merge history for an entity.

        Requires a merge log table which is not yet implemented.
        Merges currently update identity rows in place without
        recording a history trail.

        Args:
            entity_id: Entity to look up merges for.

        Returns:
            List of merge events (merged_entity, timestamp, trigger_event).
        """
        raise NotImplementedError("Merge history requires a merge log table (not yet implemented)")


# ---------------------------------------------------------------------------
# DefactoConsumer — state history consumer
# ---------------------------------------------------------------------------


class DefactoConsumer:
    """State history consumer — reads effects from Kafka, writes SCD Type 2.

    Created via Defacto.consumer(). Reads entity snapshots and tombstones
    from the Kafka effects topic and writes them as SCD Type 2 rows to
    the configured analytical store.

    The consumer is stateless — it doesn't maintain entity state. The
    analytical store IS the state. Recovery uses Kafka offset tracking:
    offsets are committed after each successful write_batch, so crashes
    replay from the last committed offset. Writes are idempotent via
    (entity_id, valid_from) natural key.

    Example:
        consumer = Defacto.consumer(
            kafka={"bootstrap.servers": "kafka:9092", "topic": "defacto-effects"},
            database="postgresql://db/defacto",
            store="postgresql://db/analytics",
        )
        consumer.run()  # blocks forever, Ctrl+C to stop
    """

    def __init__(
        self,
        *,
        consumer: Any,
        topic: str,
        definitions_db: DefinitionsStore,
        state_history: Any,
        dead_letter: Any = None,
        batch_size: int,
        batch_timeout_ms: int,
    ) -> None:
        """Initialize with pre-configured Kafka consumer and backends.

        Use Defacto.consumer() to construct — it handles wiring.

        Args:
            consumer: confluent_kafka.Consumer instance.
            topic: Kafka topic to consume from.
            definitions_db: Shared definitions store (for version checks).
            state_history: StateHistoryBackend to write SCD Type 2 rows.
            dead_letter: Optional DeadLetterSink for routing write failures.
            batch_size: Max messages per write batch.
            batch_timeout_ms: Max wait time before flushing a partial batch.
        """
        self._consumer = consumer
        self._topic = topic
        self._definitions_db = definitions_db
        self._state_history = state_history
        self._dead_letter = dead_letter
        self._batch_size = batch_size
        self._batch_timeout_s = batch_timeout_ms / 1000
        self._running = False
        self._subscribed = False
        self._warmup_buffer: list[Any] = []
        self._current_version: str = definitions_db.active_version()

    def run(self) -> None:
        """Block and continuously consume effects, writing state history.

        Reads from Kafka in batches, writes to the analytical store,
        commits offsets. Runs until interrupted or close() is called.
        """
        self._ensure_subscribed()
        self._running = True
        try:
            while self._running:
                self._consume_batch()
        except KeyboardInterrupt:
            pass
        finally:
            self.close()

    def run_once(self) -> ConsumerResult:
        """Process available messages up to one batch and return.

        Useful for testing and controlled batch consumption.

        Returns:
            ConsumerResult with processing stats.
        """
        self._ensure_subscribed()
        return self._consume_batch()

    def status(self) -> ConsumerStatus:
        """Get current consumer status — lag, offset, throughput."""
        # Lag requires comparing current offset with high watermark
        assignments = self._consumer.assignment()
        lag = 0
        last_offset = 0
        for tp in assignments:
            low, high = self._consumer.get_watermark_offsets(tp, timeout=2)
            committed = self._consumer.committed([tp], timeout=2)
            if committed and committed[0].offset >= 0:
                lag += high - committed[0].offset
                last_offset = max(last_offset, committed[0].offset)
            else:
                lag += high - low

        return ConsumerStatus(
            lag=lag,
            last_offset=last_offset,
            last_write_time=None,  # requires per-batch timestamp tracking
            write_throughput=0.0,  # requires moving average calculation
        )

    def close(self) -> None:
        """Commit offsets and close all connections.

        Safe to call multiple times — the Kafka consumer may raise
        on double-close, which is suppressed.
        """
        self._running = False
        try:
            self._consumer.close()
        except RuntimeError:
            pass  # already closed
        self._state_history.close()
        self._definitions_db.close()

    def __enter__(self) -> Self:
        """Context manager entry."""
        return self

    def __exit__(self, *args: Any) -> None:
        """Context manager exit — calls close()."""
        self.close()

    # ── Private helpers ──

    def _classify_message(
        self,
        msg: Any,
        snapshots: list[dict[str, Any]],
        tombstones: list[dict[str, Any]],
    ) -> None:
        """Parse a Kafka message and append to the appropriate list."""
        headers = dict(msg.headers() or [])
        msg_type = headers.get("type", b"snapshot")
        if isinstance(msg_type, bytes):
            msg_type = msg_type.decode("utf-8")
        record = json.loads(msg.value())

        if msg_type == "tombstone":
            tombstones.append(record)
        else:
            snapshots.append(record)

    def _ensure_subscribed(self) -> None:
        """Subscribe to the topic and wait for partition assignment.

        Kafka consumer group rebalance takes a few seconds after subscribe.
        We poll until partitions are assigned so the first real consume
        doesn't miss messages due to rebalance timing. Any messages
        received during warmup are buffered for the next _consume_batch.
        """
        if not self._subscribed:
            self._consumer.subscribe([self._topic])
            # Wait for partition assignment (up to 10s)
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                msg = self._consumer.poll(timeout=0.5)
                if msg is not None and not msg.error():
                    self._warmup_buffer.append(msg)
                if self._consumer.assignment():
                    break
            self._subscribed = True

    def _consume_batch(self) -> ConsumerResult:
        """Accumulate up to batch_size messages or until timeout, then write.

        Messages are classified by Kafka header ('type': 'snapshot' or
        'tombstone'). Payloads are clean dicts — no transport markers.
        Offset is committed after successful write_batch.

        Checks for definition version changes before each batch. If the
        active version changed (another process called activate()), the
        consumer reloads definitions and switches state history tables.
        """
        # Check for version change (simple SELECT, negligible overhead)
        try:
            db_version = self._definitions_db.active_version()
        except ConfigError:
            db_version = self._current_version
        if db_version != self._current_version:
            new_defs = self._definitions_db.get(db_version)
            self._state_history.set_version(
                db_version, new_defs.get("entities", {}),
            )
            self._current_version = db_version

        snapshots: list[dict[str, Any]] = []
        tombstones: list[dict[str, Any]] = []
        start = time.monotonic()
        count = 0

        # Drain any messages received during subscription warmup
        for msg in self._warmup_buffer:
            self._classify_message(msg, snapshots, tombstones)
            count += 1
        self._warmup_buffer.clear()

        while count < self._batch_size:
            remaining = self._batch_timeout_s - (time.monotonic() - start)
            if remaining <= 0:
                break

            msg = self._consumer.poll(timeout=remaining)
            if msg is None:
                break
            if msg.error():
                continue

            self._classify_message(msg, snapshots, tombstones)
            count += 1

        if not snapshots and not tombstones:
            return ConsumerResult(
                snapshots_processed=0,
                tombstones_processed=0,
                batches_written=0,
                duration_ms=int((time.monotonic() - start) * 1000),
            )

        try:
            self._state_history.write_batch(snapshots, tombstones)
        except Exception as exc:
            # Write failed — route all records to dead letter, don't commit
            # offset so Kafka redelivers on next poll.
            error_msg = f"State history write failed: {exc}"
            if self._dead_letter:
                failures = [
                    EventFailure(
                        raw=record,
                        error=error_msg,
                        stage="consumption",
                        entity_id=record.get("entity_id"),
                        entity_type=record.get("entity_type"),
                        recoverable=True,
                    )
                    for record in [*snapshots, *tombstones]
                ]
                self._dead_letter.send(failures)
            return ConsumerResult(
                snapshots_processed=0,
                tombstones_processed=0,
                batches_written=0,
                duration_ms=int((time.monotonic() - start) * 1000),
            )

        self._consumer.commit()

        return ConsumerResult(
            snapshots_processed=len(snapshots),
            tombstones_processed=len(tombstones),
            batches_written=1,
            duration_ms=int((time.monotonic() - start) * 1000),
        )


# ---------------------------------------------------------------------------
# Standalone function
# ---------------------------------------------------------------------------


def validate_definitions(definitions: dict[str, Any] | Definitions) -> ValidationResult:
    """Validate definitions without a Defacto instance.

    For CI/CD pipelines — validates YAML definitions before deployment
    without connecting to a database or creating a Defacto instance.

    Args:
        definitions: Definition set — Definitions object or dict with
            'entities', 'sources', and optionally 'schemas' keys.

    Returns:
        ValidationResult with errors (blocking) and warnings (informational).
    """
    if isinstance(definitions, dict):
        definitions = Definitions.from_dict(definitions)
    return definitions.validate()
