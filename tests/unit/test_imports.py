"""Verify every class is importable from its expected location.

This test catches broken imports, circular dependencies, and missing
re-exports. If this fails, something is structurally wrong.
"""


def test_top_level_core():
    from defacto import Defacto, DefactoConsumer, validate_definitions
    from defacto import DefinitionsManager, DefinitionsDraft
    from defacto import LedgerInspection, IdentityInspection


def test_top_level_definitions():
    from defacto import Entity, State, Handler, Effect, Property
    from defacto import Identity, Relationship, TimeRule
    from defacto import Source, Schema, Definitions


def test_top_level_errors():
    from defacto import DefactoError, ConfigError, DefinitionError
    from defacto import IngestError, ValidationError, BuildError
    from defacto import StorageError, IdentityError, NotFoundError, ConsumerError


def test_top_level_results():
    from defacto import IngestResult, BuildResult, TickResult
    from defacto import ValidationResult, Timeline, TimelineEntry, BuildStatus
    from defacto import EventFailure, ConsumerResult, ConsumerStatus, RegisterResult


def test_definitions_module():
    from defacto.definitions import Entity, State, Handler, Effect, Property
    from defacto.definitions import Identity, Relationship, TimeRule
    from defacto.definitions import Source, Schema, Definitions


def test_errors_module():
    from defacto.errors import DefactoError, ConfigError, DefinitionError
    from defacto.errors import IngestError, ValidationError, BuildError
    from defacto.errors import StorageError, IdentityError, NotFoundError, ConsumerError


def test_results_module():
    from defacto.results import IngestResult, BuildResult, TickResult
    from defacto.results import ValidationResult, Timeline, TimelineEntry, BuildStatus
    from defacto.results import EventFailure, ConsumerResult, ConsumerStatus, RegisterResult


def test_internal_modules():
    from defacto._pipeline import Pipeline
    from defacto._build import BuildManager
    from defacto._publisher import Publisher, KafkaPublisher, InlinePublisher
    from defacto._identity import IdentityResolver
    from defacto._ddl import DDLGenerator
    from defacto.backends._definition_store import DefinitionsStore
    from defacto._dead_letter import DeadLetterSink, KafkaDeadLetter, FileDeadLetter, NullDeadLetter


def test_backends_module():
    from defacto.backends import IdentityBackend, SqliteIdentity, PostgresIdentity
    from defacto.backends import LedgerBackend, SqliteLedger, PostgresLedger, TieredLedger
    from defacto.backends import StateHistoryBackend, SqliteStateHistory, PostgresStateHistory


def test_query_module():
    from defacto.query import DefactoQuery, DefactoTable, TableCollection
