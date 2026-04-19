"""Defacto — event-sourcing entity engine with declarative YAML state machines.

Usage:
    from defacto import Defacto

    # From YAML directory
    d = Defacto("project/")

    # From dict
    d = Defacto({"entities": {...}, "sources": {...}})

    # Ingest, build, query
    d.ingest("app", events)
    d.build()
    d.table("customer").execute()

    # Validation (CI/CD)
    from defacto import validate_definitions
    result = validate_definitions(yaml_defs)
"""

__version__ = "0.1.0"

# Core entry point
from defacto._defacto import (
    DefinitionsDraft,
    DefinitionsManager,
    IdentityInspection,
    LedgerInspection,
    Defacto,
    DefactoConsumer,
    validate_definitions,
)

# Definition objects
from defacto.definitions import (
    Definitions,
    Effect,
    Entity,
    Handler,
    Identity,
    Property,
    Relationship,
    Schema,
    Source,
    State,
    TimeRule,
)

# Logging
from defacto._logging import configure_logging

# Error types
from defacto.errors import (
    BuildError,
    ConfigError,
    ConsumerError,
    DefinitionError,
    IdentityError,
    IngestError,
    DefactoError,
    NotFoundError,
    StorageError,
    ValidationError,
)

# Result types
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
    TimelineEntry,
    ValidationResult,
)

__all__ = [
    # Core
    "Defacto",
    "DefactoConsumer",
    "validate_definitions",
    "DefinitionsManager",
    "DefinitionsDraft",
    "LedgerInspection",
    "IdentityInspection",
    # Logging
    "configure_logging",
    # Definitions
    "Entity",
    "State",
    "Handler",
    "Effect",
    "Property",
    "Identity",
    "Relationship",
    "TimeRule",
    "Source",
    "Schema",
    "Definitions",
    # Errors
    "DefactoError",
    "ConfigError",
    "DefinitionError",
    "IngestError",
    "ValidationError",
    "BuildError",
    "StorageError",
    "IdentityError",
    "NotFoundError",
    "ConsumerError",
    # Results
    "IngestResult",
    "EventFailure",
    "BuildResult",
    "TickResult",
    "MergeResult",
    "EraseResult",
    "ConsumerResult",
    "ConsumerStatus",
    "ValidationResult",
    "Timeline",
    "TimelineEntry",
    "BuildStatus",
    "RegisterResult",
]
