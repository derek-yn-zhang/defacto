"""Defacto backend interfaces — pluggable storage for identity, ledger, and state history.

Each backend type has an abstract interface and concrete implementations.
The framework selects the implementation based on configuration (database URL).

Import backends from here:
    from defacto.backends import IdentityBackend, PostgresIdentity
    from defacto.backends import LedgerBackend, PostgresLedger, TieredLedger
    from defacto.backends import StateHistoryBackend, SqliteStateHistory
"""

from defacto.backends._identity import (
    IdentityBackend,
    PostgresIdentity,
    SqliteIdentity,
)
from defacto.backends._ledger import (
    LedgerBackend,
    PostgresLedger,
    SqliteLedger,
    TieredLedger,
)
from defacto.backends._state_history import (
    DuckDBStateHistory,
    PostgresStateHistory,
    SqliteStateHistory,
    StateHistoryBackend,
)

__all__ = [
    "DuckDBStateHistory",
    "IdentityBackend",
    "LedgerBackend",
    "PostgresIdentity",
    "PostgresLedger",
    "PostgresStateHistory",
    "SqliteIdentity",
    "SqliteLedger",
    "SqliteStateHistory",
    "StateHistoryBackend",
    "TieredLedger",
]
