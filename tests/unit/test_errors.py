"""Verify the error hierarchy works correctly.

Errors are fully functional (not stubs) — they store message, details,
and recoverable flag, and work as normal Python exceptions.
"""

import pytest

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


class TestErrorHierarchy:
    """All error types inherit from DefactoError."""

    @pytest.mark.parametrize("error_cls", [
        ConfigError, DefinitionError, IngestError, ValidationError,
        BuildError, StorageError, IdentityError, NotFoundError, ConsumerError,
    ])
    def test_inherits_from_defacto_error(self, error_cls):
        assert issubclass(error_cls, DefactoError)
        assert issubclass(error_cls, Exception)

    def test_defacto_error_is_exception(self):
        with pytest.raises(DefactoError):
            raise ConfigError("test")


class TestErrorAttributes:
    """Errors carry structured context."""

    def test_basic_error(self):
        e = ConfigError("invalid URL")
        assert e.message == "invalid URL"
        assert e.details == {}
        assert e.recoverable is False

    def test_error_with_details(self):
        e = DefinitionError(
            "unknown state 'foo'",
            details={"entity": "customer", "state": "foo"},
        )
        assert e.message == "unknown state 'foo'"
        assert e.details["entity"] == "customer"
        assert e.details["state"] == "foo"

    def test_recoverable_error(self):
        e = StorageError("connection refused")
        assert e.recoverable is True  # storage errors are recoverable by default

    def test_non_recoverable_error(self):
        e = DefinitionError("invalid YAML")
        assert e.recoverable is False

    def test_repr(self):
        e = ConfigError("bad url")
        assert repr(e) == "ConfigError('bad url')"

    def test_str(self):
        e = IngestError("missing field 'email'")
        assert str(e) == "missing field 'email'"


class TestStorageErrorsContextManager:
    """storage_errors() wraps raw database exceptions."""

    def test_wraps_sqlite_error(self):
        """sqlite3.Error is caught and re-raised as StorageError."""
        import sqlite3
        from defacto.errors import storage_errors

        with pytest.raises(StorageError) as exc_info:
            with storage_errors("test operation"):
                raise sqlite3.OperationalError("database is locked")

        assert "test operation failed" in str(exc_info.value)
        assert exc_info.value.recoverable is True
        assert exc_info.value.details["operation"] == "test operation"

    def test_chains_original_exception(self):
        """Original exception is chained via __cause__."""
        import sqlite3
        from defacto.errors import storage_errors

        with pytest.raises(StorageError) as exc_info:
            with storage_errors("test"):
                raise sqlite3.IntegrityError("UNIQUE constraint")

        assert exc_info.value.__cause__ is not None
        assert isinstance(exc_info.value.__cause__, sqlite3.IntegrityError)
