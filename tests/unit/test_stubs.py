"""Verify that stub methods raise NotImplementedError and ABCs can't be instantiated.

This test ensures the API surface is properly stubbed — every method exists
and is callable (just not implemented yet). As implementation progresses,
these tests get replaced with real behavior tests.
"""

import pytest

from defacto import Defacto, DefactoConsumer, validate_definitions
from defacto.backends import IdentityBackend, LedgerBackend, StateHistoryBackend


class TestDefactoStubs:
    """Defacto class — implemented methods tested in test_defacto.py."""

    def test_consumer_importable(self):
        """DefactoConsumer is importable and has the expected interface."""
        assert hasattr(DefactoConsumer, "run")
        assert hasattr(DefactoConsumer, "run_once")
        assert hasattr(DefactoConsumer, "status")
        assert hasattr(DefactoConsumer, "close")

    def test_validate_definitions(self):
        result = validate_definitions({})
        assert result.valid


class TestBackendAbstracts:
    """Backend interfaces are abstract — can't be instantiated."""

    def test_identity_backend_is_abstract(self):
        with pytest.raises(TypeError):
            IdentityBackend()  # type: ignore

    def test_ledger_backend_is_abstract(self):
        with pytest.raises(TypeError):
            LedgerBackend()  # type: ignore

    def test_state_history_backend_is_abstract(self):
        with pytest.raises(TypeError):
            StateHistoryBackend()  # type: ignore
