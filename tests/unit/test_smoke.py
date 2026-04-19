"""Smoke tests — verify the project builds and imports work."""


def test_python_import():
    import defacto
    assert defacto.__version__ == "2.0.0-dev"


def test_rust_import():
    from defacto._core import __version__
    assert __version__ == "2.0.0-dev"
