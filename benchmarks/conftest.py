"""Benchmark configuration and shared fixtures.

Benchmarks require a release build for meaningful numbers:
    make release    # builds Rust extension with optimizations
    make bench      # runs benchmarks

pytest-benchmark is configured here with settings appropriate for
performance measurement: warmup enabled, GC disabled during measurement,
minimum rounds for statistical significance.

Benchmarks are disabled by default (--benchmark-disable in pyproject.toml's
addopts). Enabled explicitly via `make bench` which passes --benchmark-enable.
"""

import os
import sys
import warnings

import pytest

from benchmarks.datagen import BENCH_DEFINITIONS, SyntheticGenerator


def pytest_configure(config):
    """Warn if running benchmarks without a release build.

    Debug builds are 10-50x slower than release. Benchmarking a debug
    build produces numbers that don't reflect production performance.
    """
    # Check if we're actually running benchmarks (not just collecting)
    if config.option.benchmark_disable:
        return

    # Heuristic: release builds are significantly larger than debug
    try:
        from defacto import _core
        so_path = _core.__file__
        if so_path:
            size_mb = os.path.getsize(so_path) / (1024 * 1024)
            # Release .so is typically 2-5MB, debug is 20-50MB
            if size_mb > 15:
                warnings.warn(
                    f"Rust extension appears to be a debug build ({size_mb:.0f}MB). "
                    f"Run 'make release' for meaningful benchmark numbers.",
                    stacklevel=1,
                )
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Data fixtures — deterministic, shared across benchmark functions
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def small_events():
    """1,000 events across 100 entities — quick benchmarks."""
    return SyntheticGenerator(
        BENCH_DEFINITIONS, n_events=1_000, n_entities=100, seed=42,
    ).generate()


@pytest.fixture(scope="session")
def medium_events():
    """10,000 events across 1,000 entities — standard benchmarks."""
    return SyntheticGenerator(
        BENCH_DEFINITIONS, n_events=10_000, n_entities=1_000, seed=42,
    ).generate()


@pytest.fixture(scope="session")
def large_events():
    """100,000 events across 10,000 entities — scale benchmarks."""
    return SyntheticGenerator(
        BENCH_DEFINITIONS, n_events=100_000, n_entities=10_000, seed=42,
    ).generate()


@pytest.fixture(scope="session")
def bench_definitions():
    """Benchmark entity/source definitions."""
    return BENCH_DEFINITIONS
