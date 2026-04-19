.PHONY: setup setup-test build release test-unit test-rust test-correctness test-hypothesis test-integration test test-all bench bench-rust verify infra infra-down clean

# ── Setup ──

# Dev setup — unit tests and Hypothesis only
setup:
	uv sync --group dev
	maturin develop --uv

# Full test setup — includes backend deps for integration tests
setup-test:
	uv sync --group test
	maturin develop --uv

# ── Build ──

# Rebuild Rust extension (incremental, fast)
build:
	maturin develop --uv

# Rebuild in release mode (for benchmarks)
release:
	maturin develop --uv --release

# ── Test ──

# PYTHONPATH ensures Python finds the defacto package from the source tree.
# The Rust extension (.so) lives in python/defacto/ after maturin develop.
DEFACTO_RUN = PYTHONPATH=$(PWD)/python .venv/bin/python -m pytest

# Rust unit tests
test-rust:
	cargo test -p defacto-engine --lib

# Python unit tests (no infrastructure needed)
test-unit:
	$(DEFACTO_RUN) tests/unit/ -v

# Correctness contract tests (SQLite always, Postgres if available)
test-correctness:
	$(DEFACTO_RUN) tests/correctness/ -v --ignore=tests/correctness/test_invariants.py

# Hypothesis property-based tests (random operation sequences + random definitions)
test-hypothesis:
	$(DEFACTO_RUN) tests/correctness/test_invariants.py -v

# Integration tests (needs docker compose up)
test-integration:
	$(DEFACTO_RUN) tests/integration/ -v

# All local tests (unit + correctness + hypothesis, no infrastructure)
test:
	$(DEFACTO_RUN) tests/unit/ tests/correctness/ -v

# Everything (unit + correctness + integration, needs docker compose up)
test-all:
	$(DEFACTO_RUN) tests/ -m "not cloud" -v

# ── Verify ──

# TLA+ protocol verification (~8 seconds, 297K states)
verify:
	cd protocol && java -cp ../tla2tools.jar tlc2.TLC DefactoProtocol

# ── Benchmark ──

# Python benchmarks (needs release build + docker compose up)
bench:
	$(DEFACTO_RUN) benchmarks/ --benchmark-enable

# Rust criterion benchmarks
bench-rust:
	cargo bench -p defacto-engine

# ── Infrastructure ──

# Start all Docker services
infra:
	docker compose up -d

# Stop all Docker services
infra-down:
	docker compose down

# ── Clean ──

clean:
	cargo clean
	rm -rf .pytest_cache .benchmarks .hypothesis
