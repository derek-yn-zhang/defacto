# Defacto

An entity engine that turns operational events into temporal, queryable state.

You define your entities as state machines in YAML. Defacto handles ingestion, identity resolution, state computation, and temporal queries.

```python
from defacto import Defacto

d = Defacto("definitions/")
d.ingest("app", events, process=True)

# current state
d.table("customer").execute()

# point-in-time
d.history("customer").as_of("2024-01-15").execute()
```

## Status

Pre-alpha. The engine works, the API is stabilizing, documentation is in progress.

## Getting started

Requires Python 3.12+ and a Rust toolchain.

```bash
git clone https://github.com/derek-yn-zhang/defacto.git
cd defacto
make setup
python examples/quickstart/main.py
```

See `examples/quickstart/` for a minimal example and `examples/showcase/` for multi-entity, multi-source, identity resolution, merges, and erasure.
