---
slug: /
sidebar_position: 0
title: Defacto
---

# Defacto

Most systems lose meaning at the point of capture. Rows get mutated, history gets overwritten, and business logic ends up scattered across pipelines, dbt models, and dashboards that don't agree with each other.

Defacto takes a different approach. You declare what your entities are and how events affect them. Defacto captures the facts, interprets them through your definitions, and maintains a complete temporal history that you can query at any point in time.

```bash
pip install defacto
```

## How it works

You write definitions that describe your entities as state machines: what states they can be in, what events cause transitions, what properties to track, and how to identify them from raw event data.

You feed defacto raw events from any source. It normalizes them, resolves which entity each one belongs to (even across sources), interprets them through the state machine, and produces queryable state.

```python
from defacto import Defacto

d = Defacto("definitions/")
d.ingest("app", events, process=True)

d.table("customer").execute()                        # current state
d.history("customer").as_of("2024-01-15").execute()  # point-in-time
d.history("customer").execute()                      # full history
```

## Capabilities

- **Temporal state history** for every entity, queryable at any point in time
- **Identity resolution** across multiple sources with automatic merge handling
- **Declarative YAML definitions** with states, transitions, guards, computed properties, and time rules
- **Multiple storage backends** including SQLite, Postgres, DuckDB, Delta Lake, and Kafka

## Who this is for

- Data engineers building entity lifecycle pipelines
- Backend engineers who need temporal state without building it from scratch
- Teams dealing with identity resolution across multiple data sources
- Anyone who needs to answer "what did this entity look like on date X"

Follow the [quickstart](/get-started/quickstart) to go from install to working queries in five minutes, or read [how it works](/get-started/how-it-works) for the architecture.
