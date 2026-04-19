---
slug: /
sidebar_position: 0
title: Defacto
---

# Defacto

Defacto is an entity engine for turning operational events into temporal, queryable state. You define your entities as state machines in YAML, and defacto handles ingestion, identity resolution, state computation, and temporal queries.

```bash
pip install defacto
```

## Core concepts

At the heart of defacto are two building blocks:

**Definitions** describe your entities as state machines. What states they can be in, what events cause transitions, what properties to track, and how to identify them from raw event data.

**Events** are raw operational data from your systems. Signups, purchases, clicks, webhooks. Defacto normalizes them through your definitions, resolves which entity each one belongs to, and interprets them through the state machine.

The result is a complete temporal history of every entity, queryable at any point in time.

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

Follow the [quickstart](/get-started/quickstart) to go from install to working queries in five minutes, or read [how it works](/get-started/how-it-works) for the architecture.
