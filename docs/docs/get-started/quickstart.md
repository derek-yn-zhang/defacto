---
sidebar_position: 1
title: Quickstart
---

# Quickstart

This guide walks through defining an entity, ingesting events, and querying state. By the end you'll have a working entity engine running locally.

## Install

```bash
pip install defacto
```

Defacto uses SQLite by default, so there's nothing else to set up.

## Entity definition

Create a project directory with two subdirectories: `entities/` and `sources/`.

The entity definition describes the state machine for a customer. Customers start as `lead`, transition to `active` on signup, can upgrade their plan, and move to `churned` on cancel.

**entities/customer.yaml**

```yaml
customer:
  starts: lead

  identity:
    email: { match: exact }

  properties:
    email: { type: string }
    plan: { type: string, default: free }

  states:
    lead:
      when:
        signup:
          effects:
            - create
            - { set: { property: email, from: event.email } }
            - { transition: { to: active } }

    active:
      when:
        upgrade:
          guard: "event.plan != entity.plan"
          effects:
            - { set: { property: plan, from: event.plan } }
        cancel:
          effects:
            - { transition: { to: churned } }

    churned: {}
```

The `identity` section tells defacto how to recognize this entity. Customers are identified by email, so two events with the same email address are about the same customer.

The `churned` state has no handlers, making it terminal.

## Source definition

The source definition tells defacto how to read raw events from a particular system: which field contains the event type, which contains the timestamp, and how fields map to entity properties.

**sources/app.yaml**

```yaml
app:
  event_type: type
  timestamp: timestamp

  events:
    signup:
      mappings:
        email: { from: email }
      hints:
        customer: [email]

    upgrade:
      mappings:
        email: { from: email }
        plan: { from: plan }
      hints:
        customer: [email]

    cancel:
      mappings:
        email: { from: email }
      hints:
        customer: [email]
```

The `hints` section connects events to entities. `customer: [email]` means "use the email field to identify which customer this event belongs to."

## Ingesting events

Point defacto at your project directory and send it some events.

```python
from defacto import Defacto

d = Defacto("my-project/")

d.ingest("app", [
    {"type": "signup", "timestamp": "2024-01-01T10:00:00Z", "email": "alice@example.com"},
    {"type": "signup", "timestamp": "2024-01-05T14:00:00Z", "email": "bob@example.com"},
    {"type": "upgrade", "timestamp": "2024-01-15T09:00:00Z", "email": "alice@example.com", "plan": "pro"},
    {"type": "cancel", "timestamp": "2024-03-01T12:00:00Z", "email": "bob@example.com"},
], process=True)
```

Setting `process=True` tells defacto to interpret events through the state machine as they arrive. Without it, events are appended to the ledger and processed when you call `build()`.

## Querying state

### Current state

```python
df = d.table("customer").execute()
```

```
customer_state             email plan
        active alice@example.com  pro
       churned   bob@example.com free
```

Alice is active on the pro plan. Bob churned.

### Point-in-time

```python
df = d.history("customer").as_of("2024-01-10").execute()
```

```
customer_state             email plan
        active alice@example.com free
        active   bob@example.com free
```

On January 10th, both were active on the free plan. Alice hadn't upgraded yet, Bob hadn't cancelled.

### Full history

```python
df = d.history("customer").execute()
```

```
customer_state             email plan valid_from                  valid_to
        active alice@example.com free 2024-01-01T10:00:00+00:00 2024-01-15T09:00:00+00:00
        active   bob@example.com free 2024-01-05T14:00:00+00:00 2024-03-01T12:00:00+00:00
        active alice@example.com  pro 2024-01-15T09:00:00+00:00
       churned   bob@example.com free 2024-03-01T12:00:00+00:00
```

Each row represents a period of time when that state was true. An empty `valid_to` means the state is current.

## Summary

| Component | What it does |
|---|---|
| Entity definition | Describes the state machine (states, transitions, properties, identity) |
| Source definition | Maps raw event fields to entity properties and identity hints |
| `ingest()` | Normalizes events and appends them to the ledger |
| `process=True` | Interprets events through the state machine on ingest |
| `table()` | Queries current entity state |
| `history().as_of()` | Queries entity state at a specific point in time |
| `history()` | Returns the full temporal state history |

The default backend is SQLite. For production, pass a Postgres connection string and everything else stays the same.

For a more complete example with multiple entity types, multiple sources, automatic merges, and GDPR erasure, see `examples/showcase/` in the [repository](https://github.com/derek-yn-zhang/defacto).
