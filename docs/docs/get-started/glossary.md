---
sidebar_position: 3
title: Glossary
---

# Glossary

Terms used throughout the documentation, organized from foundational to derived.

## Entity

A thing in the real world that has identity, state, and history. Customers, orders, devices, accounts. An entity is a conceptual object whose state changes over time as events are interpreted through its state machine.

## Event

A record of something that happened. A dict with at least an event type and a timestamp. Events are immutable and come from external systems (application databases, webhooks, CSV files, Kafka topics). You don't need to design your events for defacto. You bring whatever you have and define how to interpret it.

## State

The current position of an entity in its lifecycle. Examples: "lead", "active", "churned", "shipped". An entity is always in exactly one state.

## Definition

A YAML specification of an entity type's behavior, including its states, transitions, guards, properties, identity fields, and time rules. Definitions are the core user-facing artifact in defacto.

## Source

A specification of how raw events from a particular system map to defacto's normalized format. Declares which field is the event type, which is the timestamp, how to map fields, and which fields identify which entities. You typically have one source per external system.

## Handler

A rule that fires when a specific event type arrives for an entity in a specific state. Contains a list of effects (what to do) and an optional guard (a condition that must be true for the effects to apply).

## Effect

An action that modifies entity state when a handler fires. There are five kinds:

| Effect | What it does |
|---|---|
| `create` | Initialize a new entity (idempotent) |
| `transition` | Move to a different state |
| `set` | Assign a value to a property |
| `increment` | Add to a numeric property |
| `relate` | Create a relationship to another entity |

Effects are applied in order within a handler. Each effect sees the state changes from previous effects.

## Guard

A boolean expression that must evaluate to true for a handler's effects to apply. Guards can reference event data (`event.plan`) and current entity state (`entity.mrr`).

## Property

A typed attribute on an entity. Properties have a type (string, number, integer, boolean, datetime), an optional default value, and optional sensitivity and governance labels. Properties become columns in the state history table and persist across state transitions.

## Time rule

A rule that fires based on elapsed time rather than events. Three types are supported:

| Type | Fires when |
|---|---|
| `inactivity` | No events received for the specified duration |
| `expiration` | The specified duration has elapsed since entity creation |
| `state_duration` | The entity has been in its current state for the specified duration |

Time rules produce effects just like event handlers. They're how you model things like "churn a customer after 90 days of inactivity."

## Expression

A formula evaluated against event data and entity state. Expressions are used in guards, computed properties, field mappings, and identity normalization. Defacto includes 38 built-in functions for string manipulation, math, datetime operations, type casting, arrays, and null handling.

## Ledger

The append-only, immutable log of all events. The single source of truth. The ledger stores both the raw input (exactly as received) and the normalized form (after field mapping and type coercion), so normalization logic can change without re-ingesting from source systems.

## Snapshot

A complete picture of an entity's state at a point in time, including entity ID, state, all property values, and timestamps. Snapshots are written to state history after each state change. They're complete (not deltas) so downstream consumers are stateless.

## Build

The process of interpreting events from the ledger to produce entity state. Defacto supports several build modes (incremental, full, full renormalize, full identity reset) and auto-detects the appropriate mode based on what changed in the definitions.

## Merge

When two entity IDs turn out to represent the same real-world entity. This is detected automatically when multiple identity hints in a single event resolve to different entities. Defacto reassigns events, rebuilds the combined history, and removes the duplicate. Merge selection is deterministic so multiple shards converge without coordination.

## Erase

Permanently delete an entity and all associated data: events, identity mappings, merge log entries, in-memory state, and state history. Cascades through merge chains. Used for GDPR right-to-erasure compliance.

## Redact

Remove sensitive field values from events while preserving the event structure. The entity persists with redacted values. Used when you need to remove specific data (like PII) without deleting the entire entity.

## Shard

A partition of the entity space. Each shard owns a deterministic subset of entities via SHA-256 hash partitioning. All shards share the same ledger and identity table. Sharding parallelizes I/O across processes, which is the primary scaling mechanism.

## Tombstone

A marker indicating that an entity no longer exists as a separate entity (it was merged into another, or erased). Tombstones flow through the same publish path as snapshots, ensuring downstream consumers know to remove the entity from their stores.
