---
sidebar_position: 2
title: How it works
---

# How it works

This page covers the architecture behind defacto. You don't need to understand all of it to use the framework, but it helps when you're making decisions about definitions, deployment, or debugging.

## Data model

Defacto organizes data into three layers.

**Events** are facts about what happened. A customer signed up, an order was placed, a payment failed. Events go into the ledger and never change. They're the raw material.

**Definitions** are your interpretation of what those facts mean. A signup creates a customer. An upgrade changes their plan. Ninety days of silence means they churned. Definitions are written in YAML and can be versioned.

**Entity state** is the result of applying definitions to events. It's computed, not stored independently. If you change your definitions and call `build()`, defacto recomputes state from the original events without losing anything.

This separation is what allows defacto to re-normalize events when source mappings change, re-resolve identity when matching rules change, and recover from crashes without data loss. The ledger is the single source of truth, and everything else is derived from it.

## The ledger

Every event goes into the ledger, which is an append-only log. The ledger stores both the raw input (exactly as received) and the normalized form (after field mapping and type coercion).

Storing both forms matters because normalization logic can change. If you rename a field in your source definition or add a type coercion, defacto can re-normalize from the original raw input. You never need to go back to the source system, which is especially important when the source is a third-party API or a Kafka topic you can't replay.

## Processing pipeline

When events arrive, they flow through four stages.

### Normalize

Raw events are transformed using your source definitions. Fields are mapped, types are coerced, derived fields are computed, and identity hints are extracted. This stage is pure computation with no side effects, so it runs in parallel across events.

### Resolve identity

Each event needs to be associated with an entity. The identity resolver takes the hints from the normalized event (email, phone, account_id) and maps them to entity IDs.

If a hint already exists in the identity table, the event goes to the existing entity. If the hint is new, a new entity is created. If multiple hints in a single event resolve to different entities, defacto detects this as a merge: two records that were tracked separately are actually the same real-world thing.

### Interpret

Events are processed through your state machines. For each event, defacto finds the matching handler in the entity's current state, evaluates the guard if there is one, and applies the effects in order. If any effect changes the entity's state or properties, a snapshot is produced.

Interpretation runs in parallel across entities but sequentially within each entity, because event order matters for state machine correctness.

### Publish

Snapshots and tombstones (for merged or erased entities) are written to state history. In development mode this writes directly to the database. In production with Kafka, snapshots are published to a topic and consumed by separate processes that write to analytical stores.

## Identity resolution

Each entity type declares identity fields, which are the fields that identify it. A customer might be identified by email and phone. An order by order_id. When an event arrives, defacto extracts those fields as hints and looks them up.

Most of the time this is straightforward: the hint maps to a known entity and processing continues. But sometimes an event carries two hints that point to different entities. For example, email `alice@example.com` maps to customer A, and phone `+1-555-0001` maps to customer B. This means A and B are the same person.

When this happens, defacto triggers a merge. It picks a winner deterministically (so multiple shards agree without coordination), reassigns the loser's identity mappings and events to the winner, records the merge in an audit log, and rebuilds the winner's state from the combined event history.

Identity resolution works across sources. If your app identifies customers by email and your billing system uses account_id, defacto will resolve both to the same entity as soon as one event carries both hints. No cross-referencing setup is needed.

## Build modes

When you call `build()`, defacto compares the current definitions against what was used for the last build and picks the appropriate mode.

| What changed | Mode | What happens |
|---|---|---|
| New events only | Incremental | Process from cursor to ledger head |
| Entity definitions | Full | Clear entity state, replay all events |
| Source definitions | Full renormalize | Re-normalize from raw input, then replay |
| Identity config | Full identity reset | Clear identity mappings, re-resolve, replay |
| Nothing | Skip | Nothing to do |

The cascade picks the most expensive applicable mode. If both source and entity definitions changed, you get a full renormalize (which includes a full rebuild). You don't need to select a mode manually.

During incremental builds, defacto checks for events with timestamps older than the previous batch (late arrivals from clock skew or out-of-order delivery). Affected entities are automatically rebuilt in a post-pass.

## State history

Every state change produces a row in the state history table, following the SCD Type 2 pattern. Each row has a `valid_from` timestamp (when this state began) and a `valid_to` timestamp (when it ended, or empty if current).

| Query | Method |
|---|---|
| Current state of all entities | `table("customer")` |
| State at a specific point in time | `history("customer").as_of("2024-01-15")` |
| Complete state history | `history("customer")` |

## Crash recovery

If defacto crashes mid-build, a dirty flag stays set in the database. On next startup, it detects the flag and runs a full rebuild from the ledger. The ledger is append-only, so no data is lost.

On a normal restart, the in-memory entity state is empty but the last build completed successfully. Defacto loads current entity state from state history (proportional to entity count, not event count) and runs an incremental build to catch up.

## Rust core

Normalization, expression evaluation, and interpretation run in Rust. Python handles all I/O: database reads and writes, Kafka, S3.

This split exists because the compute operations are CPU-bound and benefit from releasing Python's global lock for multi-core parallelism, while I/O benefits from Python's database driver ecosystem. You don't need Rust installed to use defacto; the compiled extension is included in the pip package.

The practical consequence is that the scaling path is I/O parallelism (sharding across processes) rather than adding compute threads. The computation is already fast. The bottleneck is database I/O.

## Formal verification

Defacto's cross-shard protocol is verified using TLA+ model checking, which exhaustively explores every possible sequence of operations to prove that safety invariants hold.

The implementation is also stress-tested with Hypothesis property-based testing, which generates random definitions, random events, and random operation sequences, and checks correctness invariants after every operation.
