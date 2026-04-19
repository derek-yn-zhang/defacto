---
sidebar_position: 3
title: Lifecycle
---

# Lifecycle

Beyond defining and querying entities, defacto provides operations for managing entities over time: ingestion modes, builds, merges, erasure, redaction, and time rule evaluation.

## Ingestion

There are two ways to ingest events, depending on whether you want them processed immediately or in a separate step.

### Inline processing

Setting `process=True` tells defacto to interpret events through the state machine as they're ingested. Events are buffered internally and processed in batches for efficiency.

```python
d.ingest("app", events, process=True)
```

This is the simplest model. State is updated within the ingest call, and you can query it immediately after.

### Append and build

Without `process=True`, events are appended to the ledger but not interpreted. You call `build()` separately to process them.

```python
d.ingest("app", batch_1)
d.ingest("app", batch_2)
d.ingest("billing", batch_3)

result = d.build()
```

This is useful when events arrive frequently but you only need state updated periodically, or when you want to accumulate events from multiple sources before processing.

## Build

`build()` processes events from the ledger into entity state. It detects what changed since the last build and picks the appropriate mode automatically.

```python
result = d.build()
print(result.mode)              # INCREMENTAL, FULL, etc.
print(result.events_processed)
```

You can override the auto-detection if needed:

```python
d.build(full=True)        # force a full rebuild (clear state, replay all events)
d.build(from_raw=True)    # force re-normalization from raw input, then rebuild
```

Build results include the mode that was selected, event and entity counts, timing information, and any failures that occurred during processing.

## Merge

Merges happen in two ways: automatically through identity resolution, or explicitly through the `merge()` method.

### Automatic merges

When defacto processes an event with multiple identity hints that resolve to different entities, it detects that those entities are actually the same real-world thing. The merge happens automatically as part of ingestion or building.

For example, if Alice signed up with `alice@example.com` and later an event arrives carrying both `alice@example.com` and `+1-555-0001`, and `+1-555-0001` is already mapped to a different entity, defacto merges them.

### Explicit merges

You can also trigger a merge manually when you know two entities should be combined, for instance after a manual review or an external identity resolution process.

```python
result = d.merge(from_entity_id, into_entity_id, reason="manual_review")
print(result.events_reassigned)
print(result.entities_rebuilt)
```

The `reason` parameter is recorded in the audit log for traceability.

### What happens during a merge

Regardless of how a merge is triggered, the same protocol runs:

1. The loser's identity mappings are reassigned to the winner
2. The loser's events are reassigned to the winner in the event-entity mapping
3. The merge is recorded in the merge log (audit trail)
4. The winner's state is rebuilt from all combined events
5. The loser is tombstoned (removed from state history)

Merge selection is deterministic. The lexicographically smaller entity ID is always the winner, so multiple shards processing the same merge independently will agree on the outcome.

## Erase

`erase()` permanently deletes an entity and all of its associated data. This is designed for GDPR right-to-erasure compliance.

```python
result = d.erase(entity_id)
print(result.events_deleted)
print(result.entities_erased)
```

Erasure deletes:
- All identity mappings for the entity
- All events associated with the entity from the ledger
- Merge log entries involving the entity
- In-memory state
- State history rows

If the entity was involved in merges (either as a winner that absorbed other entities, or as a loser that was merged into another), the erasure cascades through the merge chain. All entities in the chain are erased.

Erasure is irreversible. Once an entity is erased, the data cannot be recovered.

## Redact

`redact()` removes sensitive field values from an entity's events while preserving the event structure. The entity continues to exist with sanitized data.

```python
count = d.redact(entity_id)
print(f"{count} events redacted")
```

Redaction replaces fields marked with `sensitive` in the entity definition with `[REDACTED]` in both the normalized data and the raw input stored in the ledger. After redaction, rebuilds produce entities with the redacted values.

Use `redact()` when you need to remove specific sensitive data (like PII) but keep the entity and its history intact. Use `erase()` when the entire entity must be removed.

Which fields are redacted is controlled by the `sensitive` and `treatment` options on properties in your entity definition:

```yaml
properties:
  email: { type: string, sensitive: pii, treatment: redact }
  phone: { type: string, sensitive: pii, treatment: mask }
```

## Tick

`tick()` evaluates time rules for all entities. Time rules (inactivity, expiration, state_duration) are defined in your entity definitions, but they need to be evaluated periodically to take effect.

```python
result = d.tick()
print(result.effects_produced)
print(result.entities_affected)
```

Time rules are also evaluated automatically during event processing (before each event is interpreted) and after builds. Calling `tick()` manually is useful when you want to trigger time-based transitions without ingesting new events, for instance on a scheduled basis.

In a sharded deployment, `tick()` also validates that each entity in memory still exists in the shared ledger. Entities that were erased or merged on another shard are cleaned up.

## Build status

You can inspect the current build state for a version, including the cursor position, pending events, and whether the state is dirty (a previous build was interrupted).

```python
status = d.build_status()
print(status.cursor)
print(status.pending_events)
print(status.dirty)
print(status.last_build_time)
```
