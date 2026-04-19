---
sidebar_position: 1
title: API
---

# API Reference

Complete reference for the public Python API.

## Defacto

The main entry point. Handles ingestion, building, querying, and lifecycle operations.

### Constructor

```python
from defacto import Defacto

d = Defacto(config, **kwargs)
```

`config` can be a directory path (string), a definitions dict, or a `Definitions` object. Defacto auto-detects the type.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `config` | `str`, `dict`, or `Definitions` | required | YAML directory path, definitions dict, or Definitions object |
| `database` | `str` | SQLite (auto) | Database URL or file path |
| `batch_size` | `int` | `100` | Events per processing batch |
| `workers` | `int` | `1` | Rust thread pool size |
| `shard_id` | `int` | `None` | Shard index (0-based) |
| `total_shards` | `int` | `None` | Total number of shards |
| `namespace` | `str` | `"defacto"` | Postgres schema prefix |
| `kafka` | `dict` | `None` | `{"bootstrap_servers": "...", "topic": "..."}` |
| `cold_ledger` | `str` | `None` | S3 path for Delta Lake cold storage |
| `dead_letter` | `dict` | `None` | `{"type": "file", "path": "..."}` or `{"type": "kafka", "topic": "..."}` |
| `log_level` | `str` | `"INFO"` | `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `log_format` | `str` | `"console"` | `"console"` or `"json"` |

### Ingestion

#### ingest()

```python
result = d.ingest(source, events, process=None)
```

| Parameter | Type | Description |
|---|---|---|
| `source` | `str` | Source name (must match a source definition) |
| `events` | `list[dict]` | Raw event dicts |
| `process` | `bool` or `str` or `None` | `True`: process inline. `None`: append only. String: process using that version |

Returns `IngestResult`.

### Building

#### build()

```python
result = d.build(version=None, full=False, from_raw=False)
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `version` | `str` | active version | Definition version to build |
| `full` | `bool` | `False` | Force full rebuild |
| `from_raw` | `bool` | `False` | Force re-normalization from raw input |

Returns `BuildResult`.

#### build_status()

```python
status = d.build_status(version=None)
```

Returns `BuildStatus` with `cursor`, `pending_events`, `dirty`, `last_build_time`, `last_build_mode`.

### Lifecycle

#### tick()

```python
result = d.tick(version=None, as_of=None)
```

Evaluates time rules for all entities. Returns `TickResult`.

#### merge()

```python
result = d.merge(from_entity_id, into_entity_id, reason="")
```

Merges two entities. Returns `MergeResult`.

#### erase()

```python
result = d.erase(entity_id)
```

Permanently deletes an entity and cascades through merge chains. Returns `EraseResult`.

#### redact()

```python
count = d.redact(entity_id)
```

Redacts sensitive fields in an entity's events. Returns the number of events redacted.

### Queries

#### table()

```python
df = d.table("customer").execute()
```

Returns a `DefactoTable` with current state (where `valid_to` is null).

#### history()

```python
df = d.history("customer").execute()
df = d.history("customer").as_of("2024-01-15").execute()
```

Returns a `DefactoTable` with full SCD Type 2 history.

#### tables()

```python
collection = d.tables("customer", "order")
collection = d.tables()  # all entity types
```

Returns a `TableCollection`.

#### query()

```python
df = d.query("SELECT * FROM customer_history WHERE mrr > 100").execute()
```

Raw SQL with validation against entity definitions. Returns a `DefactoTable`.

#### timeline()

```python
timeline = d.timeline(entity_id)
```

Returns a `Timeline` with chronological state change entries.

#### assert_entity()

```python
d.assert_entity(entity_id, state="active", plan="pro", mrr=99.0)
```

Raises `AssertionError` if the entity doesn't match. For testing and CI.

### Definition management

#### d.definitions

```python
d.definitions.versions()       # list all registered versions
d.definitions.active()         # current active version name
d.definitions.get(version)     # definitions dict for a version
d.definitions.register(version, definitions)
d.definitions.activate(version)
d.definitions.draft(version, based_on=None)
```

#### DefinitionsDraft

```python
draft = d.definitions.draft("v2", based_on="v1")
draft.add_property("customer", "ltv", {"type": "number", "default": 0})
draft.add_state("customer", "suspended", {...})
draft.add_transition("customer", "active", "suspended", "suspend", {...})
draft.add_handler("customer", "active", "downgrade", {...})
draft.update_identity("customer", {"email": {"match": "exact"}})
draft.remove_property("customer", "old_field")
draft.remove_state("customer", "deprecated_state")
draft.validate()     # returns ValidationResult
draft.diff()         # dict of changes vs base version
draft.impact()       # predicted build mode
draft.register()     # commit to database
```

### Inspection

#### d.ledger

```python
d.ledger.count()                # total events
d.ledger.count(source="app")    # events from a specific source
d.ledger.events_for(entity_id)  # events for a specific entity
```

#### d.identity

```python
d.identity.lookup(hint_value)   # entity ID for a hint
d.identity.hints(entity_id)     # all hints for an entity
```

### Standalone validation

```python
from defacto import validate_definitions

result = validate_definitions(definitions_dict)
result.valid     # bool
result.errors    # list[str]
result.warnings  # list[str]
```

### close()

```python
d.close()
```

Flushes pending events and releases all connections. Also works as a context manager:

```python
with Defacto("definitions/") as d:
    d.ingest("app", events, process=True)
```

## DefactoTable

Returned by `table()`, `history()`, and `query()`. Wraps an Ibis expression. All Ibis methods (filter, select, mutate, order_by, group_by, join) pass through and return new `DefactoTable` instances.

| Method | Description |
|---|---|
| `.execute()` | Execute and return pandas DataFrame |
| `.as_of(timestamp)` | Filter to state at a point in time (use on `history()`, not `table()`) |
| `.resolve_merges()` | Include pre-merge history from absorbed entities |
| `.to_pandas()` | Same as `.execute()` |
| `.to_pyarrow()` | Execute and return PyArrow Table |
| `.to_parquet(path)` | Export to Parquet file |
| `.to_csv(path)` | Export to CSV file |
| `.sql()` | Return generated SQL without executing |

## TableCollection

Returned by `tables()`. Iterable, indexable.

| Method | Description |
|---|---|
| `collection["customer"]` | Get a specific entity table |
| `for name, table in collection` | Iterate over entity tables |
| `.history()` | Return new collection with full SCD history |
| `.to_parquet(dir)` | Export to directory (one file per entity type) |
| `.to_csv(dir)` | Export to CSV directory |
| `.to_duckdb(path)` | Export to DuckDB file |
| `.to_pandas()` | Dict of DataFrames |
| `.to_networkx()` | NetworkX DiGraph |
| `.to_graph_json()` | `{nodes, edges}` for D3/Cytoscape |
| `.to_neo4j(url, auth=(user, pass))` | Export to Neo4j |

## Result types

All results are frozen dataclasses (immutable).

### IngestResult

| Field | Type | Description |
|---|---|---|
| `events_ingested` | `int` | Events accepted |
| `events_failed` | `int` | Events that failed normalization |
| `duplicates_skipped` | `int` | Events rejected by dedup |
| `failures` | `list[EventFailure]` | Details for each failed event |

### BuildResult

| Field | Type | Description |
|---|---|---|
| `mode` | `str` | `SKIP`, `INCREMENTAL`, `FULL`, `FULL_RENORMALIZE`, `FULL_WITH_IDENTITY_RESET` |
| `events_processed` | `int` | Events interpreted |
| `entities_created` | `int` | New entities |
| `entities_updated` | `int` | Existing entities with state changes |
| `merges_detected` | `int` | Identity merges during build |
| `late_arrivals` | `int` | Events with timestamps before watermark |
| `failures` | `list[EventFailure]` | Events that failed interpretation |

### TickResult

| Field | Type | Description |
|---|---|---|
| `effects_produced` | `int` | Time rule effects that fired |
| `entities_affected` | `int` | Entities with state changes |
| `transitions` | `int` | State transitions produced |

### MergeResult

| Field | Type | Description |
|---|---|---|
| `from_entity_id` | `str` | Entity merged away (loser) |
| `into_entity_id` | `str` | Entity merged into (winner) |
| `events_reassigned` | `int` | Events moved from loser to winner |
| `entities_rebuilt` | `int` | Entities rebuilt after merge |

### EraseResult

| Field | Type | Description |
|---|---|---|
| `entity_id` | `str` | Entity erased |
| `entities_erased` | `int` | Total entities erased (including merge cascade) |
| `events_deleted` | `int` | Ledger rows deleted |

### Timeline

| Field | Type | Description |
|---|---|---|
| `entity_id` | `str` | Entity this timeline is for |
| `entity_type` | `str` | Entity type |
| `entries` | `list[TimelineEntry]` | Chronological entries |

### TimelineEntry

| Field | Type | Description |
|---|---|---|
| `timestamp` | `datetime` | When the event occurred |
| `event_type` | `str` | Normalized event type |
| `event_id` | `str` | Unique event identifier |
| `effects` | `list[str]` | Human-readable effect descriptions |
| `state_before` | `str` or `None` | State before this event |
| `state_after` | `str` or `None` | State after this event |

### BuildStatus

| Field | Type | Description |
|---|---|---|
| `cursor` | `int` | Last processed sequence number |
| `pending_events` | `int` | Events not yet processed |
| `dirty` | `bool` | Whether a previous build was interrupted |
| `last_build_time` | `str` or `None` | ISO 8601 timestamp of last build |
| `last_build_mode` | `str` or `None` | Mode of last build |

## Error types

All errors inherit from `DefactoError`.

| Error | When |
|---|---|
| `ConfigError` | Bad connection string, missing parameters |
| `DefinitionError` | Invalid YAML, schema violations |
| `IngestError` | All events in a batch fail normalization |
| `ValidationError` | Schema validation failure |
| `BuildError` | Interpretation failure |
| `StorageError` | Database read/write failure |
| `IdentityError` | Merge conflict, cache corruption |
| `NotFoundError` | Entity or version doesn't exist |
| `ConsumerError` | Kafka consumer failure |
| `QueryError` | Invalid SQL reference |
