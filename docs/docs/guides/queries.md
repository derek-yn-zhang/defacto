---
sidebar_position: 2
title: Queries
---

# Queries

Once events are ingested and interpreted, you query entity state through defacto's query layer. The query layer is read-only and completely separate from the write path. It connects to wherever state history lives and returns lazy expressions that execute on `.execute()`.

## Current state

`table()` returns the current state of all entities of a given type. This is equivalent to filtering the history table to rows where `valid_to` is null.

```python
df = d.table("customer").execute()
```

```
customer_id                          customer_state             email       plan   mrr
4b574976-...                                 active alice@example.com enterprise 199.0
a1edf991-...                                 active   bob@example.com    starter  19.0
```

You can filter, select, and chain operations using standard Ibis methods. The results stay lazy until you call `.execute()`.

```python
active = d.table("customer").filter(
    d.table("customer")._expr.customer_state == "active"
).execute()
```

## Point-in-time queries

`history().as_of()` returns entity state as it was at a specific moment. This filters the SCD Type 2 history to the row that was valid at the given timestamp.

```python
df = d.history("customer").as_of("2024-01-10").execute()
```

```
customer_state             email plan  mrr
        active alice@example.com free 49.0
        active   bob@example.com free 19.0
```

The timestamp can be an ISO 8601 string or a Python `datetime` object.

:::note[table() vs history()]

`table()` returns only current state (where `valid_to` is null). Point-in-time queries need the full history, so use `history().as_of()` instead of `table().as_of()`.

:::

## Full history

`history()` returns the complete SCD Type 2 history for an entity type. Every state change is a row, with `valid_from` and `valid_to` timestamps marking when that state was active.

```python
df = d.history("customer").execute()
```

```
customer_state       plan   mrr valid_from                  valid_to
         trial       free     0 2024-01-01T10:00:00+00:00 2024-01-02T09:00:00+00:00
        active        pro  49.0 2024-01-02T09:00:00+00:00 2024-02-01T10:00:00+00:00
        active enterprise 199.0 2024-02-01T10:00:00+00:00
```

To get the history of a specific entity, filter by the entity ID column:

```python
df = d.history("customer").filter(
    d.history("customer")._expr.customer_id == alice_id
).execute()
```

## Merge history

When entities are merged, the loser's state history rows are marked with a `merged_into` column pointing to the winner. By default, queries only return the winner's history.

`resolve_merges()` includes the loser's pre-merge history in the result, giving you a unified timeline across the merge.

```python
df = d.history("customer").resolve_merges().execute()
```

This is useful for seeing the complete journey of a canonical entity, including any records that were merged into it.

## Multiple entity types

`tables()` returns a collection of entity tables. You can iterate over them, index by name, or export them all at once.

```python
collection = d.tables("customer", "order")

# iterate
for entity_type, table in collection:
    print(entity_type, len(table.execute()))

# index
customers = collection["customer"].execute()
```

If you call `tables()` with no arguments, it selects all entity types from your definitions.

## Exports

The query layer supports several export formats, available on both individual tables and collections.

### Single table exports

```python
d.table("customer").to_parquet("customers.parquet")
d.table("customer").to_csv("customers.csv")
d.table("customer").to_pandas()     # returns pandas DataFrame
d.table("customer").to_pyarrow()    # returns PyArrow Table
```

### Collection exports

```python
collection = d.tables("customer", "order")

# directory of files (one per entity type + relationships)
collection.to_parquet("/export/parquet/")
collection.to_csv("/export/csv/")

# snapshot to DuckDB file
collection.to_duckdb("/export/snapshot.db")

# dict of DataFrames
dfs = collection.to_pandas()

# graph formats
collection.to_networkx()                               # NetworkX DiGraph
collection.to_graph_json()                              # {nodes, edges} for D3/Cytoscape
collection.to_neo4j("bolt://localhost", auth=("neo4j", "pass"))
```

## Graph queries

Defacto includes graph query support for traversing relationships between entities. The default backend uses recursive SQL CTEs, so no additional dependencies are needed.

```python
# entities directly connected to a customer
d.graph.neighbors("customer_abc123")

# follow a relationship chain
d.graph.traverse("customer_abc123", "placed", depth=2)

# shortest path between two entities
d.graph.path("customer_abc123", "order_xyz789")

# entities connected via a specific relationship type
d.graph.related("customer_abc123", "placed")
```

For graph analytics (community detection, centrality, similarity), defacto can use NetworkX or Neo4j as the graph backend. NetworkX is auto-materialized on first access to `d.graph`.

## Raw SQL

If you need to write SQL directly, `query()` validates your SQL against the entity definitions before executing. This catches typos in table and column names before they reach the database.

```python
df = d.query("""
    SELECT customer_id, customer_state, mrr
    FROM customer_history
    WHERE valid_to IS NULL AND mrr > 100
""").execute()
```

## SQL inspection

To see the SQL that Ibis would generate without executing it:

```python
sql = d.table("customer").filter(
    d.table("customer")._expr.mrr > 100
).sql()
print(sql)
```

## Timeline

`timeline()` returns a chronological view of a specific entity's lifecycle. Each entry shows the event, the state change, and what effects were produced.

```python
timeline = d.timeline(entity_id)
for entry in timeline.entries:
    print(f"{entry.timestamp}  {entry.state_after}  {entry.effects}")
```

```
2024-01-01 10:00:00+00:00   trial  ['Entity created', 'email: alice@example.com', ...]
2024-01-02 09:00:00+00:00  active  ['State: trial → active', 'plan: free → pro', ...]
2024-02-01 10:00:00+00:00  active  ['plan: pro → enterprise', 'mrr: 49.0 → 199.0', ...]
```

This is primarily a debugging tool for understanding how an entity reached its current state.

## Standalone query access

You can connect to a state history store without a full defacto pipeline. This is useful for read-only analytics or connecting to a store that a consumer process is writing to.

```python
from defacto.query import DefactoQuery

q = DefactoQuery.connect("postgresql://analytics-db/...", definitions, "v1")
df = q.table("customer").execute()
```
