---
sidebar_position: 4
title: Deployment
---

# Deployment

Defacto is a Python library that runs the same code at every scale. The difference between development and production is configuration, not code. This guide covers the deployment progression from local development to sharded production.

## Development

The default backend is SQLite. You don't need to configure anything.

```python
from defacto import Defacto

d = Defacto("definitions/")
d.ingest("app", events, process=True)
d.table("customer").execute()
```

SQLite stores everything in a single file inside a `.defacto/` directory in your project folder. This is ideal for developing definitions, running tests, prototyping queries, and learning the framework.

SQLite is single-writer, so it won't work for concurrent multi-process deployments. For that, you need Postgres.

## Production with Postgres

To move to Postgres, pass a connection string. Everything else stays the same.

```python
d = Defacto("definitions/", database="postgresql://user:pass@host:5432/mydb")
```

Postgres gives you durable storage, concurrent reads, JSONB indexing, and multi-process safe identity resolution. A single process against a properly tuned Postgres handles a high volume of events per day, which is sufficient for most production deployments.

### Batch processing

If your events arrive in batches (from an orchestrator, a file drop, or a scheduled pipeline), use the append-then-build pattern.

```python
d.ingest("app", events)
d.build()
```

This works well with orchestrators like Dagster or Airflow. A pipeline step calls `ingest()` with a batch of events, then `build()` processes them. The build auto-detects what mode to use based on whether definitions changed.

### Streaming

For real-time processing, use `process=True` on ingest. Events are interpreted through the state machine as they arrive.

```python
d = Defacto("definitions/", database="postgresql://...")
d.ingest("app", events, process=True)
```

To propagate state changes downstream in real time, add Kafka. Defacto publishes entity snapshots to a Kafka topic, and separate consumer processes write state history to analytical stores.

```python
d = Defacto("definitions/",
    database="postgresql://...",
    kafka={"bootstrap_servers": "localhost:9092", "topic": "entity-state"})
```

## Consumers

A `DefactoConsumer` reads entity snapshots from Kafka and writes SCD Type 2 history to a store. Consumers are stateless and recover from Kafka offsets.

```python
consumer = Defacto.consumer(
    kafka={"bootstrap_servers": "localhost:9092"},
    database="postgresql://...",
    store="postgresql://analytics-db/...")
consumer.run()  # blocks and consumes continuously
```

You can run multiple consumers writing to different stores. Each consumer manages its own Kafka offsets independently. For example, one consumer writes to Postgres for operational queries and another writes to DuckDB for local analytics. They're completely independent.

## Sharding

When a single process isn't enough throughput, you can shard across multiple processes. Each shard owns a deterministic subset of entities via SHA-256 hash partitioning.

```python
d = Defacto("definitions/",
    database="postgresql://...",
    shard_id=0, total_shards=4)
```

All shards share the same Postgres ledger and identity table. Each shard has its own database connections and processes only its owned entities. This parallelizes the I/O, which is the bottleneck.

Cross-shard identity resolution works automatically. If a merge is detected on one shard, other shards pick it up through the shared merge log and correct their caches.

Shard assignment is currently manual (you set `shard_id` on each process). Automated shard claiming with heartbeats and failover is planned as a separate `defacto-cluster` library.

## Tiered storage

For long retention or cost-sensitive deployments, the TieredLedger combines hot Postgres with cold Delta Lake on S3.

```python
d = Defacto("definitions/",
    database="postgresql://...",
    cold_ledger="s3://bucket/cold/")
```

New events go to the hot tier (Postgres) for real-time operations. Periodically, events are flushed to the cold tier and pruned from Postgres. Replays stitch hot and cold storage transparently.

Cold storage compresses significantly compared to Postgres row storage and can hold years of event history cost-effectively.

## Storage backends

### Ledger

The ledger is shared across all processes. All shards read and write to the same ledger, which is what makes identity resolution and merge detection work across the cluster.

| Backend | Use case |
|---|---|
| SQLite | Development, testing, single-process |
| Postgres | Production, multi-process, sharding |
| Postgres + Delta Lake | Long retention, cost-sensitive, compliance |
| Aurora / CockroachDB | Distributed writes, high shard counts (future) |

### State history

State history is the output of the pipeline. Your state history backend should match the scale and access pattern of your queries. With Kafka, you can fan out to multiple stores simultaneously, so you don't have to pick just one.

| Backend | Use case |
|---|---|
| SQLite | Development, testing |
| Postgres | Production, operational queries, moderate volume |
| DuckDB | Local analytics, embedded, single-user |
| BigQuery / Snowflake | Heavy analytics, large teams, warehouse-scale (future) |

### Identity

| Backend | Use case |
|---|---|
| SQLite | Development, testing |
| Postgres | Production, multi-process |
| Redis | Extreme scale, microsecond lookups (future) |

## Namespace isolation

If you need multiple independent defacto environments on the same Postgres database (for example, staging and production), use the `namespace` parameter.

```python
d = Defacto("definitions/", database="postgresql://...", namespace="staging")
```

Infrastructure tables go in the `{namespace}` schema and state history goes in `{namespace}_{version}` schemas. The default namespace is `defacto`.

## Configuration

The `Defacto` constructor accepts several configuration parameters.

| Parameter | Default | Description |
|---|---|---|
| `database` | SQLite (auto) | Database URL or file path |
| `batch_size` | 100 | Events per processing batch |
| `workers` | 1 | Rust thread pool size for parallelism |
| `shard_id` | None | Shard index (0-based) |
| `total_shards` | None | Total number of shards |
| `namespace` | `"defacto"` | Postgres schema prefix |
| `kafka` | None | Kafka config dict (`bootstrap_servers`, `topic`) |
| `cold_ledger` | None | S3 path for Delta Lake cold storage |
| `dead_letter` | None | Dead letter sink config (`type`, `path` or `topic`) |
| `log_level` | `"INFO"` | Logging level |
| `log_format` | `"console"` | Log format: `"console"` or `"json"` |

## Logging

Defacto uses structured logging with hierarchical loggers. In console mode, logs are formatted for readability. In JSON mode, all context fields are included for log aggregation.

```python
d = Defacto("definitions/", log_level="DEBUG", log_format="json")
```

Loggers are organized by subsystem: `defacto.pipeline`, `defacto.lifecycle`, `defacto.storage`, `defacto.dead_letter`.

## Recommendations

**Starting out.** Use SQLite. Develop your definitions, test with sample data, iterate on queries. No infrastructure needed.

**First production deployment.** Single Postgres process. Choose batch or streaming depending on your latency requirements. This handles most workloads.

**Scaling up.** Add sharding when single-process throughput isn't enough. Start with a small number of shards and monitor. The bottleneck is shared Postgres write capacity, so the ceiling depends on your Postgres instance.

**Multiple downstream consumers.** Add Kafka when you need real-time state propagation or when different teams need entity state in different stores. Each consumer is independent.

**Heavy analytics.** Don't run heavy analytical queries against your operational Postgres. Fan out via Kafka and use a separate consumer writing to an analytical store like BigQuery, Snowflake, or DuckDB.

**Long retention.** Add TieredLedger when Postgres storage costs become a concern or compliance requires years of event history.

Start simple, measure, scale when you need to. The same code runs at every scale.
