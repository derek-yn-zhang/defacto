---
sidebar_position: 4
title: Configuration
---

# Configuration

All configuration is passed through the `Defacto` constructor. There are no config files or environment variables.

## Constructor parameters

```python
d = Defacto(
    config,
    database="postgresql://user:pass@host:5432/mydb",
    batch_size=100,
    workers=1,
    shard_id=0,
    total_shards=4,
    namespace="defacto",
    kafka={"bootstrap_servers": "localhost:9092", "topic": "entity-state"},
    cold_ledger="s3://bucket/cold/",
    dead_letter={"type": "file", "path": "/var/log/defacto/dead_letter.jsonl"},
    log_level="INFO",
    log_format="console",
)
```

### config

The first positional argument. Accepts three formats:

| Format | Example | Description |
|---|---|---|
| Directory path | `"my-project/"` | Loads YAML from `entities/`, `sources/`, `schemas/` subdirectories |
| Dict | `{"entities": {...}, "sources": {...}}` | Definitions as a Python dict |
| Definitions object | `Definitions.from_directory("...")` | Pre-loaded definitions |

### database

Database URL for the ledger, identity, and state history backends.

| URL | Backend |
|---|---|
| (omitted) | SQLite, auto-created in `.defacto/` inside the project directory |
| `"postgresql://user:pass@host:5432/db"` | Postgres |

SQLite is single-writer and intended for development. Postgres supports concurrent access and is required for sharding.

### batch_size

Number of events per processing batch when using `process=True`. Events are buffered until the batch is full, then processed together. Larger batches reduce per-event overhead but increase latency.

Default: `100`

### workers

Size of the Rust thread pool for normalization and interpretation. In practice, increasing this has minimal effect because I/O dominates pipeline time, not computation.

Default: `1`

### shard_id and total_shards

Enable sharding. Each process owns a deterministic subset of entities.

```python
d = Defacto("defs/", database="postgresql://...", shard_id=0, total_shards=4)
```

`shard_id` must be between 0 and `total_shards - 1`. All shards must use the same `total_shards` value. Entity assignment is via SHA-256 hash of the entity ID.

Both must be set together, or neither.

### namespace

Postgres schema prefix for all tables. Useful for running multiple independent defacto environments on the same database (e.g., staging and production).

Default: `"defacto"`

Infrastructure tables go in `{namespace}.` and state history tables go in `{namespace}_{version}.`.

### kafka

Enables Kafka publishing. Snapshots are published to the topic after each batch, partitioned by entity ID for ordering guarantees.

```python
kafka={
    "bootstrap_servers": "localhost:9092",
    "topic": "entity-state",
}
```

When Kafka is configured, defacto uses `KafkaPublisher` instead of `InlinePublisher`. Messages are compressed with lz4 and include a header indicating whether the message is a snapshot or a tombstone.

### cold_ledger

S3 path for Delta Lake cold storage. Enables the TieredLedger, which combines hot Postgres with cold Delta Lake for cost-effective long-term retention.

```python
cold_ledger="s3://my-bucket/defacto/cold/"
```

Requires the `deltalake` package: `pip install defacto[tiered]`.

### dead_letter

Where to route events that fail processing. Failed events are captured in result objects regardless of this setting. The dead letter sink provides a durable secondary destination.

```python
# File-based (JSONL, one line per failure)
dead_letter={"type": "file", "path": "/var/log/defacto/dead_letter.jsonl"}

# Kafka topic
dead_letter={"type": "kafka", "bootstrap_servers": "localhost:9092", "topic": "dead-letter"}
```

Default: no dead letter sink (failures only in result objects).

### log_level

Controls the verbosity of defacto's logging output.

| Level | What's logged |
|---|---|
| `DEBUG` | Everything, including per-batch details |
| `INFO` | Operation completions (ingest, build, merge, erase) |
| `WARNING` | Definition warnings, validation issues |
| `ERROR` | Storage failures, delivery errors |

Default: `"INFO"`

### log_format

| Format | Description |
|---|---|
| `"console"` | Human-readable, fixed-width columns |
| `"json"` | Structured JSON, one object per line. All context fields included |

Default: `"console"`

JSON format is intended for log aggregation systems (Datadog, Elasticsearch, CloudWatch). Console format is for local development.

## Consumer configuration

```python
consumer = Defacto.consumer(
    kafka={"bootstrap_servers": "localhost:9092"},
    database="postgresql://...",
    store="postgresql://analytics-db/...",
    batch_size=1000,
    batch_timeout_ms=5000,
    dead_letter=None,
    log_level="INFO",
    log_format="console",
)
```

| Parameter | Default | Description |
|---|---|---|
| `kafka` | required | Kafka connection config |
| `database` | required | Shared database (reads definitions and version info) |
| `store` | required | State history destination (where SCD tables are written) |
| `batch_size` | `1000` | Messages per write batch |
| `batch_timeout_ms` | `5000` | Max wait time before flushing a partial batch |
| `dead_letter` | `None` | Same format as above |
| `log_level` | `"INFO"` | Same as above |
| `log_format` | `"console"` | Same as above |

## Optional dependencies

Defacto's core depends only on `ibis-framework[duckdb]` and `pyyaml`. Backend-specific features require extras.

```bash
pip install defacto[postgres]     # psycopg for Postgres
pip install defacto[kafka]        # confluent-kafka
pip install defacto[tiered]       # deltalake for Delta Lake cold storage
pip install defacto[networkx]     # networkx + scipy for graph analytics
pip install defacto[neo4j]        # neo4j driver
pip install defacto[s3]           # boto3 for S3 access
pip install defacto[all]          # everything
```
