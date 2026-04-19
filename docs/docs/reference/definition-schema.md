---
sidebar_position: 3
title: Definition Schema
---

# Definition Schema

Complete YAML schema for entities, sources, and schemas. This is the reference for every field and option available in defacto definitions. For a guided introduction, see the [definitions guide](/guides/definitions).

## Entity

```yaml
<entity_name>:
  starts: <state_name>
  identity: { ... }
  properties: { ... }
  states: { ... }
  relationships: [ ... ]
  always: { ... }
```

| Field | Required | Description |
|---|---|---|
| `starts` | yes | Initial state for new entities. Must exist in `states` |
| `identity` | yes | Identity field configuration |
| `properties` | no | Typed attributes on the entity |
| `states` | yes | State machine definition |
| `relationships` | no | Connections to other entity types |
| `always` | no | Handlers that fire in any state |

### identity

```yaml
identity:
  <field_name>:
    match: exact
    normalize: "str::to_lowercase(value)"
```

| Field | Default | Description |
|---|---|---|
| `match` | `exact` | Matching strategy: `exact` or `case_insensitive` |
| `normalize` | none | Expression applied to values before matching. Uses `value` as the input variable |

### properties

```yaml
properties:
  <property_name>:
    type: string
    default: ""
    sensitive: pii
    treatment: mask
    allowed: [free, pro, enterprise]
    min: 0
    max: 10000
    compute: "entity.mrr * 12"
```

| Field | Required | Description |
|---|---|---|
| `type` | yes | `string`, `number`, `integer`, `boolean`, or `datetime` |
| `default` | no | Value assigned when the entity is created |
| `allowed` | no | List of valid values |
| `min` | no | Minimum numeric value |
| `max` | no | Maximum numeric value |
| `sensitive` | no | Sensitivity label: `pii`, `phi`, or `pci` |
| `treatment` | no | Redaction behavior: `hash`, `mask`, or `redact` |
| `compute` | no | Expression recalculated after every state change |

Property names cannot collide with system columns. The system columns are `{entity_name}_id`, `{entity_name}_state`, `valid_from`, `valid_to`, `merged_into`, `last_event_time`, `state_entered_time`, and `created_time`.

### states

```yaml
states:
  <state_name>:
    when:
      <event_type>:
        guard: "expression"
        effects: [ ... ]
    after:
      - type: inactivity
        threshold: 90d
        effects: [ ... ]
```

A state with no `when` and no `after` is terminal.

### handler

```yaml
<event_type>:
  guard: "boolean expression"
  effects:
    - create
    - { transition: { to: <state_name> } }
    - { set: { property: <name>, from: event.<field> } }
    - { set: { property: <name>, value: <literal> } }
    - { set: { property: <name>, compute: "expression" } }
    - { set: { property: <name>, from: event.<field>, condition: "expression" } }
    - { increment: { property: <name>, by: <number> } }
    - { relate: { type: <rel_type>, target: <entity_type>, hints: { <entity>: [<field>] } } }
```

| Field | Required | Description |
|---|---|---|
| `guard` | no | Boolean expression. If false, handler is skipped |
| `effects` | yes | List of effects to apply (at least one) |

### effects

| Effect | Fields | Description |
|---|---|---|
| `create` | (none) | Initialize new entity. Idempotent |
| `transition` | `to` | Move to target state |
| `set` | `property`, one of `from`/`value`/`compute`, optional `condition` | Assign property value |
| `increment` | `property`, optional `by` (default 1) | Add to numeric property |
| `relate` | `type`, `target`, optional `hints` | Create relationship to another entity |

For `set`, exactly one of `from`, `value`, or `compute` must be specified. `from` uses dot-path syntax (`event.email`, `entity.mrr`). `compute` evaluates an expression. `value` is a literal.

### time rules

```yaml
after:
  - type: inactivity
    threshold: 90d
    effects: [ ... ]
```

| Field | Required | Description |
|---|---|---|
| `type` | yes | `inactivity`, `expiration`, or `state_duration` |
| `threshold` | yes | Duration: `30d`, `24h`, `90m`, `45s` |
| `effects` | yes | List of effects to apply when threshold is met |

### relationships

```yaml
relationships:
  - type: placed
    target: order
    cardinality: has_many
    properties:
      total: { type: number }
```

| Field | Required | Description |
|---|---|---|
| `type` | yes | Relationship name |
| `target` | yes | Target entity type (must be defined) |
| `cardinality` | yes | `has_many`, `has_one`, `belongs_to`, or `many_to_many` |
| `properties` | no | Typed attributes on the relationship (same format as entity properties) |

Both sides of a relationship must be declared. Valid cardinality pairs: `has_many`/`belongs_to`, `has_one`/`belongs_to`, `many_to_many`/`many_to_many`.

### always

```yaml
always:
  <event_type>:
    guard: "expression"
    effects: [ ... ]
```

Same format as state handlers. Fires regardless of which state the entity is in. Both state and always handlers fire if both match.

## Source

```yaml
<source_name>:
  event_type: <field_name>
  timestamp: <field_name>
  event_id: [<field_name>, ...]

  events:
    <event_name>:
      raw_type: "external.event.name"
      mappings:
        <output_field>:
          from: <source_field>
          compute: "expression"
          type: string
          default: "fallback"
      hints:
        <entity_type>: [<field_name>, ...]
```

### top-level fields

| Field | Required | Description |
|---|---|---|
| `event_type` | yes | Raw field containing the event type |
| `timestamp` | yes | Raw field containing the timestamp (ISO 8601 or Unix epoch) |
| `event_id` | no | Fields to hash for deduplication. Default: event type + all mapped data fields |

### event definition

| Field | Required | Description |
|---|---|---|
| `raw_type` | no | The external event name. Default: use the defacto event name as-is |
| `mappings` | yes | Field mapping configuration |
| `hints` | yes | Identity hints connecting events to entities |

### field mapping

| Field | Description |
|---|---|
| `from` | Source field. Supports dot-path (`user.address.city`) and array indexing (`items.0.id`) |
| `compute` | Expression using `event.*` for derived fields |
| `type` | Type coercion: `string`, `number`, `integer`, `boolean`, `datetime` |
| `default` | Fallback value if the source field is missing or null |

`from` and `compute` are mutually exclusive.

### hints

```yaml
hints:
  customer: [email, phone]
  order: [order_id]
```

Maps entity types to the field names that identify them. A single event can carry hints for multiple entity types.

## Schema

```yaml
<event_type>:
  fields:
    <field_name>:
      type: string
      required: true
      allowed: [a, b, c]
      min: 0
      max: 100
      min_length: 1
      regex: "^[a-z]+$"
```

| Field | Required | Description |
|---|---|---|
| `type` | yes | `string`, `number`, `integer`, `boolean`, or `datetime` |
| `required` | no | Whether the field must be present and non-null. Default: false |
| `allowed` | no | List of permitted values |
| `min` | no | Minimum numeric value (number/integer only) |
| `max` | no | Maximum numeric value (number/integer only) |
| `min_length` | no | Minimum string length (string only) |
| `regex` | no | Regex pattern the string must match (string only) |

Schemas define the contract between sources and entities. Source mappings must produce the fields the schema requires. At registration time, defacto validates that source output matches schema expectations.

## File organization

Definitions can be loaded from a directory or passed as a dict.

### Directory layout

```
my-project/
  entities/
    customer.yaml
    order.yaml
  sources/
    app.yaml
    billing.yaml
  schemas/         # optional
    customer_signup.yaml
```

Each file contains one or more definitions of its type. The directory names (`entities/`, `sources/`, `schemas/`) are required.

```python
d = Defacto("my-project/")
```

### Dict format

```python
d = Defacto({
    "entities": {
        "customer": { "starts": "lead", ... }
    },
    "sources": {
        "app": { "event_type": "type", ... }
    },
    "schemas": {}
})
```
