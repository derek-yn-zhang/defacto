---
sidebar_position: 1
title: Definitions
---

# Definitions

Definitions are the core of defacto. They describe your entities as state machines: what states they can be in, what events cause transitions, what properties to track, and how to resolve identity from raw event data.

Definitions are written in YAML and organized into three types: entities, sources, and schemas.

## Entities and states

An entity definition starts with a name, a starting state, and a set of states. Each state can have handlers that fire when specific events arrive.

```yaml
customer:
  starts: lead

  states:
    lead:
      when:
        signup:
          effects:
            - create
            - { transition: { to: active } }

    active:
      when:
        cancel:
          effects:
            - { transition: { to: churned } }

    churned: {}
```

The `starts` field determines the initial state for new entities. When the first event for a new customer arrives and the `signup` handler fires, defacto creates the entity in `lead` and immediately transitions it to `active`.

A state with no handlers (like `churned` above) is terminal. Entities in that state won't respond to any events unless you add an `always` handler (covered below).

## Effects

Effects are the actions that modify entity state when a handler fires. There are five kinds.

### create

Initializes a new entity. This is idempotent, so it's safe to include in handlers that might fire more than once.

```yaml
effects:
  - create
```

### transition

Moves the entity to a different state.

```yaml
effects:
  - { transition: { to: active } }
```

### set

Assigns a value to a property. There are three ways to specify the value:

```yaml
# From an event field
- { set: { property: email, from: event.email } }

# From a literal value
- { set: { property: status, value: "pending" } }

# From a computed expression
- { set: { property: annual_revenue, compute: "entity.mrr * 12" } }
```

You can also add a per-effect condition that must be true for the set to apply:

```yaml
- { set: { property: high_score, from: event.score, condition: "event.score > entity.high_score" } }
```

### increment

Adds to a numeric property. Defaults to incrementing by 1.

```yaml
- { increment: { property: login_count } }
- { increment: { property: balance, by: -50 } }
```

### relate

Creates a relationship to another entity. The target entity is identified using the same hint mechanism as identity resolution.

```yaml
- { relate: { type: placed_by, target: customer, hints: { customer: [email] } } }
```

Effects are applied in order within a handler. Each effect sees the state changes from previous effects, so a `set` that runs after a `transition` will see the new state.

## Properties

Properties are typed attributes on an entity. They persist across state transitions and become columns in the state history table.

```yaml
properties:
  email: { type: string }
  mrr: { type: number, default: 0 }
  plan: { type: string, allowed: [free, pro, enterprise] }
  signup_date: { type: datetime }
  annual_revenue: { type: number, compute: "entity.mrr * 12" }
```

Supported types are `string`, `number`, `integer`, `boolean`, and `datetime`.

| Option | What it does |
|---|---|
| `default` | Value used when the entity is created |
| `allowed` | Restricts the property to specific values |
| `min`, `max` | Numeric bounds (validation) |
| `compute` | Expression that's recalculated after every state change |
| `sensitive` | Sensitivity label: `pii`, `phi`, or `pci` |
| `treatment` | How to handle sensitive data on redact: `hash`, `mask`, or `redact` |

Computed properties are useful for derived values like `annual_revenue` above. The expression is re-evaluated whenever the entity's state changes, so computed properties always reflect the current state.

## Guards

Guards are boolean expressions that determine whether a handler's effects should apply. If the guard evaluates to false, the handler is skipped entirely.

```yaml
active:
  when:
    upgrade:
      guard: "event.plan != entity.plan"
      effects:
        - { set: { property: plan, from: event.plan } }
```

Guards can reference event data with `event.*` and current entity state with `entity.*`. They support the full expression language, including functions:

```yaml
guard: "event.amount > 0 and days_since(entity.last_purchase) < 365"
```

## Always handlers

Handlers in the `always` section fire regardless of which state the entity is in. They're useful for events that should be handled the same way everywhere.

```yaml
customer:
  starts: lead
  states:
    lead: { ... }
    active: { ... }
    churned: { ... }

  always:
    admin_reset:
      effects:
        - { transition: { to: lead } }
```

Both the state-specific handler and the always handler will fire if both match the incoming event type. State effects apply first, then always effects.

## Identity

The identity section declares which fields identify an entity. When defacto processes an event, it extracts these fields as hints and uses them to find or create the entity.

```yaml
identity:
  email:
    match: exact
    normalize: "str::to_lowercase(value)"
  phone:
    match: exact
```

| Option | What it does |
|---|---|
| `match` | Matching strategy: `exact` or `case_insensitive` |
| `normalize` | Expression applied to hint values before matching |

The `normalize` expression is useful for canonicalizing values. In the example above, all email comparisons happen in lowercase, so `Alice@Example.com` and `alice@example.com` resolve to the same entity.

An entity can have multiple identity fields. If an event carries two hints that resolve to different entities, defacto detects this as a merge and combines them automatically. See [how it works](/get-started/how-it-works#identity-resolution) for details.

## Time rules

Time rules fire based on elapsed time rather than events. They're defined in the `after` section of a state.

```yaml
active:
  when:
    upgrade: { ... }
  after:
    - type: inactivity
      threshold: 90d
      effects:
        - { transition: { to: churned } }
```

Three types are available:

| Type | Fires when |
|---|---|
| `inactivity` | No events received for the specified duration |
| `expiration` | The specified duration has elapsed since entity creation |
| `state_duration` | The entity has been in its current state for the specified duration |

Threshold values use duration syntax: `30d` (days), `24h` (hours), `90m` (minutes), `45s` (seconds).

Time rules are evaluated during event processing and when you call `tick()`. They produce effects just like event handlers.

## Relationships

Relationships connect entities to each other. They're declared on the entity definition and created via the `relate` effect.

```yaml
customer:
  relationships:
    - type: placed
      target: order
      cardinality: has_many

order:
  relationships:
    - type: placed_by
      target: customer
      cardinality: belongs_to
```

Both sides of a relationship must be declared. Valid cardinality values are `has_many`, `has_one`, `belongs_to`, and `many_to_many`.

Relationships can have their own typed properties:

```yaml
relationships:
  - type: placed
    target: order
    cardinality: has_many
    properties:
      total: { type: number }
```

Relationship history is tracked with the same SCD Type 2 pattern as entity state.

## Sources

Source definitions tell defacto how to read raw events from external systems. Each source declares the event type field, timestamp field, and per-event-type field mappings.

```yaml
billing:
  event_type: event
  timestamp: created_at
  event_id: [transaction_id]

  events:
    order_created:
      raw_type: "order.created"
      mappings:
        order_id: { from: id }
        email: { from: customer_email }
        total: { from: amount, type: number }
        item_count: { from: line_item_count, type: integer }
      hints:
        order: [order_id]
        customer: [email]
```

The `raw_type` field lets you map the external system's event names to your internal names. In this example, the billing system sends events with `event: "order.created"`, which defacto maps to the `order_created` handler.

Field mapping options:

| Option | What it does |
|---|---|
| `from` | Source field name (supports dot-path: `user.address.city`) |
| `compute` | Expression for derived fields |
| `type` | Type coercion: `string`, `number`, `integer`, `boolean`, `datetime` |
| `default` | Fallback value if the field is missing |

The `from` and `compute` options are mutually exclusive.

The `event_id` field controls deduplication. By default, defacto hashes the event type and all mapped data fields. If you specify `event_id: [transaction_id]`, only the event type and the transaction_id field are hashed, which is useful when you have a natural unique identifier.

The `hints` section connects events to entities. `customer: [email]` means "use the email field from this event to identify which customer it belongs to." A single event can carry hints for multiple entity types, which is how cross-entity relationships are established.

## Schemas

Schemas define the contract between sources and entities. They specify what a normalized event must look like after a source handler processes it: which fields must be present, what types they should be, and what values are valid.

This matters because the three definition types form a pipeline:

1. **Sources** take raw events and produce normalized events through field mappings
2. **Schemas** define what those normalized events must contain
3. **Entities** consume normalized events through state machine handlers

When you register definitions, defacto validates this chain. If a source's field mappings don't produce the fields that the schema requires, or produce fields that aren't in the schema, you get a validation error before anything runs. At runtime, if the data types don't match what the engine expects, the event fails normalization.

```yaml
order_created:
  fields:
    order_id:
      type: string
      required: true
    total:
      type: number
      required: true
      min: 0
    email:
      type: string
      required: true
```

This schema says that any `order_created` event must have an `order_id` string, a `total` number that's at least 0, and an `email` string. The source mappings for `order_created` must produce all three, and the entity handler can rely on them being present and correctly typed.

Field options:

| Option | What it does |
|---|---|
| `type` | Expected type: `string`, `number`, `integer`, `boolean`, `datetime` |
| `required` | Whether the field must be present and non-null |
| `allowed` | List of permitted values |
| `min`, `max` | Numeric bounds |
| `min_length` | Minimum string length |
| `regex` | Pattern the string must match |

## Definition versioning

Defacto supports multiple versions of definitions. When you change your definitions and activate a new version, defacto detects what changed and picks the appropriate build mode automatically.

```python
d = Defacto("definitions/")
d.ingest("app", events, process=True)

# Later, register updated definitions
d.definitions.register("v2", updated_definitions)
d.definitions.activate("v2")  # auto-detects build mode and rebuilds
```

For incremental changes, you can use the draft API to modify definitions one operation at a time, with validation after each step:

```python
draft = d.definitions.draft("v2", based_on="v1")
draft.add_property("customer", "lifetime_value", {"type": "number", "default": 0})
draft.validate()    # check for errors
draft.diff()        # see what changed vs v1
draft.impact()      # predicted build mode
draft.register()    # commit to database
```

## Validation

Defacto validates definitions at registration time and logs warnings at startup. Validation catches structural errors (missing states, invalid transitions, type mismatches) and produces warnings for potential issues.

Warnings include:

- **Dead-end states**: states with no outgoing transitions (intentional terminal states are fine, but worth flagging)
- **Unused event types**: events defined in sources that no state handles
- **Write-never-read properties**: properties set by effects but never referenced in guards or computed expressions

For CI/CD pipelines, you can validate definitions without a database:

```python
from defacto import validate_definitions

result = validate_definitions(definitions_dict)
if not result.valid:
    for error in result.errors:
        print(error)
```
