//! Core types shared across all engine modules.
//!
//! These types define the data that flows through the system: entity state,
//! normalized events, entity snapshots, and effects. Every module depends
//! on these — they are the shared vocabulary of the engine.

use std::collections::HashMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Value — the universal property type
// ---------------------------------------------------------------------------

/// Core value type used throughout the engine.
///
/// Every entity property, event field, and expression result is a Value.
/// Maps directly to Python types via PyO3: String↔str, Float↔float,
/// Int↔int, Bool↔bool, DateTime↔datetime, Null↔None.
///
/// Duration and Array are used within expressions and event data but
/// don't map to Python types directly — they're converted to appropriate
/// Python representations (timedelta, list) during PyO3 crossing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    String(String),
    Float(f64),
    Int(i64),
    Bool(bool),
    /// Timestamp — used for datetime properties, event timestamps, and now().
    DateTime(DateTime<Utc>),
    /// Duration literal — 30d, 24h, 90m, 45s. Used in datetime arithmetic:
    /// DateTime + Duration → DateTime, DateTime - DateTime → Duration.
    Duration(Duration),
    /// Array of values — used for event fields that contain lists (tags, items).
    /// Accessed via numeric dot notation: event.items.0.product_id.
    Array(Vec<Value>),
    Null,
}

// ---------------------------------------------------------------------------
// Entity state — what lives in the DashMap
// ---------------------------------------------------------------------------

/// Current state of a single entity in the in-memory store.
///
/// One EntityState per entity in the DashMap. Updated during interpretation
/// as events produce effects. The timestamps track when the entity last
/// received an event and when it entered its current state — both are
/// needed for time rule evaluation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EntityState {
    pub entity_id: String,
    pub entity_type: String,
    /// Current position in the state machine (e.g., "active", "churned").
    pub state: String,
    /// Property values. Keys match the entity definition's property names.
    pub properties: HashMap<String, Value>,
    /// When this entity last received an event. Used by inactivity time rules.
    pub last_event_time: Option<DateTime<Utc>>,
    /// When the entity entered its current state. Used by state_duration time rules.
    pub state_entered_time: Option<DateTime<Utc>>,
    /// When this entity was first created. Used by expiration time rules.
    pub created_time: Option<DateTime<Utc>>,
}

// ---------------------------------------------------------------------------
// Normalized event — held in Rust between normalize and interpret
// ---------------------------------------------------------------------------

/// An event after normalization but before interpretation.
///
/// Raw input has been transformed through source mappings: fields extracted,
/// types coerced, timestamps parsed, event_id computed. The raw input is
/// preserved for the ledger write. Resolution hints are extracted for
/// identity resolution.
///
/// These are held in Rust memory between the normalize() and interpret()
/// calls — they never cross back to Python, avoiding unnecessary PyO3
/// conversion overhead.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NormalizedEvent {
    /// Content-based hash for deduplication.
    pub event_id: String,
    /// Event time from the source (not ingestion time).
    pub timestamp: DateTime<Utc>,
    /// Which source produced this event (e.g., "app", "stripe").
    pub source: String,
    /// Normalized event type (e.g., "customer_signup").
    pub event_type: String,
    /// Normalized event fields after mapping, coercion, and compute.
    pub data: HashMap<String, Value>,
    /// Original input from the source, preserved for the ledger.
    pub raw: String,
    /// Identity hints extracted during normalization.
    /// Key = entity type (e.g., "customer"), value = (field_name, hint_value) pairs
    /// (e.g., [("email", "alice@test.com"), ("phone", "555-1234")]).
    pub resolution_hints: HashMap<String, Vec<(String, String)>>,
}

// ---------------------------------------------------------------------------
// Entity snapshot — returned after interpretation
// ---------------------------------------------------------------------------

/// Full entity state after effects have been applied.
///
/// This is what gets published to Kafka and written to state history.
/// It's a complete snapshot — not a delta — so downstream consumers
/// are stateless. They receive the full picture and write it directly
/// as a new SCD Type 2 row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EntitySnapshot {
    pub entity_id: String,
    pub entity_type: String,
    pub state: String,
    pub properties: HashMap<String, Value>,
    /// Relationships that changed in this event (not all relationships).
    pub relationships: Vec<RelationshipSnapshot>,
    /// When this entity last received an event. Flows to state history for
    /// cold start recovery and analytics ("when was this entity last active?").
    pub last_event_time: Option<DateTime<Utc>>,
    /// When the entity entered its current state. Flows to state history for
    /// cold start recovery and "how long in this state?" queries.
    pub state_entered_time: Option<DateTime<Utc>>,
    /// When this entity was first created. Flows to state history for
    /// cold start recovery and entity age queries.
    pub created_time: Option<DateTime<Utc>>,
}

/// A relationship included in an entity snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RelationshipSnapshot {
    pub relationship_type: String,
    pub target_entity_id: String,
    pub properties: HashMap<String, Value>,
}

// ---------------------------------------------------------------------------
// Effect — state changes produced by interpretation
// ---------------------------------------------------------------------------

/// A state change produced when a handler's effects are applied.
///
/// Effects are the output of interpretation. They describe what changed,
/// not how to change it — the interpreter applies them to the in-memory
/// EntityState.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Effect {
    /// Create a new entity in its start state. Idempotent.
    Create,
    /// Transition to a new state.
    Transition { to: String },
    /// Set a property to a value.
    Set { property: String, value: Value },
    /// Add a value to a numeric property.
    Increment { property: String, by: Value },
    /// Create or update a relationship.
    Relate {
        relationship_type: String,
        target_entity_id: String,
        properties: HashMap<String, Value>,
    },
}

// ---------------------------------------------------------------------------
// Operators — used in the expression AST
// ---------------------------------------------------------------------------

/// Binary operators in the expression language.
///
/// Used in guards, conditions, computed properties, and normalizations.
/// The expression evaluator dispatches on these to produce a Value result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Op {
    // Comparison — produce Bool
    Eq,
    Neq,
    Gt,
    Lt,
    Gte,
    Lte,
    // Logical — operate on Bool, produce Bool
    And,
    Or,
    // Arithmetic — operate on numeric, produce numeric
    Add,
    Sub,
    Mul,
    Div,
    Pow,
}

/// Unary operators in the expression language.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum UnaryOp {
    /// Logical negation: `not x` or `!x`
    Not,
    /// Arithmetic negation: `-x`
    Negate,
}

// ---------------------------------------------------------------------------
// JSON conversion — shared between definitions compilation and normalization
// ---------------------------------------------------------------------------

/// Convert a `serde_json::Value` to a Defacto `Value`.
///
/// Used during both definition compilation (parsing defaults, allowed values)
/// and event normalization (extracting field values from raw JSON). Living
/// here in `types.rs` avoids duplicating this logic across modules.
///
/// JSON types map as:
/// - string → `Value::String`
/// - integer number → `Value::Int`
/// - float number → `Value::Float`
/// - boolean → `Value::Bool`
/// - null → `Value::Null`
/// - array → `Value::Array` (recursive)
/// - object → `Value::Null` (nested objects accessed via dot-path field references)
/// Convert a Defacto Value to a serde_json::Value for clean JSON serialization.
///
/// Used by the normalizer when building ledger rows — avoids the tagged enum
/// format that serde produces for Value (e.g., `{"String": "alice"}` instead
/// of just `"alice"`).
pub fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::String(s) => serde_json::Value::String(s.clone()),
        Value::Int(i) => serde_json::json!(i),
        Value::Float(f) => serde_json::json!(f),
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Null => serde_json::Value::Null,
        Value::DateTime(dt) => serde_json::Value::String(dt.to_rfc3339()),
        Value::Duration(d) => serde_json::json!(d.as_secs_f64()),
        Value::Array(items) => {
            serde_json::Value::Array(items.iter().map(value_to_json).collect())
        }
    }
}

/// Convert a HashMap<String, Value> to a serde_json::Value object.
///
/// Used for serializing normalized event data in ledger rows.
pub fn values_to_json(map: &HashMap<String, Value>) -> serde_json::Value {
    let obj: serde_json::Map<String, serde_json::Value> = map
        .iter()
        .map(|(k, v)| (k.clone(), value_to_json(v)))
        .collect();
    serde_json::Value::Object(obj)
}

pub fn json_to_value(v: &serde_json::Value) -> Value {
    match v {
        serde_json::Value::String(s) => Value::String(s.clone()),
        serde_json::Value::Number(n) => {
            // Prefer integer if the JSON number has no decimal
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float(f)
            } else {
                Value::Null
            }
        }
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Array(arr) => Value::Array(arr.iter().map(json_to_value).collect()),
        // Nested objects aren't directly representable as Value — they're
        // accessed via dotted field paths in expressions and field mappings.
        serde_json::Value::Object(_) => Value::Null,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_serde_roundtrip() {
        let values = vec![
            Value::String("hello".into()),
            Value::Float(3.14),
            Value::Int(42),
            Value::Bool(true),
            Value::Null,
        ];
        for v in &values {
            let json = serde_json::to_string(v).unwrap();
            let back: Value = serde_json::from_str(&json).unwrap();
            assert_eq!(*v, back);
        }
    }

    #[test]
    fn entity_state_construction() {
        let state = EntityState {
            entity_id: "customer_0001".into(),
            entity_type: "customer".into(),
            state: "active".into(),
            properties: HashMap::from([
                ("mrr".into(), Value::Float(99.0)),
                ("plan".into(), Value::String("pro".into())),
            ]),
            last_event_time: None,
            state_entered_time: None,
            created_time: None,
        };
        assert_eq!(state.entity_id, "customer_0001");
        assert_eq!(state.properties.len(), 2);
    }

    #[test]
    fn normalized_event_construction() {
        let event = NormalizedEvent {
            event_id: "e_abc123".into(),
            timestamp: Utc::now(),
            source: "app".into(),
            event_type: "customer_signup".into(),
            data: HashMap::from([("email".into(), Value::String("alice@test.com".into()))]),
            raw: r#"{"type":"user.created"}"#.into(),
            resolution_hints: HashMap::from([("customer".into(), vec![("email".into(), "alice@test.com".into())])]),
        };
        assert_eq!(event.source, "app");
    }

    #[test]
    fn entity_snapshot_construction() {
        let snapshot = EntitySnapshot {
            entity_id: "customer_0001".into(),
            entity_type: "customer".into(),
            state: "active".into(),
            properties: HashMap::new(),
            relationships: vec![RelationshipSnapshot {
                relationship_type: "placed_order".into(),
                target_entity_id: "order_0042".into(),
                properties: HashMap::from([("total".into(), Value::Float(149.99))]),
            }],
            last_event_time: None,
            state_entered_time: None,
            created_time: None,
        };
        assert_eq!(snapshot.relationships.len(), 1);
    }

    #[test]
    fn effect_variants() {
        let effects = vec![
            Effect::Create,
            Effect::Transition {
                to: "active".into(),
            },
            Effect::Set {
                property: "mrr".into(),
                value: Value::Float(99.0),
            },
            Effect::Increment {
                property: "login_count".into(),
                by: Value::Int(1),
            },
        ];
        assert_eq!(effects.len(), 4);
    }

    #[test]
    fn operators_are_eq() {
        // Op and UnaryOp derive Eq (no f64 fields) so they can be HashMap keys
        assert_eq!(Op::Eq, Op::Eq);
        assert_ne!(Op::Add, Op::Sub);
        assert_eq!(UnaryOp::Not, UnaryOp::Not);
    }
}
