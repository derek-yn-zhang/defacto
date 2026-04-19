//! Compiled entity definitions — state machines, handlers, properties.
//!
//! An entity definition describes one entity type: its lifecycle (states and
//! transitions), properties (typed fields), identity (how hints resolve),
//! relationships (connections to other entities), and time rules (inactivity,
//! expiration). This is the compiled form — expressions are pre-parsed into
//! ASTs, ready for fast evaluation during interpretation.
//!
//! ## EffectSpec vs Effect
//!
//! This module defines `EffectSpec` — the *definition* form of an effect.
//! It describes what to do and where to get values, but doesn't have the
//! actual values yet (those come from event data at runtime).
//!
//! Compare with `Effect` in `types.rs` — the *runtime* form produced by the
//! interpreter after resolving field references, evaluating expressions, and
//! looking up entity state. The interpreter reads `EffectSpec` instructions +
//! event/entity data → produces `Effect` results.
//!
//! ```text
//! Compile time:  EffectSpec::Set { property: "mrr", source: FromField(["event", "mrr"]) }
//! Runtime:       Effect::Set { property: "mrr", value: Value::Float(99.0) }
//! ```

use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::expression::Expression;
use crate::types::Value;

/// A compiled entity definition — everything the interpreter needs
/// to process events for this entity type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompiledEntity {
    /// Entity type name (e.g., "customer").
    pub name: String,
    /// Initial state for newly created entities (e.g., "lead").
    pub starts: String,
    /// State machine definition. Key = state name.
    pub states: HashMap<String, CompiledState>,
    /// Entity properties with types, defaults, and constraints.
    pub properties: HashMap<String, CompiledProperty>,
    /// Identity resolution configuration — which fields identify this entity.
    pub identity: Vec<CompiledIdentityField>,
    /// Relationships to other entity types.
    pub relationships: Vec<CompiledRelationship>,
    /// Handlers that fire in ANY state. Key = event type.
    pub always_handlers: HashMap<String, CompiledHandler>,
}

/// A single state in the entity's state machine.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompiledState {
    /// Event handlers for this state. Key = event type name.
    /// "When in this state and this event arrives, do these things."
    pub handlers: HashMap<String, CompiledHandler>,
    /// Time-based rules. "After this duration in this state, do these things."
    pub time_rules: Vec<CompiledTimeRule>,
}

/// An event handler — a guard expression and a list of effects.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompiledHandler {
    /// Optional guard expression. If present, must evaluate to true for
    /// the handler to fire. Example: `event.plan != entity.plan`.
    pub guard: Option<Expression>,
    /// Effects to apply when this handler fires.
    pub effects: Vec<CompiledEffect>,
}

/// A single effect within a handler, with an optional condition.
///
/// The condition is evaluated at interpretation time — if false, this
/// individual effect is skipped (other effects in the handler still fire).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompiledEffect {
    /// What to do — the instruction for the interpreter.
    pub spec: EffectSpec,
    /// When to do it — if present, the effect only fires when this
    /// evaluates to true. Example: `event.score > entity.high_score`.
    pub condition: Option<Expression>,
}

// ---------------------------------------------------------------------------
// EffectSpec — the definition form of an effect (compile-time instructions)
// ---------------------------------------------------------------------------

/// How a set effect gets its value — resolved at interpretation time.
///
/// At compile time, we know WHERE the value comes from but not WHAT it is
/// (except for literals). The interpreter uses this to resolve the actual
/// value from event data or entity state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CompiledSetSource {
    /// Field path reference: `"event.email"` → `["event", "email"]`.
    /// Resolved by looking up the dot-separated path in the event data
    /// or entity state at interpretation time.
    FromField(Vec<String>),
    /// Literal value known at compile time: `value: 42`.
    /// No runtime resolution needed — the value is used directly.
    Literal(Value),
    /// Expression evaluated at interpretation time: `"entity.mrr * 12"`.
    /// Pre-parsed into an AST during compilation for fast evaluation.
    Compute(Expression),
}

/// What a definition says an effect should do — instructions for the interpreter.
///
/// This is the DEFINITION form of an effect. It describes what to do and where
/// to get values, but doesn't have the actual values yet (those come from event
/// data and entity state at runtime).
///
/// Compare with [`Effect`](crate::types::Effect) which is the RUNTIME form —
/// what the interpreter actually produced after resolving values. The interpreter
/// reads `EffectSpec` + event/entity data → produces `Effect`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum EffectSpec {
    /// Create a new entity in its start state. Idempotent — no-op if entity exists.
    Create,
    /// Transition to a new state.
    Transition { to: String },
    /// Set a property. The value source is resolved at interpretation time.
    Set {
        property: String,
        source: CompiledSetSource,
    },
    /// Increment a numeric property by a fixed amount.
    Increment { property: String, by: Value },
    /// Create a relationship. The target entity ID is unknown at compile time —
    /// it's resolved via identity resolution at interpretation time using the hints.
    Relate {
        relationship_type: String,
        target_entity_type: String,
        hints: HashMap<String, Vec<String>>,
    },
}

/// A property definition — type, default, constraints, sensitivity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompiledProperty {
    /// Property type: "string", "number", "integer", "boolean", "datetime".
    pub prop_type: String,
    /// Default value for new entities.
    pub default: Option<Value>,
    /// Sensitivity label: "pii", "phi", "pci". Used for erase/redaction.
    pub sensitive: Option<String>,
    /// Built-in treatment: "hash", "mask", "redact".
    pub treatment: Option<String>,
    /// Allowed values — validation constraint.
    pub allowed: Option<Vec<Value>>,
    /// Minimum numeric value.
    pub min: Option<f64>,
    /// Maximum numeric value.
    pub max: Option<f64>,
    /// Computed property expression — value is derived, not set directly.
    pub compute: Option<Expression>,
}

/// Configuration for one identity field (e.g., email, phone).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompiledIdentityField {
    /// Field name used in resolution hints (e.g., "email").
    pub field_name: String,
    /// Optional normalization expression applied before matching.
    /// Example: `str::to_lowercase(value)`.
    pub normalize: Option<Expression>,
    /// Match strategy: "exact" or "case_insensitive".
    pub match_strategy: String,
}

/// A time-based rule that fires after a duration threshold.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompiledTimeRule {
    /// Rule type: determines what timestamp the threshold is measured from.
    pub rule_type: TimeRuleType,
    /// Duration threshold (e.g., 90 days for inactivity).
    pub threshold: Duration,
    /// Effects to apply when the threshold is crossed.
    pub effects: Vec<CompiledEffect>,
}

/// What kind of time rule — determines the reference timestamp.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeRuleType {
    /// No events received for the threshold duration. Measured from last_event_time.
    Inactivity,
    /// Time since entity creation. Measured from created_time.
    Expiration,
    /// Time in the current state. Measured from state_entered_time.
    StateDuration,
}

/// A relationship to another entity type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompiledRelationship {
    /// Relationship type name (e.g., "placed_order").
    pub rel_type: String,
    /// Target entity type name (e.g., "order").
    pub target: String,
    /// Cardinality: "has_many", "has_one", "belongs_to", "many_to_many".
    pub cardinality: String,
    /// Typed properties on the relationship edge.
    pub properties: HashMap<String, CompiledProperty>,
}
