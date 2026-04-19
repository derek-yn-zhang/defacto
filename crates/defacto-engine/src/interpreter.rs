//! Event interpretation — events + entity state → effects → snapshots.
//!
//! The interpreter is the core state machine evaluator. It processes normalized
//! events through compiled entity definitions, applying effects to entity state
//! and producing snapshots of the resulting state.
//!
//! ## Processing model
//!
//! Events are grouped by entity_id and processed in parallel across entities
//! (via Rayon), but sequentially within each entity (event ordering matters).
//!
//! For each event hitting an entity:
//! 1. Check pre-event time rules — have any thresholds been crossed since
//!    the last event? If yes, apply those effects first (correctness guarantee).
//! 2. Find handlers — both state-specific and `always` handlers. Both fire
//!    if both match, with state effects first then always effects.
//! 3. Evaluate guard expressions — if a handler's guard fails, skip it.
//! 4. Apply effects sequentially — each effect modifies state before the
//!    next is evaluated (effects are composable).
//!
//! ## Snapshots
//!
//! One snapshot per state change — every event that modifies an entity produces
//! a snapshot with that event's timestamp. Only events that actually change state
//! produce snapshots (guards that fail, effects that are no-ops). Unchanged
//! entities (all guards failed) are skipped.
//!
//! ## EffectSpec → Effect
//!
//! This module bridges the compile-time `EffectSpec` (what the definition says
//! to do) and the runtime `Effect` (what actually happened). The interpreter
//! resolves field references, evaluates compute expressions, and applies the
//! resulting values to entity state. See `apply_effect()` for the bridge logic.

use std::collections::HashMap;

use rayon::prelude::*;

use crate::definitions::{
    CompiledDefinitions, CompiledEntity, CompiledHandler, CompiledSetSource, EffectSpec,
};
use crate::error::DefactoError;
use crate::expression::{self, evaluator::EvalContext};
use crate::store::Store;
use crate::types::{EntitySnapshot, EntityState, NormalizedEvent, RelationshipSnapshot, Value};

// ===========================================================================
// Public API
// ===========================================================================

/// Result of interpreting a batch — successes and failures separated.
///
/// Mirrors `NormalizeResult`: good entities produce snapshots, failed entities
/// produce failures with context. The batch never fails as a whole — partial
/// success is always returned.
pub struct InterpretResult {
    pub snapshots: Vec<EntitySnapshot>,
    pub failures: Vec<InterpretFailure>,
}

/// A single entity that failed interpretation.
///
/// Carries the entity identity and error message so the caller can route
/// the failure to a dead letter sink with enough context to debug.
pub struct InterpretFailure {
    pub entity_id: String,
    pub entity_type: String,
    pub error: String,
}

/// Interpret a batch of normalized events against entity state machines.
///
/// `entity_mapping` connects events (by index into `events`) to their resolved
/// entity IDs and entity types. The mapping comes from identity resolution
/// (done in Python) between the normalize and interpret steps.
///
/// Returns `InterpretResult` with snapshots (entities that succeeded) and
/// failures (entities that errored). Failed entities are skipped — their
/// state in the store is unchanged. Successful entities get their state
/// updates and produce snapshots for publishing.
pub fn interpret(
    events: &[NormalizedEvent],
    entity_mapping: &[(usize, String, String)], // (event_index, entity_id, entity_type)
    store: &Store,
    definitions: &CompiledDefinitions,
) -> InterpretResult {
    // Group events by (entity_id, entity_type), preserving event order within each group
    let groups = group_by_entity(events, entity_mapping);

    // Process each entity group in parallel — Rayon decides whether to actually
    // parallelize based on the number of groups and available threads.
    // Each result is either a snapshot (Ok) or an InterpretFailure (Err).
    // Entity ID/type are only cloned on the error path to avoid unnecessary
    // allocations when everything succeeds (the common case).
    let results: Vec<Result<Vec<EntitySnapshot>, InterpretFailure>> = groups
        .par_iter()
        .map(|((entity_id, entity_type), event_refs)| {
            interpret_entity(entity_id, entity_type, event_refs, store, definitions)
                .map_err(|err| InterpretFailure {
                    entity_id: entity_id.clone(),
                    entity_type: entity_type.clone(),
                    error: err.to_string(),
                })
        })
        .collect();

    // Partition into snapshots and failures — never fail the whole batch
    let mut snapshots = Vec::new();
    let mut failures = Vec::new();
    for result in results {
        match result {
            Ok(entity_snapshots) => snapshots.extend(entity_snapshots),
            Err(failure) => failures.push(failure),
        }
    }

    InterpretResult {
        snapshots,
        failures,
    }
}

// ===========================================================================
// Event grouping
// ===========================================================================

/// Group events by (entity_id, entity_type), preserving order within each group.
///
/// Returns a map from (entity_id, entity_type) → Vec of event references.
/// Events within each group maintain their original batch order — this is
/// critical because event ordering affects state machine transitions.
fn group_by_entity<'a>(
    events: &'a [NormalizedEvent],
    entity_mapping: &[(usize, String, String)],
) -> HashMap<(String, String), Vec<&'a NormalizedEvent>> {
    let mut groups: HashMap<(String, String), Vec<&NormalizedEvent>> = HashMap::new();

    for (event_idx, entity_id, entity_type) in entity_mapping {
        if let Some(event) = events.get(*event_idx) {
            groups
                .entry((entity_id.clone(), entity_type.clone()))
                .or_default()
                .push(event);
        }
    }

    groups
}

// ===========================================================================
// Per-entity interpretation
// ===========================================================================

/// Process all events for a single entity, sequentially.
///
/// Clones entity state from the store (one read), processes all events,
/// writes back once (one write). Returns a snapshot for every event that
/// changes state — the SCD Type 2 history table gets one row per state
/// change, not just the final state.
fn interpret_entity(
    entity_id: &str,
    entity_type: &str,
    events: &[&NormalizedEvent],
    store: &Store,
    definitions: &CompiledDefinitions,
) -> Result<Vec<EntitySnapshot>, DefactoError> {
    let entity_def = definitions.entities.get(entity_type).ok_or_else(|| {
        DefactoError::Build(format!(
            "Entity type '{entity_type}' not found in compiled definitions"
        ))
    })?;

    // Get existing entity state or start with None (will be created by Effect::Create)
    let mut entity = store.get_entity(entity_id);
    let mut changed = false;
    let mut snapshots = Vec::new();

    for event in events {
        // Pre-event time rule check — apply overdue time rules before this event.
        // If time rules fire, snapshot the state change before the event.
        // state_entered_time is set to the actual threshold crossing time
        // by evaluate_time_rules, so we use that as last_event_time for
        // the snapshot's valid_from.
        if let Some(ref mut state) = entity {
            if crate::tick::evaluate_time_rules(state, event.timestamp, entity_def)? {
                changed = true;
                state.last_event_time = state.state_entered_time;
                snapshots.push(build_snapshot(state, Vec::new()));
            }
        }

        // Process the event through the state machine
        let (event_changed, relationships) =
            interpret_event(&mut entity, entity_id, event, entity_def, store)?;

        if event_changed {
            changed = true;
            if let Some(ref mut state) = entity {
                state.last_event_time = Some(event.timestamp);
                evaluate_computed_properties(state, entity_def)?;
                snapshots.push(build_snapshot(state, relationships));
            }
        }
    }

    // Write final state to store (one write regardless of how many snapshots)
    if changed {
        if let Some(state) = entity {
            store.update_entity(state);
        }
    }

    Ok(snapshots)
}

// ===========================================================================
// Per-event interpretation
// ===========================================================================

/// Process one event against entity state.
///
/// Finds matching handlers (state-specific + always), evaluates guards,
/// and applies effects. Both state and always handlers fire if both match —
/// state effects first, then always effects. Each handler's guard is
/// evaluated independently.
///
/// Returns (changed, relationships) — whether entity state was modified,
/// and any relationship snapshots produced by Relate effects.
fn interpret_event(
    entity: &mut Option<EntityState>,
    entity_id: &str,
    event: &NormalizedEvent,
    entity_def: &CompiledEntity,
    store: &Store,
) -> Result<(bool, Vec<RelationshipSnapshot>), DefactoError> {
    let mut changed = false;
    let mut relationships = Vec::new();

    // Collect handlers: state-specific first, then always
    let handlers = find_handlers(entity, event, entity_def);

    for handler in &handlers {
        // Evaluate guard — skip this handler if guard fails.
        // The guard borrows entity immutably via EvalContext. The borrow
        // is released before apply_effect needs mutable access.
        if let Some(ref guard) = handler.guard {
            let ctx = build_eval_context(event, entity);
            let guard_result = expression::evaluator::evaluate(guard, &ctx).map_err(|e| {
                DefactoError::Build(format!(
                    "Entity '{}' state '{}' handler '{}': guard evaluation failed: {e}",
                    entity_def.name,
                    entity.as_ref().map(|s| s.state.as_str()).unwrap_or("(new)"),
                    event.event_type,
                ))
            })?;
            // ctx dropped here — immutable borrow released

            match guard_result {
                Value::Bool(true) => {} // guard passes, continue to effects
                Value::Bool(false) => continue, // guard fails, skip this handler
                other => {
                    return Err(DefactoError::Build(format!(
                        "Entity '{}' handler '{}': guard must evaluate to boolean, got {}",
                        entity_def.name,
                        event.event_type,
                        expression::evaluator::type_name(&other),
                    )));
                }
            }
        }

        // Apply each effect in sequence — effects are composable,
        // each one modifies state before the next is evaluated.
        //
        // The pattern: evaluate condition (borrows entity immutably),
        // release borrow, then apply effect (borrows entity mutably).
        // Context is rebuilt only when property-modifying effects fire.
        for compiled_effect in &handler.effects {
            // Evaluate per-effect condition (if present)
            if let Some(ref condition) = compiled_effect.condition {
                let ctx = build_eval_context(event, entity);
                let cond_result =
                    expression::evaluator::evaluate(condition, &ctx).map_err(|e| {
                        DefactoError::Build(format!(
                            "Entity '{}' effect condition failed: {e}",
                            entity_def.name,
                        ))
                    })?;
                // ctx dropped here — immutable borrow released

                match cond_result {
                    Value::Bool(true) => {}
                    Value::Bool(false) => continue,
                    _ => continue, // non-boolean condition treated as false
                }
            }

            let (effect_changed, rel) =
                apply_effect(entity, entity_id, &compiled_effect.spec, event, entity_def, store)?;

            if effect_changed {
                changed = true;
            }
            if let Some(r) = rel {
                relationships.push(r);
            }
        }
    }

    Ok((changed, relationships))
}

/// Find all handlers that match this event type — state-specific first, then always.
///
/// Both fire if both exist. State effects apply first, then always effects.
/// This matches the semantic meaning of "always" — state-independent effects
/// that happen regardless of which state the entity is in.
fn find_handlers<'a>(
    entity: &Option<EntityState>,
    event: &NormalizedEvent,
    entity_def: &'a CompiledEntity,
) -> Vec<&'a CompiledHandler> {
    let mut handlers = Vec::new();

    // State-specific handler (only if entity exists and has a current state)
    if let Some(ref state) = entity {
        if let Some(compiled_state) = entity_def.states.get(&state.state) {
            if let Some(handler) = compiled_state.handlers.get(&event.event_type) {
                handlers.push(handler);
            }
        }
    } else {
        // Entity doesn't exist yet — check the starts state, but only for
        // handlers that contain a Create effect. Non-create handlers can't
        // operate on a non-existent entity (transitions, sets, etc. all
        // require an existing entity). Without this check, out-of-order
        // events (e.g., upgrade arriving before signup) would error on
        // the first transition effect and poison the entity for the
        // entire batch — blocking the later signup from creating it.
        if let Some(starts_state) = entity_def.states.get(&entity_def.starts) {
            if let Some(handler) = starts_state.handlers.get(&event.event_type) {
                let has_create = handler.effects.iter().any(|e| {
                    matches!(e.spec, EffectSpec::Create)
                });
                if has_create {
                    handlers.push(handler);
                }
            }
        }
    }

    // Always handler (fires regardless of state)
    if let Some(handler) = entity_def.always_handlers.get(&event.event_type) {
        handlers.push(handler);
    }

    handlers
}

// ===========================================================================
// Effect application — the EffectSpec → Effect bridge
// ===========================================================================

/// Apply a single EffectSpec to entity state.
///
/// This is where the compile-time `EffectSpec` (what the definition says) becomes
/// a runtime state change. Field references are resolved against event data and
/// entity properties, compute expressions are evaluated, and the entity state is
/// mutated in place.
///
/// Returns (changed, optional_relationship).
fn apply_effect(
    entity: &mut Option<EntityState>,
    entity_id: &str,
    spec: &EffectSpec,
    event: &NormalizedEvent,
    entity_def: &CompiledEntity,
    store: &Store,
) -> Result<(bool, Option<RelationshipSnapshot>), DefactoError> {
    match spec {
        EffectSpec::Create => {
            if entity.is_none() {
                // Initialize new entity with starts state and default properties
                *entity = Some(create_entity(entity_id, entity_def, event));
                store.created.insert(entity_id.to_string());
                Ok((true, None))
            } else {
                // Entity already exists — Create is idempotent
                Ok((false, None))
            }
        }

        EffectSpec::Transition { to } => {
            let state = entity.as_mut().ok_or_else(|| {
                DefactoError::Build(format!(
                    "Entity '{}': transition to '{to}' but entity doesn't exist \
                     (missing 'create' effect before transition?)",
                    entity_def.name,
                ))
            })?;
            state.state = to.clone();
            state.state_entered_time = Some(event.timestamp);
            Ok((true, None))
        }

        EffectSpec::Set { property, source } => {
            let state = entity.as_mut().ok_or_else(|| {
                DefactoError::Build(format!(
                    "Entity '{}': set property '{property}' but entity doesn't exist",
                    entity_def.name,
                ))
            })?;

            let value = resolve_set_source(source, event, state, &entity_def.name)?;
            state.properties.insert(property.clone(), value);
            Ok((true, None))
        }

        EffectSpec::Increment { property, by } => {
            let state = entity.as_mut().ok_or_else(|| {
                DefactoError::Build(format!(
                    "Entity '{}': increment '{property}' but entity doesn't exist",
                    entity_def.name,
                ))
            })?;

            let current = state
                .properties
                .get(property)
                .cloned()
                .unwrap_or(Value::Int(0));

            let new_value = add_values(&current, by).map_err(|e| {
                DefactoError::Build(format!(
                    "Entity '{}': increment '{property}' failed: {e}",
                    entity_def.name,
                ))
            })?;

            state.properties.insert(property.clone(), new_value);
            Ok((true, None))
        }

        EffectSpec::Relate {
            relationship_type,
            target_entity_type,
            hints,
        } => {
            // Try to resolve the target entity ID from the identity cache.
            // Extract hint values from the event data and look them up,
            // scoped by the target entity type.
            let mut target_entity_id = None;
            for (_hint_entity_type, hint_fields) in hints {
                for field in hint_fields {
                    if let Some(Value::String(hint_value)) = event.data.get(field) {
                        if let Some(resolved_id) = store.get_identity(target_entity_type, hint_value) {
                            target_entity_id = Some(resolved_id);
                            break;
                        }
                    }
                }
                if target_entity_id.is_some() {
                    break;
                }
            }

            if let Some(target_id) = target_entity_id {
                Ok((
                    true,
                    Some(RelationshipSnapshot {
                        relationship_type: relationship_type.clone(),
                        target_entity_id: target_id,
                        properties: HashMap::new(),
                    }),
                ))
            } else {
                // Cache miss — relationship will be resolved on next build
                // when identity cache is fully populated
                Ok((false, None))
            }
        }
    }
}

/// Resolve a `CompiledSetSource` to a concrete `Value`.
///
/// This is where field references (event.email, entity.mrr) and compute
/// expressions (entity.mrr * 12) are resolved against actual data.
fn resolve_set_source(
    source: &CompiledSetSource,
    event: &NormalizedEvent,
    entity: &EntityState,
    entity_name: &str,
) -> Result<Value, DefactoError> {
    match source {
        CompiledSetSource::FromField(path) => {
            Ok(resolve_from_field(path, event, entity))
        }

        CompiledSetSource::Literal(v) => Ok(v.clone()),

        CompiledSetSource::Compute(expr) => {
            let ctx = EvalContext::full(&event.data, &entity.properties);
            expression::evaluator::evaluate(expr, &ctx).map_err(|e| {
                DefactoError::Build(format!(
                    "Entity '{entity_name}': compute expression failed: {e}"
                ))
            })
        }
    }
}

/// Resolve a dot-separated field path against event data or entity properties.
///
/// `["event", "email"]` → look up "email" in event.data
/// `["entity", "mrr"]` → look up "mrr" in entity.properties
/// Missing fields return Value::Null (consistent with expression evaluator).
fn resolve_from_field(
    path: &[String],
    event: &NormalizedEvent,
    entity: &EntityState,
) -> Value {
    if path.len() < 2 {
        return Value::Null;
    }

    let context = &path[0];
    let field = &path[1];

    match context.as_str() {
        "event" => event.data.get(field.as_str()).cloned().unwrap_or(Value::Null),
        "entity" => entity.properties.get(field.as_str()).cloned().unwrap_or(Value::Null),
        _ => Value::Null,
    }
}

// ===========================================================================
// Entity creation and snapshots
// ===========================================================================

/// Initialize a new EntityState with the starts state and default property values.
///
/// Called when an `EffectSpec::Create` fires for an entity that doesn't exist yet.
/// Properties are initialized from the compiled definition's defaults — if a property
/// has `default: 0`, it starts at 0. Properties without defaults start as Null.
fn create_entity(
    entity_id: &str,
    entity_def: &CompiledEntity,
    event: &NormalizedEvent,
) -> EntityState {
    let mut properties = HashMap::new();
    for (prop_name, prop_def) in &entity_def.properties {
        let default = prop_def.default.clone().unwrap_or(Value::Null);
        properties.insert(prop_name.clone(), default);
    }

    EntityState {
        entity_id: entity_id.to_string(),
        entity_type: entity_def.name.clone(),
        state: entity_def.starts.clone(),
        properties,
        last_event_time: None,
        state_entered_time: Some(event.timestamp),
        created_time: Some(event.timestamp),
    }
}

/// Build an EntitySnapshot from final entity state after all events.
///
/// Snapshots are complete — not deltas. They contain the full entity state
/// so downstream consumers (state history, Kafka) are stateless.
fn build_snapshot(
    entity: &EntityState,
    relationships: Vec<RelationshipSnapshot>,
) -> EntitySnapshot {
    EntitySnapshot {
        entity_id: entity.entity_id.clone(),
        entity_type: entity.entity_type.clone(),
        state: entity.state.clone(),
        properties: entity.properties.clone(),
        relationships,
        last_event_time: entity.last_event_time,
        state_entered_time: entity.state_entered_time,
        created_time: entity.created_time,
    }
}

/// Evaluate all computed properties on an entity after effects are applied.
///
/// Computed properties (e.g., `annual_revenue: entity.mrr * 12`) are
/// derived values recalculated after every state change. They're defined
/// on the property with `compute:`, not as effects.
fn evaluate_computed_properties(
    entity: &mut EntityState,
    entity_def: &CompiledEntity,
) -> Result<(), DefactoError> {
    // Collect computed values first (can't borrow entity mutably while reading)
    let mut updates: Vec<(String, Value)> = Vec::new();

    let empty_event: HashMap<String, Value> = HashMap::new();
    for (prop_name, prop_def) in &entity_def.properties {
        if let Some(ref expr) = prop_def.compute {
            let ctx = EvalContext::full(&empty_event, &entity.properties);
            match expression::evaluator::evaluate(expr, &ctx) {
                Ok(value) => updates.push((prop_name.clone(), value)),
                Err(e) => {
                    return Err(DefactoError::Build(format!(
                        "Entity '{}': computed property '{}' failed: {e}",
                        entity.entity_type, prop_name,
                    )));
                }
            }
        }
    }

    for (name, value) in updates {
        entity.properties.insert(name, value);
    }

    Ok(())
}

// ===========================================================================
// Evaluation context + arithmetic helpers
// ===========================================================================

/// Build an expression evaluation context from current event and entity state.
///
/// The context borrows event data and entity properties — no cloning.
/// This is critical for performance: the interpreter evaluates guards,
/// conditions, and compute expressions on every event for every handler.
///
/// Rebuilt after property-modifying effects (Set, Increment, Create) so
/// subsequent conditions and computes see updated values.
fn build_eval_context<'a>(
    event: &'a NormalizedEvent,
    entity: &'a Option<EntityState>,
) -> EvalContext<'a> {
    static EMPTY_PROPS: std::sync::LazyLock<HashMap<String, Value>> =
        std::sync::LazyLock::new(HashMap::new);
    let entity_props = entity
        .as_ref()
        .map(|s| &s.properties)
        .unwrap_or(&EMPTY_PROPS);
    EvalContext::full(&event.data, entity_props)
}

/// Add two Values together (for increment effects).
///
/// Supports Int+Int, Float+Float, and mixed Int+Float (coerces to Float).
fn add_values(a: &Value, b: &Value) -> Result<Value, String> {
    match (a, b) {
        (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a + b)),
        (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a + b)),
        (Value::Int(a), Value::Float(b)) => Ok(Value::Float(*a as f64 + b)),
        (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a + *b as f64)),
        _ => Err(format!(
            "cannot add {} and {} — increment requires numeric values",
            expression::evaluator::type_name(a),
            expression::evaluator::type_name(b),
        )),
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::definitions::*;
    use crate::expression::Expression;
    use crate::types::Op;
    use std::time::Duration;

    // ── Test helpers ──

    /// Build a minimal compiled entity for testing.
    fn test_entity_def() -> CompiledEntity {
        let mut properties = HashMap::new();
        properties.insert(
            "mrr".into(),
            CompiledProperty {
                prop_type: "number".into(),
                default: Some(Value::Int(0)),
                sensitive: None,
                treatment: None,
                allowed: None,
                min: None,
                max: None,
                compute: None,
            },
        );
        properties.insert(
            "plan".into(),
            CompiledProperty {
                prop_type: "string".into(),
                default: Some(Value::String("free".into())),
                sensitive: None,
                treatment: None,
                allowed: None,
                min: None,
                max: None,
                compute: None,
            },
        );

        let mut lead_handlers = HashMap::new();
        lead_handlers.insert(
            "signup".into(),
            CompiledHandler {
                guard: None,
                effects: vec![
                    CompiledEffect {
                        spec: EffectSpec::Create,
                        condition: None,
                    },
                    CompiledEffect {
                        spec: EffectSpec::Set {
                            property: "mrr".into(),
                            source: CompiledSetSource::FromField(vec![
                                "event".into(),
                                "mrr".into(),
                            ]),
                        },
                        condition: None,
                    },
                    CompiledEffect {
                        spec: EffectSpec::Transition {
                            to: "active".into(),
                        },
                        condition: None,
                    },
                ],
            },
        );

        let mut active_handlers = HashMap::new();
        active_handlers.insert(
            "upgrade".into(),
            CompiledHandler {
                guard: Some(Expression::BinaryOp(
                    Box::new(Expression::FieldAccess(vec![
                        "event".into(),
                        "plan".into(),
                    ])),
                    Op::Neq,
                    Box::new(Expression::FieldAccess(vec![
                        "entity".into(),
                        "plan".into(),
                    ])),
                )),
                effects: vec![CompiledEffect {
                    spec: EffectSpec::Set {
                        property: "plan".into(),
                        source: CompiledSetSource::FromField(vec![
                            "event".into(),
                            "plan".into(),
                        ]),
                    },
                    condition: None,
                }],
            },
        );

        let mut states = HashMap::new();
        states.insert(
            "lead".into(),
            CompiledState {
                handlers: lead_handlers,
                time_rules: Vec::new(),
            },
        );
        states.insert(
            "active".into(),
            CompiledState {
                handlers: active_handlers,
                time_rules: Vec::new(),
            },
        );
        states.insert(
            "churned".into(),
            CompiledState {
                handlers: HashMap::new(),
                time_rules: Vec::new(),
            },
        );

        CompiledEntity {
            name: "customer".into(),
            starts: "lead".into(),
            states,
            properties,
            identity: vec![CompiledIdentityField {
                field_name: "email".into(),
                normalize: None,
                match_strategy: "exact".into(),
            }],
            relationships: Vec::new(),
            always_handlers: HashMap::new(),
        }
    }

    fn test_event(event_type: &str, data: HashMap<String, Value>) -> NormalizedEvent {
        use chrono::Utc;
        NormalizedEvent {
            event_id: "e_test".into(),
            timestamp: Utc::now(),
            source: "app".into(),
            event_type: event_type.into(),
            data,
            raw: "{}".into(),
            resolution_hints: HashMap::new(),
        }
    }

    fn test_definitions(entity_def: CompiledEntity) -> CompiledDefinitions {
        let mut entities = HashMap::new();
        entities.insert(entity_def.name.clone(), entity_def);
        CompiledDefinitions {
            entities,
            sources: HashMap::new(),
            schemas: HashMap::new(),
            definition_hash: String::new(),
            source_hash: String::new(),
            identity_hash: String::new(),
        }
    }

    // ── Effect application tests ──

    #[test]
    fn create_new_entity() {
        let def = test_entity_def();
        let event = test_event("signup", HashMap::from([("mrr".into(), Value::Int(99))]));
        let store = Store::new();
        let mut entity = None;

        let (changed, _) =
            apply_effect(&mut entity, "c1", &EffectSpec::Create, &event, &def, &store).unwrap();

        assert!(changed);
        let state = entity.unwrap();
        assert_eq!(state.state, "lead");
        assert_eq!(state.properties["mrr"], Value::Int(0)); // default, not event value
        assert_eq!(state.properties["plan"], Value::String("free".into()));
    }

    #[test]
    fn create_idempotent() {
        let def = test_entity_def();
        let event = test_event("signup", HashMap::new());
        let store = Store::new();
        let mut entity = Some(create_entity("c1", &def, &event));

        let (changed, _) =
            apply_effect(&mut entity, "c1", &EffectSpec::Create, &event, &def, &store).unwrap();

        assert!(!changed); // no-op — entity already exists
    }

    #[test]
    fn transition_updates_state() {
        let def = test_entity_def();
        let event = test_event("signup", HashMap::new());
        let store = Store::new();
        let mut entity = Some(create_entity("c1", &def, &event));

        let spec = EffectSpec::Transition {
            to: "active".into(),
        };
        let (changed, _) = apply_effect(&mut entity, "c1", &spec, &event, &def, &store).unwrap();

        assert!(changed);
        assert_eq!(entity.unwrap().state, "active");
    }

    #[test]
    fn set_from_event_field() {
        let def = test_entity_def();
        let event = test_event(
            "signup",
            HashMap::from([("mrr".into(), Value::Float(99.0))]),
        );
        let store = Store::new();
        let mut entity = Some(create_entity("c1", &def, &event));

        let spec = EffectSpec::Set {
            property: "mrr".into(),
            source: CompiledSetSource::FromField(vec!["event".into(), "mrr".into()]),
        };
        apply_effect(&mut entity, "c1", &spec, &event, &def, &store).unwrap();

        assert_eq!(entity.unwrap().properties["mrr"], Value::Float(99.0));
    }

    #[test]
    fn set_from_literal() {
        let def = test_entity_def();
        let event = test_event("signup", HashMap::new());
        let store = Store::new();
        let mut entity = Some(create_entity("c1", &def, &event));

        let spec = EffectSpec::Set {
            property: "plan".into(),
            source: CompiledSetSource::Literal(Value::String("pro".into())),
        };
        apply_effect(&mut entity, "c1", &spec, &event, &def, &store).unwrap();

        assert_eq!(
            entity.unwrap().properties["plan"],
            Value::String("pro".into())
        );
    }

    #[test]
    fn set_from_compute() {
        let def = test_entity_def();
        let event = test_event("signup", HashMap::new());
        let store = Store::new();
        let mut entity = Some(create_entity("c1", &def, &event));
        entity.as_mut().unwrap().properties.insert("mrr".into(), Value::Float(99.0));

        let spec = EffectSpec::Set {
            property: "mrr".into(),
            source: CompiledSetSource::Compute(Expression::BinaryOp(
                Box::new(Expression::FieldAccess(vec!["entity".into(), "mrr".into()])),
                Op::Mul,
                Box::new(Expression::Literal(Value::Int(12))),
            )),
        };
        apply_effect(&mut entity, "c1", &spec, &event, &def, &store).unwrap();

        assert_eq!(entity.unwrap().properties["mrr"], Value::Float(1188.0));
    }

    #[test]
    fn increment_numeric() {
        let def = test_entity_def();
        let event = test_event("signup", HashMap::new());
        let store = Store::new();
        let mut entity = Some(create_entity("c1", &def, &event));
        entity.as_mut().unwrap().properties.insert("mrr".into(), Value::Int(10));

        let spec = EffectSpec::Increment {
            property: "mrr".into(),
            by: Value::Int(5),
        };
        apply_effect(&mut entity, "c1", &spec, &event, &def, &store).unwrap();

        assert_eq!(entity.unwrap().properties["mrr"], Value::Int(15));
    }

    // ── Handler tests ──

    #[test]
    fn guard_passes_effects_applied() {
        let def = test_entity_def();
        let store = Store::new();

        let event = test_event(
            "upgrade",
            HashMap::from([("plan".into(), Value::String("pro".into()))]),
        );

        // Create entity in active state with plan=free
        let mut entity = Some(create_entity("c1", &def, &event));
        entity.as_mut().unwrap().state = "active".into();
        entity.as_mut().unwrap().properties.insert("plan".into(), Value::String("free".into()));

        let (changed, _) = interpret_event(&mut entity, "c1", &event, &def, &store).unwrap();
        assert!(changed);
        assert_eq!(entity.unwrap().properties["plan"], Value::String("pro".into()));
    }

    #[test]
    fn guard_fails_no_effects() {
        let def = test_entity_def();
        let store = Store::new();

        // Upgrade event with same plan as entity → guard fails
        let event = test_event(
            "upgrade",
            HashMap::from([("plan".into(), Value::String("free".into()))]),
        );

        let mut entity = Some(create_entity("c1", &def, &event));
        entity.as_mut().unwrap().state = "active".into();
        entity.as_mut().unwrap().properties.insert("plan".into(), Value::String("free".into()));

        let (changed, _) = interpret_event(&mut entity, "c1", &event, &def, &store).unwrap();
        assert!(!changed);
    }

    #[test]
    fn always_handler_fires() {
        let mut def = test_entity_def();
        def.always_handlers.insert(
            "profile_update".into(),
            CompiledHandler {
                guard: None,
                effects: vec![CompiledEffect {
                    spec: EffectSpec::Set {
                        property: "plan".into(),
                        source: CompiledSetSource::FromField(vec!["event".into(), "plan".into()]),
                    },
                    condition: None,
                }],
            },
        );
        let store = Store::new();

        let event = test_event(
            "profile_update",
            HashMap::from([("plan".into(), Value::String("enterprise".into()))]),
        );

        let mut entity = Some(create_entity("c1", &def, &event));
        entity.as_mut().unwrap().state = "active".into();

        let (changed, _) = interpret_event(&mut entity, "c1", &event, &def, &store).unwrap();
        assert!(changed);
        assert_eq!(
            entity.unwrap().properties["plan"],
            Value::String("enterprise".into())
        );
    }

    #[test]
    fn no_handler_no_change() {
        let def = test_entity_def();
        let store = Store::new();

        let event = test_event("unknown_event", HashMap::new());
        let mut entity = Some(create_entity("c1", &def, &event));
        entity.as_mut().unwrap().state = "active".into();

        let (changed, _) = interpret_event(&mut entity, "c1", &event, &def, &store).unwrap();
        assert!(!changed);
    }

    #[test]
    fn effects_composable_condition_sees_previous_effect() {
        // Effect #1 sets mrr=100, Effect #2 has condition mrr > 50 → should pass
        let mut def = test_entity_def();
        let handler = CompiledHandler {
            guard: None,
            effects: vec![
                CompiledEffect {
                    spec: EffectSpec::Set {
                        property: "mrr".into(),
                        source: CompiledSetSource::Literal(Value::Int(100)),
                    },
                    condition: None,
                },
                CompiledEffect {
                    spec: EffectSpec::Set {
                        property: "plan".into(),
                        source: CompiledSetSource::Literal(Value::String("premium".into())),
                    },
                    // This condition should see mrr=100 from the previous effect
                    condition: Some(Expression::BinaryOp(
                        Box::new(Expression::FieldAccess(vec!["entity".into(), "mrr".into()])),
                        Op::Gt,
                        Box::new(Expression::Literal(Value::Int(50))),
                    )),
                },
            ],
        };
        def.states.get_mut("active").unwrap().handlers.insert("test_event".into(), handler);
        let store = Store::new();

        let event = test_event("test_event", HashMap::new());
        let mut entity = Some(create_entity("c1", &def, &event));
        entity.as_mut().unwrap().state = "active".into();

        let (changed, _) = interpret_event(&mut entity, "c1", &event, &def, &store).unwrap();
        assert!(changed);
        let state = entity.unwrap();
        assert_eq!(state.properties["mrr"], Value::Int(100));
        // Condition saw mrr=100 from previous effect → plan was set
        assert_eq!(state.properties["plan"], Value::String("premium".into()));
    }

    #[test]
    fn effects_composable_condition_fails_on_previous_effect() {
        // Effect #1 sets mrr=10, Effect #2 has condition mrr > 50 → should NOT pass
        let mut def = test_entity_def();
        let handler = CompiledHandler {
            guard: None,
            effects: vec![
                CompiledEffect {
                    spec: EffectSpec::Set {
                        property: "mrr".into(),
                        source: CompiledSetSource::Literal(Value::Int(10)),
                    },
                    condition: None,
                },
                CompiledEffect {
                    spec: EffectSpec::Set {
                        property: "plan".into(),
                        source: CompiledSetSource::Literal(Value::String("premium".into())),
                    },
                    condition: Some(Expression::BinaryOp(
                        Box::new(Expression::FieldAccess(vec!["entity".into(), "mrr".into()])),
                        Op::Gt,
                        Box::new(Expression::Literal(Value::Int(50))),
                    )),
                },
            ],
        };
        def.states.get_mut("active").unwrap().handlers.insert("test_event".into(), handler);
        let store = Store::new();

        let event = test_event("test_event", HashMap::new());
        let mut entity = Some(create_entity("c1", &def, &event));
        entity.as_mut().unwrap().state = "active".into();

        let (changed, _) = interpret_event(&mut entity, "c1", &event, &def, &store).unwrap();
        assert!(changed); // mrr was still set
        let state = entity.unwrap();
        assert_eq!(state.properties["mrr"], Value::Int(10));
        // Condition saw mrr=10 → plan was NOT set, still at default
        assert_eq!(state.properties["plan"], Value::String("free".into()));
    }

    // ── Integration tests ──

    #[test]
    fn full_entity_lifecycle() {
        let def = test_entity_def();
        let defs = test_definitions(def);
        let store = Store::new();

        let events = vec![
            test_event(
                "signup",
                HashMap::from([("mrr".into(), Value::Float(99.0))]),
            ),
        ];

        let mapping = vec![(0_usize, "c1".into(), "customer".into())];
        let snapshots = interpret(&events, &mapping, &store, &defs).snapshots;

        assert_eq!(snapshots.len(), 1);
        let snap = &snapshots[0];
        assert_eq!(snap.entity_id, "c1");
        assert_eq!(snap.state, "active"); // created in lead, transitioned to active
        assert_eq!(snap.properties["mrr"], Value::Float(99.0)); // set from event
    }

    #[test]
    fn multiple_events_same_entity() {
        let def = test_entity_def();
        let defs = test_definitions(def);
        let store = Store::new();

        let events = vec![
            test_event(
                "signup",
                HashMap::from([("mrr".into(), Value::Float(50.0))]),
            ),
            test_event(
                "upgrade",
                HashMap::from([("plan".into(), Value::String("pro".into()))]),
            ),
        ];

        let mapping = vec![
            (0_usize, "c1".into(), "customer".into()),
            (1_usize, "c1".into(), "customer".into()),
        ];
        let snapshots = interpret(&events, &mapping, &store, &defs).snapshots;

        // Per-event snapshots: one per state change
        assert_eq!(snapshots.len(), 2);
        // First: signup creates entity and transitions to active (create + set mrr + transition)
        assert_eq!(snapshots[0].state, "active");
        assert_eq!(snapshots[0].properties["mrr"], Value::Float(50.0));
        // Second: upgrade sets plan to pro (state stays active)
        assert_eq!(snapshots[1].state, "active");
        assert_eq!(snapshots[1].properties["plan"], Value::String("pro".into()));
    }

    #[test]
    fn multiple_entities_parallel() {
        let def = test_entity_def();
        let defs = test_definitions(def);
        let store = Store::new();

        let events = vec![
            test_event(
                "signup",
                HashMap::from([("mrr".into(), Value::Float(50.0))]),
            ),
            test_event(
                "signup",
                HashMap::from([("mrr".into(), Value::Float(100.0))]),
            ),
        ];

        let mapping = vec![
            (0_usize, "c1".into(), "customer".into()),
            (1_usize, "c2".into(), "customer".into()),
        ];
        let snapshots = interpret(&events, &mapping, &store, &defs).snapshots;

        assert_eq!(snapshots.len(), 2);
        // Both entities should be in store
        assert!(store.get_entity("c1").is_some());
        assert!(store.get_entity("c2").is_some());
    }

    #[test]
    fn empty_batch() {
        let def = test_entity_def();
        let defs = test_definitions(def);
        let store = Store::new();

        let snapshots = interpret(&[], &[], &store, &defs).snapshots;
        assert!(snapshots.is_empty());
    }

    #[test]
    fn all_guards_fail_no_snapshot() {
        let def = test_entity_def();
        let defs = test_definitions(def.clone());
        let store = Store::new();

        // Put entity in active state with plan=pro
        let event = test_event("signup", HashMap::new());
        let mut entity = create_entity("c1", &def, &event);
        entity.state = "active".into();
        entity.properties.insert("plan".into(), Value::String("pro".into()));
        store.update_entity(entity);
        store.take_dirty(); // clear dirty from setup

        // Upgrade event with same plan → guard fails
        let events = vec![test_event(
            "upgrade",
            HashMap::from([("plan".into(), Value::String("pro".into()))]),
        )];
        let mapping = vec![(0_usize, "c1".into(), "customer".into())];

        let snapshots = interpret(&events, &mapping, &store, &defs).snapshots;
        assert!(snapshots.is_empty()); // no snapshot — nothing changed
    }

    // Time rule tests are in tick.rs — shared evaluate_time_rules() tested there.

    // ── Arithmetic helper test ──

    #[test]
    fn add_values_int() {
        assert_eq!(add_values(&Value::Int(10), &Value::Int(5)).unwrap(), Value::Int(15));
    }

    #[test]
    fn add_values_float() {
        assert_eq!(add_values(&Value::Float(10.0), &Value::Float(5.5)).unwrap(), Value::Float(15.5));
    }

    #[test]
    fn add_values_mixed() {
        assert_eq!(add_values(&Value::Int(10), &Value::Float(5.5)).unwrap(), Value::Float(15.5));
    }

    #[test]
    fn add_values_non_numeric() {
        assert!(add_values(&Value::String("a".into()), &Value::Int(1)).is_err());
    }
}
