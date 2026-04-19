//! Time rule evaluation — catching entities that stop receiving events.
//!
//! Time rules fire based on duration, not events: inactivity (no events for N days),
//! expiration (N days since creation), state_duration (N days in current state).
//!
//! There are two evaluation paths:
//!
//! 1. **Pre-event** (called by the interpreter): before interpreting any event for
//!    an entity, check if time rules have fired since the last event. This ensures
//!    entity state is correct regardless of tick frequency. Correctness guarantee.
//!
//! 2. **Periodic tick** (this module's public API): scan all entities with active
//!    time rules and fire any whose thresholds have been crossed. This catches
//!    entities that will never receive another event. Completeness guarantee.
//!
//! Both paths use the same `evaluate_time_rules()` function — one implementation,
//! one set of tests, changes propagate to both automatically.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use rayon::prelude::*;

use crate::definitions::{
    CompiledDefinitions, CompiledEntity, CompiledSetSource, EffectSpec, TimeRuleType,
};
use crate::error::DefactoError;
use crate::expression::{self, evaluator::EvalContext};
use crate::store::Store;
use crate::types::{EntitySnapshot, EntityState, Value};

// ===========================================================================
// Public API
// ===========================================================================

/// Evaluate time rules for ALL entities at the given timestamp.
///
/// Scans the store for entities with active time rules whose thresholds
/// have been crossed. Produces effects (typically state transitions),
/// updates entity state in the store, and returns snapshots for entities
/// that changed.
///
/// Called periodically by the Python orchestration layer (`m.tick()`).
pub fn tick(
    as_of: DateTime<Utc>,
    store: &Store,
    definitions: &CompiledDefinitions,
) -> Result<Vec<EntitySnapshot>, DefactoError> {
    // Collect entity IDs + types — we need to iterate without holding DashMap refs
    // during mutation (DashMap allows concurrent access but not iter + modify)
    let entities: Vec<(String, String)> = store
        .entities
        .iter()
        .map(|entry| {
            let state = entry.value();
            (state.entity_id.clone(), state.entity_type.clone())
        })
        .collect();

    // Process entities in parallel — Rayon decides if parallelism is worth it
    let results: Vec<Result<Option<EntitySnapshot>, DefactoError>> = entities
        .par_iter()
        .map(|(entity_id, entity_type)| {
            tick_one_entity(entity_id, entity_type, as_of, store, definitions)
        })
        .collect();

    let mut snapshots = Vec::new();
    for result in results {
        if let Some(snapshot) = result? {
            snapshots.push(snapshot);
        }
    }

    Ok(snapshots)
}

/// Evaluate time rules for specific entities only.
///
/// Used for targeted evaluation — e.g., checking a subset of entities
/// rather than scanning the entire store. Same logic as `tick()` but
/// scoped to the given entity IDs.
pub fn tick_entities(
    entity_ids: &[String],
    as_of: DateTime<Utc>,
    store: &Store,
    definitions: &CompiledDefinitions,
) -> Result<Vec<EntitySnapshot>, DefactoError> {
    let results: Vec<Result<Option<EntitySnapshot>, DefactoError>> = entity_ids
        .par_iter()
        .filter_map(|entity_id| {
            let entity_type = store
                .get_entity(entity_id)
                .map(|state| state.entity_type.clone())?;
            Some(tick_one_entity(entity_id, &entity_type, as_of, store, definitions))
        })
        .collect();

    let mut snapshots = Vec::new();
    for result in results {
        if let Some(snapshot) = result? {
            snapshots.push(snapshot);
        }
    }

    Ok(snapshots)
}

// ===========================================================================
// Shared time rule evaluation — used by both tick and interpreter
// ===========================================================================

/// Evaluate time rules for a single entity and apply effects.
///
/// Checks each time rule in the entity's current state against `as_of`.
/// If a threshold has been crossed, applies the rule's effects (typically
/// a state transition). Returns whether anything changed.
///
/// This is the single implementation of time rule logic — called by both
/// the periodic tick sweep and the interpreter's pre-event check. Having
/// one function ensures both paths evaluate identically.
pub fn evaluate_time_rules(
    entity: &mut EntityState,
    as_of: DateTime<Utc>,
    entity_def: &CompiledEntity,
) -> Result<bool, DefactoError> {
    let mut changed = false;

    let state = match entity_def.states.get(&entity.state) {
        Some(s) => s,
        None => return Ok(false),
    };

    for rule in &state.time_rules {
        // Check if the threshold has been crossed, and compute the exact
        // moment it crossed (reference_time + threshold). The crossing time
        // is used as the transition timestamp — not as_of (when we checked).
        // This ensures SCD history records when the state actually changed,
        // not when tick happened to run.
        let crossing_time = match rule.rule_type {
            TimeRuleType::Inactivity => {
                // Time since last event > threshold
                entity.last_event_time.and_then(|last| {
                    let elapsed = as_of.signed_duration_since(last);
                    if elapsed.to_std().unwrap_or_default() > rule.threshold {
                        Some(last + chrono::Duration::from_std(rule.threshold).unwrap_or_default())
                    } else {
                        None
                    }
                })
            }
            TimeRuleType::Expiration => {
                // Time since entity creation > threshold
                entity.created_time.and_then(|created| {
                    let elapsed = as_of.signed_duration_since(created);
                    if elapsed.to_std().unwrap_or_default() > rule.threshold {
                        Some(created + chrono::Duration::from_std(rule.threshold).unwrap_or_default())
                    } else {
                        None
                    }
                })
            }
            TimeRuleType::StateDuration => {
                // Time in current state > threshold
                entity.state_entered_time.and_then(|entered| {
                    let elapsed = as_of.signed_duration_since(entered);
                    if elapsed.to_std().unwrap_or_default() > rule.threshold {
                        Some(entered + chrono::Duration::from_std(rule.threshold).unwrap_or_default())
                    } else {
                        None
                    }
                })
            }
        };

        if let Some(transition_time) = crossing_time {
            // Apply time rule effects directly to entity state.
            //
            // Time rules support Transition and Set effects. Create and Relate
            // don't make sense for time rules (the entity already exists, and
            // relationships require event data for identity resolution).
            for compiled_effect in &rule.effects {
                match &compiled_effect.spec {
                    EffectSpec::Transition { to } => {
                        entity.state = to.clone();
                        entity.state_entered_time = Some(transition_time);
                        changed = true;
                    }
                    EffectSpec::Set { property, source } => {
                        let value = resolve_time_rule_source(
                            source, entity, &entity_def.name,
                        )?;
                        entity.properties.insert(property.clone(), value);
                        changed = true;
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(changed)
}

// ===========================================================================
// Internal helpers
// ===========================================================================

/// Process time rules for one entity: read from store, evaluate, write back.
fn tick_one_entity(
    entity_id: &str,
    entity_type: &str,
    as_of: DateTime<Utc>,
    store: &Store,
    definitions: &CompiledDefinitions,
) -> Result<Option<EntitySnapshot>, DefactoError> {
    let entity_def = match definitions.entities.get(entity_type) {
        Some(def) => def,
        None => return Ok(None),
    };

    let mut entity = match store.get_entity(entity_id) {
        Some(state) => state,
        None => return Ok(None),
    };

    let changed = evaluate_time_rules(&mut entity, as_of, entity_def)?;

    if !changed {
        return Ok(None);
    }

    // Use state_entered_time as last_event_time for the snapshot — it was
    // set to the actual threshold crossing time by evaluate_time_rules.
    // This ensures the SCD row's valid_from reflects when the state
    // actually changed, not when tick happened to run.
    entity.last_event_time = entity.state_entered_time;

    let snapshot = EntitySnapshot {
        entity_id: entity.entity_id.clone(),
        entity_type: entity.entity_type.clone(),
        state: entity.state.clone(),
        properties: entity.properties.clone(),
        relationships: Vec::new(),
        last_event_time: entity.last_event_time,
        state_entered_time: entity.state_entered_time,
        created_time: entity.created_time,
    };
    store.update_entity(entity);
    Ok(Some(snapshot))
}

/// Resolve a set effect source in a time rule context.
///
/// Time rules have no event data — `event.*` references resolve to Null.
/// `entity.*` references work normally against current entity properties.
fn resolve_time_rule_source(
    source: &CompiledSetSource,
    entity: &EntityState,
    entity_name: &str,
) -> Result<Value, DefactoError> {
    match source {
        CompiledSetSource::FromField(path) => {
            // Only entity.* makes sense — event.* returns Null (no event)
            if path.len() >= 2 && path[0] == "entity" {
                Ok(entity.properties.get(&path[1]).cloned().unwrap_or(Value::Null))
            } else {
                Ok(Value::Null)
            }
        }
        CompiledSetSource::Literal(v) => Ok(v.clone()),
        CompiledSetSource::Compute(expr) => {
            let empty: HashMap<String, Value> = HashMap::new();
            let ctx = EvalContext::full(&empty, &entity.properties);
            expression::evaluator::evaluate(expr, &ctx).map_err(|e| {
                DefactoError::Build(format!(
                    "Entity '{entity_name}': time rule compute expression failed: {e}"
                ))
            })
        }
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::definitions::*;
    use std::time::Duration;

    fn make_entity(id: &str, state: &str, last_event: DateTime<Utc>) -> EntityState {
        EntityState {
            entity_id: id.into(),
            entity_type: "customer".into(),
            state: state.into(),
            properties: HashMap::from([("mrr".into(), Value::Int(99))]),
            last_event_time: Some(last_event),
            state_entered_time: Some(last_event),
            created_time: Some(last_event),
        }
    }

    fn entity_def_with_inactivity(threshold_days: u64) -> CompiledEntity {
        let mut active_handlers = HashMap::new();
        let active_state = CompiledState {
            handlers: active_handlers,
            time_rules: vec![CompiledTimeRule {
                rule_type: TimeRuleType::Inactivity,
                threshold: Duration::from_secs(threshold_days * 86400),
                effects: vec![CompiledEffect {
                    spec: EffectSpec::Transition { to: "churned".into() },
                    condition: None,
                }],
            }],
        };

        let mut states = HashMap::new();
        states.insert("active".into(), active_state);
        states.insert("churned".into(), CompiledState {
            handlers: HashMap::new(),
            time_rules: Vec::new(),
        });

        CompiledEntity {
            name: "customer".into(),
            starts: "active".into(),
            states,
            properties: HashMap::from([("mrr".into(), CompiledProperty {
                prop_type: "number".into(),
                default: Some(Value::Int(0)),
                sensitive: None, treatment: None, allowed: None,
                min: None, max: None, compute: None,
            })]),
            identity: Vec::new(),
            relationships: Vec::new(),
            always_handlers: HashMap::new(),
        }
    }

    // ── evaluate_time_rules tests ──

    #[test]
    fn inactivity_threshold_crossed() {
        let now = Utc::now();
        let day_1 = now - chrono::Duration::days(120);
        let def = entity_def_with_inactivity(90);
        let mut entity = make_entity("c1", "active", day_1);

        let changed = evaluate_time_rules(&mut entity, now, &def).unwrap();
        assert!(changed);
        assert_eq!(entity.state, "churned");
    }

    #[test]
    fn inactivity_threshold_not_crossed() {
        let now = Utc::now();
        let recent = now - chrono::Duration::days(30);
        let def = entity_def_with_inactivity(90);
        let mut entity = make_entity("c1", "active", recent);

        let changed = evaluate_time_rules(&mut entity, now, &def).unwrap();
        assert!(!changed);
        assert_eq!(entity.state, "active");
    }

    #[test]
    fn expiration_threshold_crossed() {
        let now = Utc::now();
        let day_1 = now - chrono::Duration::days(400);
        let mut def = entity_def_with_inactivity(90);
        def.states.get_mut("active").unwrap().time_rules = vec![CompiledTimeRule {
            rule_type: TimeRuleType::Expiration,
            threshold: Duration::from_secs(365 * 86400),
            effects: vec![CompiledEffect {
                spec: EffectSpec::Transition { to: "churned".into() },
                condition: None,
            }],
        }];
        let mut entity = make_entity("c1", "active", day_1);

        let changed = evaluate_time_rules(&mut entity, now, &def).unwrap();
        assert!(changed);
        assert_eq!(entity.state, "churned");
    }

    #[test]
    fn state_duration_threshold_crossed() {
        let now = Utc::now();
        let day_1 = now - chrono::Duration::days(20);
        let mut def = entity_def_with_inactivity(90);
        def.states.get_mut("active").unwrap().time_rules = vec![CompiledTimeRule {
            rule_type: TimeRuleType::StateDuration,
            threshold: Duration::from_secs(14 * 86400),
            effects: vec![CompiledEffect {
                spec: EffectSpec::Transition { to: "churned".into() },
                condition: None,
            }],
        }];
        let mut entity = make_entity("c1", "active", day_1);

        let changed = evaluate_time_rules(&mut entity, now, &def).unwrap();
        assert!(changed);
        assert_eq!(entity.state, "churned");
    }

    #[test]
    fn no_time_rules_no_change() {
        let now = Utc::now();
        let mut def = entity_def_with_inactivity(90);
        def.states.get_mut("active").unwrap().time_rules.clear();
        let mut entity = make_entity("c1", "active", now);

        let changed = evaluate_time_rules(&mut entity, now, &def).unwrap();
        assert!(!changed);
    }

    // ── tick tests ──

    #[test]
    fn tick_scans_all_entities() {
        let now = Utc::now();
        let day_1 = now - chrono::Duration::days(120);
        let def = entity_def_with_inactivity(90);
        let mut defs_map = HashMap::new();
        defs_map.insert("customer".into(), def);
        let definitions = CompiledDefinitions {
            entities: defs_map,
            sources: HashMap::new(),
            schemas: HashMap::new(),
            definition_hash: String::new(),
            source_hash: String::new(),
            identity_hash: String::new(),
        };

        let store = Store::new();
        store.update_entity(make_entity("c1", "active", day_1));
        store.update_entity(make_entity("c2", "active", now)); // recent, won't churn
        store.take_dirty(); // clear setup dirty flags

        let snapshots = tick(now, &store, &definitions).unwrap();

        // Only c1 should have churned (120 days > 90 days)
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].entity_id, "c1");
        assert_eq!(snapshots[0].state, "churned");

        // Store should be updated
        assert_eq!(store.get_entity("c1").unwrap().state, "churned");
        assert_eq!(store.get_entity("c2").unwrap().state, "active");
    }

    #[test]
    fn tick_entities_scoped() {
        let now = Utc::now();
        let day_1 = now - chrono::Duration::days(120);
        let def = entity_def_with_inactivity(90);
        let mut defs_map = HashMap::new();
        defs_map.insert("customer".into(), def);
        let definitions = CompiledDefinitions {
            entities: defs_map,
            sources: HashMap::new(),
            schemas: HashMap::new(),
            definition_hash: String::new(),
            source_hash: String::new(),
            identity_hash: String::new(),
        };

        let store = Store::new();
        store.update_entity(make_entity("c1", "active", day_1));
        store.update_entity(make_entity("c2", "active", day_1));
        store.take_dirty();

        // Only check c1
        let snapshots = tick_entities(
            &["c1".into()], now, &store, &definitions,
        ).unwrap();

        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].entity_id, "c1");
        // c2 was NOT checked
        assert_eq!(store.get_entity("c2").unwrap().state, "active");
    }

    #[test]
    fn tick_empty_store() {
        let definitions = CompiledDefinitions {
            entities: HashMap::new(),
            sources: HashMap::new(),
            schemas: HashMap::new(),
            definition_hash: String::new(),
            source_hash: String::new(),
            identity_hash: String::new(),
        };
        let store = Store::new();

        let snapshots = tick(Utc::now(), &store, &definitions).unwrap();
        assert!(snapshots.is_empty());
    }

    #[test]
    fn tick_no_changes() {
        let now = Utc::now();
        let def = entity_def_with_inactivity(90);
        let mut defs_map = HashMap::new();
        defs_map.insert("customer".into(), def);
        let definitions = CompiledDefinitions {
            entities: defs_map,
            sources: HashMap::new(),
            schemas: HashMap::new(),
            definition_hash: String::new(),
            source_hash: String::new(),
            identity_hash: String::new(),
        };

        let store = Store::new();
        store.update_entity(make_entity("c1", "active", now)); // recent
        store.take_dirty();

        let snapshots = tick(now, &store, &definitions).unwrap();
        assert!(snapshots.is_empty());
    }
}
