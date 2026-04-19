//! Compiled definitions — the optimized internal representation of YAML definitions.
//!
//! When definitions are registered (via `DefactoCore::compile()`), they're parsed
//! from dicts into these compiled structs. Expressions are pre-parsed into ASTs,
//! duration thresholds are parsed into `Duration`, and hashes are computed for
//! change detection.
//!
//! A `CompiledDefinitions` instance is immutable after creation — if definitions
//! change, a new instance is compiled and the old one is replaced atomically.
//!
//! ## Compilation pipeline
//!
//! ```text
//! Python Definitions.to_dict()  →  JSON dict  →  CompiledDefinitions::compile()
//!                                                   ├── compile_entity() × N
//!                                                   │     ├── compile_state()
//!                                                   │     │     ├── compile_handler()
//!                                                   │     │     │     └── compile_effect()
//!                                                   │     │     └── compile_time_rule()
//!                                                   │     ├── compile_property()
//!                                                   │     ├── compile_identity_field()
//!                                                   │     └── compile_relationship()
//!                                                   ├── compile_source() × N
//!                                                   │     └── compile_source_event()
//!                                                   ├── compile_schema() × N
//!                                                   └── compute hashes (SHA-256)
//! ```

pub mod entity;
pub mod schema;
pub mod source;

use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Map;
use sha2::{Digest, Sha256};

pub use self::entity::*;
pub use self::schema::*;
pub use self::source::*;

use crate::error::DefactoError;
use crate::expression::{self, Expression};
use crate::types::{json_to_value, Value};

/// A complete set of compiled definitions for one version.
///
/// Contains all entity definitions, source configurations, and event schemas
/// for a single version (e.g., "v1"). Multiple versions can exist simultaneously
/// (for blue-green deployments), each stored under its version key in DefactoCore.
///
/// The three hashes enable change detection: if any hash differs from the stored
/// build state, the framework knows which build mode is needed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompiledDefinitions {
    /// Compiled entity definitions. Key = entity type name.
    pub entities: HashMap<String, CompiledEntity>,
    /// Compiled source configurations. Key = source name.
    pub sources: HashMap<String, CompiledSource>,
    /// Compiled event schemas. Key = event type name.
    pub schemas: HashMap<String, CompiledSchema>,
    /// Hash of entity definitions — changes trigger FULL rebuild.
    pub definition_hash: String,
    /// Hash of source definitions — changes trigger FULL_RENORMALIZE.
    pub source_hash: String,
    /// Hash of identity configuration — changes trigger FULL_WITH_IDENTITY_RESET.
    pub identity_hash: String,
}

impl CompiledDefinitions {
    /// Compile raw definition dicts into optimized internal representation.
    ///
    /// Parses entity definitions, source configurations, and event schemas.
    /// Pre-compiles all expressions (guards, conditions, computes, normalizations)
    /// into ASTs. Computes the three hashes for change detection.
    ///
    /// Called once per version registration. Not performance-critical.
    pub fn compile(definitions: &serde_json::Value) -> Result<Self, DefactoError> {
        let obj = definitions
            .as_object()
            .ok_or_else(|| DefactoError::Definition("definitions must be a JSON object".into()))?;

        // Compile entities
        let mut entities = HashMap::new();
        if let Some(entities_val) = obj.get("entities") {
            let entities_obj = require_object(entities_val, "entities")?;
            for (name, body) in entities_obj {
                let body_obj = require_object(body, &format!("entity '{name}'"))?;
                entities.insert(name.clone(), compile_entity(name, body_obj)?);
            }
        }

        // Compile sources
        let mut sources = HashMap::new();
        if let Some(sources_val) = obj.get("sources") {
            let sources_obj = require_object(sources_val, "sources")?;
            for (name, body) in sources_obj {
                let body_obj = require_object(body, &format!("source '{name}'"))?;
                sources.insert(name.clone(), compile_source(name, body_obj)?);
            }
        }

        // Compile schemas
        let mut schemas = HashMap::new();
        if let Some(schemas_val) = obj.get("schemas") {
            let schemas_obj = require_object(schemas_val, "schemas")?;
            for (name, body) in schemas_obj {
                let body_obj = require_object(body, &format!("schema '{name}'"))?;
                schemas.insert(name.clone(), compile_schema(name, body_obj)?);
            }
        }

        // Compute hashes from the raw JSON — canonical serialization via BTreeMap (sorted keys)
        let empty = serde_json::json!({});
        let definition_hash = compute_hash(obj.get("entities").unwrap_or(&empty));
        let source_hash = compute_hash(obj.get("sources").unwrap_or(&empty));
        let identity_hash = compute_hash(&extract_identity_config(obj.get("entities")));

        Ok(Self {
            entities,
            sources,
            schemas,
            definition_hash,
            source_hash,
            identity_hash,
        })
    }

    /// Return the three hashes for change detection.
    ///
    /// Used by the Python BuildManager to compare against stored hashes
    /// and determine the minimum build mode needed.
    pub fn hashes(&self) -> (&str, &str, &str) {
        (&self.definition_hash, &self.source_hash, &self.identity_hash)
    }
}

// ===========================================================================
// Helper functions
// ===========================================================================

/// Require a JSON value to be an object, returning a useful error if not.
fn require_object<'a>(
    value: &'a serde_json::Value,
    context: &str,
) -> Result<&'a Map<String, serde_json::Value>, DefactoError> {
    value
        .as_object()
        .ok_or_else(|| DefactoError::Definition(format!("{context}: expected a JSON object")))
}

/// Require a string field from a JSON object.
fn require_str<'a>(
    obj: &'a Map<String, serde_json::Value>,
    field: &str,
    context: &str,
) -> Result<&'a str, DefactoError> {
    obj.get(field)
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            DefactoError::Definition(format!("{context}: missing required string field '{field}'"))
        })
}

/// Parse an optional expression string from a JSON object.
///
/// Returns `Ok(None)` if the field is absent or null, `Ok(Some(expr))` if valid,
/// `Err` if the expression string fails to parse.
fn parse_optional_expr(
    obj: &Map<String, serde_json::Value>,
    field: &str,
    context: &str,
) -> Result<Option<Expression>, DefactoError> {
    match obj.get(field) {
        Some(serde_json::Value::String(s)) => {
            let expr = expression::parser::parse(s).map_err(|e| {
                DefactoError::Definition(format!(
                    "{context}: failed to parse {field} expression '{s}': {e}"
                ))
            })?;
            Ok(Some(expr))
        }
        Some(serde_json::Value::Null) | None => Ok(None),
        Some(other) => Err(DefactoError::Definition(format!(
            "{context}: '{field}' must be a string expression, got {other}"
        ))),
    }
}

/// Parse a duration threshold string like "90d", "24h", "90m", "45s".
///
/// Used for time rule thresholds in entity definitions. These are simple
/// duration literals — not full expressions.
fn parse_duration(s: &str, context: &str) -> Result<Duration, DefactoError> {
    let s = s.trim();
    if s.len() < 2 {
        return Err(DefactoError::Definition(format!(
            "{context}: invalid duration '{s}' — expected format like '90d', '24h', '90m', '45s'"
        )));
    }
    let (digits, unit) = s.split_at(s.len() - 1);
    let n: u64 = digits.parse().map_err(|_| {
        DefactoError::Definition(format!(
            "{context}: invalid duration '{s}' — '{digits}' is not a valid number"
        ))
    })?;
    match unit {
        "d" => Ok(Duration::from_secs(n * 86400)),
        "h" => Ok(Duration::from_secs(n * 3600)),
        "m" => Ok(Duration::from_secs(n * 60)),
        "s" => Ok(Duration::from_secs(n)),
        _ => Err(DefactoError::Definition(format!(
            "{context}: invalid duration unit '{unit}' in '{s}' — expected 'd', 'h', 'm', or 's'"
        ))),
    }
}

/// Compute SHA-256 hash of a JSON value using canonical serialization.
///
/// `serde_json` uses `BTreeMap` for JSON objects by default, which produces
/// sorted keys — so the same logical definition always produces the same hash
/// regardless of the order Python dict keys were inserted.
fn compute_hash(v: &serde_json::Value) -> String {
    let canonical = serde_json::to_string(v).unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(canonical.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// Extract identity configuration from all entities for hashing.
///
/// Returns a JSON object containing only the identity blocks from each entity:
/// `{"customer": {"email": {"normalize": "...", "match": "exact"}, ...}, ...}`
///
/// This allows identity_hash to change independently from definition_hash
/// when only identity config is modified (e.g., changing a normalize expression).
fn extract_identity_config(entities: Option<&serde_json::Value>) -> serde_json::Value {
    let Some(entities_val) = entities else {
        return serde_json::json!({});
    };
    let Some(entities_obj) = entities_val.as_object() else {
        return serde_json::json!({});
    };
    let mut identity_map = serde_json::Map::new();
    for (name, body) in entities_obj {
        if let Some(identity) = body.get("identity") {
            identity_map.insert(name.clone(), identity.clone());
        }
    }
    serde_json::Value::Object(identity_map)
}

// ===========================================================================
// Entity compilation
// ===========================================================================

/// Compile a single entity definition from its JSON body.
fn compile_entity(
    name: &str,
    body: &Map<String, serde_json::Value>,
) -> Result<CompiledEntity, DefactoError> {
    let ctx = format!("Entity '{name}'");

    let starts = require_str(body, "starts", &ctx)?.to_string();

    // Identity fields
    let identity_obj = body
        .get("identity")
        .and_then(|v| v.as_object())
        .ok_or_else(|| DefactoError::Definition(format!("{ctx}: missing required field 'identity'")))?;
    let mut identity = Vec::new();
    for (field_name, field_val) in identity_obj {
        let field_obj = require_object(field_val, &format!("{ctx} identity field '{field_name}'"))?;
        identity.push(compile_identity_field(field_name, &ctx, field_obj)?);
    }

    // Properties
    let mut properties = HashMap::new();
    if let Some(props_val) = body.get("properties") {
        let props_obj = require_object(props_val, &format!("{ctx} properties"))?;
        for (prop_name, prop_val) in props_obj {
            let prop_obj = require_object(prop_val, &format!("{ctx} property '{prop_name}'"))?;
            properties.insert(
                prop_name.clone(),
                compile_property(&format!("{ctx} property '{prop_name}'"), prop_obj)?,
            );
        }
    }

    // States
    let states_val = body
        .get("states")
        .ok_or_else(|| DefactoError::Definition(format!("{ctx}: missing required field 'states'")))?;
    let states_obj = require_object(states_val, &format!("{ctx} states"))?;
    let mut states = HashMap::new();
    for (state_name, state_val) in states_obj {
        let state_ctx = format!("{ctx} state '{state_name}'");
        // Handle null (bare YAML key) and empty object (terminal states)
        let state = if state_val.is_null() || (state_val.is_object() && state_val.as_object().unwrap().is_empty()) {
            CompiledState {
                handlers: HashMap::new(),
                time_rules: Vec::new(),
            }
        } else {
            let state_obj = require_object(state_val, &state_ctx)?;
            compile_state(name, state_name, state_obj)?
        };
        states.insert(state_name.clone(), state);
    }

    // Relationships
    let mut relationships = Vec::new();
    if let Some(rels_val) = body.get("relationships") {
        if let Some(rels_arr) = rels_val.as_array() {
            for rel_val in rels_arr {
                let rel_obj = require_object(rel_val, &format!("{ctx} relationship"))?;
                relationships.push(compile_relationship(&ctx, rel_obj)?);
            }
        }
    }

    // Always handlers
    let mut always_handlers = HashMap::new();
    if let Some(always_val) = body.get("always") {
        let always_obj = require_object(always_val, &format!("{ctx} always"))?;
        for (event_type, handler_val) in always_obj {
            let handler_obj = require_object(handler_val, &format!("{ctx} always handler '{event_type}'"))?;
            always_handlers.insert(
                event_type.clone(),
                compile_handler(&format!("{ctx} always handler '{event_type}'"), handler_obj)?,
            );
        }
    }

    Ok(CompiledEntity {
        name: name.to_string(),
        starts,
        states,
        properties,
        identity,
        relationships,
        always_handlers,
    })
}

/// Compile a single state definition.
fn compile_state(
    entity: &str,
    state_name: &str,
    body: &Map<String, serde_json::Value>,
) -> Result<CompiledState, DefactoError> {
    let ctx = format!("Entity '{entity}' state '{state_name}'");

    // Event handlers
    let mut handlers = HashMap::new();
    if let Some(when_val) = body.get("when") {
        let when_obj = require_object(when_val, &format!("{ctx} when"))?;
        for (event_type, handler_val) in when_obj {
            let handler_obj = require_object(handler_val, &format!("{ctx} handler '{event_type}'"))?;
            handlers.insert(
                event_type.clone(),
                compile_handler(&format!("{ctx} handler '{event_type}'"), handler_obj)?,
            );
        }
    }

    // Time rules
    let mut time_rules = Vec::new();
    if let Some(after_val) = body.get("after") {
        if let Some(after_arr) = after_val.as_array() {
            for rule_val in after_arr {
                let rule_obj = require_object(rule_val, &format!("{ctx} time rule"))?;
                time_rules.push(compile_time_rule(&ctx, rule_obj)?);
            }
        }
    }

    Ok(CompiledState {
        handlers,
        time_rules,
    })
}

/// Compile a handler (guard expression + effects list).
fn compile_handler(
    ctx: &str,
    body: &Map<String, serde_json::Value>,
) -> Result<CompiledHandler, DefactoError> {
    let guard = parse_optional_expr(body, "guard", ctx)?;

    let effects_val = body
        .get("effects")
        .ok_or_else(|| DefactoError::Definition(format!("{ctx}: missing required field 'effects'")))?;
    let effects_arr = effects_val
        .as_array()
        .ok_or_else(|| DefactoError::Definition(format!("{ctx}: 'effects' must be an array")))?;

    let mut effects = Vec::new();
    for (i, effect_val) in effects_arr.iter().enumerate() {
        effects.push(compile_effect(&format!("{ctx} effect {i}"), effect_val)?);
    }

    Ok(CompiledHandler { guard, effects })
}

/// Compile a single effect (spec + optional condition).
fn compile_effect(
    ctx: &str,
    effect_val: &serde_json::Value,
) -> Result<CompiledEffect, DefactoError> {
    // Effects are either bare strings ("create") or single-key dicts
    let (spec, condition_obj) = match effect_val {
        serde_json::Value::String(s) => {
            if s == "create" {
                (EffectSpec::Create, None)
            } else {
                return Err(DefactoError::Definition(format!(
                    "{ctx}: unknown string effect '{s}', expected 'create' or a dict"
                )));
            }
        }
        serde_json::Value::Object(obj) => {
            if obj.len() != 1 {
                return Err(DefactoError::Definition(format!(
                    "{ctx}: effect dict must have exactly one key (the effect kind)"
                )));
            }
            let (kind, body_val) = obj.iter().next().unwrap();
            // "create" can also appear as {"create": null}
            if kind == "create" {
                (EffectSpec::Create, None)
            } else {
                let body = require_object(body_val, &format!("{ctx} '{kind}'"))?;
                let spec = compile_effect_spec(ctx, kind, body)?;
                // Extract condition from the effect body (applies to set, increment, relate)
                let condition = parse_optional_expr(body, "condition", ctx)?;
                (spec, condition)
            }
        }
        _ => {
            return Err(DefactoError::Definition(format!(
                "{ctx}: effect must be a string or dict"
            )));
        }
    };

    Ok(CompiledEffect {
        spec,
        condition: condition_obj,
    })
}

/// Compile the spec (instruction) part of an effect from its kind and body.
fn compile_effect_spec(
    ctx: &str,
    kind: &str,
    body: &Map<String, serde_json::Value>,
) -> Result<EffectSpec, DefactoError> {
    match kind {
        "transition" => {
            let to = require_str(body, "to", &format!("{ctx} 'transition'"))?.to_string();
            Ok(EffectSpec::Transition { to })
        }
        "set" => {
            let property = require_str(body, "property", &format!("{ctx} 'set'"))?.to_string();

            // Determine value source: exactly one of from, value, or compute
            let has_from = body.contains_key("from");
            let has_value = body.contains_key("value");
            let has_compute = body.contains_key("compute");

            let source = match (has_from, has_value, has_compute) {
                (true, false, false) => {
                    let from_str = require_str(body, "from", &format!("{ctx} 'set'"))?;
                    // Split dotted path: "event.email" → ["event", "email"]
                    let path = from_str.split('.').map(String::from).collect();
                    CompiledSetSource::FromField(path)
                }
                (false, true, false) => {
                    let val = json_to_value(body.get("value").unwrap());
                    CompiledSetSource::Literal(val)
                }
                (false, false, true) => {
                    let expr_str = require_str(body, "compute", &format!("{ctx} 'set'"))?;
                    let expr = expression::parser::parse(expr_str).map_err(|e| {
                        DefactoError::Definition(format!(
                            "{ctx}: failed to parse compute expression '{expr_str}': {e}"
                        ))
                    })?;
                    CompiledSetSource::Compute(expr)
                }
                (false, false, false) => {
                    return Err(DefactoError::Definition(format!(
                        "{ctx}: set effect for '{property}' has no value source \
                         (need 'from', 'value', or 'compute')"
                    )));
                }
                _ => {
                    return Err(DefactoError::Definition(format!(
                        "{ctx}: set effect for '{property}' has multiple value sources \
                         (use only one of 'from', 'value', 'compute')"
                    )));
                }
            };

            Ok(EffectSpec::Set { property, source })
        }
        "increment" => {
            let property = require_str(body, "property", &format!("{ctx} 'increment'"))?.to_string();
            let by = body
                .get("by")
                .map(json_to_value)
                .unwrap_or(Value::Int(1));
            Ok(EffectSpec::Increment { property, by })
        }
        "relate" => {
            let relationship_type =
                require_str(body, "type", &format!("{ctx} 'relate'"))?.to_string();
            let target_entity_type =
                require_str(body, "target", &format!("{ctx} 'relate'"))?.to_string();

            let mut hints = HashMap::new();
            if let Some(hints_val) = body.get("hints") {
                if let Some(hints_obj) = hints_val.as_object() {
                    for (entity_type, fields_val) in hints_obj {
                        if let Some(fields_arr) = fields_val.as_array() {
                            let fields: Vec<String> = fields_arr
                                .iter()
                                .filter_map(|v| v.as_str().map(String::from))
                                .collect();
                            hints.insert(entity_type.clone(), fields);
                        }
                    }
                }
            }

            Ok(EffectSpec::Relate {
                relationship_type,
                target_entity_type,
                hints,
            })
        }
        _ => Err(DefactoError::Definition(format!(
            "{ctx}: unknown effect kind '{kind}', expected one of: \
             create, transition, set, increment, relate"
        ))),
    }
}

/// Compile a property definition.
fn compile_property(
    ctx: &str,
    body: &Map<String, serde_json::Value>,
) -> Result<CompiledProperty, DefactoError> {
    let prop_type = require_str(body, "type", ctx)?.to_string();
    let default = body.get("default").map(json_to_value);
    let sensitive = body.get("sensitive").and_then(|v| v.as_str()).map(String::from);
    let treatment = body.get("treatment").and_then(|v| v.as_str()).map(String::from);

    let allowed = body.get("allowed").and_then(|v| v.as_array()).map(|arr| {
        arr.iter().map(json_to_value).collect()
    });

    let min = body.get("min").and_then(|v| v.as_f64());
    let max = body.get("max").and_then(|v| v.as_f64());
    let compute = parse_optional_expr(body, "compute", ctx)?;

    Ok(CompiledProperty {
        prop_type,
        default,
        sensitive,
        treatment,
        allowed,
        min,
        max,
        compute,
    })
}

/// Compile a single identity field.
fn compile_identity_field(
    field_name: &str,
    ctx: &str,
    body: &Map<String, serde_json::Value>,
) -> Result<CompiledIdentityField, DefactoError> {
    let field_ctx = format!("{ctx} identity field '{field_name}'");
    let normalize = parse_optional_expr(body, "normalize", &field_ctx)?;
    let match_strategy = body
        .get("match")
        .and_then(|v| v.as_str())
        .unwrap_or("exact")
        .to_string();

    Ok(CompiledIdentityField {
        field_name: field_name.to_string(),
        normalize,
        match_strategy,
    })
}

/// Compile a time rule (type + duration threshold + effects).
fn compile_time_rule(
    ctx: &str,
    body: &Map<String, serde_json::Value>,
) -> Result<CompiledTimeRule, DefactoError> {
    let rule_type_str = require_str(body, "type", &format!("{ctx} time rule"))?;
    let rule_type = match rule_type_str {
        "inactivity" => TimeRuleType::Inactivity,
        "expiration" => TimeRuleType::Expiration,
        "state_duration" => TimeRuleType::StateDuration,
        _ => {
            return Err(DefactoError::Definition(format!(
                "{ctx}: unknown time rule type '{rule_type_str}', \
                 expected 'inactivity', 'expiration', or 'state_duration'"
            )));
        }
    };

    let threshold_str = require_str(body, "threshold", &format!("{ctx} time rule"))?;
    let threshold = parse_duration(threshold_str, &format!("{ctx} time rule"))?;

    let mut effects = Vec::new();
    if let Some(effects_val) = body.get("effects") {
        if let Some(effects_arr) = effects_val.as_array() {
            for (i, effect_val) in effects_arr.iter().enumerate() {
                effects.push(compile_effect(
                    &format!("{ctx} time rule '{rule_type_str}' effect {i}"),
                    effect_val,
                )?);
            }
        }
    }

    Ok(CompiledTimeRule {
        rule_type,
        threshold,
        effects,
    })
}

/// Compile a relationship declaration.
fn compile_relationship(
    ctx: &str,
    body: &Map<String, serde_json::Value>,
) -> Result<CompiledRelationship, DefactoError> {
    let rel_type = require_str(body, "type", &format!("{ctx} relationship"))?.to_string();
    let target = require_str(body, "target", &format!("{ctx} relationship"))?.to_string();
    let cardinality = require_str(body, "cardinality", &format!("{ctx} relationship"))?.to_string();

    let mut properties = HashMap::new();
    if let Some(props_val) = body.get("properties") {
        let props_obj = require_object(props_val, &format!("{ctx} relationship properties"))?;
        for (prop_name, prop_val) in props_obj {
            let prop_obj = require_object(
                prop_val,
                &format!("{ctx} relationship '{rel_type}' property '{prop_name}'"),
            )?;
            properties.insert(
                prop_name.clone(),
                compile_property(
                    &format!("{ctx} relationship '{rel_type}' property '{prop_name}'"),
                    prop_obj,
                )?,
            );
        }
    }

    Ok(CompiledRelationship {
        rel_type,
        target,
        cardinality,
        properties,
    })
}

// ===========================================================================
// Source compilation
// ===========================================================================

/// Compile a source definition.
fn compile_source(
    name: &str,
    body: &Map<String, serde_json::Value>,
) -> Result<CompiledSource, DefactoError> {
    let ctx = format!("Source '{name}'");

    let event_type_field = require_str(body, "event_type", &ctx)?.to_string();
    let timestamp_field = require_str(body, "timestamp", &ctx)?.to_string();

    let event_id_fields = body.get("event_id").and_then(|v| v.as_array()).map(|arr| {
        arr.iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect()
    });

    let events_val = body
        .get("events")
        .ok_or_else(|| DefactoError::Definition(format!("{ctx}: missing required field 'events'")))?;
    let events_obj = require_object(events_val, &format!("{ctx} events"))?;
    let mut events = HashMap::new();
    for (event_name, event_val) in events_obj {
        let event_obj = require_object(event_val, &format!("{ctx} event '{event_name}'"))?;
        events.insert(
            event_name.clone(),
            compile_source_event(&format!("{ctx} event '{event_name}'"), event_obj)?,
        );
    }

    Ok(CompiledSource {
        name: name.to_string(),
        event_type_field,
        timestamp_field,
        event_id_fields,
        events,
    })
}

/// Compile a single source event definition (mappings + hints).
fn compile_source_event(
    ctx: &str,
    body: &Map<String, serde_json::Value>,
) -> Result<CompiledSourceEvent, DefactoError> {
    let raw_type = body.get("raw_type").and_then(|v| v.as_str()).map(String::from);

    // Field mappings
    let mut mappings = HashMap::new();
    if let Some(mappings_val) = body.get("mappings") {
        let mappings_obj = require_object(mappings_val, &format!("{ctx} mappings"))?;
        for (field_name, mapping_val) in mappings_obj {
            let mapping_obj = require_object(mapping_val, &format!("{ctx} mapping '{field_name}'"))?;
            mappings.insert(
                field_name.clone(),
                compile_field_mapping(&format!("{ctx} mapping '{field_name}'"), mapping_obj)?,
            );
        }
    }

    // Identity hints
    let mut hints = HashMap::new();
    if let Some(hints_val) = body.get("hints") {
        if let Some(hints_obj) = hints_val.as_object() {
            for (entity_type, fields_val) in hints_obj {
                if let Some(fields_arr) = fields_val.as_array() {
                    let fields: Vec<String> = fields_arr
                        .iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect();
                    hints.insert(entity_type.clone(), fields);
                }
            }
        }
    }

    Ok(CompiledSourceEvent {
        raw_type,
        mappings,
        hints,
    })
}

/// Compile a single field mapping (from field, compute expression, type coercion, default).
fn compile_field_mapping(
    ctx: &str,
    body: &Map<String, serde_json::Value>,
) -> Result<CompiledFieldMapping, DefactoError> {
    let from_field = body.get("from").and_then(|v| v.as_str()).map(String::from);
    let compute = parse_optional_expr(body, "compute", ctx)?;
    let type_coercion = body.get("type").and_then(|v| v.as_str()).map(String::from);
    let default = body.get("default").map(json_to_value);

    Ok(CompiledFieldMapping {
        from_field,
        compute,
        type_coercion,
        default,
    })
}

// ===========================================================================
// Schema compilation
// ===========================================================================

/// Compile a schema definition.
fn compile_schema(
    name: &str,
    body: &Map<String, serde_json::Value>,
) -> Result<CompiledSchema, DefactoError> {
    let ctx = format!("Schema '{name}'");

    let fields_val = body
        .get("fields")
        .ok_or_else(|| DefactoError::Definition(format!("{ctx}: missing required field 'fields'")))?;
    let fields_obj = require_object(fields_val, &format!("{ctx} fields"))?;

    let mut fields = HashMap::new();
    for (field_name, field_val) in fields_obj {
        let field_obj = require_object(field_val, &format!("{ctx} field '{field_name}'"))?;
        fields.insert(field_name.clone(), compile_schema_field(&ctx, field_name, field_obj)?);
    }

    Ok(CompiledSchema {
        name: name.to_string(),
        fields,
    })
}

/// Compile a single schema field (type + validation constraints).
fn compile_schema_field(
    ctx: &str,
    field_name: &str,
    body: &Map<String, serde_json::Value>,
) -> Result<CompiledSchemaField, DefactoError> {
    let field_type = require_str(body, "type", &format!("{ctx} field '{field_name}'"))?.to_string();
    let required = body.get("required").and_then(|v| v.as_bool()).unwrap_or(false);

    let allowed = body.get("allowed").and_then(|v| v.as_array()).map(|arr| {
        arr.iter().map(json_to_value).collect()
    });

    let min = body.get("min").and_then(|v| v.as_f64());
    let max = body.get("max").and_then(|v| v.as_f64());
    let min_length = body.get("min_length").and_then(|v| v.as_u64()).map(|n| n as usize);
    let regex = body.get("regex").and_then(|v| v.as_str()).map(String::from);

    Ok(CompiledSchemaField {
        field_type,
        required,
        allowed,
        min,
        max,
        min_length,
        regex,
    })
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ── Helper tests ──

    #[test]
    fn parse_duration_days() {
        assert_eq!(parse_duration("90d", "test").unwrap(), Duration::from_secs(90 * 86400));
    }

    #[test]
    fn parse_duration_hours() {
        assert_eq!(parse_duration("24h", "test").unwrap(), Duration::from_secs(24 * 3600));
    }

    #[test]
    fn parse_duration_minutes() {
        assert_eq!(parse_duration("90m", "test").unwrap(), Duration::from_secs(90 * 60));
    }

    #[test]
    fn parse_duration_seconds() {
        assert_eq!(parse_duration("45s", "test").unwrap(), Duration::from_secs(45));
    }

    #[test]
    fn parse_duration_invalid_unit() {
        assert!(parse_duration("10x", "test").is_err());
    }

    #[test]
    fn parse_duration_too_short() {
        assert!(parse_duration("d", "test").is_err());
    }

    #[test]
    fn json_to_value_string() {
        assert_eq!(json_to_value(&json!("hello")), Value::String("hello".into()));
    }

    #[test]
    fn json_to_value_int() {
        assert_eq!(json_to_value(&json!(42)), Value::Int(42));
    }

    #[test]
    fn json_to_value_float() {
        assert_eq!(json_to_value(&json!(3.14)), Value::Float(3.14));
    }

    #[test]
    fn json_to_value_bool() {
        assert_eq!(json_to_value(&json!(true)), Value::Bool(true));
    }

    #[test]
    fn json_to_value_null() {
        assert_eq!(json_to_value(&json!(null)), Value::Null);
    }

    #[test]
    fn json_to_value_array() {
        assert_eq!(
            json_to_value(&json!([1, "two", true])),
            Value::Array(vec![Value::Int(1), Value::String("two".into()), Value::Bool(true)])
        );
    }

    #[test]
    fn compute_hash_deterministic() {
        let v = json!({"b": 2, "a": 1});
        assert_eq!(compute_hash(&v), compute_hash(&v));
    }

    #[test]
    fn compute_hash_different_input() {
        assert_ne!(compute_hash(&json!({"a": 1})), compute_hash(&json!({"a": 2})));
    }

    // ── Effect compilation tests ──

    #[test]
    fn compile_effect_create_string() {
        let effect = compile_effect("test", &json!("create")).unwrap();
        assert_eq!(effect.spec, EffectSpec::Create);
        assert!(effect.condition.is_none());
    }

    #[test]
    fn compile_effect_transition() {
        let effect = compile_effect("test", &json!({"transition": {"to": "active"}})).unwrap();
        assert_eq!(effect.spec, EffectSpec::Transition { to: "active".into() });
    }

    #[test]
    fn compile_effect_set_from_field() {
        let effect = compile_effect("test", &json!({"set": {"property": "email", "from": "event.email"}})).unwrap();
        match &effect.spec {
            EffectSpec::Set { property, source: CompiledSetSource::FromField(path) } => {
                assert_eq!(property, "email");
                assert_eq!(path, &vec!["event".to_string(), "email".to_string()]);
            }
            _ => panic!("expected Set with FromField"),
        }
    }

    #[test]
    fn compile_effect_set_literal() {
        let effect = compile_effect("test", &json!({"set": {"property": "status", "value": "active"}})).unwrap();
        match &effect.spec {
            EffectSpec::Set { source: CompiledSetSource::Literal(v), .. } => {
                assert_eq!(v, &Value::String("active".into()));
            }
            _ => panic!("expected Set with Literal"),
        }
    }

    #[test]
    fn compile_effect_set_compute() {
        let effect = compile_effect("test", &json!({"set": {"property": "annual", "compute": "entity.mrr * 12"}})).unwrap();
        match &effect.spec {
            EffectSpec::Set { source: CompiledSetSource::Compute(_), .. } => {}
            _ => panic!("expected Set with Compute"),
        }
    }

    #[test]
    fn compile_effect_set_with_condition() {
        let effect = compile_effect("test", &json!({"set": {
            "property": "high_score", "from": "event.score",
            "condition": "event.score > entity.high_score"
        }})).unwrap();
        assert!(effect.condition.is_some());
    }

    #[test]
    fn compile_effect_increment() {
        let effect = compile_effect("test", &json!({"increment": {"property": "count", "by": 5}})).unwrap();
        assert_eq!(effect.spec, EffectSpec::Increment { property: "count".into(), by: Value::Int(5) });
    }

    #[test]
    fn compile_effect_increment_default_by() {
        let effect = compile_effect("test", &json!({"increment": {"property": "count"}})).unwrap();
        assert_eq!(effect.spec, EffectSpec::Increment { property: "count".into(), by: Value::Int(1) });
    }

    #[test]
    fn compile_effect_relate() {
        let effect = compile_effect("test", &json!({"relate": {
            "type": "placed_by", "target": "customer",
            "hints": {"customer": ["email"]}
        }})).unwrap();
        match &effect.spec {
            EffectSpec::Relate { relationship_type, target_entity_type, hints } => {
                assert_eq!(relationship_type, "placed_by");
                assert_eq!(target_entity_type, "customer");
                assert_eq!(hints.get("customer"), Some(&vec!["email".to_string()]));
            }
            _ => panic!("expected Relate"),
        }
    }

    #[test]
    fn compile_effect_unknown_kind() {
        assert!(compile_effect("test", &json!({"delete": {}})).is_err());
    }

    #[test]
    fn compile_effect_bad_expression() {
        let result = compile_effect("test", &json!({"set": {"property": "x", "compute": "!!bad!!"}}));
        assert!(result.is_err());
    }

    // ── Sub-function tests ──

    #[test]
    fn compile_property_basic() {
        let body = json!({"type": "number", "default": 0});
        let prop = compile_property("test", body.as_object().unwrap()).unwrap();
        assert_eq!(prop.prop_type, "number");
        assert_eq!(prop.default, Some(Value::Int(0)));
    }

    #[test]
    fn compile_property_with_compute() {
        let body = json!({"type": "number", "compute": "entity.mrr * 12"});
        let prop = compile_property("test", body.as_object().unwrap()).unwrap();
        assert!(prop.compute.is_some());
    }

    #[test]
    fn compile_identity_field_with_normalize() {
        let body = json!({"normalize": "str::to_lowercase(value)", "match": "exact"});
        let field = compile_identity_field("email", "test", body.as_object().unwrap()).unwrap();
        assert_eq!(field.field_name, "email");
        assert!(field.normalize.is_some());
        assert_eq!(field.match_strategy, "exact");
    }

    #[test]
    fn compile_time_rule_inactivity() {
        let body = json!({"type": "inactivity", "threshold": "90d", "effects": [{"transition": {"to": "churned"}}]});
        let rule = compile_time_rule("test", body.as_object().unwrap()).unwrap();
        assert_eq!(rule.rule_type, TimeRuleType::Inactivity);
        assert_eq!(rule.threshold, Duration::from_secs(90 * 86400));
        assert_eq!(rule.effects.len(), 1);
    }

    #[test]
    fn compile_handler_with_guard() {
        let body = json!({"guard": "event.amount > 0", "effects": ["create"]});
        let handler = compile_handler("test", body.as_object().unwrap()).unwrap();
        assert!(handler.guard.is_some());
        assert_eq!(handler.effects.len(), 1);
    }

    #[test]
    fn compile_schema_basic() {
        let body = json!({"fields": {"email": {"type": "string", "required": true}}});
        let schema = compile_schema("signup", body.as_object().unwrap()).unwrap();
        assert_eq!(schema.name, "signup");
        assert!(schema.fields["email"].required);
    }

    #[test]
    fn compile_source_basic() {
        let body = json!({
            "event_type": "type", "timestamp": "created_at",
            "events": {"signup": {"mappings": {"email": {"from": "user_email"}}, "hints": {"customer": ["email"]}}}
        });
        let source = compile_source("app", body.as_object().unwrap()).unwrap();
        assert_eq!(source.name, "app");
        assert!(source.events.contains_key("signup"));
    }

    #[test]
    fn compile_source_event_with_compute() {
        let body = json!({
            "mappings": {"email": {"compute": "str::to_lowercase(event.email)"}},
            "hints": {"customer": ["email"]}
        });
        let event = compile_source_event("test", body.as_object().unwrap()).unwrap();
        assert!(event.mappings["email"].compute.is_some());
    }

    // ── Integration tests ──

    #[test]
    fn compile_full_definitions() {
        let defs = json!({
            "entities": {
                "customer": {
                    "starts": "lead",
                    "identity": {"email": {"normalize": "str::to_lowercase(value)", "match": "exact"}},
                    "properties": {"mrr": {"type": "number", "default": 0}, "plan": {"type": "string"}},
                    "states": {
                        "lead": {"when": {"signup": {"effects": ["create", {"set": {"property": "mrr", "from": "event.mrr"}}, {"transition": {"to": "active"}}]}}},
                        "active": {
                            "when": {"upgrade": {"guard": "event.plan != entity.plan", "effects": [{"set": {"property": "plan", "from": "event.new_plan"}}]}},
                            "after": [{"type": "inactivity", "threshold": "90d", "effects": [{"transition": {"to": "churned"}}]}]
                        },
                        "churned": {}
                    },
                    "relationships": [{"type": "placed_order", "target": "order", "cardinality": "has_many"}],
                    "always": {"profile_updated": {"effects": [{"set": {"property": "plan", "from": "event.plan", "condition": "event.plan != null"}}]}}
                }
            },
            "sources": {
                "app": {
                    "event_type": "type", "timestamp": "created_at",
                    "events": {"signup": {"raw_type": "user.created", "mappings": {"email": {"from": "user_email"}}, "hints": {"customer": ["email"]}}}
                }
            },
            "schemas": {
                "signup": {"fields": {"email": {"type": "string", "required": true}}}
            }
        });

        let compiled = CompiledDefinitions::compile(&defs).unwrap();

        // Entities compiled
        assert!(compiled.entities.contains_key("customer"));
        let customer = &compiled.entities["customer"];
        assert_eq!(customer.starts, "lead");
        assert_eq!(customer.identity.len(), 1);
        assert!(customer.identity[0].normalize.is_some());
        assert_eq!(customer.states.len(), 3);
        assert!(customer.states["active"].time_rules.len() == 1);
        assert!(customer.always_handlers.contains_key("profile_updated"));

        // Sources compiled
        assert!(compiled.sources.contains_key("app"));

        // Schemas compiled
        assert!(compiled.schemas.contains_key("signup"));

        // Hashes are non-empty
        assert!(!compiled.definition_hash.is_empty());
        assert!(!compiled.source_hash.is_empty());
        assert!(!compiled.identity_hash.is_empty());
    }

    #[test]
    fn compile_hash_stability() {
        let defs = json!({"entities": {"x": {"starts": "a", "identity": {"id": {"match": "exact"}}, "states": {"a": {}}}}});
        let h1 = CompiledDefinitions::compile(&defs).unwrap().definition_hash;
        let h2 = CompiledDefinitions::compile(&defs).unwrap().definition_hash;
        assert_eq!(h1, h2);
    }

    #[test]
    fn compile_hash_sensitivity() {
        let defs1 = json!({"entities": {"x": {"starts": "a", "identity": {"id": {"match": "exact"}}, "states": {"a": {}}}}});
        let defs2 = json!({"entities": {"x": {"starts": "b", "identity": {"id": {"match": "exact"}}, "states": {"b": {}}}}});
        let c1 = CompiledDefinitions::compile(&defs1).unwrap();
        let c2 = CompiledDefinitions::compile(&defs2).unwrap();
        assert_ne!(c1.definition_hash, c2.definition_hash);
    }

    #[test]
    fn compile_identity_hash_independent() {
        // Changing identity should change identity_hash but definition_hash also changes
        // (identity is part of entities). The cascade in BuildManager checks identity first.
        let defs1 = json!({"entities": {"x": {"starts": "a", "identity": {"id": {"match": "exact"}}, "states": {"a": {}}}}});
        let defs2 = json!({"entities": {"x": {"starts": "a", "identity": {"id": {"match": "case_insensitive"}}, "states": {"a": {}}}}});
        let c1 = CompiledDefinitions::compile(&defs1).unwrap();
        let c2 = CompiledDefinitions::compile(&defs2).unwrap();
        assert_ne!(c1.identity_hash, c2.identity_hash);
        // Source hash unchanged since no sources changed
        assert_eq!(c1.source_hash, c2.source_hash);
    }

    #[test]
    fn compile_empty_definitions() {
        let compiled = CompiledDefinitions::compile(&json!({})).unwrap();
        assert!(compiled.entities.is_empty());
        assert!(compiled.sources.is_empty());
        assert!(compiled.schemas.is_empty());
    }

    #[test]
    fn compile_bad_expression_error_context() {
        let defs = json!({"entities": {"customer": {
            "starts": "a", "identity": {"id": {"match": "exact"}},
            "states": {"a": {"when": {"signup": {"guard": "!!invalid!!", "effects": ["create"]}}}}
        }}});
        let err = CompiledDefinitions::compile(&defs).unwrap_err();
        let msg = err.to_string();
        // Error should mention the entity, state, and the bad expression
        assert!(msg.contains("customer"), "error should mention entity name: {msg}");
        assert!(msg.contains("guard"), "error should mention the field: {msg}");
    }

    #[test]
    fn compile_terminal_state_null() {
        let defs = json!({"entities": {"x": {
            "starts": "a", "identity": {"id": {"match": "exact"}},
            "states": {"a": null}
        }}});
        let compiled = CompiledDefinitions::compile(&defs).unwrap();
        let state = &compiled.entities["x"].states["a"];
        assert!(state.handlers.is_empty());
        assert!(state.time_rules.is_empty());
    }
}
