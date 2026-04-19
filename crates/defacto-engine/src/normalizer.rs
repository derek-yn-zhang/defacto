//! Event normalization — raw input → structured events.
//!
//! The normalizer transforms raw input records (JSON dicts from external systems)
//! into NormalizedEvents using compiled source definitions. For each raw record:
//!
//! 1. Match the raw event type to a source event definition
//! 2. Extract the timestamp from the configured field
//! 3. Apply field mappings (from, compute, type coercion, default)
//! 4. Compute the content-based event_id (SHA-256 of configured fields)
//! 5. Extract identity resolution hints
//!
//! Normalization is a pure function — no I/O, no state mutation. It can be
//! parallelized across events using the Rayon thread pool.

use std::collections::HashMap;

use chrono::{DateTime, TimeZone, Utc};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::definitions::entity::CompiledEntity;
use crate::definitions::{CompiledSchema, CompiledSource, CompiledSourceEvent};
use crate::error::DefactoError;
use crate::expression::{self, evaluator::EvalContext};
use crate::types::{json_to_value, values_to_json, NormalizedEvent, Value};

/// Result of normalizing a batch of raw events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizeResult {
    /// Successfully normalized events. Held in Rust for the subsequent interpret() call.
    pub events: Vec<NormalizedEvent>,
    /// Ledger rows — the subset of event fields that Python needs for the database write.
    /// Each row is a JSON object ready for Postgres COPY or SQLite INSERT.
    pub ledger_rows: Vec<serde_json::Value>,
    /// Events that failed normalization, with error context.
    pub failures: Vec<NormalizeFailure>,
}

/// A single event that failed normalization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizeFailure {
    /// The original raw input record.
    pub raw: serde_json::Value,
    /// Human-readable error message.
    pub error: String,
    /// Source name (e.g., "app").
    pub source: String,
    /// Which event type failed (None if event type wasn't recognized).
    pub handler: Option<String>,
    /// Which field caused the error (if applicable).
    pub field: Option<String>,
}

// ===========================================================================
// Public API
// ===========================================================================

/// Normalize a batch of raw events through source definitions.
///
/// Good events produce NormalizedEvents. Bad events produce NormalizeFailures.
/// The batch is NOT all-or-nothing — partial success is the default.
///
/// Events are processed in parallel via Rayon — each event is independent,
/// so there's no shared state or ordering constraint during normalization.
pub fn normalize(
    source: &CompiledSource,
    _schemas: &HashMap<String, CompiledSchema>,
    entities: &HashMap<String, CompiledEntity>,
    raw_events: &[serde_json::Value],
) -> Result<NormalizeResult, DefactoError> {
    // Build reverse lookup: raw_type → (defacto_event_name, event_definition).
    // Allocated once per batch, not per event.
    let type_lookup = build_type_lookup(source);

    // Normalize each event in parallel. Each result is independent.
    let results: Vec<Result<(NormalizedEvent, serde_json::Value), NormalizeFailure>> = raw_events
        .par_iter()
        .map(|raw| normalize_one(raw, source, &type_lookup, entities))
        .collect();

    // Partition into successes and failures
    let mut events = Vec::new();
    let mut ledger_rows = Vec::new();
    let mut failures = Vec::new();

    for result in results {
        match result {
            Ok((event, ledger_row)) => {
                events.push(event);
                ledger_rows.push(ledger_row);
            }
            Err(failure) => failures.push(failure),
        }
    }

    Ok(NormalizeResult {
        events,
        ledger_rows,
        failures,
    })
}

/// Re-normalize raw events, preserving sequence correlation.
///
/// Same normalization logic as `normalize()`, but each input carries a
/// sequence number (its ledger primary key) and each output pairs that
/// sequence with the result. Used by FULL_RENORMALIZE to update ledger
/// rows in place — the caller knows exactly which row to update.
///
/// Discards `NormalizedEvent` (the subsequent FULL build loads events
/// via `load_events`). Only the ledger row (data, event_id, event_type,
/// resolution_hints) is needed for the database update.
pub fn renormalize(
    source: &CompiledSource,
    _schemas: &HashMap<String, CompiledSchema>,
    entities: &HashMap<String, CompiledEntity>,
    raw_events: &[(i64, serde_json::Value)],
) -> Vec<(i64, Result<serde_json::Value, String>)> {
    let type_lookup = build_type_lookup(source);

    raw_events
        .par_iter()
        .map(|(seq, raw)| {
            match normalize_one(raw, source, &type_lookup, entities) {
                Ok((_event, ledger_row)) => (*seq, Ok(ledger_row)),
                Err(failure) => (*seq, Err(failure.error)),
            }
        })
        .collect()
}

// ===========================================================================
// Per-event normalization
// ===========================================================================

/// Build a reverse lookup from raw event type → (defacto event name, definition).
///
/// Source events have a `raw_type` field (what the external system calls the event).
/// If `raw_type` is None, the Defacto event name is used as the raw type.
fn build_type_lookup<'a>(
    source: &'a CompiledSource,
) -> HashMap<&'a str, (&'a str, &'a CompiledSourceEvent)> {
    let mut lookup = HashMap::new();
    for (event_name, event_def) in &source.events {
        let raw_type = event_def
            .raw_type
            .as_deref()
            .unwrap_or(event_name.as_str());
        lookup.insert(raw_type, (event_name.as_str(), event_def));
    }
    lookup
}

/// Normalize a single raw event.
///
/// Returns the NormalizedEvent (held in Rust) and a ledger row (returned to Python).
/// On failure, returns a NormalizeFailure with context about what went wrong.
fn normalize_one(
    raw: &serde_json::Value,
    source: &CompiledSource,
    type_lookup: &HashMap<&str, (&str, &CompiledSourceEvent)>,
    entities: &HashMap<String, CompiledEntity>,
) -> Result<(NormalizedEvent, serde_json::Value), NormalizeFailure> {
    // 1. Extract raw event type
    let raw_type = raw
        .get(&source.event_type_field)
        .and_then(|v| v.as_str())
        .ok_or_else(|| NormalizeFailure {
            raw: raw.clone(),
            error: format!(
                "Missing or non-string event type field '{}'",
                source.event_type_field
            ),
            source: source.name.clone(),
            handler: None,
            field: Some(source.event_type_field.clone()),
        })?;

    // 2. Match raw_type to source event definition
    let (event_name, event_def) =
        type_lookup
            .get(raw_type)
            .ok_or_else(|| NormalizeFailure {
                raw: raw.clone(),
                error: format!(
                    "Unknown event type '{}' — not defined in source '{}'",
                    raw_type, source.name
                ),
                source: source.name.clone(),
                handler: None,
                field: Some(source.event_type_field.clone()),
            })?;

    // 3. Extract and parse timestamp
    let timestamp = extract_timestamp(raw, &source.timestamp_field, &source.name, event_name)?;

    // 4. Apply field mappings
    let data = apply_mappings(raw, event_def, &source.name, event_name)?;

    // 5. Compute content-based event ID
    let event_id = compute_event_id(&data, source.event_id_fields.as_deref(), event_name, &timestamp);

    // 6. Extract identity resolution hints (with normalize expressions applied)
    let resolution_hints = extract_hints(event_def, &data, entities);

    // 7. Preserve the raw event as a JSON string for the ledger
    let raw_json = serde_json::to_string(raw).unwrap_or_default();

    let event = NormalizedEvent {
        event_id: event_id.clone(),
        timestamp,
        source: source.name.clone(),
        event_type: event_name.to_string(),
        data: data.clone(),
        raw: raw_json.clone(),
        resolution_hints: resolution_hints.clone(),
    };

    // Build ledger row — a JSON dict ready for database write.
    // Pass the original raw event (serde_json::Value), not the stringified version,
    // so Python receives it as a dict, not a double-encoded string.
    let ledger_row = build_ledger_row(&event_id, &timestamp, &source.name, event_name, &data, raw, &resolution_hints);

    Ok((event, ledger_row))
}

// ===========================================================================
// Helpers
// ===========================================================================

/// Extract and parse the timestamp from a raw event.
///
/// Supports ISO 8601 strings and Unix epoch timestamps (integer or float seconds).
fn extract_timestamp(
    raw: &serde_json::Value,
    timestamp_field: &str,
    source_name: &str,
    event_name: &str,
) -> Result<DateTime<Utc>, NormalizeFailure> {
    let ts_val = raw.get(timestamp_field).ok_or_else(|| NormalizeFailure {
        raw: raw.clone(),
        error: format!("Missing timestamp field '{timestamp_field}'"),
        source: source_name.into(),
        handler: Some(event_name.into()),
        field: Some(timestamp_field.into()),
    })?;

    match ts_val {
        // ISO 8601 string: "2024-01-15T10:00:00Z"
        serde_json::Value::String(s) => {
            DateTime::parse_from_rfc3339(s)
                .map(|dt| dt.with_timezone(&Utc))
                .map_err(|e| NormalizeFailure {
                    raw: raw.clone(),
                    error: format!(
                        "Failed to parse timestamp '{s}' as ISO 8601: {e}. \
                         Use a compute mapping with parse_datetime() for custom formats."
                    ),
                    source: source_name.into(),
                    handler: Some(event_name.into()),
                    field: Some(timestamp_field.into()),
                })
        }
        // Unix epoch (integer seconds)
        serde_json::Value::Number(n) => {
            if let Some(secs) = n.as_i64() {
                Utc.timestamp_opt(secs, 0)
                    .single()
                    .ok_or_else(|| NormalizeFailure {
                        raw: raw.clone(),
                        error: format!("Invalid Unix timestamp: {secs}"),
                        source: source_name.into(),
                        handler: Some(event_name.into()),
                        field: Some(timestamp_field.into()),
                    })
            } else if let Some(secs_f) = n.as_f64() {
                // Float epoch: seconds with fractional part (milliseconds)
                let secs = secs_f as i64;
                let nsecs = ((secs_f - secs as f64) * 1_000_000_000.0) as u32;
                Utc.timestamp_opt(secs, nsecs)
                    .single()
                    .ok_or_else(|| NormalizeFailure {
                        raw: raw.clone(),
                        error: format!("Invalid Unix timestamp: {secs_f}"),
                        source: source_name.into(),
                        handler: Some(event_name.into()),
                        field: Some(timestamp_field.into()),
                    })
            } else {
                Err(NormalizeFailure {
                    raw: raw.clone(),
                    error: format!("Timestamp field '{timestamp_field}' is not a valid number"),
                    source: source_name.into(),
                    handler: Some(event_name.into()),
                    field: Some(timestamp_field.into()),
                })
            }
        }
        _ => Err(NormalizeFailure {
            raw: raw.clone(),
            error: format!(
                "Timestamp field '{timestamp_field}' must be an ISO 8601 string or Unix epoch number, got {}",
                json_type_name(ts_val)
            ),
            source: source_name.into(),
            handler: Some(event_name.into()),
            field: Some(timestamp_field.into()),
        }),
    }
}

/// Apply field mappings to extract normalized data fields from a raw event.
///
/// For each mapping:
/// - `from_field`: extract from raw event via dot-path
/// - `compute`: evaluate expression against event context
/// - `type_coercion`: convert to target type
/// - `default`: fallback if value is null/missing
fn apply_mappings(
    raw: &serde_json::Value,
    event_def: &CompiledSourceEvent,
    source_name: &str,
    event_name: &str,
) -> Result<HashMap<String, Value>, NormalizeFailure> {
    let mut data = HashMap::new();

    for (field_name, mapping) in &event_def.mappings {
        // Extract or compute the raw value
        let mut value = if let Some(from_path) = &mapping.from_field {
            // Extract from raw event via dot-path
            let json_val = resolve_json_path(raw, from_path);
            json_to_value(&json_val)
        } else if let Some(expr) = &mapping.compute {
            // Evaluate compute expression against event data
            // Build a flat event context from the raw JSON for expression evaluation
            let event_map = json_object_to_value_map(raw);
            let ctx = EvalContext::event_only(&event_map);
            expression::evaluator::evaluate(expr, &ctx).map_err(|e| NormalizeFailure {
                raw: raw.clone(),
                error: format!(
                    "Compute expression failed for field '{field_name}': {e}"
                ),
                source: source_name.into(),
                handler: Some(event_name.into()),
                field: Some(field_name.clone()),
            })?
        } else {
            // No from_field and no compute — use Null (default will apply)
            Value::Null
        };

        // Apply type coercion if specified
        if let Some(target_type) = &mapping.type_coercion {
            if value != Value::Null {
                value = coerce_value(value, target_type).map_err(|e| NormalizeFailure {
                    raw: raw.clone(),
                    error: format!(
                        "Type coercion failed for field '{field_name}' to '{target_type}': {e}"
                    ),
                    source: source_name.into(),
                    handler: Some(event_name.into()),
                    field: Some(field_name.clone()),
                })?;
            }
        }

        // Apply default if value is still null
        if value == Value::Null {
            if let Some(default) = &mapping.default {
                value = default.clone();
            }
        }

        data.insert(field_name.clone(), value);
    }

    Ok(data)
}

/// Walk a JSON value by dot-separated path, supporting nested objects and array indexing.
///
/// `"user.email"` → walks into nested object.
/// `"items.0.product_id"` → numeric segments index into arrays.
/// Missing keys at any level return `serde_json::Value::Null`.
fn resolve_json_path(value: &serde_json::Value, path: &str) -> serde_json::Value {
    let segments: Vec<&str> = path.split('.').collect();
    let mut current = value;

    for segment in &segments {
        match current {
            serde_json::Value::Object(map) => {
                current = map.get(*segment).unwrap_or(&serde_json::Value::Null);
            }
            serde_json::Value::Array(arr) => {
                if let Ok(index) = segment.parse::<usize>() {
                    current = arr.get(index).unwrap_or(&serde_json::Value::Null);
                } else {
                    return serde_json::Value::Null;
                }
            }
            // Can't go deeper into a scalar
            _ => return serde_json::Value::Null,
        }
    }

    current.clone()
}

/// Convert a JSON object's top-level fields to a flat `HashMap<String, Value>`.
///
/// Used to build expression evaluation contexts from raw event data.
/// Only top-level fields are extracted — nested access uses dot-path syntax
/// in the expression language (event.user.email).
fn json_object_to_value_map(value: &serde_json::Value) -> HashMap<String, Value> {
    let mut map = HashMap::new();
    if let Some(obj) = value.as_object() {
        for (key, val) in obj {
            map.insert(key.clone(), json_to_value(val));
        }
    }
    map
}

/// Apply type coercion to a Value.
///
/// Coercion only runs on non-Null values. If coercion fails, returns an error
/// string — the caller wraps it in a NormalizeFailure. Default values are NOT
/// applied on coercion failure — a bad value is a bug, not a missing value.
fn coerce_value(value: Value, target_type: &str) -> Result<Value, String> {
    match target_type {
        "string" => match value {
            Value::String(_) => Ok(value),
            Value::Int(i) => Ok(Value::String(i.to_string())),
            Value::Float(f) => Ok(Value::String(f.to_string())),
            Value::Bool(b) => Ok(Value::String(b.to_string())),
            _ => Err(format!("cannot coerce {} to string", type_name(&value))),
        },
        "number" => match value {
            Value::Float(_) => Ok(value),
            Value::Int(i) => Ok(Value::Float(i as f64)),
            Value::String(s) => s
                .parse::<f64>()
                .map(Value::Float)
                .map_err(|_| format!("cannot parse '{s}' as number")),
            _ => Err(format!("cannot coerce {} to number", type_name(&value))),
        },
        "integer" => match value {
            Value::Int(_) => Ok(value),
            Value::Float(f) => Ok(Value::Int(f as i64)),
            Value::String(s) => s
                .parse::<i64>()
                .map(Value::Int)
                .map_err(|_| format!("cannot parse '{s}' as integer")),
            _ => Err(format!("cannot coerce {} to integer", type_name(&value))),
        },
        "boolean" => match value {
            Value::Bool(_) => Ok(value),
            Value::String(s) => match s.to_lowercase().as_str() {
                "true" | "1" | "yes" => Ok(Value::Bool(true)),
                "false" | "0" | "no" => Ok(Value::Bool(false)),
                _ => Err(format!("cannot parse '{s}' as boolean")),
            },
            Value::Int(i) => Ok(Value::Bool(i != 0)),
            _ => Err(format!("cannot coerce {} to boolean", type_name(&value))),
        },
        "datetime" => match value {
            Value::DateTime(_) => Ok(value),
            Value::String(s) => DateTime::parse_from_rfc3339(&s)
                .map(|dt| Value::DateTime(dt.with_timezone(&Utc)))
                .map_err(|e| format!("cannot parse '{s}' as datetime (ISO 8601): {e}")),
            _ => Err(format!("cannot coerce {} to datetime", type_name(&value))),
        },
        _ => Err(format!("unknown target type '{target_type}'")),
    }
}

/// Compute a content-based event ID using SHA-256.
///
/// If `id_fields` is specified, only those fields are hashed (for targeted dedup).
/// If None, all data fields are hashed (hash of the entire normalized payload).
/// The ID is prefixed with "e_" for readability in logs and debugging.
fn compute_event_id(
    data: &HashMap<String, Value>,
    id_fields: Option<&[String]>,
    event_type: &str,
    _timestamp: &DateTime<Utc>,
) -> String {
    // Event type is always part of the hash — a signup and a cancel with
    // identical data fields are never the same event.
    let mut hasher = Sha256::new();
    hasher.update(event_type.as_bytes());
    hasher.update(b"|");

    match id_fields {
        Some(fields) => {
            // Explicit fields: event_type + specified data fields only.
            let mut selected: Vec<(&String, &Value)> = data
                .iter()
                .filter(|(k, _)| fields.iter().any(|f| f == *k))
                .collect();
            selected.sort_by_key(|(k, _)| *k);
            hasher.update(serde_json::to_string(&selected).unwrap_or_default().as_bytes());
        }
        None => {
            // Default: event_type + all data fields. No timestamp — retries
            // of the same event with slightly different timestamps should
            // still be deduplicated.
            let mut all: Vec<(&String, &Value)> = data.iter().collect();
            all.sort_by_key(|(k, _)| *k);
            hasher.update(serde_json::to_string(&all).unwrap_or_default().as_bytes());
        }
    };

    format!("e_{:x}", hasher.finalize())
}

/// Extract identity resolution hints from the normalized event data.
///
/// For each entity type in the event's hint config, collects the values of
/// the specified fields. These hints are sent to the identity resolver to
/// map events to entity IDs.
/// Extract identity resolution hints from normalized event data.
///
/// For each entity type's hint fields, extract the value from the event data,
/// apply the identity normalize expression if one is defined (e.g.,
/// `str::to_lowercase(value)`), and collect the normalized values.
///
/// The normalize expression is looked up from the entity's compiled identity
/// config by matching the field name. If no entity definition exists or no
/// normalize expression is defined, the raw value is used as-is.
fn extract_hints(
    event_def: &CompiledSourceEvent,
    data: &HashMap<String, Value>,
    entities: &HashMap<String, CompiledEntity>,
) -> HashMap<String, Vec<(String, String)>> {
    let mut hints = HashMap::new();

    for (entity_type, hint_fields) in &event_def.hints {
        // Look up entity's identity config for normalize expressions
        let identity_fields = entities.get(entity_type).map(|e| &e.identity);

        let values: Vec<(String, String)> = hint_fields
            .iter()
            .filter_map(|field_name| {
                // Extract raw value from event data
                let raw_value = match data.get(field_name) {
                    Some(Value::String(s)) => Value::String(s.clone()),
                    Some(Value::Int(i)) => Value::String(i.to_string()),
                    Some(Value::Float(f)) => Value::String(f.to_string()),
                    _ => return None,
                };

                // Apply identity normalize expression if defined for this field
                let normalize_expr = identity_fields
                    .and_then(|fields| fields.iter().find(|f| f.field_name == *field_name))
                    .and_then(|f| f.normalize.as_ref());

                let hint_value = if let Some(expr) = normalize_expr {
                    let ctx = EvalContext::value_only(raw_value.clone());
                    match expression::evaluator::evaluate(expr, &ctx) {
                        Ok(Value::String(s)) => s,
                        Ok(_) => value_to_hint_string(&raw_value),
                        Err(_) => value_to_hint_string(&raw_value),
                    }
                } else {
                    value_to_hint_string(&raw_value)
                };

                Some((field_name.clone(), hint_value))
            })
            .collect();

        if !values.is_empty() {
            hints.insert(entity_type.clone(), values);
        }
    }

    hints
}

/// Convert a Value to its string representation for use as a hint value.
fn value_to_hint_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        _ => String::new(),
    }
}

/// Build a ledger row as a JSON object ready for database write.
///
/// This is the data that crosses back to Python for Postgres COPY or SQLite INSERT.
/// It's a flat dict with all the fields the ledger table needs.
fn build_ledger_row(
    event_id: &str,
    timestamp: &DateTime<Utc>,
    source_name: &str,
    event_type: &str,
    data: &HashMap<String, Value>,
    raw: &serde_json::Value,
    hints: &HashMap<String, Vec<(String, String)>>,
) -> serde_json::Value {
    // Convert data to plain JSON (not tagged enum format) so Python
    // receives {"email": "alice@test.com"} not {"email": {"String": "alice@test.com"}}
    //
    // Convert hints from Vec<(field_name, value)> to {field_name: value} dicts
    // so Python receives {"customer": {"email": "alice@test.com"}} not
    // {"customer": [["email", "alice@test.com"]]}
    let hints_json: serde_json::Map<String, serde_json::Value> = hints
        .iter()
        .map(|(entity_type, pairs)| {
            let fields: serde_json::Map<String, serde_json::Value> = pairs
                .iter()
                .map(|(field, value)| (field.clone(), serde_json::Value::String(value.clone())))
                .collect();
            (entity_type.clone(), serde_json::Value::Object(fields))
        })
        .collect();

    serde_json::json!({
        "event_id": event_id,
        "timestamp": timestamp.to_rfc3339(),
        "source": source_name,
        "event_type": event_type,
        "data": values_to_json(data),
        "raw": raw,
        "resolution_hints": hints_json,
    })
}

/// Human-readable type name for error messages.
fn type_name(v: &Value) -> &'static str {
    match v {
        Value::String(_) => "string",
        Value::Int(_) => "integer",
        Value::Float(_) => "float",
        Value::Bool(_) => "boolean",
        Value::DateTime(_) => "datetime",
        Value::Duration(_) => "duration",
        Value::Array(_) => "array",
        Value::Null => "null",
    }
}

/// Human-readable type name for JSON values in error messages.
fn json_type_name(v: &serde_json::Value) -> &'static str {
    match v {
        serde_json::Value::String(_) => "string",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Null => "null",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::definitions::{CompiledFieldMapping, CompiledSource, CompiledSourceEvent};
    use chrono::Datelike;
    use serde_json::json;

    /// Build a minimal source for testing.
    fn test_source() -> CompiledSource {
        let mut mappings = HashMap::new();
        mappings.insert(
            "email".into(),
            CompiledFieldMapping {
                from_field: Some("user_email".into()),
                compute: None,
                type_coercion: None,
                default: None,
            },
        );
        mappings.insert(
            "plan".into(),
            CompiledFieldMapping {
                from_field: Some("subscription_plan".into()),
                compute: None,
                type_coercion: None,
                default: Some(Value::String("free".into())),
            },
        );

        let mut events = HashMap::new();
        events.insert(
            "customer_signup".into(),
            CompiledSourceEvent {
                raw_type: Some("user.created".into()),
                mappings,
                hints: HashMap::from([("customer".into(), vec!["email".into()])]),
            },
        );

        CompiledSource {
            name: "app".into(),
            event_type_field: "type".into(),
            timestamp_field: "created_at".into(),
            event_id_fields: None,
            events,
        }
    }

    fn test_raw_event() -> serde_json::Value {
        json!({
            "type": "user.created",
            "created_at": "2024-01-15T10:00:00Z",
            "user_email": "alice@test.com",
            "subscription_plan": "pro"
        })
    }

    // ── Helper tests ──

    #[test]
    fn resolve_json_path_flat() {
        let v = json!({"email": "alice@test.com"});
        assert_eq!(resolve_json_path(&v, "email"), json!("alice@test.com"));
    }

    #[test]
    fn resolve_json_path_nested() {
        let v = json!({"user": {"address": {"city": "Portland"}}});
        assert_eq!(resolve_json_path(&v, "user.address.city"), json!("Portland"));
    }

    #[test]
    fn resolve_json_path_array_index() {
        let v = json!({"items": [{"id": "a"}, {"id": "b"}]});
        assert_eq!(resolve_json_path(&v, "items.0.id"), json!("a"));
        assert_eq!(resolve_json_path(&v, "items.1.id"), json!("b"));
    }

    #[test]
    fn resolve_json_path_missing() {
        let v = json!({"email": "alice@test.com"});
        assert_eq!(resolve_json_path(&v, "nonexistent"), json!(null));
    }

    #[test]
    fn resolve_json_path_nested_missing() {
        let v = json!({"user": {"name": "Alice"}});
        assert_eq!(resolve_json_path(&v, "user.email"), json!(null));
    }

    #[test]
    fn coerce_string_to_number() {
        assert_eq!(coerce_value(Value::String("42.5".into()), "number").unwrap(), Value::Float(42.5));
    }

    #[test]
    fn coerce_string_to_integer() {
        assert_eq!(coerce_value(Value::String("42".into()), "integer").unwrap(), Value::Int(42));
    }

    #[test]
    fn coerce_string_to_boolean() {
        assert_eq!(coerce_value(Value::String("true".into()), "boolean").unwrap(), Value::Bool(true));
        assert_eq!(coerce_value(Value::String("false".into()), "boolean").unwrap(), Value::Bool(false));
    }

    #[test]
    fn coerce_int_to_string() {
        assert_eq!(coerce_value(Value::Int(42), "string").unwrap(), Value::String("42".into()));
    }

    #[test]
    fn coerce_bad_string_to_number() {
        assert!(coerce_value(Value::String("not_a_number".into()), "number").is_err());
    }

    #[test]
    fn coerce_string_to_datetime() {
        let result = coerce_value(Value::String("2024-01-15T10:00:00Z".into()), "datetime");
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), Value::DateTime(_)));
    }

    #[test]
    fn compute_event_id_deterministic() {
        let data = HashMap::from([("email".into(), Value::String("alice@test.com".into()))]);
        let ts = Utc.with_ymd_and_hms(2024, 1, 15, 10, 0, 0).unwrap();
        let id1 = compute_event_id(&data, None, "signup", &ts);
        let id2 = compute_event_id(&data, None, "signup", &ts);
        assert_eq!(id1, id2);
        assert!(id1.starts_with("e_"));
    }

    #[test]
    fn compute_event_id_with_fields() {
        let data = HashMap::from([
            ("email".into(), Value::String("alice@test.com".into())),
            ("extra".into(), Value::String("ignored".into())),
        ]);
        let ts = Utc.with_ymd_and_hms(2024, 1, 15, 10, 0, 0).unwrap();
        let id_all = compute_event_id(&data, None, "signup", &ts);
        let id_email = compute_event_id(&data, Some(&["email".into()]), "signup", &ts);
        // Different because id_all includes "extra"
        assert_ne!(id_all, id_email);
    }

    #[test]
    fn compute_event_id_different_data() {
        let d1 = HashMap::from([("email".into(), Value::String("alice@test.com".into()))]);
        let d2 = HashMap::from([("email".into(), Value::String("bob@test.com".into()))]);
        let ts = Utc.with_ymd_and_hms(2024, 1, 15, 10, 0, 0).unwrap();
        assert_ne!(compute_event_id(&d1, None, "signup", &ts), compute_event_id(&d2, None, "signup", &ts));
    }

    #[test]
    fn compute_event_id_different_event_types() {
        let data = HashMap::from([("email".into(), Value::String("bob@test.com".into()))]);
        let ts = Utc.with_ymd_and_hms(2024, 1, 15, 10, 0, 0).unwrap();
        let id_signup = compute_event_id(&data, None, "signup", &ts);
        let id_cancel = compute_event_id(&data, None, "cancel", &ts);
        assert_ne!(id_signup, id_cancel);
    }

    #[test]
    fn compute_event_id_ignores_timestamp() {
        // Timestamp is NOT part of the hash — retries with different
        // timestamps should still deduplicate.
        let data = HashMap::from([("email".into(), Value::String("bob@test.com".into()))]);
        let ts1 = Utc.with_ymd_and_hms(2024, 1, 15, 10, 0, 0).unwrap();
        let ts2 = Utc.with_ymd_and_hms(2024, 3, 1, 12, 0, 0).unwrap();
        let id1 = compute_event_id(&data, None, "signup", &ts1);
        let id2 = compute_event_id(&data, None, "signup", &ts2);
        assert_eq!(id1, id2);
    }

    // ── Timestamp tests ──

    #[test]
    fn extract_timestamp_iso8601() {
        let raw = json!({"ts": "2024-01-15T10:00:00Z"});
        let ts = extract_timestamp(&raw, "ts", "test", "test_event").unwrap();
        assert_eq!(ts.year(), 2024);
    }

    #[test]
    fn extract_timestamp_unix_epoch() {
        let raw = json!({"ts": 1705312800});
        let ts = extract_timestamp(&raw, "ts", "test", "test_event").unwrap();
        assert_eq!(ts.year(), 2024);
    }

    #[test]
    fn extract_timestamp_missing() {
        let raw = json!({"other": "value"});
        let result = extract_timestamp(&raw, "ts", "test", "test_event");
        assert!(result.is_err());
        assert!(result.unwrap_err().error.contains("Missing timestamp"));
    }

    #[test]
    fn extract_timestamp_bad_format() {
        let raw = json!({"ts": "not-a-date"});
        let result = extract_timestamp(&raw, "ts", "test", "test_event");
        assert!(result.is_err());
        assert!(result.unwrap_err().error.contains("parse_datetime()"));
    }

    // ── normalize_one tests ──

    #[test]
    fn normalize_one_basic() {
        let source = test_source();
        let lookup = build_type_lookup(&source);
        let raw = test_raw_event();

        let (event, _ledger_row) = normalize_one(&raw, &source, &lookup, &HashMap::new()).unwrap();
        assert_eq!(event.source, "app");
        assert_eq!(event.event_type, "customer_signup");
        assert_eq!(event.data["email"], Value::String("alice@test.com".into()));
        assert_eq!(event.data["plan"], Value::String("pro".into()));
        assert!(event.event_id.starts_with("e_"));
    }

    #[test]
    fn normalize_one_default_value() {
        let source = test_source();
        let lookup = build_type_lookup(&source);
        // Raw event without subscription_plan → default "free" applies
        let raw = json!({
            "type": "user.created",
            "created_at": "2024-01-15T10:00:00Z",
            "user_email": "alice@test.com"
        });

        let (event, _) = normalize_one(&raw, &source, &lookup, &HashMap::new()).unwrap();
        assert_eq!(event.data["plan"], Value::String("free".into()));
    }

    #[test]
    fn normalize_one_hints_extracted() {
        let source = test_source();
        let lookup = build_type_lookup(&source);
        let raw = test_raw_event();

        let (event, _) = normalize_one(&raw, &source, &lookup, &HashMap::new()).unwrap();
        assert_eq!(
            event.resolution_hints["customer"],
            vec![("email".to_string(), "alice@test.com".to_string())]
        );
    }

    #[test]
    fn normalize_one_unknown_event_type() {
        let source = test_source();
        let lookup = build_type_lookup(&source);
        let raw = json!({"type": "unknown.event", "created_at": "2024-01-15T10:00:00Z"});

        let result = normalize_one(&raw, &source, &lookup, &HashMap::new());
        assert!(result.is_err());
        let failure = result.unwrap_err();
        assert!(failure.error.contains("Unknown event type"));
    }

    #[test]
    fn normalize_one_missing_event_type_field() {
        let source = test_source();
        let lookup = build_type_lookup(&source);
        let raw = json!({"created_at": "2024-01-15T10:00:00Z"});

        let result = normalize_one(&raw, &source, &lookup, &HashMap::new());
        assert!(result.is_err());
        assert!(result.unwrap_err().error.contains("Missing"));
    }

    #[test]
    fn normalize_one_ledger_row_format() {
        let source = test_source();
        let lookup = build_type_lookup(&source);
        let raw = test_raw_event();

        let (_, ledger_row) = normalize_one(&raw, &source, &lookup, &HashMap::new()).unwrap();
        // Ledger row should have all required fields
        assert!(ledger_row.get("event_id").is_some());
        assert!(ledger_row.get("timestamp").is_some());
        assert!(ledger_row.get("source").is_some());
        assert!(ledger_row.get("event_type").is_some());
        assert!(ledger_row.get("data").is_some());
        assert!(ledger_row.get("raw").is_some());
        assert!(ledger_row.get("resolution_hints").is_some());
    }

    #[test]
    fn ledger_row_data_is_plain_json() {
        // Data values must serialize as plain JSON ("alice@test.com"),
        // not as tagged enums ({"String": "alice@test.com"}).
        let source = test_source();
        let lookup = build_type_lookup(&source);
        let raw = test_raw_event();

        let (_, ledger_row) = normalize_one(&raw, &source, &lookup, &HashMap::new()).unwrap();
        let email = &ledger_row["data"]["email"];
        assert!(email.is_string(), "data.email should be a plain JSON string, got: {email}");
        assert_eq!(email.as_str().unwrap(), "alice@test.com");
    }

    #[test]
    fn ledger_row_raw_is_object() {
        // Raw field must be a JSON object (dict), not a JSON string.
        // A string would mean double-encoding: '"{\"email\": ...}"'
        let source = test_source();
        let lookup = build_type_lookup(&source);
        let raw = test_raw_event();

        let (_, ledger_row) = normalize_one(&raw, &source, &lookup, &HashMap::new()).unwrap();
        assert!(
            ledger_row["raw"].is_object(),
            "raw should be a JSON object, got: {}",
            ledger_row["raw"]
        );
    }

    // ── Batch tests ──

    #[test]
    fn normalize_batch_all_succeed() {
        let source = test_source();
        let raw_events = vec![test_raw_event(), test_raw_event()];
        let result = normalize(&source, &HashMap::new(), &HashMap::new(), &raw_events).unwrap();
        assert_eq!(result.events.len(), 2);
        assert_eq!(result.ledger_rows.len(), 2);
        assert!(result.failures.is_empty());
    }

    #[test]
    fn normalize_batch_partial_failure() {
        let source = test_source();
        let raw_events = vec![
            test_raw_event(),
            json!({"type": "unknown", "created_at": "2024-01-15T10:00:00Z"}),
        ];
        let result = normalize(&source, &HashMap::new(), &HashMap::new(), &raw_events).unwrap();
        assert_eq!(result.events.len(), 1);
        assert_eq!(result.failures.len(), 1);
    }

    #[test]
    fn normalize_batch_all_fail() {
        let source = test_source();
        let raw_events = vec![
            json!({"type": "unknown", "created_at": "2024-01-15T10:00:00Z"}),
            json!({"no_type_field": true}),
        ];
        let result = normalize(&source, &HashMap::new(), &HashMap::new(), &raw_events).unwrap();
        assert!(result.events.is_empty());
        assert_eq!(result.failures.len(), 2);
    }

    #[test]
    fn normalize_batch_empty() {
        let source = test_source();
        let result = normalize(&source, &HashMap::new(), &HashMap::new(), &[]).unwrap();
        assert!(result.events.is_empty());
        assert!(result.failures.is_empty());
    }

    // ── Identity normalize tests ──

    #[test]
    fn extract_hints_applies_normalize_expression() {
        use crate::definitions::entity::{CompiledEntity, CompiledIdentityField};

        let source = test_source();
        let lookup = build_type_lookup(&source);
        let raw = json!({
            "type": "user.created",
            "created_at": "2024-01-15T10:00:00Z",
            "user_email": "Alice@TEST.com"
        });

        // Build entity with identity normalize: str::to_lowercase(value)
        let normalize_expr = crate::expression::parser::parse("str::to_lowercase(value)")
            .expect("parse normalize expression");
        let entity = CompiledEntity {
            name: "customer".into(),
            starts: "lead".into(),
            states: HashMap::new(),
            properties: HashMap::new(),
            identity: vec![CompiledIdentityField {
                field_name: "email".into(),
                normalize: Some(normalize_expr),
                match_strategy: "exact".into(),
            }],
            relationships: vec![],
            always_handlers: HashMap::new(),
        };
        let entities = HashMap::from([("customer".into(), entity)]);

        let (event, _) = normalize_one(&raw, &source, &lookup, &entities).unwrap();
        // Hint value should be lowercased by the normalize expression
        assert_eq!(
            event.resolution_hints["customer"],
            vec![("email".to_string(), "alice@test.com".to_string())]
        );
    }

    #[test]
    fn extract_hints_without_normalize_passes_raw() {
        let source = test_source();
        let lookup = build_type_lookup(&source);
        let raw = json!({
            "type": "user.created",
            "created_at": "2024-01-15T10:00:00Z",
            "user_email": "Alice@TEST.com"
        });

        // No entity definitions → no normalize → raw value preserved
        let (event, _) = normalize_one(&raw, &source, &lookup, &HashMap::new()).unwrap();
        assert_eq!(
            event.resolution_hints["customer"],
            vec![("email".to_string(), "Alice@TEST.com".to_string())]
        );
    }

    #[test]
    fn extract_hints_missing_entity_def_no_crash() {
        let source = test_source();
        let lookup = build_type_lookup(&source);
        let raw = test_raw_event();

        // Entity definitions exist but for a different type → graceful fallback
        let entities = HashMap::from([("not_customer".into(), CompiledEntity {
            name: "not_customer".into(),
            starts: "start".into(),
            states: HashMap::new(),
            properties: HashMap::new(),
            identity: vec![],
            relationships: vec![],
            always_handlers: HashMap::new(),
        })]);

        let (event, _) = normalize_one(&raw, &source, &lookup, &entities).unwrap();
        // Should still extract hints, just without normalization
        assert!(!event.resolution_hints["customer"].is_empty());
    }

    // ── renormalize tests ──

    #[test]
    fn renormalize_preserves_sequences() {
        let source = test_source();
        let raw1 = test_raw_event();
        let raw2 = json!({
            "type": "user.created",
            "created_at": "2024-01-16T10:00:00Z",
            "user_email": "bob@test.com"
        });

        let results = renormalize(&source, &HashMap::new(), &HashMap::new(), &[
            (42, raw1),
            (99, raw2),
        ]);

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, 42);
        assert_eq!(results[1].0, 99);
        assert!(results[0].1.is_ok());
        assert!(results[1].1.is_ok());
    }

    #[test]
    fn renormalize_same_output_as_normalize() {
        let source = test_source();
        let raw = test_raw_event();

        let norm_result = normalize(&source, &HashMap::new(), &HashMap::new(), &[raw.clone()]).unwrap();
        let renorm_result = renormalize(&source, &HashMap::new(), &HashMap::new(), &[(1, raw)]);

        let norm_row = &norm_result.ledger_rows[0];
        let renorm_row = renorm_result[0].1.as_ref().unwrap();
        assert_eq!(norm_row["event_id"], renorm_row["event_id"]);
        assert_eq!(norm_row["data"], renorm_row["data"]);
        assert_eq!(norm_row["resolution_hints"], renorm_row["resolution_hints"]);
    }

    #[test]
    fn renormalize_failure_includes_sequence() {
        let source = test_source();
        let bad_raw = json!({"no_type_field": true});

        let results = renormalize(&source, &HashMap::new(), &HashMap::new(), &[(77, bad_raw)]);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 77);
        assert!(results[0].1.is_err());
    }
}
