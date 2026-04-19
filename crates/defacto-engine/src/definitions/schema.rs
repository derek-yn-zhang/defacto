//! Compiled event schemas — field validation rules.
//!
//! Schemas define what an event type looks like: its fields, their types,
//! and validation constraints. They're the contract between sources (which
//! produce events) and entities (which consume them).
//!
//! Compiled from YAML/dict at definition registration time. Used during
//! normalization to validate event fields before they enter the ledger.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::types::Value;

/// A compiled event schema — validation rules for one event type.
///
/// Created during `CompiledDefinitions::compile()`. Each schema corresponds
/// to one event type (e.g., "customer_signup") and defines what fields
/// that event must/may contain and their constraints.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompiledSchema {
    /// Event type name (e.g., "customer_signup").
    pub name: String,
    /// Field definitions. Key = field name.
    pub fields: HashMap<String, CompiledSchemaField>,
}

/// Validation rules for a single field in an event schema.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompiledSchemaField {
    /// Expected type: "string", "number", "integer", "boolean", "datetime".
    pub field_type: String,
    /// Whether this field must be present and non-null.
    pub required: bool,
    /// Allowed values — if set, the field value must be one of these.
    pub allowed: Option<Vec<Value>>,
    /// Minimum numeric value.
    pub min: Option<f64>,
    /// Maximum numeric value.
    pub max: Option<f64>,
    /// Minimum string length.
    pub min_length: Option<usize>,
    /// Regex pattern the string value must match.
    pub regex: Option<String>,
}
