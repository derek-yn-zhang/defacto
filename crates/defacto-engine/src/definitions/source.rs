//! Compiled source definitions — how raw input becomes normalized events.
//!
//! Sources define the mapping from external system formats to Defacto's
//! internal event format. Each source knows how to extract event types,
//! timestamps, and fields from raw input records.
//!
//! Compiled from YAML/dict at definition registration time. Used during
//! normalization to transform raw input into NormalizedEvents.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::expression::Expression;
use crate::types::Value;

/// A compiled source — how one external system's data is normalized.
///
/// Each source (e.g., "app", "stripe") has its own event type field,
/// timestamp field, and per-event-type mappings that extract and transform
/// fields from the raw input.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompiledSource {
    /// Source name (e.g., "app"). Used in `m.ingest(source_name, ...)`.
    pub name: String,
    /// Which raw field contains the event type (e.g., "type").
    pub event_type_field: String,
    /// Which raw field contains the timestamp (e.g., "created_at").
    pub timestamp_field: String,
    /// Fields used to compute content-based event ID. None = hash all fields.
    pub event_id_fields: Option<Vec<String>>,
    /// Per-event-type configurations. Key = Defacto event type name.
    pub events: HashMap<String, CompiledSourceEvent>,
}

/// Configuration for one event type within a source.
///
/// Maps from raw input format to normalized event fields, and provides
/// identity resolution hints.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompiledSourceEvent {
    /// What the source calls this event type (e.g., "user.created").
    /// None = same as the Defacto event type name.
    pub raw_type: Option<String>,
    /// Field mappings — how to extract each normalized field from raw input.
    pub mappings: HashMap<String, CompiledFieldMapping>,
    /// Identity hints — which fields to use for identity resolution.
    /// Key = entity type (e.g., "customer"), value = field names (e.g., ["email"]).
    pub hints: HashMap<String, Vec<String>>,
}

/// How to extract one field from raw input during normalization.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompiledFieldMapping {
    /// Source field name. Supports dot notation for nested access (e.g., "user.email").
    /// None if `compute` is used instead.
    pub from_field: Option<String>,
    /// Computed value — a pre-compiled expression (e.g., `str::to_lowercase(event.email)`).
    /// Mutually exclusive with `from_field`.
    pub compute: Option<Expression>,
    /// Type coercion to apply after extraction (e.g., "number", "boolean").
    pub type_coercion: Option<String>,
    /// Default value if the source field is missing or null.
    pub default: Option<Value>,
}
