//! Type conversions between Python and Rust.
//!
//! Every time data crosses the PyO3 boundary, it goes through these
//! conversion functions. Python dicts become `serde_json::Value` (for raw
//! event processing) or `HashMap<String, Value>` (for expression evaluation
//! contexts). Rust Values become Python objects for return values.
//!
//! Two conversion paths exist because raw events have nested structure
//! (dicts within dicts, arrays of dicts) that our `Value` enum doesn't
//! represent — `Value` has no `Map` variant. So raw events use `serde_json::Value`
//! which handles arbitrary JSON natively, while expression evaluation contexts
//! use flat `HashMap<String, Value>` maps.
//!
//! The conversion overhead is ~100μs for 100 events — negligible compared
//! to the ~2ms Postgres round trips that happen between Rust calls.

use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyFloat, PyInt, PyList, PyString};

use defacto_engine::types::{EntitySnapshot, Value};

// ===========================================================================
// Python → serde_json::Value (for raw event processing)
// ===========================================================================

/// Convert a Python dict to a `serde_json::Value` for raw event processing.
///
/// Supports nested dicts, lists, and all Python scalar types. This is the
/// primary conversion path for raw events entering the normalizer — raw events
/// have arbitrary nested structure that `serde_json::Value` handles natively.
pub fn py_dict_to_json(dict: &Bound<'_, PyDict>) -> PyResult<serde_json::Value> {
    let mut map = serde_json::Map::new();
    for (key, value) in dict.iter() {
        let key_str: String = key.extract()?;
        map.insert(key_str, py_to_json(&value)?);
    }
    Ok(serde_json::Value::Object(map))
}

/// Convert a single Python object to `serde_json::Value`.
///
/// Type dispatch order matters: bool must be checked before int because
/// Python's `bool` is a subclass of `int` (`isinstance(True, int)` is True).
///
/// Public because `DefactoCore::normalize()` calls this to convert individual
/// raw event objects (which may be dicts or other Python types).
pub fn py_to_json(obj: &Bound<'_, PyAny>) -> PyResult<serde_json::Value> {
    // None → null
    if obj.is_none() {
        return Ok(serde_json::Value::Null);
    }

    // Bool before int — Python bool is a subclass of int
    if obj.is_instance_of::<PyBool>() {
        let b: bool = obj.extract()?;
        return Ok(serde_json::Value::Bool(b));
    }

    // Int → JSON number
    if obj.is_instance_of::<PyInt>() {
        let i: i64 = obj.extract()?;
        return Ok(serde_json::json!(i));
    }

    // Float → JSON number
    if obj.is_instance_of::<PyFloat>() {
        let f: f64 = obj.extract()?;
        return Ok(serde_json::json!(f));
    }

    // String → JSON string
    if obj.is_instance_of::<PyString>() {
        let s: String = obj.extract()?;
        return Ok(serde_json::Value::String(s));
    }

    // List → JSON array (recursive)
    if obj.is_instance_of::<PyList>() {
        let list = obj.downcast::<PyList>()?;
        let items: Vec<serde_json::Value> = list
            .iter()
            .map(|item| py_to_json(&item))
            .collect::<PyResult<_>>()?;
        return Ok(serde_json::Value::Array(items));
    }

    // Dict → JSON object (recursive)
    if obj.is_instance_of::<PyDict>() {
        let dict = obj.downcast::<PyDict>()?;
        return py_dict_to_json(dict);
    }

    // Fallback: convert via str() representation
    let repr: String = obj.str()?.extract()?;
    Ok(serde_json::Value::String(repr))
}

// ===========================================================================
// Python → HashMap<String, Value> (for expression evaluation contexts)
// ===========================================================================

/// Convert a Python dict to a flat `HashMap<String, Value>`.
///
/// Used when building expression evaluation contexts (event fields, entity
/// properties). These are flat maps — nested structure is accessed via
/// dot-path field references in the expression language, not via nested maps.
pub fn py_dict_to_value(dict: &Bound<'_, PyDict>) -> PyResult<HashMap<String, Value>> {
    let mut map = HashMap::new();
    for (key, value) in dict.iter() {
        let key_str: String = key.extract()?;
        map.insert(key_str, py_to_value(&value)?);
    }
    Ok(map)
}

/// Convert a single Python object to a Defacto `Value`.
fn py_to_value(obj: &Bound<'_, PyAny>) -> PyResult<Value> {
    if obj.is_none() {
        return Ok(Value::Null);
    }
    if obj.is_instance_of::<PyBool>() {
        return Ok(Value::Bool(obj.extract()?));
    }
    if obj.is_instance_of::<PyInt>() {
        return Ok(Value::Int(obj.extract()?));
    }
    if obj.is_instance_of::<PyFloat>() {
        return Ok(Value::Float(obj.extract()?));
    }
    if obj.is_instance_of::<PyString>() {
        return Ok(Value::String(obj.extract()?));
    }
    if obj.is_instance_of::<PyList>() {
        let list = obj.downcast::<PyList>()?;
        let items: Vec<Value> = list
            .iter()
            .map(|item| py_to_value(&item))
            .collect::<PyResult<_>>()?;
        return Ok(Value::Array(items));
    }
    // Nested dicts become Null in Value — access nested fields via dot-paths
    Ok(Value::Null)
}

// ===========================================================================
// Value → Python (for returning results to Python)
// ===========================================================================

/// Convert a Rust `Value` to a Python object.
///
/// Used when returning results from Rust to Python — ledger rows, entity
/// snapshots, validation results. Each Value variant maps to its natural
/// Python counterpart.
pub fn value_to_py_object(py: Python<'_>, value: &Value) -> PyResult<PyObject> {
    match value {
        Value::String(s) => Ok(s.into_pyobject(py)?.into_any().unbind()),
        Value::Int(i) => Ok(i.into_pyobject(py)?.into_any().unbind()),
        Value::Float(f) => Ok(f.into_pyobject(py)?.into_any().unbind()),
        Value::Bool(b) => Ok(PyBool::new(py, *b).to_owned().into_any().unbind()),
        Value::Null => Ok(py.None()),
        Value::DateTime(dt) => {
            // Return as ISO 8601 string — simpler than constructing Python datetime
            let iso = dt.to_rfc3339();
            Ok(iso.into_pyobject(py)?.into_any().unbind())
        }
        Value::Duration(d) => {
            // Return as total seconds float — simpler than Python timedelta
            Ok(d.as_secs_f64().into_pyobject(py)?.into_any().unbind())
        }
        Value::Array(items) => {
            let py_items: Vec<PyObject> = items
                .iter()
                .map(|item| value_to_py_object(py, item))
                .collect::<PyResult<_>>()?;
            Ok(PyList::new(py, &py_items)?.into_any().unbind())
        }
    }
}

// ===========================================================================
// EntitySnapshot → Python dict (for interpretation results)
// ===========================================================================

/// Convert an EntitySnapshot to a Python dict.
///
/// Used when returning interpretation results to Python. The snapshot
/// becomes a dict with entity_id, entity_type, state, properties (dict),
/// relationships (list of dicts), and timing fields (ISO 8601 strings or None).
pub fn snapshot_to_py_dict(py: Python<'_>, snapshot: &EntitySnapshot) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("entity_id", &snapshot.entity_id)?;
    dict.set_item("entity_type", &snapshot.entity_type)?;
    dict.set_item("state", &snapshot.state)?;

    // Convert properties HashMap<String, Value> → Python dict
    let props_dict = PyDict::new(py);
    for (key, value) in &snapshot.properties {
        props_dict.set_item(key, value_to_py_object(py, value)?)?;
    }
    dict.set_item("properties", props_dict)?;

    // Convert relationships Vec<RelationshipSnapshot> → Python list of dicts
    let mut rel_items: Vec<PyObject> = Vec::with_capacity(snapshot.relationships.len());
    for rel in &snapshot.relationships {
        let rel_dict = PyDict::new(py);
        rel_dict.set_item("relationship_type", &rel.relationship_type)?;
        rel_dict.set_item("target_entity_id", &rel.target_entity_id)?;
        let rel_props = PyDict::new(py);
        for (key, value) in &rel.properties {
            rel_props.set_item(key, value_to_py_object(py, value)?)?;
        }
        rel_dict.set_item("properties", rel_props)?;
        rel_items.push(rel_dict.into_any().unbind());
    }
    let rels_list = PyList::new(py, &rel_items)?;
    dict.set_item("relationships", rels_list)?;

    // Timing fields — ISO 8601 strings or None. These flow through to state
    // history for cold start recovery and analytics queries.
    let to_iso = |dt: &Option<chrono::DateTime<chrono::Utc>>| -> PyObject {
        match dt {
            Some(d) => d.to_rfc3339().into_pyobject(py).unwrap().into_any().unbind(),
            None => py.None(),
        }
    };
    dict.set_item("last_event_time", to_iso(&snapshot.last_event_time))?;
    dict.set_item("state_entered_time", to_iso(&snapshot.state_entered_time))?;
    dict.set_item("created_time", to_iso(&snapshot.created_time))?;

    Ok(dict.into_any().unbind())
}
