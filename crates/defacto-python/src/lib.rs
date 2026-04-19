//! PyO3 bindings — exposes the Rust computation core to Python as `defacto._core`.
//!
//! This module defines `DefactoCore`, the single Python-visible class that owns
//! all Rust computation state. Python calls methods on this class for normalization,
//! interpretation, time rule evaluation, and entity state management.
//!
//! All computation methods release the GIL (`py.allow_threads()`) so Rayon
//! workers can run in parallel. The GIL is reacquired only when converting
//! results back to Python objects.

mod convert;

use std::collections::HashMap;
use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use defacto_engine::definitions::CompiledDefinitions;
use defacto_engine::store::Store;
use defacto_engine::types::NormalizedEvent;

/// Rust computation core — owns entity state, compiled definitions,
/// identity cache, and the worker thread pool.
///
/// Created from Python via `DefactoCore(workers=4)`. All computation methods
/// release the GIL and use the internal Rayon thread pool for parallelism.
///
/// This is the only Rust class visible to Python. It corresponds to the
/// `_core.pyi` type stubs that the Python orchestration layer depends on.
#[pyclass]
struct DefactoCore {
    /// In-memory entity state, relationships, identity cache, batch tracking.
    store: Store,
    /// Compiled definitions per version. Key = version name (e.g., "v1").
    /// Arc for lock-free sharing across worker threads during interpretation.
    definitions: HashMap<String, Arc<CompiledDefinitions>>,
    /// Currently active version name. Set by `activate()`.
    active_version: Option<String>,
    /// Normalized events held between normalize() and interpret() calls.
    /// Cleared after interpret() consumes them.
    normalized_events: Vec<NormalizedEvent>,
    /// Rayon thread pool for parallel computation. Sized by `workers` parameter.
    pool: rayon::ThreadPool,
}

#[pymethods]
impl DefactoCore {
    /// Create a new DefactoCore with a Rayon thread pool of the given size.
    #[new]
    #[pyo3(signature = (workers=1))]
    fn new(workers: usize) -> PyResult<Self> {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(workers)
            .build()
            .map_err(|e: rayon::ThreadPoolBuildError| {
                pyo3::exceptions::PyRuntimeError::new_err(e.to_string())
            })?;

        Ok(Self {
            store: Store::new(),
            definitions: HashMap::new(),
            active_version: None,
            normalized_events: Vec::new(),
            pool,
        })
    }

    // ── Definition management ──

    /// Compile definitions dict into optimized internal representation.
    ///
    /// Converts the Python dict to JSON, then compiles into Rust structs with
    /// pre-parsed expression ASTs and computed hashes. Stores the compiled
    /// definitions under the version key for later activation.
    fn compile(&mut self, version: String, definitions: &Bound<'_, PyDict>) -> PyResult<()> {
        // Convert Python dict → JSON string → serde_json::Value.
        // Using Python's json.dumps is simple and correct for a once-per-registration
        // operation. The JSON round-trip cost is negligible.
        let json_module = definitions.py().import("json")?;
        let json_str: String = json_module
            .call_method1("dumps", (definitions,))?
            .extract()?;
        let value: serde_json::Value = serde_json::from_str(&json_str)
            .map_err(|e| to_py_err(defacto_engine::error::DefactoError::Definition(
                format!("Failed to parse definitions JSON: {e}")
            )))?;

        let compiled = CompiledDefinitions::compile(&value).map_err(to_py_err)?;
        self.definitions.insert(version, Arc::new(compiled));
        Ok(())
    }

    /// Set the active version for interpretation.
    fn activate(&mut self, version: String) -> PyResult<()> {
        if !self.definitions.contains_key(&version) {
            return Err(pyo3::exceptions::PyValueError::new_err(
                format!("Version '{version}' has not been compiled")
            ));
        }
        self.active_version = Some(version);
        Ok(())
    }

    // ── Normalization ──

    /// Normalize raw events through source definitions.
    ///
    /// Converts Python dicts to JSON, runs normalization in Rust (with GIL
    /// released for Rayon parallelism), stores normalized events internally
    /// for the subsequent interpret() call, and returns ledger rows + failures
    /// to Python for database writes.
    ///
    /// Returns a dict with:
    ///   "ledger_rows": list[dict] — fields needed for database write
    ///   "failures": list[dict] — events that failed normalization
    ///   "count": int — number of successfully normalized events
    fn normalize(&mut self, py: Python<'_>, source: String, raw_events: Vec<Bound<'_, PyAny>>) -> PyResult<PyObject> {
        // Get compiled definitions for the active version
        let active = self.active_version.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("No active version — call activate() first")
        })?;
        let compiled = self.definitions.get(active).ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err(
                format!("Active version '{}' not found in compiled definitions", active)
            )
        })?;
        let compiled = compiled.clone(); // Arc clone — cheap, needed to move into closure

        let source_def = compiled.sources.get(&source).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(
                format!("Source '{}' not found in definitions", source)
            )
        })?.clone();

        let schemas = compiled.schemas.clone();
        let entities = compiled.entities.clone();

        // Convert Python dicts to serde_json::Value
        let json_events: Vec<serde_json::Value> = raw_events
            .iter()
            .map(|event| convert::py_to_json(event))
            .collect::<PyResult<_>>()?;

        // Release GIL and run normalization — Rayon parallelism is available
        let result = py.allow_threads(|| {
            defacto_engine::normalizer::normalize(&source_def, &schemas, &entities, &json_events)
        }).map_err(to_py_err)?;

        let event_count = result.events.len();

        // Store normalized events for the subsequent interpret() call
        self.normalized_events = result.events;

        // Convert ledger rows to Python list.
        // serde_json::Value → JSON string → Python json.loads.
        let json_module = py.import("json")?;
        let mut ledger_items: Vec<PyObject> = Vec::with_capacity(result.ledger_rows.len());
        for row in &result.ledger_rows {
            let json_str = serde_json::to_string(row).unwrap_or_default();
            let py_obj = json_module.call_method1("loads", (json_str,))?;
            ledger_items.push(py_obj.unbind());
        }
        let ledger_list = PyList::new(py, &ledger_items)?;

        // Convert failures to Python list
        let mut failure_items: Vec<PyObject> = Vec::with_capacity(result.failures.len());
        for f in &result.failures {
            let dict = PyDict::new(py);
            let raw_str = serde_json::to_string(&f.raw).unwrap_or_default();
            dict.set_item("raw", raw_str)?;
            dict.set_item("error", &f.error)?;
            dict.set_item("source", &f.source)?;
            dict.set_item("handler", f.handler.as_deref())?;
            dict.set_item("field", f.field.as_deref())?;
            failure_items.push(dict.into_any().unbind());
        }
        let failures_list = PyList::new(py, &failure_items)?;

        // Build return dict
        let result_dict = PyDict::new(py);
        result_dict.set_item("ledger_rows", ledger_list)?;
        result_dict.set_item("failures", failures_list)?;
        result_dict.set_item("count", event_count)?;

        Ok(result_dict.into_any().unbind())
    }

    // ── Re-normalization (FULL_RENORMALIZE build mode) ──

    /// Re-normalize ledger events from raw, preserving sequence correlation.
    ///
    /// Takes a list of dicts with {sequence: int, source: str, raw: dict}.
    /// Groups by source, normalizes each group in parallel (Rayon), and
    /// returns results paired with their sequence numbers so the caller
    /// can update the correct ledger rows.
    ///
    /// Returns a dict with:
    ///   "successes": list of (sequence, ledger_row_dict) tuples
    ///   "failures": list of (sequence, error_string) tuples
    fn renormalize(&mut self, py: Python<'_>, events: Vec<Bound<'_, PyAny>>) -> PyResult<PyObject> {
        let active = self.active_version.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("No active version — call activate() first")
        })?;
        let compiled = self.definitions.get(active).ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err(
                format!("Active version '{}' not found in compiled definitions", active)
            )
        })?.clone();

        // Extract (sequence, source, raw) and group by source
        let mut by_source: std::collections::HashMap<String, Vec<(i64, serde_json::Value)>> =
            std::collections::HashMap::new();

        for event in &events {
            let dict = event.downcast::<PyDict>()?;

            let seq: i64 = dict
                .get_item("sequence")?
                .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err("Missing 'sequence'"))?
                .extract()?;

            let source: String = dict
                .get_item("source")?
                .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err("Missing 'source'"))?
                .extract()?;

            let raw_obj = dict
                .get_item("raw")?
                .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err("Missing 'raw'"))?;
            let raw = convert::py_to_json(&raw_obj)?;

            by_source.entry(source).or_default().push((seq, raw));
        }

        // Process each source group with GIL released
        let mut all_successes: Vec<(i64, serde_json::Value)> = Vec::new();
        let mut all_failures: Vec<(i64, String)> = Vec::new();

        for (source_name, source_events) in &by_source {
            if let Some(source_def) = compiled.sources.get(source_name) {
                let source_def = source_def.clone();
                let schemas = compiled.schemas.clone();
                let entities = compiled.entities.clone();

                let results = py.allow_threads(|| {
                    defacto_engine::normalizer::renormalize(
                        &source_def, &schemas, &entities, source_events,
                    )
                });

                for (seq, result) in results {
                    match result {
                        Ok(ledger_row) => all_successes.push((seq, ledger_row)),
                        Err(error) => all_failures.push((seq, error)),
                    }
                }
            } else {
                // Source no longer exists in definitions — all events fail
                for (seq, _) in source_events {
                    all_failures.push((
                        *seq,
                        format!("Source '{}' not found in current definitions", source_name),
                    ));
                }
            }
        }

        // Detect duplicate event_ids and embed duplicate_of into each row.
        // When event_id_fields change, previously-distinct events may now
        // produce the same event_id. The earliest sequence (canonical) gets
        // duplicate_of: null. Later sequences get duplicate_of: canonical_seq.
        // This way Python receives one unified data structure — no merging.
        let mut canonical: std::collections::HashMap<String, i64> =
            std::collections::HashMap::with_capacity(all_successes.len());
        let mut dup_map: std::collections::HashMap<i64, i64> =
            std::collections::HashMap::new();

        // Sort by sequence so lowest sequence is always seen first
        all_successes.sort_by_key(|(seq, _)| *seq);

        for (seq, row) in &all_successes {
            if let Some(eid) = row.get("event_id").and_then(|v| v.as_str()) {
                match canonical.entry(eid.to_owned()) {
                    std::collections::hash_map::Entry::Vacant(e) => { e.insert(*seq); }
                    std::collections::hash_map::Entry::Occupied(e) => {
                        dup_map.insert(*seq, *e.get());
                    }
                }
            }
        }

        // Set duplicate_of on each row
        for (seq, row) in &mut all_successes {
            if let Some(&canon_seq) = dup_map.get(seq) {
                row.as_object_mut().unwrap().insert(
                    "duplicate_of".to_owned(),
                    serde_json::Value::Number(canon_seq.into()),
                );
            } else {
                row.as_object_mut().unwrap().insert(
                    "duplicate_of".to_owned(),
                    serde_json::Value::Null,
                );
            }
        }

        // Convert successes to Python: list of (int, dict) tuples
        let json_module = py.import("json")?;
        let mut success_items: Vec<PyObject> = Vec::with_capacity(all_successes.len());
        for (seq, row) in &all_successes {
            let json_str = serde_json::to_string(row).unwrap_or_default();
            let py_row = json_module.call_method1("loads", (json_str,))?;
            let tuple = pyo3::types::PyTuple::new(py, &[seq.into_pyobject(py)?.into_any(), py_row.into_any()])?;
            success_items.push(tuple.into_any().unbind());
        }
        let success_list = PyList::new(py, &success_items)?;

        // Convert failures to Python: list of (int, str) tuples
        let mut failure_items: Vec<PyObject> = Vec::with_capacity(all_failures.len());
        for (seq, error) in &all_failures {
            let tuple = pyo3::types::PyTuple::new(py, &[
                seq.into_pyobject(py)?.into_any(),
                error.into_pyobject(py)?.into_any(),
            ])?;
            failure_items.push(tuple.into_any().unbind());
        }
        let failure_list = PyList::new(py, &failure_items)?;

        let result_dict = PyDict::new(py);
        result_dict.set_item("successes", success_list)?;
        result_dict.set_item("failures", failure_list)?;

        Ok(result_dict.into_any().unbind())
    }

    // ── Event loading (for builds — events from ledger, not normalize) ──

    /// Load pre-normalized events from the ledger into Rust memory.
    ///
    /// Used during builds (INCREMENTAL, FULL, PARTIAL) when events are replayed
    /// from the ledger rather than freshly normalized. Converts Python dicts to
    /// NormalizedEvent structs and stores them for the subsequent interpret() call.
    ///
    /// Each event dict must have: event_id, event_type, timestamp, source,
    /// data, raw, resolution_hints.
    fn load_events(&mut self, _py: Python<'_>, events: Vec<Bound<'_, PyAny>>) -> PyResult<()> {
        let mut normalized = Vec::with_capacity(events.len());

        for event in &events {
            let dict = event.downcast::<PyDict>()?;

            let event_id: String = dict
                .get_item("event_id")?
                .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err("Missing 'event_id'"))?
                .extract()?;

            let event_type: String = dict
                .get_item("event_type")?
                .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err("Missing 'event_type'"))?
                .extract()?;

            let timestamp_str: String = dict
                .get_item("timestamp")?
                .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err("Missing 'timestamp'"))?
                .extract()?;

            let source: String = dict
                .get_item("source")?
                .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err("Missing 'source'"))?
                .extract()?;

            // Parse ISO 8601 timestamp
            let timestamp = chrono::DateTime::parse_from_rfc3339(&timestamp_str)
                .map(|dt| dt.with_timezone(&chrono::Utc))
                .map_err(|e| {
                    pyo3::exceptions::PyValueError::new_err(format!(
                        "Invalid timestamp '{timestamp_str}': {e}"
                    ))
                })?;

            // Convert data: Python dict → HashMap<String, Value>
            let data_obj = dict
                .get_item("data")?
                .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err("Missing 'data'"))?;
            let data_dict = data_obj.downcast::<PyDict>()?;
            let data = convert::py_dict_to_value(data_dict)?;

            // Convert raw: Python dict → JSON string
            let raw_obj = dict
                .get_item("raw")?
                .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err("Missing 'raw'"))?;
            let raw_json = convert::py_to_json(&raw_obj)?;
            let raw = serde_json::to_string(&raw_json).unwrap_or_default();

            // Convert resolution_hints: {entity_type: {field_name: value}}
            let hints_obj = dict
                .get_item("resolution_hints")?
                .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err("Missing 'resolution_hints'"))?;
            let hints_dict = hints_obj.downcast::<PyDict>()?;
            let mut resolution_hints = HashMap::new();
            for (key, value) in hints_dict.iter() {
                let entity_type: String = key.extract()?;
                // Hints are stored as {field_name: hint_value} dicts
                let fields_dict = value.downcast::<PyDict>()?;
                let mut pairs: Vec<(String, String)> = Vec::new();
                for (fk, fv) in fields_dict.iter() {
                    pairs.push((fk.extract()?, fv.extract()?));
                }
                resolution_hints.insert(entity_type, pairs);
            }

            normalized.push(NormalizedEvent {
                event_id,
                timestamp,
                source,
                event_type,
                data,
                raw,
                resolution_hints,
            });
        }

        self.normalized_events = normalized;
        Ok(())
    }

    // ── Interpretation ──

    /// Interpret normalized events against entity state machines.
    ///
    /// Takes (event_index, entity_id, entity_type) tuples connecting the
    /// internally-held events to their resolved entity IDs and types.
    /// Consumes the normalized events (clears them after interpretation).
    /// Returns a dict with "snapshots" (succeeded) and "failures" (errored).
    /// Failed entities are skipped — their state is unchanged. The batch
    /// never fails as a whole.
    fn interpret(&mut self, py: Python<'_>, entity_mapping: Vec<(usize, String, String)>) -> PyResult<PyObject> {
        let active = self.active_version.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("No active version — call activate() first")
        })?;
        let compiled = self.definitions.get(active).ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err(
                format!("Active version '{}' not found in compiled definitions", active)
            )
        })?.clone();

        // Take normalized events — consumed by interpretation, cleared for next batch
        let events = std::mem::take(&mut self.normalized_events);

        // Release GIL and run interpretation with Rayon parallelism
        let result = py.allow_threads(|| {
            defacto_engine::interpreter::interpret(&events, &entity_mapping, &self.store, &compiled)
        });

        // Convert snapshots to Python dicts
        let snapshot_items: Vec<PyObject> = result.snapshots
            .iter()
            .map(|snap| convert::snapshot_to_py_dict(py, snap))
            .collect::<PyResult<_>>()?;
        let snapshot_list = PyList::new(py, &snapshot_items)?;

        // Convert failures to Python dicts
        let mut failure_items: Vec<PyObject> = Vec::with_capacity(result.failures.len());
        for f in &result.failures {
            let dict = PyDict::new(py);
            dict.set_item("entity_id", &f.entity_id)?;
            dict.set_item("entity_type", &f.entity_type)?;
            dict.set_item("error", &f.error)?;
            failure_items.push(dict.into_any().unbind());
        }
        let failure_list = PyList::new(py, &failure_items)?;

        let result_dict = PyDict::new(py);
        result_dict.set_item("snapshots", snapshot_list)?;
        result_dict.set_item("failures", failure_list)?;

        Ok(result_dict.into_any().unbind())
    }

    // ── Time rules ──

    /// Evaluate time rules for all entities at the given timestamp.
    ///
    /// Scans all entities, fires any time rules whose thresholds have been
    /// crossed. Returns snapshots for entities that changed.
    fn tick(&self, py: Python<'_>, as_of: String) -> PyResult<PyObject> {
        let timestamp = chrono::DateTime::parse_from_rfc3339(&as_of)
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(
                format!("Invalid ISO 8601 timestamp '{as_of}': {e}")
            ))?;

        let active = self.active_version.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("No active version")
        })?;
        let compiled = self.definitions.get(active).ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("Active version not found")
        })?.clone();

        let snapshots = py.allow_threads(|| {
            defacto_engine::tick::tick(timestamp, &self.store, &compiled)
        }).map_err(to_py_err)?;

        let snapshot_items: Vec<PyObject> = snapshots
            .iter()
            .map(|snap| convert::snapshot_to_py_dict(py, snap))
            .collect::<PyResult<_>>()?;
        let snapshot_list = PyList::new(py, &snapshot_items)?;
        Ok(snapshot_list.into_any().unbind())
    }

    /// Evaluate time rules for specific entities only.
    fn tick_entities(&self, py: Python<'_>, entity_ids: Vec<String>, as_of: String) -> PyResult<PyObject> {
        let timestamp = chrono::DateTime::parse_from_rfc3339(&as_of)
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(
                format!("Invalid ISO 8601 timestamp '{as_of}': {e}")
            ))?;

        let active = self.active_version.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("No active version")
        })?;
        let compiled = self.definitions.get(active).ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("Active version not found")
        })?.clone();

        let snapshots = py.allow_threads(|| {
            defacto_engine::tick::tick_entities(&entity_ids, timestamp, &self.store, &compiled)
        }).map_err(to_py_err)?;

        let snapshot_items: Vec<PyObject> = snapshots
            .iter()
            .map(|snap| convert::snapshot_to_py_dict(py, snap))
            .collect::<PyResult<_>>()?;
        let snapshot_list = PyList::new(py, &snapshot_items)?;
        Ok(snapshot_list.into_any().unbind())
    }

    // ── Identity cache (bulk operations for pipeline efficiency) ──

    /// Bulk identity cache lookup, scoped by entity type.
    ///
    /// Takes (entity_type, hint_value) pairs. Returns a dict with:
    ///   "resolved": list of (entity_type, hint_value, entity_id) — cache hits
    ///   "unresolved": list of (entity_type, hint_value) — cache misses
    fn resolve_cache(&self, py: Python<'_>, hints: Vec<(String, String)>) -> PyResult<PyObject> {
        let mut resolved: Vec<(String, String, String)> = Vec::new();
        let mut unresolved: Vec<(String, String)> = Vec::new();

        for (entity_type, hint_value) in hints {
            if let Some(entity_id) = self.store.get_identity(&entity_type, &hint_value) {
                resolved.push((entity_type, hint_value, entity_id));
            } else {
                unresolved.push((entity_type, hint_value));
            }
        }

        let dict = PyDict::new(py);
        dict.set_item("resolved", resolved)?;
        dict.set_item("unresolved", unresolved)?;
        Ok(dict.into_any().unbind())
    }

    /// Bulk cache warming after identity resolution.
    ///
    /// Loads (entity_type, hint_value, entity_id) triples into the DashMap cache.
    fn update_cache(&self, mappings: Vec<(String, String, String)>) -> PyResult<()> {
        self.store.bulk_load_identity(mappings);
        Ok(())
    }

    /// Remove specific hints from the identity cache.
    ///
    /// Called by erase to keep the cache in sync with the backend after
    /// identity deletion. Takes (entity_type, hint_value) pairs.
    fn remove_from_cache(&self, hints: Vec<(String, String)>) -> PyResult<()> {
        self.store.remove_identity_batch(&hints);
        Ok(())
    }

    // ── Entity state management ──

    /// Merge entity state: absorb from_id into into_id.
    ///
    /// Removes from_id from the store, keeping into_id as the surviving entity.
    /// Returns the surviving entity's snapshot. Property merging and event
    /// replay are handled by the Python Pipeline (PARTIAL rebuild for the
    /// affected entities).
    fn merge(&self, py: Python<'_>, from_id: String, into_id: String) -> PyResult<PyObject> {
        // Remove the merged-away entity, keeping its timing fields
        let loser = self.store.remove_entity(&from_id);

        // Update the winner's timing fields from the loser:
        // - last_event_time: take the more recent (entity was active more recently)
        // - created_time: take the earlier (entity has existed longer)
        // - state_entered_time: keep the winner's (state machine is the winner's)
        //
        // Either entity may not exist in DashMap (ghost entity — identity
        // mapped but never created by a Create effect). Return a minimal
        // snapshot with entity_type from the loser or an empty string.
        // The rebuild post-pass will create the entity from the combined
        // event history.
        let mut entity = match self.store.get_entity(&into_id) {
            Some(e) => e,
            None => {
                // Ghost entity — return minimal snapshot so metadata
                // merge (identity, event_entities, merge_log) proceeds.
                let entity_type = loser.as_ref()
                    .map(|l| l.entity_type.clone())
                    .unwrap_or_default();
                let snapshot = defacto_engine::types::EntitySnapshot {
                    entity_id: into_id,
                    entity_type,
                    state: String::new(),
                    properties: HashMap::new(),
                    relationships: Vec::new(),
                    last_event_time: None,
                    state_entered_time: None,
                    created_time: None,
                };
                return convert::snapshot_to_py_dict(py, &snapshot);
            }
        };

        if let Some(loser) = &loser {
            // More recent last_event_time
            match (entity.last_event_time, loser.last_event_time) {
                (Some(w), Some(l)) if l > w => entity.last_event_time = Some(l),
                (None, Some(l)) => entity.last_event_time = Some(l),
                _ => {}
            }
            // Earlier created_time
            match (entity.created_time, loser.created_time) {
                (Some(w), Some(l)) if l < w => entity.created_time = Some(l),
                (None, Some(l)) => entity.created_time = Some(l),
                _ => {}
            }
            self.store.update_entity(entity.clone());
        }

        let snapshot = defacto_engine::types::EntitySnapshot {
            entity_id: entity.entity_id.clone(),
            entity_type: entity.entity_type.clone(),
            state: entity.state.clone(),
            properties: entity.properties.clone(),
            relationships: Vec::new(),
            last_event_time: entity.last_event_time,
            state_entered_time: entity.state_entered_time,
            created_time: entity.created_time,
        };
        convert::snapshot_to_py_dict(py, &snapshot)
    }

    /// Remove entity from in-memory state.
    fn delete(&self, entity_id: String) -> PyResult<()> {
        self.store.remove_entity(&entity_id);
        Ok(())
    }

    /// Clear all in-memory entity state. Used before full rebuilds.
    /// Identity cache is preserved — use clear_identity_cache() separately
    /// for FULL_WITH_IDENTITY_RESET.
    fn clear(&self) -> PyResult<()> {
        self.store.clear();
        Ok(())
    }

    /// Clear only the identity cache. For FULL_WITH_IDENTITY_RESET builds.
    fn clear_identity_cache(&self) -> PyResult<()> {
        self.store.clear_identity_cache();
        Ok(())
    }

    /// Load entities into DashMap from state history dicts.
    ///
    /// Used during cold start recovery to restore entity state without
    /// replaying the ledger. Each dict has: entity_id, entity_type, state,
    /// properties (dict), and optional timing fields (last_event_time,
    /// state_entered_time, created_time as ISO 8601 strings).
    ///
    /// Entities are loaded without marking dirty — they're being restored,
    /// not modified.
    fn load_entities(&self, _py: Python<'_>, entities: Vec<Bound<'_, PyAny>>) -> PyResult<usize> {
        let mut count = 0;
        for entity in &entities {
            let dict = entity.downcast::<PyDict>()?;

            let entity_id: String = dict
                .get_item("entity_id")?
                .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err("Missing 'entity_id'"))?
                .extract()?;
            let entity_type: String = dict
                .get_item("entity_type")?
                .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err("Missing 'entity_type'"))?
                .extract()?;
            let state: String = dict
                .get_item("state")?
                .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err("Missing 'state'"))?
                .extract()?;

            let props_obj = dict
                .get_item("properties")?
                .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err("Missing 'properties'"))?;
            let props_dict = props_obj.downcast::<PyDict>()?;
            let properties = convert::py_dict_to_value(props_dict)?;

            // Parse optional timing fields (ISO 8601 strings → DateTime<Utc>)
            let parse_ts = |key: &str| -> PyResult<Option<chrono::DateTime<chrono::Utc>>> {
                match dict.get_item(key)? {
                    Some(val) if !val.is_none() => {
                        let s: String = val.extract()?;
                        chrono::DateTime::parse_from_rfc3339(&s)
                            .map(|dt| Some(dt.with_timezone(&chrono::Utc)))
                            .map_err(|e| pyo3::exceptions::PyValueError::new_err(
                                format!("Invalid timestamp for '{key}': {e}")
                            ))
                    }
                    _ => Ok(None),
                }
            };

            let last_event_time = parse_ts("last_event_time")?;
            let state_entered_time = parse_ts("state_entered_time")?;
            let created_time = parse_ts("created_time")?;

            self.store.load_entity(defacto_engine::types::EntityState {
                entity_id,
                entity_type,
                state,
                properties,
                last_event_time,
                state_entered_time,
                created_time,
            });
            count += 1;
        }
        Ok(count)
    }

    // ── Query support ──

    /// Number of entities currently in memory.
    fn entity_count(&self) -> usize {
        self.store.entity_count()
    }

    /// Return definition_hash, source_hash, identity_hash for a version.
    ///
    /// Returns a Python dict with three keys. Used by BuildManager to compare
    /// against stored hashes and determine the minimum build mode needed.
    fn definition_hashes(&self, py: Python<'_>, version: String) -> PyResult<PyObject> {
        let compiled = self.definitions.get(&version)
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err(
                format!("Version '{version}' has not been compiled")
            ))?;
        let (def_hash, src_hash, id_hash) = compiled.hashes();
        let dict = PyDict::new(py);
        dict.set_item("definition_hash", def_hash)?;
        dict.set_item("source_hash", src_hash)?;
        dict.set_item("identity_hash", id_hash)?;
        Ok(dict.into())
    }

    // ── Lifecycle ──

    /// Release thread pool and all internal state.
    fn close(&mut self) -> PyResult<()> {
        self.normalized_events.clear();
        self.definitions.clear();
        self.store.entities.clear();
        self.store.relationships.clear();
        self.store.identity_cache.clear();
        self.store.created.clear();
        self.store.dirty.clear();
        Ok(())
    }
}

/// Convert a DefactoError into a Python exception.
///
/// Can't use `impl From<DefactoError> for PyErr` due to the orphan rule
/// (both types are from external crates). This helper is called explicitly
/// in method bodies via `.map_err(to_py_err)`.
fn to_py_err(err: defacto_engine::error::DefactoError) -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(err.to_string())
}

/// Register the defacto._core module.
#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", "2.0.0-dev")?;
    m.add_class::<DefactoCore>()?;
    Ok(())
}
