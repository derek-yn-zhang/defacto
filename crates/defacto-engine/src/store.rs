//! In-memory entity state store.
//!
//! The store holds all entity state, relationships, identity cache, and
//! per-batch tracking sets. It uses DashMap for concurrent access across
//! worker threads — each worker processes different entities, accessing
//! different DashMap keys with no contention.
//!
//! Entity state is disposable — it can be rebuilt from the ledger at any time.
//! The store is working memory for interpretation, not a persistence concern.
//! This is why we use an in-memory HashMap (via DashMap) instead of RocksDB
//! or an external KV store: the interpretation loop needs nanosecond access,
//! and durability is handled by the Postgres ledger.

use std::collections::HashMap;

use dashmap::{DashMap, DashSet};

use crate::types::{EntityState, Value};

/// In-memory entity state, shared across worker threads.
///
/// Owned by DefactoCore. All mutable entity data lives here. When `clear()`
/// is called (for full rebuilds), everything in the store is wiped — compiled
/// definitions and the Rayon thread pool live outside the store and survive.
pub struct Store {
    /// Entity state indexed by entity_id. The primary data structure.
    /// Workers access different keys concurrently — DashMap's internal sharding
    /// ensures no contention between workers processing different entities.
    pub entities: DashMap<String, EntityState>,

    /// Relationship state indexed by (source_entity_id, relationship_type, target_entity_id).
    /// Updated when Effect::Relate is applied during interpretation.
    pub relationships: DashMap<(String, String, String), HashMap<String, Value>>,

    /// Identity cache — "entity_type\0hint_value" → entity_id.
    /// Scoped by entity type so the same hint value for different entity types
    /// resolves independently (e.g., email for customer vs email for fraud_case).
    /// This is the in-process cache that sits in front of the Postgres identity store.
    /// Most lookups hit this cache (nanoseconds). Cache misses fall through to Postgres
    /// via the Python IdentityResolver.
    pub identity_cache: DashMap<String, String>,

    /// Entity IDs created during the current batch. Cleared after each batch.
    /// Aggregated into BuildResult.entities_created.
    pub created: DashSet<String>,

    /// Entity IDs modified during the current batch. Cleared after each batch.
    /// Used for state history writes — only dirty entities need new SCD Type 2 rows.
    pub dirty: DashSet<String>,
}

/// Build a composite cache key from entity type and hint value.
///
/// Uses a null byte separator — impossible in normal text data, so the
/// composite key is unambiguous. This gives us entity-type-scoped identity
/// resolution with a flat DashMap (one allocation, one hash per lookup).
fn identity_key(entity_type: &str, hint_value: &str) -> String {
    format!("{}\0{}", entity_type, hint_value)
}

impl Store {
    /// Create an empty store.
    pub fn new() -> Self {
        Self {
            entities: DashMap::new(),
            relationships: DashMap::new(),
            identity_cache: DashMap::new(),
            created: DashSet::new(),
            dirty: DashSet::new(),
        }
    }

    /// Look up an entity by ID. Returns None if the entity doesn't exist.
    ///
    /// Clones the entity state — callers get a snapshot, not a reference.
    /// This is intentional: the interpreter works on a local copy and writes
    /// the updated state back via `update_entity()`.
    pub fn get_entity(&self, entity_id: &str) -> Option<EntityState> {
        self.entities.get(entity_id).map(|entry| entry.value().clone())
    }

    /// Insert or update an entity in the store. Marks it as dirty.
    ///
    /// The entity_id is taken from the EntityState itself. If an entity with
    /// this ID already exists, it's replaced. The entity is added to the dirty
    /// set so the orchestration layer knows which entities need new state
    /// history rows.
    pub fn update_entity(&self, entity: EntityState) {
        self.dirty.insert(entity.entity_id.clone());
        self.entities.insert(entity.entity_id.clone(), entity);
    }

    /// Load an entity into the store without marking it dirty.
    ///
    /// Used during cold start recovery to restore entity state from state
    /// history. These entities haven't changed — they're being restored, not
    /// created or modified — so they shouldn't trigger state history writes.
    pub fn load_entity(&self, entity: EntityState) {
        self.entities.insert(entity.entity_id.clone(), entity);
    }

    /// Remove an entity from the store. Returns the removed state, if it existed.
    ///
    /// Used for erase operations and during merges (the merged-away entity is
    /// removed, its events replayed into the surviving entity).
    pub fn remove_entity(&self, entity_id: &str) -> Option<EntityState> {
        self.entities.remove(entity_id).map(|(_, state)| state)
    }

    /// Clear all entity state, relationships, and batch tracking.
    ///
    /// Identity cache is NOT cleared — it persists across rebuilds because
    /// identity mappings are still valid even when entity state is rebuilt.
    /// Use `clear_identity_cache()` separately for FULL_WITH_IDENTITY_RESET.
    pub fn clear(&self) {
        self.entities.clear();
        self.relationships.clear();
        self.created.clear();
        self.dirty.clear();
    }

    /// Clear only the identity cache. Called during FULL_WITH_IDENTITY_RESET.
    ///
    /// Separate from `clear()` because most rebuilds should keep identity
    /// mappings intact — only identity config changes require a reset.
    pub fn clear_identity_cache(&self) {
        self.identity_cache.clear();
    }

    /// Number of entities currently in the store.
    pub fn entity_count(&self) -> usize {
        self.entities.len()
    }

    /// Collect and return all entity IDs created during this batch, then clear the set.
    ///
    /// Called once per batch by the orchestration layer to report how many
    /// entities were created. Draining the set resets it for the next batch.
    pub fn take_created(&self) -> Vec<String> {
        let ids: Vec<String> = self.created.iter().map(|r| r.key().clone()).collect();
        self.created.clear();
        ids
    }

    /// Collect and return all entity IDs modified during this batch, then clear the set.
    ///
    /// Called once per batch to determine which entities need new state history
    /// rows written. Draining the set resets it for the next batch.
    pub fn take_dirty(&self) -> Vec<String> {
        let ids: Vec<String> = self.dirty.iter().map(|r| r.key().clone()).collect();
        self.dirty.clear();
        ids
    }

    /// Look up an entity ID from the identity cache, scoped by entity type.
    ///
    /// Returns None on cache miss — the caller (Python IdentityResolver)
    /// should fall through to the Postgres identity backend.
    pub fn get_identity(&self, entity_type: &str, hint_value: &str) -> Option<String> {
        let key = identity_key(entity_type, hint_value);
        self.identity_cache.get(&key).map(|entry| entry.value().clone())
    }

    /// Add an (entity_type, hint_value) → entity_id mapping to the identity cache.
    ///
    /// Called after a successful backend identity lookup to warm the cache,
    /// or after creating a new identity mapping.
    pub fn set_identity(&self, entity_type: &str, hint_value: &str, entity_id: String) {
        let key = identity_key(entity_type, hint_value);
        self.identity_cache.insert(key, entity_id);
    }

    /// Bulk-load identity mappings into the cache. Called on startup for cache warming.
    ///
    /// Takes (entity_type, hint_value, entity_id) triples from the identity
    /// backend and loads them so subsequent lookups hit the in-memory cache.
    pub fn bulk_load_identity(&self, mappings: Vec<(String, String, String)>) {
        for (entity_type, hint_value, entity_id) in mappings {
            let key = identity_key(&entity_type, &hint_value);
            self.identity_cache.insert(key, entity_id);
        }
    }

    /// Remove identity cache entries for specific hints. Called by erase
    /// to keep the cache in sync with the backend after hint deletion.
    pub fn remove_identity_batch(&self, hints: &[(String, String)]) {
        for (entity_type, hint_value) in hints {
            let key = identity_key(entity_type, hint_value);
            self.identity_cache.remove(&key);
        }
    }
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entity(id: &str, state: &str) -> EntityState {
        EntityState {
            entity_id: id.into(),
            entity_type: "customer".into(),
            state: state.into(),
            properties: HashMap::new(),
            last_event_time: None,
            state_entered_time: None,
            created_time: None,
        }
    }

    #[test]
    fn store_new_is_empty() {
        let store = Store::new();
        assert_eq!(store.entity_count(), 0);
        assert!(store.entities.is_empty());
        assert!(store.relationships.is_empty());
        assert!(store.identity_cache.is_empty());
        assert!(store.created.is_empty());
        assert!(store.dirty.is_empty());
    }

    // ── Entity CRUD ──

    #[test]
    fn get_entity_missing() {
        let store = Store::new();
        assert!(store.get_entity("nonexistent").is_none());
    }

    #[test]
    fn update_and_get_entity() {
        let store = Store::new();
        store.update_entity(make_entity("c1", "active"));
        let entity = store.get_entity("c1").unwrap();
        assert_eq!(entity.entity_id, "c1");
        assert_eq!(entity.state, "active");
    }

    #[test]
    fn update_entity_marks_dirty() {
        let store = Store::new();
        store.update_entity(make_entity("c1", "active"));
        assert!(store.dirty.contains("c1"));
    }

    #[test]
    fn update_entity_overwrites() {
        let store = Store::new();
        store.update_entity(make_entity("c1", "active"));
        store.update_entity(make_entity("c1", "churned"));
        assert_eq!(store.get_entity("c1").unwrap().state, "churned");
        assert_eq!(store.entity_count(), 1);
    }

    #[test]
    fn remove_entity_returns_state() {
        let store = Store::new();
        store.update_entity(make_entity("c1", "active"));
        let removed = store.remove_entity("c1").unwrap();
        assert_eq!(removed.state, "active");
        assert!(store.get_entity("c1").is_none());
        assert_eq!(store.entity_count(), 0);
    }

    #[test]
    fn remove_entity_missing() {
        let store = Store::new();
        assert!(store.remove_entity("nonexistent").is_none());
    }

    // ── Clear ──

    #[test]
    fn clear_preserves_identity_cache() {
        let store = Store::new();
        store.update_entity(make_entity("c1", "active"));
        store.set_identity("customer", "alice@test.com", "c1".into());
        store.clear();
        assert_eq!(store.entity_count(), 0);
        assert!(store.dirty.is_empty());
        // Identity cache survives clear
        assert_eq!(store.get_identity("customer", "alice@test.com"), Some("c1".into()));
    }

    #[test]
    fn clear_identity_cache() {
        let store = Store::new();
        store.set_identity("customer", "alice@test.com", "c1".into());
        store.clear_identity_cache();
        assert!(store.get_identity("customer", "alice@test.com").is_none());
    }

    // ── Batch tracking ──

    #[test]
    fn take_created_drains() {
        let store = Store::new();
        store.created.insert("c1".into());
        store.created.insert("c2".into());
        let mut ids = store.take_created();
        ids.sort();
        assert_eq!(ids, vec!["c1", "c2"]);
        // Set is now empty
        assert!(store.take_created().is_empty());
    }

    #[test]
    fn take_dirty_drains() {
        let store = Store::new();
        store.update_entity(make_entity("c1", "active"));
        store.update_entity(make_entity("c2", "lead"));
        let mut ids = store.take_dirty();
        ids.sort();
        assert_eq!(ids, vec!["c1", "c2"]);
        assert!(store.take_dirty().is_empty());
    }

    // ── Identity cache ──

    #[test]
    fn identity_cache_miss() {
        let store = Store::new();
        assert!(store.get_identity("customer", "unknown").is_none());
    }

    #[test]
    fn identity_cache_hit() {
        let store = Store::new();
        store.set_identity("customer", "alice@test.com", "c1".into());
        assert_eq!(store.get_identity("customer", "alice@test.com"), Some("c1".into()));
    }

    #[test]
    fn identity_cache_scoped_by_entity_type() {
        let store = Store::new();
        store.set_identity("customer", "alice@test.com", "cust_001".into());
        store.set_identity("fraud_case", "alice@test.com", "fraud_001".into());
        // Same hint value, different entity types → different entities
        assert_eq!(store.get_identity("customer", "alice@test.com"), Some("cust_001".into()));
        assert_eq!(store.get_identity("fraud_case", "alice@test.com"), Some("fraud_001".into()));
    }

    #[test]
    fn bulk_load_identity() {
        let store = Store::new();
        store.bulk_load_identity(vec![
            ("customer".into(), "alice@test.com".into(), "c1".into()),
            ("customer".into(), "bob@test.com".into(), "c2".into()),
        ]);
        assert_eq!(store.get_identity("customer", "alice@test.com"), Some("c1".into()));
        assert_eq!(store.get_identity("customer", "bob@test.com"), Some("c2".into()));
    }

    #[test]
    fn entity_count() {
        let store = Store::new();
        assert_eq!(store.entity_count(), 0);
        store.update_entity(make_entity("c1", "active"));
        assert_eq!(store.entity_count(), 1);
        store.update_entity(make_entity("c2", "lead"));
        assert_eq!(store.entity_count(), 2);
        store.remove_entity("c1");
        assert_eq!(store.entity_count(), 1);
    }
}
