"""Identity resolution coordination.

Bridges the Rust identity cache (DashMap in DefactoCore) and the Python
identity backend (Postgres/SQLite). Most lookups hit the Rust cache
(nanoseconds). Cache misses fall through to the backend (milliseconds).

This is NOT the same as backends/_identity.py — that defines the storage
interface. This module coordinates between the cache and the storage.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from defacto.backends import IdentityBackend, LedgerBackend


class IdentityResolver:
    """Coordinates identity resolution between Rust cache and Python backend.

    The identity cache (DashMap) lives in DefactoCore. The durable identity
    store lives in Postgres/SQLite via IdentityBackend. This class bridges them.

    The resolve_batch method is the hot path — called once per batch during
    ingest and during builds. It handles cache lookup, backend fallback,
    new entity creation, merge detection, and stale cache validation.

    Stale cache validation (TLA+ verified): after a cross-shard merge,
    other shards' caches may still map hints to the merged-away entity.
    Cache hits are validated against the merge_log — any entity_id that
    is a merge loser gets re-resolved from the identity backend.
    """

    def __init__(
        self, core: Any, backend: IdentityBackend,
        ledger: LedgerBackend | None = None,
    ) -> None:
        """Initialize with DefactoCore, identity backend, and ledger.

        Args:
            core: DefactoCore instance (owns the DashMap cache).
            backend: IdentityBackend for durable storage.
            ledger: LedgerBackend for merge_log validation. Optional for
                backward compatibility — when None, stale cache validation
                is skipped (safe for single-shard deployments).
        """
        self._core = core
        self._backend = backend
        self._ledger = ledger

    def resolve_batch(
        self, event_hints: list[dict[str, list[str]]]
    ) -> tuple[list[tuple[int, str, str]], list[tuple[str, str]]]:
        """Resolve identity hints from a batch of normalized events.

        Args:
            event_hints: Per-event resolution_hints dicts. Index = event index.
                Each dict maps entity_type → list of hint value strings.
                Example: [{"customer": ["alice@test.com", "555-1234"]}, ...]

        Returns:
            (entity_mapping, merges) where:
            - entity_mapping: list of (event_index, entity_id, entity_type)
              ready for core.interpret()
            - merges: list of (from_id, into_id) for detected identity merges
        """
        if not event_hints:
            return [], []

        # Step 1: Deduplicate all (entity_type, hint_value) pairs across events.
        # Resolution hints are {entity_type: {field_name: value}} dicts.
        # The field_name is stored in the identity table for inspection
        # but the cache/lookup key is (entity_type, hint_value).
        all_hints: list[tuple[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for hints_dict in event_hints:
            for entity_type, fields in hints_dict.items():
                for _field_name, hv in fields.items():
                    key = (entity_type, hv)
                    if key not in seen:
                        seen.add(key)
                        all_hints.append(key)

        if not all_hints:
            return [], []

        # Step 2: Bulk cache lookup
        cache_result = self._core.resolve_cache(all_hints)
        resolved_map: dict[tuple[str, str], str] = {}
        for entity_type, hint_value, entity_id in cache_result["resolved"]:
            resolved_map[(entity_type, hint_value)] = entity_id

        # Step 2b: Validate cache hits against merge_log.
        # A cache-hit entity_id that's a merge loser is stale — the entity
        # was merged away on another shard, but this shard's cache hasn't
        # been updated. Re-resolve these hints from the backend.
        # Cost: one indexed SELECT per batch against merge_log (tiny table).
        # TLA+ verified: without this, events get permanently misrouted
        # to merged-away entities (formal/DefactoProtocol.tla).
        unresolved = list(cache_result["unresolved"])
        if resolved_map and self._ledger:
            cache_hit_ids = set(resolved_map.values())
            stale_ids = self._ledger.find_merge_losers(list(cache_hit_ids))
            if stale_ids:
                stale_keys = [
                    key for key, eid in resolved_map.items()
                    if eid in stale_ids
                ]
                for key in stale_keys:
                    del resolved_map[key]
                    unresolved.append(key)

        # Step 3: Handle unresolved hints via backend
        if unresolved:
            unresolved_set = {(et, hv) for et, hv in unresolved}

            # Group unresolved hints by (event_index, entity_type) and assign
            # one candidate UUID per group. Hints for the same event and entity
            # type share a candidate so the backend creates one entity for all.
            candidate_map: dict[tuple[int, str], str] = {}
            backend_hints: list[tuple[str, str, str, str]] = []
            backend_seen: set[tuple[str, str]] = set()

            for event_idx, hints_dict in enumerate(event_hints):
                for entity_type, fields in hints_dict.items():
                    for field_name, hv in fields.items():
                        if (entity_type, hv) in unresolved_set and (entity_type, hv) not in backend_seen:
                            group_key = (event_idx, entity_type)
                            if group_key not in candidate_map:
                                candidate_map[group_key] = str(uuid.uuid4())
                            backend_hints.append(
                                (entity_type, field_name, hv, candidate_map[group_key])
                            )
                            backend_seen.add((entity_type, hv))

            # One backend round-trip for all unresolved hints
            new_mappings = self._backend.resolve_and_create(backend_hints)

            # Update resolved map and warm the cache
            cache_updates: list[tuple[str, str, str]] = []
            for entity_type, hint_value, entity_id in new_mappings:
                resolved_map[(entity_type, hint_value)] = entity_id
                cache_updates.append((entity_type, hint_value, entity_id))

            if cache_updates:
                self._core.update_cache(cache_updates)

        # Step 4: Build entity_mapping, detect merges, fix cache
        entity_mapping: list[tuple[int, str, str]] = []
        merges: list[tuple[str, str]] = []
        seen_merges: set[tuple[str, str]] = set()
        cache_corrections: list[tuple[str, str, str]] = []

        for event_idx, hints_dict in enumerate(event_hints):
            for entity_type, fields in hints_dict.items():
                # Collect all entity_ids this event's hints resolved to
                entity_ids: set[str] = set()
                hint_values = list(fields.values())
                for hv in hint_values:
                    eid = resolved_map.get((entity_type, hv))
                    if eid:
                        entity_ids.add(eid)

                if not entity_ids:
                    continue

                if len(entity_ids) == 1:
                    entity_mapping.append((event_idx, entity_ids.pop(), entity_type))
                else:
                    # Merge: multiple entity_ids for same event + entity_type.
                    # Lexicographically first ID survives (deterministic).
                    sorted_ids = sorted(entity_ids)
                    into_id = sorted_ids[0]
                    for from_id in sorted_ids[1:]:
                        merge_key = (from_id, into_id)
                        if merge_key not in seen_merges:
                            merges.append(merge_key)
                            seen_merges.add(merge_key)

                    # Fix cache: update hints that pointed to losers
                    for hv in hint_values:
                        key = (entity_type, hv)
                        if resolved_map.get(key) != into_id:
                            resolved_map[key] = into_id
                            cache_corrections.append((entity_type, hv, into_id))

                    entity_mapping.append((event_idx, into_id, entity_type))

        # Batch update cache with merge corrections
        if cache_corrections:
            self._core.update_cache(cache_corrections)

        return entity_mapping, merges

    def execute_merge(self, from_id: str, into_id: str) -> None:
        """Execute an identity merge: reassign hints in backend and cache.

        Updates the durable store (identity table) AND the Rust identity
        cache. For identity-discovered merges the cache is already updated
        during resolve_batch, so the update_cache call is a harmless
        overwrite. For external merges (m.merge()) the cache has stale
        entries that must be corrected here.
        """
        moved = self._backend.merge(from_id, into_id)
        if moved:
            self._core.update_cache([
                (entity_type, hint_value, into_id)
                for entity_type, hint_value in moved
            ])

    def resolve_hint(
        self, entity_type: str, hint_value: str,
    ) -> str | None:
        """Resolve a single identity hint to its current entity_id.

        Checks the Rust cache first, then falls back to the backend.
        Used by merge event replay during IDENTITY_RESET to find
        current entity_ids when stored IDs are stale.

        Returns:
            Entity ID, or None if the hint is unknown.
        """
        result = self._core.resolve_cache([(entity_type, hint_value)])
        if result["resolved"]:
            return result["resolved"][0][2]
        return self._backend.lookup(entity_type, hint_value)

    def warmup(self) -> None:
        """Load full identity mapping from backend into Rust cache.

        Called on startup. Bulk-loads all (entity_type, hint_value, entity_id)
        triples from the backend into the DashMap cache.
        """
        mappings = self._backend.warmup()
        if mappings:
            self._core.update_cache(mappings)

    def reset(self) -> None:
        """Delete all identity data from backend and clear Rust cache.

        Used during FULL_WITH_IDENTITY_RESET builds.
        """
        self._backend.reset()
        self._core.clear_identity_cache()
