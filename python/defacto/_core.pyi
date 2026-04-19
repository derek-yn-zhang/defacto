"""Type stubs for defacto._core (Rust extension via PyO3).

The Rust computation core exposed as a Python module. Contains DefactoCore,
which owns all computation state: entity store (DashMap), identity cache
(DashMap), compiled definitions (Arc), and the Rayon thread pool.
"""

from typing import Any

__version__: str

class DefactoCore:
    """Rust computation core.

    Owns entity state, compiled definitions, expression ASTs, and identity
    cache. All computation methods release the GIL and use the internal
    Rayon thread pool (sized by workers).
    """

    def __init__(self, workers: int = 1) -> None:
        """Initialize with a Rayon thread pool of the given size."""

    # ── Definition management ──

    def compile(self, version: str, definitions: dict[str, Any]) -> None:
        """Compile definitions dict into optimized internal representation.

        Pre-compiles all expressions into ASTs. Computes definition_hash,
        source_hash, and identity_hash for change detection.
        """

    def activate(self, version: str) -> None:
        """Set the active version for interpretation."""

    # ── Event loading (for builds) ──

    def load_events(self, events: list[dict[str, Any]]) -> None:
        """Load pre-normalized events from the ledger into Rust memory.

        Used during builds when events are replayed from the ledger rather
        than freshly normalized. Stores events for the subsequent interpret() call.
        Each event must have: event_id, event_type, timestamp, source,
        data, raw, resolution_hints.
        """

    # ── Normalization ──

    def normalize(self, source: str, raw_events: list[dict[str, Any]]) -> dict[str, Any]:
        """Normalize raw events through source definitions.

        Returns a dict with three keys:
          "ledger_rows": list[dict] — fields needed for database write
          "failures": list[dict] — events that failed normalization
          "count": int — number of successfully normalized events

        Full normalized events are held internally in Rust memory for the
        subsequent interpret() call. Only the ledger-write fields and
        failures cross back to Python.

        GIL released. Workers parallelize across events.
        """

    # ── Re-normalization ──

    def renormalize(self, events: list[dict[str, Any]]) -> dict[str, Any]:
        """Re-normalize ledger events from raw, preserving sequence correlation.

        Takes list of dicts with {sequence: int, source: str, raw: dict}.
        Groups by source, normalizes each group with Rayon parallelism.

        Returns a dict with:
          "successes": list of (sequence, ledger_row_dict) tuples
          "failures": list of (sequence, error_string) tuples

        GIL released per source group.
        """

    # ── Interpretation ──

    def interpret(self, entity_mapping: list[tuple[int, str, str]]) -> dict[str, Any]:
        """Interpret normalized events against entity state machines.

        Takes (event_index, entity_id, entity_type) tuples connecting
        internally-held events to their resolved entity IDs and types.
        Returns full entity snapshots (complete current state after effects)
        for entities that succeeded. Consumes the normalized events.

        Failed entities are skipped — their state in the DashMap store is
        unchanged. Successful entities get their state updates regardless
        of whether other entities in the batch failed.

        Returns a dict with:
          "snapshots": list[dict] — full entity state dicts for entities
              that succeeded. These get published to Kafka and written to
              state history as SCD Type 2 rows.
          "failures": list[dict] — entities that errored, each with
              entity_id, entity_type, error string.

        GIL released. Workers parallelize across entities.
        """

    # ── Time rules ──

    def tick(self, as_of: str) -> list[dict[str, Any]]:
        """Evaluate time rules for all entities at the given timestamp.

        Returns entity snapshots for entities where time rules fired.
        GIL released.
        """

    def tick_entities(self, entity_ids: list[str], as_of: str) -> list[dict[str, Any]]:
        """Evaluate time rules for specific entities only."""

    # ── Identity cache (bulk operations for pipeline efficiency) ──

    def resolve_cache(self, hints: list[tuple[str, str]]) -> dict[str, Any]:
        """Bulk identity cache lookup, scoped by entity type.

        Takes (entity_type, hint_value) pairs. Returns a dict with:
          "resolved": list of (entity_type, hint_value, entity_id) — cache hits
          "unresolved": list of (entity_type, hint_value) — cache misses
        """

    def update_cache(self, mappings: list[tuple[str, str, str]]) -> None:
        """Bulk cache warming. Loads (entity_type, hint_value, entity_id) triples."""

    def remove_from_cache(self, hints: list[tuple[str, str]]) -> None:
        """Remove specific hints from the identity cache.

        Called by erase to keep the cache in sync with the backend.
        Takes (entity_type, hint_value) pairs.
        """

    # ── Entity state management ──

    def merge(self, from_id: str, into_id: str) -> dict[str, Any]:
        """Merge entity state: absorb from_id into into_id.

        Returns the merged entity snapshot for into_id.
        """

    def delete(self, entity_id: str) -> None:
        """Remove entity from in-memory state."""

    def clear(self) -> None:
        """Clear all in-memory entity state. Identity cache preserved."""

    def clear_identity_cache(self) -> None:
        """Clear only the identity cache. For FULL_WITH_IDENTITY_RESET."""

    def load_entities(self, entities: list[dict[str, Any]]) -> int:
        """Load entities into DashMap from state history dicts.

        Used during cold start recovery. Entities are loaded without
        marking dirty. Returns number of entities loaded.
        """

    # ── Query support ──

    def entity_count(self) -> int:
        """Number of entities currently in memory."""

    def definition_hashes(self, version: str) -> dict[str, str]:
        """Return definition_hash, source_hash, identity_hash for a version."""

    # ── Lifecycle ──

    def close(self) -> None:
        """Release thread pool and all internal state."""
