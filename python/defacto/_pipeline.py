"""Batch processing pipeline coordination.

The pipeline coordinates one batch through all processing steps:
normalize (Rust) → ledger write (Python) → identity resolution (Python)
→ event_entities write (Python) → interpret (Rust) → publish (Python).

This is the heart of the orchestration layer. It owns no state — it
receives the core, backends, and publisher as dependencies.
"""

from __future__ import annotations

import hashlib
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from defacto.results import BuildResult, EventFailure, IngestResult

logger = logging.getLogger("defacto.pipeline")


@contextmanager
def _timed(timing: dict, key: str):
    """Record wall-clock duration of a block in milliseconds.

    Usage:
        timing = {}
        with _timed(timing, "normalize_ms"):
            result = core.normalize(source, events)
        # timing["normalize_ms"] == 4.2
    """
    start = time.perf_counter()
    yield
    timing[key] = round((time.perf_counter() - start) * 1000, 2)

if TYPE_CHECKING:
    from defacto._build import BuildManager
    from defacto._identity import IdentityResolver
    from defacto._publisher import Publisher
    from defacto.backends import LedgerBackend

# Internal batch size for replay processing during builds
_REPLAY_BATCH_SIZE = 1000

# System event type for merge directives
_DEFACTO_MERGE_EVENT_TYPE = "_defacto_merge"


def _shard_hash(entity_id: str) -> int:
    """Deterministic shard hash from entity_id.

    First 4 bytes of SHA-256, interpreted as unsigned big-endian integer.
    Range 0 to 4,294,967,295 — fits in Postgres BIGINT without sign issues.
    Stored in event_entities.shard_hash for fast shard-aware replay.
    """
    return int.from_bytes(hashlib.sha256(entity_id.encode()).digest()[:4])


def _shard_for_entity(entity_id: str, total_shards: int) -> int:
    """Deterministic shard assignment via SHA-256 hash partitioning."""
    return _shard_hash(entity_id) % total_shards

# Batch size for ledger updates during renormalize pre-pass
_RENORMALIZE_DB_BATCH = 1000


class Pipeline:
    """Coordinates one batch through normalize → write → resolve → interpret → publish.

    Each method call processes one batch and returns results. The pipeline
    is stateless between calls — all state lives in the core, backends,
    and build manager.
    """

    def __init__(
        self,
        core: Any,  # DefactoCore (Rust)
        ledger: LedgerBackend,
        identity: IdentityResolver,
        publisher: Publisher,
        build_manager: BuildManager,
        dead_letter: Any = None,  # DeadLetterSink or None
        shard_id: int | None = None,
        total_shards: int | None = None,
    ) -> None:
        """Initialize with all dependencies.

        The pipeline owns no state itself — it coordinates these components
        in the correct order for each batch.

        Args:
            core: DefactoCore (Rust) — normalization, interpretation, entity state.
            ledger: Durable event storage — append, replay, event_entities.
            identity: Cache + backend coordinator — resolves hints to entity IDs.
            publisher: Effect delivery — Kafka (production) or inline (development).
            build_manager: Build state tracking — cursor, dirty flag, mode detection.
            dead_letter: Optional dead letter sink for routing failures.
                None = failures only in result objects (NullDeadLetter behavior).
            shard_id: This process's shard index. None = unsharded (all entities).
            total_shards: Total number of shards. None = unsharded.
        """
        self._core = core
        self._ledger = ledger
        self._identity = identity
        self._publisher = publisher
        self._build_manager = build_manager
        self._dead_letter = dead_letter
        self._shard_id = shard_id
        self._total_shards = total_shards
        # Thread pool for parallel I/O — ledger and identity operations
        # overlap on separate connections. Works for all backends: SQLite
        # serializes on WAL lock, Postgres overlaps on separate connections.
        self._io_pool = ThreadPoolExecutor(max_workers=2)

    def _owns_entity(self, entity_id: str) -> bool:
        """Check if this shard owns the entity.

        Always True when not sharded. When sharded, uses deterministic
        SHA-256 hash partitioning.
        """
        if self._total_shards is None:
            return True
        return _shard_for_entity(entity_id, self._total_shards) == self._shard_id

    # ── Ingest path ──

    def process_batch(
        self,
        source: str,
        raw_events: list[dict[str, Any]],
        version: str,
    ) -> tuple[IngestResult, list[dict[str, Any]]]:
        """Process a batch of raw events end to end.

        Steps:
        1. Normalize raw events (Rust, GIL released)
        2. Sort by timestamp (prevents false late arrivals from ordering noise)
        3. Append to ledger
        4. Resolve identity (cache + backend)
        5. Write event-entity mappings
        6. Execute merges if detected
        7. Interpret events against entity state machines (Rust)
        8. Publish snapshots and tombstones
        9. Advance cursor + update watermark

        Returns:
            Tuple of (IngestResult, list of entity snapshot dicts).
        """
        timing: dict[str, float] = {}
        t_start = time.perf_counter()

        # Step 1: Normalize
        with _timed(timing, "normalize_ms"):
            result = self._core.normalize(source, raw_events)

        ledger_rows = result["ledger_rows"]
        failures = result["failures"]
        event_count = result["count"]

        if not ledger_rows:
            return IngestResult(
                events_ingested=0,
                events_failed=len(failures),
                duplicates_skipped=0,
                failures=[self._make_normalize_failure(f) for f in failures],
            ), []

        # Step 2: Sort by timestamp to prevent false late arrivals.
        # normalize() stores events in Rust in input order, so we track
        # original indices to remap for interpret() later.
        indexed = sorted(
            enumerate(ledger_rows),
            key=lambda x: x[1].get("timestamp", ""),
        )
        original_indices = [i for i, _ in indexed]
        sorted_rows = [row for _, row in indexed]

        # Batch timestamp — max across all events (last after sort)
        batch_timestamp = sorted_rows[-1].get("timestamp", "")

        # Steps 3+4: Parallel I/O — ledger append and identity resolution
        # on separate connections. SQLite serializes; Postgres overlaps.
        event_hints: list[dict[str, list[str]]] = [
            row.get("resolution_hints", {}) for row in sorted_rows
        ]
        with _timed(timing, "io_ms"):
            seq_future = self._io_pool.submit(
                self._ledger.append_batch, sorted_rows,
            )
            id_future = self._io_pool.submit(
                self._identity.resolve_batch, event_hints,
            )
            sequences = seq_future.result()
            entity_mapping, merges = id_future.result()

        # Step 5: Write event-entity mappings (needs results from both 3+4)
        # Batched in same transaction as ledger append via begin/commit.
        event_entity_mappings: list[tuple[int, str, str]] = [
            (sequences[sorted_idx], entity_id, entity_type)
            for sorted_idx, entity_id, entity_type in entity_mapping
        ]
        self._ledger.begin()
        self._ledger.write_event_entities(event_entity_mappings)

        # Step 6: Execute merges (metadata)
        tombstones, merge_rebuild_pairs = self._execute_merges(
            merges, batch_timestamp,
        )

        # Step 7: Interpret — remap sorted indices back to original
        # positions in Rust's normalized_events vector. Filter to only
        # entities owned by this shard (no-op when unsharded).
        interpret_mapping = [
            (original_indices[sorted_idx], entity_id, entity_type)
            for sorted_idx, entity_id, entity_type in entity_mapping
            if self._owns_entity(entity_id)
        ]
        with _timed(timing, "interpret_ms"):
            interpret_result = self._core.interpret(interpret_mapping)
        snapshots = interpret_result["snapshots"]
        interpret_failures = interpret_result["failures"]

        # Step 8: Add valid_from to snapshots for state history writes.
        # Each snapshot carries its own last_event_time from the Rust
        # interpreter — the timestamp of the event that produced it.
        for snap in snapshots:
            snap["valid_from"] = snap.get("last_event_time") or batch_timestamp

        # Step 9: Publish interpretation snapshots + merge tombstones
        with _timed(timing, "publish_ms"):
            self._publisher.publish(snapshots, tombstones)

        # Step 10: Advance cursor + watermark + commit
        if sequences:
            self._build_manager.advance_cursor(version, max(sequences))
        if batch_timestamp:
            self._build_manager.update_watermark(
                version, datetime.fromisoformat(batch_timestamp),
            )
        self._ledger.commit()

        # Step 11: Rebuild merge winners for immediately correct state.
        merge_rebuild_failures = self._rebuild_merge_winners(version, merge_rebuild_pairs)

        timing["total_ms"] = round((time.perf_counter() - t_start) * 1000, 2)
        timing["overhead_ms"] = round(
            timing["total_ms"]
            - timing["normalize_ms"]
            - timing["io_ms"]
            - timing["interpret_ms"]
            - timing["publish_ms"],
            2,
        )

        # Collect all failures — normalization + interpretation + publishing.
        # Each stage produces EventFailure with a stage field so callers
        # can distinguish and route appropriately.
        all_failures: list[EventFailure] = []
        all_failures.extend(self._make_normalize_failure(f) for f in failures)
        all_failures.extend(
            self._make_interpret_failure(f) for f in interpret_failures
        )
        all_failures.extend(
            self._make_publish_failure(err, eid)
            for err, eid in self._publisher.drain_errors()
        )
        all_failures.extend(merge_rebuild_failures)

        # Route failures to dead letter sink (if configured)
        if all_failures and self._dead_letter:
            self._dead_letter.send(all_failures)

        ingest_result = IngestResult(
            events_ingested=event_count,
            events_failed=len(all_failures),
            duplicates_skipped=len(raw_events) - event_count - len(failures),
            failures=all_failures,
            timing=timing,
        )

        logger.info("ingest batch completed", extra={
            "operation": "ingest",
            "events_ingested": event_count,
            "events_failed": len(all_failures),
            "duplicates_skipped": len(raw_events) - event_count - len(failures),
            "duration_ms": timing.get("total_ms", 0),
        })
        if all_failures:
            logger.warning("ingest failures detected", extra={
                "operation": "ingest",
                "events_failed": len(all_failures),
                "stages": list({f.stage for f in all_failures}),
            })

        return ingest_result, snapshots

    # ── Build paths ──

    def build_incremental(self, version: str) -> BuildResult:
        """Process new events from cursor to head of ledger.

        After replay, checks for late arrivals (events with timestamps
        older than the watermark). If found, rebuilds affected entities
        via partial replay — the PARTIAL post-pass.
        """
        start = time.monotonic()
        status = self._build_manager.get_status(version)
        watermark = self._build_manager.get_watermark(version)
        self._build_manager.set_dirty(version)

        # Explicit transaction keeps the Postgres server-side cursor
        # alive while event_entities and build_state writes happen
        # within the loop. Without this, auto_commit kills the cursor.
        # SQLite doesn't need this (its cursors survive without a
        # transaction), and BEGIN would block the identity connection.
        self._ledger.begin()
        from_seq = status.cursor + 1
        if self._total_shards is not None and self._ledger.has_event_entities():
            replay_iter = self._ledger.replay_for_shard(
                self._shard_id, self._total_shards, from_sequence=from_seq,
            )
        else:
            replay_iter = self._ledger.replay(from_sequence=from_seq)
        totals, max_ts, late_entity_ids, merge_rebuild_pairs, replay_failures = self._replay_events(
            replay_iter, version, watermark=watermark,
        )
        self._ledger.commit()

        # Post-pass: rebuild entities whose history changed.

        # Late arrivals: full resolution (identity might have changed).
        if late_entity_ids:
            saved_cursor = self._build_manager.get_status(version).cursor
            partial_totals, _, _, _, partial_failures = self._rebuild_entities(
                version, list(late_entity_ids),
            )
            self._build_manager.advance_cursor(version, saved_cursor)
            totals["events_processed"] += partial_totals["events_processed"]
            totals["merges_detected"] += partial_totals["merges_detected"]
            replay_failures.extend(partial_failures)

        # Merge winners: pre-resolved (cursor managed by helper).
        replay_failures.extend(
            self._rebuild_merge_winners(version, merge_rebuild_pairs)
        )

        # Post-build tick — evaluate time rules for entities with overdue
        # thresholds. Validates each ticked entity still exists in the
        # shared ledger — cleans up orphans from cross-shard erases/merges.
        tick_snapshots = self._tick_and_validate()
        if tick_snapshots:
            self._publisher.publish(tick_snapshots, [])
            self._publisher.drain_errors()
            totals["effects_produced"] += len(tick_snapshots)

        # Update watermark + finalize
        if max_ts:
            self._build_manager.update_watermark(
                version, datetime.fromisoformat(max_ts),
            )
        # Route replay failures to dead letter (same as ingest path).
        if replay_failures and self._dead_letter:
            self._dead_letter.send(replay_failures)

        self._build_manager.clear_dirty(version, "INCREMENTAL")
        duration = int((time.monotonic() - start) * 1000)
        result = BuildResult(
            mode="INCREMENTAL",
            events_failed=len(replay_failures),
            failures=replay_failures,
            duration_ms=duration,
            **totals,
        )
        logger.info("build completed", extra={
            "operation": "build",
            "mode": "INCREMENTAL",
            "events_processed": totals["events_processed"],
            "effects_produced": totals["effects_produced"],
            "merges_detected": totals["merges_detected"],
            "events_failed": len(replay_failures),
            "duration_ms": duration,
        })
        if replay_failures:
            logger.warning("build failures detected", extra={
                "operation": "build",
                "mode": "INCREMENTAL",
                "events_failed": len(replay_failures),
            })
        if late_entity_ids:
            logger.warning("late arrivals detected", extra={
                "operation": "build",
                "late_entity_count": len(late_entity_ids),
            })
        return result

    def build_full(self, version: str, *, mode: str = "FULL") -> BuildResult:
        """Rebuild all entity state from the ledger.

        Clears in-memory state, then replays all events from sequence 0.
        For RENORMALIZE and IDENTITY_RESET modes, event_entities are cleared
        and rewritten during replay (resolution hints or entity IDs changed).

        After replay: merge post-pass rebuilds any winners whose state
        is incomplete (loser's effects lost during mid-replay merge),
        then tick evaluates overdue time rules.
        """
        start = time.monotonic()
        self._core.clear()
        self._build_manager.set_dirty(version)

        # When identity changes (new entity_ids) or resolution_hints change
        # (new hint extraction), existing event_entities are stale. Clear them
        # so the replay writes fresh mappings.
        if mode in ("FULL_RENORMALIZE", "FULL_WITH_IDENTITY_RESET"):
            self._ledger.clear_event_entities()

        # Explicit transaction keeps the replay cursor alive while
        # event_entities and build_state writes happen within the loop.
        # Without this, auto_commit on writes kills the cursor —
        # "database is locked" on SQLite, "cursor does not exist" on Postgres.
        # Shard-aware replay: when sharded and event_entities is populated
        # (not cleared above), filter at the database level so each shard
        # reads only its ~1/N of events instead of the full ledger.
        # Requires event_entities from a prior build or process=True ingest.
        use_shard_replay = (
            self._total_shards is not None
            and mode not in ("FULL_RENORMALIZE", "FULL_WITH_IDENTITY_RESET")
            and self._ledger.has_event_entities()
        )

        self._ledger.begin()
        if use_shard_replay:
            replay_iter = self._ledger.replay_for_shard(
                self._shard_id, self._total_shards, from_sequence=0,
            )
        else:
            replay_iter = self._ledger.replay(from_sequence=0)
        totals, max_ts, _, merge_rebuild_pairs, replay_failures = self._replay_events(
            replay_iter, version,
        )
        self._ledger.commit()

        # Post-pass: rebuild merge winners (cursor managed by helper).
        replay_failures.extend(
            self._rebuild_merge_winners(version, merge_rebuild_pairs)
        )

        # Post-rebuild tick — catch entities with overdue time rules that
        # never received a subsequent event during replay. Validates
        # against the shared ledger for cross-shard consistency.
        tick_snapshots = self._tick_and_validate()
        if tick_snapshots:
            self._publisher.publish(tick_snapshots, [])
            self._publisher.drain_errors()
            totals["effects_produced"] += len(tick_snapshots)

        # Update watermark + finalize
        if max_ts:
            self._build_manager.update_watermark(
                version, datetime.fromisoformat(max_ts),
            )
        # Route replay failures to dead letter (same as ingest path).
        if replay_failures and self._dead_letter:
            self._dead_letter.send(replay_failures)

        self._build_manager.clear_dirty(version, mode)
        duration = int((time.monotonic() - start) * 1000)
        result = BuildResult(
            mode=mode,
            events_failed=len(replay_failures),
            failures=replay_failures,
            duration_ms=duration,
            **totals,
        )
        logger.info("build completed", extra={
            "operation": "build",
            "mode": mode,
            "events_processed": totals["events_processed"],
            "effects_produced": totals["effects_produced"],
            "merges_detected": totals["merges_detected"],
            "events_failed": len(replay_failures),
            "duration_ms": duration,
        })
        if replay_failures:
            logger.warning("build failures detected", extra={
                "operation": "build",
                "mode": mode,
                "events_failed": len(replay_failures),
            })
        return result

    def build_partial(
        self, version: str, affected_entity_ids: list[str]
    ) -> BuildResult:
        """Rebuild state for specific entities only.

        Used standalone for external merge handling. Late arrival partials
        during INCREMENTAL builds use _rebuild_entities directly.
        """
        start = time.monotonic()
        self._build_manager.set_dirty(version)

        totals, _, _, _, partial_failures = self._rebuild_entities(version, affected_entity_ids)

        if partial_failures and self._dead_letter:
            self._dead_letter.send(partial_failures)

        self._build_manager.clear_dirty(version, "PARTIAL")
        duration = int((time.monotonic() - start) * 1000)
        result = BuildResult(
            mode="PARTIAL",
            events_failed=len(partial_failures),
            failures=partial_failures,
            duration_ms=duration,
            **totals,
        )
        logger.info("build completed", extra={
            "operation": "build",
            "mode": "PARTIAL",
            "events_processed": totals["events_processed"],
            "effects_produced": totals["effects_produced"],
            "events_failed": len(partial_failures),
            "duration_ms": duration,
        })
        if partial_failures:
            logger.warning("build failures detected", extra={
                "operation": "build",
                "mode": "PARTIAL",
                "events_failed": len(partial_failures),
            })
        return result

    # ── Renormalize pre-pass ──

    def renormalize_ledger(self, version: str) -> int:
        """Re-normalize all ledger events from raw through current source definitions.

        Streams events from the ledger, re-normalizes through
        DefactoCore.renormalize() (Rust, Rayon-parallel, sequence-correlated),
        and batch-updates the ledger with new data/event_id/event_type/
        resolution_hints. The raw column is unchanged.

        Also handles dedup: when event_id_fields change, previously-distinct
        events may produce the same event_id. Rust detects these during
        renormalization and embeds duplicate_of directly into each success
        row. update_normalized writes event_id and duplicate_of atomically
        in one UPDATE per row. Every renormalize is a clean slate — all
        rows get a fresh duplicate_of value (NULL or canonical_sequence).

        Called as a pre-pass before FULL_RENORMALIZE builds. After this, the
        ledger reflects current source definitions and the normal FULL build
        that follows processes correctly normalized events.

        Returns:
            Number of events successfully re-normalized.
        """
        events = [
            {"sequence": e["sequence"], "source": e["source"], "raw": e["raw"]}
            for e in self._ledger.replay(from_sequence=0, include_duplicates=True)
        ]
        if not events:
            return 0

        result = self._core.renormalize(events)

        # Each success row includes duplicate_of (NULL for canonical,
        # canonical_sequence for duplicates). update_normalized writes
        # event_id, data, hints, AND duplicate_of in one atomic UPDATE
        # per row — no ordering issues, no separate mark/clear calls.
        successes = result["successes"]
        if successes:
            for i in range(0, len(successes), _RENORMALIZE_DB_BATCH):
                self._ledger.update_normalized(successes[i:i + _RENORMALIZE_DB_BATCH])

        return len(successes)

    # ── Private helpers ──

    def _replay_events(
        self,
        events_iter: Any,
        version: str,
        *,
        watermark: str = "",
    ) -> tuple[dict[str, int], str, set[str], dict[str, str], list[EventFailure]]:
        """Stream events from an iterator, processing in batches.

        Shared by build_incremental, build_full, and _rebuild_entities.

        Args:
            events_iter: Iterator of event dicts from the ledger.
            version: Definition version for cursor advancement.
            watermark: If set, events with timestamp < watermark are flagged
                as late arrivals and their entity_ids are collected.

        Returns:
            (totals, max_timestamp, late_entity_ids, merge_rebuild_pairs, failures) —
            totals dict for BuildResult, max_timestamp for watermark,
            merge_rebuild_pairs maps winner entity_id → entity_type for
            entities that need reconstruction via _rebuild_entities,
            failures is the accumulated list of EventFailure from all batches.
        """
        events_processed = 0
        merges_detected = 0
        late_events = 0
        max_timestamp = ""
        late_entity_ids: set[str] = set()
        merge_rebuild_pairs: dict[str, str] = {}
        all_failures: list[EventFailure] = []

        batch: list[dict[str, Any]] = []
        for event in events_iter:
            batch.append(event)
            if len(batch) >= _REPLAY_BATCH_SIZE:
                stats = self._process_replay_batch(batch, version, watermark=watermark)
                events_processed += stats["events"]
                merges_detected += stats["merges"]
                late_events += stats["late_event_count"]
                if stats["max_timestamp"] > max_timestamp:
                    max_timestamp = stats["max_timestamp"]
                late_entity_ids.update(stats["late_entity_ids"])
                merge_rebuild_pairs.update(stats["merge_rebuild_pairs"])
                all_failures.extend(stats["failures"])
                batch = []

        if batch:
            stats = self._process_replay_batch(batch, version, watermark=watermark)
            events_processed += stats["events"]
            merges_detected += stats["merges"]
            late_events += stats["late_event_count"]
            if stats["max_timestamp"] > max_timestamp:
                max_timestamp = stats["max_timestamp"]
            late_entity_ids.update(stats["late_entity_ids"])
            merge_rebuild_pairs.update(stats["merge_rebuild_pairs"])
            all_failures.extend(stats["failures"])

        totals = {
            "events_processed": events_processed,
            "effects_produced": 0,
            "entities_created": 0,
            "entities_updated": 0,
            "merges_detected": merges_detected,
            "late_arrivals": late_events,
        }
        return totals, max_timestamp, late_entity_ids, merge_rebuild_pairs, all_failures

    def _process_replay_batch(
        self,
        events: list[dict[str, Any]],
        version: str,
        *,
        watermark: str = "",
    ) -> dict[str, Any]:
        """Process a batch of events already in the ledger.

        Two modes based on whether events have pre-resolved entity_mapping
        (entity_id field present from replay_for_shard):

        Pre-resolved: identity resolution, event_entities writes, and merge
        execution are all skipped — they already happened during ingest.
        The entity_mapping comes directly from the replay query.

        Full resolution: the standard path for first builds — resolves
        identity from scratch, writes event_entities, handles merges.
        """
        self._core.load_events(events)
        batch_timestamp = events[-1].get("timestamp", "") if events else ""

        # Filter out system events (merge directives skip normal
        # processing — they're handled separately).
        def _is_system_event(e: dict) -> bool:
            return e.get("event_type") == _DEFACTO_MERGE_EVENT_TYPE

        if "entity_id" in events[0]:
            entity_mapping = [
                (i, e["entity_id"], e["entity_type"])
                for i, e in enumerate(events)
                if not _is_system_event(e)
            ]
            merges: list[tuple[str, str]] = []
            tombstones: list[dict[str, Any]] = []
            merge_rebuild_pairs: list[tuple[str, str]] = []
        else:
            # Full resolution path — first builds, unsharded, etc.
            # Separate merge events from regular events, tracking the
            # mapping from regular_hints index → events index so
            # entity_mapping indices resolve to the right event.
            merge_events: list[dict[str, Any]] = []
            regular_hints: list[dict[str, Any]] = []
            regular_to_event: list[int] = []
            for i, e in enumerate(events):
                if _is_system_event(e):
                    merge_events.append(e)
                else:
                    regular_hints.append(e.get("resolution_hints", {}))
                    regular_to_event.append(i)

            entity_mapping, merges = self._identity.resolve_batch(regular_hints)

            # Collect external merge directives — executed AFTER interpretation
            # because the entities need to exist in DashMap first.
            # Merge events store hints so the merge survives IDENTITY_RESET
            # (where stored entity_ids are stale). If hints resolve to
            # current entity_ids, use those. If they resolve to the same
            # entity (already merged in identity), skip the merge.
            external_merges: list[tuple[str, str]] = []
            for me in merge_events:
                data = me.get("data", {})
                from_id = data.get("from_entity_id", "")
                into_id = data.get("into_entity_id", "")
                from_hints = data.get("from_hints", {})
                into_hints = data.get("into_hints", {})
                if from_hints and into_hints:
                    resolved_from = self._resolve_merge_hints(from_hints)
                    resolved_into = self._resolve_merge_hints(into_hints)
                    if resolved_from and resolved_into:
                        from_id = resolved_from
                        into_id = resolved_into
                if from_id and into_id and from_id != into_id:
                    external_merges.append((from_id, into_id))

            # Map resolve_batch indices (into regular_hints) back to
            # events indices for sequence lookup and interpretation.
            entity_mapping = [
                (regular_to_event[hint_idx], entity_id, entity_type)
                for hint_idx, entity_id, entity_type in entity_mapping
            ]
            ee_mappings: list[tuple[int, str, str]] = [
                (events[event_idx]["sequence"], entity_id, entity_type)
                for event_idx, entity_id, entity_type in entity_mapping
            ]
            self._ledger.write_event_entities(ee_mappings)
            # Execute identity-discovered merges before interpretation
            # (they affect entity mapping).
            tombstones, merge_rebuild_pairs = self._execute_merges(
                merges, batch_timestamp,
            )

        # Detect late arrivals (INCREMENTAL builds only)
        late_entity_ids: set[str] = set()
        late_event_count = 0
        if watermark:
            for event_idx, entity_id, _entity_type in entity_mapping:
                if events[event_idx]["timestamp"] < watermark:
                    late_entity_ids.add(entity_id)
                    late_event_count += 1

        # Filter to entities owned by this shard (no-op when unsharded
        # or when replay_for_shard already filtered).
        shard_mapping = [
            (idx, eid, etype) for idx, eid, etype in entity_mapping
            if self._owns_entity(eid)
        ]
        interpret_result = self._core.interpret(shard_mapping)
        snapshots = interpret_result["snapshots"]
        interpret_failures = interpret_result["failures"]
        for snap in snapshots:
            snap["valid_from"] = snap.get("last_event_time") or batch_timestamp

        # Execute external merge directives AFTER interpretation —
        # entities must exist in DashMap for core.merge() to work.
        # Filter to merges where this shard owns the winner — during
        # FULL_RENORMALIZE all shards see all events, but only the
        # winner's shard should execute the merge.
        if "entity_id" not in events[0] and external_merges:
            owned_merges = [
                (from_id, into_id) for from_id, into_id in external_merges
                if self._owns_entity(into_id)
            ]
            if owned_merges:
                ext_tombstones, ext_pairs = self._execute_merges(
                    owned_merges, batch_timestamp,
                )
                tombstones.extend(ext_tombstones)
                merge_rebuild_pairs.extend(ext_pairs)

        self._publisher.publish(snapshots, tombstones)

        # Collect failures — same pattern as process_batch ingest path.
        batch_failures: list[EventFailure] = []
        batch_failures.extend(
            self._make_interpret_failure(f) for f in interpret_failures
        )
        batch_failures.extend(
            self._make_publish_failure(err, eid)
            for err, eid in self._publisher.drain_errors()
        )

        if events:
            last_seq = max(e["sequence"] for e in events)
            self._build_manager.advance_cursor(version, last_seq)

        return {
            "events": len(events),
            "merges": len(merges),
            "max_timestamp": batch_timestamp,
            "late_entity_ids": late_entity_ids,
            "late_event_count": late_event_count,
            "merge_rebuild_pairs": dict(merge_rebuild_pairs),
            "failures": batch_failures,
        }

    def _rebuild_merge_winners(
        self, version: str,
        pairs: list[tuple[str, str]] | dict[str, str],
    ) -> list[EventFailure]:
        """Finalize merges by rebuilding winners via _rebuild_entities.

        Wraps _rebuild_entities in pre-resolved mode for each winner,
        with cursor save/restore (rebuilds replay old events that would
        regress the cursor).

        Called from process_batch (ingest), build_incremental,
        build_full, and m.merge(). One method, one merge protocol.

        Returns accumulated failures from all rebuilds.
        """
        items = pairs.items() if isinstance(pairs, dict) else pairs
        items = list(items)
        if not items:
            return []
        all_failures: list[EventFailure] = []
        saved_cursor = self._build_manager.get_status(version).cursor
        for eid, etype in items:
            _, _, _, _, rebuild_failures = self._rebuild_entities(
                version, [eid], entity_type=etype,
            )
            all_failures.extend(rebuild_failures)
        self._build_manager.advance_cursor(version, saved_cursor)

        if all_failures and self._dead_letter:
            self._dead_letter.send(all_failures)
        return all_failures

    def _rebuild_entities(
        self, version: str, entity_ids: list[str],
        *, entity_type: str | None = None,
    ) -> tuple[dict[str, int], str, set[str], dict[str, str], list[EventFailure]]:
        """Delete entities from state and replay all their events.

        The ONE way to reconstruct entity state. Used for late arrivals,
        merge winners, and build_partial. Clears state history via
        publisher, clears DashMap, then replays all events through the
        pipeline.

        Two modes via _process_replay_batch:
        - Full resolution (entity_type=None): events replayed without
          entity_id. Identity re-resolved, event_entities rewritten.
          Used for late arrivals where identity might have changed.
        - Pre-resolved (entity_type set): events replayed with entity_id
          + entity_type attached. Skips identity and event_entities.
          Filters out system events. Used for merge rebuilds where
          the entity mapping is already known.

        Does not manage the dirty flag — the caller handles lifecycle.

        Returns (totals, max_timestamp, late_entity_ids, merge_rebuild_pairs, failures).
        """
        for eid in entity_ids:
            if entity_type is not None:
                # Merge rebuild: delete stale state history to avoid
                # SCD valid_from collisions (same events, same timestamps).
                # Late arrival rebuilds don't need this — new events have
                # different timestamps, SCD close logic handles it.
                self._publisher.delete_entity(eid)
            self._core.delete(eid)

        events_iter = self._ledger.replay_for_entities(entity_ids)

        if entity_type is not None:
            # Pre-resolved: attach entity mapping, filter system events.
            # Used for merge rebuilds — identity is already resolved.
            # Generator to avoid materializing all events into memory.
            def _attach_entity_id(it):
                for e in it:
                    if not e.get("event_type", "").startswith("_defacto_"):
                        e["entity_id"] = entity_ids[0]
                        e["entity_type"] = entity_type
                        yield e
            events_iter = _attach_entity_id(events_iter)

        return self._replay_events(events_iter, version)  # returns 5-tuple including failures

    def _tick_and_validate(
        self, as_of: str | None = None,
    ) -> list[dict[str, Any]]:
        """Evaluate time rules and validate ticked entities still exist.

        Runs core.tick() to find entities with overdue time rules, then
        validates each one against the shared ledger. Entities erased
        or merged away by another shard are removed from DashMap and
        their snapshots are dropped.

        Args:
            as_of: ISO 8601 timestamp to evaluate against.
                Default: current UTC time.

        Returns:
            Valid tick snapshots ready for publishing.
        """
        now = as_of or datetime.now(timezone.utc).isoformat()
        tick_snapshots = self._core.tick(now)
        if not tick_snapshots:
            return []

        # One batch query to check which ticked entities still exist
        tick_ids = [s["entity_id"] for s in tick_snapshots]
        existing = self._ledger.existing_entity_ids(tick_ids)

        valid = []
        for snap in tick_snapshots:
            eid = snap["entity_id"]
            if eid in existing:
                snap["valid_from"] = now
                valid.append(snap)
            else:
                # Erased or merged away on another shard — clean orphan
                self._core.delete(eid)
        return valid

    def _execute_merges(
        self, merges: list[tuple[str, str]], batch_timestamp: str = ""
    ) -> tuple[list[dict[str, Any]], list[tuple[str, str]]]:
        """Execute merge metadata. Returns (tombstones, rebuild_pairs).

        Updates all inline stores: identity (table + cache),
        event_entities (reassign loser → winner), merge_log (audit),
        DashMap (remove loser, update winner timing). Produces
        tombstones to close the loser's state history row.

        Does NOT produce winner snapshots — the winner's state is
        incomplete after core.merge (loser's pre-merge effects lost).
        The caller rebuilds the winner via _rebuild_entities to get
        correct combined state.

        Args:
            merges: List of (from_id, into_id) merge pairs.
            batch_timestamp: ISO 8601 timestamp for tombstone valid_to.

        Returns:
            (tombstones, rebuild_pairs) — tombstones close the loser's
            state history row, rebuild_pairs are (entity_id, entity_type)
            tuples for winners that need _rebuild_entities.
        """
        tombstones: list[dict[str, Any]] = []
        rebuild_pairs: list[tuple[str, str]] = []
        for from_id, into_id in merges:
            self._identity.execute_merge(from_id, into_id)
            self._ledger.merge_event_entities(from_id, into_id)
            self._ledger.write_merge_log(from_id, into_id, batch_timestamp)

            merged = self._core.merge(from_id, into_id)
            entity_type = merged.get("entity_type", "")

            # Ghost entities (events arrived before create) have no entity_type
            # in DashMap. Fall back to event_entities which always has the type.
            if not entity_type:
                entity_type = self._ledger.lookup_entity_type(into_id) or ""

            tombstones.append({
                "entity_id": from_id,
                "entity_type": entity_type,
                "merged_into": into_id,
                "timestamp": batch_timestamp,
            })
            rebuild_pairs.append((into_id, entity_type))
        return tombstones, rebuild_pairs

    def _resolve_merge_hints(
        self, hints: dict[str, dict[str, str | list[str]]],
    ) -> str | None:
        """Resolve merge event hints to a current entity_id.

        Merge events store identifying hints ({entity_type: {field: value(s)}})
        so the merge survives IDENTITY_RESET builds. Tries each hint
        until one resolves. Hint values may be lists when multiple hints
        share the same type (e.g., two emails after a prior merge).

        Returns:
            Current entity_id, or None if no hint resolves.
        """
        for entity_type, fields in hints.items():
            for _field_name, hint_value in fields.items():
                values = hint_value if isinstance(hint_value, list) else [hint_value]
                for hv in values:
                    entity_id = self._identity.resolve_hint(
                        entity_type, hv,
                    )
                    if entity_id:
                        return entity_id
        return None

    @staticmethod
    def _make_normalize_failure(f: dict[str, Any]) -> EventFailure:
        """Convert a normalization failure dict into an EventFailure."""
        return EventFailure(
            raw=f.get("raw", {}),
            error=f.get("error", ""),
            stage="normalization",
            source=f.get("source", ""),
            handler=f.get("handler"),
            field=f.get("field"),
        )

    @staticmethod
    def _make_interpret_failure(f: dict[str, Any]) -> EventFailure:
        """Convert an interpretation failure dict into an EventFailure."""
        return EventFailure(
            raw={},
            error=f.get("error", ""),
            stage="interpretation",
            entity_id=f.get("entity_id"),
            entity_type=f.get("entity_type"),
        )

    @staticmethod
    def _make_publish_failure(error: str, entity_id: str) -> EventFailure:
        """Convert a publishing delivery error into an EventFailure."""
        return EventFailure(
            raw={},
            error=error,
            stage="publishing",
            entity_id=entity_id,
            recoverable=True,
        )
