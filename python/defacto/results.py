"""Result types returned by Defacto operations.

Every operation (ingest, build, tick, erase) returns a result dataclass
with structured information about what happened. These are frozen — they
are immutable snapshots of operation outcomes.

All result types are pure data containers with no methods beyond what
dataclasses provide (__init__, __repr__, __eq__).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


# ---------------------------------------------------------------------------
# Ingest results
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EventFailure:
    """An event that failed at any stage of the pipeline.

    Carries enough context to diagnose, route to a dead letter sink,
    and determine whether the failure is retryable. The stage field
    distinguishes where the failure occurred; optional fields carry
    context specific to that stage.
    """

    raw: dict
    """The input that failed — raw event (normalization) or normalized event (interpretation)."""

    error: str
    """Human-readable description of the failure."""

    stage: str = "normalization"
    """Pipeline stage: 'normalization', 'interpretation', 'publishing', 'consumption'."""

    source: str = ""
    """Source name the event was ingested from."""

    handler: str | None = None
    """Handler that failed (normalization only)."""

    field: str | None = None
    """Specific field that caused the failure (normalization only)."""

    entity_id: str | None = None
    """Entity ID involved (interpretation, publishing, consumption)."""

    entity_type: str | None = None
    """Entity type involved (interpretation, publishing, consumption)."""

    recoverable: bool = False
    """Whether retrying might succeed (True for infrastructure failures)."""


@dataclass(frozen=True)
class IngestResult:
    """Outcome of an ingest operation.

    Good events proceed through the pipeline. Failed events are captured
    in `failures` with full context. If a dead letter is configured,
    failures are also routed there automatically.
    """

    events_ingested: int
    """Events successfully normalized and appended to the ledger."""

    events_failed: int
    """Events that failed normalization."""

    duplicates_skipped: int
    """Events rejected by dedup (event_id already exists in ledger)."""

    duplicate_ids: list[str] = field(default_factory=list)
    """Event IDs of skipped duplicates."""

    failures: list[EventFailure] = field(default_factory=list)
    """Failed events with full context for inspection or dead letter routing."""

    build_result: BuildResult | None = None
    """Build result if process= was specified on ingest, None otherwise."""

    timing: dict[str, float] = field(default_factory=dict)
    """Per-stage wall-clock durations in milliseconds.

    Keys: normalize_ms, ledger_ms, identity_ms, interpret_ms, publish_ms,
    total_ms. 'overhead_ms' = total - sum(stages) captures Python
    orchestration cost (ThreadPoolExecutor, result collection, etc.).

    Empty dict when timing is not available (e.g., buffered ingest
    that hasn't flushed yet).
    """


# ---------------------------------------------------------------------------
# Build results
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BuildResult:
    """Outcome of a build operation.

    Reports the build mode chosen (auto-detected or forced), how many events
    were processed, and key metrics like merges and late arrivals.
    """

    mode: str
    """Build mode: INCREMENTAL, FULL, FULL_WITH_IDENTITY_RESET, FULL_RENORMALIZE, PARTIAL, SKIP."""

    events_processed: int
    """Total events processed in this build."""

    effects_produced: int
    """Total effects produced by interpretation."""

    entities_created: int
    """New entities created during this build."""

    entities_updated: int
    """Existing entities modified during this build."""

    merges_detected: int
    """Identity merges that occurred during this build."""

    late_arrivals: int
    """Events with timestamps older than the watermark."""

    duration_ms: int
    """Wall-clock time for the build in milliseconds."""

    events_failed: int = 0
    """Events that failed interpretation or publishing during replay."""

    failures: list[EventFailure] = field(default_factory=list)
    """Failed events with full context — same as IngestResult.failures."""

    timing: dict[str, float] = field(default_factory=dict)
    """Per-stage wall-clock durations in milliseconds.

    For builds, keys include: replay_ms, interpret_ms, publish_ms,
    total_ms. The breakdown depends on the build mode — FULL includes
    replay from ledger, INCREMENTAL includes cursor-to-head replay.
    """


# ---------------------------------------------------------------------------
# Tick results
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TickResult:
    """Outcome of a tick (time rule evaluation) operation."""

    effects_produced: int
    """Effects produced by time rules firing."""

    entities_affected: int
    """Number of entities that had time rules fire."""

    transitions: int
    """State transitions triggered by time rules."""


# ---------------------------------------------------------------------------
# Merge results
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MergeResult:
    """Outcome of merging two entities.

    Reports which entities were involved, how many events were
    reassigned to the winner, and what tombstones were produced.
    """

    from_entity_id: str
    """Entity merged away (the loser)."""

    into_entity_id: str
    """Entity merged into (the winner)."""

    events_reassigned: int
    """Events moved from loser to winner in event_entities."""

    entities_rebuilt: int
    """Entities whose state was reconstructed after the merge."""

    tombstones_produced: int
    """Tombstone records produced for the loser."""

    failures: list[EventFailure] = field(default_factory=list)
    """Events that failed interpretation during the winner rebuild."""


# ---------------------------------------------------------------------------
# Erase results
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EraseResult:
    """Outcome of erasing an entity.

    Reports the cascade — how many entities, events, and merge log
    entries were deleted. Includes the root entity and any entities
    that were previously merged into it.
    """

    entity_id: str
    """Root entity that was erased."""

    entities_erased: int
    """Total entities deleted (includes merge cascade)."""

    events_deleted: int
    """Total events removed from the ledger."""

    merge_log_cleaned: int
    """Merge log entries removed."""



# ---------------------------------------------------------------------------
# Consumer results
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ConsumerResult:
    """Outcome of a consumer.run_once() call — one batch of consumption."""

    snapshots_processed: int
    """Entity snapshots written to the state history store."""

    tombstones_processed: int
    """Tombstone messages processed (merged entity histories closed)."""

    batches_written: int
    """Number of write batches executed against the store."""

    duration_ms: int
    """Wall-clock time in milliseconds."""


@dataclass(frozen=True)
class ConsumerStatus:
    """Current status of a running state history consumer."""

    lag: int
    """Messages behind the latest Kafka offset."""

    last_offset: int
    """Last committed Kafka offset."""

    last_write_time: datetime | None
    """Timestamp of the last successful write batch, or None if no writes yet."""

    write_throughput: float
    """Snapshots per second (moving average)."""


# ---------------------------------------------------------------------------
# Validation results
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ValidationResult:
    """Outcome of definition validation.

    Errors are blocking — definitions with errors cannot be registered.
    Warnings are informational — unreachable states, unused handlers, etc.
    """

    valid: bool
    """True if no blocking errors were found."""

    errors: list[str] = field(default_factory=list)
    """Blocking issues that prevent registration."""

    warnings: list[str] = field(default_factory=list)
    """Non-blocking issues (unreachable states, unused handlers, etc.)."""


# ---------------------------------------------------------------------------
# Debugging results
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TimelineEntry:
    """A single entry in an entity's timeline.

    Each entry represents one event that affected the entity, showing
    what effects it produced and how the entity's state changed.
    """

    timestamp: datetime
    """When the event occurred."""

    event_type: str
    """Normalized event type (e.g., 'customer_signup', 'plan_upgrade')."""

    event_id: str
    """Unique event identifier."""

    effects: list[str]
    """Human-readable effect descriptions (e.g., 'Updated mrr: 29.0 → 99.0')."""

    state_before: str | None
    """Entity state before this event, or None if entity didn't exist yet."""

    state_after: str | None
    """Entity state after this event."""


@dataclass(frozen=True)
class Timeline:
    """Complete timeline for a specific entity.

    Shows every event that affected the entity, in chronological order,
    with the effects each event produced.
    """

    entity_id: str
    """Canonical entity ID."""

    entity_type: str
    """Entity type name."""

    entries: list[TimelineEntry] = field(default_factory=list)
    """Timeline entries in chronological order."""


@dataclass(frozen=True)
class BuildStatus:
    """Current build state for a definition version.

    Used to inspect where the build cursor is, whether the last build
    completed cleanly, and what hashes are stored for change detection.
    """

    cursor: int
    """Last processed ledger sequence number."""

    total_events: int
    """Total events in the ledger."""

    pending_events: int
    """Events not yet processed (total - cursor)."""

    last_build_time: datetime | None
    """When the last build completed, or None if never built."""

    last_build_mode: str | None
    """Mode of the last build (INCREMENTAL, FULL, etc.), or None."""

    definition_hash: str
    """Hash of the entity definitions — changes trigger FULL rebuild."""

    source_hash: str
    """Hash of source handler config — changes trigger FULL_RENORMALIZE."""

    identity_hash: str
    """Hash of identity config — changes trigger FULL_WITH_IDENTITY_RESET."""

    dirty: bool
    """True if the last build didn't complete cleanly (crash recovery needed)."""


# ---------------------------------------------------------------------------
# Definition management results
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RegisterResult:
    """Outcome of registering a new definition version.

    Includes what changed compared to the previously active version,
    and the predicted build mode needed to apply those changes.
    """

    version: str
    """The version name that was registered."""

    changes: dict
    """What changed vs the previous active version (added/removed/modified)."""

    build_mode: str
    """Predicted build mode needed: FULL, FULL_WITH_IDENTITY_RESET, FULL_RENORMALIZE."""

    warnings: list[str] = field(default_factory=list)
    """Non-blocking validation warnings."""
