"""Synthetic event generator for benchmarks.

Produces deterministic, reproducible event streams with realistic
distributions. The generator reads entity and source definitions to
derive valid state machine paths automatically — no hardcoded
transitions. Works with any Defacto definitions.

Key properties:
  - Definition-driven: reads states, handlers, and effects to know which
    events are valid from which state (the definitions are the grammar)
  - Seeded RNG: same seed = identical events every time
  - Zipf entity distribution: a few entities get many events, most get few
  - State machine progression: only generates events valid in the entity's
    current state — no invalid event types that would be no-ops
  - Merge scenarios: configurable fraction of entities with multiple hints
  - Chronological timestamps: monotonically increasing with random jitter

Usage:
    from benchmarks.datagen import SyntheticGenerator, BENCH_DEFINITIONS

    gen = SyntheticGenerator(BENCH_DEFINITIONS, n_events=10_000, n_entities=1_000)
    events = gen.generate()               # all events as list[dict]
    for batch in gen.batches(size=100):    # streaming batches
        m.ingest("bench", batch)
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator


# ---------------------------------------------------------------------------
# State machine graph extraction
# ---------------------------------------------------------------------------


def extract_state_graph(entity_def: dict[str, Any]) -> dict[str, dict[str, str]]:
    """Extract the state machine graph from an entity definition.

    Reads states and their event handlers to build a map of:
      state → {event_type → target_state}

    Target state is determined by scanning the handler's effects for a
    transition effect. If no transition, the entity stays in the current state.

    Args:
        entity_def: Single entity definition dict (e.g., defs["entities"]["customer"])

    Returns:
        Dict of state_name → {event_type → target_state_name}
    """
    graph: dict[str, dict[str, str]] = {}
    starts = entity_def.get("starts", "")

    for state_name, state_def in entity_def.get("states", {}).items():
        handlers: dict[str, str] = {}
        for event_type, handler in state_def.get("when", {}).items():
            target = state_name  # default: stay in current state
            for effect in handler.get("effects", []):
                if isinstance(effect, dict) and "transition" in effect:
                    target = effect["transition"]["to"]
            handlers[event_type] = target
        graph[state_name] = handlers

    return graph


def extract_event_fields(source_def: dict[str, Any]) -> dict[str, list[str]]:
    """Extract required fields per event type from a source definition.

    Reads the mappings for each event to determine which fields the raw
    event must contain.

    Args:
        source_def: Single source definition dict (e.g., defs["sources"]["bench"])

    Returns:
        Dict of event_type → list of required field names.
    """
    fields: dict[str, list[str]] = {}
    for event_type, event_def in source_def.get("events", {}).items():
        field_names = list(event_def.get("mappings", {}).keys())
        fields[event_type] = field_names
    return fields


def extract_create_events(entity_def: dict[str, Any]) -> set[str]:
    """Find which event types have a 'create' effect.

    These are the events that can bring an entity into existence.
    Typically only 'signup' in the starts state, but could be any
    event with "create" in its effects.

    Returns:
        Set of event_type names that have a create effect.
    """
    creators: set[str] = set()
    for state_def in entity_def.get("states", {}).values():
        for event_type, handler in state_def.get("when", {}).items():
            for effect in handler.get("effects", []):
                if effect == "create":
                    creators.add(event_type)
    return creators


def extract_hint_fields(source_def: dict[str, Any]) -> dict[str, list[str]]:
    """Extract which fields are identity hints per event type.

    Returns:
        Dict of event_type → list of hint field names.
    """
    hints: dict[str, list[str]] = {}
    for event_type, event_def in source_def.get("events", {}).items():
        event_hints = event_def.get("hints", {})
        # hints format: {entity_type: [field_name, ...]}
        all_hint_fields: list[str] = []
        for entity_type, fields in event_hints.items():
            all_hint_fields.extend(fields)
        hints[event_type] = all_hint_fields
    return hints


# ---------------------------------------------------------------------------
# Synthetic event generator
# ---------------------------------------------------------------------------


class SyntheticGenerator:
    """Definition-driven event generator with realistic distributions.

    Reads entity and source definitions to derive valid state machine
    paths automatically. No hardcoded transitions — works with any
    Defacto definitions.

    Args:
        definitions: Full definitions dict with 'entities' and 'sources' keys.
        n_events: Total number of events to generate.
        n_entities: Target number of unique entities.
        entity_type: Which entity type to generate events for.
        source: Which source to use for event field mappings.
        distribution: 'zipf' (power-law, realistic) or 'uniform' (baseline).
        merge_ratio: Fraction of entities with a second identity hint.
        seed: RNG seed for reproducibility.
    """

    def __init__(
        self,
        definitions: dict[str, Any],
        *,
        n_events: int = 10_000,
        n_entities: int = 1_000,
        entity_type: str = "customer",
        source: str = "bench",
        distribution: str = "zipf",
        merge_ratio: float = 0.05,
        seed: int = 42,
    ) -> None:
        self.definitions = definitions
        self.n_events = n_events
        self.n_entities = n_entities
        self.entity_type = entity_type
        self.source = source
        self.distribution = distribution
        self.merge_ratio = merge_ratio
        self.seed = seed

        # Extract the grammar from definitions
        entity_def = definitions["entities"][entity_type]
        source_def = definitions["sources"][source]

        self._graph = extract_state_graph(entity_def)
        self._fields = extract_event_fields(source_def)
        self._creators = extract_create_events(entity_def)
        self._hint_fields = extract_hint_fields(source_def)
        self._starts = entity_def["starts"]
        self._type_field = source_def.get("event_type", "type")
        self._ts_field = source_def.get("timestamp", "ts")

    def generate(self) -> list[dict]:
        """Generate all events in chronological order.

        For each event:
        1. Pick an entity via distribution (Zipf or uniform)
        2. If entity doesn't exist yet → generate a create event
        3. Otherwise → pick a valid event type for the entity's current state
        4. Apply the transition to update the entity's state
        5. Generate the event dict with required fields + timestamp

        Returns:
            List of raw event dicts ready for m.ingest(source, events).
        """
        rng = random.Random(self.seed)

        # Build Zipf weights for entity selection
        weights = self._build_weights()

        # Entity state tracking: idx → current state (None = not created)
        entity_states: dict[int, str | None] = {}
        # Entity identity: idx → email (and optionally phone)
        entity_emails: dict[int, str] = {}
        entity_phones: dict[int, str] = {}

        # Pre-assign merge entities (those with phone hints)
        merge_count = max(1, int(self.n_entities * self.merge_ratio))
        merge_set = set(range(merge_count))

        # Plan values for upgrade events
        plans = ["starter", "pro", "enterprise", "team"]

        # Timestamp generation
        base_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
        avg_interval = timedelta(seconds=3600 / max(1, self.n_events / 24))

        events: list[dict] = []

        for i in range(self.n_events):
            # Pick entity
            entity_idx = rng.choices(range(self.n_entities), weights=weights, k=1)[0]

            # Ensure entity has an email
            if entity_idx not in entity_emails:
                entity_emails[entity_idx] = f"user{entity_idx}@bench.defacto.dev"
                if entity_idx in merge_set:
                    entity_phones[entity_idx] = f"555-{entity_idx:06d}"

            email = entity_emails[entity_idx]
            current_state = entity_states.get(entity_idx)

            # Determine valid event type
            if current_state is None:
                # Entity doesn't exist — must use a create event
                # Find a create event in the starts state
                event_type = self._pick_create_event(rng)
                target = self._graph.get(self._starts, {}).get(event_type, self._starts)
                entity_states[entity_idx] = target
            else:
                # Pick a valid event from the current state's handlers
                available = self._graph.get(current_state, {})
                if not available:
                    # Terminal state — skip this entity, pick another event
                    continue
                event_type = rng.choice(list(available.keys()))
                entity_states[entity_idx] = available[event_type]

            # Generate timestamp
            jitter = timedelta(seconds=rng.uniform(0, avg_interval.total_seconds() * 2))
            ts = base_time + (avg_interval * i) + jitter

            # Build event dict
            event = {
                self._type_field: event_type,
                self._ts_field: ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "email": email,
            }

            # Add fields required by this event type
            required = self._fields.get(event_type, [])
            if "plan" in required:
                event["plan"] = rng.choice(plans)
            if "phone" in required and entity_idx in merge_set:
                event["phone"] = entity_phones[entity_idx]

            # Add phone hint for merge entities on their first event
            if entity_idx in merge_set and "phone" not in event:
                event["phone"] = entity_phones.get(entity_idx, "")

            events.append(event)

        return events

    def batches(self, size: int = 100) -> Iterator[list[dict]]:
        """Stream events in fixed-size batches.

        Each call to batches() produces identical output (reseeds RNG
        via generate()).
        """
        events = self.generate()
        for i in range(0, len(events), size):
            yield events[i : i + size]

    def _build_weights(self) -> list[float]:
        """Build entity selection weights based on distribution."""
        if self.distribution == "uniform":
            return [1.0] * self.n_entities

        # Zipf: weight(k) = 1 / (k+1)^s, s=1.5
        # k+1 to avoid division by zero for k=0
        s = 1.5
        return [1.0 / ((k + 1) ** s) for k in range(self.n_entities)]

    def _pick_create_event(self, rng: random.Random) -> str:
        """Pick a create event type from the available creators."""
        if self._creators:
            return rng.choice(list(self._creators))
        # Fallback: first event in the starts state
        starts_handlers = self._graph.get(self._starts, {})
        if starts_handlers:
            return list(starts_handlers.keys())[0]
        raise ValueError(
            f"No valid create event found for entity type '{self.entity_type}' "
            f"in starts state '{self._starts}'"
        )


# ---------------------------------------------------------------------------
# Benchmark-specific entity definitions
# ---------------------------------------------------------------------------

# These definitions exercise the interesting code paths for benchmarking:
# - Multiple states with transitions
# - Computed properties (expression evaluation in hot path)
# - Multiple identity hints (merge scenarios)
#
# More complex than test definitions, but not so complex that setup
# time dominates measurement.

BENCH_DEFINITIONS = {
    "entities": {
        "customer": {
            "starts": "lead",
            "properties": {
                "email": {"type": "string"},
                "phone": {"type": "string", "default": ""},
                "plan": {"type": "string", "default": "free"},
                "lifetime_value": {"type": "number", "default": 0},
            },
            "identity": {
                "email": {"match": "exact"},
                "phone": {"match": "exact"},
            },
            "states": {
                "lead": {
                    "when": {
                        "signup": {
                            "effects": [
                                "create",
                                {"set": {"property": "email", "from": "event.email"}},
                            ],
                        },
                        "upgrade": {
                            "effects": [
                                {"transition": {"to": "active"}},
                                {"set": {"property": "plan", "from": "event.plan"}},
                                {"increment": {"property": "lifetime_value", "by": 99}},
                            ],
                        },
                    },
                },
                "active": {
                    "when": {
                        "upgrade": {
                            "effects": [
                                {"set": {"property": "plan", "from": "event.plan"}},
                                {"increment": {"property": "lifetime_value", "by": 49}},
                            ],
                        },
                        "downgrade": {
                            "effects": [
                                {"transition": {"to": "lead"}},
                                {"set": {"property": "plan", "compute": "'free'"}},
                            ],
                        },
                    },
                },
            },
        },
    },
    "sources": {
        "bench": {
            "event_type": "type",
            "timestamp": "ts",
            "events": {
                "signup": {
                    "raw_type": "signup",
                    "mappings": {
                        "email": {"from": "email"},
                        "phone": {"from": "phone", "default": ""},
                    },
                    "hints": {"customer": ["email"]},
                },
                "upgrade": {
                    "raw_type": "upgrade",
                    "mappings": {
                        "email": {"from": "email"},
                        "plan": {"from": "plan"},
                    },
                    "hints": {"customer": ["email"]},
                },
                "downgrade": {
                    "raw_type": "downgrade",
                    "mappings": {"email": {"from": "email"}},
                    "hints": {"customer": ["email"]},
                },
            },
        },
    },
    "schemas": {},
}
