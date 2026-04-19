"""Random definition and event generation for property-based testing.

Generates valid entity definitions with tunable complexity, then produces
event sequences that exercise those definitions. Used by Hypothesis to
find implementation bugs the interpreter can't handle.

The definition generator is construction-by-construction: every generated
definition is structurally valid and will compile in Rust. This tests the
interpreter against definition shapes we haven't manually written.

The event generator walks the state machine graph to produce events that
are valid for each entity's current state, with configurable merge ratios,
late arrivals, and guard-triggering values.

Parameterizable levers:
    n_states:           2-6 states per entity type
    n_properties:       1-5 properties per entity type
    guard_probability:  0.0-0.5 of handlers having guard expressions
    time_rule_probability: 0.0-0.3 of states having time rules
    n_entity_types:     1-3 entity types (exercises cross-type identity)
"""

from __future__ import annotations

import random
import string
from typing import Any

from hypothesis import strategies as st


# ---------------------------------------------------------------------------
# Property type system — valid types and their value generators
# ---------------------------------------------------------------------------

PROPERTY_TYPES = ["string", "number", "integer", "boolean"]

# Values that are safe to use in events and guards for each property type
PROPERTY_VALUES: dict[str, list[Any]] = {
    "string": ["alpha", "beta", "gamma", "delta", "epsilon"],
    "number": [0, 1.5, 10, 99.99, 250],
    "integer": [0, 1, 5, 10, 100],
    "boolean": [True, False],
}

PROPERTY_DEFAULTS: dict[str, Any] = {
    "string": "",
    "number": 0,
    "integer": 0,
    "boolean": False,
}

# Guard expressions by property type — references entity.{prop} and event.{field}
GUARD_TEMPLATES: dict[str, list[str]] = {
    "string": [
        'event.{field} != entity.{prop}',
        'event.{field} != ""',
        'len(event.{field}) > 0',
    ],
    "number": [
        'event.{field} > 0',
        'event.{field} > entity.{prop}',
        'entity.{prop} < 1000',
    ],
    "integer": [
        'event.{field} > 0',
        'event.{field} != entity.{prop}',
        'entity.{prop} < 100',
    ],
    "boolean": [
        'entity.{prop} == false',
        'entity.{prop} != true',
    ],
}


# ---------------------------------------------------------------------------
# Name generators — deterministic, readable names
# ---------------------------------------------------------------------------

def _state_name(i: int) -> str:
    """Generate a state name: s0, s1, s2, ..."""
    return f"s{i}"


def _prop_name(i: int) -> str:
    """Generate a property name: p0, p1, p2, ..."""
    return f"p{i}"


def _event_name(from_state: str, to_state: str) -> str:
    """Generate an event type name from a transition: s0_to_s1."""
    return f"{from_state}_to_{to_state}"


def _entity_name(i: int) -> str:
    """Generate an entity type name: entity0, entity1, ..."""
    return f"ent{i}"


# ---------------------------------------------------------------------------
# Definition generator
# ---------------------------------------------------------------------------


@st.composite
def random_definitions(
    draw,
    n_entity_types: int = 1,
    min_states: int = 2,
    max_states: int = 5,
    min_properties: int = 1,
    max_properties: int = 4,
    guard_probability: float = 0.3,
    time_rule_probability: float = 0.2,
):
    """Generate a complete, valid Defacto definition dict.

    The definition is constructed to be valid by design:
    - Every state is reachable from the starts state
    - The starts state has a handler with a create effect
    - All transitions target existing states
    - All property references in effects are valid
    - Guards reference valid properties with correct types
    - Source mappings match entity properties
    - Identity hints reference valid source fields

    Args:
        n_entity_types: Number of entity types to generate.
        min_states: Minimum states per entity type.
        max_states: Maximum states per entity type.
        min_properties: Minimum properties per entity type.
        max_properties: Maximum properties per entity type.
        guard_probability: Probability that a handler has a guard expression.
        time_rule_probability: Probability that a state has a time rule.

    Returns:
        A definitions dict with 'entities', 'sources', 'schemas' keys.
    """
    entities: dict[str, Any] = {}
    sources: dict[str, Any] = {}

    for ent_idx in range(n_entity_types):
        entity_name = _entity_name(ent_idx)

        # --- Properties ---
        n_props = draw(st.integers(min_value=min_properties, max_value=max_properties))
        properties: dict[str, dict[str, Any]] = {}
        prop_types: dict[str, str] = {}  # prop_name → type, for guard generation
        for pi in range(n_props):
            pname = _prop_name(pi)
            ptype = draw(st.sampled_from(PROPERTY_TYPES))
            properties[pname] = {
                "type": ptype,
                "default": PROPERTY_DEFAULTS[ptype],
            }
            prop_types[pname] = ptype

        # Always add an identity field (email-like)
        properties["ident"] = {"type": "string", "default": ""}
        prop_types["ident"] = "string"

        # --- States and transitions ---
        n_states = draw(st.integers(min_value=min_states, max_value=max_states))
        state_names = [_state_name(i) for i in range(n_states)]
        starts = state_names[0]

        # Build transition graph: ensure every state is reachable from starts.
        # Strategy: create a spanning tree (linear chain), then add random edges.
        transitions: list[tuple[str, str]] = []  # (from_state, to_state)

        # Spanning tree: s0→s1→s2→...→sN (every state reachable)
        for i in range(n_states - 1):
            transitions.append((state_names[i], state_names[i + 1]))

        # Random extra transitions for graph density
        n_extra = draw(st.integers(min_value=0, max_value=n_states))
        for _ in range(n_extra):
            from_state = draw(st.sampled_from(state_names))
            to_state = draw(st.sampled_from(state_names))
            if from_state != to_state:
                transitions.append((from_state, to_state))

        # Optional: add self-transitions (event updates properties without state change)
        n_self = draw(st.integers(min_value=0, max_value=2))
        for _ in range(n_self):
            state = draw(st.sampled_from(state_names))
            transitions.append((state, state))

        # --- Build states with handlers ---
        states: dict[str, dict[str, Any]] = {s: {"when": {}} for s in state_names}

        # Group transitions by source state
        transitions_by_state: dict[str, list[tuple[str, str]]] = {s: [] for s in state_names}
        for from_s, to_s in transitions:
            transitions_by_state[from_s].append((from_s, to_s))

        # All event types we'll create (for source definition)
        all_event_types: set[str] = set()
        # Track which fields each event type needs
        event_fields: dict[str, list[tuple[str, str]]] = {}  # event_type → [(field, prop_type)]

        for from_s, trans_list in transitions_by_state.items():
            for from_state, to_state in trans_list:
                if from_state == to_state:
                    event_type = f"{from_state}_update"
                else:
                    event_type = _event_name(from_state, to_state)

                all_event_types.add(event_type)
                effects: list[Any] = []

                # Create effect — only in starts state, only on the first handler
                is_starts_handler = (from_state == starts and
                                     len(states[from_state]["when"]) == 0)
                if is_starts_handler:
                    effects.append("create")

                # Transition effect (skip for self-transitions)
                if from_state != to_state:
                    effects.append({"transition": {"to": to_state}})

                # Property set effects — pick 1-2 properties to set from event
                settable_props = [p for p in prop_types if p != "ident"]
                if settable_props:
                    n_sets = draw(st.integers(
                        min_value=1,
                        max_value=min(2, len(settable_props)),
                    ))
                    chosen = draw(st.sampled_from(
                        [sorted(settable_props)[:n_sets]]  # deterministic subset
                    )) if len(settable_props) <= n_sets else sorted(settable_props)[:n_sets]

                    for pname in chosen:
                        field_name = f"f_{pname}"
                        effects.append({
                            "set": {"property": pname, "from": f"event.{field_name}"},
                        })
                        if event_type not in event_fields:
                            event_fields[event_type] = []
                        event_fields[event_type].append((field_name, prop_types[pname]))

                # Always set identity field
                effects.append({
                    "set": {"property": "ident", "from": "event.ident"},
                })
                if event_type not in event_fields:
                    event_fields[event_type] = []
                if ("ident", "string") not in event_fields[event_type]:
                    event_fields[event_type].append(("ident", "string"))

                # Guard expression (probabilistic)
                handler: dict[str, Any] = {"effects": effects}
                if settable_props and draw(st.floats(min_value=0, max_value=1)) < guard_probability:
                    guard_prop = draw(st.sampled_from(settable_props))
                    guard_type = prop_types[guard_prop]
                    templates = GUARD_TEMPLATES.get(guard_type, [])
                    if templates:
                        field_name = f"f_{guard_prop}"
                        template = draw(st.sampled_from(templates))
                        handler["guard"] = template.format(
                            field=field_name, prop=guard_prop,
                        )
                        # Ensure the guard field is in event_fields
                        if (field_name, guard_type) not in event_fields.get(event_type, []):
                            if event_type not in event_fields:
                                event_fields[event_type] = []
                            event_fields[event_type].append((field_name, guard_type))

                # Avoid duplicate event types in the same state
                if event_type not in states[from_state]["when"]:
                    states[from_state]["when"][event_type] = handler

        # Time rules (probabilistic, on non-starts states)
        for sname in state_names[1:]:
            if draw(st.floats(min_value=0, max_value=1)) < time_rule_probability:
                # Pick a target state for the time rule transition
                other_states = [s for s in state_names if s != sname]
                if other_states:
                    target = draw(st.sampled_from(other_states))
                    rule_type = draw(st.sampled_from([
                        "inactivity", "state_duration",
                    ]))
                    threshold = draw(st.sampled_from(["1d", "7d", "30d", "90d"]))
                    states[sname]["after"] = [{
                        "type": rule_type,
                        "threshold": threshold,
                        "effects": [{"transition": {"to": target}}],
                    }]

        # --- Entity definition ---
        entities[entity_name] = {
            "starts": starts,
            "properties": properties,
            "identity": {"ident": {"match": "exact"}},
            "states": states,
        }

        # --- Source definition ---
        source_events: dict[str, Any] = {}
        for event_type in sorted(all_event_types):
            fields = event_fields.get(event_type, [])
            mappings: dict[str, Any] = {}
            for field_name, field_type in fields:
                mappings[field_name] = {"from": field_name}

            # Always include identity field mapping
            if "ident" not in mappings:
                mappings["ident"] = {"from": "ident"}

            source_events[event_type] = {
                "raw_type": event_type,
                "mappings": mappings,
                "hints": {entity_name: ["ident"]},
            }

        sources[f"src_{entity_name}"] = {
            "event_type": "type",
            "timestamp": "ts",
            "events": source_events,
        }

    return {
        "entities": entities,
        "sources": sources,
        "schemas": {},
        "_meta": {
            # Metadata for event generation — not part of the definition,
            # stripped before passing to Defacto.
            "event_fields": {
                ent_name: event_fields
                for ent_name, event_fields in [(f"ent{i}", event_fields) for i in range(n_entity_types)]
            },
            "prop_types": {
                _entity_name(i): prop_types
                for i in range(n_entity_types)
            },
        },
    }


# ---------------------------------------------------------------------------
# Event generator — produces valid events for a generated definition
# ---------------------------------------------------------------------------


@st.composite
def random_events(draw, definitions: dict[str, Any], n_events: int = 10):
    """Generate a list of valid events for a given definition.

    Walks the state machine graph to produce events that are valid
    for each entity's current state. Includes identity hints for
    merge scenarios.

    Args:
        definitions: A definition dict (from random_definitions).
        n_events: Number of events to generate.

    Returns:
        List of (source_name, events_list) tuples.
    """
    meta = definitions.get("_meta", {})
    all_events: list[tuple[str, list[dict[str, Any]]]] = []

    for entity_name, entity_def in definitions["entities"].items():
        source_name = f"src_{entity_name}"
        source_def = definitions["sources"][source_name]

        # Extract state graph
        graph: dict[str, dict[str, str]] = {}
        starts = entity_def["starts"]
        for state_name, state_def in entity_def.get("states", {}).items():
            handlers: dict[str, str] = {}
            for event_type, handler in state_def.get("when", {}).items():
                target = state_name
                for effect in handler.get("effects", []):
                    if isinstance(effect, dict) and "transition" in effect:
                        target = effect["transition"]["to"]
                handlers[event_type] = target
            graph[state_name] = handlers

        # Pool of identity values — small enough for merges to happen
        n_identities = draw(st.integers(min_value=3, max_value=8))
        ident_pool = [f"{entity_name}_{i}@test.gen" for i in range(n_identities)]

        # Track entity states for valid event generation
        entity_states: dict[str, str | None] = {}  # ident → current state

        # Get event field metadata
        entity_event_fields = meta.get("event_fields", {}).get(entity_name, {})
        entity_prop_types = meta.get("prop_types", {}).get(entity_name, {})

        events: list[dict[str, Any]] = []
        for ev_idx in range(n_events):
            ident = draw(st.sampled_from(ident_pool))
            current_state = entity_states.get(ident)

            if current_state is None:
                # Entity doesn't exist — find a create event in starts state
                available = graph.get(starts, {})
                create_events = []
                for et, handler_def in entity_def["states"].get(starts, {}).get("when", {}).items():
                    for eff in handler_def.get("effects", []):
                        if eff == "create":
                            create_events.append(et)
                if create_events:
                    event_type = draw(st.sampled_from(create_events))
                else:
                    # No create event found — use first available
                    if available:
                        event_type = draw(st.sampled_from(list(available.keys())))
                    else:
                        continue
                target = available.get(event_type, starts)
                entity_states[ident] = target
            else:
                # Pick a valid event for current state
                available = graph.get(current_state, {})
                if not available:
                    continue  # terminal state
                event_type = draw(st.sampled_from(sorted(available.keys())))
                entity_states[ident] = available[event_type]

            # Build event dict
            ts_day = (ev_idx % 28) + 1
            ts_hour = ev_idx % 24
            event: dict[str, Any] = {
                "type": event_type,
                "ts": f"2024-01-{ts_day:02d}T{ts_hour:02d}:00:00Z",
                "ident": ident,
            }

            # Add fields required by this event type
            for field_name, field_type in entity_event_fields.get(event_type, []):
                if field_name == "ident":
                    continue  # already set
                values = PROPERTY_VALUES.get(field_type, ["default"])
                event[field_name] = draw(st.sampled_from(values))

            events.append(event)

        if events:
            all_events.append((source_name, events))

    return all_events


# ---------------------------------------------------------------------------
# Operation sequence generator — lifecycle operations for a definition
# ---------------------------------------------------------------------------


@st.composite
def random_operations(draw, definitions: dict[str, Any], max_ops: int = 12):
    """Generate a random sequence of lifecycle operations.

    Operations: ingest, build, merge, erase, tick.
    Each operation is a tuple describing what to do.

    Args:
        definitions: A definition dict (from random_definitions).
        max_ops: Maximum number of operations.

    Returns:
        List of operation tuples.
    """
    entity_names = list(definitions["entities"].keys())
    meta = definitions.get("_meta", {})

    # Pool of identity values for merge/erase — matches event generator
    ident_pool = [f"{entity_names[0]}_{i}@test.gen" for i in range(8)]

    n_ops = draw(st.integers(min_value=2, max_value=max_ops))
    ops: list[tuple] = []

    for _ in range(n_ops):
        op_type = draw(st.sampled_from(["ingest", "build", "merge", "erase", "tick"]))

        if op_type == "ingest":
            # Generate a small batch of events
            event_batches = draw(random_events(definitions, n_events=draw(
                st.integers(min_value=1, max_value=5),
            )))
            if event_batches:
                source_name, events = event_batches[0]
                ops.append(("ingest", source_name, events))
            else:
                ops.append(("build",))

        elif op_type == "merge":
            ident_a = draw(st.sampled_from(ident_pool))
            ident_b = draw(st.sampled_from(ident_pool))
            entity_name = entity_names[0]
            ops.append(("merge", entity_name, ident_a, ident_b))

        elif op_type == "erase":
            ident = draw(st.sampled_from(ident_pool))
            entity_name = entity_names[0]
            ops.append(("erase", entity_name, ident))

        else:
            ops.append((op_type,))

    return ops
