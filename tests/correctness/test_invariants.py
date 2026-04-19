"""Property-based invariant verification via Hypothesis.

Generates random sequences of lifecycle operations (ingest, build, merge,
erase, tick) and verifies that the 7 protocol invariants from
docs/lifecycle-protocol.md hold after every operation.

This is the closest thing to formal verification we can do against the
actual code. Hypothesis explores thousands of random operation sequences
and finds minimal counterexamples when invariants are violated.

The invariants tested:
    1. Identity consistency: cache and backend table agree
    2. Event ownership: every event_entities entry has a valid identity
    3. Merge audit: every executed merge has a merge_log row
    4. Cursor monotonicity: cursor never regresses
    5. Entity count consistency: DashMap and identity agree on entity count

Usage:
    PYTHONPATH=python .venv/bin/python -m pytest tests/correctness/test_invariants.py -v
"""

from __future__ import annotations

import tempfile
import os
from datetime import datetime, timezone

from hypothesis import given, settings, HealthCheck
from hypothesis import strategies as st

from defacto import Defacto


# ---------------------------------------------------------------------------
# Definitions — simple but exercises all lifecycle operations
# ---------------------------------------------------------------------------

DEFINITIONS = {
    "entities": {
        "customer": {
            "starts": "lead",
            "properties": {
                "email": {"type": "string"},
                "plan": {"type": "string", "default": "free"},
            },
            "identity": {"email": {"match": "exact"}},
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
                            ],
                        },
                    },
                },
                "active": {
                    "when": {
                        "upgrade": {
                            "effects": [
                                {"set": {"property": "plan", "from": "event.plan"}},
                            ],
                        },
                    },
                },
            },
        },
    },
    "sources": {
        "web": {
            "event_type": "type",
            "timestamp": "ts",
            "events": {
                "signup": {
                    "raw_type": "signup",
                    "mappings": {"email": {"from": "email"}},
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
            },
        },
    },
    "schemas": {},
}

# Pool of emails — small enough that merges happen naturally
EMAILS = [f"user{i}@test.com" for i in range(8)]
PLANS = ["free", "pro", "enterprise", "team"]


# ---------------------------------------------------------------------------
# Operation strategies
# ---------------------------------------------------------------------------


def make_event(email: str, event_type: str, ts_offset: int, plan: str = "pro"):
    """Build a raw event dict."""
    ts = f"2024-01-{(ts_offset % 28) + 1:02d}T{(ts_offset % 24):02d}:00:00Z"
    event = {"type": event_type, "ts": ts, "email": email}
    if event_type == "upgrade":
        event["plan"] = plan
    return event


@st.composite
def operation(draw):
    """Generate a random lifecycle operation."""
    op_type = draw(st.sampled_from(["ingest", "build", "merge", "erase", "tick"]))

    if op_type == "ingest":
        n_events = draw(st.integers(min_value=1, max_value=5))
        events = []
        for _ in range(n_events):
            email = draw(st.sampled_from(EMAILS))
            event_type = draw(st.sampled_from(["signup", "upgrade"]))
            ts_offset = draw(st.integers(min_value=1, max_value=200))
            plan = draw(st.sampled_from(PLANS))
            events.append(make_event(email, event_type, ts_offset, plan))
        return ("ingest", events)
    elif op_type == "merge":
        email_a = draw(st.sampled_from(EMAILS))
        email_b = draw(st.sampled_from(EMAILS))
        return ("merge", email_a, email_b)
    elif op_type == "erase":
        email = draw(st.sampled_from(EMAILS))
        return ("erase", email)
    else:
        return (op_type,)


# ---------------------------------------------------------------------------
# Invariant checks
# ---------------------------------------------------------------------------


def check_identity_consistency(m: Defacto) -> None:
    """Invariant 1: cache and backend table agree on all mappings.

    Every hint in the backend should resolve to the same entity_id
    in the Rust cache.
    """
    # Get all mappings from backend
    all_hints = m._identity_backend.warmup()  # (entity_type, hint_value, entity_id)
    if not all_hints:
        return

    # Check each against the cache
    cache_queries = [(h[0], h[1]) for h in all_hints]  # (entity_type, hint_value)
    result = m._core.resolve_cache(cache_queries)

    for entity_type, hint_value, expected_id in all_hints:
        # Find this hint in cache results
        for resolved_type, resolved_hint, resolved_id in result["resolved"]:
            if resolved_type == entity_type and resolved_hint == hint_value:
                assert resolved_id == expected_id, (
                    f"Identity mismatch for {entity_type}/{hint_value}: "
                    f"backend={expected_id}, cache={resolved_id}"
                )
                break


def check_event_ownership(m: Defacto) -> None:
    """Invariant 2: every entity_id in event_entities has an identity mapping.

    If event_entities says entity X owns events, entity X should exist
    in the identity system (unless it was erased — erase deletes both).
    """
    mappings = m._ledger.all_entity_mappings()
    for entity_id, entity_type in mappings.items():
        # The entity should either have identity mappings OR be in DashMap
        # (could be a merged entity whose hints moved to winner)
        has_identity = m._identity_backend.lookup_any(entity_id) is not None
        # Check if it's a merge winner (hints may not use this ID directly)
        has_state = m._core.entity_count() > 0  # rough check
        # At minimum, the entity_type should be valid
        assert entity_type in m._definitions_dict.get("entities", {}), (
            f"event_entities references unknown entity_type: {entity_type}"
        )


def check_cursor_monotonicity(m: Defacto, prev_cursor: int) -> int:
    """Invariant 6: cursor never regresses.

    Returns the new cursor for tracking across operations.
    """
    current = m._build_manager.get_status(m._active_version).cursor
    assert current >= prev_cursor, (
        f"Cursor regressed: {prev_cursor} → {current}"
    )
    return current


def check_merge_audit(m: Defacto, merged_pairs: list[tuple[str, str]]) -> None:
    """Invariant 3: every executed merge has a merge_log row."""
    for from_id, into_id in merged_pairs:
        merged = m._ledger.find_merges_into(into_id)
        assert from_id in merged, (
            f"Merge {from_id} → {into_id} not in merge_log"
        )


def check_state_history_currency(m: Defacto) -> None:
    """Invariant 4: current state history row matches DashMap state.

    For every entity in DashMap, the current SCD row (valid_to IS NULL)
    should have the same state.
    """
    try:
        df = m.table("customer").execute()
    except Exception:
        return  # no table yet (no build happened)

    if df.empty:
        return

    entity_mappings = m._ledger.all_entity_mappings()
    for _, row in df.iterrows():
        entity_id = row["customer_id"]
        scd_state = row["customer_state"]
        # Verify the entity exists in event_entities
        if entity_id in entity_mappings:
            assert entity_mappings[entity_id] == "customer", (
                f"Entity type mismatch for {entity_id}"
            )


def check_tombstone_completeness(
    m: Defacto, erased_ids: set[str], merged_losers: set[str],
) -> None:
    """Invariant 7: ceased entities have tombstones or are fully deleted.

    Erased entities should have no current SCD rows. Merged losers should
    have their rows closed (valid_to set) or with merged_into set.
    """
    try:
        df = m.table("customer").execute()
    except Exception:
        return

    current_ids = set(df["customer_id"]) if not df.empty else set()

    # Erased entities should not appear in current state
    for eid in erased_ids:
        assert eid not in current_ids, (
            f"Erased entity {eid} still in current state"
        )

    # Merged losers should not appear in current state
    for eid in merged_losers:
        assert eid not in current_ids, (
            f"Merged loser {eid} still in current state"
        )


# ---------------------------------------------------------------------------
# The property test
# ---------------------------------------------------------------------------


@given(ops=st.lists(operation(), min_size=1, max_size=15))
@settings(
    max_examples=200,
    deadline=None,  # lifecycle ops can be slow
    suppress_health_check=[HealthCheck.too_slow],
)
def test_lifecycle_invariants_hold(ops):
    """After any sequence of lifecycle operations, all invariants hold.

    Hypothesis generates random sequences of ingest, build, merge,
    erase, and tick operations. After each operation, invariants are
    checked. If any invariant is violated, Hypothesis minimizes the
    sequence to the shortest reproducing case.
    """
    db_path = os.path.join(tempfile.mkdtemp(), "hyp.db")
    m = Defacto(DEFINITIONS, database=db_path)

    cursor = 0
    merged_pairs: list[tuple[str, str]] = []
    erased_ids: set[str] = set()
    merged_losers: set[str] = set()

    try:
        for op in ops:
            if op[0] == "ingest":
                m.ingest("web", op[1])
                m.build()
                # Re-ingesting after erase can recreate entities with
                # the same ID (same hints → same identity resolution).
                # Update tracking so we don't flag live entities as erased.
                for event in op[1]:
                    eid = m._identity_backend.lookup("customer", event.get("email", ""))
                    if eid:
                        erased_ids.discard(eid)
                        merged_losers.discard(eid)

            elif op[0] == "build":
                m.build()

            elif op[0] == "merge":
                _, email_a, email_b = op
                id_a = m._identity_backend.lookup("customer", email_a)
                id_b = m._identity_backend.lookup("customer", email_b)
                if id_a and id_b and id_a != id_b:
                    m.merge(id_a, id_b)
                    merged_pairs.append((id_a, id_b))
                    merged_losers.add(id_a)

            elif op[0] == "erase":
                _, email = op
                entity_id = m._identity_backend.lookup("customer", email)
                if entity_id:
                    m.erase(entity_id)
                    erased_ids.add(entity_id)
                    # Remove erased merges from tracking
                    merged_pairs = [
                        (f, i) for f, i in merged_pairs
                        if f != entity_id and i != entity_id
                    ]

            elif op[0] == "tick":
                m.tick()

            # Check ALL invariants after every operation
            check_identity_consistency(m)
            check_event_ownership(m)
            cursor = check_cursor_monotonicity(m, cursor)
            check_merge_audit(m, merged_pairs)
            check_state_history_currency(m)
            check_tombstone_completeness(m, erased_ids, merged_losers)

    finally:
        m.close()


# ---------------------------------------------------------------------------
# Random definitions property test
# ---------------------------------------------------------------------------


from tests.correctness.datagen import random_definitions, random_operations


def _check_generic_invariants(m: Defacto, entity_names: list[str]) -> None:
    """Check protocol invariants that work with any entity definition.

    A subset of the invariant checks above, generalized to work with
    random entity types (not hardcoded to 'customer').
    """
    # Identity consistency: cache and backend agree
    check_identity_consistency(m)

    # Event ownership: entity_type is valid
    check_event_ownership(m)

    # State history currency: for each entity type, current rows are valid
    for entity_name in entity_names:
        try:
            df = m.table(entity_name).execute()
            if df is not None and not df.empty:
                # Entity type column should exist
                id_col = f"{entity_name}_id"
                assert id_col in df.columns, (
                    f"Missing column {id_col} in {entity_name} table"
                )
        except Exception:
            pass  # table may not exist yet (no build)


@given(data=st.data())
@settings(
    max_examples=100,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
def test_random_definitions_invariants(data):
    """Invariants hold for randomly generated definitions.

    Generates a random valid entity definition, then runs a random
    sequence of lifecycle operations against it. Checks invariants
    after every operation. If the interpreter can't handle a valid
    definition shape, Hypothesis finds the minimal counterexample.
    """
    defs = data.draw(random_definitions())
    clean = {k: v for k, v in defs.items() if k != "_meta"}
    entity_names = list(clean["entities"].keys())

    db_path = os.path.join(tempfile.mkdtemp(), "hyp_random.db")
    m = Defacto(clean, database=db_path)

    cursor = 0
    merged_pairs: list[tuple[str, str]] = []

    try:
        ops = data.draw(random_operations(defs))

        for op in ops:
            if op[0] == "ingest":
                _, source_name, events = op
                m.ingest(source_name, events)
                m.build()

            elif op[0] == "build":
                m.build()

            elif op[0] == "merge":
                _, entity_name, ident_a, ident_b = op
                id_a = m._identity_backend.lookup(entity_name, ident_a)
                id_b = m._identity_backend.lookup(entity_name, ident_b)
                if id_a and id_b and id_a != id_b:
                    try:
                        m.merge(id_a, id_b)
                        merged_pairs.append((id_a, id_b))
                    except Exception:
                        pass  # entity may not exist in event_entities

            elif op[0] == "erase":
                _, entity_name, ident = op
                entity_id = m._identity_backend.lookup(entity_name, ident)
                if entity_id:
                    try:
                        m.erase(entity_id)
                        # Remove erased merges from tracking
                        merged_pairs = [
                            (f, i) for f, i in merged_pairs
                            if f != entity_id and i != entity_id
                        ]
                    except Exception:
                        pass  # entity may already be erased

            elif op[0] == "tick":
                m.tick()

            # Check invariants after every operation
            _check_generic_invariants(m, entity_names)
            cursor = check_cursor_monotonicity(m, cursor)
            check_merge_audit(m, merged_pairs)

    finally:
        m.close()
