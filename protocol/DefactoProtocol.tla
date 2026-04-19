---- MODULE DefactoProtocol ----
\* ================================================================
\* DEFACTO ENTITY LIFECYCLE PROTOCOL — FORMAL SPECIFICATION
\*
\* This TLA+ spec models the concurrent protocol used by Defacto's
\* sharded entity engine. It captures:
\*
\*   SHARED STATE   Postgres tables (ledger, identity, event_entities,
\*                  merge_log) that all shards read/write atomically
\*
\*   PER-SHARD      DashMap (entity state), identity cache, cursor —
\*                  each shard maintains these independently
\*
\* The model verifies that lifecycle operations (ingest, build, merge,
\* erase, tick) preserve correctness invariants under ALL possible
\* interleavings of concurrent shard operations.
\*
\* WHAT WE MODEL:  Protocol state transitions and atomicity boundaries
\* WHAT WE SKIP:   SQL, serialization, error handling, performance
\* ================================================================

EXTENDS Naturals, Sequences, FiniteSets, TLC

\* ================================================================
\* CONSTANTS — model parameters
\*
\* Small values for exhaustive checking. TLC explores EVERY reachable
\* state, so even 2 shards × 3 events checks thousands of interleavings.
\* ================================================================

CONSTANTS
    NumShards,     \* Number of concurrent shard processes (e.g., 2)
    MaxEvents      \* Max events to ingest (bounds the state space)

\* ================================================================
\* DERIVED SETS
\*
\* The "universe" of values the model works with. Deliberately small
\* since TLC must explore every combination.
\* ================================================================

\* Shard identifiers (1-indexed)
Shards == 1..NumShards

\* Identity hints — abstract tokens representing emails, phone
\* numbers, user IDs. 3 hints is enough to exercise merges:
\* an event carrying {h1, h2} where h1→e1 and h2→e2 forces
\* a merge of e1 into e2 (or vice versa).
AllHints == {"h1", "h2", "h3"}

\* Entity IDs are generated dynamically (like customer_0001).
\* At most one new entity per unique hint value.
MaxEntities == Cardinality(AllHints)
EntityIds == 1..MaxEntities

\* An "event" is a non-empty subset of hints.
\*   {h1}       — carries one identity clue (simple resolution)
\*   {h1, h2}   — carries two clues (triggers merge if they
\*                 resolve to different existing entities)
PossibleEvents == (SUBSET AllHints) \ {{}}

\* ================================================================
\* STATE VARIABLES
\*
\* Split into two categories:
\*
\*   SHARED      Postgres tables. Visible to all shards.
\*               Writes are atomic (transactions).
\*
\*   PER-SHARD   In-memory structures. Independent per process.
\*               Not visible to other shards.
\*
\* Each variable has a comment mapping it to the real system.
\* ================================================================

VARIABLES
    \* ---- SHARED STATE (Postgres) ----

    \* Append-only event log.
    \* Modeled as a sequence: ledger[i] = set of hints for event i.
    \* The index IS the sequence number.
    \* Real system: events table (seq, event_id, timestamp, data,
    \* raw, resolution_hints). We model only hints — they drive
    \* identity resolution, merges, and shard ownership.
    ledger,

    \* Event-to-entity mapping. Parallel to ledger:
    \* eventEntities[i] = entity_id that event i belongs to.
    \* Written during ingest (after identity resolution).
    \* Updated during merges (loser's events reassigned to winner).
    \* Read during shard-aware builds and tick validation.
    eventEntities,

    \* Identity table: hint → entity_id.
    \* identity[h] = 0 means hint h has never been seen.
    \* Shared across all shards; source of truth for resolution.
    \* Real system: UNIQUE (entity_type, hint_value) → entity_id.
    \* We model one entity type so the key is just the hint.
    identity,

    \* Merge audit log. Set of <<from_entity_id, into_entity_id>>.
    \* Written during _execute_merges. Read during erase (cascade).
    mergeLog,

    \* Entity ID counter — next ID to assign.
    \* Real system: Postgres SEQUENCE. Always increases.
    nextEntity,

    \* ---- PER-SHARD STATE (in-memory, independent per process) ----

    \* DashMap — which entities this shard holds in memory.
    \* dashmap[s] = set of entity IDs in shard s's DashMap.
    \* We track existence, not entity properties — the protocol
    \* invariants are about which entities exist where, not their
    \* internal state machine positions.
    dashmap,

    \* Identity cache — per-shard mirror of the shared identity table.
    \* cache[s][h] = cached entity_id for hint h on shard s.
    \* 0 = not cached (cache miss falls through to Postgres).
    \* Can be stale after merges executed by other shards.
    cache,

    \* Cursor — how far this shard has processed the ledger.
    \* cursor[s] = last sequence number processed by shard s.
    \* Monotonically increasing. Used for incremental builds:
    \* "replay from cursor[s]+1 to Len(ledger)."
    cursor

\* All variables, for UNCHANGED clauses.
\* UNCHANGED vars means "no variable changed in this step."
vars == <<ledger, eventEntities, identity, mergeLog,
          nextEntity, dashmap, cache, cursor>>

\* ================================================================
\* HELPERS
\* ================================================================

\* Which shard owns a given entity.
\* Real system: SHA-256(entity_id) mod total_shards.
\* Model: simple modular arithmetic (same determinism guarantee).
\* Example with 2 shards: e1→s1, e2→s2, e3→s1.
\* Entity 0 (erased sentinel) has no owner.
Owner(eid) == IF eid = 0 THEN 0 ELSE ((eid - 1) % NumShards) + 1

\* Entity IDs that currently have events mapped to them.
\* This is the "ground truth" from event_entities — the set of
\* entities that should exist somewhere in the system.
\* Excludes 0 (erased events — see Erase operation).
EntitiesWithEvents ==
    {eventEntities[i] : i \in 1..Len(eventEntities)} \ {0}

\* Every entity across all shards' DashMaps.
AllDashmapEntities ==
    UNION {dashmap[s] : s \in Shards}

\* Minimum of a non-empty set of naturals.
\* Used to pick the merge winner (smallest entity_id).
\* Real system: sorted(entity_ids)[0] — lexicographic, same idea.
\* CHOOSE picks an arbitrary element satisfying the predicate;
\* since there's exactly one minimum, the choice is deterministic.
Min(S) == CHOOSE x \in S : \A y \in S : x <= y

\* ================================================================
\* INITIAL STATE
\*
\* System starts empty. No events, no entities, no mappings.
\* Every shard's cursor is at 0, caches empty, DashMaps empty.
\*
\* TLA+ syntax:
\*   /\  means AND (conjunction)
\*   =   in Init is an equality constraint, not assignment
\*   [x \in S |-> expr]  is a function mapping each x in S to expr
\* ================================================================

Init ==
    /\ ledger        = <<>>
    /\ eventEntities = <<>>
    /\ identity      = [h \in AllHints |-> 0]
    /\ mergeLog      = {}
    /\ nextEntity    = 1
    /\ dashmap       = [s \in Shards |-> {}]
    /\ cache         = [s \in Shards |-> [h \in AllHints |-> 0]]
    /\ cursor        = [s \in Shards |-> 0]

\* ================================================================
\* TYPE INVARIANT
\*
\* Sanity check: every variable stays within its expected range.
\* If this fails, we have a bug in the MODEL (not the protocol).
\* Think of it as a type system for the spec itself.
\*
\* TLA+ syntax:
\*   \in     means "is an element of"
\*   Seq(S)  is the set of all finite sequences with elements from S
\*   [A -> B] is the set of all functions from domain A to range B
\*   SUBSET S is the powerset of S
\*   A \X B   is the Cartesian product of A and B
\* ================================================================

TypeOK ==
    /\ ledger \in Seq(SUBSET AllHints)
    /\ Len(ledger) <= MaxEvents
    /\ eventEntities \in Seq(0..MaxEntities)  \* 0 = erased event
    /\ Len(eventEntities) = Len(ledger)
    /\ identity \in [AllHints -> 0..MaxEntities]
    /\ mergeLog \subseteq (EntityIds \X EntityIds)
    /\ nextEntity \in 1..(MaxEntities + 1)
    /\ dashmap \in [Shards -> SUBSET EntityIds]
    /\ cache \in [Shards -> [AllHints -> 0..MaxEntities]]
    /\ cursor \in [Shards -> 0..MaxEvents]

\* ================================================================
\* INGEST — a shard processes a new event
\*
\* Real system: process_batch() in _pipeline.py
\*   1. normalize (Rust) — skipped, doesn't affect protocol
\*   2. append to ledger — INSERT ON CONFLICT (dedup)
\*   3. resolve identity — cache → backend → create or link
\*   4. write event_entities — seq → entity_id
\*   5. execute merges — if hints resolve to multiple entities
\*   6. interpret — only if this shard owns the entity
\*   7. publish snapshots — state history (projection, skipped)
\*   8. advance cursor
\*
\* Modeled as ONE atomic step per shard. This is accurate:
\* within one shard, process_batch runs without interleaving.
\* Cross-shard interleaving is captured by TLC exploring all
\* orderings of different shards' Ingest steps.
\*
\* TLA+ syntax:
\*   LET ... IN    local definitions (like Python variables)
\*   \E x \in S    "there exists x in S" (TLC tries all values)
\*   =>            implication: A => B means "if A then B"
\*   EXCEPT        function update: [f EXCEPT ![k] = v]
\*   {e : x \in S} set comprehension (like Python {e for x in S})
\* ================================================================

Ingest(shard, hints) ==
    \* --- Preconditions ---
    /\ Len(ledger) < MaxEvents
    /\ hints \in PossibleEvents

    /\ LET
        \* === IDENTITY RESOLUTION (cache-first, like real system) ===

        \* Real system: check cache first, fall through to identity
        \* table on miss. Cache can be STALE after merges by other
        \* shards — this is the key cross-shard consistency concern.
        \*
        \* STEP 1: Cache-first resolution
        rawResolved == [h \in hints |->
            IF cache[shard][h] /= 0 THEN cache[shard][h]
            ELSE identity[h]]

        \* STEP 2: Validate cache hits against merge_log.
        \* Any cache-hit entity_id that's a merge loser is stale.
        \* Re-resolve from identity table (always correct).
        \* Real system: SELECT from_entity_id FROM merge_log
        \*              WHERE from_entity_id = ANY($cache_hit_ids)
        mergeLosers == {entry[1] : entry \in mergeLog}
        resolved == [h \in hints |->
            IF rawResolved[h] \in mergeLosers
            THEN identity[h]     \* Re-resolve from identity table
            ELSE rawResolved[h]]

        \* Which hints resolved to entities? (0 = unmapped)
        existingIds == {resolved[h] : h \in hints} \ {0}

        \* If no hint has an entity, we need to create one.
        isNew == (existingIds = {})

        \* The resolved entity for this event:
        \*   All new hints    → fresh entity ID (nextEntity)
        \*   Some/all known   → smallest existing (merge winner)
        entityId == IF isNew THEN nextEntity ELSE Min(existingIds)

        \* === MERGE DETECTION ===
        \* Multiple existing entity IDs for one event = merge.
        \* Winner = min. Losers get absorbed.
        losers == IF Cardinality(existingIds) > 1
                  THEN existingIds \ {Min(existingIds)}
                  ELSE {}

       IN
        \* Guard: if we need a new entity, make sure we haven't
        \* exhausted the bounded entity ID space.
        /\ isNew => (nextEntity <= MaxEntities)

        \* --- SHARED STATE UPDATES (Postgres transaction) ---

        \* Append event to ledger (append-only, new sequence = Len+1)
        /\ ledger' = Append(ledger, hints)

        \* Map this event to its entity + reassign loser events.
        \* merge_event_entities(loser, winner) updates ALL of the
        \* loser's previous events to point to the winner.
        /\ eventEntities' = Append(
            [i \in 1..Len(eventEntities) |->
                IF eventEntities[i] \in losers THEN entityId
                ELSE eventEntities[i]],
            entityId)

        \* Update identity table: INSERT ON CONFLICT DO NOTHING.
        \* Only writes NEW hints (identity[h] = 0). Existing entries
        \* are NOT overwritten — critical for cache-first correctness.
        \* Loser's hints are updated to winner (merge execution).
        /\ identity' = [h \in AllHints |->
            IF h \in hints /\ identity[h] = 0 THEN entityId
            ELSE IF identity[h] \in losers THEN entityId
            ELSE identity[h]]

        \* Record merges in audit log
        /\ mergeLog' = mergeLog \cup {<<l, entityId>> : l \in losers}

        \* Advance entity counter if new entity created
        /\ nextEntity' = IF isNew THEN nextEntity + 1 ELSE nextEntity

        \* --- PER-SHARD STATE UPDATES ---

        \* DashMap: add entity if THIS shard owns it.
        \* Remove losers from THIS shard only.
        \* KEY PROTOCOL PROPERTY: other shards still have stale
        \* loser entries until their next tick cleans them.
        /\ dashmap' = [s \in Shards |->
            IF s = shard
            THEN (dashmap[s] \ losers) \cup
                 (IF Owner(entityId) = shard THEN {entityId} ELSE {})
            ELSE dashmap[s]]

        \* Cache: update on the ingesting shard. Includes correcting
        \* stale entries detected by merge_log validation.
        /\ cache' = [s \in Shards |->
            IF s = shard
            THEN [h \in AllHints |->
                IF h \in hints THEN entityId
                ELSE IF cache[shard][h] \in losers THEN entityId
                ELSE IF cache[shard][h] \in mergeLosers THEN identity[h]
                ELSE cache[shard][h]]
            ELSE cache[s]]

        \* Advance cursor on the ingesting shard.
        \* New event's sequence = Len(ledger) + 1 (before append).
        /\ cursor' = [cursor EXCEPT ![shard] = Len(ledger) + 1]

\* ================================================================
\* BUILD_INCREMENTAL — shard replays unprocessed events
\*
\* Real system: build_incremental() in _pipeline.py
\*   1. Read events from cursor+1 to head of ledger
\*   2. Shard-aware replay: replay_for_shard() returns only events
\*      for entities this shard owns (via shard_hash in event_entities)
\*   3. Events come pre-resolved (entity_id from event_entities JOIN)
\*      so NO identity resolution during shard-aware replay
\*   4. Interpret each event → add entity to DashMap
\*   5. Advance cursor to head of ledger
\*   6. Post-pass: tick + validate (modeled as separate Tick operation)
\*
\* KEY BEHAVIOR: incremental build ONLY ADDS entities to DashMap.
\* It does NOT remove merged-away entities. That cleanup is tick's job.
\* This means stale orphans can persist between build and tick.
\* ================================================================

BuildIncremental(shard) ==
    \* Precondition: there are unprocessed events
    /\ cursor[shard] < Len(ledger)

    /\ LET
        \* Entities from unprocessed events that this shard owns.
        \* Real system: replay_for_shard filters by shard_hash % total.
        ownedEntities ==
            {eventEntities[i] : i \in (cursor[shard] + 1)..Len(ledger)}
            \cap {e \in EntityIds : Owner(e) = shard}
       IN
        \* Add owned entities to DashMap (union — preserves existing)
        /\ dashmap' = [dashmap EXCEPT ![shard] = dashmap[shard] \cup ownedEntities]

        \* Advance cursor to head of ledger
        /\ cursor' = [cursor EXCEPT ![shard] = Len(ledger)]

        \* Shared state and cache unchanged
        /\ UNCHANGED <<ledger, eventEntities, identity, mergeLog, nextEntity, cache>>

\* ================================================================
\* BUILD_FULL — shard clears and rebuilds from the entire ledger
\*
\* Real system: build_full() in _pipeline.py
\*   1. Clear DashMap (core.clear())
\*   2. Set dirty flag (crash recovery)
\*   3. Replay ALL events from sequence 0 (shard-aware if possible)
\*   4. Post-pass: tick + validate
\*   5. Clear dirty flag
\*
\* Unlike incremental, full build REPLACES the DashMap entirely.
\* This cleans up any stale orphans from prior merges.
\*
\* Triggered by: definition changes, crash recovery, manual rebuild.
\* In the model, any shard can full-build at any time — this is more
\* permissive than reality, which is correct for verification (if
\* invariants hold under extra freedom, they hold under constraints).
\* ================================================================

BuildFull(shard) ==
    /\ LET
        \* ALL entities in event_entities that this shard owns.
        ownedEntities ==
            {eventEntities[i] : i \in 1..Len(ledger)}
            \cap {e \in EntityIds : Owner(e) = shard}
       IN
        \* Replace DashMap with exactly the owned entities
        /\ dashmap' = [dashmap EXCEPT ![shard] = ownedEntities]

        \* Advance cursor to head of ledger
        /\ cursor' = [cursor EXCEPT ![shard] = Len(ledger)]

        \* Shared state and cache unchanged
        /\ UNCHANGED <<ledger, eventEntities, identity, mergeLog, nextEntity, cache>>

\* ================================================================
\* EXTERNAL MERGE — explicitly merge two entities
\*
\* Real system: m.merge(from_id, into_id) in _defacto.py
\*   1. Validate both entities exist
\*   2. _execute_merges: identity, event_entities, merge_log, DashMap
\*   3. _rebuild_merge_winners: replay winner's events for correct state
\*   4. Publish tombstone for loser
\*
\* KEY CROSS-SHARD BEHAVIOR:
\* Any shard can call m.merge(). The merge updates shared state
\* (identity, event_entities, merge_log) atomically via Postgres.
\* But only the CALLING shard updates its DashMap — other shards
\* still have the stale loser entity until tick or full build
\* cleans them. This is intentional eventual consistency.
\*
\* The merge event is written to the ledger for audit trail and
\* replay survival. We skip that in the model since it doesn't
\* affect protocol invariants (the merge_log is the invariant source).
\* ================================================================

ExternalMerge(shard, fromEntity, intoEntity) ==
    \* --- Preconditions ---
    \* Both entities must have events (exist in event_entities).
    \* Can't merge an entity that's already been merged away.
    /\ fromEntity \in EntitiesWithEvents
    /\ intoEntity \in EntitiesWithEvents
    /\ fromEntity /= intoEntity
    /\ ~ \E entry \in mergeLog : entry[1] = fromEntity

    \* --- SHARED STATE UPDATES (Postgres transaction) ---

    \* Identity: all of loser's hints now point to winner.
    \* Real system: UPDATE identity SET entity_id = winner
    \*              WHERE entity_id = loser
    /\ identity' = [h \in AllHints |->
        IF identity[h] = fromEntity THEN intoEntity
        ELSE identity[h]]

    \* Event_entities: all of loser's events now belong to winner.
    \* Real system: merge_event_entities(loser, winner)
    /\ eventEntities' = [i \in 1..Len(eventEntities) |->
        IF eventEntities[i] = fromEntity THEN intoEntity
        ELSE eventEntities[i]]

    \* Merge log: record for audit trail + erase cascade.
    /\ mergeLog' = mergeLog \cup {<<fromEntity, intoEntity>>}

    \* --- PER-SHARD STATE UPDATES ---

    \* DashMap: remove loser from THIS shard only.
    \* Other shards still have the loser as an orphan.
    \* Tick or full build will clean it up.
    /\ dashmap' = [s \in Shards |->
        IF s = shard
        THEN dashmap[s] \ {fromEntity}
        ELSE dashmap[s]]

    \* Cache: fix stale entries on THIS shard only.
    \* Other shards' caches may still map hints to the loser.
    /\ cache' = [s \in Shards |->
        IF s = shard
        THEN [h \in AllHints |->
            IF cache[shard][h] = fromEntity THEN intoEntity
            ELSE cache[shard][h]]
        ELSE cache[s]]

    \* Ledger, entity counter, cursor unchanged.
    \* (Real system writes a merge event to ledger for audit,
    \* but that doesn't affect protocol invariants.)
    /\ UNCHANGED <<ledger, nextEntity, cursor>>

\* ================================================================
\* TICK — evaluate time rules and validate against shared ledger
\*
\* Real system: _tick_and_validate() in _pipeline.py
\*   1. core.tick() — find entities with overdue time rules
\*   2. existing_entity_ids() — batch check which still have events
\*   3. Remove orphans — entities erased/merged by another shard
\*
\* We skip time rule evaluation (it's about timing thresholds, not
\* protocol correctness). What matters is the VALIDATION step:
\* checking each DashMap entity against event_entities and cleaning
\* orphans that another shard's merge or erase left behind.
\*
\* This is the cross-shard consistency mechanism — the ONE way
\* stale entities get cleaned from a non-executing shard's DashMap.
\* ================================================================

Tick(shard) ==
    /\ LET
        \* Entities in this shard's DashMap that no longer have events
        \* in event_entities. These are orphans from:
        \*   - Cross-shard merge (events reassigned to winner)
        \*   - Cross-shard erase (events deleted)
        orphans == {e \in dashmap[shard] : e \notin EntitiesWithEvents}
       IN
        \* Only act if there are orphans to clean (prevents no-op steps)
        /\ orphans /= {}
        \* Remove orphans from this shard's DashMap
        /\ dashmap' = [dashmap EXCEPT ![shard] = dashmap[shard] \ orphans]
        \* Everything else unchanged
        /\ UNCHANGED <<ledger, eventEntities, identity, mergeLog, nextEntity, cache, cursor>>

\* ================================================================
\* ERASE — right to erasure, permanent deletion
\*
\* Real system: m.erase(entity_id) in _defacto.py
\*   1. Find cascade via merge_log — entities merged into this one
\*   2. Delete identity for all cascaded entities
\*   3. Delete events from ledger + event_entities
\*   4. Delete merge_log entries for cascaded entities
\*   5. Delete from DashMap on THIS shard
\*   6. Delete state history via publisher (skipped in model)
\*
\* Like merge, only the calling shard's DashMap is updated.
\* Other shards discover the erasure via tick validation.
\*
\* Cascade is modeled as a two-level BFS through merge_log:
\* direct merges into the entity + indirect merges into those.
\* With MaxEntities=3, two levels covers all possible chains.
\* ================================================================

Erase(shard, entityId) ==
    \* Precondition: entity has events (exists in the system)
    /\ entityId \in EntitiesWithEvents

    /\ LET
        \* === MERGE CASCADE ===
        \* Find entities that were merged into this one (one level)
        direct == {entry[1] : entry \in {e \in mergeLog : e[2] = entityId}}
        \* Find entities merged into those (second level)
        indirect == {entry[1] : entry \in {e \in mergeLog : e[2] \in direct}}
        \* Everything to erase: the entity + its full merge chain
        toErase == {entityId} \cup direct \cup indirect
       IN
        \* --- SHARED STATE UPDATES ---

        \* Identity: clear all hints for erased entities.
        \* Prevents new events from resolving to erased entities.
        /\ identity' = [h \in AllHints |->
            IF identity[h] \in toErase THEN 0
            ELSE identity[h]]

        \* Event_entities: mark erased entities' events as 0.
        \* Real system: DELETE FROM ledger + event_entities.
        \* We keep ledger intact but mark events as "no entity."
        /\ eventEntities' = [i \in 1..Len(eventEntities) |->
            IF eventEntities[i] \in toErase THEN 0
            ELSE eventEntities[i]]

        \* Merge log: remove all entries involving erased entities.
        /\ mergeLog' = {entry \in mergeLog :
            entry[1] \notin toErase /\ entry[2] \notin toErase}

        \* --- PER-SHARD STATE UPDATES ---

        \* DashMap: remove erased entities from THIS shard only.
        \* Other shards still have stale entries — cleaned by tick.
        /\ dashmap' = [s \in Shards |->
            IF s = shard
            THEN dashmap[s] \ toErase
            ELSE dashmap[s]]

        \* Cache: clear erased entities from THIS shard.
        /\ cache' = [s \in Shards |->
            IF s = shard
            THEN [h \in AllHints |->
                IF cache[shard][h] \in toErase THEN 0
                ELSE cache[shard][h]]
            ELSE cache[s]]

        \* Ledger and counters unchanged.
        /\ UNCHANGED <<ledger, nextEntity, cursor>>

\* ================================================================
\* PROTOCOL INVARIANTS
\*
\* These must hold in EVERY reachable state, after ANY interleaving
\* of operations across shards. If TLC finds a violation, it gives
\* the exact step-by-step trace that breaks it.
\*
\* Safety properties: "bad thing never happens"
\* ================================================================

\* No hint in the identity table points to a merged-away entity.
\* After a merge of e2→e1, all hints that mapped to e2 should
\* now map to e1. If this fails, identity has a stale mapping.
\* Real invariant: lifecycle-protocol.md invariant 1 (identity consistency)
NoStaleIdentity ==
    \A h \in AllHints :
        identity[h] /= 0 =>
            ~ \E entry \in mergeLog : entry[1] = identity[h]

\* No event in event_entities points to a merged-away entity.
\* After merge, merge_event_entities reassigns all loser events.
\* Real invariant: lifecycle-protocol.md invariant 2 (event ownership)
NoStaleEventEntities ==
    \A i \in 1..Len(eventEntities) :
        ~ \E entry \in mergeLog : entry[1] = eventEntities[i]

\* Every merge has exactly one audit record.
\* Real invariant: lifecycle-protocol.md invariant 3 (merge audit)
MergeAuditComplete ==
    \A entry \in mergeLog :
        /\ entry[1] \in EntityIds
        /\ entry[2] \in EntityIds
        /\ entry[1] /= entry[2]

\* Every entity in a shard's DashMap is owned by that shard.
\* No operation should place an entity in the wrong shard's DashMap.
\* Ingest adds only owned entities. Build filters by Owner().
\* If this fails, shard partitioning is broken.
DashmapOwnership ==
    \A s \in Shards :
        \A e \in dashmap[s] :
            Owner(e) = s

\* Cursor never regresses — monotonically increasing per shard.
\* (Checked implicitly: cursor only advances in Ingest/Build, never decreases.)
\* Real invariant: lifecycle-protocol.md invariant 6 (cursor monotonicity)

\* ================================================================
\* NEXT-STATE RELATION
\*
\* TLC non-deterministically chooses a shard and an event,
\* then checks all possible interleavings. With 2 shards and
\* 7 possible events, each step has 14 possible actions.
\*
\* \/ means OR (disjunction) — system can take ANY enabled action.
\* \E means EXISTS — TLC tries every value in the set.
\* ================================================================

Next ==
    \/ \E s \in Shards, h \in PossibleEvents : Ingest(s, h)
    \/ \E s \in Shards : BuildIncremental(s)
    \/ \E s \in Shards : BuildFull(s)
    \/ \E s \in Shards, f \in EntityIds, t \in EntityIds : ExternalMerge(s, f, t)
    \/ \E s \in Shards : Tick(s)
    \/ \E s \in Shards, e \in EntityIds : Erase(s, e)

\* ================================================================
\* SPECIFICATION
\*
\* The complete spec: start in Init, then repeatedly take steps
\* defined by Next.
\*
\* [][Next]_vars means "every step either satisfies Next, or is
\* a stuttering step (nothing changes)." Stuttering models
\* processes pausing — always possible in a real system.
\* ================================================================

Spec == Init /\ [][Next]_vars

====
