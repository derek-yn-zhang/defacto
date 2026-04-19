"""Verify result dataclasses construct and inspect correctly."""

from defacto.results import (
    BuildResult,
    BuildStatus,
    EraseResult,
    EventFailure,
    IngestResult,
    MergeResult,
    RegisterResult,
    TickResult,
    ValidationResult,
)


class TestBuildResult:
    def test_construction(self):
        r = BuildResult(
            mode="FULL",
            events_processed=1000,
            effects_produced=500,
            entities_created=100,
            entities_updated=50,
            merges_detected=2,
            late_arrivals=0,
            duration_ms=1500,
        )
        assert r.mode == "FULL"
        assert r.events_processed == 1000
        assert r.duration_ms == 1500

    def test_repr(self):
        r = BuildResult(
            mode="INCREMENTAL", events_processed=10, effects_produced=5,
            entities_created=1, entities_updated=2, merges_detected=0,
            late_arrivals=0, duration_ms=50,
        )
        assert "BuildResult" in repr(r)
        assert "INCREMENTAL" in repr(r)

    def test_frozen(self):
        r = BuildResult(
            mode="FULL", events_processed=0, effects_produced=0,
            entities_created=0, entities_updated=0, merges_detected=0,
            late_arrivals=0, duration_ms=0,
        )
        try:
            r.mode = "INCREMENTAL"  # type: ignore
            assert False, "Should not allow mutation"
        except AttributeError:
            pass  # frozen dataclass


class TestIngestResult:
    def test_defaults(self):
        r = IngestResult(events_ingested=10, events_failed=0, duplicates_skipped=2)
        assert r.failures == []
        assert r.duplicate_ids == []
        assert r.build_result is None

    def test_with_failures(self):
        failure = EventFailure(
            raw={"email": None},
            error="missing required field 'email'",
            source="app",
            handler="signup",
            field="email",
        )
        r = IngestResult(
            events_ingested=9,
            events_failed=1,
            duplicates_skipped=0,
            failures=[failure],
        )
        assert len(r.failures) == 1
        assert r.failures[0].field == "email"


class TestValidationResult:
    def test_valid(self):
        r = ValidationResult(valid=True)
        assert r.valid
        assert r.errors == []
        assert r.warnings == []

    def test_with_warnings(self):
        r = ValidationResult(
            valid=True,
            warnings=["State 'suspended' has no inbound transitions"],
        )
        assert r.valid
        assert len(r.warnings) == 1


class TestBuildResultFailures:
    def test_defaults_to_empty(self):
        r = BuildResult(
            mode="FULL", events_processed=10, effects_produced=5,
            entities_created=1, entities_updated=2, merges_detected=0,
            late_arrivals=0, duration_ms=50,
        )
        assert r.failures == []
        assert r.events_failed == 0

    def test_with_failures(self):
        failure = EventFailure(
            raw={}, error="interpret error", stage="interpretation",
            entity_id="ent_1", entity_type="customer",
        )
        r = BuildResult(
            mode="INCREMENTAL", events_processed=100, effects_produced=50,
            entities_created=0, entities_updated=10, merges_detected=0,
            late_arrivals=0, duration_ms=200,
            events_failed=1, failures=[failure],
        )
        assert r.events_failed == 1
        assert len(r.failures) == 1
        assert r.failures[0].stage == "interpretation"


class TestMergeResult:
    def test_construction(self):
        r = MergeResult(
            from_entity_id="loser",
            into_entity_id="winner",
            events_reassigned=5,
            entities_rebuilt=1,
            tombstones_produced=1,
        )
        assert r.from_entity_id == "loser"
        assert r.into_entity_id == "winner"
        assert r.events_reassigned == 5
        assert r.entities_rebuilt == 1
        assert r.tombstones_produced == 1
        assert r.failures == []

    def test_with_failures(self):
        failure = EventFailure(
            raw={}, error="rebuild error", stage="interpretation",
        )
        r = MergeResult(
            from_entity_id="a", into_entity_id="b",
            events_reassigned=0, entities_rebuilt=1,
            tombstones_produced=1, failures=[failure],
        )
        assert len(r.failures) == 1

    def test_frozen(self):
        r = MergeResult(
            from_entity_id="a", into_entity_id="b",
            events_reassigned=0, entities_rebuilt=0,
            tombstones_produced=0,
        )
        try:
            r.from_entity_id = "c"  # type: ignore
            assert False, "Should not allow mutation"
        except AttributeError:
            pass


class TestEraseResult:
    def test_construction(self):
        r = EraseResult(
            entity_id="ent_1",
            entities_erased=3,
            events_deleted=15,
            merge_log_cleaned=3,
        )
        assert r.entity_id == "ent_1"
        assert r.entities_erased == 3
        assert r.events_deleted == 15
        assert r.merge_log_cleaned == 3

    def test_frozen(self):
        r = EraseResult(
            entity_id="ent_1", entities_erased=1,
            events_deleted=5, merge_log_cleaned=1,
        )
        try:
            r.entity_id = "other"  # type: ignore
            assert False, "Should not allow mutation"
        except AttributeError:
            pass


class TestRegisterResult:
    def test_construction(self):
        r = RegisterResult(
            version="v2",
            changes={"customer": {"added_properties": ["churn_risk"]}},
            build_mode="FULL",
            warnings=[],
        )
        assert r.version == "v2"
        assert r.build_mode == "FULL"
