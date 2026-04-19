"""Verify definition objects construct, inspect, serialize, and validate correctly.

Definition objects (Entity, State, Property, etc.) are the core domain model.
Tests cover construction, from_dict/to_dict serialization, roundtrip consistency,
YAML loading, directory scanning, and validation.
"""

import os
import tempfile

import pytest
import yaml

from defacto.definitions import (
    Definitions,
    Effect,
    Entity,
    Handler,
    Identity,
    Property,
    Relationship,
    Schema,
    Source,
    State,
    TimeRule,
)
from defacto.errors import DefinitionError


class TestProperty:
    def test_basic(self):
        p = Property("number", default=0)
        assert p.type == "number"
        assert p.default == 0

    def test_with_constraints(self):
        p = Property("string", allowed=["a", "b", "c"], sensitive="pii", treatment="mask")
        assert p.allowed == ["a", "b", "c"]
        assert p.sensitive == "pii"
        assert p.treatment == "mask"

    def test_computed(self):
        p = Property("number", compute="entity.mrr * 12")
        assert p.compute == "entity.mrr * 12"

    def test_repr(self):
        p = Property("string")
        assert "Property" in repr(p)

    def test_from_dict_basic(self):
        p = Property.from_dict({"type": "number"})
        assert p.type == "number"
        assert p.default is None

    def test_from_dict_full(self):
        p = Property.from_dict({
            "type": "string",
            "default": "unknown",
            "sensitive": "pii",
            "treatment": "mask",
            "allowed": ["a", "b"],
            "min": 0,
            "max": 100,
            "compute": "entity.mrr * 12",
        })
        assert p.type == "string"
        assert p.default == "unknown"
        assert p.sensitive == "pii"
        assert p.treatment == "mask"
        assert p.allowed == ["a", "b"]
        assert p.min == 0
        assert p.max == 100
        assert p.compute == "entity.mrr * 12"

    def test_from_dict_missing_type(self):
        with pytest.raises(DefinitionError, match="missing required field 'type'"):
            Property.from_dict({"default": 0})

    def test_to_dict_minimal(self):
        d = Property("number").to_dict()
        assert d == {"type": "number"}

    def test_to_dict_full(self):
        p = Property("number", default=0, sensitive="pii", treatment="hash")
        d = p.to_dict()
        assert d == {"type": "number", "default": 0, "sensitive": "pii", "treatment": "hash"}

    def test_roundtrip(self):
        original = Property("string", default="x", allowed=["x", "y"], compute="entity.a")
        assert Property.from_dict(original.to_dict()) == original


class TestIdentity:
    def test_kwargs_constructor(self):
        i = Identity(
            email=Identity.Field(normalize="str::to_lowercase(value)", match="exact"),
            phone=Identity.Field(match="exact"),
        )
        assert "email" in i.fields
        assert "phone" in i.fields
        assert i.fields["email"].normalize == "str::to_lowercase(value)"
        assert i.fields["phone"].match == "exact"

    def test_field_defaults(self):
        f = Identity.Field()
        assert f.match == "exact"
        assert f.normalize is None

    def test_repr(self):
        i = Identity(email=Identity.Field())
        assert "Identity" in repr(i)
        assert "email" in repr(i)

    def test_equality(self):
        a = Identity(email=Identity.Field(match="exact"))
        b = Identity(email=Identity.Field(match="exact"))
        assert a == b

    def test_inequality(self):
        a = Identity(email=Identity.Field(match="exact"))
        b = Identity(email=Identity.Field(match="case_insensitive"))
        assert a != b

    def test_field_from_dict(self):
        f = Identity.Field.from_dict({"match": "case_insensitive", "normalize": "str::to_lowercase(value)"})
        assert f.match == "case_insensitive"
        assert f.normalize == "str::to_lowercase(value)"

    def test_field_from_dict_defaults(self):
        f = Identity.Field.from_dict({})
        assert f.match == "exact"
        assert f.normalize is None

    def test_field_to_dict(self):
        f = Identity.Field(match="exact", normalize="str::trim(value)")
        assert f.to_dict() == {"match": "exact", "normalize": "str::trim(value)"}

    def test_field_to_dict_no_normalize(self):
        f = Identity.Field(match="exact")
        assert f.to_dict() == {"match": "exact"}

    def test_from_dict(self):
        i = Identity.from_dict({
            "email": {"normalize": "str::to_lowercase(value)", "match": "exact"},
            "phone": {"match": "exact"},
        })
        assert "email" in i.fields
        assert "phone" in i.fields
        assert i.fields["email"].normalize == "str::to_lowercase(value)"

    def test_from_dict_empty_raises(self):
        with pytest.raises(DefinitionError, match="at least one identity field"):
            Identity.from_dict({})

    def test_to_dict(self):
        i = Identity(email=Identity.Field(normalize="str::to_lowercase(value)"), phone=Identity.Field())
        d = i.to_dict()
        assert d["email"]["normalize"] == "str::to_lowercase(value)"
        assert d["phone"] == {"match": "exact"}

    def test_roundtrip(self):
        original = Identity(
            email=Identity.Field(normalize="str::to_lowercase(value)", match="exact"),
            phone=Identity.Field(match="exact"),
        )
        assert Identity.from_dict(original.to_dict()) == original


class TestEffect:
    def test_create(self):
        e = Effect.create()
        assert e.kind == "create"

    def test_transition(self):
        e = Effect.transition("active")
        assert e.kind == "transition"
        assert e.to == "active"

    def test_set_from_event(self):
        e = Effect.set("mrr", from_="event.new_mrr")
        assert e.kind == "set"
        assert e.property == "mrr"
        assert e.from_ == "event.new_mrr"

    def test_set_with_condition(self):
        e = Effect.set("high_score", from_="event.score", condition="event.score > entity.high_score")
        assert e.condition == "event.score > entity.high_score"

    def test_set_literal(self):
        e = Effect.set("status", value="active")
        assert e.value == "active"

    def test_set_computed(self):
        e = Effect.set("annual_mrr", compute="entity.mrr * 12")
        assert e.compute == "entity.mrr * 12"

    def test_increment(self):
        e = Effect.increment("login_count", by=1)
        assert e.kind == "increment"
        assert e.by == 1

    def test_relate(self):
        e = Effect.relate("placed_by", target="customer", hints={"customer": ["email"]})
        assert e.kind == "relate"
        assert e.target == "customer"

    def test_equality(self):
        a = Effect.create()
        b = Effect.create()
        assert a == b

        c = Effect.transition("active")
        d = Effect.transition("churned")
        assert c != d

    # -- from_dict --

    def test_from_dict_create_string(self):
        e = Effect.from_dict("create")
        assert e.kind == "create"

    def test_from_dict_create_dict(self):
        """YAML 'create: {}' or 'create:' (null) should also work."""
        e = Effect.from_dict({"create": None})
        assert e.kind == "create"

    def test_from_dict_transition(self):
        e = Effect.from_dict({"transition": {"to": "active"}})
        assert e.kind == "transition"
        assert e.to == "active"

    def test_from_dict_set_from(self):
        e = Effect.from_dict({"set": {"property": "email", "from": "event.email"}})
        assert e.kind == "set"
        assert e.property == "email"
        assert e.from_ == "event.email"

    def test_from_dict_set_value(self):
        e = Effect.from_dict({"set": {"property": "status", "value": "active"}})
        assert e.value == "active"

    def test_from_dict_set_compute(self):
        e = Effect.from_dict({"set": {"property": "annual", "compute": "entity.mrr * 12"}})
        assert e.compute == "entity.mrr * 12"

    def test_from_dict_set_with_condition(self):
        e = Effect.from_dict({"set": {
            "property": "high_score",
            "from": "event.score",
            "condition": "event.score > entity.high_score",
        }})
        assert e.condition == "event.score > entity.high_score"

    def test_from_dict_increment(self):
        e = Effect.from_dict({"increment": {"property": "count", "by": 5}})
        assert e.kind == "increment"
        assert e.property == "count"
        assert e.by == 5

    def test_from_dict_increment_default_by(self):
        e = Effect.from_dict({"increment": {"property": "count"}})
        assert e.by == 1

    def test_from_dict_relate(self):
        e = Effect.from_dict({"relate": {
            "type": "placed_by",
            "target": "customer",
            "hints": {"customer": ["email"]},
        }})
        assert e.kind == "relate"
        assert e.target == "customer"
        assert e.hints == {"customer": ["email"]}

    def test_from_dict_unknown_string(self):
        with pytest.raises(DefinitionError, match="unknown string effect 'destroy'"):
            Effect.from_dict("destroy")

    def test_from_dict_unknown_kind(self):
        with pytest.raises(DefinitionError, match="unknown kind 'delete'"):
            Effect.from_dict({"delete": {}})

    def test_from_dict_missing_property(self):
        with pytest.raises(DefinitionError, match="missing required field 'property'"):
            Effect.from_dict({"set": {"from": "event.x"}})

    def test_from_dict_missing_transition_to(self):
        with pytest.raises(DefinitionError, match="missing required field 'to'"):
            Effect.from_dict({"transition": {}})

    # -- to_dict --

    def test_to_dict_create(self):
        assert Effect.create().to_dict() == "create"

    def test_to_dict_transition(self):
        assert Effect.transition("active").to_dict() == {"transition": {"to": "active"}}

    def test_to_dict_set(self):
        d = Effect.set("email", from_="event.email").to_dict()
        assert d == {"set": {"property": "email", "from": "event.email"}}

    def test_to_dict_increment(self):
        d = Effect.increment("count", by=3).to_dict()
        assert d == {"increment": {"property": "count", "by": 3}}

    def test_to_dict_relate(self):
        d = Effect.relate("placed_by", target="customer", hints={"customer": ["email"]}).to_dict()
        assert d == {"relate": {"type": "placed_by", "target": "customer", "hints": {"customer": ["email"]}}}

    # -- roundtrip --

    def test_roundtrip_create(self):
        assert Effect.from_dict(Effect.create().to_dict()) == Effect.create()

    def test_roundtrip_set_with_condition(self):
        original = Effect.set("x", from_="event.x", condition="event.x > 0")
        assert Effect.from_dict(original.to_dict()) == original

    def test_roundtrip_relate(self):
        original = Effect.relate("placed_by", target="customer", hints={"c": ["e"]})
        assert Effect.from_dict(original.to_dict()) == original


class TestHandler:
    def test_basic(self):
        h = Handler(effects=[Effect.create(), Effect.transition("active")])
        assert len(h.effects) == 2
        assert h.guard is None

    def test_with_guard(self):
        h = Handler(guard="event.plan != entity.plan", effects=[Effect.set("plan", from_="event.plan")])
        assert h.guard == "event.plan != entity.plan"

    def test_from_dict_basic(self):
        h = Handler.from_dict({"effects": ["create", {"transition": {"to": "active"}}]})
        assert len(h.effects) == 2
        assert h.guard is None

    def test_from_dict_with_guard(self):
        h = Handler.from_dict({
            "guard": "event.amount > 0",
            "effects": [{"set": {"property": "mrr", "from": "event.mrr"}}],
        })
        assert h.guard == "event.amount > 0"

    def test_from_dict_missing_effects(self):
        with pytest.raises(DefinitionError, match="missing required field 'effects'"):
            Handler.from_dict({"guard": "true"})

    def test_from_dict_empty_effects(self):
        with pytest.raises(DefinitionError, match="must not be empty"):
            Handler.from_dict({"effects": []})

    def test_to_dict(self):
        h = Handler(guard="event.x > 0", effects=[Effect.create()])
        d = h.to_dict()
        assert d == {"guard": "event.x > 0", "effects": ["create"]}

    def test_to_dict_no_guard(self):
        h = Handler(effects=[Effect.transition("active")])
        d = h.to_dict()
        assert d == {"effects": [{"transition": {"to": "active"}}]}

    def test_roundtrip(self):
        original = Handler(guard="event.x > 0", effects=[
            Effect.create(),
            Effect.set("email", from_="event.email"),
        ])
        assert Handler.from_dict(original.to_dict()) == original


class TestTimeRule:
    def test_inactivity(self):
        tr = TimeRule.inactivity("90d", effects=[Effect.transition("churned")])
        assert tr.type == "inactivity"
        assert tr.threshold == "90d"
        assert len(tr.effects) == 1

    def test_expiration(self):
        tr = TimeRule.expiration("365d", effects=[Effect.transition("expired")])
        assert tr.type == "expiration"

    def test_state_duration(self):
        tr = TimeRule.state_duration("14d", effects=[Effect.transition("delivered")])
        assert tr.type == "state_duration"

    def test_from_dict(self):
        tr = TimeRule.from_dict({
            "type": "inactivity",
            "threshold": "90d",
            "effects": [{"transition": {"to": "churned"}}],
        })
        assert tr.type == "inactivity"
        assert tr.threshold == "90d"
        assert len(tr.effects) == 1

    def test_from_dict_missing_type(self):
        with pytest.raises(DefinitionError, match="missing required field 'type'"):
            TimeRule.from_dict({"threshold": "90d"})

    def test_from_dict_missing_threshold(self):
        with pytest.raises(DefinitionError, match="missing required field 'threshold'"):
            TimeRule.from_dict({"type": "inactivity"})

    def test_from_dict_unknown_type(self):
        with pytest.raises(DefinitionError, match="unknown type 'timeout'"):
            TimeRule.from_dict({"type": "timeout", "threshold": "30d"})

    def test_to_dict(self):
        tr = TimeRule.inactivity("90d", effects=[Effect.transition("churned")])
        d = tr.to_dict()
        assert d == {
            "type": "inactivity",
            "threshold": "90d",
            "effects": [{"transition": {"to": "churned"}}],
        }

    def test_roundtrip(self):
        original = TimeRule.expiration("365d", effects=[Effect.transition("expired")])
        assert TimeRule.from_dict(original.to_dict()) == original


class TestState:
    def test_empty(self):
        s = State()
        assert s.when == {}
        assert s.after == []

    def test_with_handlers(self):
        s = State(when={
            "signup": Handler(effects=[Effect.create()]),
            "upgrade": Handler(
                guard="event.plan != entity.plan",
                effects=[Effect.set("plan", from_="event.plan")],
            ),
        })
        assert "signup" in s.when
        assert "upgrade" in s.when
        assert s.when["upgrade"].guard is not None

    def test_with_time_rules(self):
        s = State(after=[TimeRule.inactivity("30d", effects=[Effect.transition("churned")])])
        assert len(s.after) == 1

    def test_from_dict_full(self):
        s = State.from_dict({
            "when": {
                "signup": {"effects": ["create", {"transition": {"to": "active"}}]},
            },
            "after": [
                {"type": "inactivity", "threshold": "90d", "effects": [{"transition": {"to": "churned"}}]},
            ],
        })
        assert "signup" in s.when
        assert len(s.after) == 1

    def test_from_dict_empty(self):
        """Terminal state: empty dict."""
        s = State.from_dict({})
        assert s.when == {}
        assert s.after == []

    def test_from_dict_none(self):
        """Terminal state: YAML bare key produces None."""
        s = State.from_dict(None)
        assert s.when == {}
        assert s.after == []

    def test_to_dict_terminal(self):
        assert State().to_dict() == {}

    def test_to_dict_with_handlers(self):
        s = State(when={"signup": Handler(effects=[Effect.create()])})
        d = s.to_dict()
        assert d == {"when": {"signup": {"effects": ["create"]}}}

    def test_roundtrip(self):
        original = State(
            when={"upgrade": Handler(guard="event.x > 0", effects=[Effect.set("x", from_="event.x")])},
            after=[TimeRule.inactivity("30d", effects=[Effect.transition("churned")])],
        )
        assert State.from_dict(original.to_dict()) == original


class TestRelationship:
    def test_basic(self):
        r = Relationship(type="placed_order", target="order", cardinality="has_many")
        assert r.type == "placed_order"
        assert r.target == "order"
        assert r.cardinality == "has_many"
        assert r.properties == {}

    def test_with_properties(self):
        r = Relationship(
            type="placed_order", target="order", cardinality="has_many",
            properties={"total": Property("number")},
        )
        assert "total" in r.properties

    def test_from_dict_basic(self):
        r = Relationship.from_dict({"type": "placed_order", "target": "order", "cardinality": "has_many"})
        assert r.type == "placed_order"
        assert r.target == "order"
        assert r.cardinality == "has_many"
        assert r.properties == {}

    def test_from_dict_with_properties(self):
        r = Relationship.from_dict({
            "type": "placed_order", "target": "order", "cardinality": "has_many",
            "properties": {"total": {"type": "number"}},
        })
        assert r.properties["total"].type == "number"

    def test_from_dict_missing_field(self):
        with pytest.raises(DefinitionError, match="missing required field 'target'"):
            Relationship.from_dict({"type": "placed_order", "cardinality": "has_many"})

    def test_to_dict(self):
        r = Relationship(type="placed_order", target="order", cardinality="has_many")
        assert r.to_dict() == {"type": "placed_order", "target": "order", "cardinality": "has_many"}

    def test_roundtrip(self):
        original = Relationship(
            type="placed_order", target="order", cardinality="has_many",
            properties={"total": Property("number")},
        )
        assert Relationship.from_dict(original.to_dict()) == original


class TestEntity:
    def _make_entity(self) -> Entity:
        return Entity(
            name="customer",
            starts="lead",
            identity=Identity(email=Identity.Field(match="exact")),
            properties={"mrr": Property("number", default=0)},
            states={
                "lead": State(when={"signup": Handler(effects=[Effect.create(), Effect.transition("active")])}),
                "active": State(),
            },
        )

    def test_construction(self):
        e = self._make_entity()
        assert e.name == "customer"
        assert e.starts == "lead"
        assert "mrr" in e.properties
        assert "lead" in e.states
        assert "active" in e.states

    def test_repr(self):
        e = self._make_entity()
        assert "Entity" in repr(e)

    def test_from_dict_minimal(self):
        e = Entity.from_dict({"customer": {
            "starts": "lead",
            "identity": {"email": {"match": "exact"}},
            "states": {"lead": None, "active": {}},
        }})
        assert e.name == "customer"
        assert e.starts == "lead"
        assert "email" in e.identity.fields
        assert "lead" in e.states
        assert "active" in e.states
        assert e.properties == {}
        assert e.relationships == []
        assert e.always == {}

    def test_from_dict_full(self):
        e = Entity.from_dict({"customer": {
            "starts": "lead",
            "identity": {
                "email": {"normalize": "str::to_lowercase(value)", "match": "exact"},
                "phone": {"match": "exact"},
            },
            "properties": {
                "mrr": {"type": "number", "default": 0},
                "plan": {"type": "string"},
            },
            "states": {
                "lead": {
                    "when": {"signup": {"effects": ["create", {"transition": {"to": "active"}}]}},
                },
                "active": {
                    "when": {"upgrade": {
                        "guard": "event.plan != entity.plan",
                        "effects": [{"set": {"property": "plan", "from": "event.new_plan"}}],
                    }},
                    "after": [{"type": "inactivity", "threshold": "90d", "effects": [{"transition": {"to": "churned"}}]}],
                },
                "churned": {},
            },
            "relationships": [
                {"type": "placed_order", "target": "order", "cardinality": "has_many"},
            ],
            "always": {
                "profile_updated": {"effects": [{"set": {"property": "mrr", "from": "event.mrr"}}]},
            },
        }})
        assert e.name == "customer"
        assert len(e.identity.fields) == 2
        assert e.properties["mrr"].default == 0
        assert len(e.states) == 3
        assert e.states["active"].when["upgrade"].guard == "event.plan != entity.plan"
        assert len(e.states["active"].after) == 1
        assert len(e.relationships) == 1
        assert "profile_updated" in e.always

    def test_from_dict_missing_starts(self):
        with pytest.raises(DefinitionError, match="missing required field 'starts'"):
            Entity.from_dict({"customer": {
                "identity": {"email": {"match": "exact"}},
                "states": {"lead": {}},
            }})

    def test_from_dict_not_single_key(self):
        with pytest.raises(DefinitionError, match="single-key dict"):
            Entity.from_dict({"a": {}, "b": {}})

    def test_to_dict(self):
        e = self._make_entity()
        d = e.to_dict()
        assert "customer" in d
        body = d["customer"]
        assert body["starts"] == "lead"
        assert "identity" in body
        assert "states" in body

    def test_roundtrip(self):
        original = self._make_entity()
        reconstructed = Entity.from_dict(original.to_dict())
        assert reconstructed == original


class TestSource:
    def test_construction(self):
        s = Source(
            name="app",
            event_type="type",
            timestamp="created_at",
            events={
                "signup": Source.Event(
                    raw_type="user.created",
                    mappings={"email": {"from": "user_email"}},
                    hints={"customer": ["email"]},
                ),
            },
        )
        assert s.name == "app"
        assert "signup" in s.events
        assert s.events["signup"].raw_type == "user.created"

    def test_repr(self):
        s = Source(name="app", event_type="type", timestamp="ts", events={})
        assert "Source" in repr(s)

    def test_from_dict(self):
        s = Source.from_dict({"app": {
            "event_type": "type",
            "timestamp": "created_at",
            "event_id": ["id"],
            "events": {
                "signup": {
                    "raw_type": "user.created",
                    "mappings": {"email": {"from": "user_email"}},
                    "hints": {"customer": ["email"]},
                },
            },
        }})
        assert s.name == "app"
        assert s.event_type == "type"
        assert s.timestamp == "created_at"
        assert s.event_id == ["id"]
        assert s.events["signup"].raw_type == "user.created"

    def test_from_dict_minimal(self):
        s = Source.from_dict({"app": {
            "event_type": "type",
            "timestamp": "ts",
            "events": {"signup": {"mappings": {}, "hints": {}}},
        }})
        assert s.name == "app"
        assert s.events["signup"].raw_type is None

    def test_from_dict_missing_field(self):
        with pytest.raises(DefinitionError, match="missing required field 'timestamp'"):
            Source.from_dict({"app": {"event_type": "type", "events": {}}})

    def test_to_dict(self):
        s = Source(
            name="app", event_type="type", timestamp="ts",
            events={"signup": Source.Event(
                raw_type="user.created",
                mappings={"email": {"from": "user_email"}},
                hints={"customer": ["email"]},
            )},
        )
        d = s.to_dict()
        assert "app" in d
        assert d["app"]["events"]["signup"]["raw_type"] == "user.created"

    def test_roundtrip(self):
        original = Source(
            name="app", event_type="type", timestamp="ts", event_id=["id"],
            events={"signup": Source.Event(
                raw_type="user.created",
                mappings={"email": {"from": "user_email"}},
                hints={"customer": ["email"]},
            )},
        )
        assert Source.from_dict(original.to_dict()) == original


class TestSchema:
    def test_construction(self):
        s = Schema("signup", fields={
            "email": Schema.Field("string", required=True),
            "plan": Schema.Field("string", allowed=["free", "pro"]),
        })
        assert s.name == "signup"
        assert s.fields["email"].required is True
        assert s.fields["plan"].allowed == ["free", "pro"]

    def test_field_defaults(self):
        f = Schema.Field("number")
        assert f.required is False
        assert f.allowed is None
        assert f.min is None

    def test_repr(self):
        s = Schema("test", fields={})
        assert "Schema" in repr(s)

    def test_field_from_dict_basic(self):
        f = Schema.Field.from_dict({"type": "string"})
        assert f.type == "string"
        assert f.required is False

    def test_field_from_dict_full(self):
        f = Schema.Field.from_dict({
            "type": "number",
            "required": True,
            "allowed": [1, 2, 3],
            "min": 0,
            "max": 100,
            "min_length": 3,
            "regex": "^[a-z]+$",
        })
        assert f.required is True
        assert f.allowed == [1, 2, 3]
        assert f.min == 0
        assert f.min_length == 3
        assert f.regex == "^[a-z]+$"

    def test_field_from_dict_missing_type(self):
        with pytest.raises(DefinitionError, match="missing required field 'type'"):
            Schema.Field.from_dict({"required": True})

    def test_field_to_dict_minimal(self):
        assert Schema.Field("string").to_dict() == {"type": "string"}

    def test_field_to_dict_full(self):
        f = Schema.Field("number", required=True, min=0, max=100)
        d = f.to_dict()
        assert d == {"type": "number", "required": True, "min": 0, "max": 100}

    def test_field_roundtrip(self):
        original = Schema.Field("string", required=True, allowed=["a", "b"], regex="^[a-z]$")
        assert Schema.Field.from_dict(original.to_dict()) == original

    def test_from_dict(self):
        s = Schema.from_dict({"signup": {
            "fields": {
                "email": {"type": "string", "required": True},
                "plan": {"type": "string", "allowed": ["free", "pro"]},
            },
        }})
        assert s.name == "signup"
        assert s.fields["email"].required is True

    def test_from_dict_missing_fields(self):
        with pytest.raises(DefinitionError, match="missing required field 'fields'"):
            Schema.from_dict({"signup": {}})

    def test_to_dict(self):
        s = Schema("signup", fields={"email": Schema.Field("string", required=True)})
        d = s.to_dict()
        assert d == {"signup": {"fields": {"email": {"type": "string", "required": True}}}}

    def test_roundtrip(self):
        original = Schema("signup", fields={
            "email": Schema.Field("string", required=True),
            "plan": Schema.Field("string", allowed=["free", "pro"]),
        })
        assert Schema.from_dict(original.to_dict()) == original


class TestDefinitions:
    def test_empty(self):
        d = Definitions()
        assert d.entities == {}
        assert d.sources == {}
        assert d.schemas == {}

    def test_from_dict_empty(self):
        d = Definitions.from_dict({})
        assert d.entities == {}
        assert d.sources == {}
        assert d.schemas == {}

    def test_from_dict_full(self):
        d = Definitions.from_dict({
            "entities": {
                "customer": {
                    "starts": "lead",
                    "identity": {"email": {"match": "exact"}},
                    "states": {"lead": None},
                },
            },
            "sources": {
                "app": {
                    "event_type": "type",
                    "timestamp": "ts",
                    "events": {"signup": {"mappings": {}, "hints": {}}},
                },
            },
            "schemas": {
                "signup": {"fields": {"email": {"type": "string"}}},
            },
        })
        assert "customer" in d.entities
        assert "app" in d.sources
        assert "signup" in d.schemas

    def test_to_dict(self):
        defs = Definitions(
            entities={"customer": Entity(
                name="customer", starts="lead",
                identity=Identity(email=Identity.Field()),
                states={"lead": State()},
            )},
        )
        d = defs.to_dict()
        assert "entities" in d
        assert "customer" in d["entities"]
        assert d["entities"]["customer"]["starts"] == "lead"

    def test_roundtrip(self):
        original = Definitions(
            entities={"customer": Entity(
                name="customer", starts="lead",
                identity=Identity(email=Identity.Field()),
                properties={"mrr": Property("number", default=0)},
                states={"lead": State(), "active": State()},
            )},
            sources={"app": Source(
                name="app", event_type="type", timestamp="ts",
                events={"signup": Source.Event(mappings={}, hints={})},
            )},
            schemas={"signup": Schema("signup", fields={
                "email": Schema.Field("string", required=True),
            })},
        )
        reconstructed = Definitions.from_dict(original.to_dict())
        assert reconstructed.entities["customer"] == original.entities["customer"]
        assert reconstructed.sources["app"] == original.sources["app"]
        assert reconstructed.schemas["signup"] == original.schemas["signup"]


# ---------------------------------------------------------------------------
# YAML serialization tests
# ---------------------------------------------------------------------------


class TestYaml:
    """Test from_yaml / to_yaml roundtrips through actual YAML files."""

    def test_entity_yaml_roundtrip(self):
        original = Entity(
            name="customer", starts="lead",
            identity=Identity(email=Identity.Field(normalize="str::to_lowercase(value)")),
            properties={"mrr": Property("number", default=0)},
            states={"lead": State(when={"signup": Handler(effects=[Effect.create()])}), "active": State()},
        )
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(original.to_yaml())
            path = f.name
        try:
            loaded = Entity.from_yaml(path)
            assert loaded == original
        finally:
            os.unlink(path)

    def test_source_yaml_roundtrip(self):
        original = Source(
            name="app", event_type="type", timestamp="ts",
            events={"signup": Source.Event(mappings={}, hints={"customer": ["email"]})},
        )
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(original.to_yaml())
            path = f.name
        try:
            loaded = Source.from_yaml(path)
            assert loaded == original
        finally:
            os.unlink(path)

    def test_schema_yaml_roundtrip(self):
        original = Schema("signup", fields={"email": Schema.Field("string", required=True)})
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(original.to_yaml())
            path = f.name
        try:
            loaded = Schema.from_yaml(path)
            assert loaded == original
        finally:
            os.unlink(path)

    def test_entity_from_yaml_file_not_found(self):
        with pytest.raises(DefinitionError, match="file not found"):
            Entity.from_yaml("/nonexistent/path.yaml")

    def test_source_from_yaml_file_not_found(self):
        with pytest.raises(DefinitionError, match="file not found"):
            Source.from_yaml("/nonexistent/path.yaml")

    def test_schema_from_yaml_file_not_found(self):
        with pytest.raises(DefinitionError, match="file not found"):
            Schema.from_yaml("/nonexistent/path.yaml")

    def test_entity_from_yaml_invalid_yaml(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("{ invalid yaml: [unclosed")
            path = f.name
        try:
            with pytest.raises(DefinitionError, match="Invalid YAML"):
                Entity.from_yaml(path)
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# Directory loading tests
# ---------------------------------------------------------------------------


class TestFromDirectory:
    """Test Definitions.from_directory scanning YAML files from disk."""

    def _write_yaml(self, path: str, data: dict) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            yaml.dump(data, f)

    def test_basic_load(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._write_yaml(os.path.join(tmp, "entities", "customer.yaml"), {
                "customer": {
                    "starts": "lead",
                    "identity": {"email": {"match": "exact"}},
                    "states": {"lead": {}},
                },
            })
            self._write_yaml(os.path.join(tmp, "sources", "app.yaml"), {
                "app": {
                    "event_type": "type", "timestamp": "ts",
                    "events": {"signup": {"mappings": {}, "hints": {}}},
                },
            })
            self._write_yaml(os.path.join(tmp, "events", "signup.yaml"), {
                "signup": {"fields": {"email": {"type": "string"}}},
            })

            defs = Definitions.from_directory(tmp)
            assert "customer" in defs.entities
            assert "app" in defs.sources
            assert "signup" in defs.schemas

    def test_missing_dirs_ok(self):
        with tempfile.TemporaryDirectory() as tmp:
            # Only entities, no sources or events
            self._write_yaml(os.path.join(tmp, "entities", "customer.yaml"), {
                "customer": {
                    "starts": "lead",
                    "identity": {"email": {"match": "exact"}},
                    "states": {"lead": {}},
                },
            })

            defs = Definitions.from_directory(tmp)
            assert "customer" in defs.entities
            assert defs.sources == {}
            assert defs.schemas == {}

    def test_empty_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            defs = Definitions.from_directory(tmp)
            assert defs.entities == {}
            assert defs.sources == {}
            assert defs.schemas == {}

    def test_multiple_definitions_per_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            # Two schemas in one file
            self._write_yaml(os.path.join(tmp, "events", "all.yaml"), {
                "signup": {"fields": {"email": {"type": "string"}}},
                "upgrade": {"fields": {"plan": {"type": "string"}}},
            })

            defs = Definitions.from_directory(tmp)
            assert "signup" in defs.schemas
            assert "upgrade" in defs.schemas

    def test_duplicate_name_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._write_yaml(os.path.join(tmp, "entities", "a.yaml"), {
                "customer": {
                    "starts": "lead",
                    "identity": {"email": {"match": "exact"}},
                    "states": {"lead": {}},
                },
            })
            self._write_yaml(os.path.join(tmp, "entities", "b.yaml"), {
                "customer": {
                    "starts": "active",
                    "identity": {"phone": {"match": "exact"}},
                    "states": {"active": {}},
                },
            })

            with pytest.raises(DefinitionError, match="Duplicate entity 'customer'"):
                Definitions.from_directory(tmp)

    def test_to_yaml_roundtrip(self):
        """Write definitions to directory, read them back."""
        original = Definitions(
            entities={"customer": Entity(
                name="customer", starts="lead",
                identity=Identity(email=Identity.Field()),
                states={"lead": State()},
            )},
            schemas={"signup": Schema("signup", fields={
                "email": Schema.Field("string", required=True),
            })},
        )

        with tempfile.TemporaryDirectory() as tmp:
            original.to_yaml(tmp)
            loaded = Definitions.from_directory(tmp)
            assert loaded.entities["customer"] == original.entities["customer"]
            assert loaded.schemas["signup"] == original.schemas["signup"]


# ---------------------------------------------------------------------------
# Structural validation tests
# ---------------------------------------------------------------------------


class TestEntityValidation:
    def _entity(self, **overrides) -> Entity:
        """Build a valid entity, then override specific fields for testing."""
        defaults = dict(
            name="customer", starts="lead",
            identity=Identity(email=Identity.Field()),
            properties={"mrr": Property("number", default=0), "plan": Property("string")},
            states={
                "lead": State(when={"signup": Handler(effects=[
                    Effect.create(), Effect.set("mrr", from_="event.mrr"), Effect.transition("active"),
                ])}),
                "active": State(),
            },
        )
        defaults.update(overrides)
        return Entity(**defaults)

    def test_valid_entity(self):
        assert self._entity().validate() == []

    def test_starts_not_in_states(self):
        e = self._entity(starts="ghost")
        errors = e.validate()
        assert any("starts state 'ghost'" in err for err in errors)

    def test_no_states(self):
        e = self._entity(states={})
        errors = e.validate()
        assert any("at least one state" in err for err in errors)

    def test_invalid_property_type(self):
        e = self._entity(properties={"x": Property("invalid_type")})
        errors = e.validate()
        assert any("invalid type 'invalid_type'" in err for err in errors)

    def test_property_name_collides_with_system_column(self):
        e = self._entity(properties={"customer_id": Property("string")})
        errors = e.validate()
        assert any("collides with system column" in err for err in errors)

    def test_transition_target_not_in_states(self):
        e = self._entity(states={
            "lead": State(when={"signup": Handler(effects=[Effect.transition("nowhere")])}),
        })
        errors = e.validate()
        assert any("transition target 'nowhere'" in err for err in errors)

    def test_set_references_undeclared_property(self):
        e = self._entity(states={
            "lead": State(when={"signup": Handler(effects=[Effect.set("ghost", from_="event.x")])}),
            "active": State(),
        })
        errors = e.validate()
        assert any("undeclared property 'ghost'" in err for err in errors)

    def test_set_no_value_source(self):
        e = self._entity(states={
            "lead": State(when={"signup": Handler(effects=[Effect(kind="set", property="mrr")])}),
            "active": State(),
        })
        errors = e.validate()
        assert any("no value source" in err for err in errors)

    def test_set_multiple_value_sources(self):
        e = self._entity(states={
            "lead": State(when={"signup": Handler(effects=[
                Effect(kind="set", property="mrr", from_="event.x", value=42),
            ])}),
            "active": State(),
        })
        errors = e.validate()
        assert any("multiple value sources" in err for err in errors)

    def test_increment_references_undeclared_property(self):
        e = self._entity(states={
            "lead": State(when={"signup": Handler(effects=[Effect.increment("ghost")])}),
            "active": State(),
        })
        errors = e.validate()
        assert any("undeclared property 'ghost'" in err for err in errors)

    def test_invalid_cardinality(self):
        e = self._entity(relationships=[
            Relationship(type="foo", target="bar", cardinality="invalid"),
        ])
        errors = e.validate()
        assert any("invalid cardinality 'invalid'" in err for err in errors)

    def test_always_handler_validated(self):
        e = self._entity(always={
            "update": Handler(effects=[Effect.set("ghost", from_="event.x")]),
        })
        errors = e.validate()
        assert any("undeclared property 'ghost'" in err for err in errors)

    def test_collects_multiple_errors(self):
        """validate() should collect ALL errors, not stop at the first."""
        e = self._entity(
            starts="ghost",
            properties={"x": Property("bad_type")},
        )
        errors = e.validate()
        assert len(errors) >= 2

    # -- Type matching checks --

    def test_default_value_wrong_type(self):
        e = self._entity(properties={"mrr": Property("number", default="not_a_number")})
        errors = e.validate()
        assert any("default value 'not_a_number' doesn't match type 'number'" in err for err in errors)

    def test_default_value_correct_type(self):
        e = self._entity(properties={"mrr": Property("number", default=42)})
        errors = e.validate()
        assert not any("default" in err for err in errors)

    def test_default_bool_not_accepted_as_integer(self):
        """Python's bool is a subclass of int — we should catch this."""
        e = self._entity(properties={"count": Property("integer", default=True)})
        errors = e.validate()
        assert any("is boolean" in err for err in errors)

    def test_allowed_values_wrong_type(self):
        e = self._entity(properties={"plan": Property("string", allowed=[1, 2, 3])})
        errors = e.validate()
        assert any("allowed value 1 doesn't match type 'string'" in err for err in errors)

    def test_allowed_values_correct_type(self):
        e = self._entity(properties={"plan": Property("string", allowed=["free", "pro"])})
        errors = e.validate()
        assert not any("allowed" in err for err in errors)

    def test_increment_on_non_numeric_property(self):
        e = self._entity(states={
            "lead": State(when={"signup": Handler(effects=[Effect.create(), Effect.increment("plan")])}),
            "active": State(),
        })
        errors = e.validate()
        assert any("not numeric" in err for err in errors)

    def test_increment_on_numeric_property_ok(self):
        e = self._entity(states={
            "lead": State(when={"signup": Handler(effects=[Effect.create(), Effect.increment("mrr")])}),
            "active": State(),
        })
        errors = e.validate()
        assert not any("not numeric" in err for err in errors)

    def test_set_literal_value_wrong_type(self):
        e = self._entity(states={
            "lead": State(when={"signup": Handler(effects=[
                Effect.create(), Effect.set("mrr", value="not_a_number"),
            ])}),
            "active": State(),
        })
        errors = e.validate()
        assert any("doesn't match property 'mrr' type 'number'" in err for err in errors)

    # -- Lifecycle checks --

    def test_starts_state_no_create_effect(self):
        e = self._entity(states={
            "lead": State(when={"signup": Handler(effects=[Effect.transition("active")])}),
            "active": State(),
        })
        errors = e.validate()
        assert any("no handler with a 'create' effect" in err for err in errors)

    def test_starts_state_with_create_effect_ok(self):
        """Default _entity has create in lead — should be fine."""
        errors = self._entity().validate()
        assert not any("create" in err for err in errors)

    def test_unreachable_state(self):
        e = self._entity(states={
            "lead": State(when={"signup": Handler(effects=[Effect.create(), Effect.transition("active")])}),
            "active": State(),
            "orphan": State(),  # no transitions lead here
        })
        errors = e.validate()
        assert any("state 'orphan' is unreachable" in err for err in errors)

    def test_all_states_reachable(self):
        errors = self._entity().validate()
        assert not any("unreachable" in err for err in errors)


class TestSourceValidation:
    def test_valid_source(self):
        s = Source(
            name="app", event_type="type", timestamp="ts",
            events={"signup": Source.Event(
                mappings={"email": {"from": "user_email"}},
                hints={"customer": ["email"]},
            )},
        )
        assert s.validate() == []

    def test_empty_event_type(self):
        s = Source(name="app", event_type="", timestamp="ts", events={})
        errors = s.validate()
        assert any("non-empty string" in err for err in errors)

    def test_no_hints(self):
        s = Source(
            name="app", event_type="type", timestamp="ts",
            events={"signup": Source.Event(mappings={}, hints={})},
        )
        errors = s.validate()
        assert any("at least one identity hint" in err for err in errors)

    def test_from_and_compute_mutually_exclusive(self):
        s = Source(
            name="app", event_type="type", timestamp="ts",
            events={"signup": Source.Event(
                mappings={"email": {"from": "user_email", "compute": "str::to_lowercase(event.email)"}},
                hints={"customer": ["email"]},
            )},
        )
        errors = s.validate()
        assert any("mutually exclusive" in err for err in errors)


class TestSchemaValidation:
    def test_valid_schema(self):
        s = Schema("signup", fields={
            "email": Schema.Field("string", required=True),
            "amount": Schema.Field("number", min=0),
        })
        assert s.validate() == []

    def test_invalid_field_type(self):
        s = Schema("signup", fields={"x": Schema.Field("invalid")})
        errors = s.validate()
        assert any("invalid type 'invalid'" in err for err in errors)

    def test_numeric_constraint_on_string(self):
        s = Schema("signup", fields={"name": Schema.Field("string", min=0)})
        errors = s.validate()
        assert any("'min' constraint only applies to numeric" in err for err in errors)

    def test_string_constraint_on_number(self):
        s = Schema("signup", fields={"count": Schema.Field("number", min_length=3)})
        errors = s.validate()
        assert any("'min_length' constraint only applies to string" in err for err in errors)

    def test_regex_on_non_string(self):
        s = Schema("signup", fields={"count": Schema.Field("integer", regex="^\\d+$")})
        errors = s.validate()
        assert any("'regex' constraint only applies to string" in err for err in errors)


# ---------------------------------------------------------------------------
# Cross-definition validation tests
# ---------------------------------------------------------------------------


class TestDefinitionsValidation:
    """Test Definitions.validate() for cross-definition consistency."""

    def _customer_entity(self, **overrides) -> Entity:
        defaults = dict(
            name="customer", starts="lead",
            identity=Identity(email=Identity.Field()),
            properties={"mrr": Property("number", default=0)},
            states={
                "lead": State(when={"signup": Handler(effects=[Effect.create(), Effect.transition("active")])}),
                "active": State(),
            },
        )
        defaults.update(overrides)
        return Entity(**defaults)

    def test_valid_definitions(self):
        defs = Definitions(
            entities={
                "customer": self._customer_entity(
                    relationships=[Relationship(type="placed_order", target="order", cardinality="has_many")],
                ),
                "order": Entity(
                    name="order", starts="placed",
                    identity=Identity(order_id=Identity.Field()),
                    states={"placed": State(when={"order_placed": Handler(effects=[Effect.create()])})},
                    relationships=[Relationship(type="placed_by", target="customer", cardinality="belongs_to")],
                ),
            },
            sources={"app": Source(
                name="app", event_type="type", timestamp="ts",
                events={"signup": Source.Event(
                    mappings={"email": {"from": "user_email"}},
                    hints={"customer": ["email"]},
                )},
            )},
            schemas={
                "signup": Schema("signup", fields={"email": Schema.Field("string")}),
                "order_placed": Schema("order_placed", fields={"total": Schema.Field("number")}),
            },
        )
        result = defs.validate()
        assert result.valid, f"Expected valid but got errors: {result.errors}"

    def test_hint_references_nonexistent_entity(self):
        defs = Definitions(
            entities={"customer": self._customer_entity()},
            sources={"app": Source(
                name="app", event_type="type", timestamp="ts",
                events={"signup": Source.Event(
                    mappings={}, hints={"ghost": ["email"]},
                )},
            )},
        )
        result = defs.validate()
        assert not result.valid
        assert any("entity 'ghost' which is not defined" in err for err in result.errors)

    def test_hint_references_nonexistent_identity_field(self):
        defs = Definitions(
            entities={"customer": self._customer_entity()},
            sources={"app": Source(
                name="app", event_type="type", timestamp="ts",
                events={"signup": Source.Event(
                    mappings={}, hints={"customer": ["phone"]},
                )},
            )},
        )
        result = defs.validate()
        assert not result.valid
        assert any("'phone' is not an identity field" in err for err in result.errors)

    def test_relationship_target_not_defined(self):
        defs = Definitions(
            entities={"customer": self._customer_entity(
                relationships=[Relationship(type="placed_order", target="ghost", cardinality="has_many")],
            )},
        )
        result = defs.validate()
        assert not result.valid
        assert any("target entity 'ghost' is not defined" in err for err in result.errors)

    def test_relationship_no_inverse(self):
        defs = Definitions(
            entities={
                "customer": self._customer_entity(
                    relationships=[Relationship(type="placed_order", target="order", cardinality="has_many")],
                ),
                "order": Entity(
                    name="order", starts="placed",
                    identity=Identity(order_id=Identity.Field()),
                    states={"placed": State(when={"order_placed": Handler(effects=[Effect.create()])})},
                    # No inverse relationship back to customer
                ),
            },
        )
        result = defs.validate()
        assert not result.valid
        assert any("has no inverse" in err for err in result.errors)

    def test_relationship_cardinality_mismatch(self):
        defs = Definitions(
            entities={
                "customer": self._customer_entity(
                    relationships=[Relationship(type="placed_order", target="order", cardinality="has_many")],
                ),
                "order": Entity(
                    name="order", starts="placed",
                    identity=Identity(order_id=Identity.Field()),
                    states={"placed": State(when={"order_placed": Handler(effects=[Effect.create()])})},
                    # has_many ↔ has_many is not a valid pairing
                    relationships=[Relationship(type="placed_by", target="customer", cardinality="has_many")],
                ),
            },
        )
        result = defs.validate()
        assert not result.valid
        assert any("cardinality mismatch" in err for err in result.errors)

    def test_relationship_type_contains_colon(self):
        defs = Definitions(
            entities={"customer": self._customer_entity(
                relationships=[Relationship(type="placed:order", target="customer", cardinality="has_many")],
            )},
        )
        result = defs.validate()
        assert not result.valid
        assert any("reserved as a key delimiter" in err for err in result.errors)

    def test_aggregates_structural_errors(self):
        """Definitions.validate() should include errors from sub-type validate()."""
        defs = Definitions(
            entities={"customer": Entity(
                name="customer", starts="ghost",
                identity=Identity(email=Identity.Field()),
                properties={"x": Property("bad_type")},
                states={"lead": State()},
            )},
        )
        result = defs.validate()
        assert not result.valid
        # Should have both the structural errors from Entity.validate()
        assert any("starts state 'ghost'" in err for err in result.errors)
        assert any("invalid type 'bad_type'" in err for err in result.errors)

    def test_standalone_validate_definitions_function(self):
        """Test the standalone validate_definitions() function from defacto._defacto."""
        from defacto import validate_definitions

        result = validate_definitions({
            "entities": {
                "customer": {
                    "starts": "lead",
                    "identity": {"email": {"match": "exact"}},
                    "states": {"lead": {
                        "when": {"signup": {"effects": ["create"]}},
                    }},
                },
            },
        })
        assert result.valid, f"Expected valid but got errors: {result.errors}"

    def test_standalone_validate_definitions_with_errors(self):
        from defacto import validate_definitions

        result = validate_definitions({
            "entities": {
                "customer": {
                    "starts": "ghost",
                    "identity": {"email": {"match": "exact"}},
                    "states": {"lead": {}},
                },
            },
        })
        assert not result.valid
        assert any("starts state 'ghost'" in err for err in result.errors)

    # -- Event type consistency --

    def test_entity_handler_event_without_schema(self):
        defs = Definitions(
            entities={"customer": self._customer_entity()},
            schemas={"other_event": Schema("other_event", fields={"x": Schema.Field("string")})},
        )
        result = defs.validate()
        assert not result.valid
        assert any("'signup' is handled by" in err and "no schema" in err for err in result.errors)

    def test_source_event_without_schema(self):
        defs = Definitions(
            entities={"customer": self._customer_entity()},
            sources={"app": Source(
                name="app", event_type="type", timestamp="ts",
                events={"signup": Source.Event(
                    mappings={}, hints={"customer": ["email"]},
                )},
            )},
            schemas={"other": Schema("other", fields={"x": Schema.Field("string")})},
        )
        result = defs.validate()
        assert any("produces event 'signup' but no schema" in err for err in result.errors)

    def test_event_consistency_skipped_without_schemas(self):
        """When no schemas are defined, event consistency checks are skipped."""
        defs = Definitions(entities={"customer": self._customer_entity()})
        result = defs.validate()
        assert not any("no schema" in err for err in result.errors)

    # -- Warnings --

    def test_warning_treatment_without_sensitive(self):
        defs = Definitions(
            entities={"customer": Entity(
                name="customer", starts="lead",
                identity=Identity(email=Identity.Field()),
                properties={"email": Property("string", treatment="hash")},
                states={"lead": State(when={"signup": Handler(effects=[Effect.create()])})},
            )},
        )
        result = defs.validate()
        assert any("treatment 'hash' but no sensitivity label" in w for w in result.warnings)

    def test_no_warning_treatment_with_sensitive(self):
        defs = Definitions(
            entities={"customer": Entity(
                name="customer", starts="lead",
                identity=Identity(email=Identity.Field()),
                properties={"email": Property("string", sensitive="pii", treatment="hash")},
                states={"lead": State(when={"signup": Handler(effects=[Effect.create()])})},
            )},
        )
        result = defs.validate()
        assert not result.warnings

    # -- Source ↔ Schema field consistency --

    def test_schema_requires_field_source_doesnt_produce(self):
        defs = Definitions(
            entities={"customer": self._customer_entity()},
            sources={"app": Source(
                name="app", event_type="type", timestamp="ts",
                events={"signup": Source.Event(
                    mappings={"name": {"from": "user_name"}},
                    hints={"customer": ["email"]},
                )},
            )},
            schemas={"signup": Schema("signup", fields={
                "email": Schema.Field("string", required=True),
                "name": Schema.Field("string"),
            })},
        )
        result = defs.validate()
        assert any(
            "requires field 'email'" in err and "doesn't produce it" in err
            for err in result.errors
        )

    def test_source_produces_field_not_in_schema(self):
        defs = Definitions(
            entities={"customer": self._customer_entity()},
            sources={"app": Source(
                name="app", event_type="type", timestamp="ts",
                events={"signup": Source.Event(
                    mappings={"email": {"from": "e"}, "extra": {"from": "x"}},
                    hints={"customer": ["email"]},
                )},
            )},
            schemas={"signup": Schema("signup", fields={
                "email": Schema.Field("string"),
            })},
        )
        result = defs.validate()
        assert any(
            "produces field 'extra' not declared in schema" in err
            for err in result.errors
        )

    def test_source_schema_fields_match(self):
        defs = Definitions(
            entities={"customer": self._customer_entity()},
            sources={"app": Source(
                name="app", event_type="type", timestamp="ts",
                events={"signup": Source.Event(
                    mappings={"email": {"from": "user_email"}},
                    hints={"customer": ["email"]},
                )},
            )},
            schemas={"signup": Schema("signup", fields={
                "email": Schema.Field("string", required=True),
            })},
        )
        result = defs.validate()
        # No source↔schema field errors
        assert not any("produces field" in err or "requires field" in err for err in result.errors)
