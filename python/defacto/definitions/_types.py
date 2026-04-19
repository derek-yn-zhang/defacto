"""Defacto definition objects.

These classes describe a domain: entity types, their states and properties,
how events are normalized from sources, and what events look like (schemas).

Three representations exist for every definition:
    YAML — human-authored, lives in git
    Python objects — programmatic, typed, inspectable (this module)
    Dict — serialization bridge (JSON/YAML parsed, database stored)

All conversion between representations goes through from_dict/to_dict.
YAML parsing is handled by from_yaml (reads file → parses → from_dict).
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar, Self

import yaml

from defacto.errors import DefinitionError
from defacto.results import ValidationResult


# ---------------------------------------------------------------------------
# Property — an attribute of an entity or relationship edge
# ---------------------------------------------------------------------------


@dataclass
class Property:
    """A typed attribute on an entity or relationship.

    Properties define columns in state history tables. Each property has a
    type, optional default value, optional sensitivity labels for governance,
    and optional validation constraints.

    Examples:
        Property("number", default=0)
        Property("string", sensitive="pii", treatment="mask")
        Property("string", allowed=["free", "pro", "enterprise"])
        Property("number", compute="entity.mrr * 12")  # derived property
    """

    type: str
    """Data type: 'string', 'number', 'integer', 'boolean', 'datetime'."""

    default: Any = None
    """Default value for new entities. None means no default (null)."""

    sensitive: str | None = None
    """Sensitivity label: 'pii', 'phi', 'pci'. Metadata only — informs treatments."""

    treatment: str | None = None
    """Built-in treatment applied at interpretation time: 'hash', 'mask', 'redact'."""

    allowed: list[Any] | None = None
    """Allowed values. Validated at interpretation time."""

    min: float | None = None
    """Minimum value (numeric properties only)."""

    max: float | None = None
    """Maximum value (numeric properties only)."""

    compute: str | None = None
    """Expression for computed/derived properties: 'entity.mrr * 12'.
    Computed properties are recalculated on every state change, not stored directly."""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        """Create a property from a parsed YAML dict.

        Args:
            data: Dict with 'type' (required) and optional keys: 'default',
                'sensitive', 'treatment', 'allowed', 'min', 'max', 'compute'.

        Raises:
            DefinitionError: If 'type' key is missing.
        """
        if "type" not in data:
            raise DefinitionError("Property: missing required field 'type'")
        return cls(
            type=data["type"],
            default=data.get("default"),
            sensitive=data.get("sensitive"),
            treatment=data.get("treatment"),
            allowed=data.get("allowed"),
            min=data.get("min"),
            max=data.get("max"),
            compute=data.get("compute"),
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize this property to a dictionary.

        Only includes keys with non-None values for cleanliness.
        """
        d: dict[str, Any] = {"type": self.type}
        if self.default is not None:
            d["default"] = self.default
        if self.sensitive is not None:
            d["sensitive"] = self.sensitive
        if self.treatment is not None:
            d["treatment"] = self.treatment
        if self.allowed is not None:
            d["allowed"] = self.allowed
        if self.min is not None:
            d["min"] = self.min
        if self.max is not None:
            d["max"] = self.max
        if self.compute is not None:
            d["compute"] = self.compute
        return d


# ---------------------------------------------------------------------------
# Identity — how events are resolved to an entity
# ---------------------------------------------------------------------------


class Identity:
    """Identity resolution configuration for an entity type.

    Defines which fields from events can be used to identify this entity,
    and how those fields should be matched (exact, case-insensitive) and
    optionally normalized before matching.

    The constructor accepts keyword arguments where each key is a field name
    and each value is an Identity.Field configuration:

        Identity(
            email=Identity.Field(normalize="str::to_lowercase(value)", match="exact"),
            phone=Identity.Field(match="exact"),
        )
    """

    @dataclass
    class Field:
        """Configuration for a single identity field.

        Controls how hint values are matched against existing identity mappings
        and optionally transformed before matching.
        """

        match: str = "exact"
        """Match strategy: 'exact' or 'case_insensitive'."""

        normalize: str | None = None
        """Expression applied to hint values before matching.
        Uses the same expression language as guards/conditions.
        Example: 'str::to_lowercase(value)' normalizes emails before lookup."""

        @classmethod
        def from_dict(cls, data: dict[str, Any]) -> Self:
            """Create an identity field from a parsed YAML dict.

            Args:
                data: Dict with optional 'match' and 'normalize' keys.
            """
            return cls(
                match=data.get("match", "exact"),
                normalize=data.get("normalize"),
            )

        def to_dict(self) -> dict[str, Any]:
            """Serialize this identity field to a dictionary."""
            d: dict[str, Any] = {"match": self.match}
            if self.normalize is not None:
                d["normalize"] = self.normalize
            return d

    fields: dict[str, Identity.Field]
    """Map of field name → field configuration."""

    def __init__(self, **fields: Identity.Field) -> None:
        """Create identity config from keyword arguments.

        Each keyword is a field name, each value is an Identity.Field.
        At least one field is required for identity resolution to work.
        """
        self.fields = fields

    def __repr__(self) -> str:
        field_strs = ", ".join(f"{k}={v!r}" for k, v in self.fields.items())
        return f"Identity({field_strs})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Identity):
            return NotImplemented
        return self.fields == other.fields

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        """Create identity config from a parsed YAML dict.

        Args:
            data: Dict where keys are field names and values are field
                config dicts (with 'match' and optional 'normalize').

        Raises:
            DefinitionError: If data is empty (at least one field required).
        """
        if not data:
            raise DefinitionError("Identity: at least one identity field is required")
        fields = {
            name: Identity.Field.from_dict(config) if isinstance(config, dict) else Identity.Field()
            for name, config in data.items()
        }
        return cls(**fields)

    def to_dict(self) -> dict[str, Any]:
        """Serialize this identity config to a dictionary."""
        return {name: field.to_dict() for name, field in self.fields.items()}


# ---------------------------------------------------------------------------
# Effect — a state change produced by interpretation
# ---------------------------------------------------------------------------


@dataclass
class Effect:
    """A declarative description of an entity state change.

    Effects are not constructed directly — use the class methods:
        Effect.create()                                    # create a new entity
        Effect.transition("active")                        # change state
        Effect.set("mrr", from_="event.new_mrr")          # set a property
        Effect.increment("login_count", by=1)              # increment a property
        Effect.relate("placed_by", target="customer")      # create a relationship

    Each effect may have a condition — an expression that must be true for
    the effect to apply:
        Effect.set("high_score", from_="event.score",
                   condition="event.score > entity.high_score")
    """

    kind: str
    """Effect type: 'create', 'transition', 'set', 'increment', 'relate'."""

    # -- Fields used by different effect kinds --

    property: str | None = None
    """Property name (for 'set' and 'increment' effects)."""

    from_: str | None = None
    """Value source reference: 'event.field' or 'entity.field' (for 'set')."""

    value: Any = None
    """Literal value (for 'set' with a static value)."""

    compute: str | None = None
    """Expression to compute the value (for 'set' with dynamic computation)."""

    to: str | None = None
    """Target state name (for 'transition' effects)."""

    by: int | float | None = None
    """Increment amount (for 'increment' effects)."""

    target: str | None = None
    """Target entity type (for 'relate' effects)."""

    hints: dict[str, list[str]] | None = None
    """Identity hints for the relationship target (for 'relate' effects)."""

    condition: str | None = None
    """Optional guard expression — effect only applies if this evaluates to true."""

    # -- Named constructors --

    @classmethod
    def create(cls) -> Self:
        """Create a new entity in its start state. Idempotent — no-op if entity exists."""
        return cls(kind="create")

    @classmethod
    def transition(cls, to: str) -> Self:
        """Transition the entity to a new state.

        Args:
            to: Target state name. Must exist in the entity's state definitions.
        """
        return cls(kind="transition", to=to)

    @classmethod
    def set(
        cls,
        property: str,
        *,
        from_: str | None = None,
        value: Any = None,
        compute: str | None = None,
        condition: str | None = None,
    ) -> Self:
        """Set a property value.

        Exactly one value source should be provided: from_, value, or compute.

        Args:
            property: Property name to set.
            from_: Reference to event or entity field ('event.amount', 'entity.old_plan').
            value: Literal value to set.
            compute: Expression to evaluate ('entity.mrr * 12').
            condition: Optional guard — effect only applies if true.
        """
        return cls(
            kind="set",
            property=property,
            from_=from_,
            value=value,
            compute=compute,
            condition=condition,
        )

    @classmethod
    def increment(cls, property: str, *, by: int | float = 1) -> Self:
        """Increment a numeric property by the given amount.

        Args:
            property: Property name to increment.
            by: Amount to add to the current value. Default 1.
        """
        return cls(kind="increment", property=property, by=by)

    @classmethod
    def relate(
        cls,
        type: str,
        *,
        target: str,
        hints: dict[str, list[str]] | None = None,
        condition: str | None = None,
    ) -> Self:
        """Create a relationship to another entity.

        Args:
            type: Relationship type name ('placed_order', 'belongs_to').
            target: Target entity type name.
            hints: Identity hints to resolve the target entity.
            condition: Optional guard — relationship only created if true.
        """
        # 'to' field is not used here — we reuse the same dataclass but
        # 'target' holds the entity type, not a state name.
        return cls(
            kind="relate",
            target=target,
            hints=hints,
            condition=condition,
            # Store the relationship type in 'property' field to avoid adding
            # another field. This is an internal representation detail — the
            # named constructor API is what users see.
            property=type,
        )

    # -- Serialization --

    _VALID_KINDS = {"create", "transition", "set", "increment", "relate"}

    @classmethod
    def from_dict(cls, data: str | dict[str, Any]) -> Self:
        """Create an effect from a parsed YAML element.

        YAML effects are either bare strings ('create') or single-key dicts
        where the key is the effect kind and the value is the body:
            'create'
            {'transition': {'to': 'active'}}
            {'set': {'property': 'email', 'from': 'event.email'}}

        Note: YAML uses 'from' (reserved in Python), mapped to 'from_' internally.

        Raises:
            DefinitionError: If the effect kind is unknown or required fields are missing.
        """
        # Bare string — only "create" is valid as a string
        if isinstance(data, str):
            if data == "create":
                return cls.create()
            raise DefinitionError(
                f"Effect: unknown string effect '{data}', expected 'create' or a dict"
            )

        if not isinstance(data, dict) or len(data) != 1:
            raise DefinitionError(
                f"Effect: expected a string or single-key dict, got {type(data).__name__}"
            )

        kind = next(iter(data))
        body = data[kind]

        if kind not in cls._VALID_KINDS:
            raise DefinitionError(
                f"Effect: unknown kind '{kind}', expected one of {sorted(cls._VALID_KINDS)}"
            )

        if kind == "create":
            # Allow {"create": null} or {"create": {}} from YAML
            return cls.create()

        if kind == "transition":
            if "to" not in body:
                raise DefinitionError("Effect 'transition': missing required field 'to'")
            return cls.transition(body["to"])

        if kind == "set":
            if "property" not in body:
                raise DefinitionError("Effect 'set': missing required field 'property'")
            return cls.set(
                body["property"],
                from_=body.get("from"),
                value=body.get("value"),
                compute=body.get("compute"),
                condition=body.get("condition"),
            )

        if kind == "increment":
            if "property" not in body:
                raise DefinitionError("Effect 'increment': missing required field 'property'")
            return cls.increment(body["property"], by=body.get("by", 1))

        # kind == "relate"
        if "type" not in body:
            raise DefinitionError("Effect 'relate': missing required field 'type'")
        if "target" not in body:
            raise DefinitionError("Effect 'relate': missing required field 'target'")
        return cls.relate(
            body["type"],
            target=body["target"],
            hints=body.get("hints"),
            condition=body.get("condition"),
        )

    def to_dict(self) -> str | dict[str, Any]:
        """Serialize this effect to its YAML-compatible dict form.

        Returns a string for 'create', or a single-key dict for other kinds.
        """
        if self.kind == "create":
            return "create"

        if self.kind == "transition":
            return {"transition": {"to": self.to}}

        if self.kind == "set":
            body: dict[str, Any] = {"property": self.property}
            if self.from_ is not None:
                body["from"] = self.from_
            if self.value is not None:
                body["value"] = self.value
            if self.compute is not None:
                body["compute"] = self.compute
            if self.condition is not None:
                body["condition"] = self.condition
            return {"set": body}

        if self.kind == "increment":
            body = {"property": self.property, "by": self.by}
            return {"increment": body}

        # kind == "relate" — relationship type is stored in self.property
        body = {"type": self.property, "target": self.target}
        if self.hints is not None:
            body["hints"] = self.hints
        if self.condition is not None:
            body["condition"] = self.condition
        return {"relate": body}


# ---------------------------------------------------------------------------
# Handler — what happens when an event arrives in a state
# ---------------------------------------------------------------------------


@dataclass
class Handler:
    """An event handler within a state — what happens when an event arrives.

    Handlers optionally have a guard expression that must evaluate to true
    for the handler to fire. If the guard fails, the event is ignored in
    this state for this handler.

    In YAML, handlers live under the 'when:' key in a state definition:
        states:
          active:
            when:
              plan_upgrade:
                guard: "event.plan != entity.plan"
                effects:
                  - set: { property: plan, from: event.new_plan }
    """

    effects: list[Effect]
    """Effects produced when this handler fires. At least one required."""

    guard: str | None = None
    """Expression that must be true for this handler to fire.
    Has access to event.* and entity.* context.
    Example: 'event.amount > 0 and entity.state == \"active\"'"""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        """Create a handler from a parsed YAML dict.

        Args:
            data: Dict with 'effects' (required list) and optional 'guard' string.

        Raises:
            DefinitionError: If 'effects' is missing or empty.
        """
        if "effects" not in data:
            raise DefinitionError("Handler: missing required field 'effects'")
        effects_data = data["effects"]
        if not effects_data:
            raise DefinitionError("Handler: 'effects' must not be empty")
        return cls(
            effects=[Effect.from_dict(e) for e in effects_data],
            guard=data.get("guard"),
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize this handler to a dictionary."""
        d: dict[str, Any] = {"effects": [e.to_dict() for e in self.effects]}
        if self.guard is not None:
            d["guard"] = self.guard
        return d


# ---------------------------------------------------------------------------
# TimeRule — what happens based on time passing
# ---------------------------------------------------------------------------


@dataclass
class TimeRule:
    """A time-based rule evaluated periodically and on event arrival.

    Time rules fire when a duration threshold is met. They produce effects
    just like event handlers — typically state transitions.

    Three types:
        inactivity  — fires when no events received for threshold duration
        expiration  — fires when threshold duration has passed since entity creation
        state_duration — fires when entity has been in current state for threshold

    In YAML, time rules live under the 'after:' key in a state definition:
        states:
          active:
            after:
              - inactivity: 90d
                effects:
                  - transition: { to: churned }
    """

    type: str
    """Rule type: 'inactivity', 'expiration', 'state_duration'."""

    threshold: str
    """Duration threshold: '30d', '24h', '90m'."""

    effects: list[Effect] = field(default_factory=list)
    """Effects produced when the rule fires."""

    @classmethod
    def inactivity(cls, threshold: str, *, effects: list[Effect]) -> Self:
        """No events received for the threshold duration.

        Args:
            threshold: Duration string ('30d', '24h').
            effects: Effects to produce when the rule fires.
        """
        return cls(type="inactivity", threshold=threshold, effects=effects)

    @classmethod
    def expiration(cls, threshold: str, *, effects: list[Effect]) -> Self:
        """Threshold duration has passed since entity creation.

        Args:
            threshold: Duration string ('365d', '30d').
            effects: Effects to produce when the rule fires.
        """
        return cls(type="expiration", threshold=threshold, effects=effects)

    @classmethod
    def state_duration(cls, threshold: str, *, effects: list[Effect]) -> Self:
        """Entity has been in the current state for the threshold duration.

        Args:
            threshold: Duration string ('14d', '1h').
            effects: Effects to produce when the rule fires.
        """
        return cls(type="state_duration", threshold=threshold, effects=effects)

    _VALID_TYPES = {"inactivity", "expiration", "state_duration"}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        """Create a time rule from a parsed YAML dict.

        Uses explicit format: {'type': 'inactivity', 'threshold': '90d', 'effects': [...]}.

        Raises:
            DefinitionError: If required fields are missing or type is unknown.
        """
        if "type" not in data:
            raise DefinitionError("TimeRule: missing required field 'type'")
        if "threshold" not in data:
            raise DefinitionError("TimeRule: missing required field 'threshold'")
        rule_type = data["type"]
        if rule_type not in cls._VALID_TYPES:
            raise DefinitionError(
                f"TimeRule: unknown type '{rule_type}', expected one of {sorted(cls._VALID_TYPES)}"
            )
        return cls(
            type=rule_type,
            threshold=data["threshold"],
            effects=[Effect.from_dict(e) for e in data.get("effects", [])],
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize this time rule to a dictionary."""
        return {
            "type": self.type,
            "threshold": self.threshold,
            "effects": [e.to_dict() for e in self.effects],
        }


# ---------------------------------------------------------------------------
# State — one position in the entity lifecycle
# ---------------------------------------------------------------------------


@dataclass
class State:
    """A state in the entity's lifecycle state machine.

    Each state defines what happens when events arrive ('when') and what
    happens based on time passing ('after'). States with no handlers or
    time rules are terminal — the entity stays there unless a global
    handler ('always') triggers a transition.

    In YAML:
        states:
          active:
            when:
              upgrade: { effects: [...] }
              close: { effects: [...] }
            after:
              - inactivity: 90d
                effects: [...]
    """

    when: dict[str, Handler] = field(default_factory=dict)
    """Event handlers. Key = event type, value = handler with guard + effects.
    'On this event, do these things.'"""

    after: list[TimeRule] = field(default_factory=list)
    """Time-based rules. 'After this duration, do these things.'"""

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> Self:
        """Create a state from a parsed YAML dict.

        Handles terminal states where YAML produces None (bare key with no value)
        or empty dict.

        Args:
            data: Dict with optional 'when' and 'after' keys, or None/{} for terminal states.
        """
        if not data:
            # YAML `delivered:` → None, `delivered: {}` → {} — both are terminal states
            return cls()
        when = {
            event_type: Handler.from_dict(handler_data)
            for event_type, handler_data in data.get("when", {}).items()
        }
        after = [TimeRule.from_dict(tr) for tr in data.get("after", [])]
        return cls(when=when, after=after)

    def to_dict(self) -> dict[str, Any]:
        """Serialize this state to a dictionary.

        Returns empty dict for terminal states (no handlers, no time rules).
        """
        d: dict[str, Any] = {}
        if self.when:
            d["when"] = {event_type: h.to_dict() for event_type, h in self.when.items()}
        if self.after:
            d["after"] = [tr.to_dict() for tr in self.after]
        return d


# ---------------------------------------------------------------------------
# Relationship — how entities connect to each other
# ---------------------------------------------------------------------------


@dataclass
class Relationship:
    """A declared relationship between two entity types.

    Both sides of a relationship must be declared. If customer has
    'placed_order → order (has_many)', then order must have
    'placed_by → customer (belongs_to)'. This is validated at registration.

    Relationships can have typed properties on the edge (e.g., order total,
    relationship grade).

    In YAML:
        relationships:
          - type: placed_order
            target: order
            cardinality: has_many
            properties:
              total: { type: number }
    """

    type: str
    """Relationship name: 'placed_order', 'belongs_to'."""

    target: str
    """Target entity type name. Must exist as a registered entity."""

    cardinality: str
    """'has_many', 'has_one', 'belongs_to', 'many_to_many'."""

    properties: dict[str, Property] = field(default_factory=dict)
    """Typed properties on the relationship edge."""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        """Create a relationship from a parsed YAML dict.

        Args:
            data: Dict with 'type', 'target', 'cardinality' (required),
                and optional 'properties'.

        Raises:
            DefinitionError: If required fields are missing.
        """
        for required in ("type", "target", "cardinality"):
            if required not in data:
                raise DefinitionError(f"Relationship: missing required field '{required}'")
        properties = {
            name: Property.from_dict(prop_data)
            for name, prop_data in data.get("properties", {}).items()
        }
        return cls(
            type=data["type"],
            target=data["target"],
            cardinality=data["cardinality"],
            properties=properties,
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize this relationship to a dictionary."""
        d: dict[str, Any] = {
            "type": self.type,
            "target": self.target,
            "cardinality": self.cardinality,
        }
        if self.properties:
            d["properties"] = {name: p.to_dict() for name, p in self.properties.items()}
        return d


# ---------------------------------------------------------------------------
# Entity — the core domain object
# ---------------------------------------------------------------------------


@dataclass
class Entity:
    """An entity type definition — the core of Defacto's domain model.

    Describes what an entity is, how it behaves, what properties it has,
    how events are resolved to it, and how it relates to other entities.

    A business person reading an Entity definition should understand:
    - What states the entity goes through (lifecycle)
    - What events affect it and how (handlers)
    - What data it carries (properties)
    - What it connects to (relationships)

    In YAML:
        customer:
          starts: lead
          identity:
            email: { normalize: "str::to_lowercase(value)", match: exact }
          properties:
            mrr: { type: number, default: 0 }
          states:
            lead:
              when:
                signup: { effects: [...] }
            active: ...
          relationships:
            - type: placed_order
              target: order
              cardinality: has_many
    """

    name: str
    """Entity type name. Used in table names, identity resolution, and throughout the system."""

    starts: str
    """Initial state for newly created entities. Must exist in states."""

    identity: Identity
    """Identity resolution configuration — which fields identify this entity."""

    properties: dict[str, Property] = field(default_factory=dict)
    """Entity properties. Define columns in state history tables."""

    states: dict[str, State] = field(default_factory=dict)
    """State machine definition. At least one state required."""

    relationships: list[Relationship] = field(default_factory=list)
    """Relationships to other entity types."""

    always: dict[str, Handler] = field(default_factory=dict)
    """Event handlers that fire in ANY state (global handlers)."""

    @classmethod
    def from_yaml(cls, path: str) -> Self:
        """Parse an entity definition from a YAML file.

        The file should have one top-level key (the entity name):
            customer:
              starts: lead
              ...

        Args:
            path: Path to the YAML file.

        Returns:
            An Entity instance.

        Raises:
            DefinitionError: If the file can't be read or contains invalid YAML.
        """
        try:
            with open(path) as f:
                data = yaml.safe_load(f)
        except FileNotFoundError:
            raise DefinitionError(f"Entity definition file not found: '{path}'")
        except yaml.YAMLError as e:
            raise DefinitionError(f"Invalid YAML in entity definition '{path}': {e}")
        return cls.from_dict(data)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        """Create an entity definition from a parsed YAML dict.

        The dict has the entity name as a single top-level key:
            {'customer': {'starts': 'lead', 'identity': {...}, ...}}

        This matches the output of yaml.safe_load() on a YAML entity file.

        Args:
            data: Single-key dict where key = entity name, value = entity body.

        Raises:
            DefinitionError: If the dict structure is invalid or required fields
                are missing (starts, identity, properties, states).
        """
        if not isinstance(data, dict) or len(data) != 1:
            raise DefinitionError(
                "Entity: expected a single-key dict {'entity_name': {...}}"
            )

        name = next(iter(data))
        body = data[name]

        for required in ("starts", "identity", "states"):
            if required not in body:
                raise DefinitionError(
                    f"Entity '{name}': missing required field '{required}'"
                )

        identity = Identity.from_dict(body["identity"])

        properties = {
            prop_name: Property.from_dict(prop_data)
            for prop_name, prop_data in body.get("properties", {}).items()
        }

        states = {
            state_name: State.from_dict(state_data)
            for state_name, state_data in body["states"].items()
        }

        relationships = [
            Relationship.from_dict(rel_data)
            for rel_data in body.get("relationships", [])
        ]

        always = {
            event_type: Handler.from_dict(handler_data)
            for event_type, handler_data in body.get("always", {}).items()
        }

        return cls(
            name=name,
            starts=body["starts"],
            identity=identity,
            properties=properties,
            states=states,
            relationships=relationships,
            always=always,
        )

    def to_yaml(self) -> str:
        """Serialize this entity definition to a YAML string."""
        return yaml.dump(self.to_dict(), default_flow_style=False, sort_keys=False)

    def to_dict(self) -> dict[str, Any]:
        """Serialize this entity definition to a dictionary.

        Returns a single-key dict: {'entity_name': {body}}.
        """
        body: dict[str, Any] = {
            "starts": self.starts,
            "identity": self.identity.to_dict(),
        }
        if self.properties:
            body["properties"] = {
                name: p.to_dict() for name, p in self.properties.items()
            }
        body["states"] = {
            name: s.to_dict() for name, s in self.states.items()
        }
        if self.relationships:
            body["relationships"] = [r.to_dict() for r in self.relationships]
        if self.always:
            body["always"] = {
                event_type: h.to_dict() for event_type, h in self.always.items()
            }
        return {self.name: body}

    _VALID_PROPERTY_TYPES: ClassVar[set[str]] = {"string", "number", "integer", "boolean", "datetime"}
    _NUMERIC_PROPERTY_TYPES: ClassVar[set[str]] = {"number", "integer"}
    _VALID_CARDINALITIES: ClassVar[set[str]] = {"has_many", "has_one", "belongs_to", "many_to_many"}

    # Maps property type names to the Python types that are valid defaults/allowed values
    _TYPE_PYTHON_MAP: ClassVar[dict[str, tuple[type, ...]]] = {
        "string": (str,),
        "number": (int, float),
        "integer": (int,),
        "boolean": (bool,),
        "datetime": (str,),  # datetimes are strings in YAML, parsed at runtime
    }

    def validate(self) -> list[str]:
        """Check this entity for structural errors.

        Returns a list of error strings. Empty list means valid.
        These are checks that can be done on a single entity in isolation —
        cross-definition checks (relationship targets exist, etc.) happen
        in Definitions.validate().
        """
        errors: list[str] = []
        self._validate_structure(errors)
        self._validate_properties(errors)
        self._validate_relationships(errors)
        self._validate_effects(errors)
        self._validate_lifecycle(errors)
        return errors

    def _validate_structure(self, errors: list[str]) -> None:
        """Core structural checks: states exist, identity present."""
        prefix = f"Entity '{self.name}'"

        if not self.states:
            errors.append(f"{prefix}: at least one state is required")

        if self.starts not in self.states:
            errors.append(
                f"{prefix}: starts state '{self.starts}' not found in states "
                f"{sorted(self.states.keys())}"
            )

        if not self.identity.fields:
            errors.append(f"{prefix}: identity must have at least one field")

    def _validate_properties(self, errors: list[str]) -> None:
        """Property types, defaults, allowed values, and system column collisions."""
        prefix = f"Entity '{self.name}'"
        system_columns = {f"{self.name}_id", f"{self.name}_state"}

        for prop_name, prop in self.properties.items():
            prop_prefix = f"{prefix} property '{prop_name}'"

            # Type must be valid
            if prop.type not in self._VALID_PROPERTY_TYPES:
                errors.append(
                    f"{prop_prefix}: invalid type '{prop.type}', "
                    f"expected one of {sorted(self._VALID_PROPERTY_TYPES)}"
                )
                continue  # skip further checks if type itself is invalid

            # Default value must match the declared type
            if prop.default is not None:
                expected = self._TYPE_PYTHON_MAP[prop.type]
                # bool is a subclass of int in Python, so check bool first
                if prop.type != "boolean" and isinstance(prop.default, bool):
                    errors.append(
                        f"{prop_prefix}: default value {prop.default!r} is boolean, "
                        f"expected {prop.type}"
                    )
                elif not isinstance(prop.default, expected):
                    errors.append(
                        f"{prop_prefix}: default value {prop.default!r} doesn't match "
                        f"type '{prop.type}'"
                    )

            # Allowed values must all match the declared type
            if prop.allowed is not None:
                expected = self._TYPE_PYTHON_MAP[prop.type]
                for val in prop.allowed:
                    if prop.type != "boolean" and isinstance(val, bool):
                        errors.append(
                            f"{prop_prefix}: allowed value {val!r} is boolean, "
                            f"expected {prop.type}"
                        )
                    elif not isinstance(val, expected):
                        errors.append(
                            f"{prop_prefix}: allowed value {val!r} doesn't match "
                            f"type '{prop.type}'"
                        )

            # System column collision
            if prop_name in system_columns:
                errors.append(f"{prop_prefix}: collides with system column")

    def _validate_relationships(self, errors: list[str]) -> None:
        """Relationship cardinality values."""
        prefix = f"Entity '{self.name}'"

        for rel in self.relationships:
            if rel.cardinality not in self._VALID_CARDINALITIES:
                errors.append(
                    f"{prefix} relationship '{rel.type}': invalid cardinality "
                    f"'{rel.cardinality}', expected one of {sorted(self._VALID_CARDINALITIES)}"
                )

    def _validate_effects(self, errors: list[str]) -> None:
        """Walk all handlers (state, time rule, always) and validate their effects."""
        prefix = f"Entity '{self.name}'"

        def check_effect(effect: Effect, location: str) -> None:
            """Validate a single effect against declared properties and states."""
            if effect.kind == "transition":
                if effect.to not in self.states:
                    errors.append(
                        f"{prefix} {location}: transition target '{effect.to}' "
                        f"not found in states {sorted(self.states.keys())}"
                    )

            elif effect.kind == "set":
                if effect.property and effect.property not in self.properties:
                    errors.append(
                        f"{prefix} {location}: set references undeclared "
                        f"property '{effect.property}'"
                    )
                # Exactly one value source
                source_count = sum(
                    1 for x in (effect.from_, effect.value, effect.compute)
                    if x is not None
                )
                if source_count == 0:
                    errors.append(
                        f"{prefix} {location}: set effect for '{effect.property}' "
                        f"has no value source (need 'from', 'value', or 'compute')"
                    )
                elif source_count > 1:
                    errors.append(
                        f"{prefix} {location}: set effect for '{effect.property}' "
                        f"has multiple value sources (use only one of "
                        f"'from', 'value', 'compute')"
                    )
                # Literal value type check — if setting with a literal, it should
                # match the property type
                if (
                    effect.property
                    and effect.value is not None
                    and effect.property in self.properties
                ):
                    prop = self.properties[effect.property]
                    if prop.type in self._TYPE_PYTHON_MAP:
                        expected = self._TYPE_PYTHON_MAP[prop.type]
                        if not isinstance(effect.value, expected):
                            errors.append(
                                f"{prefix} {location}: set literal value "
                                f"{effect.value!r} doesn't match property "
                                f"'{effect.property}' type '{prop.type}'"
                            )

            elif effect.kind == "increment":
                if effect.property and effect.property not in self.properties:
                    errors.append(
                        f"{prefix} {location}: increment references undeclared "
                        f"property '{effect.property}'"
                    )
                # Increment only makes sense on numeric properties
                if effect.property and effect.property in self.properties:
                    prop = self.properties[effect.property]
                    if prop.type not in self._NUMERIC_PROPERTY_TYPES:
                        errors.append(
                            f"{prefix} {location}: increment on property "
                            f"'{effect.property}' which is '{prop.type}', "
                            f"not numeric"
                        )

        def check_handler(handler: Handler, location: str) -> None:
            """Validate all effects in a handler."""
            for effect in handler.effects:
                check_effect(effect, location)

        # State handlers
        for state_name, state in self.states.items():
            for event_type, handler in state.when.items():
                check_handler(handler, f"state '{state_name}' handler '{event_type}'")
            for rule in state.after:
                if rule.type not in TimeRule._VALID_TYPES:
                    errors.append(
                        f"{prefix} state '{state_name}': invalid time rule type "
                        f"'{rule.type}'"
                    )
                loc = f"state '{state_name}' time rule '{rule.type}'"
                for effect in rule.effects:
                    check_effect(effect, loc)

        # Always handlers
        for event_type, handler in self.always.items():
            check_handler(handler, f"always handler '{event_type}'")

    def state_graph(self) -> dict[str, set[str]]:
        """Extract the state machine transition graph.

        Returns a dict of state_name → set of target state names reachable
        via event handlers, time rules, and always handlers. Used by
        validation (reachability, dead-end detection) and by the deep
        validation warnings in Definitions._collect_warnings.
        """
        graph: dict[str, set[str]] = {s: set() for s in self.states}

        for state_name, state in self.states.items():
            for handler in state.when.values():
                for effect in handler.effects:
                    if effect.kind == "transition" and effect.to:
                        graph[state_name].add(effect.to)
            for rule in state.after:
                for effect in rule.effects:
                    if effect.kind == "transition" and effect.to:
                        graph[state_name].add(effect.to)

        # Always handlers can transition from any state
        always_targets: set[str] = set()
        for handler in self.always.values():
            for effect in handler.effects:
                if effect.kind == "transition" and effect.to:
                    always_targets.add(effect.to)
        if always_targets:
            for targets in graph.values():
                targets.update(always_targets)

        return graph

    def reachable_states(self) -> set[str]:
        """Find all states reachable from the starts state via BFS."""
        graph = self.state_graph()
        reachable: set[str] = {self.starts}
        frontier = [self.starts]
        while frontier:
            current = frontier.pop()
            for target in graph.get(current, set()):
                if target not in reachable:
                    reachable.add(target)
                    frontier.append(target)
        return reachable

    def _validate_lifecycle(self, errors: list[str]) -> None:
        """Lifecycle checks: start state has create, unreachable states."""
        prefix = f"Entity '{self.name}'"

        # The starts state should have at least one handler with a create effect
        if self.starts in self.states:
            starts_state = self.states[self.starts]
            has_create = any(
                effect.kind == "create"
                for handler in starts_state.when.values()
                for effect in handler.effects
            )
            if not has_create:
                errors.append(
                    f"{prefix}: starts state '{self.starts}' has no handler with "
                    f"a 'create' effect — entities cannot be created"
                )

        # Unreachable states — no path from starts
        unreachable = set(self.states.keys()) - self.reachable_states()
        for state_name in sorted(unreachable):
            errors.append(
                f"{prefix}: state '{state_name}' is unreachable — no transitions "
                f"lead to it and it is not the starts state"
            )


# ---------------------------------------------------------------------------
# Source — how raw input becomes events
# ---------------------------------------------------------------------------


class Source:
    """A source definition — how raw input from an external system is normalized.

    Sources define where to find the event type and timestamp in raw records,
    and how to map raw fields to normalized event fields for each event type.

    In YAML:
        app:
          event_type: type
          timestamp: created_at
          events:
            customer_signup:
              raw_type: "user.created"
              mappings:
                email: { from: user_email }
              hints:
                customer: [email]
    """

    @dataclass
    class Event:
        """Configuration for one event type produced by a source.

        Defines how to normalize raw data for this event type — which raw
        event type it corresponds to, how fields are mapped, and which
        fields serve as identity hints for which entity types.
        """

        mappings: dict[str, dict[str, Any]]
        """Field mappings. Key = normalized field name, value = mapping config.
        Mapping config keys: 'from' (field reference), 'compute' (expression),
        'type' (coercion), 'default' (fallback value)."""

        hints: dict[str, list[str]]
        """Identity hints. Key = entity type name, value = list of field names
        to use as identity hints for that entity type."""

        raw_type: str | None = None
        """Event type value in the raw input (what the source calls it).
        If None, uses the key from the events dict (same name in source and Defacto)."""

    def __init__(
        self,
        name: str,
        *,
        event_type: str,
        timestamp: str,
        events: dict[str, Source.Event],
        event_id: list[str] | None = None,
    ) -> None:
        """Create a source definition.

        Args:
            name: Source name. Used in m.ingest(source_name, ...).
            event_type: Raw field name containing the event type.
            timestamp: Raw field name containing the timestamp.
            events: Event types this source produces. Key = Defacto event type name.
            event_id: Fields used to compute content-based event ID for dedup.
                Default: hash all fields.
        """
        self.name = name
        self.event_type = event_type
        self.timestamp = timestamp
        self.events = events
        self.event_id = event_id

    def __repr__(self) -> str:
        return f"Source({self.name!r}, events={list(self.events.keys())})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Source):
            return NotImplemented
        return (
            self.name == other.name
            and self.event_type == other.event_type
            and self.timestamp == other.timestamp
            and self.events == other.events
            and self.event_id == other.event_id
        )

    @classmethod
    def from_yaml(cls, path: str) -> Self:
        """Parse a source definition from a YAML file.

        Raises:
            DefinitionError: If the file can't be read or contains invalid YAML.
        """
        try:
            with open(path) as f:
                data = yaml.safe_load(f)
        except FileNotFoundError:
            raise DefinitionError(f"Source definition file not found: '{path}'")
        except yaml.YAMLError as e:
            raise DefinitionError(f"Invalid YAML in source definition '{path}': {e}")
        return cls.from_dict(data)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        """Create a source definition from a parsed YAML dict.

        The dict has the source name as a single top-level key:
            {'app': {'event_type': 'type', 'timestamp': 'created_at', ...}}

        Args:
            data: Single-key dict where key = source name, value = source body.

        Raises:
            DefinitionError: If required fields are missing (event_type, timestamp, events).
        """
        if not isinstance(data, dict) or len(data) != 1:
            raise DefinitionError(
                "Source: expected a single-key dict {'source_name': {...}}"
            )

        name = next(iter(data))
        body = data[name]

        for required in ("event_type", "timestamp", "events"):
            if required not in body:
                raise DefinitionError(
                    f"Source '{name}': missing required field '{required}'"
                )

        events = {}
        for event_name, event_data in body["events"].items():
            events[event_name] = Source.Event(
                raw_type=event_data.get("raw_type"),
                mappings=event_data.get("mappings", {}),
                hints=event_data.get("hints", {}),
            )

        return cls(
            name=name,
            event_type=body["event_type"],
            timestamp=body["timestamp"],
            events=events,
            event_id=body.get("event_id"),
        )

    def to_yaml(self) -> str:
        """Serialize this source definition to a YAML string."""
        return yaml.dump(self.to_dict(), default_flow_style=False, sort_keys=False)

    def to_dict(self) -> dict[str, Any]:
        """Serialize this source definition to a dictionary.

        Returns a single-key dict: {'source_name': {body}}.
        """
        events_dict: dict[str, Any] = {}
        for event_name, event in self.events.items():
            event_dict: dict[str, Any] = {}
            if event.raw_type is not None:
                event_dict["raw_type"] = event.raw_type
            if event.mappings:
                event_dict["mappings"] = event.mappings
            if event.hints:
                event_dict["hints"] = event.hints
            events_dict[event_name] = event_dict

        body: dict[str, Any] = {
            "event_type": self.event_type,
            "timestamp": self.timestamp,
        }
        if self.event_id is not None:
            body["event_id"] = self.event_id
        body["events"] = events_dict
        return {self.name: body}

    def validate(self) -> list[str]:
        """Check this source for structural errors.

        Returns a list of error strings. Empty list means valid.
        """
        errors: list[str] = []
        prefix = f"Source '{self.name}'"

        if not self.event_type:
            errors.append(f"{prefix}: 'event_type' must be a non-empty string")

        if not self.timestamp:
            errors.append(f"{prefix}: 'timestamp' must be a non-empty string")

        for event_name, event in self.events.items():
            # Every event should have at least one hint
            if not event.hints:
                errors.append(
                    f"{prefix} event '{event_name}': must have at least one "
                    f"identity hint (otherwise events can't be resolved to entities)"
                )

            # Field mappings: from and compute are mutually exclusive
            for field_name, mapping in event.mappings.items():
                has_from = "from" in mapping
                has_compute = "compute" in mapping
                if has_from and has_compute:
                    errors.append(
                        f"{prefix} event '{event_name}' mapping '{field_name}': "
                        f"'from' and 'compute' are mutually exclusive"
                    )

        return errors


# ---------------------------------------------------------------------------
# Schema — the contract for what an event type looks like
# ---------------------------------------------------------------------------


class Schema:
    """An event schema — the contract between sources and entities.

    Defines what fields an event type must have, their types, and validation
    constraints. Sources produce events conforming to the schema. Entities
    consume events that match the schema. Cross-definition validation ensures
    consistency at registration time.

    In YAML:
        customer_signup:
          fields:
            email: { type: string, required: true }
            plan: { type: string, allowed: [free, pro, enterprise] }
    """

    @dataclass
    class Field:
        """Validation constraints for a single event field.

        Applied after normalization to catch bad data before interpretation.
        """

        type: str
        """Expected data type: 'string', 'number', 'integer', 'boolean', 'datetime'."""

        required: bool = False
        """Must be present and non-null in the event data."""

        allowed: list[Any] | None = None
        """Allowed values. Event is invalid if field value not in this list."""

        min: float | None = None
        """Minimum value (numeric fields only)."""

        max: float | None = None
        """Maximum value (numeric fields only)."""

        min_length: int | None = None
        """Minimum string length."""

        regex: str | None = None
        """Regex pattern the value must match."""

        @classmethod
        def from_dict(cls, data: dict[str, Any]) -> Self:
            """Create a schema field from a parsed YAML dict.

            Args:
                data: Dict with 'type' (required) and optional validation keys.

            Raises:
                DefinitionError: If 'type' key is missing.
            """
            if "type" not in data:
                raise DefinitionError("Schema field: missing required field 'type'")
            return cls(
                type=data["type"],
                required=data.get("required", False),
                allowed=data.get("allowed"),
                min=data.get("min"),
                max=data.get("max"),
                min_length=data.get("min_length"),
                regex=data.get("regex"),
            )

        def to_dict(self) -> dict[str, Any]:
            """Serialize this schema field to a dictionary."""
            d: dict[str, Any] = {"type": self.type}
            if self.required:
                d["required"] = self.required
            if self.allowed is not None:
                d["allowed"] = self.allowed
            if self.min is not None:
                d["min"] = self.min
            if self.max is not None:
                d["max"] = self.max
            if self.min_length is not None:
                d["min_length"] = self.min_length
            if self.regex is not None:
                d["regex"] = self.regex
            return d

    def __init__(self, name: str, *, fields: dict[str, Schema.Field]) -> None:
        """Create an event schema.

        Args:
            name: Event type name. Must match event types used in sources and
                entity handlers.
            fields: Field definitions. Key = field name, value = validation config.
        """
        self.name = name
        self.fields = fields

    def __repr__(self) -> str:
        return f"Schema({self.name!r}, fields={list(self.fields.keys())})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Schema):
            return NotImplemented
        return self.name == other.name and self.fields == other.fields

    @classmethod
    def from_yaml(cls, path: str) -> Self:
        """Parse an event schema from a YAML file.

        Raises:
            DefinitionError: If the file can't be read or contains invalid YAML.
        """
        try:
            with open(path) as f:
                data = yaml.safe_load(f)
        except FileNotFoundError:
            raise DefinitionError(f"Schema definition file not found: '{path}'")
        except yaml.YAMLError as e:
            raise DefinitionError(f"Invalid YAML in schema definition '{path}': {e}")
        return cls.from_dict(data)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        """Create an event schema from a parsed YAML dict.

        The dict has the schema name as a single top-level key:
            {'customer_signup': {'fields': {'email': {'type': 'string', ...}}}}

        Args:
            data: Single-key dict where key = event type name, value = schema body.

        Raises:
            DefinitionError: If the dict structure is invalid or 'fields' is missing.
        """
        if not isinstance(data, dict) or len(data) != 1:
            raise DefinitionError(
                "Schema: expected a single-key dict {'event_type_name': {...}}"
            )

        name = next(iter(data))
        body = data[name]

        if "fields" not in body:
            raise DefinitionError(f"Schema '{name}': missing required field 'fields'")

        fields = {
            field_name: Schema.Field.from_dict(field_data)
            for field_name, field_data in body["fields"].items()
        }
        return cls(name=name, fields=fields)

    def to_yaml(self) -> str:
        """Serialize this schema to a YAML string."""
        return yaml.dump(self.to_dict(), default_flow_style=False, sort_keys=False)

    def to_dict(self) -> dict[str, Any]:
        """Serialize this schema to a dictionary.

        Returns a single-key dict: {'event_type_name': {body}}.
        """
        return {
            self.name: {
                "fields": {
                    name: f.to_dict() for name, f in self.fields.items()
                }
            }
        }

    _VALID_FIELD_TYPES = {"string", "number", "integer", "boolean", "datetime"}
    _NUMERIC_TYPES = {"number", "integer"}
    _STRING_TYPES = {"string"}

    def validate(self) -> list[str]:
        """Check this schema for structural errors.

        Returns a list of error strings. Empty list means valid.
        """
        errors: list[str] = []
        prefix = f"Schema '{self.name}'"

        for field_name, field in self.fields.items():
            if field.type not in self._VALID_FIELD_TYPES:
                errors.append(
                    f"{prefix} field '{field_name}': invalid type '{field.type}', "
                    f"expected one of {sorted(self._VALID_FIELD_TYPES)}"
                )
                continue

            # Numeric constraints only on numeric types
            if field.min is not None and field.type not in self._NUMERIC_TYPES:
                errors.append(
                    f"{prefix} field '{field_name}': 'min' constraint only "
                    f"applies to numeric types, not '{field.type}'"
                )
            if field.max is not None and field.type not in self._NUMERIC_TYPES:
                errors.append(
                    f"{prefix} field '{field_name}': 'max' constraint only "
                    f"applies to numeric types, not '{field.type}'"
                )

            # String constraints only on string types
            if field.min_length is not None and field.type not in self._STRING_TYPES:
                errors.append(
                    f"{prefix} field '{field_name}': 'min_length' constraint only "
                    f"applies to string types, not '{field.type}'"
                )
            if field.regex is not None and field.type not in self._STRING_TYPES:
                errors.append(
                    f"{prefix} field '{field_name}': 'regex' constraint only "
                    f"applies to string types, not '{field.type}'"
                )

        return errors


# ---------------------------------------------------------------------------
# Definitions — the complete definition set for a version
# ---------------------------------------------------------------------------


@dataclass
class Definitions:
    """A complete set of definitions — entities, sources, and schemas.

    This is what gets registered as a version. Contains everything the
    framework needs to know about a domain: what entities exist, how events
    are normalized, and what events look like.

    Can be loaded from a directory (YAML files), constructed programmatically,
    or deserialized from a dict (database storage).
    """

    entities: dict[str, Entity] = field(default_factory=dict)
    """Entity definitions. Key = entity type name."""

    sources: dict[str, Source] = field(default_factory=dict)
    """Source definitions. Key = source name."""

    schemas: dict[str, Schema] = field(default_factory=dict)
    """Event schemas. Key = event type name."""

    @classmethod
    def from_directory(cls, path: str) -> Self:
        """Load definitions from a project directory.

        Expected structure:
            path/
              entities/    # one YAML per entity type
              sources/     # one YAML per source
              events/      # one YAML per event schema (optional)

        Each YAML file can contain one or more top-level keys (definitions).
        Missing subdirectories are fine — schemas are optional, and you might
        not have sources yet during development.

        Args:
            path: Path to the project directory.

        Raises:
            DefinitionError: If duplicate names are found across files.
        """
        entities: dict[str, Entity] = {}
        sources: dict[str, Source] = {}
        schemas: dict[str, Schema] = {}

        def _load_yaml_files(directory: str) -> list[dict[str, Any]]:
            """Load all YAML files from a directory, returning parsed dicts."""
            dir_path = Path(directory)
            if not dir_path.is_dir():
                return []
            results = []
            for filepath in sorted(dir_path.iterdir()):
                if filepath.suffix in (".yaml", ".yml"):
                    with open(filepath) as f:
                        data = yaml.safe_load(f)
                    if data:
                        results.append(data)
            return results

        # Load entities
        for file_data in _load_yaml_files(os.path.join(path, "entities")):
            for name, body in file_data.items():
                if name in entities:
                    raise DefinitionError(
                        f"Duplicate entity '{name}' found in {path}/entities/"
                    )
                entities[name] = Entity.from_dict({name: body})

        # Load sources
        for file_data in _load_yaml_files(os.path.join(path, "sources")):
            for name, body in file_data.items():
                if name in sources:
                    raise DefinitionError(
                        f"Duplicate source '{name}' found in {path}/sources/"
                    )
                sources[name] = Source.from_dict({name: body})

        # Load schemas from events/ directory
        for file_data in _load_yaml_files(os.path.join(path, "events")):
            for name, body in file_data.items():
                if name in schemas:
                    raise DefinitionError(
                        f"Duplicate schema '{name}' found in {path}/events/"
                    )
                schemas[name] = Schema.from_dict({name: body})

        return cls(entities=entities, sources=sources, schemas=schemas)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        """Create definitions from a parsed config dict.

        The dict uses the aggregation format where entity/source/schema names
        are keys, and values are the inner body dicts (no name wrapper):
            {'entities': {'customer': {body}, 'order': {body}},
             'sources': {'app': {body}},
             'schemas': {'customer_signup': {body}}}

        Args:
            data: Dict with 'entities', 'sources', and optionally 'schemas' keys.
        """
        entities = {
            name: Entity.from_dict({name: entity_body})
            for name, entity_body in data.get("entities", {}).items()
        }
        sources = {
            name: Source.from_dict({name: source_body})
            for name, source_body in data.get("sources", {}).items()
        }
        schemas = {
            name: Schema.from_dict({name: schema_body})
            for name, schema_body in data.get("schemas", {}).items()
        }
        return cls(entities=entities, sources=sources, schemas=schemas)

    def to_dict(self) -> dict[str, Any]:
        """Serialize all definitions to a dictionary.

        Returns the aggregation format: names as keys, inner body dicts as values.
        """
        d: dict[str, Any] = {}
        if self.entities:
            d["entities"] = {
                name: entity.to_dict()[name]
                for name, entity in self.entities.items()
            }
        if self.sources:
            d["sources"] = {
                name: source.to_dict()[name]
                for name, source in self.sources.items()
            }
        if self.schemas:
            d["schemas"] = {
                name: schema.to_dict()[name]
                for name, schema in self.schemas.items()
            }
        return d

    def to_yaml(self, path: str) -> None:
        """Write definitions to a directory as YAML files.

        Creates one file per definition in the appropriate subdirectory.

        Args:
            path: Output directory. Creates entities/, sources/, events/ subdirs.
        """
        for subdir, items, to_dict_fn in [
            ("entities", self.entities, lambda e: e.to_dict()),
            ("sources", self.sources, lambda s: s.to_dict()),
            ("events", self.schemas, lambda s: s.to_dict()),
        ]:
            if not items:
                continue
            dir_path = Path(path) / subdir
            dir_path.mkdir(parents=True, exist_ok=True)
            for name, obj in items.items():
                filepath = dir_path / f"{name}.yaml"
                with open(filepath, "w") as f:
                    yaml.dump(to_dict_fn(obj), f, default_flow_style=False, sort_keys=False)

    # Valid cardinality pairings — (source_cardinality, inverse_cardinality)
    _VALID_CARDINALITY_PAIRS = {
        ("has_many", "belongs_to"),
        ("has_one", "belongs_to"),
        ("belongs_to", "has_many"),
        ("belongs_to", "has_one"),
        ("many_to_many", "many_to_many"),
    }

    def validate(self) -> ValidationResult:
        """Validate all definitions — structural checks per type plus cross-definition consistency.

        Collects ALL errors rather than failing on the first. Returns a
        ValidationResult with errors (blocking) and warnings (informational).

        Can be called standalone for CI/linting, or automatically at registration time.
        """
        errors: list[str] = []
        warnings: list[str] = []

        # Structural validation — each type validates itself
        for entity in self.entities.values():
            errors.extend(entity.validate())
        for source in self.sources.values():
            errors.extend(source.validate())
        for schema in self.schemas.values():
            errors.extend(schema.validate())

        # Cross-definition checks
        self._validate_hints(errors)
        self._validate_relationships_cross(errors)
        self._validate_event_type_consistency(errors)
        self._validate_source_schema_fields(errors)
        self._collect_warnings(warnings)

        return ValidationResult(
            valid=len(errors) == 0, errors=errors, warnings=warnings,
        )

    def _validate_hints(self, errors: list[str]) -> None:
        """Source hints reference existing entities and their identity fields."""
        for source_name, source in self.sources.items():
            for event_name, event in source.events.items():
                for hint_entity, hint_fields in event.hints.items():
                    if hint_entity not in self.entities:
                        errors.append(
                            f"Source '{source_name}' event '{event_name}': "
                            f"hint references entity '{hint_entity}' which is not defined"
                        )
                    else:
                        entity = self.entities[hint_entity]
                        for hint_field in hint_fields:
                            if hint_field not in entity.identity.fields:
                                errors.append(
                                    f"Source '{source_name}' event '{event_name}': "
                                    f"hint field '{hint_field}' is not an identity field "
                                    f"on entity '{hint_entity}' "
                                    f"(identity fields: {sorted(entity.identity.fields.keys())})"
                                )

    def _validate_relationships_cross(self, errors: list[str]) -> None:
        """Relationship targets exist, both sides declared, cardinality pairing valid."""
        for entity_name, entity in self.entities.items():
            for rel in entity.relationships:
                if rel.target not in self.entities:
                    errors.append(
                        f"Entity '{entity_name}' relationship '{rel.type}': "
                        f"target entity '{rel.target}' is not defined"
                    )

                if ":" in rel.type:
                    errors.append(
                        f"Entity '{entity_name}' relationship '{rel.type}': "
                        f"type name contains ':' which is reserved as a key delimiter"
                    )

        # Check inverse relationships and cardinality pairing
        for entity_name, entity in self.entities.items():
            for rel in entity.relationships:
                if rel.target not in self.entities:
                    continue
                target_entity = self.entities[rel.target]
                inverse = next(
                    (r for r in target_entity.relationships if r.target == entity_name),
                    None,
                )
                if inverse is None:
                    errors.append(
                        f"Entity '{entity_name}' relationship '{rel.type}' → "
                        f"'{rel.target}' has no inverse. Entity '{rel.target}' "
                        f"must declare a relationship back to '{entity_name}'"
                    )
                else:
                    pair = (rel.cardinality, inverse.cardinality)
                    if pair not in self._VALID_CARDINALITY_PAIRS:
                        errors.append(
                            f"Relationship cardinality mismatch: "
                            f"'{entity_name}.{rel.type}' is '{rel.cardinality}' "
                            f"but '{rel.target}.{inverse.type}' is '{inverse.cardinality}'"
                        )

    def _validate_event_type_consistency(self, errors: list[str]) -> None:
        """Event types referenced in entity handlers should have schemas (if schemas exist).

        Also checks that source-produced event types have matching schemas.
        Only validates when schemas are defined — schemas are optional during development.
        """
        if not self.schemas:
            return

        # Collect all event types referenced in entity handlers
        entity_event_types: dict[str, list[str]] = {}  # event_type → [locations]
        for entity_name, entity in self.entities.items():
            for state_name, state in entity.states.items():
                for event_type in state.when:
                    entity_event_types.setdefault(event_type, []).append(
                        f"Entity '{entity_name}' state '{state_name}'"
                    )
            for event_type in entity.always:
                entity_event_types.setdefault(event_type, []).append(
                    f"Entity '{entity_name}' always"
                )

        # Entity handlers reference event types without schemas
        for event_type, locations in entity_event_types.items():
            if event_type not in self.schemas:
                errors.append(
                    f"Event type '{event_type}' is handled by {locations[0]} "
                    f"but has no schema defined"
                )

        # Source events should have matching schemas
        for source_name, source in self.sources.items():
            for event_name in source.events:
                if event_name not in self.schemas:
                    errors.append(
                        f"Source '{source_name}' produces event '{event_name}' "
                        f"but no schema is defined for it"
                    )

    def _validate_source_schema_fields(self, errors: list[str]) -> None:
        """Source mappings produce the fields that schemas declare, and vice versa.

        Only checks source→schema pairs where both exist. Skips if no schemas defined.
        """
        if not self.schemas:
            return

        for source_name, source in self.sources.items():
            for event_name, event in source.events.items():
                if event_name not in self.schemas:
                    continue  # already reported by _validate_event_type_consistency

                schema = self.schemas[event_name]
                produced_fields = set(event.mappings.keys())
                declared_fields = set(schema.fields.keys())

                # Schema requires fields that the source doesn't produce
                for field_name, field in schema.fields.items():
                    if field.required and field_name not in produced_fields:
                        errors.append(
                            f"Schema '{event_name}' requires field '{field_name}' "
                            f"but source '{source_name}' mapping doesn't produce it"
                        )

                # Source produces fields not declared in the schema
                for field_name in produced_fields:
                    if field_name not in declared_fields:
                        errors.append(
                            f"Source '{source_name}' mapping for '{event_name}' "
                            f"produces field '{field_name}' not declared in schema"
                        )

    def _collect_warnings(self, warnings: list[str]) -> None:
        """Non-blocking warnings — valid but suspicious patterns.

        Uses Entity.state_graph() for graph analysis (same graph used by
        _validate_lifecycle for reachability). Checks for patterns that
        aren't errors but suggest the user should double-check.
        """
        for entity_name, entity in self.entities.items():
            prefix = f"Entity '{entity_name}'"

            # --- Property checks ---

            self._warn_property_issues(entity, prefix, warnings)

            # --- State machine graph analysis ---

            graph = entity.state_graph()
            self._warn_dead_end_states(entity, graph, prefix, warnings)
            self._warn_unused_event_types(entity, entity_name, prefix, warnings)
            self._warn_write_never_read(entity, prefix, warnings)

    def _warn_property_issues(
        self, entity: Entity, prefix: str, warnings: list[str],
    ) -> None:
        """Properties with treatment but no sensitivity label."""
        for prop_name, prop in entity.properties.items():
            if prop.treatment and not prop.sensitive:
                warnings.append(
                    f"{prefix} property '{prop_name}': "
                    f"has treatment '{prop.treatment}' but no sensitivity label — "
                    f"consider adding 'sensitive' (pii, phi, pci)"
                )

    def _warn_dead_end_states(
        self, entity: Entity, graph: dict[str, set[str]],
        prefix: str, warnings: list[str],
    ) -> None:
        """States with no outgoing transitions.

        Terminal states are valid (churned, closed, delivered) but worth
        flagging so the user confirms it's intentional.
        """
        for state_name, targets in graph.items():
            has_handlers = bool(entity.states[state_name].when)
            has_time_rules = bool(entity.states[state_name].after)
            if not targets and not has_handlers and not has_time_rules and state_name != entity.starts:
                warnings.append(
                    f"{prefix}: state '{state_name}' is a dead-end — "
                    f"no event handlers or time rules lead out of it"
                )

    def _warn_unused_event_types(
        self, entity: Entity, entity_name: str,
        prefix: str, warnings: list[str],
    ) -> None:
        """Event types in sources that no state handles.

        These events would be silently ignored (no handler match → no-op).
        May indicate a typo or missing handler.
        """
        all_handled: set[str] = set()
        for state in entity.states.values():
            all_handled.update(state.when.keys())
        all_handled.update(entity.always.keys())

        for source_name, source in self.sources.items():
            for event_name, event in source.events.items():
                if entity_name in event.hints and event_name not in all_handled:
                    warnings.append(
                        f"{prefix}: event type '{event_name}' "
                        f"(source '{source_name}') has identity hints for "
                        f"this entity but no state handles it — events will "
                        f"be silently ignored"
                    )

    def _warn_write_never_read(
        self, entity: Entity, prefix: str, warnings: list[str],
    ) -> None:
        """Properties set by effects but never referenced in guards or compute.

        May be intentional (used in downstream queries only) but worth
        flagging. Skips identity fields (read via resolution, not guards).
        """
        written: set[str] = set()
        read: set[str] = set()

        for state in entity.states.values():
            for handler in state.when.values():
                if handler.guard:
                    for prop_name in entity.properties:
                        if f"entity.{prop_name}" in handler.guard:
                            read.add(prop_name)
                for effect in handler.effects:
                    if effect.kind in ("set", "increment") and effect.property:
                        written.add(effect.property)
                    if effect.condition:
                        for prop_name in entity.properties:
                            if f"entity.{prop_name}" in effect.condition:
                                read.add(prop_name)

        for prop_name, prop in entity.properties.items():
            if prop.compute:
                for other_name in entity.properties:
                    if f"entity.{other_name}" in prop.compute:
                        read.add(other_name)

        for prop_name in sorted(written - read):
            if prop_name not in entity.identity.fields:
                warnings.append(
                    f"{prefix} property '{prop_name}': set by effects but never "
                    f"referenced in guards or compute expressions — may be "
                    f"intentional if used in queries"
                )
