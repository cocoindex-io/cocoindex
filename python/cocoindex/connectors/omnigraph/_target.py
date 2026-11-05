"""Omnigraph target connector: schemas, handlers, sink, and user-facing targets."""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import datetime
import functools
import hashlib
import json
import re
import types
import typing
import uuid
import weakref
from collections.abc import AsyncIterator, Callable, Sequence
from typing import Any, Generic, Literal, NamedTuple

import cocoindex as coco
import msgspec
import omnigraph as _og
from cocoindex._internal.component_ctx import current_app_name
from cocoindex._internal.context_keys import ContextProvider
from cocoindex._internal.datatype import TypeChecker
from cocoindex._internal.memo_fingerprint import canonical_module_name
from cocoindex.connectorkits import statediff
from cocoindex.connectorkits.fingerprint import fingerprint_object
from cocoindex.connectorkits.target import ManagedBy
from cocoindex.connectors.omnigraph._client import ConnectionFactory, _GraphClient
from cocoindex.connectors.omnigraph._cluster import ClusterConfig, SchemaDeployError
from cocoindex.connectors.omnigraph._gq import (
    PropertyValue,
    Query,
    Statement,
    _find_type_block,
    build_edge_delete,
    build_edge_insert,
    build_endpoint_stub,
    build_incident_edges_query,
    build_node_delete,
    build_node_upsert,
    edge_keyed_on_endpoints,
    edge_types_referencing,
    incident_edge_patterns,
    merge_type_into_schema,
    ownership_marker,
    ownership_property,
    release_ownership,
    remove_type_from_schema,
    render_edge_type,
    render_node_type,
    render_query,
    validate_identifier,
    validate_pg_type,
)
from typing_extensions import TypeVar


def derive_coco_key(parts: object) -> str:
    """Stable value for the synthetic `coco_key` property.

    Omnigraph gives edges no settable id, its auto-assigned ULID is not
    filterable, and `where` accepts exactly one equality predicate — so the
    connector declares `coco_key` on every type it manages and addresses
    entities through it. This computes what goes in there.

    Fingerprints the key tuple rather than concatenating into a string:
    `f"{a}_{b}"` collides whenever parts contain underscores, silently merging
    two distinct entities onto one target state.
    """
    return fingerprint_object(parts).hex()


ValueEncoder = Callable[[Any], Any]


class OmnigraphType(NamedTuple):
    """Annotation overriding the default Python-to-`.pg` type mapping.

    Use as ``Annotated[int, OmnigraphType("I32")]`` when the default (``I64``)
    is wider than the schema should declare.
    """

    pg_type: str


class PropertyDef(NamedTuple):
    name: str
    pg_type: str
    #: Applied to non-`None` values before they are sent. `None` means the
    #: built-in encoding for `pg_type` (ISO format for `Date`/`DateTime`,
    #: mapped over a list of either), so a hand-built definition behaves
    #: like one `NodeSchema.from_class` produced.
    encoder: ValueEncoder | None = None


_SCALARS: dict[Any, str] = {
    str: "String",
    bool: "Bool",
    int: "I64",
    float: "F64",
    datetime.date: "Date",
    datetime.datetime: "DateTime",
    # `bytes` deliberately has no mapping: Omnigraph's `Blob` is an external
    # URI reference the engine *fetches* (a `file://` value), not inline
    # bytes — a Python `bytes` value can never be a `Blob`, so it falls
    # through to the generic "no mapping" TypeError below instead of being
    # silently declared as one.
}


def _isoformat(value: Any) -> str:
    return value.isoformat()  # type: ignore[no-any-return]


def _isoformat_milliseconds(value: Any) -> str:
    return value.isoformat(timespec="milliseconds")  # type: ignore[no-any-return]


#: `PropertyDef.encoder` for the scalar `.pg` types whose Python value isn't
#: `json.dumps`-safe as-is. Verified against the 0.14 engine: `Date` accepts
#: `"2026-01-01"` only (a time of day is refused), and `DateTime` accepts ISO
#: with or without an offset but refuses non-zero digits past the third
#: fractional one — so a datetime is cut to milliseconds, the precision the
#: engine stores anyway.
_ENCODERS: dict[str, ValueEncoder] = {
    "Date": _isoformat,
    "DateTime": _isoformat_milliseconds,
}

#: Encoders a key property of each `.pg` type may carry: the built-in one
#: for that type, and the `isoformat` method a person writing a schema by
#: hand would reach for, which is the same encoding. Keyed by type, not one
#: flat list: `datetime.date.isoformat` on a DateTime key drops the time
#: from every value, so two instants on one date were stored as a single
#: midnight node while tracked as two, and removing one declaration deleted
#: the node the other still declared. Anything else changes what the graph
#: keys on and is refused at `node_target`.
_KEY_ENCODERS: dict[str, tuple[ValueEncoder, ...]] = {
    "Date": (_ENCODERS["Date"], datetime.date.isoformat),
    "DateTime": (_ENCODERS["DateTime"], datetime.datetime.isoformat),
}


@functools.cache
def _list_encoder(element: ValueEncoder) -> ValueEncoder:
    # Cached so `_encoder_for` hands back one object per element encoder:
    # `_encoder_identity` tells the built-in encoding of a type from a custom
    # one by identity.
    return lambda values: [element(v) for v in values]


def _encoder_for(pg_type: str) -> ValueEncoder | None:
    """Look up by the *resolved* pg_type string (stripped of `?`), not the
    Python annotation — this way it applies the same whether the field came
    from a plain `datetime.date` or an `Annotated[..., OmnigraphType(...)]`
    override that happens to resolve to `Date`/`DateTime`.

    A list type `[T]` gets its element's encoder mapped over the list: the
    schema accepts `[Date]`, and without this every write of such a value
    died in `json.dumps` while the type itself had been declared fine.
    """
    base = pg_type.rstrip("?")
    if base.startswith("[") and base.endswith("]"):
        element = _ENCODERS.get(base[1:-1])
        return _list_encoder(element) if element is not None else None
    return _ENCODERS.get(base)


def _pg_type_for(annotation: Any) -> str:
    """Map a Python annotation to a `.pg` type, honouring OmnigraphType.

    ``bool`` is checked before ``int`` because ``bool`` is a subclass of
    ``int`` and would otherwise map to ``I64``.

    ``OmnigraphType`` lets the app author write an arbitrary `pg_type`
    string, and that string is spliced directly into generated query text
    downstream — so every value this function returns, on every path
    (including the OmnigraphType override), goes through
    `_gq.validate_pg_type` before it reaches the caller.
    """
    origin = typing.get_origin(annotation)
    if origin is typing.Annotated:
        base, *meta = typing.get_args(annotation)
        for m in meta:
            if isinstance(m, OmnigraphType):
                validate_pg_type(m.pg_type)
                return m.pg_type
        return _pg_type_for(base)
    if origin in (typing.Union, types.UnionType):
        args = [a for a in typing.get_args(annotation) if a is not type(None)]
        if len(args) == 1:
            inner = _pg_type_for(args[0])
            pg_type = inner if inner.endswith("?") else f"{inner}?"
            validate_pg_type(pg_type)
            return pg_type
        raise TypeError(f"no Omnigraph type mapping for union {annotation!r}")
    if origin in (list, Sequence):
        (item,) = typing.get_args(annotation)
        inner = _pg_type_for(item)
        # Verified against the engine (2026-08-26): a list element must be a
        # plain scalar. `[String?]` and `[I64?]` fail with "expected
        # core_type"; `[[String]]` fails with "expected base_type". Emitting
        # them would produce a `.pg` the engine rejects at init time, so fail
        # here with a message that names the offending annotation.
        if inner.endswith("?") or inner.startswith("["):
            raise TypeError(
                f"Omnigraph list elements must be non-nullable scalars; "
                f"{annotation!r} maps to [{inner}], which the engine rejects"
            )
        pg_type = f"[{inner}]"
        validate_pg_type(pg_type)
        return pg_type
    # bool is a subclass of int, so it must be checked before int; dict
    # lookup on the exact annotation naturally does this since `_SCALARS`
    # keys on the type object itself, not an isinstance chain.
    if annotation in _SCALARS:
        pg_type = _SCALARS[annotation]
        validate_pg_type(pg_type)
        return pg_type
    raise TypeError(f"no Omnigraph type mapping for {annotation!r}")


def _properties_from_class(target: type) -> dict[str, PropertyDef]:
    """Introspect a dataclass into `PropertyDef`s, one per field, in order."""
    hints = typing.get_type_hints(target, include_extras=True)
    properties: dict[str, PropertyDef] = {}
    for field in dataclasses.fields(target):
        validate_identifier(field.name, "property name")
        # `id` is the one name Omnigraph reserves whether this schema ends
        # up describing a node or an edge, and it's by far the likeliest
        # collision in a real dataclass — so it's worth catching here,
        # where the class name is in scope. The name-set that depends on
        # which kind of type this becomes (an edge's `src`/`dst`/`from`/
        # `to`) is enforced by `_gq.render_edge_type` instead.
        if field.name == "id":
            raise ValueError(
                f"{target.__name__}.id: 'id' is reserved by Omnigraph for "
                f"the engine's own identity column and cannot be declared "
                f"as a property. Rename the field (e.g. "
                f"'{target.__name__.lower()}_id')."
            )
        pg_type = _pg_type_for(hints[field.name])
        properties[field.name] = PropertyDef(field.name, pg_type, _encoder_for(pg_type))
    return properties


@dataclasses.dataclass(frozen=True)
class NodeSchema:
    """A node type's properties and the one of them that is its `@key`."""

    properties: dict[str, PropertyDef]
    key: tuple[str, ...]

    @classmethod
    async def from_class(cls, target: type, *, key: str | Sequence[str]) -> NodeSchema:
        key_tuple = (key,) if isinstance(key, str) else tuple(key)
        if not key_tuple:
            raise ValueError(
                f"{target.__name__} needs at least one key property: Omnigraph "
                f"upserts by key on a keyed node `insert`, but an unkeyed "
                f"`insert` is a strict insert — every re-run would duplicate "
                f"every node"
            )
        if len(key_tuple) > 1:
            raise ValueError(
                f"{target.__name__} declares a composite key {key_tuple!r}, but "
                f"Omnigraph node types support exactly one @key property — the "
                f"engine rejects a two-@key schema outright. Derive a single "
                f"key field (e.g. a generated id) and key on that."
            )
        properties = _properties_from_class(target)
        for k in key_tuple:
            if k not in properties:
                raise ValueError(
                    f"key property {k!r} is not a field of {target.__name__}"
                )
            if properties[k].pg_type.endswith("?"):
                raise ValueError(
                    f"key property {k!r} of {target.__name__} must not be nullable"
                )
        return cls(properties=properties, key=key_tuple)

    def render(self, type_name: str, *, owner: str) -> str:
        return render_node_type(
            type_name,
            [(p.name, p.pg_type) for p in self.properties.values()],
            key=self.key,
            owner=owner,
        )


@dataclasses.dataclass(frozen=True)
class EdgeSchema:
    """An edge type's own properties.

    An edge has no key of its own — its identity is always `(from_id,
    to_id)` — so unlike `NodeSchema` there is none to declare.
    """

    properties: dict[str, PropertyDef]

    @classmethod
    async def from_class(cls, target: type) -> EdgeSchema:
        return cls(properties=_properties_from_class(target))


# ---------------------------------------------------------------------------
# Container handlers: a node type or edge type, and how its schema evolves
# ---------------------------------------------------------------------------


class _TypeKey(NamedTuple):
    """A managed type's tracking identity.

    `type_kind` is part of it because a `.pg` may legally declare `node Link`
    and `edge Link` side by side — they are different types, and the sink has
    to know which block it is editing.

    Deliberately does NOT carry the branch (or the store URI): both live on
    the `ConnectionFactory`, which is resolved from `db_key` at action time,
    never captured at declare time — a delete action runs in a process where
    the declaring code never executed. Same split the sibling connectors
    make: neo4j's `_TableKey` is `(db_key, table_name)` and keeps the
    database name on the factory. Point two apps at different branches by
    giving them different `ContextKey`s.
    """

    db_key: str
    type_kind: str  # "node" | "edge"
    type_name: str


_TYPE_KEY_CHECKER: TypeChecker[tuple[str, str, str]] = TypeChecker(tuple[str, str, str])


@dataclasses.dataclass(frozen=True)
class _TypeSpec:
    schema: NodeSchema | EdgeSchema | None
    key: tuple[str, ...]
    from_type: str | None
    to_type: str | None
    managed_by: ManagedBy
    #: The app declaring the type (`AppConfig.name`). Rendered into the
    #: block as its ownership property, and persisted in the tracking
    #: record so a removal — which runs from tracking alone — knows whose
    #: blocks it may take along.
    owner: str
    # Populated only for edge specs: the *endpoint* node types' own key
    # definitions. Needed by the sink to build endpoint stubs and the edge's
    # `from`/`to` refs — `from_type`/`to_type` name the endpoint types, but
    # nothing else here carries their key definitions, since `schema` above is
    # the edge's own (possibly absent) property schema, not the endpoints'.
    from_key_property: PropertyDef | None = None
    to_key_property: PropertyDef | None = None


class _TypeMainRecord(msgspec.Struct, frozen=True, array_like=True):
    """A managed type's identity — a change here forces a full rebuild.

    Deliberately does NOT carry neo4j's `has_schema` flag.
    `_EdgeTypeHandler._render` emits a byte-identical fragment for
    `schema=None` and for an empty schema, so tracking that distinction would
    promote `None` ↔ `{}` — a difference the graph cannot observe — into a
    destructive drop-and-recreate.
    """

    key: tuple[str, ...]
    from_type: str | None
    to_type: str | None
    #: Constant for every record in one app's store — the store is the
    #: app's — so it never differs between records and never forces a
    #: rebuild; it is here because a removal has nothing else to read it
    #: from.
    owner: str


class _PropertyRecord(msgspec.Struct, frozen=True, array_like=True):
    """One property as the type's tracking record remembers it.

    `pg_type` is the bare `.pg` type string: nullability is already spelled
    in it as a trailing `?`, so neo4j's `(type, nullable)` split would only
    restate the same bit.

    `encoder` is `_encoder_identity` of the property's encoder. The rendered
    `.pg` never changes with an encoder, so without this a changed encoder
    was invisible at the type level: reconciling each node re-encodes its
    values and would notice, but a memoized declaring component is skipped
    whole and never reconciles anything. A change here is a "lossy"
    invalidation, which also invalidates every memo that declared into this
    type, so its component runs again and its rows are re-upserted.
    """

    pg_type: str
    encoder: str


def _code_digest(code: types.CodeType) -> str:
    consts = tuple(
        _code_digest(c) if isinstance(c, types.CodeType) else repr(c)
        for c in code.co_consts
    )
    payload = repr((code.co_code, consts, code.co_names)).encode()
    return hashlib.sha256(payload).hexdigest()[:16]


def _encoder_identity(prop_def: PropertyDef) -> str:
    """A stable name for what `prop_def.encoder` does, across runs.

    Empty for the built-in encoding of the property's type, whether the
    definition left `encoder` unset or names the same function `from_class`
    fills in — the two are one encoding. Otherwise the callable's qualified
    name, with a digest of its code for a Python function, so editing a
    lambda's body counts as a change the way swapping `str.lower` for
    `str.upper` does. Closed-over values are not part of it, as with a
    `@coco.fn` body's own helpers.
    """
    encoder = prop_def.encoder
    if encoder is None or encoder is _encoder_for(prop_def.pg_type):
        return ""
    module = (
        canonical_module_name(encoder)
        if hasattr(encoder, "__module__")
        else type(encoder).__module__
    )
    qualname = getattr(encoder, "__qualname__", None) or type(encoder).__qualname__
    code = getattr(encoder, "__code__", None)
    if not isinstance(code, types.CodeType):
        return f"{module}.{qualname}"
    return f"{module}.{qualname}#{_code_digest(code)}"


_PROPERTY_SUBKEY_PREFIX = "prop:"


def _property_subkey(name: str) -> str:
    return f"{_PROPERTY_SUBKEY_PREFIX}{name}"


def _property_name(subkey: str) -> str:
    return subkey[len(_PROPERTY_SUBKEY_PREFIX) :]


#: Identity in `main`, one entry per property in `sub`.
_TypeTrackingRecord = statediff.MutualTrackingRecord[
    statediff.CompositeTrackingRecord[_TypeMainRecord, str, _PropertyRecord]
]


def _type_tracking_record_from_spec(spec: _TypeSpec) -> _TypeTrackingRecord:
    """Build the tracking record for `spec`, already wrapped in
    `MutualTrackingRecord`.

    Returns the *wrapped* record where neo4j's equivalent returns the bare
    composite and wraps at the call site. Every caller here wants the wrapped
    form, and `managed_by` riding along on the persisted record is the whole
    point: it is what the removal path consults to decide it must not drop.
    """
    sub = (
        {
            _property_subkey(p.name): _PropertyRecord(p.pg_type, _encoder_identity(p))
            for p in spec.schema.properties.values()
        }
        if spec.schema is not None
        else {}
    )
    return statediff.MutualTrackingRecord(
        tracking_record=statediff.CompositeTrackingRecord(
            main=_TypeMainRecord(
                key=spec.key,
                from_type=spec.from_type,
                to_type=spec.to_type,
                owner=spec.owner,
            ),
            sub=sub,
        ),
        managed_by=spec.managed_by,
    )


class _TypeAction(NamedTuple):
    key: _TypeKey
    # The desired spec, carried through so `_type_child_handler` can build the
    # `_NodeHandler`/`_EdgeHandler` for this type's children from the action
    # alone. `NON_EXISTENCE` when the type is being dropped; the engine never
    # asks the factory for a deleted container.
    spec: _TypeSpec | coco.NonExistenceType
    pg_fragment: str | None
    main_action: statediff.DiffAction | None
    property_actions: dict[str, statediff.DiffAction]
    #: The app the action belongs to: whose ownership property the block
    #: carries, and the only app whose edge types a node drop takes along.
    owner: str
    # A `managed_by=user` declaration of a type the connector created: the
    # block's ownership property is stale and has to be removed, or a later
    # drop of a node type it references takes the user's type along.
    release_ownership: bool = False


_ChildInvalidation = Literal["destructive", "lossy"] | None


def _reconcile_removal(
    key: _TypeKey,
    prev_possible_records: typing.Collection[_TypeTrackingRecord],
    prev_may_be_missing: bool,
) -> coco.TargetReconcileOutput[_TypeAction, _TypeTrackingRecord, Any] | None:
    """Reconcile a type the app no longer declares.

    `resolve_system_transition` returns `None` when nothing is tracked and
    when any previous record is user-managed: the connector did not create a
    `managed_by=user` type and does not get to delete it, since dropping the
    block takes every row in it along. That is why the tracking record
    carries `managed_by`.
    """
    main_action, _ = statediff.diff_composite(
        statediff.resolve_system_transition(
            statediff.TrackingRecordTransition(
                coco.NON_EXISTENCE, prev_possible_records, prev_may_be_missing
            )
        )
    )
    if main_action is None:
        # `None` is safe only for a type that is gone. The engine builds a
        # type's child handler only when its `reconcile` returns an output,
        # so `reconcile` below returns one for every type that exists, even
        # unchanged (as sqlite's `_TableHandler` does), and
        # `_apply_type_schema` skips the write for an unchanged type.
        return None
    # Every record in the store is this app's, so any of them names it.
    owner = next(iter(prev_possible_records)).tracking_record.main.owner
    return coco.TargetReconcileOutput(
        action=_TypeAction(
            key=key,
            spec=coco.NON_EXISTENCE,
            pg_fragment=None,
            main_action=main_action,
            property_actions={},
            owner=owner,
        ),
        sink=_type_sink(key.db_key),
        tracking_record=coco.NON_EXISTENCE,
        child_invalidation="destructive",
    )


#: The handler type of a type's children: `_NodeHandler` for a node type,
#: `_EdgeHandler` for an edge type.
_ChildHandlerT = typing.TypeVar(
    "_ChildHandlerT", bound="_NodeHandler | _EdgeHandler", covariant=True
)


class _TypeHandlerBase(
    coco.TargetHandler[_TypeSpec, _TypeTrackingRecord, _ChildHandlerT]
):
    def _render(self, key: _TypeKey, spec: _TypeSpec) -> str:
        raise NotImplementedError  # pragma: no cover - each handler overrides it

    def reconcile(
        self,
        key: coco.StableKey,
        desired_target_state: _TypeSpec | coco.NonExistenceType,
        prev_possible_records: typing.Collection[_TypeTrackingRecord],
        prev_may_be_missing: bool,
        /,
    ) -> (
        coco.TargetReconcileOutput[_TypeAction, _TypeTrackingRecord, _ChildHandlerT]
        | None
    ):
        key = _TypeKey(*_TYPE_KEY_CHECKER.check(key))
        if coco.is_non_existence(desired_target_state):
            return _reconcile_removal(key, prev_possible_records, prev_may_be_missing)

        spec = desired_target_state
        desired = _type_tracking_record_from_spec(spec)

        # --- The write path: what actually has to be applied. ---
        # A SYSTEM declaration owns the desired schema, including when the
        # previous declaration was USER-managed. Keep those previous schemas
        # in the diff: filtering them out makes a simultaneous ownership and
        # schema change look like an already-converged empty transition, then
        # persists the new tracking record without ever applying its DDL.
        # USER declarations still emit no schema action at all.
        write_transition = (
            statediff.TrackingRecordTransition(
                desired.tracking_record,
                [p.tracking_record for p in prev_possible_records],
                prev_may_be_missing,
            )
            if spec.managed_by == ManagedBy.SYSTEM
            else None
        )
        main_action, property_transitions = statediff.diff_composite(write_transition)
        property_actions: dict[str, statediff.DiffAction] = {}
        if main_action is None:
            # `_apply_type_schema` re-renders and re-issues this type's whole
            # fragment on every write, so once a main action is scheduled the
            # per-property actions are already subsumed by it. This is neo4j's
            # `if main_action is None`, deliberately not sqlite's
            # `in (None, "upsert")` — sqlite needs the wider gate because it
            # emits per-column DDL, and we never do.
            for sub_key, transition in property_transitions.items():
                action = statediff.diff(transition)
                if action is not None:
                    property_actions[sub_key] = action

        # The reverse handoff. A SYSTEM declaration after a USER-managed
        # record has to re-render the block even when the schema is otherwise
        # unchanged: the release dropped `coco_managed_by_<app>`, and until it is back
        # a drop of a node type this type references treats it as the user's
        # and refuses.
        if (
            main_action is None
            and not property_actions
            and spec.managed_by == ManagedBy.SYSTEM
            and any(p.managed_by == ManagedBy.USER for p in prev_possible_records)
        ):
            main_action = "upsert"

        # --- The tracked path: what CocoIndex has actually seen before. ---
        # The same records diffed as if `prev` were complete and ignoring
        # ownership, for the guard and the child invalidation below. Not the
        # write path: under `prev_may_be_missing` its main action is "upsert",
        # which leaves no property actions for the guard to see.
        tracked_main, tracked_transitions = statediff.diff_composite(
            statediff.TrackingRecordTransition(
                desired.tracking_record,
                [p.tracking_record for p in prev_possible_records],
                False,
            )
        )
        tracked_actions: dict[str, statediff.DiffAction] = {}
        if tracked_main is None:
            for sub_key, transition in tracked_transitions.items():
                action = statediff.diff(transition)
                if action is not None:
                    tracked_actions[sub_key] = action

        # A USER-managed type's schema is the user's, so it is never checked
        # here. `!=`, not `is not`: `ManagedBy` is a StrEnum and a plain
        # `"user"` must count.
        if spec.managed_by != ManagedBy.USER and tracked_main is None:
            # The engine refuses to add a non-nullable property to an existing
            # type (OG-MF-103, "requires a backfill and is not supported in
            # schema migration v1"; verified). Refuse here, naming the
            # property, rather than with the engine's error; never escalate to
            # dropping a populated type.
            #
            # "upsert" counts as an addition alongside "insert": it means at
            # least one candidate previous state lacked the property, and we
            # don't get to pick which candidate is the engine's real state.
            desired_sub = desired.tracking_record.sub
            non_nullable_adds = sorted(
                _property_name(sub_key)
                for sub_key, action in tracked_actions.items()
                if action in ("insert", "upsert")
                and not desired_sub[sub_key].pg_type.endswith("?")
            )
            if non_nullable_adds:
                raise ValueError(
                    f"Cannot add non-nullable propert"
                    f"{'y' if len(non_nullable_adds) == 1 else 'ies'} "
                    f"{non_nullable_adds!r} to existing Omnigraph type: the engine "
                    f"only accepts non-nullable properties at initial creation. "
                    f"Make them optional (e.g. `str | None`), or drop and recreate "
                    f"the type deliberately."
                )

        child_invalidation: _ChildInvalidation = None
        if tracked_main == "replace":
            child_invalidation = "destructive"
        elif tracked_main is None and any(
            a != "insert" for a in tracked_actions.values()
        ):
            # A dropped or retyped property — and, after an interrupted update,
            # one that only some candidate previous states carry — re-issues
            # the whole fragment, so every child must re-upsert defensively.
            child_invalidation = "lossy"

        # Handing a type the connector created to the user is the one thing
        # a USER declaration writes: the block keeps its ownership property
        # otherwise, and a later drop of a node type it references reads
        # that as current ownership and removes the user's type with it.
        release_ownership = spec.managed_by == ManagedBy.USER and any(
            p.managed_by == ManagedBy.SYSTEM for p in prev_possible_records
        )

        return coco.TargetReconcileOutput(
            action=_TypeAction(
                key=key,
                spec=spec,
                pg_fragment=self._render(key, spec),
                main_action=main_action,
                property_actions=property_actions,
                owner=spec.owner,
                release_ownership=release_ownership,
            ),
            sink=_type_sink(key.db_key),
            tracking_record=desired,
            child_invalidation=child_invalidation,
        )


class _NodeTypeHandler(_TypeHandlerBase["_NodeHandler"]):
    def _render(self, key: _TypeKey, spec: _TypeSpec) -> str:
        assert isinstance(spec.schema, NodeSchema)
        return spec.schema.render(key.type_name, owner=spec.owner)


class _EdgeTypeHandler(_TypeHandlerBase["_EdgeHandler"]):
    def _render(self, key: _TypeKey, spec: _TypeSpec) -> str:
        assert spec.from_type is not None and spec.to_type is not None
        props = (
            [(p.name, p.pg_type) for p in spec.schema.properties.values()]
            if spec.schema is not None
            else []
        )
        return render_edge_type(
            key.type_name, spec.from_type, spec.to_type, props, owner=spec.owner
        )


# ---------------------------------------------------------------------------
# Child handlers: individual nodes and edges within a type
# ---------------------------------------------------------------------------


class _EntityTrackingRecord(msgspec.Struct, frozen=True, array_like=True):
    """Fingerprint of the *encoded* property values: what the graph holds, so
    a changed `PropertyDef.encoder` is a change. A fingerprint rather than the
    values, and `array_like`, because there is one per node and per edge."""

    fingerprint: bytes


class _NodeValue(NamedTuple):
    properties: dict[str, Any]


class _EdgeValue(NamedTuple):
    from_id: Any
    to_id: Any
    properties: dict[str, Any]


class _NodeAction(NamedTuple):
    op: str  # "upsert" | "delete"
    key: _TypeKey
    type_name: str
    properties: Sequence[PropertyValue]
    coco_key: str
    #: On a delete: the key property in its transport encoding, so the sink
    #: can turn the delete into a key-only upsert when an edge outside the
    #: batch still references the node (see `_keep_referenced_nodes`).
    key_properties: tuple[PropertyValue, ...] = ()


class _EdgeAction(NamedTuple):
    op: str  # "upsert" | "delete"
    key: _TypeKey
    type_name: str
    coco_key: str
    from_id: Any
    to_id: Any
    properties: Sequence[PropertyValue]
    from_type: str
    to_type: str
    from_key_property: PropertyDef
    to_key_property: PropertyDef


def _entity_record(encoded: Sequence[PropertyValue]) -> _EntityTrackingRecord:
    return _EntityTrackingRecord(
        fingerprint=fingerprint_object({p.name: p.value for p in encoded})
    )


def _needs_write(
    desired: _EntityTrackingRecord,
    prev_possible_records: typing.Collection[_EntityTrackingRecord],
    prev_may_be_missing: bool,
) -> bool:
    if prev_may_be_missing or not prev_possible_records:
        return True
    return not all(p.fingerprint == desired.fingerprint for p in prev_possible_records)


def _encode_properties(
    properties: dict[str, Any], property_defs: dict[str, PropertyDef]
) -> tuple[PropertyValue, ...]:
    """Resolve a raw `{name: value}` dict into typed, encoded `PropertyValue`s.

    Builders need each property's `pg_type` (to declare it in the query
    signature), and `json.dumps` fails outright on `datetime.date`/
    `datetime.datetime` — both are `PropertyDef` concerns, so this is done
    here, where the schema is in scope, rather than in `plan_commits`, which
    is pure and has no schema to consult.
    """
    out = []
    for name, value in properties.items():
        prop_def = property_defs[name]
        out.append(_encode_property(prop_def, value))
    return tuple(out)


_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.UTC)


def _tracking_key_value(prop_def: PropertyDef, value: Any) -> Any:
    """The value a key property is tracked by: the graph's own identity.

    Omnigraph identifies a `DateTime`-keyed node by the instant, as epoch
    milliseconds — `12:00+00:00` and `14:00+02:00` are one node, precision
    below a millisecond is dropped, and a naive value is read as UTC (all
    verified against the engine). Tracking by the ISO spelling gave one
    graph node two tracking keys; removing one declaration then deleted the
    node the other still declared. Every other key is tracked by the
    encoded value the graph is sent, which is what it keys on.

    The instant can only be derived from a `datetime`, so a `DateTime` key
    accepts nothing else: a hand-built `PropertyDef("at", "DateTime")` lets
    a dict row carry an ISO string, and a string tracked by its spelling
    reopens the two-keys-one-node hole. A `Date` key likewise takes a
    `datetime.date` — and not a `datetime`, whose ISO form carries a time.
    """
    base = prop_def.pg_type.rstrip("?")
    if base == "DateTime":
        if not isinstance(value, datetime.datetime):
            raise TypeError(
                f"Omnigraph key property {prop_def.name!r} is a DateTime: pass a "
                f"`datetime.datetime`, not {type(value).__name__}. The graph "
                f"identifies the node by the instant, which is derived from the "
                f"datetime; a string would be tracked by its spelling, and two "
                f"spellings of one instant would be two tracking keys for one node."
            )
        if value.tzinfo is None:
            value = value.replace(tzinfo=datetime.UTC)
        return (value - _EPOCH) // datetime.timedelta(milliseconds=1)
    if base == "Date" and (
        not isinstance(value, datetime.date) or isinstance(value, datetime.datetime)
    ):
        raise TypeError(
            f"Omnigraph key property {prop_def.name!r} is a Date: pass a "
            f"`datetime.date`, not {type(value).__name__}. The key is tracked by "
            f"the date's ISO form, which only a `date` yields exactly."
        )
    return _encode_property(prop_def, value).value


def _transport_key_value(prop_def: PropertyDef, tracked: Any) -> Any:
    """The transport form of a key value the engine handed back as a
    StableKey. Every tracking key is already the value the graph is sent —
    except a `DateTime` key, tracked by its instant in epoch milliseconds,
    which goes back out as ISO."""
    if prop_def.pg_type.rstrip("?") == "DateTime":
        return (_EPOCH + datetime.timedelta(milliseconds=tracked)).isoformat()
    return tracked


def _encode_property(prop_def: PropertyDef, value: Any) -> PropertyValue:
    """Encode `value` for the transport: the definition's own encoder, or —
    when it sets none — the built-in encoding for its `.pg` type.

    The built-in encoding follows the type, not the definition: a hand-built
    `PropertyDef("at", "DateTime")` has no encoder set, and keying on that
    field alone sent a raw `datetime` to `json.dumps` and a raw `date` into
    the engine's StableKey, while the same schema inferred from a dataclass
    worked because `from_class` filled the encoder in.
    """
    encoder = prop_def.encoder
    if encoder is None:
        encoder = _encoder_for(prop_def.pg_type)
    if value is not None and encoder is not None:
        value = encoder(value)
    return PropertyValue(prop_def.name, prop_def.pg_type, value)


class _NodeHandler(coco.TargetHandler[_NodeValue, _EntityTrackingRecord, None]):
    def __init__(
        self,
        type_name: str,
        key_fields: tuple[str, ...],
        key: _TypeKey,
        property_defs: dict[str, PropertyDef],
    ) -> None:
        self._type_name = type_name
        self._key_fields = key_fields
        self._key = key
        self._property_defs = property_defs

    def reconcile(
        self,
        key: coco.StableKey,
        desired_target_state: _NodeValue | coco.NonExistenceType,
        prev_possible_records: typing.Collection[_EntityTrackingRecord],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[_NodeAction, _EntityTrackingRecord, None] | None:
        # `declare_node` keys the target state by a 1-tuple.
        key_tuple = key if isinstance(key, tuple) else (key,)
        coco_key = derive_coco_key(key_tuple)

        if coco.is_non_existence(desired_target_state):
            # Only "nothing tracked and nothing possibly missing" proves there
            # is nothing to remove; otherwise the entity may still be in the
            # graph untracked, and deleting a missing one is a no-op.
            if not prev_possible_records and not prev_may_be_missing:
                return None
            key_properties = tuple(
                PropertyValue(
                    field,
                    self._property_defs[field].pg_type,
                    _transport_key_value(self._property_defs[field], value),
                )
                for field, value in zip(self._key_fields, key_tuple, strict=True)
            )
            return coco.TargetReconcileOutput(
                action=_NodeAction(
                    "delete",
                    self._key,
                    self._type_name,
                    (),
                    coco_key,
                    key_properties=key_properties,
                ),
                sink=_entity_sink(self._key.db_key),
                tracking_record=coco.NON_EXISTENCE,
            )

        # A keyed node `insert` merges by key tuple, so a new node and a
        # changed node are both an "upsert" — unlike edges, there is no
        # separate insert/replace distinction to make here. Encode first:
        # the fingerprint covers what the engine is sent.
        encoded = _encode_properties(
            desired_target_state.properties, self._property_defs
        )
        desired = _entity_record(encoded)
        if not _needs_write(desired, prev_possible_records, prev_may_be_missing):
            return None
        return coco.TargetReconcileOutput(
            action=_NodeAction("upsert", self._key, self._type_name, encoded, coco_key),
            sink=_entity_sink(self._key.db_key),
            tracking_record=desired,
        )


class _EdgeHandler(coco.TargetHandler[_EdgeValue, _EntityTrackingRecord, None]):
    def __init__(
        self,
        type_name: str,
        key: _TypeKey,
        from_type: str,
        to_type: str,
        from_key_property: PropertyDef,
        to_key_property: PropertyDef,
        property_defs: dict[str, PropertyDef],
    ) -> None:
        self._type_name = type_name
        self._key = key
        self._from_type = from_type
        self._to_type = to_type
        self._from_key_property = from_key_property
        self._to_key_property = to_key_property
        self._property_defs = property_defs

    def reconcile(
        self,
        key: coco.StableKey,
        desired_target_state: _EdgeValue | coco.NonExistenceType,
        prev_possible_records: typing.Collection[_EntityTrackingRecord],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[_EdgeAction, _EntityTrackingRecord, None] | None:
        # The edge's StableKey is its (from_id, to_id) tuple.
        coco_key = derive_coco_key(key)

        if coco.is_non_existence(desired_target_state):
            # As for a node.
            if not prev_possible_records and not prev_may_be_missing:
                return None
            return coco.TargetReconcileOutput(
                action=_EdgeAction(
                    "delete",
                    self._key,
                    self._type_name,
                    coco_key,
                    None,
                    None,
                    (),
                    self._from_type,
                    self._to_type,
                    self._from_key_property,
                    self._to_key_property,
                ),
                sink=_entity_sink(self._key.db_key),
                tracking_record=coco.NON_EXISTENCE,
            )

        # Every edge type the connector writes is keyed on its endpoints
        # (`@key(@src, @dst)`; a user-managed one is checked at the type
        # sink), so an `insert` of an edge that exists updates it, like a
        # keyed node `insert`.
        encoded = _encode_properties(
            desired_target_state.properties, self._property_defs
        )
        desired = _entity_record(encoded)
        if not _needs_write(desired, prev_possible_records, prev_may_be_missing):
            return None
        return coco.TargetReconcileOutput(
            action=_EdgeAction(
                "upsert",
                self._key,
                self._type_name,
                coco_key,
                desired_target_state.from_id,
                desired_target_state.to_id,
                encoded,
                self._from_type,
                self._to_type,
                self._from_key_property,
                self._to_key_property,
            ),
            sink=_entity_sink(self._key.db_key),
            tracking_record=desired,
        )


# ---------------------------------------------------------------------------
# Commit planning: reconcile actions -> ordered Mutations, one per commit
# ---------------------------------------------------------------------------

#: Byte budget of one commit's request body. `omnigraph-server` refuses a
#: JSON request body over 1 MiB (`DEFAULT_REQUEST_BODY_LIMIT_BYTES`, fixed
#: in 0.14; verified: a 1.1 MB `/mutate` answers 413 "request body refused:
#: length limit exceeded", and at 2.3 MB the server drops the connection).
#: Commits are cut by `_statement_bytes`, an upper bound on what each
#: statement adds; the rest of the limit covers the request envelope.
_MAX_REQUEST_BYTES = 900_000
#: Index digits `render_query` gives a parameter (`s{i}_...`), assumed for
#: every statement so an estimate never falls short.
_PARAM_INDEX_DIGITS = 6


#: `.pg` types a node key may have for that node type to be usable as an
#: edge endpoint. An edge's `from`/`to` is the engine's own node `id`, which
#: is always a String rendering of the key value — so the connector has to
#: reproduce that rendering exactly, and these are the types where it can:
#: `str` is itself, and Rust's i64/u32/... Display is `str(int)` digit for
#: digit. Verified against the engine, which renders the others in forms no
#: caller would guess: a `Date` key of 2026-01-05 has id `"20458"` (days
#: since the epoch) and a `DateTime` id is epoch milliseconds.
_ENDPOINT_KEY_TYPES = frozenset({"String", "I32", "I64", "U32", "U64"})


def _endpoint_ref(value: Any) -> PropertyValue:
    """Build the PropertyValue addressing an edge endpoint.

    Always `String`: an edge's `from`/`to` holds the endpoint node's `id`,
    not its key property, and that id is a String whatever the key's own
    declared type is — passing an `I64` for an `I64`-keyed endpoint is
    rejected outright ("cannot assign/compare I64 with String for property
    `to`", verified against the engine). `_validate_edge_endpoint` is what
    keeps `str(value)` a faithful rendering of the id, by refusing endpoint
    types whose key this can't reproduce.

    `.name` is ignored by `build_edge_insert` — only `.pg_type` and
    `.value` address the endpoint.
    """
    return PropertyValue("ref", "String", str(value))


def _statement_bytes(statement: Statement) -> int:
    """An upper estimate of what `statement` adds to a rendered request body:
    its text, and per bind the parameter's name three times (signature,
    reference, params key), its type, its JSON value and the separators."""
    size = len(statement.body) + 1
    for bind in statement.binds:
        name = len(bind.label) + _PARAM_INDEX_DIGITS + 2
        # As the SDK encodes it: `json.dumps` escapes non-ASCII as `\uXXXX`.
        value = json.dumps(bind.value, default=str)
        size += 3 * name + len(bind.pg_type) + len(value) + 10
    return size


def _chunk_by_bytes(statements: Sequence[Statement]) -> list[list[Statement]]:
    """Split `statements`, in order, into chunks that each render to a
    request body within `_MAX_REQUEST_BYTES`.

    A statement that cannot fit on its own is refused here: the server
    answers a body somewhat over its limit with 413 but drops the connection
    for a much larger one, which reads as a write of unknown outcome."""
    chunks: list[list[Statement]] = [[]]
    size = 0
    for statement in statements:
        cost = _statement_bytes(statement)
        if cost > _MAX_REQUEST_BYTES:
            raise ValueError(
                f"`{statement.body[:60]}...` is about {cost:,} bytes as a request, "
                f"over the {_MAX_REQUEST_BYTES:,}-byte budget for omnigraph-server's "
                f"1 MiB request limit; store a value that large outside the graph"
            )
        if chunks[-1] and size + cost > _MAX_REQUEST_BYTES:
            chunks.append([])
            size = 0
        chunks[-1].append(statement)
        size += cost
    return chunks


def plan_commits(actions: Sequence[_NodeAction | _EdgeAction]) -> list[Query]:
    """Turn reconcile actions into the commits that apply them, in order.

    Writes come first, then deletes: the engine refuses a mutation that
    mixes the two ("split into separate mutations: (1) inserts and updates,
    then (2) deletes", verified against 0.14), so a batch with both needs
    two commits at least.

    Within the writes, node upserts precede edge upserts: statements run in
    the order written, and an edge ahead of its own endpoint's upsert fails
    "not found" and forces the stub-and-retry recovery, which is meant for
    an endpoint another component owns. No stubs are planned here: a keyed
    `insert` replaces the whole record, so stubbing every endpoint up front
    would null out a node's other properties. The sink stubs only the
    endpoint the engine reports missing (see `_apply_entity_actions`).

    Within the deletes, edges precede nodes.

    Each phase is cut into commits that fit the server's request limit.
    """
    node_writes: list[Statement] = []
    edge_writes: list[Statement] = []
    edge_deletes: list[Statement] = []
    node_deletes: list[Statement] = []
    for action in actions:
        if isinstance(action, _NodeAction):
            if action.op == "delete":
                node_deletes.append(
                    build_node_delete(action.type_name, action.coco_key)
                )
            else:
                node_writes.append(
                    build_node_upsert(
                        action.type_name, action.properties, action.coco_key
                    )
                )
        elif action.op == "delete":
            edge_deletes.append(build_edge_delete(action.type_name, action.coco_key))
        else:
            edge_writes.append(
                build_edge_insert(
                    action.type_name,
                    _endpoint_ref(action.from_id),
                    _endpoint_ref(action.to_id),
                    action.properties,
                    action.coco_key,
                )
            )
    return [
        render_query(chunk)
        for phase in (node_writes + edge_writes, edge_deletes + node_deletes)
        if phase
        for chunk in _chunk_by_bytes(phase)
    ]


# ---------------------------------------------------------------------------
# Sinks
# ---------------------------------------------------------------------------


#: The engine's refusal of an edge whose endpoint node does not exist yet:
#: `__src '<key>' not found in <Type>` for `from`, `__dst ...` for `to`
#: (verified against 0.14). The key is matched up to the whole suffix, not the
#: next apostrophe, since a key may contain one (`O'Brien`).
_ENDPOINT_NOT_FOUND_RE = re.compile(r"__(src|dst) '([\s\S]*?)' not found in (\w+)")


def _parse_missing_endpoint(error: Exception) -> tuple[str, str, str] | None:
    """Extract `(role, key, type_name)` from a "not found" failure, or
    `None` if `error` doesn't match. Doesn't build the stub itself — the
    caller tracks which `(type_name, key)` pairs it has already stubbed, so
    it needs the parsed identity before deciding whether to act on it."""
    m = _ENDPOINT_NOT_FOUND_RE.search(str(error))
    if m is None:
        return None
    return m.group(1), m.group(2), m.group(3)


def _build_endpoint_stub(
    role: str, key_str: str, type_name: str, edge_actions: Sequence[_EdgeAction]
) -> Statement | None:
    """Build the key-only stub for the endpoint the engine reported missing.
    Returns `None` if no action in this batch accounts for the (type, key)
    — nothing safe to build a stub from.

    The engine's error names the endpoint's type and the string form of its
    key value, but not the key's *field* name or its `pg_type` — those come
    from matching (type, key) against the `_EdgeAction`s in this batch,
    which is also how the actual (correctly typed) key value is recovered
    rather than re-parsing it out of the error text.
    """
    for action in edge_actions:
        if action.op == "delete":
            # A delete carries no endpoints (`from_id`/`to_id` are `None`) and
            # needs none — deleting by `coco_key` never touches them. Without
            # this, `str(None)` matches any node whose key value is literally
            # "None" and can build a meaningless stub from the delete action.
            continue
        if (
            role == "src"
            and action.from_type == type_name
            and str(action.from_id) == key_str
        ):
            value, key_property = action.from_id, action.from_key_property
        elif (
            role == "dst"
            and action.to_type == type_name
            and str(action.to_id) == key_str
        ):
            value, key_property = action.to_id, action.to_key_property
        else:
            continue
        ref = _encode_property(key_property, value)
        return build_endpoint_stub(
            type_name,
            [ref],
            derive_coco_key((value,)),
        )
    return None


async def _mutate_with_endpoint_retry(
    client: _GraphClient,
    commit: Query,
    *,
    branch: str,
    edge_actions: Sequence[_EdgeAction],
) -> None:
    """Apply `commit`, stubbing each endpoint the engine reports missing.

    A stub is built only after the engine has confirmed the node is absent,
    so it never overwrites a node's properties (a keyed `insert` replaces the
    whole record). The engine reports one missing endpoint per attempt, so a
    commit may need one round per distinct endpoint its edges reference. The
    loop is bounded by that set: each `(type, key)` is stubbed at most once,
    and a "not found" for an endpoint already stubbed, or one no edge here
    references, propagates.
    """
    stubbed: set[tuple[str, str]] = set()
    while True:
        try:
            await client.mutate(commit, branch=branch)
            return
        except _og.BadRequestError as e:
            parsed = _parse_missing_endpoint(e)
            if parsed is None:
                raise
            role, key_str, type_name = parsed
            if (type_name, key_str) in stubbed:
                raise
            stub = _build_endpoint_stub(role, key_str, type_name, edge_actions)
            if stub is None:
                raise
            stubbed.add((type_name, key_str))
            await client.mutate(render_query([stub]), branch=branch)


class _SchemaGate:
    """Orders scratch-branch work against schema deployments on one graph.

    The server refuses a schema change while any non-main branch lives, so
    within this process scratch work holds the gate shared and a schema
    deployment holds it exclusive; a waiting deployment keeps new scratch
    work out until it is done. Other processes are handled by the bounded
    retry in `deploy_schema`.
    """

    def __init__(self) -> None:
        self._cond = asyncio.Condition()
        self._readers = 0
        self._writer = False
        self._writers_waiting = 0

    @contextlib.asynccontextmanager
    async def shared(self) -> AsyncIterator[None]:
        async with self._cond:
            await self._cond.wait_for(
                lambda: not self._writer and not self._writers_waiting
            )
            self._readers += 1
        try:
            yield
        finally:
            async with self._cond:
                self._readers -= 1
                self._cond.notify_all()

    @contextlib.asynccontextmanager
    async def exclusive(self) -> AsyncIterator[None]:
        async with self._cond:
            self._writers_waiting += 1
            try:
                await self._cond.wait_for(
                    lambda: not self._writer and self._readers == 0
                )
            finally:
                self._writers_waiting -= 1
            self._writer = True
        try:
            yield
        finally:
            async with self._cond:
                self._writer = False
                self._cond.notify_all()


_schema_gates: weakref.WeakKeyDictionary[
    asyncio.AbstractEventLoop, dict[tuple[str, str], _SchemaGate]
] = weakref.WeakKeyDictionary()


def _schema_gate(conn: ConnectionFactory) -> _SchemaGate:
    gates = _schema_gates.setdefault(asyncio.get_running_loop(), {})
    return gates.setdefault((conn.base_url, conn.graph_id), _SchemaGate())


#: Prefix of every branch this connector creates. Nothing else creates
#: branches under it.
_SCRATCH_BRANCH_PREFIX = "coco_scratch_"
_SCRATCH_BRANCH_RE = re.compile(
    re.escape(_SCRATCH_BRANCH_PREFIX) + r"(\d{8}T\d{6}Z)_[0-9a-f]{32}$"
)
#: A scratch branch older than this was left by a process that died mid-sync.
_SCRATCH_BRANCH_TTL = datetime.timedelta(hours=1)
#: Scratch branches this process has created and not yet removed.
_live_scratch_branches: set[str] = set()


def _scratch_branch_name(now: datetime.datetime) -> str:
    return f"{_SCRATCH_BRANCH_PREFIX}{now:%Y%m%dT%H%M%SZ}_{uuid.uuid4().hex}"


def _scratch_branch_created_at(name: str) -> datetime.datetime | None:
    m = _SCRATCH_BRANCH_RE.match(name)
    if m is None:
        return None
    return datetime.datetime.strptime(m.group(1), "%Y%m%dT%H%M%SZ").replace(
        tzinfo=datetime.UTC
    )


@contextlib.asynccontextmanager
async def _scratch_branch(client: _GraphClient, *, frm: str) -> AsyncIterator[str]:
    """Own a scratch branch for the duration of the block.

    The block ends by merging it with `client.branch_merge`, which deletes
    it in the same call. If the block fails, or the create itself does (its
    answer may be what was lost), the branch is deleted here: a leftover
    non-main branch blocks every schema deployment on the graph. The name
    carries its creation time, so `_reap_abandoned_scratch_branches` can
    tell an abandoned branch from a live one on any host.
    """
    async with _schema_gate(client.conn).shared():
        name = _scratch_branch_name(datetime.datetime.now(datetime.UTC))
        _live_scratch_branches.add(name)
        try:
            await client.branch_create(name, frm=frm)
            yield name
        except BaseException as e:
            try:
                await client.branch_delete(name)
            except _og.NotFoundError:
                pass
            except _og.OmnigraphError as cleanup:
                # Report the original failure; the reaper removes the branch
                # once its TTL has passed.
                e.add_note(f"deleting scratch branch {name!r} also failed: {cleanup}")
            raise
        finally:
            _live_scratch_branches.discard(name)


async def _reap_abandoned_scratch_branches(client: _GraphClient) -> list[str]:
    """Delete every `coco_scratch_*` branch past its TTL and return the names.

    A branch this process still uses is never touched, and neither is one
    whose name carries no creation time, nor any user branch.
    """
    now = datetime.datetime.now(datetime.UTC)
    reaped: list[str] = []
    for name in await client.branch_list():
        if name in _live_scratch_branches:
            continue
        created = _scratch_branch_created_at(name)
        if created is None or now - created < _SCRATCH_BRANCH_TTL:
            continue
        with contextlib.suppress(_og.NotFoundError):
            await client.branch_delete(name)
            reaped.append(name)
    return reaped


async def _deploy_schema(
    client: _GraphClient, *, served: str, desired: str, types: set[str]
) -> None:
    async def reap() -> None:
        await _reap_abandoned_scratch_branches(client)

    await client.apply_schema(
        desired, served=served, types=types, on_blocked_by_branches=reap
    )


async def _keep_referenced_nodes(
    client: _GraphClient, actions: Sequence[_NodeAction | _EdgeAction], *, branch: str
) -> list[_NodeAction | _EdgeAction]:
    """Turn the delete of a node an edge still references into a key-only
    upsert of that node: the stub an edge gets when it arrives before its
    node.

    Deleting a node cascades to its edges, while their components' tracking
    says they exist, so they would be gone for good. A reference is an edge
    in the graph that this batch does not delete, or an edge this batch
    writes (writes land before deletes). A node undeclared together with all
    of its edges is deleted outright.

    Costs one schema read per batch with a node delete, and one read per
    (edge type, end) for each delete of a type some edge type references.
    An edge another component inserts between that read and this commit is
    still cascaded, the same window every unlocked write has.
    """
    deletes = [a for a in actions if isinstance(a, _NodeAction) and a.op == "delete"]
    if not deletes:
        return list(actions)
    written_endpoints = {
        endpoint
        for a in actions
        if isinstance(a, _EdgeAction) and a.op == "upsert"
        for endpoint in ((a.from_type, str(a.from_id)), (a.to_type, str(a.to_id)))
    }
    existing = await client.read_schema()
    patterns = {
        type_name: incident_edge_patterns(existing, type_name)
        for type_name in {a.type_name for a in deletes}
    }
    if not any(patterns.values()) and not written_endpoints:
        return list(actions)
    deleted_edges = {
        (a.type_name, a.coco_key)
        for a in actions
        if isinstance(a, _EdgeAction) and a.op == "delete"
    }
    out: list[_NodeAction | _EdgeAction] = []
    for action in actions:
        if not (isinstance(action, _NodeAction) and action.op == "delete"):
            out.append(action)
            continue
        (key,) = action.key_properties
        referenced = (action.type_name, str(key.value)) in written_endpoints
        for pattern in [] if referenced else patterns[action.type_name]:
            rows = await client.query(
                render_query(
                    [
                        build_incident_edges_query(
                            action.type_name, action.coco_key, pattern
                        )
                    ]
                ),
                branch=branch,
            )
            if any(
                (pattern.edge_type, row["edge_key"]) not in deleted_edges
                for row in rows
            ):
                referenced = True
                break
        if referenced:
            action = _NodeAction(
                "upsert",
                action.key,
                action.type_name,
                action.key_properties,
                action.coco_key,
            )
        out.append(action)
    return out


async def _apply_entity_actions(
    conn: ConnectionFactory, actions: Sequence[_NodeAction | _EdgeAction]
) -> None:
    client = _GraphClient(conn)
    actions = await _keep_referenced_nodes(client, actions, branch=conn.branch)
    commits = plan_commits(actions)
    edge_actions = [a for a in actions if isinstance(a, _EdgeAction)]
    if len(commits) == 1:
        # One commit is atomic on its own, so it goes straight to the live
        # branch. Not through `_mutate_with_endpoint_retry`: its recovery is
        # two commits (the stub, then the retry), which on the live branch
        # would leave an untracked stub behind if the retry then failed. A
        # failed mutation applies nothing (see TestMutationAtomicityLive), so
        # when recovery is needed this falls through and redoes the batch on
        # a scratch branch.
        try:
            await client.mutate(commits[0], branch=conn.branch)
            return
        except _og.BadRequestError as e:
            if _parse_missing_endpoint(e) is None:
                raise
    # Several commits are made atomic together by applying them on a scratch
    # branch and merging it in one step.
    async with _scratch_branch(client, frm=conn.branch) as scratch:
        for commit in commits:
            await _mutate_with_endpoint_retry(
                client, commit, branch=scratch, edge_actions=edge_actions
            )
        await client.branch_merge(scratch, into=conn.branch)


@dataclasses.dataclass(frozen=True)
class _EntitySink:
    """The entity sink of one graph: a batch never spans graphs, so a failure
    on one graph never makes the engine replay another graph's landed writes."""

    db_key: str

    async def __call__(
        self,
        context_provider: ContextProvider,
        actions: Sequence[_NodeAction | _EdgeAction],
        /,
    ) -> None:
        await _apply_entity_actions(context_provider.get(self.db_key), actions)


def _entity_sink(db_key: str) -> coco.TargetActionSink[_NodeAction | _EdgeAction]:
    return coco.TargetActionSink.from_async_fn(_EntitySink(db_key))


def _check_no_kind_clash(existing_pg: str, pending: Sequence[_TypeAction]) -> None:
    """Refuse to add a type whose name is already taken by the OTHER kind.

    A `.pg` holding both `node Link` and `edge Link` is accepted by `schema
    apply` — but at the mutation level the name resolves to the node type
    alone, so `insert Link { from: ..., to: ... }` fails with "type `Link`
    has no property `from`" (verified against the binary). The edge type
    exists in the schema and can never be written to, read back, or
    deleted. Fail here, naming both, rather than let a component declare
    edges into a type nothing can address.

    Checked against the live schema rather than tracked at declare time:
    the two kinds register under separate providers, so a node and an edge
    of the same name can be declared by components that never see each
    other, and only the graph itself has the whole picture.
    """
    other = {"node": "edge", "edge": "node"}
    for action in pending:
        if coco.is_non_existence(action.spec):
            continue
        clashing = other[action.key.type_kind]
        if _find_type_block(existing_pg, clashing, action.key.type_name) is None:
            continue
        raise ValueError(
            f"Omnigraph type name {action.key.type_name!r} is already used by "
            f"a {clashing} type in this graph, and a mutation can only ever "
            f"address one of them — the name resolves to the {clashing}, so "
            f"the {action.key.type_kind} type would be unwritable. Rename one "
            f"of them."
        )


def _check_no_kind_clash_within_batch(pending: Sequence[_TypeAction]) -> None:
    """Refuse a batch that writes a node and an edge under the same name.

    Same rule as `_check_no_kind_clash`, applied to the batch: the live
    schema cannot show two types that are both new.
    """
    kind_by_name: dict[str, str] = {}
    for action in pending:
        if coco.is_non_existence(action.spec):
            continue
        name = action.key.type_name
        first_kind = kind_by_name.setdefault(name, action.key.type_kind)
        if first_kind != action.key.type_kind:
            raise ValueError(
                f"Omnigraph type name {name!r} is declared as both a node and "
                f"an edge in the same sync, and a mutation can only ever "
                f"address one of them — the name resolves to the node, so the "
                f"edge type would be unwritable. Rename one of them."
            )


class _BlockedRemoval(NamedTuple):
    """A node type this batch removes or rebuilds, and the edge types not in
    this batch that still point at it."""

    node_name: str
    is_drop: bool  # False for a "replace" (a `@key` change)
    edge_names: list[str]


def _blocked_node_removals(
    existing_pg: str, pending: Sequence[_TypeAction]
) -> list[_BlockedRemoval]:
    """Node removals in `pending` that would leave an edge type dangling.

    The engine rejects such a schema outright — `catalog error: edge 'X' has
    an unresolved endpoint`, verified against the binary — and the connector
    could emit one two different ways: dropping a node type leaves the
    dangling edge in the FINAL schema, and a "replace" (a `@key` change)
    leaves it in the INTERMEDIATE schema that the drop half applies first.

    Every mounted type is its own processing component, so a referencing
    edge type's removal reaches the sink in its own batch, not alongside
    the node's — unless the batcher happened to group them. That is why
    this has to be checked here: the engine's own error names the edge but
    offers no way forward, and arrives after the connector has already
    decided to write.
    """
    going_away = {
        a.key.type_name: coco.is_non_existence(a.spec)
        for a in pending
        if a.key.type_kind == "node"
        and (coco.is_non_existence(a.spec) or a.main_action == "replace")
    }
    if not going_away:
        return []
    also_dropped = {
        a.key.type_name
        for a in pending
        if a.key.type_kind == "edge" and coco.is_non_existence(a.spec)
    }
    # An edge type this batch re-declares references what its new spec says,
    # not what the served schema still says: re-pointed away from the node,
    # it neither blocks the node's removal nor goes along with it.
    redeclared: dict[str, tuple[str | None, str | None]] = {
        a.key.type_name: (a.spec.from_type, a.spec.to_type)
        for a in pending
        if a.key.type_kind == "edge"
        and not coco.is_non_existence(a.spec)
        and a.spec.from_type is not None
    }
    blocked = []
    for node_name, is_drop in sorted(going_away.items()):
        edge_names = sorted(
            name
            for name in set(edge_types_referencing(existing_pg, node_name))
            - also_dropped
            if node_name in redeclared.get(name, (node_name,))
        )
        if edge_names:
            blocked.append(_BlockedRemoval(node_name, is_drop, edge_names))
    return blocked


def _orphaned_endpoint_error(
    blocked: _BlockedRemoval, *, foreign: dict[str, str | None], marker: str
) -> ValueError:
    """`foreign` maps each referencing edge type this app does not own to
    the ownership property its block does declare, if any — another app's
    — so the message can say whose it is."""
    plural = len(blocked.edge_names) > 1
    if foreign:
        many = len(foreign) > 1
        owned_by_others = "; ".join(
            f"{name!r} is marked `{other}`" for name, other in foreign.items() if other
        )
        advice = (
            f"{list(foreign)!r} {'are' if many else 'is'} not managed by this app "
            f"({'their blocks declare' if many else 'its block declares'} no "
            f"`{marker}` property{'; ' + owned_by_others if owned_by_others else ''}), "
            f"so {'they' if many else 'it'} will not be removed for you: have the "
            f"app that owns {'them' if many else 'it'} stop declaring "
            f"{'them' if many else 'it'} first, remove {'them' if many else 'it'} "
            f"from the schema yourself, or keep the node type."
        )
    else:
        advice = (
            f"Stop declaring {'those edge types' if plural else 'that edge type'} "
            f"as well, or keep the node type. A `@key` change counts here too: it "
            f"drops and recreates the type, so its endpoints go with it."
        )
    return ValueError(
        f"Cannot remove Omnigraph node type {blocked.node_name!r}: edge "
        f"type{'s' if plural else ''} {blocked.edge_names!r} still "
        f"{'reference' if plural else 'references'} it as an endpoint, "
        f"and the engine rejects a schema with an unresolved endpoint. {advice}"
    )


def _check_user_edges_keyed(existing_pg: str, names: Sequence[str]) -> None:
    """A user-managed edge type has to be keyed on its endpoints, as the
    connector's own are: an edge is written with a plain `insert`, which
    updates a keyed edge and adds another copy of an unkeyed one."""
    unkeyed = sorted(
        n for n in names if edge_keyed_on_endpoints(existing_pg, n) is False
    )
    if unkeyed:
        raise ValueError(
            f"Omnigraph edge type{'s' if len(unkeyed) > 1 else ''} {unkeyed!r} "
            f"{'are' if len(unkeyed) > 1 else 'is'} declared managed_by='user' but "
            f"not keyed on {'their' if len(unkeyed) > 1 else 'its'} endpoints: add "
            f"`@key(@src, @dst)` to the edge type in the schema, or every write "
            f"of an edge would add another copy of it."
        )


async def _apply_type_schema(
    client: _GraphClient, actions: Sequence[_TypeAction]
) -> None:
    """Fold every schema-changing action for one graph into one read and at
    most two deployments.

    Omnigraph's schema is deployed whole-graph (a deployment of one type's
    fragment alone drops every other type), while each type reconciles to
    its own fragment, so the batch is folded into the served source and
    deployed once. A graph always has a schema, possibly empty: graphs are
    created by cluster deployments, never by the connector.

    A drop removes the type's block, which is also what deletes its rows:
    the engine emits no per-child deletes for a removed container.

    A `"replace"` (a `@key` or endpoint change) needs a rebuild, since the
    engine rejects an in-place key change ("removing property constraints
    ... not supported in schema migration v1"; verified). The replaced types
    are removed in a first deployment and re-added by the second; their
    children are re-declared (`child_invalidation="destructive"`).
    """
    user_edges = [
        a.key.type_name
        for a in actions
        if a.key.type_kind == "edge"
        and not coco.is_non_existence(a.spec)
        and a.spec.managed_by == ManagedBy.USER
    ]
    if user_edges:
        _check_user_edges_keyed(await client.read_schema(), user_edges)
    pending = [
        a
        for a in actions
        if a.main_action is not None or a.property_actions or a.release_ownership
    ]
    if not pending:
        return

    # Exclusive against this process's scratch branches, which would make
    # the server refuse the deployment. Concurrent read-merge-deploys of the
    # same configuration are serialized by `deploy_schema`'s directory lock.
    # One app's types at a time: an app's ownership property is what marks
    # the blocks it may take along or release. A batch can hold several apps'
    # types when they share a ContextKey name in one process.
    by_owner: dict[str, list[_TypeAction]] = {}
    for action in pending:
        by_owner.setdefault(action.owner, []).append(action)
    async with _schema_gate(client.conn).exclusive():
        for owned in by_owner.values():
            await _apply_type_schema_locked(client, owned)


async def _apply_type_schema_locked(
    client: _GraphClient, pending: Sequence[_TypeAction]
) -> None:
    _check_no_kind_clash_within_batch(pending)

    try:
        existing = await client.read_schema()
    except _og.NotFoundError:
        conn = client.conn
        raise SchemaDeployError(
            f"graph {conn.graph_id!r} is not served by {conn.base_url}; add it to "
            f"the cluster configuration's `graphs:` and run `omnigraph cluster "
            f"apply`. CocoIndex never creates or deletes graphs."
        ) from None

    _check_no_kind_clash(existing, pending)

    # `_apply_type_schema` hands over one app's types at a time.
    marker = ownership_property(pending[0].owner)

    # A dropped node type may still have an edge type of this app pointing
    # at it, whose own removal is queued behind this batch (waiting for it
    # deadlocks), so this app's edge types go along now. Another app's, or
    # one with no ownership property (a user's), is not removed on its
    # behalf, and a `@key` change keeps the edge declared: those fail here.
    taken_along: list[str] = []
    for removal in _blocked_node_removals(existing, pending):
        foreign = {
            name: found
            for name in removal.edge_names
            if (found := ownership_marker(existing, "edge", name)) != marker
        }
        if not removal.is_drop or foreign:
            raise _orphaned_endpoint_error(removal, foreign=foreign, marker=marker)
        taken_along.extend(removal.edge_names)

    types = {a.key.type_name for a in pending} | set(taken_along)
    replaced = [
        (a.key.type_kind, a.key.type_name)
        for a in pending
        if a.main_action == "replace"
    ]
    if replaced:
        intermediate = existing
        for kind, type_name in replaced:
            intermediate = remove_type_from_schema(intermediate, kind, type_name)
        await _deploy_schema(client, served=existing, desired=intermediate, types=types)
        existing = intermediate

    desired = existing
    for action in pending:
        if coco.is_non_existence(action.spec):
            desired = remove_type_from_schema(
                desired, action.key.type_kind, action.key.type_name
            )
        elif action.release_ownership:
            desired = release_ownership(
                desired, action.key.type_kind, action.key.type_name, marker
            )
        else:
            assert action.pg_fragment is not None
            desired = merge_type_into_schema(
                desired, action.key.type_kind, action.key.type_name, action.pg_fragment
            )
    for edge_name in taken_along:
        desired = remove_type_from_schema(desired, "edge", edge_name)
    # An unchanged schema needs no write — the edge type's own removal
    # arriving after a node's batch took it along is the common case.
    if desired != existing:
        await _deploy_schema(client, served=existing, desired=desired, types=types)


def _type_child_handler(
    context_provider: ContextProvider, action: _TypeAction, /
) -> _NodeHandler | _EdgeHandler:
    """The handler for a created or updated type's nodes or edges.

    The engine calls this for every type that still exists — an unchanged
    type included, since `reconcile` always returns an action for one — and
    never for a dropped type.
    """
    spec = action.spec
    assert not coco.is_non_existence(spec)
    if spec.from_type is None:
        # Node type: `key` is the node's own key field names.
        assert spec.schema is not None
        return _NodeHandler(
            action.key.type_name, spec.key, action.key, spec.schema.properties
        )
    # Edge type: `from_type`/`to_type` name the endpoints, and
    # `from_key_property`/`to_key_property` are the endpoints' own key
    # definitions — needed to build correctly typed stubs.
    assert spec.to_type is not None
    assert spec.from_key_property is not None
    assert spec.to_key_property is not None
    return _EdgeHandler(
        action.key.type_name,
        action.key,
        spec.from_type,
        spec.to_type,
        spec.from_key_property,
        spec.to_key_property,
        spec.schema.properties if spec.schema is not None else {},
    )


@dataclasses.dataclass(frozen=True)
class _TypeSink:
    """The type sink of one graph (see `_EntitySink`)."""

    db_key: str

    async def __call__(
        self, context_provider: ContextProvider, actions: Sequence[_TypeAction], /
    ) -> None:
        await _apply_type_schema(
            _GraphClient(context_provider.get(self.db_key)), actions
        )


def _type_sink(db_key: str) -> coco.TargetActionSink[_TypeAction]:
    return coco.TargetActionSink.from_async_fn(
        _TypeSink(db_key), child_handler=_type_child_handler
    )


# ---------------------------------------------------------------------------
# Root provider registration
# ---------------------------------------------------------------------------

_node_type_provider: coco.TargetStateProvider[_TypeSpec, _NodeHandler] = (
    coco.register_root_target_states_provider(
        "cocoindex/omnigraph/node_type", _NodeTypeHandler()
    )
)
_edge_type_provider: coco.TargetStateProvider[_TypeSpec, _EdgeHandler] = (
    coco.register_root_target_states_provider(
        "cocoindex/omnigraph/edge_type", _EdgeTypeHandler()
    )
)


# ---------------------------------------------------------------------------
# NodeTarget
# ---------------------------------------------------------------------------

RowT = TypeVar("RowT", default=dict[str, Any])


class NodeTarget(
    coco.ResolvesTo["NodeTarget[RowT]"], Generic[RowT, coco.MaybePendingS]
):
    """A target for writing nodes to an Omnigraph node type."""

    _provider: coco.TargetStateProvider[_NodeValue, None, coco.MaybePendingS]
    _schema: NodeSchema
    _type_name: str

    def __init__(
        self,
        provider: coco.TargetStateProvider[_NodeValue, None, coco.MaybePendingS],
        schema: NodeSchema,
        type_name: str,
    ) -> None:
        self._provider = provider
        self._schema = schema
        self._type_name = type_name

    @property
    def type_name(self) -> str:
        return self._type_name

    @property
    def schema(self) -> NodeSchema:
        return self._schema

    def declare_node(self: NodeTarget[RowT], *, node: RowT) -> None:
        """Declare a node (record) to be upserted to this node type.

        The target-state key is the graph's identity for the key value —
        see `_tracking_key_value`: epoch milliseconds for a `DateTime`, the
        ISO form for a `Date`, the value itself for a string or integer. A
        raw `datetime` is not a type the engine's StableKey accepts, and
        tracking a spelling the graph does not key on let two declarations
        of one node get two tracking keys.
        """
        properties = _record_to_dict(node, self._schema)
        key: tuple[Any, ...] = tuple(
            _tracking_key_value(self._schema.properties[k], properties[k])
            for k in self._schema.key
        )
        coco.declare_target_state(
            self._provider.target_state(key, _NodeValue(properties))
        )

    def __coco_memo_key__(self) -> str:
        return self._provider.memo_key


# ---------------------------------------------------------------------------
# EdgeTarget
# ---------------------------------------------------------------------------


class EdgeTarget(
    coco.ResolvesTo["EdgeTarget[RowT]"], Generic[RowT, coco.MaybePendingS]
):
    """A target for writing edges to an Omnigraph edge type."""

    _provider: coco.TargetStateProvider[_EdgeValue, None, coco.MaybePendingS]
    _schema: EdgeSchema | None
    _type_name: str
    _from_target: NodeTarget[Any]
    _to_target: NodeTarget[Any]

    def __init__(
        self,
        provider: coco.TargetStateProvider[_EdgeValue, None, coco.MaybePendingS],
        schema: EdgeSchema | None,
        type_name: str,
        from_target: NodeTarget[Any],
        to_target: NodeTarget[Any],
    ) -> None:
        self._provider = provider
        self._schema = schema
        self._type_name = type_name
        self._from_target = from_target
        self._to_target = to_target

    @property
    def type_name(self) -> str:
        return self._type_name

    def declare_edge(
        self: EdgeTarget[RowT], *, from_id: Any, to_id: Any, record: RowT | None = None
    ) -> None:
        """Declare an edge between the two nodes this edge type connects."""
        if record is not None and self._schema is None:
            # Omnigraph is schema-first: an edge type declares its properties
            # in `.pg`, and an insert naming one that isn't declared is
            # rejected outright ("type `X` has no property `y`", verified
            # against the engine). So a record cannot be carried by a
            # schema-less edge type, and inferring the property types at
            # write time wouldn't help — the type genuinely has no column to
            # put them in. This is the one real difference from the Neo4j
            # original a ported app hits: Neo4j's MERGE creates relationship
            # properties on the fly, so the same code works there.
            raise TypeError(
                f"Edge type {self._type_name!r} was mounted without a schema, "
                f"so it cannot carry a record. Pass the edge's own property "
                f"schema (e.g. `EdgeSchema.from_class({type(record).__name__})`) "
                f"when mounting it, or drop the `record=` argument."
            )
        if record is None and self._schema is not None:
            # The mirror of the guard above. Omitting the record leaves every
            # declared property out of the insert, and Omnigraph rejects an
            # insert missing a non-nullable one ("must provide non-nullable
            # property") with an error naming neither this call nor the
            # property. A schema of only nullable properties is fine -- that
            # is what nullable means.
            required = sorted(
                name
                for name, prop in self._schema.properties.items()
                if not prop.pg_type.endswith("?")
            )
            if required:
                raise TypeError(
                    f"Edge type {self._type_name!r} declares non-nullable "
                    f"propert{'y' if len(required) == 1 else 'ies'} "
                    f"{required!r}, so `record=` is required. Pass the record, "
                    f"or declare those properties as optional."
                )
        _check_endpoint_id(from_id, self._type_name, "from_id", self._from_target)
        _check_endpoint_id(to_id, self._type_name, "to_id", self._to_target)
        properties = (
            _record_to_dict(record, self._schema)
            if record is not None and self._schema is not None
            else {}
        )
        key = (from_id, to_id)
        coco.declare_target_state(
            self._provider.target_state(key, _EdgeValue(from_id, to_id, properties))
        )

    def __coco_memo_key__(self) -> str:
        return self._provider.memo_key


def _endpoint_key_family(pg_type: str) -> str:
    """`String` keys and integer keys are the two shapes `str(value)` can
    reproduce as a node id; every integer width renders the same way."""
    return "String" if pg_type == "String" else "integer"


def _check_endpoint_id(
    value: Any, type_name: str, what: str, endpoint: NodeTarget[Any]
) -> None:
    """An edge endpoint is addressed by its node's single key VALUE, spliced
    into the insert as `from: $e_from`. Checked here, where the offending
    `declare_edge` call is on the stack, rather than at sink time — where
    `_endpoint_ref` would raise the same underlying `TypeError` from inside
    `plan_commits`, with nothing pointing at the declaration that caused it.

    The value is also checked against the endpoint type's own key: the id
    is `str(value)`, so an integer handed to a String-keyed endpoint would
    silently address whichever node's key happens to be those digits, and a
    string handed to an integer-keyed endpoint can never match a node.
    """
    try:
        pg_type = _pg_type_for(type(value))
    except TypeError:
        pg_type = None
    if pg_type not in _ENDPOINT_KEY_TYPES:
        raise TypeError(
            f"Edge type {type_name!r}: {what}={value!r} is not usable as an "
            f"endpoint reference. It must be the endpoint node's key value — a "
            f"single string or integer, matching what `mount_edge_target` "
            f"already required of the endpoint type's key."
        )
    (key_field,) = endpoint.schema.key
    key_type = endpoint.schema.properties[key_field].pg_type
    if _endpoint_key_family(pg_type) != _endpoint_key_family(key_type):
        raise TypeError(
            f"Edge type {type_name!r}: {what}={value!r} is a {pg_type} value, but "
            f"the endpoint node type {endpoint.type_name!r} is keyed by "
            f"{key_field!r}: {key_type}. Pass that node's key value."
        )


def _record_to_dict(record: Any, schema: NodeSchema | EdgeSchema) -> dict[str, Any]:
    """Extract `{property_name: raw_value}` from a dataclass or dict record.

    Values are NOT encoded here — `_encode_properties` (used by
    `_NodeHandler`/`_EdgeHandler.reconcile`, above) applies `PropertyDef.encoder`
    at commit time, once the property's `pg_type` is back in scope.
    """
    if isinstance(record, dict):
        # A dict record's keys are unchecked user input. A misspelled name used
        # to be dropped without a word, leaving EVERY declared property `None`
        # — so the key's `coco_key` was derived from `None` and the engine
        # rejected a null into a non-nullable `@key` column, naming neither the
        # record nor the typo. (The dataclass branch below needs no equivalent:
        # its schema is normally derived from that same class by `from_class`,
        # and a missing attribute already raises.)
        unknown = sorted(set(record) - set(schema.properties))
        if unknown:
            raise ValueError(
                f"Record declares propert{'y' if len(unknown) == 1 else 'ies'} "
                f"{unknown!r} not in the schema; declared properties are "
                f"{sorted(schema.properties)!r}."
            )
        missing = sorted(
            name
            for name, prop in schema.properties.items()
            if name not in record and not prop.pg_type.endswith("?")
        )
        if missing:
            raise ValueError(
                f"Record is missing non-nullable propert"
                f"{'y' if len(missing) == 1 else 'ies'} {missing!r}. Only a "
                f"nullable property (e.g. `str | None`) may be omitted."
            )
        return {name: record.get(name) for name in schema.properties}
    return {name: getattr(record, name) for name in schema.properties}


def _validate_edge_endpoint(target: NodeTarget[Any], role: str) -> None:
    """Refuse, at mount time, a node type that cannot serve as an edge
    endpoint — naming the type and what's wrong with it, rather than
    failing mid-sync from whatever component happens to declare the edge.

    Two independent requirements:

    Its key must be one the connector can render the way the engine does.
    An edge's `from`/`to` holds the endpoint's node `id`, a String rendering
    of the key value, and only `String` and integer keys have a rendering
    `str(value)` reproduces (see `_ENDPOINT_KEY_TYPES`).

    And it must be stubbable. An edge may be written before the component
    owning one of its endpoints has run, so the connector recovers by
    inserting a key-only stub for the endpoint the engine reports missing
    (see `_build_endpoint_stub`). That stub can never satisfy a non-nullable
    property outside the key — Omnigraph rejects the insert.
    """
    schema = target.schema
    (key_field,) = schema.key
    key_type = schema.properties[key_field].pg_type
    if key_type not in _ENDPOINT_KEY_TYPES:
        raise ValueError(
            f"Node type {target.type_name!r} cannot be referenced as an edge "
            f"endpoint ({role}): its key {key_field!r} is {key_type}, and an "
            f"edge addresses an endpoint by the node's id — a String rendering "
            f"of the key that CocoIndex can only reproduce for "
            f"{sorted(_ENDPOINT_KEY_TYPES)}. Key this type on a string or an "
            f"integer instead."
        )
    unstubbable = [
        p.name
        for p in schema.properties.values()
        if p.name not in schema.key and not p.pg_type.endswith("?")
    ]
    if unstubbable:
        raise ValueError(
            f"Node type {target.type_name!r} cannot be referenced as an edge "
            f"endpoint ({role}): an edge may be written before the component "
            f"that owns the node has run, so the connector inserts a key-only "
            f"stub — which Omnigraph rejects while {unstubbable!r} "
            f"{'is' if len(unstubbable) == 1 else 'are'} non-nullable. Declare "
            f"{'it' if len(unstubbable) == 1 else 'them'} optional (e.g. "
            f"`str | None`)."
        )


def _build_edge_spec(
    schema: EdgeSchema | None,
    from_target: NodeTarget[Any],
    to_target: NodeTarget[Any],
    managed_by: ManagedBy,
    owner: str,
) -> _TypeSpec:
    """The spec `edge_target` declares, with the endpoints' key definitions
    the sink needs for stubs."""
    if schema is not None and not isinstance(schema, EdgeSchema):
        raise TypeError(
            f"An edge type's schema must be an EdgeSchema, not "
            f"{type(schema).__name__}: an edge has no key of its own, its "
            f"identity is always (from_id, to_id). Build it with "
            f"`EdgeSchema.from_class(...)`."
        )
    _validate_edge_endpoint(from_target, "from")
    _validate_edge_endpoint(to_target, "to")
    (from_key_field,) = from_target.schema.key
    (to_key_field,) = to_target.schema.key
    return _TypeSpec(
        schema=schema,
        key=(),
        from_type=from_target.type_name,
        to_type=to_target.type_name,
        managed_by=managed_by,
        owner=owner,
        from_key_property=from_target.schema.properties[from_key_field],
        to_key_property=to_target.schema.properties[to_key_field],
    )


# ---------------------------------------------------------------------------
# Module-level entry points
# ---------------------------------------------------------------------------


def _check_schema_management(
    db: coco.ContextKey[ConnectionFactory],
    kind: str,
    type_name: str,
    managed_by: ManagedBy,
) -> None:
    """A system-managed type needs a way to deploy schema; fail at declaration
    rather than mid-sync."""
    if managed_by != ManagedBy.SYSTEM:
        return
    conn = coco.use_context(db)
    if conn.cluster is None:
        raise ValueError(
            f"{kind} type {type_name!r} is managed_by='system', but its "
            f"ConnectionFactory for graph {conn.graph_id!r} has no `cluster`: "
            f"omnigraph-server takes schema changes only as a cluster deployment. "
            f"Pass cluster=ClusterConfig(...), or declare the type with "
            f"managed_by='user' and manage its schema yourself."
        )


def node_target(
    db: coco.ContextKey[ConnectionFactory],
    type_name: str,
    schema: NodeSchema,
    *,
    managed_by: ManagedBy = ManagedBy.SYSTEM,
) -> coco.TargetState[_NodeHandler]:
    """Create a `TargetState` for an Omnigraph node type.

    Must run inside a component: the type's block is marked as owned by
    the app declaring it, which is only known there.
    """
    validate_identifier(type_name, "node type")
    if len(schema.key) != 1:
        # `NodeSchema.from_class` guarantees one, but `NodeSchema` is public.
        # With no key every row would share one identity and an unkeyed
        # `insert` duplicates the row on every run; the engine rejects more
        # than one `@key`.
        raise ValueError(
            f"Node type {type_name!r} must declare exactly one key property, "
            f"not {schema.key!r}. Build the schema with "
            f"`NodeSchema.from_class(..., key=...)`."
        )
    for key_field in schema.key:
        prop = schema.properties[key_field]
        allowed = _KEY_ENCODERS.get(prop.pg_type.rstrip("?"), ())
        if prop.encoder is not None and prop.encoder not in allowed:
            # Edges address a node by the raw key value, so an encoder on the
            # key would make the graph key on something else, and changing
            # it would leave the old node behind. Only the fixed Date and
            # DateTime encodings are allowed, each on its own type.
            raise ValueError(
                f"Node type {type_name!r}: key property {key_field!r} has a custom "
                f"encoder. The key is the node's identity, and edges address it by "
                f"the raw key value, so an encoder there would make the graph's key "
                f"disagree with what edges reference. Normalize the key value before "
                f"declaring the node instead; only the built-in ISO encoding may sit "
                f"on a Date or DateTime key."
            )
    _check_schema_management(db, "Node", type_name, managed_by)
    type_key = _TypeKey(db_key=db.key, type_kind="node", type_name=type_name)
    spec = _TypeSpec(
        schema=schema,
        key=schema.key,
        from_type=None,
        to_type=None,
        managed_by=managed_by,
        owner=current_app_name(),
    )
    return _node_type_provider.target_state(type_key, spec)


def declare_node_target(
    db: coco.ContextKey[ConnectionFactory],
    type_name: str,
    schema: NodeSchema,
    *,
    managed_by: ManagedBy = ManagedBy.SYSTEM,
) -> NodeTarget[Any, coco.PendingS]:
    """Declare a node type target.

    Use this for node types that exist only as edge endpoints — no nodes
    flow into this declaration's own handler.
    """
    provider = coco.declare_target_state_with_child(
        node_target(db, type_name, schema, managed_by=managed_by)
    )
    return NodeTarget(provider, schema, type_name)


async def mount_node_target(
    db: coco.ContextKey[ConnectionFactory],
    type_name: str,
    schema: NodeSchema,
    *,
    managed_by: ManagedBy = ManagedBy.SYSTEM,
) -> NodeTarget[Any]:
    """Mount a node type target ready to receive `declare_node` calls."""
    provider = await coco.mount_target(
        node_target(db, type_name, schema, managed_by=managed_by)
    )
    return NodeTarget(provider, schema, type_name)


def edge_target(
    db: coco.ContextKey[ConnectionFactory],
    type_name: str,
    from_target: NodeTarget[Any],
    to_target: NodeTarget[Any],
    schema: EdgeSchema | None = None,
    *,
    managed_by: ManagedBy = ManagedBy.SYSTEM,
) -> coco.TargetState[_EdgeHandler]:
    """Create a `TargetState` for an Omnigraph edge type.

    Must run inside a component, like `node_target`.
    """
    validate_identifier(type_name, "edge type")
    _check_schema_management(db, "Edge", type_name, managed_by)
    spec = _build_edge_spec(
        schema, from_target, to_target, managed_by, current_app_name()
    )
    key = _TypeKey(db_key=db.key, type_kind="edge", type_name=type_name)
    return _edge_type_provider.target_state(key, spec)


def declare_edge_target(
    db: coco.ContextKey[ConnectionFactory],
    type_name: str,
    from_target: NodeTarget[Any],
    to_target: NodeTarget[Any],
    schema: EdgeSchema | None = None,
    *,
    managed_by: ManagedBy = ManagedBy.SYSTEM,
) -> EdgeTarget[Any, coco.PendingS]:
    """Declare an edge type target."""
    provider = coco.declare_target_state_with_child(
        edge_target(
            db, type_name, from_target, to_target, schema, managed_by=managed_by
        )
    )
    return EdgeTarget(provider, schema, type_name, from_target, to_target)


async def mount_edge_target(
    db: coco.ContextKey[ConnectionFactory],
    type_name: str,
    from_target: NodeTarget[Any],
    to_target: NodeTarget[Any],
    schema: EdgeSchema | None = None,
    *,
    managed_by: ManagedBy = ManagedBy.SYSTEM,
) -> EdgeTarget[Any]:
    """Mount an edge type target ready to receive `declare_edge` calls."""
    provider = await coco.mount_target(
        edge_target(
            db, type_name, from_target, to_target, schema, managed_by=managed_by
        )
    )
    return EdgeTarget(provider, schema, type_name, from_target, to_target)


# ---------------------------------------------------------------------------
# Public exports
# ---------------------------------------------------------------------------

__all__ = [
    "ClusterConfig",
    "ConnectionFactory",
    "EdgeSchema",
    "EdgeTarget",
    "NodeSchema",
    "NodeTarget",
    "OmnigraphType",
    "PropertyDef",
    "SchemaDeployError",
    "ValueEncoder",
    "declare_edge_target",
    "declare_node_target",
    "edge_target",
    "mount_edge_target",
    "mount_node_target",
    "node_target",
]
