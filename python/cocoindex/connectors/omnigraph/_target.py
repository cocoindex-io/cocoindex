"""Omnigraph target connector: schemas, handlers, sink, and user-facing targets."""

from __future__ import annotations

import contextlib
import dataclasses
import datetime
import functools
import hashlib
import re
import types
import typing
import uuid
from collections.abc import AsyncIterator, Callable, Sequence
from typing import Any, Generic, Literal, NamedTuple

import cocoindex as coco
import msgspec
from cocoindex._internal.component_ctx import current_app_name
from cocoindex._internal.context_keys import ContextProvider
from cocoindex._internal.datatype import TypeChecker
from cocoindex._internal.memo_fingerprint import canonical_module_name
from cocoindex.connectorkits import statediff
from cocoindex.connectorkits.fingerprint import fingerprint_object
from cocoindex.connectorkits.target import ManagedBy
from cocoindex.connectors.omnigraph._client import (
    ConnectionFactory,
    OmnigraphCliError,
    _CliClient,
)
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


#: `PropertyDef.encoder` for the scalar `.pg` types whose Python value isn't
#: `json.dumps`-safe as-is. Verified against the engine: `Date` accepts
#: `"2026-01-01"` and `DateTime` accepts ISO with or without a trailing `Z`,
#: so `.isoformat()` suffices for both.
_ENCODERS: dict[str, ValueEncoder] = {
    "Date": _isoformat,
    "DateTime": _isoformat,
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
    # The desired spec, carried through so `_apply_type_actions` can build the
    # `_NodeHandler`/`_EdgeHandler` child for this type without needing a
    # second lookup. `NON_EXISTENCE` when the type is being dropped — nothing
    # to build a child from, and the sink returns no `ChildTargetDef` for a
    # dropped type.
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

    `resolve_system_transition` is what makes this safe: it returns `None`
    when there is nothing tracked, and — crucially — when any previous record
    is user-managed. The connector did not create a `managed_by=user` type and
    does not get to delete it; dropping the block takes every row in it along,
    which is the one irreversible thing this connector can do to data it was
    explicitly told it does not manage. The old hand-rolled path dropped
    unconditionally and structurally could not do better, because the desired
    state is `NON_EXISTENCE` and the tracking record did not persist
    `managed_by` at all.
    """
    main_action, _ = statediff.diff_composite(
        statediff.resolve_system_transition(
            statediff.TrackingRecordTransition(
                coco.NON_EXISTENCE, prev_possible_records, prev_may_be_missing
            )
        )
    )
    if main_action is None:
        # Safe to answer bare `None` *here*, and only here: the type is gone,
        # so it owns no children that could be left without a provider.
        # Answering `None` for a type that still exists is a shipped bug, not
        # a style choice — this is a ROOT provider, the engine only refreshes
        # a child's handler when the parent's own `reconcile()` output carries
        # a fresh `ChildTargetDef`, and an unchanged type that returned `None`
        # starved its own children the very next time it reconciled as
        # unchanged: any node/edge declared under it then found no handler and
        # the engine raised `RuntimeError: provider not ready for target state
        # ...` — confirmed live, and confirmed against the house pattern
        # (sqlite's `_TableHandler.reconcile` never returns bare `None` for
        # exactly this reason). That is why `reconcile` below always returns an
        # output for a type that exists, and why `_apply_type_actions` skips
        # the schema write for an unchanged type but still builds and returns
        # its child handler.
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
        sink=_type_sink,
        tracking_record=coco.NON_EXISTENCE,
        child_invalidation="destructive",
    )


class _TypeHandlerBase(coco.TargetHandler[_TypeSpec, _TypeTrackingRecord, Any]):
    def _render(self, key: _TypeKey, spec: _TypeSpec) -> str:
        raise NotImplementedError

    def reconcile(
        self,
        key: coco.StableKey,
        desired_target_state: _TypeSpec | coco.NonExistenceType,
        prev_possible_records: typing.Collection[_TypeTrackingRecord],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[_TypeAction, _TypeTrackingRecord, Any] | None:
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
        #
        # The same records, diffed as if `prev` were known complete and
        # without `resolve_system_transition`: the non-nullable guard below
        # and the child invalidation both need to see *through* ownership
        # rather than past it.
        #
        # Reading the guard off the write path instead would silently disable
        # it. Under `--reprocess` (or any other `prev_may_be_missing`) the
        # main action becomes "upsert", which empties `property_actions` and
        # leaves the guard nothing to fire on — reintroducing the raw
        # `OG-MF-103` engine error it exists to replace.
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

        # A USER-managed type is never validated, let alone altered, by this
        # connector: the schema is the user's, migrated with `omnigraph schema
        # apply` on their own schedule, and the declaration is simply tracked
        # from then on. An earlier guard compared the declaration against the
        # *tracked* one and refused on any difference — and since reconcile
        # never reads the live schema, the migration that guard advised could
        # not unblock it: every later run failed the same way. The guard below
        # is about a schema write this connector would itself issue, so it
        # applies to SYSTEM-managed types only.
        # `==`, not `is`: ManagedBy is a StrEnum, so a plain `"user"` string
        # compares equal but is not identical — and an identity check would
        # silently fall through to full SYSTEM management, letting the
        # connector rewrite a schema the user said they own. The sibling
        # connectors compare by value (see `connectorkits.statediff`).
        if spec.managed_by != ManagedBy.USER and tracked_main is None:
            # Verified in Task 1: `schema apply` rejects adding a NON-nullable
            # property to an existing type (OG-MF-103, "requires a backfill and
            # is not supported in schema migration v1"). Only initial creation
            # or a full rebuild may declare one, so this fires on the in-place
            # path only — `tracked_main is None`, never on a "replace".
            # Failing here names the offending property in Python; letting it
            # through surfaces as an opaque error from the Rust binary with
            # nothing pointing at the user's dataclass. We do NOT silently
            # escalate to a destructive rebuild — dropping a populated type
            # because someone added a field is not a call the connector gets to
            # make on the user's behalf.
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
            sink=_type_sink,
            tracking_record=desired,
            child_invalidation=child_invalidation,
        )


class _NodeTypeHandler(_TypeHandlerBase):
    def _render(self, key: _TypeKey, spec: _TypeSpec) -> str:
        assert isinstance(spec.schema, NodeSchema)
        return spec.schema.render(key.type_name, owner=spec.owner)


class _EdgeTypeHandler(_TypeHandlerBase):
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
    """Fingerprint of the *encoded* property values, not the values themselves.

    Encoded, because that is what the graph holds: fingerprinting the raw
    Python values let a changed `PropertyDef.encoder` rewrite every stored
    value without change detection ever noticing.

    Internal-state size grows with target-state count, so storing full property
    blobs for every entity in a large graph would bloat LMDB for no
    reconciliation benefit.

    `array_like` for the same reason: this is the highest-cardinality record
    the connector persists — one per node and per edge — so encoding it as an
    object would pay for the literal key `"fingerprint"` on every single one,
    which is exactly the overhead the paragraph above exists to avoid. The
    siblings go further and track a bare `bytes` (neo4j's `_RowFingerprint`);
    the struct is kept here only for the nominal typing.
    """

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
    op: str  # "insert" | "replace" | "delete"
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


class _NodeHandler(coco.TargetHandler[_NodeValue, _EntityTrackingRecord, Any]):
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
    ) -> coco.TargetReconcileOutput[_NodeAction, _EntityTrackingRecord, Any] | None:
        # A composite key arrives as a tuple; a single-field key arrives as
        # the bare scalar, so it must be wrapped before zipping with
        # `key_fields`.
        key_tuple = key if isinstance(key, tuple) else (key,)
        if len(key_tuple) != len(self._key_fields):
            raise ValueError(
                f"Node type {self._type_name!r} declares key fields "
                f"{self._key_fields!r} but the target state's key is "
                f"{key_tuple!r}."
            )
        coco_key = derive_coco_key(key_tuple)

        if coco.is_non_existence(desired_target_state):
            # `prev_may_be_missing` means we may simply have lost track of a
            # row that is still in the graph — after a `--reprocess`, after
            # internal state was lost while the store persisted, or when a
            # prior delete's tracking record was dropped before the delete
            # landed. Only "nothing tracked AND the engine is certain nothing
            # is missing" proves there is nothing to remove; anything else
            # must still issue the delete, or the row survives untracked
            # forever (nothing else ever removes it). Deleting a nonexistent
            # entity by `coco_key` is a documented no-op, so the defensive
            # delete costs nothing when it turns out to be unnecessary. Same
            # guard every sibling connector uses, and the same reading the
            # insert path below already applies to this exact signal.
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
                sink=_entity_sink,
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
            sink=_entity_sink,
            tracking_record=desired,
        )


class _EdgeHandler(coco.TargetHandler[_EdgeValue, _EntityTrackingRecord, Any]):
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
    ) -> coco.TargetReconcileOutput[_EdgeAction, _EntityTrackingRecord, Any] | None:
        # The edge's StableKey is its (from_id, to_id) tuple.
        coco_key = derive_coco_key(key)

        if coco.is_non_existence(desired_target_state):
            # `prev_may_be_missing` means we may simply have lost track of a
            # row that is still in the graph — after a `--reprocess`, after
            # internal state was lost while the store persisted, or when a
            # prior delete's tracking record was dropped before the delete
            # landed. Only "nothing tracked AND the engine is certain nothing
            # is missing" proves there is nothing to remove; anything else
            # must still issue the delete, or the row survives untracked
            # forever (nothing else ever removes it). Deleting a nonexistent
            # entity by `coco_key` is a documented no-op, so the defensive
            # delete costs nothing when it turns out to be unnecessary. Same
            # guard every sibling connector uses, and the same reading the
            # insert path below already applies to this exact signal.
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
                sink=_entity_sink,
                tracking_record=coco.NON_EXISTENCE,
            )

        # An edge `insert` is strict — it never merges, so a changed edge
        # can't be re-inserted without duplicating it, and there is no edge
        # `update`. A brand-new edge (nothing tracked yet, and the engine is
        # certain nothing is missing) is the one case a bare insert is safe;
        # anything already tracked that changed, OR anything whose prior
        # state is merely uncertain (empty `prev_possible_records` with
        # `prev_may_be_missing` — e.g. internal state lost while the target
        # persists, or a crash between emitting a delete and it landing),
        # must be deleted and re-inserted rather than risk duplicating an
        # edge that's still there. Encoded here as a single "replace" op —
        # `plan_commits` (Task 9) is what actually splits it into two
        # mutations, since a delete and an insert can't share one mutation.
        # Deleting a nonexistent edge by `coco_key` is a no-op, so "replace"
        # is always safe even when the edge turns out not to have existed.
        #
        # Do NOT port this fork to `statediff.diff`: for edges it is inverted.
        # `diff` answers "insert" for `(prev=[], prev_may_be_missing=True)` —
        # the reading that suits a keyed upsert, where re-inserting converges.
        # An Omnigraph edge has no key to upsert on, so that exact case is the
        # one that needs "replace", and taking `diff`'s answer would duplicate
        # a live edge every time internal state was lost. The container
        # handler above uses `statediff` precisely because a type CAN be
        # re-declared idempotently; an edge cannot.
        encoded = _encode_properties(
            desired_target_state.properties, self._property_defs
        )
        desired = _entity_record(encoded)
        if not prev_possible_records and not prev_may_be_missing:
            op = "insert"
        elif not _needs_write(desired, prev_possible_records, prev_may_be_missing):
            return None
        else:
            op = "replace"

        return coco.TargetReconcileOutput(
            action=_EdgeAction(
                op,
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
            sink=_entity_sink,
            tracking_record=desired,
        )


# ---------------------------------------------------------------------------
# Commit planning: reconcile actions -> ordered Mutations, one per commit
# ---------------------------------------------------------------------------

#: Chunk cap for render_query: a single commit combining more than this
#: many entities of one type risks an unreasonably large query. Deliberately
#: a module constant, not a public kwarg — tests monkeypatch it directly.
_MAX_ENTITIES_PER_TYPE = 8192

#: Cap on a whole commit, across every type in it. The per-type cap above
#: deliberately gives a large node type and a large edge type their own
#: budgets rather than making them share one — but with nothing bounding the
#: total, a sync touching N types produced a single commit of N x 8192
#: entities (10 types = ~9.5 MB of GQ), and `_mutate_with_endpoint_retry`
#: re-sends that whole commit on every endpoint-stub retry. Set to 4x the
#: per-type cap: enough headroom for the common node-plus-its-edges shape to
#: stay in one commit, while still bounding the pathological case.
_MAX_ENTITIES_PER_COMMIT = 4 * _MAX_ENTITIES_PER_TYPE


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


def _chunk_by_type(
    items: Sequence[tuple[str, Statement]], cap: int, total_cap: int
) -> list[list[Statement]]:
    """Split into chunks bounded two ways: no chunk holds more than `cap`
    mutations for any single `type_name`, and none holds more than
    `total_cap` overall.

    The per-type cap is checked per type, not across the whole phase, so a
    large node type and a large edge type each get their own budget rather
    than sharing one. `total_cap` is what keeps that from being unbounded:
    without it a sync touching N types emitted one commit of N x `cap`
    entities, which is re-sent whole on every endpoint-stub retry.
    """
    chunks: list[list[Statement]] = []
    current: list[Statement] = []
    counts: dict[str, int] = {}
    for type_name, mutation in items:
        if counts.get(type_name, 0) >= cap or len(current) >= total_cap:
            chunks.append(current)
            current = []
            counts = {}
        current.append(mutation)
        counts[type_name] = counts.get(type_name, 0) + 1
    if current:
        chunks.append(current)
    return chunks


def plan_commits(actions: Sequence[Any]) -> list[Query]:
    """Bucket reconcile actions into three phases and combine each phase's
    mutations into one commit per phase (chunked past `_MAX_ENTITIES_PER_TYPE`).

    Phase A (pre-deletes): deletes of *replaced* edges only. Must precede
    their re-insert in phase B — delete selects on `coco_key`, and a
    replaced edge keeps the same one, so insert-then-delete would remove the
    new edge too (verified against the engine: leaves zero edges).

    Phase B (upserts): node upserts, THEN edge inserts (both brand-new and
    replaced). The order within the phase matters even though the whole
    phase is one commit: statements in a combined query execute in the
    order written, so an edge insert placed ahead of its own endpoint's
    upsert fails with "not found" and forces the stub-and-retry path on
    every run — a costly recovery meant for genuine cross-component
    ordering violations, not for two writes the same component is issuing
    together. Grouping nodes first makes that self-inflicted case
    impossible; a failure can then only mean the endpoint really is owned
    by another component that hasn't run yet.

    No endpoint stubs here — a keyed `insert` in Omnigraph is a
    full-record replace, not a partial merge (verified against the engine),
    so unconditionally stubbing an edge's endpoints would silently null out
    any other nullable properties a node already has. Instead the sink
    applies an edge insert optimistically and only builds a stub, for
    *just* the specific endpoint the engine reports missing, after that
    insert fails with a "not found" error — see `_apply_entity_actions`.

    Phase C (post-deletes): edge deletes, then node deletes — of removals
    only (a replaced edge's delete is phase A, not this). Edges must be gone
    before their nodes.

    A and C cannot merge into one commit even though both are deletes: A
    must precede B and C must follow it.
    """
    phase_a: list[tuple[str, Statement]] = []
    phase_b_nodes: list[tuple[str, Statement]] = []
    phase_b_edges: list[tuple[str, Statement]] = []
    phase_c_edges: list[tuple[str, Statement]] = []
    phase_c_nodes: list[tuple[str, Statement]] = []

    for action in actions:
        if isinstance(action, _NodeAction):
            if action.op == "upsert":
                phase_b_nodes.append(
                    (
                        action.type_name,
                        build_node_upsert(
                            action.type_name, action.properties, action.coco_key
                        ),
                    )
                )
            elif action.op == "delete":
                phase_c_nodes.append(
                    (
                        action.type_name,
                        build_node_delete(action.type_name, action.coco_key),
                    )
                )
            else:
                raise ValueError(f"unknown node action op: {action.op!r}")
        elif isinstance(action, _EdgeAction):
            if action.op == "delete":
                phase_c_edges.append(
                    (
                        action.type_name,
                        build_edge_delete(action.type_name, action.coco_key),
                    )
                )
            elif action.op in ("insert", "replace"):
                if action.op == "replace":
                    phase_a.append(
                        (
                            action.type_name,
                            build_edge_delete(action.type_name, action.coco_key),
                        )
                    )
                from_ref = _endpoint_ref(action.from_id)
                to_ref = _endpoint_ref(action.to_id)
                phase_b_edges.append(
                    (
                        action.type_name,
                        build_edge_insert(
                            action.type_name,
                            from_ref,
                            to_ref,
                            action.properties,
                            action.coco_key,
                        ),
                    )
                )
            else:
                raise ValueError(f"unknown edge action op: {action.op!r}")
        else:
            raise TypeError(f"unknown action type: {type(action)!r}")

    phase_b = phase_b_nodes + phase_b_edges
    phase_c = phase_c_edges + phase_c_nodes
    commits: list[Query] = []
    for phase in (phase_a, phase_b, phase_c):
        if not phase:
            continue
        for chunk in _chunk_by_type(
            phase, _MAX_ENTITIES_PER_TYPE, _MAX_ENTITIES_PER_COMMIT
        ):
            commits.append(render_query(chunk))
    return commits


# ---------------------------------------------------------------------------
# Sinks
# ---------------------------------------------------------------------------


#: Emitted by the engine when an edge insert references a node that doesn't
#: exist yet — verified against the binary: `src '<key>' not found in
#: <Type>` for the `from` endpoint, `dst '<key>' not found in <Type>` for
#: `to`. The CLI colors its stderr with ANSI escapes even when not a TTY
#: (verified: piped subprocess output still carries them), and a leading
#: `\b` would break on that — the color-reset sequence right before "src"/
#: "dst" ends in a letter (`\x1b[91m`), so there's no word boundary there.
#: `(src|dst) '...' not found in` is a tight enough anchor on its own; an
#: unrelated error would need this exact multi-token phrase to trigger a
#: spurious stub.
# Match the closing quote by its full error-message suffix rather than treating
# every apostrophe as the end of the key. String keys are allowed to contain
# apostrophes (for example, ``O'Brien``).
_ENDPOINT_NOT_FOUND_RE = re.compile(r"(src|dst) '([\s\S]*?)' not found in (\w+)")


def _parse_missing_endpoint(error: OmnigraphCliError) -> tuple[str, str, str] | None:
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
    client: _CliClient,
    commit: Query,
    *,
    branch: str,
    edge_actions: Sequence[_EdgeAction],
) -> None:
    """Apply `commit` optimistically — without any endpoint stub — since the
    common case is that both endpoints either already exist or aren't
    needed at all. A keyed `insert` in Omnigraph is a full-record replace,
    not a partial merge (verified against the engine), so unconditionally
    stubbing every edge's endpoints was proven to silently null out a
    node's other nullable properties whenever the owning component had
    already written real values. Building a stub only reactively, from a
    "not found" failure, is provably safe: the engine has just confirmed
    the node doesn't exist, so there's nothing to wipe.

    The engine reports only the *first* missing endpoint per attempt
    (verified: with both `from` and `to` absent, only `src` is reported),
    and with up to 1024 components running concurrently, an edge component
    preceding the components that own its endpoints is ordinary, not
    exotic. A commit carries every edge of a component, so the number of
    stub rounds it may need is the number of distinct endpoints those
    edges reference — not two, which is the budget for a single edge and
    failed any commit with three or more absent endpoints.

    The loop is bounded by exactly that set: a stub is only ever built for
    an endpoint some edge in `edge_actions` references, and each `(type,
    key)` is stubbed at most once. A "not found" that names an endpoint
    already stubbed, or one no edge here references, propagates instead
    of spinning.
    """
    stubbed: set[tuple[str, str]] = set()
    while True:
        try:
            await client.mutate(commit, branch=branch)
            return
        except OmnigraphCliError as e:
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


#: Prefix of every branch this connector creates. Nothing else creates
#: branches under it, so a branch carrying it that is visible while the
#: store lock is held was abandoned by a process that died mid-sync.
_SCRATCH_BRANCH_PREFIX = "coco_scratch_"


@contextlib.asynccontextmanager
async def _scratch_branch(client: _CliClient, *, frm: str) -> AsyncIterator[str]:
    """Own a scratch branch for the duration of the block.

    Holds the store lock from before the branch is created until after it
    is deleted: a non-main branch makes Omnigraph reject every schema apply
    on the store, and schema reconciliation takes the same lock, so a
    concurrent type update never observes the transient branch and fails
    spuriously. The branch's own liveness lock is held for the same span,
    which is what lets `_reap_abandoned_scratch_branches` tell an abandoned
    branch from a live one even when the store lock did not cover both
    parties.

    The branch is deleted whether the body succeeded or failed — `branch
    merge` does not delete its source (we don't pass `--delete-branch`,
    which would only cover the success path anyway), and a leftover
    non-main branch blocks every subsequent `schema apply` on the store
    (verified live). `branch_delete` itself raises when the branch doesn't
    exist, so if `branch_create` never landed there is nothing to delete;
    once it did, a cleanup failure must be reported — treating the sync as
    successful would persist a hidden operational failure.
    """
    async with client.store_lock():
        name = f"{_SCRATCH_BRANCH_PREFIX}{uuid.uuid4().hex}"
        async with client.hold_scratch_branch(name):
            await client.branch_create(name, frm=frm)
            try:
                yield name
            finally:
                await client.branch_delete(name)


async def _reap_abandoned_scratch_branches(client: _CliClient) -> list[str]:
    """Delete every abandoned `coco_scratch_*` branch and return their names.

    Runs under the store lock, and deletes only a branch whose liveness
    lock it can take: a live scratch branch holds that lock for its whole
    lifetime inside `_scratch_branch`, so one whose lock is free was left
    behind by a process that died between creating and deleting it.
    Abandonment is established before deletion, not inferred from the
    branch merely being visible — a reaper that took a different store
    lock than the branch's owner (verified live with two spellings of one
    store URI) deleted a branch that was still in use. User branches are
    never touched.
    """
    reaped: list[str] = []
    for name in await client.branch_list():
        if not name.startswith(_SCRATCH_BRANCH_PREFIX):
            continue
        async with client.claim_scratch_branch(name) as abandoned:
            if abandoned:
                await client.branch_delete(name)
                reaped.append(name)
    return reaped


async def _apply_schema_recovering_abandoned_branches(
    client: _CliClient, schema_pg: str
) -> None:
    """`schema apply`, recovering from a scratch branch an interrupted sync
    left behind.

    Omnigraph refuses every schema change while any non-main branch exists
    ("schema apply requires a graph with only main; found non-main
    branches: ..."). An abandoned `coco_scratch_*` branch therefore blocked
    the store's schema for good, since nothing ever listed or deleted it.
    On that refusal, reap the connector's own abandoned branches and retry
    once; if the refusal was about a user's branch, nothing is reaped and
    the original error propagates.
    """
    try:
        await client.apply_schema(schema_pg)
    except OmnigraphCliError as e:
        if "non-main branches" not in str(e):
            raise
        if not await _reap_abandoned_scratch_branches(client):
            raise
        await client.apply_schema(schema_pg)


async def _keep_referenced_nodes(
    client: _CliClient, actions: Sequence[_NodeAction | _EdgeAction], *, branch: str
) -> list[_NodeAction | _EdgeAction]:
    """Turn the delete of a node that an edge outside `actions` still
    references into a key-only upsert of that node.

    Deleting a node cascades to its edges in the graph (`affected_edges` in
    the mutation's result, verified against the engine) — silently, from
    the point of view of whichever component declared those edges: their
    tracking still says they exist, so once the node came back the edges'
    reconcile found nothing to do and they were gone for good. The edge is
    the declared state that still holds, and the connector already has a
    shape for "an edge whose endpoint nobody declares": the key-only stub
    it writes when an edge arrives before its node. A referenced node is
    reduced to that same stub, so the edge survives, and the node's next
    declaration fills it back in. Edges this batch deletes itself do not
    count, so a node undeclared together with all of its edges is deleted
    outright, as before.

    The edge types to look at come from the live schema, one read per
    batch; nodes of a type no edge type references cost nothing more. Each
    remaining delete costs one read per (edge type, end) at which its type
    sits. An edge inserted by another component between that read and this
    batch's commit is still cascaded; the window is the same one every
    unlocked entity write has.
    """
    deletes = [a for a in actions if isinstance(a, _NodeAction) and a.op == "delete"]
    if not deletes:
        return list(actions)
    existing = await client.read_schema()
    if existing is None:
        return list(actions)
    patterns = {
        type_name: incident_edge_patterns(existing, type_name)
        for type_name in {a.type_name for a in deletes}
    }
    if not any(patterns.values()):
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
        referenced = False
        for pattern in patterns[action.type_name]:
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
    context_provider: ContextProvider, actions: Sequence[_NodeAction | _EdgeAction]
) -> None:
    if not actions:
        return
    by_db: dict[str, list[_NodeAction | _EdgeAction]] = {}
    for action in actions:
        by_db.setdefault(action.key.db_key, []).append(action)

    for db_key, db_actions in by_db.items():
        conn: ConnectionFactory = context_provider.get(db_key)
        client = _CliClient(conn)
        db_actions = await _keep_referenced_nodes(
            client, db_actions, branch=conn.branch
        )
        commits = plan_commits(db_actions)
        if not commits:
            continue
        edge_actions = [a for a in db_actions if isinstance(a, _EdgeAction)]
        if len(commits) == 1:
            # One commit is atomic on its own, so send it straight to the
            # live branch — no scratch branch, no merge. Deliberately NOT
            # `_mutate_with_endpoint_retry`: that recovery is two commits
            # (the stub, then the retried commit), which on the live branch
            # would leave an orphan stub node behind — untracked, and
            # visible to readers — if the retry then failed for an
            # unrelated reason. A failed mutation applies nothing (verified
            # against the engine; see TestMutationAtomicityLive), so the
            # optimistic attempt costs nothing but the round-trip, and the
            # moment recovery is actually needed this falls through to the
            # scratch-branch path below and redoes the whole thing there.
            try:
                await client.mutate(commits[0], branch=conn.branch)
                continue
            except OmnigraphCliError as e:
                if _parse_missing_endpoint(e) is None:
                    raise
        # More than one commit needs atomicity across all of them together,
        # and one CLI invocation is one commit — so apply the whole sequence
        # on a scratch branch and merge it in as a single step. `branch merge`
        # has no compare-and-swap precondition (as of 0.10.0 only `mutate`
        # takes `--if-commit`); a conflicted merge simply surfaces as a
        # non-zero exit from `branch_merge` and propagates like any other
        # failure below.
        async with _scratch_branch(client, frm=conn.branch) as scratch:
            for commit in commits:
                await _mutate_with_endpoint_retry(
                    client, commit, branch=scratch, edge_actions=edge_actions
                )
            await client.branch_merge(scratch, into=conn.branch)


_entity_sink = coco.TargetActionSink.from_async_fn(_apply_entity_actions)


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

    Same rule and same consequence as `_check_no_kind_clash` below, but
    applied to the batch rather than the live schema — which is the only
    place it can be caught when both types are new. The live-schema check
    can only ever see types that are ALREADY in the graph, so two fresh
    clashing types slip past it on an initialized store, and on a fresh
    store it isn't reached at all (the `init` path returns first).
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
    blocked = []
    for node_name, is_drop in sorted(going_away.items()):
        edge_names = sorted(
            set(edge_types_referencing(existing_pg, node_name)) - also_dropped
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


async def _apply_type_schema(
    client: _CliClient, actions: Sequence[_TypeAction]
) -> None:
    """Fold every schema-changing action for one graph into one read and at
    most two writes.

    Omnigraph's schema is applied whole-graph, not per type (verified
    against the engine: applying a single new type's fragment alone
    silently dropped every other existing type). Task 6 reconciles each
    type independently and only ever renders that one type's fragment, so
    every action funnels through here: read the current whole-graph
    source, fold this batch's fragments into it in memory, and write the
    result back. The whole batch arrives in one call — the sink's batcher
    never has more than one group in flight — so doing a full
    read-merge-write round-trip per action would be N times the CLI
    invocations for the same end state.

    `kind` doesn't select a code path. A brand-new type's action is always
    `"create"`, even when it's the *second* type ever declared on an
    already-`init`'d graph, so `client.read_schema()` returning `None` is
    the only signal for "not yet initialized" — it subsumes the old
    create-or-alter stderr-sniffing fallback entirely.

    Two exceptions to the single write:

    `"drop"` removes the type's block. Nothing else deletes a dropped
    type's nodes or edges: when a container target state disappears the
    engine emits no per-child deletes, so if this didn't drop the block,
    the type and every row in it would persist forever, untracked.

    `"replace"` (a `@key` or edge-endpoint change) cannot go through the
    ordinary merge: the engine flatly rejects an in-place key change
    ("removing property constraints ... not supported in schema migration
    v1", verified against the binary — not a soft/hard-drop distinction,
    `--allow-data-loss` doesn't help). It needs a genuine rebuild, so a
    batch containing any `"replace"` costs one extra `schema apply` that
    lands the replaced types' removal first, before the final apply
    re-adds them under their new definitions. `child_invalidation=
    "destructive"` on a `"replace"` already tells every child entity it
    must be re-declared from scratch, which is exactly what the drop half
    does at the schema level.
    """
    pending = [
        a
        for a in actions
        if a.main_action is not None or a.property_actions or a.release_ownership
    ]
    if not pending:
        return

    # Omnigraph applies schema source as a whole. Keep the read, in-memory
    # merge, and every resulting apply under one store-scoped lock so two app
    # updates cannot both read the same schema and overwrite each other's
    # changes with competing complete-schema writes.
    async with client.store_lock():
        await _apply_type_schema_locked(client, pending)


async def _apply_type_schema_locked(
    client: _CliClient, pending: Sequence[_TypeAction]
) -> None:
    # Before the `init` path below, not after it: on a fresh graph that path
    # returns first, so a clash inside the very first batch was never checked.
    _check_no_kind_clash_within_batch(pending)

    existing = await client.read_schema()
    if existing is None:
        # Nothing to read, nothing to drop from, and `init` takes the
        # complete schema — so build the whole thing in memory and create
        # the graph in a single call.
        merged = ""
        for action in pending:
            if coco.is_non_existence(action.spec) or action.release_ownership:
                # Nothing to drop from, and nothing of the user's to release.
                continue
            assert action.pg_fragment is not None
            merged = merge_type_into_schema(
                merged, action.key.type_kind, action.key.type_name, action.pg_fragment
            )
        if merged:
            await client.init_graph(merged)
        return

    _check_no_kind_clash(existing, pending)

    # A batch is one app's, and that app's ownership property is what marks
    # the blocks it may take along or release.
    owners = {a.owner for a in pending}
    assert len(owners) == 1, owners
    marker = ownership_property(owners.pop())

    # A node type's drop may find an edge type of this app still pointing
    # at it. Every mounted type is its own component and all of them share
    # one sink batcher, so when an app is dropped the edge type's own
    # removal is queued behind this very batch — waiting for it here
    # deadlocks until a timeout (verified live). Its removal is coming
    # regardless, so take this app's own edge types along now; the edge's
    # batch then finds nothing left to remove. An edge type marked as
    # another app's is that app's to remove — its removal is not coming,
    # and a marker that only said "some app of this connector" let one
    # app's drop delete another's edges — and one without any marker is a
    # user's or another tool's; neither is removed on their behalf, and a
    # `@key` change keeps the edge declared, so all three still fail here.
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

    replaced = [
        (a.key.type_kind, a.key.type_name)
        for a in pending
        if a.main_action == "replace"
    ]
    if replaced:
        intermediate = existing
        for kind, type_name in replaced:
            intermediate = remove_type_from_schema(intermediate, kind, type_name)
        await _apply_schema_recovering_abandoned_branches(client, intermediate)
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
        await _apply_schema_recovering_abandoned_branches(client, desired)


async def _apply_type_actions(
    context_provider: ContextProvider, actions: Sequence[_TypeAction]
) -> list[coco.ChildTargetDef[_NodeHandler | _EdgeHandler] | None]:
    actions_list = list(actions)
    outputs: list[coco.ChildTargetDef[_NodeHandler | _EdgeHandler] | None] = [
        None
    ] * len(actions_list)

    by_db: dict[str, list[int]] = {}
    for i, action in enumerate(actions_list):
        by_db.setdefault(action.key.db_key, []).append(i)

    for db_key, idxs in by_db.items():
        conn: ConnectionFactory = context_provider.get(db_key)
        client = _CliClient(conn)
        await _apply_type_schema(client, [actions_list[i] for i in idxs])

        for i in idxs:
            action = actions_list[i]
            spec = action.spec
            if coco.is_non_existence(spec):
                # No child handler for a type that no longer exists.
                continue
            # An unchanged type wrote no schema, but its child handler must
            # still be rebuilt here, or the engine leaves this type's own
            # children without a provider on the next run.
            if spec.from_type is None:
                # Node type: `key` is the node's own key field names.
                assert spec.schema is not None
                outputs[i] = coco.ChildTargetDef(
                    handler=_NodeHandler(
                        action.key.type_name,
                        spec.key,
                        action.key,
                        spec.schema.properties,
                    )
                )
            else:
                # Edge type: `from_type`/`to_type` name the endpoints, and
                # `from_key_property`/`to_key_property` are the endpoints' own
                # key definitions — needed to build correctly typed stubs.
                assert spec.to_type is not None
                assert spec.from_key_property is not None
                assert spec.to_key_property is not None
                outputs[i] = coco.ChildTargetDef(
                    handler=_EdgeHandler(
                        action.key.type_name,
                        action.key,
                        spec.from_type,
                        spec.to_type,
                        spec.from_key_property,
                        spec.to_key_property,
                        spec.schema.properties if spec.schema is not None else {},
                    )
                )
    return outputs


_type_sink: coco.TargetActionSink[_TypeAction, _NodeHandler | _EdgeHandler] = (
    coco.TargetActionSink.from_async_fn(_apply_type_actions)
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
        properties = _record_to_dict(record, self._schema) if record is not None else {}
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


def _record_to_dict(
    record: Any, schema: NodeSchema | EdgeSchema | None
) -> dict[str, Any]:
    """Extract `{property_name: raw_value}` from a dataclass or dict record.

    Values are NOT encoded here — `_encode_properties` (used by
    `_NodeHandler`/`_EdgeHandler.reconcile`, above) applies `PropertyDef.encoder`
    at commit time, once the property's `pg_type` is back in scope.
    """
    property_names = schema.properties.keys() if schema is not None else None
    if isinstance(record, dict):
        if schema is None:
            return dict(record)
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
    if property_names is None:
        return {f.name: getattr(record, f.name) for f in dataclasses.fields(record)}
    return {name: getattr(record, name) for name in property_names}


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
    if len(schema.key) != 1:
        # `NodeSchema.from_class` guarantees this, but `NodeSchema` is public
        # and can be built by hand, so don't let it fall through to an
        # unpacking error with nothing naming the type.
        raise ValueError(
            f"Node type {target.type_name!r} cannot be referenced as an edge "
            f"endpoint ({role}): its key is {schema.key!r}, and an endpoint is "
            f"addressed by a single key value."
        )
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
    """Pure seam between `edge_target()` and `_TypeSpec` construction — kept
    separate so the endpoint key-field wiring (Task 10 step 4) is directly
    testable without reaching into `coco.TargetState`'s private value."""
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
    if not schema.key:
        # `NodeSchema.from_class` guarantees a key, but `NodeSchema` is public
        # and can be built by hand — and an empty key is silently catastrophic
        # rather than merely wrong: the type renders with no `@key`, every row
        # derives the same `coco_key` from the same empty tuple and so shares
        # one StableKey (last write wins in tracking), while an unkeyed
        # Omnigraph insert is a strict insert that duplicates the row on every
        # re-run. `_validate_edge_endpoint` already refuses this, but only for
        # types used as endpoints.
        raise ValueError(
            f"Node type {type_name!r} must declare a key: its schema's key is "
            f"empty, so every row would share one identity. Build the schema "
            f"with `NodeSchema.from_class(..., key=...)`, or pass a non-empty "
            f"`key` to `NodeSchema`."
        )
    for key_field in schema.key:
        prop = schema.properties[key_field]
        allowed = _KEY_ENCODERS.get(prop.pg_type.rstrip("?"), ())
        if prop.encoder is not None and prop.encoder not in allowed:
            # The key is the node's identity, and an edge addresses that
            # identity by the raw key value its author passed to
            # `declare_edge`. A custom encoder would make the graph key on
            # something else: changing one from `str.lower` to `str.upper`
            # upserted a second node under the new spelling and left the old
            # one behind, and an edge declared with the raw value would never
            # find either. Only the fixed Date/DateTime encoders are allowed
            # on a key, each on the type it encodes — pure functions of the
            # value, never changing, and such types cannot be edge endpoints
            # at all.
            raise ValueError(
                f"Node type {type_name!r}: key property {key_field!r} has a custom "
                f"encoder. The key is the node's identity, and edges address it by "
                f"the raw key value, so an encoder there would make the graph's key "
                f"disagree with what edges reference. Normalize the key value before "
                f"declaring the node instead; only the built-in ISO encoding may sit "
                f"on a Date or DateTime key."
            )
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
    "ConnectionFactory",
    "EdgeSchema",
    "EdgeTarget",
    "NodeSchema",
    "NodeTarget",
    "OmnigraphType",
    "PropertyDef",
    "ValueEncoder",
    "declare_edge_target",
    "declare_node_target",
    "edge_target",
    "mount_edge_target",
    "mount_node_target",
    "node_target",
]
