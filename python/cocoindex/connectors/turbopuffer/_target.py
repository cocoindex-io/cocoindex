"""
Turbopuffer target for CocoIndex.

This module provides a two-level target state system for Turbopuffer:
1. Namespace level: tracks namespace-level configuration (vector schema, distance metric)
   and clears the namespace when it must be rebuilt.
2. Row level: upserts/deletes individual documents within namespaces.

Turbopuffer creates namespaces implicitly on first write, so there is no explicit
"create namespace" call.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import (
    Any,
    Collection,
    Generic,
    Literal,
    NamedTuple,
    Sequence,
)

import msgspec
import numpy as np

try:
    from turbopuffer import AsyncTurbopuffer, NotFoundError  # type: ignore
except ImportError as e:
    raise ImportError(
        "turbopuffer is required to use the Turbopuffer connector. "
        "Please install cocoindex[turbopuffer]."
    ) from e

import cocoindex as coco
from cocoindex.connectorkits import statediff, target
from cocoindex.connectorkits.fingerprint import fingerprint_object
from cocoindex._internal.datatype import TypeChecker
from cocoindex.resources import schema as res_schema
from cocoindex._internal.context_keys import ContextKey, ContextProvider


# Type aliases
_RowId = str | int
_RowFingerprint = bytes
_ROW_ID_CHECKER: TypeChecker[str | int] = TypeChecker(str | int)  # type: ignore[arg-type]


DistanceMetric = Literal["cosine_distance", "euclidean_squared"]


class VectorDef(NamedTuple):
    """Turbopuffer vector specification.

    Args:
        schema: VectorSchemaProvider, ContextKey wrapping one, or an explicit VectorSchema.
                Defines the vector dimension and dtype (np.float32 → ``[N]f32``,
                np.float16 → ``[N]f16``).
    """

    schema: (
        res_schema.VectorSchemaProvider
        | coco.ContextKey[res_schema.VectorSchemaProvider]
    )


class _ResolvedVectorDef(msgspec.Struct, frozen=True, tag=True):
    """Resolved single (unnamed) vector specification."""

    schema: res_schema.VectorSchema


class _ResolvedNamedVectorsDef(msgspec.Struct, frozen=True, tag=True):
    """Resolved named vectors specification (multiple named vectors per namespace)."""

    vectors: dict[str, _ResolvedVectorDef]


async def _resolve_vector_def(vector_def: VectorDef) -> _ResolvedVectorDef:
    vs = await res_schema.get_vector_schema(vector_def.schema)
    if vs is None:
        raise ValueError(f"Invalid vector definition: {vector_def}")
    # Validate dtype upfront so bad schemas fail at construction time, not on
    # the first write. Discards the return — used for its raise side effect.
    _vector_type_str(vs)
    return _ResolvedVectorDef(schema=vs)


# Default vector field name in turbopuffer for an unnamed vector.
_DEFAULT_VECTOR_FIELD = "vector"

# Field names that cannot be used as named vector fields — they would collide
# with turbopuffer's row id at the wire level.
_RESERVED_VECTOR_FIELD_NAMES = frozenset({"id"})


@dataclass(slots=True)
class NamespaceSchema:
    """Schema definition for a Turbopuffer namespace.

    Defines the vector field(s) and the namespace-level distance metric. Turbopuffer
    applies a single ``distance_metric`` across all vector columns in a namespace.

    Use the async ``create()`` classmethod to construct from unresolved
    ``VectorDef`` (which may reference a ``VectorSchemaProvider``).
    """

    _vectors: _ResolvedVectorDef | _ResolvedNamedVectorsDef
    _distance: DistanceMetric

    def __init__(
        self,
        vectors: _ResolvedVectorDef | _ResolvedNamedVectorsDef,
        distance: DistanceMetric,
    ) -> None:
        """Construct from pre-resolved vector definitions.

        For constructing from unresolved ``VectorDef``, use the async
        classmethod ``create`` instead.
        """
        self._vectors = vectors
        self._distance = distance

    @classmethod
    async def create(
        cls,
        vectors: VectorDef | dict[str, VectorDef],
        *,
        distance: DistanceMetric = "cosine_distance",
    ) -> "NamespaceSchema":
        """Create a NamespaceSchema by resolving vector definitions.

        Args:
            vectors: Either a single ``VectorDef`` (for an unnamed vector stored
                under turbopuffer's default ``"vector"`` field) or a dict mapping
                vector field names to ``VectorDef`` (for named vectors).
            distance: Distance metric applied to all vector columns in the namespace.
                Default: ``"cosine_distance"``.
        """
        resolved: _ResolvedVectorDef | _ResolvedNamedVectorsDef
        if isinstance(vectors, VectorDef):
            resolved = await _resolve_vector_def(vectors)
        elif isinstance(vectors, dict):
            if not vectors:
                raise ValueError(
                    "Named-vectors dict is empty; declare at least one vector field."
                )
            reserved = _RESERVED_VECTOR_FIELD_NAMES & set(vectors)
            if reserved:
                raise ValueError(
                    f"Vector field name {sorted(reserved)[0]!r} is reserved "
                    f"(it collides with the row id at the wire level)."
                )
            resolved = _ResolvedNamedVectorsDef(
                vectors={
                    name: await _resolve_vector_def(vd) for name, vd in vectors.items()
                }
            )
        else:
            raise ValueError(f"Invalid vector definition: {vectors}")
        return cls(resolved, distance)

    @property
    def vectors(self) -> _ResolvedVectorDef | _ResolvedNamedVectorsDef:
        return self._vectors

    @property
    def distance(self) -> DistanceMetric:
        return self._distance


@dataclass(slots=True)
class Row:
    """A document to write to a turbopuffer namespace.

    Args:
        id: Document id (string or integer).
        vector: Vector data — for an unnamed-vector schema pass a single sequence;
                for a named-vectors schema pass a dict mapping vector field name to
                the corresponding sequence.
        attributes: Non-vector attribute fields (text, tags, metadata, etc.).
                    Turbopuffer infers attribute types from the data.
    """

    id: _RowId
    vector: Sequence[float] | np.ndarray | dict[str, Sequence[float] | np.ndarray]
    attributes: dict[str, Any] | None = None


def _vector_to_list(v: Sequence[float] | np.ndarray) -> list[float]:
    if isinstance(v, np.ndarray):
        return v.tolist()  # type: ignore[no-any-return]
    return list(v)


def _row_to_upsert(row: Row, schema: NamespaceSchema) -> dict[str, Any]:
    """Convert a Row to the dict shape turbopuffer's write API expects."""
    out: dict[str, Any] = {"id": row.id}

    if isinstance(schema.vectors, _ResolvedNamedVectorsDef):
        vector_field_names = set(schema.vectors.vectors)
        if not isinstance(row.vector, dict):
            raise ValueError(
                f"Row {row.id!r}: schema declares named vectors "
                f"({sorted(vector_field_names)}) but row.vector is not a dict."
            )
        missing = vector_field_names - set(row.vector)
        if missing:
            raise ValueError(
                f"Row {row.id!r}: missing vector fields {sorted(missing)}."
            )
        for name, vec in row.vector.items():
            out[name] = _vector_to_list(vec)
    else:
        vector_field_names = {_DEFAULT_VECTOR_FIELD}
        if isinstance(row.vector, dict):
            raise ValueError(
                f"Row {row.id!r}: schema declares a single unnamed vector but "
                f"row.vector is a dict."
            )
        out[_DEFAULT_VECTOR_FIELD] = _vector_to_list(row.vector)

    reserved = {"id"} | vector_field_names
    if row.attributes:
        for k, v in row.attributes.items():
            if k in reserved:
                raise ValueError(f"Row {row.id!r}: attribute name {k!r} is reserved.")
            out[k] = v

    return out


def _vector_type_str(vs: res_schema.VectorSchema) -> str:
    """Render a VectorSchema as turbopuffer's ``[N]fXX`` type string."""
    dt = np.dtype(vs.dtype)
    if dt == np.float32:
        suffix = "f32"
    elif dt == np.float16:
        suffix = "f16"
    else:
        raise ValueError(
            f"Turbopuffer vectors only support float32 or float16, got {dt}."
        )
    return f"[{vs.size}]{suffix}"


def _build_write_schema(schema: NamespaceSchema) -> dict[str, Any]:
    """Build the explicit ``schema`` payload passed to ``namespace.write()``."""
    out: dict[str, Any] = {}
    if isinstance(schema.vectors, _ResolvedNamedVectorsDef):
        for name, vd in schema.vectors.vectors.items():
            out[name] = {"type": _vector_type_str(vd.schema), "ann": True}
    else:
        out[_DEFAULT_VECTOR_FIELD] = {
            "type": _vector_type_str(schema.vectors.schema),
            "ann": True,
        }
    return out


# ---------- Row level (child) ----------


class _RowAction(NamedTuple):
    row_id: _RowId
    upsert: dict[str, Any] | None  # None means delete


class _RowHandler(coco.TargetHandler[Row, _RowFingerprint]):
    _client: AsyncTurbopuffer
    _namespace_name: str
    _schema: NamespaceSchema
    _sink: coco.TargetActionSink[_RowAction]

    def __init__(
        self,
        client: AsyncTurbopuffer,
        namespace_name: str,
        schema: NamespaceSchema,
    ) -> None:
        self._client = client
        self._namespace_name = namespace_name
        self._schema = schema
        self._sink = coco.TargetActionSink.from_async_fn(self._apply_actions)

    async def _apply_actions(
        self, context_provider: ContextProvider, actions: Sequence[_RowAction]
    ) -> None:
        if not actions:
            return

        upserts: list[dict[str, Any]] = []
        deletes: list[_RowId] = []

        for action in actions:
            if action.upsert is None:
                deletes.append(action.row_id)
            else:
                upserts.append(action.upsert)

        ns = self._client.namespace(self._namespace_name)

        kwargs: dict[str, Any] = {
            "distance_metric": self._schema.distance,
            "schema": _build_write_schema(self._schema),
        }
        if upserts:
            kwargs["upsert_rows"] = upserts
        if deletes:
            kwargs["deletes"] = deletes
        await ns.write(**kwargs)

    def reconcile(
        self,
        key: coco.StableKey,
        desired_state: Row | coco.NonExistenceType,
        prev_possible_records: Collection[_RowFingerprint],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[_RowAction, _RowFingerprint] | None:
        row_id = _ROW_ID_CHECKER.check(key)
        if coco.is_non_existence(desired_state):
            if not prev_possible_records and not prev_may_be_missing:
                return None
            return coco.TargetReconcileOutput(
                action=_RowAction(row_id=row_id, upsert=None),
                sink=self._sink,
                tracking_record=coco.NON_EXISTENCE,
            )

        upsert = _row_to_upsert(desired_state, self._schema)
        # Fingerprint over the full upsert payload (id is stable so it's harmless).
        target_fp = fingerprint_object(upsert)
        if not prev_may_be_missing and all(
            prev == target_fp for prev in prev_possible_records
        ):
            return None

        return coco.TargetReconcileOutput(
            action=_RowAction(row_id=row_id, upsert=upsert),
            sink=self._sink,
            tracking_record=target_fp,
        )


# ---------- Namespace level (root) ----------


class _NamespaceKey(NamedTuple):
    db_key: str
    namespace_name: str


_NAMESPACE_KEY_CHECKER = TypeChecker(tuple[str, str])


@dataclass
class _NamespaceSpec:
    schema: NamespaceSchema
    managed_by: target.ManagedBy = target.ManagedBy.SYSTEM


class _NamespaceTrackingRecordCore(msgspec.Struct, frozen=True, array_like=True):
    vectors: _ResolvedVectorDef | _ResolvedNamedVectorsDef
    distance: DistanceMetric


_NamespaceTrackingRecord = statediff.MutualTrackingRecord[_NamespaceTrackingRecordCore]


class _NamespaceAction(NamedTuple):
    key: _NamespaceKey
    spec: _NamespaceSpec | coco.NonExistenceType
    main_action: statediff.DiffAction | None


class _NamespaceHandler(
    coco.TargetHandler[_NamespaceSpec, _NamespaceTrackingRecord, _RowHandler]
):
    _sink: coco.TargetActionSink[_NamespaceAction, _RowHandler]

    def __init__(self) -> None:
        self._sink = coco.TargetActionSink.from_async_fn(self._apply_actions)

    async def _apply_actions(
        self, context_provider: ContextProvider, actions: Collection[_NamespaceAction]
    ) -> list[coco.ChildTargetDef[_RowHandler] | None]:
        actions_list = list(actions)
        outputs: list[coco.ChildTargetDef[_RowHandler] | None] = [None] * len(
            actions_list
        )

        for i, action in enumerate(actions_list):
            client = context_provider.get(action.key.db_key, AsyncTurbopuffer)

            if action.main_action in ("replace", "delete"):
                ns = client.namespace(action.key.namespace_name)
                try:
                    await ns.delete_all()
                except NotFoundError:
                    # Namespace was deleted out-of-band (e.g. via the dashboard).
                    pass

            if coco.is_non_existence(action.spec):
                outputs[i] = None
                continue

            spec = action.spec
            outputs[i] = coco.ChildTargetDef(
                handler=_RowHandler(
                    client=client,
                    namespace_name=action.key.namespace_name,
                    schema=spec.schema,
                )
            )

        return outputs

    def reconcile(
        self,
        key: coco.StableKey,
        desired_state: _NamespaceSpec | coco.NonExistenceType,
        prev_possible_records: Collection[_NamespaceTrackingRecord],
        prev_may_be_missing: bool,
        /,
    ) -> (
        coco.TargetReconcileOutput[
            _NamespaceAction, _NamespaceTrackingRecord, _RowHandler
        ]
        | None
    ):
        key = _NamespaceKey(*_NAMESPACE_KEY_CHECKER.check(key))
        tracking_record: _NamespaceTrackingRecord | coco.NonExistenceType

        if coco.is_non_existence(desired_state):
            tracking_record = coco.NON_EXISTENCE
        else:
            tracking_record = statediff.MutualTrackingRecord(
                tracking_record=_NamespaceTrackingRecordCore(
                    vectors=desired_state.schema.vectors,
                    distance=desired_state.schema.distance,
                ),
                managed_by=desired_state.managed_by,
            )

        transition = statediff.TrackingRecordTransition(
            tracking_record,
            prev_possible_records,
            prev_may_be_missing,
        )
        resolved = statediff.resolve_system_transition(transition)
        main_action = statediff.diff(resolved)

        # Namespace replacement clears all rows.
        child_invalidation: Literal["destructive"] | None = (
            "destructive" if main_action == "replace" else None
        )

        return coco.TargetReconcileOutput(
            action=_NamespaceAction(
                key=key,
                spec=desired_state,
                main_action=main_action,
            ),
            sink=self._sink,
            tracking_record=tracking_record,
            child_invalidation=child_invalidation,
        )


_namespace_provider = coco.register_root_target_states_provider(
    "cocoindex/turbopuffer/namespace", _NamespaceHandler()
)


# ---------- User-facing wrappers ----------


class NamespaceTarget(Generic[coco.MaybePendingS], coco.ResolvesTo["NamespaceTarget"]):
    """Target for declaring rows in a Turbopuffer namespace.

    Use this to declare individual rows (documents) to be stored in the namespace.
    Rows are specified using the ``Row`` dataclass.

    Example:
        ```python
        @coco.fn
        def process_doc(doc: Doc, target: NamespaceTarget) -> None:
            target.declare_row(turbopuffer.Row(
                id=doc.id,
                vector=doc.embedding,
                attributes={"text": doc.text, "tags": doc.tags},
            ))
        ```
    """

    _provider: coco.TargetStateProvider[Row, None, coco.MaybePendingS]

    def __init__(
        self,
        provider: coco.TargetStateProvider[Row, None, coco.MaybePendingS],
    ) -> None:
        self._provider = provider

    def declare_row(
        self: "NamespaceTarget[coco.ResolvedS]",
        row: Row,
    ) -> None:
        """Declare a row to be stored in the namespace.

        Args:
            row: Row containing id, vector(s), and attributes.
        """
        coco.declare_target_state(self._provider.target_state(row.id, row))

    def __coco_memo_key__(self) -> str:
        return self._provider.memo_key


def namespace_target(
    db: ContextKey[AsyncTurbopuffer],
    namespace_name: str,
    schema: NamespaceSchema,
    *,
    managed_by: target.ManagedBy = target.ManagedBy.SYSTEM,
) -> "coco.TargetState[_RowHandler]":
    """Create a TargetState for a Turbopuffer namespace.

    Use with ``coco.mount_target()`` to mount and get a child provider, or with
    ``mount_namespace_target()`` for a convenience wrapper.

    Args:
        db: ContextKey for the AsyncTurbopuffer client.
        namespace_name: Name of the namespace in Turbopuffer.
        schema: NamespaceSchema defining vector fields and distance metric.
        managed_by: Whether the namespace is managed by the system or user.
    """
    key = _NamespaceKey(db_key=db.key, namespace_name=namespace_name)
    spec = _NamespaceSpec(schema=schema, managed_by=managed_by)
    return _namespace_provider.target_state(key, spec)


def declare_namespace_target(
    db: ContextKey[AsyncTurbopuffer],
    namespace_name: str,
    schema: NamespaceSchema,
    *,
    managed_by: target.ManagedBy = target.ManagedBy.SYSTEM,
) -> "NamespaceTarget[coco.PendingS]":
    """Declare a Turbopuffer namespace target.

    Args:
        db: ContextKey for the AsyncTurbopuffer client.
        namespace_name: Name of the namespace in Turbopuffer.
        schema: NamespaceSchema defining vector fields and distance metric.
        managed_by: Whether the namespace is managed by the system or user.
    """
    provider = coco.declare_target_state_with_child(
        namespace_target(db, namespace_name, schema, managed_by=managed_by)
    )
    return NamespaceTarget(provider)


async def mount_namespace_target(
    db: ContextKey[AsyncTurbopuffer],
    namespace_name: str,
    schema: NamespaceSchema,
    *,
    managed_by: target.ManagedBy = target.ManagedBy.SYSTEM,
) -> "NamespaceTarget[coco.ResolvedS]":
    """Mount a namespace target and return a ready-to-use ``NamespaceTarget``.

    Sugar over ``namespace_target()`` + ``coco.mount_target()`` + wrapping.
    """
    provider = await coco.mount_target(
        namespace_target(db, namespace_name, schema, managed_by=managed_by)
    )
    return NamespaceTarget(provider)


__all__ = [
    "AsyncTurbopuffer",
    "DistanceMetric",
    "NamespaceSchema",
    "NamespaceTarget",
    "Row",
    "VectorDef",
    "declare_namespace_target",
    "mount_namespace_target",
    "namespace_target",
]
