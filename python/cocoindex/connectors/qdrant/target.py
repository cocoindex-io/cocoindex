"""
Qdrant target for CocoIndex.

This module provides a two-level effect system for Qdrant:
1. Collection level: Creates/drops collections in Qdrant
2. Row level: Upserts/deletes points within collections
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import threading
import uuid
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Collection,
    Generic,
    Literal,
    NamedTuple,
    Sequence,
    overload,
    cast,  # TODO(GeorgeH0): double check cast is necessary.
)

from typing_extensions import TypeVar

try:
    from qdrant_client import QdrantClient
    from qdrant_client.http import models as qdrant_models
except ImportError as e:
    raise ImportError(
        "qdrant-client is required to use the Qdrant connector. Please install cocoindex[qdrant]."
    ) from e

import numpy as np

import cocoindex as coco
from cocoindex.connectorkits import statediff
from cocoindex._internal.datatype import (
    AnyType,
    MappingType,
    SequenceType,
    StructType,
    UnionType,
    analyze_type_info,
    is_struct_type,
)
from cocoindex.resources.schema import VectorSpec

# Type aliases
_RowKey = tuple[Any, ...]
_RowValue = dict[str, Any]
_RowFingerprint = bytes
ValueEncoder = Callable[[Any], Any]


class QdrantVectorSpec(NamedTuple):
    """Qdrant vector specification with optional distance override."""

    dim: int
    distance: Literal["cosine", "dot", "euclid"] = "cosine"


def _json_encoder(value: Any) -> str:
    """Encode a value to JSON string for Qdrant payloads."""
    return json.dumps(value, default=str)


class ColumnDef(NamedTuple):
    """Definition of a payload column with optional encoder."""

    encoder: ValueEncoder | None = None


RowT = TypeVar("RowT", default=dict[str, Any])


class TableSchema(Generic[RowT]):
    """Schema definition for a Qdrant collection."""

    columns: dict[str, ColumnDef]
    primary_key: list[str]
    row_type: type[RowT] | None
    vector_specs: dict[str, QdrantVectorSpec]

    @overload
    def __init__(
        self: "TableSchema[dict[str, Any]]",
        columns: dict[str, ColumnDef],
        primary_key: list[str],
    ) -> None: ...

    @overload
    def __init__(
        self: "TableSchema[RowT]",
        columns: type[RowT],
        primary_key: list[str],
        *,
        column_specs: dict[str, ColumnDef | VectorSpec | QdrantVectorSpec]
        | None = None,
    ) -> None: ...

    def __init__(
        self,
        columns: type[RowT] | dict[str, ColumnDef],
        primary_key: list[str],
        *,
        column_specs: dict[str, ColumnDef | VectorSpec | QdrantVectorSpec]
        | None = None,
    ) -> None:
        self.vector_specs = {}

        if isinstance(columns, dict):
            self.columns = columns
            self.row_type = None
        elif is_struct_type(columns):
            self.columns = self._columns_from_struct_type(columns, column_specs)
            self.row_type = columns
        else:
            raise TypeError(
                "columns must be a struct type (dataclass, NamedTuple, Pydantic model) "
                f"or a dict[str, ColumnDef], got {type(columns)}"
            )

        self.primary_key = primary_key

        for pk in self.primary_key:
            if pk not in self.columns:
                raise ValueError(
                    f"Primary key column '{pk}' not found in columns: {list(self.columns.keys())}"
                )

        if len(self.primary_key) != 1:
            raise ValueError("Qdrant requires a single-column primary key.")

    def _columns_from_struct_type(
        self,
        struct_type: type,
        column_specs: dict[str, ColumnDef | VectorSpec | QdrantVectorSpec] | None,
    ) -> dict[str, ColumnDef]:
        struct_info = StructType(struct_type)
        columns: dict[str, ColumnDef] = {}

        for field in struct_info.fields:
            spec = column_specs.get(field.name) if column_specs else None

            if isinstance(spec, ColumnDef):
                columns[field.name] = spec
                continue

            if isinstance(spec, VectorSpec):
                if spec.dim <= 0:
                    raise ValueError(f"Invalid vector dimension: {spec.dim}")
                self.vector_specs[field.name] = QdrantVectorSpec(dim=spec.dim)
                columns[field.name] = ColumnDef()
                continue

            if isinstance(spec, QdrantVectorSpec):
                if spec.dim <= 0:
                    raise ValueError(f"Invalid vector dimension: {spec.dim}")
                self.vector_specs[field.name] = spec
                columns[field.name] = ColumnDef()
                continue

            type_info = analyze_type_info(field.type_hint)

            if type_info.base_type is np.ndarray:
                raise ValueError(
                    f"Vector field '{field.name}' requires a VectorSpec or QdrantVectorSpec."
                )

            encoder = _get_encoder(type_info)
            columns[field.name] = ColumnDef(encoder=encoder)

        return columns


class _RowAction(NamedTuple):
    key: _RowKey
    value: _RowValue | None


class _RowHandler(coco.EffectHandler[_RowKey, _RowValue, _RowFingerprint]):
    _db_key: str
    _collection_name: str
    _table_schema: TableSchema
    _sink: coco.EffectSink[_RowAction]

    def __init__(
        self,
        db_key: str,
        collection_name: str,
        table_schema: TableSchema,
    ) -> None:
        self._db_key = db_key
        self._collection_name = collection_name
        self._table_schema = table_schema
        self._sink = coco.EffectSink.from_async_fn(self._apply_actions)

    async def _apply_actions(self, actions: Sequence[_RowAction]) -> None:
        if not actions:
            return

        upserts: list[_RowAction] = []
        deletes: list[_RowAction] = []

        for action in actions:
            if action.value is None:
                deletes.append(action)
            else:
                upserts.append(action)

        client = _get_client(self._db_key)

        if upserts:
            await self._execute_upserts(client, upserts)

        if deletes:
            await self._execute_deletes(client, deletes)

    async def _execute_upserts(
        self,
        client: QdrantClient,
        upserts: list[_RowAction],
    ) -> None:
        points: list[qdrant_models.PointStruct] = []
        vector_fields = set(self._table_schema.vector_specs.keys())

        for action in upserts:
            assert action.value is not None
            row = action.value
            point_id = _qdrant_id_from_key(action.key)

            vectors: dict[str, list[float]] = {}
            payload: dict[str, Any] = {}

            for col_name, value in row.items():
                if col_name in vector_fields:
                    vectors[col_name] = _vector_to_list(value)
                elif col_name != self._table_schema.primary_key[0]:
                    payload[col_name] = value

            points.append(
                qdrant_models.PointStruct(
                    id=point_id,
                    vector=cast(qdrant_models.VectorStruct, vectors),
                    payload=payload,
                )
            )

        await asyncio.to_thread(
            client.upsert,
            collection_name=self._collection_name,
            points=points,
        )

    async def _execute_deletes(
        self,
        client: QdrantClient,
        deletes: list[_RowAction],
    ) -> None:
        point_ids = [_qdrant_id_from_key(action.key) for action in deletes]
        selector = qdrant_models.PointIdsList(
            points=cast(list[qdrant_models.ExtendedPointId], point_ids)
        )
        await asyncio.to_thread(
            client.delete,
            collection_name=self._collection_name,
            points_selector=selector,
        )

    def _compute_fingerprint(self, value: _RowValue) -> _RowFingerprint:
        serialized = json.dumps(value, sort_keys=True, default=str)
        return hashlib.blake2b(serialized.encode()).digest()

    def reconcile(
        self,
        key: _RowKey,
        desired_effect: _RowValue | coco.NonExistenceType,
        prev_possible_states: Collection[_RowFingerprint],
        prev_may_be_missing: bool,
        /,
    ) -> coco.EffectReconcileOutput[_RowAction, _RowFingerprint] | None:
        if coco.is_non_existence(desired_effect):
            if not prev_possible_states and not prev_may_be_missing:
                return None
            return coco.EffectReconcileOutput(
                action=_RowAction(key=key, value=None),
                sink=self._sink,
                state=coco.NON_EXISTENCE,
            )

        target_fp = self._compute_fingerprint(desired_effect)
        if not prev_may_be_missing and all(
            prev == target_fp for prev in prev_possible_states
        ):
            return None

        return coco.EffectReconcileOutput(
            action=_RowAction(key=key, value=desired_effect),
            sink=self._sink,
            state=target_fp,
        )


class _CollectionKey(NamedTuple):
    db_key: str
    collection_name: str


@dataclass
class _CollectionSpec:
    table_schema: TableSchema[Any]
    managed_by: Literal["system", "user"] = "system"


class _VectorState(NamedTuple):
    name: str
    dim: int
    distance: str


class _CollectionStateCore(NamedTuple):
    vectors: dict[str, _VectorState]


_CollectionState = statediff.MutualState[_CollectionStateCore]


class _CollectionAction(NamedTuple):
    key: _CollectionKey
    spec: _CollectionSpec | coco.NonExistenceType
    main_action: statediff.DiffAction | None


_db_registry: dict[str, QdrantClient] = {}
_db_registry_lock = threading.Lock()


def _get_client(db_key: str) -> QdrantClient:
    with _db_registry_lock:
        client = _db_registry.get(db_key)
    if client is None:
        raise RuntimeError(
            f"No Qdrant client registered with key '{db_key}'. Call register_db() first."
        )
    return client


def register_db(key: str, client: QdrantClient) -> "QdrantDatabase":
    with _db_registry_lock:
        if key in _db_registry:
            raise ValueError(
                f"Database with key '{key}' is already registered. "
                "Use a different key or unregister the existing one first."
            )
        _db_registry[key] = client
    return QdrantDatabase(key)


def create_client(url: str, *, prefer_grpc: bool = True, **kwargs: Any) -> QdrantClient:
    return QdrantClient(url=url, prefer_grpc=prefer_grpc, **kwargs)


def _unregister_db(key: str) -> None:
    with _db_registry_lock:
        _db_registry.pop(key, None)


def _collection_state_from_spec(spec: _CollectionSpec) -> _CollectionStateCore:
    vectors = {
        name: _VectorState(name=name, dim=vs.dim, distance=vs.distance)
        for name, vs in spec.table_schema.vector_specs.items()
    }
    return _CollectionStateCore(vectors=vectors)


class _CollectionHandler(
    coco.EffectHandler[_CollectionKey, _CollectionSpec, _CollectionState, _RowHandler]
):
    _sink: coco.EffectSink[_CollectionAction, _RowHandler]

    def __init__(self) -> None:
        self._sink = coco.EffectSink.from_async_fn(self._apply_actions)

    async def _apply_actions(
        self, actions: Collection[_CollectionAction]
    ) -> list[coco.ChildEffectDef[_RowHandler] | None]:
        actions_list = list(actions)
        outputs: list[coco.ChildEffectDef[_RowHandler] | None] = [None] * len(
            actions_list
        )

        by_key: dict[_CollectionKey, list[int]] = {}
        for i, action in enumerate(actions_list):
            by_key.setdefault(action.key, []).append(i)

        for key, idxs in by_key.items():
            client = _get_client(key.db_key)
            for i in idxs:
                action = actions_list[i]

                if action.main_action in ("replace", "delete"):
                    try:
                        await asyncio.to_thread(
                            client.delete_collection,
                            collection_name=key.collection_name,
                        )
                    except Exception:
                        pass

                if coco.is_non_existence(action.spec):
                    outputs[i] = None
                    continue

                spec = action.spec
                outputs[i] = coco.ChildEffectDef(
                    handler=_RowHandler(
                        db_key=key.db_key,
                        collection_name=key.collection_name,
                        table_schema=spec.table_schema,
                    )
                )

                if action.main_action in ("insert", "upsert", "replace"):
                    await self._create_collection(
                        client,
                        key.collection_name,
                        spec.table_schema,
                        if_not_exists=(action.main_action == "upsert"),
                    )

        return outputs

    async def _create_collection(
        self,
        client: QdrantClient,
        collection_name: str,
        schema: TableSchema[Any],
        *,
        if_not_exists: bool,
    ) -> None:
        if not schema.vector_specs:
            raise ValueError("Qdrant collection requires at least one vector field.")

        if if_not_exists and await asyncio.to_thread(
            _collection_exists, client, collection_name
        ):
            return

        vectors_config = {
            name: qdrant_models.VectorParams(
                size=spec.dim,
                distance=_distance_from_spec(spec.distance),
            )
            for name, spec in schema.vector_specs.items()
        }

        await asyncio.to_thread(
            client.create_collection,
            collection_name=collection_name,
            vectors_config=vectors_config,
        )

    def reconcile(
        self,
        key: _CollectionKey,
        desired_effect: _CollectionSpec | coco.NonExistenceType,
        prev_possible_states: Collection[_CollectionState],
        prev_may_be_missing: bool,
        /,
    ) -> (
        coco.EffectReconcileOutput[_CollectionAction, _CollectionState, _RowHandler]
        | None
    ):
        desired_state: _CollectionState | coco.NonExistenceType

        if coco.is_non_existence(desired_effect):
            desired_state = coco.NON_EXISTENCE
        else:
            desired_state = statediff.MutualState(
                state=_collection_state_from_spec(desired_effect),
                managed_by=desired_effect.managed_by,
            )

        transition = statediff.StateTransition(
            desired_state,
            prev_possible_states,
            prev_may_be_missing,
        )
        resolved = statediff.resolve_system_transition(transition)
        main_action = statediff.diff(resolved)

        return coco.EffectReconcileOutput(
            action=_CollectionAction(
                key=key,
                spec=desired_effect,
                main_action=main_action,
            ),
            sink=self._sink,
            state=desired_state,
        )


_collection_provider = coco.register_root_effect_provider(
    "cocoindex.io/qdrant/collection", _CollectionHandler()
)


class TableTarget(
    Generic[RowT, coco.MaybePendingS], coco.ResolvesTo["TableTarget[RowT]"]
):
    _provider: coco.EffectProvider[_RowKey, _RowValue, None, coco.MaybePendingS]
    _table_schema: TableSchema[RowT]

    def __init__(
        self,
        provider: coco.EffectProvider[_RowKey, _RowValue, None, coco.MaybePendingS],
        table_schema: TableSchema[RowT],
    ) -> None:
        self._provider = provider
        self._table_schema = table_schema

    def declare_row(self: "TableTarget[RowT]", scope: coco.Scope, *, row: RowT) -> None:
        row_dict = self._row_to_dict(row)
        pk_values = tuple(row_dict[pk] for pk in self._table_schema.primary_key)
        coco.declare_effect(scope, self._provider.effect(pk_values, row_dict))

    def _row_to_dict(self, row: RowT) -> dict[str, Any]:
        out: dict[str, Any] = {}
        vector_fields = set(self._table_schema.vector_specs.keys())

        for col_name, col_def in self._table_schema.columns.items():
            if isinstance(row, dict):
                value = row.get(col_name)
            else:
                value = getattr(row, col_name)

            if col_name in vector_fields and value is not None:
                value = _vector_to_list(value)
            elif value is not None and col_def.encoder is not None:
                value = col_def.encoder(value)

            out[col_name] = value

        return out

    def __coco_memo_key__(self) -> str:
        return self._provider.memo_key


class QdrantDatabase:
    """Handle for a registered Qdrant client."""

    _key: str

    def __init__(self, key: str) -> None:
        self._key = key

    @property
    def key(self) -> str:
        return self._key

    def __enter__(self) -> "QdrantDatabase":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        _unregister_db(self._key)

    def declare_collection_target(
        self,
        scope: coco.Scope,
        collection_name: str,
        table_schema: TableSchema[RowT],
        *,
        managed_by: Literal["system", "user"] = "system",
    ) -> TableTarget[RowT, coco.PendingS]:
        key = _CollectionKey(db_key=self._key, collection_name=collection_name)
        spec = _CollectionSpec(table_schema=table_schema, managed_by=managed_by)
        provider = coco.declare_effect_with_child(
            scope, _collection_provider.effect(key, spec)
        )
        return TableTarget(provider, table_schema)


def _collection_exists(client: QdrantClient, collection_name: str) -> bool:
    if hasattr(client, "collection_exists"):
        return bool(client.collection_exists(collection_name))
    try:
        client.get_collection(collection_name)
        return True
    except Exception:
        return False


def _distance_from_spec(distance: str) -> qdrant_models.Distance:
    distance_key = distance.lower()
    if distance_key in ("cosine",):
        return qdrant_models.Distance.COSINE
    if distance_key in ("dot", "dotproduct"):
        return qdrant_models.Distance.DOT
    if distance_key in ("euclid", "euclidean", "l2"):
        return qdrant_models.Distance.EUCLID
    raise ValueError(f"Unsupported Qdrant distance metric: {distance}")


def _vector_to_list(value: Any) -> list[float]:
    if isinstance(value, np.ndarray):
        return [float(v) for v in value.astype(float).tolist()]
    if isinstance(value, (list, tuple)):
        return [float(v) for v in value]
    raise TypeError(f"Vector value must be a numpy array or list, got {type(value)}")


def _qdrant_id_from_key(key: _RowKey) -> str | int:
    if len(key) != 1:
        raise ValueError("Qdrant requires a single-column primary key.")
    value = key[0]
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, (str, int)):
        return value
    return str(value)


def _get_encoder(type_info: Any) -> ValueEncoder | None:
    variant = type_info.variant

    if isinstance(variant, StructType):
        return _json_encoder

    if isinstance(variant, UnionType):
        return _json_encoder

    if isinstance(variant, AnyType):
        return _json_encoder

    if isinstance(variant, SequenceType):
        elem_info = analyze_type_info(variant.elem_type)
        return _get_encoder(elem_info)

    if isinstance(variant, MappingType):
        value_info = analyze_type_info(variant.value_type)
        return _get_encoder(value_info)

    return None


__all__ = [
    "ColumnDef",
    "QdrantDatabase",
    "QdrantVectorSpec",
    "TableSchema",
    "TableTarget",
    "create_client",
    "register_db",
]
