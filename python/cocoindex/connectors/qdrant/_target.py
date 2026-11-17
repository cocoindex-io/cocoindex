"""
Qdrant target for CocoIndex.

This module provides a two-level target state system for Qdrant:
1. Collection level: Creates/drops collections in Qdrant
2. Point level: Upserts/deletes points within collections
"""

from __future__ import annotations

import asyncio
import uuid

import msgspec
import numpy as np
from dataclasses import dataclass
from typing import (
    Any,
    Collection,
    Generic,
    Literal,
    NamedTuple,
    Sequence,
    cast,
)

from cocoindex._internal.extras import install_hint

try:
    from qdrant_client import QdrantClient
    from qdrant_client.http import models as qdrant_models
except ImportError as e:
    raise ImportError(
        "qdrant-client is required to use the Qdrant connector. "
        + install_hint("qdrant")
    ) from e

import cocoindex as coco
from cocoindex.connectorkits import _container, statediff, target
from cocoindex.connectorkits._request_size import split_by_size
from cocoindex.connectorkits.fingerprint import fingerprint_object
from cocoindex._internal.datatype import TypeChecker
from cocoindex.resources import schema as res_schema
from cocoindex._internal.context_keys import ContextKey, ContextProvider

# Public alias for Qdrant point model
PointStruct = qdrant_models.PointStruct

# Type aliases
_PointId = str | int
_PointFingerprint = bytes
_POINT_ID_CHECKER: TypeChecker[str | int] = TypeChecker(str | int)  # type: ignore[arg-type]

# The most points, or point ids, one write request carries, in bytes of their
# JSON form: half of Qdrant's default request size limit, 32 MiB
# (`service.max_request_size_mb`), which Qdrant enforces on REST requests. A
# point larger than this goes in a request of its own.
_MAX_WRITE_REQUEST_BYTES = 16 * 1024 * 1024


class QdrantVectorDef(NamedTuple):
    """Qdrant vector specification with optional distance and multivector config.

    Args:
        schema: VectorSchemaProvider or MultiVectorSchemaProvider
        distance: Distance metric to use (cosine, dot, or euclid)
        multivector_comparator: Comparator to use for multivector (only applies when schema
                                is MultiVectorSchemaProvider)
    """

    schema: (
        res_schema.VectorSchemaProvider
        | res_schema.MultiVectorSchemaProvider
        | coco.ContextKey[
            res_schema.VectorSchemaProvider | res_schema.MultiVectorSchemaProvider
        ]
    )
    distance: Literal["cosine", "dot", "euclid"] = "cosine"
    multivector_comparator: Literal["max_sim"] = "max_sim"


class QdrantSparseVectorDef(NamedTuple):
    """Qdrant sparse vector specification.

    Sparse vectors in Qdrant are always named and use dot-product scoring.
    ``modifier="idf"`` enables Qdrant's IDF modifier; ``None`` (the default)
    applies no modifier.
    """

    modifier: Literal["idf"] | None = None


class _ResolvedQdrantVectorDef(msgspec.Struct, frozen=True, tag=True):
    """Resolved single (unnamed) vector specification.

    This is the internal resolved form after calling __coco_vector_schema__()
    or __coco_multi_vector_schema__() on the providers.
    """

    schema: res_schema.VectorSchema | res_schema.MultiVectorSchema
    distance: Literal["cosine", "dot", "euclid"]
    multivector_comparator: Literal["max_sim"]


class _ResolvedQdrantSparseVectorDef(msgspec.Struct, frozen=True, tag=True):
    """Resolved sparse vector specification."""

    modifier: Literal["idf"] | None = None


class _ResolvedQdrantNamedVectorsDef(msgspec.Struct, frozen=True, tag=True):
    """Resolved named vectors specification.

    Dense and sparse vectors share one namespace in Qdrant (the server
    rejects duplicate names across the two kinds), so a single dict holds
    both; the tagged union discriminates them.
    """

    vectors: dict[str, _ResolvedQdrantVectorDef | _ResolvedQdrantSparseVectorDef]


async def _resolve_vector_def(vector_def: QdrantVectorDef) -> _ResolvedQdrantVectorDef:
    resolved_schema: res_schema.VectorSchema | res_schema.MultiVectorSchema
    vs = await res_schema.get_vector_schema(vector_def.schema)
    if vs is not None:
        resolved_schema = vs
    else:
        mvs = await res_schema.get_multi_vector_schema(vector_def.schema)
        if mvs is not None:
            resolved_schema = mvs
        else:
            raise ValueError(f"Invalid vector definition: {vector_def}")
    return _ResolvedQdrantVectorDef(
        schema=resolved_schema,
        distance=vector_def.distance,
        multivector_comparator=vector_def.multivector_comparator,
    )


def _resolve_sparse_vector_def(
    sparse_vector_def: QdrantSparseVectorDef,
) -> _ResolvedQdrantSparseVectorDef:
    return _ResolvedQdrantSparseVectorDef(modifier=sparse_vector_def.modifier)


@dataclass(slots=True)
class CollectionSchema:
    """Schema definition for a Qdrant collection.

    Defines the vector fields for the collection. Each vector field is specified by name
    and a QdrantVectorDef (which wraps a VectorSchemaProvider or MultiVectorSchemaProvider).

    Args:
        vectors: Either a single QdrantVectorDef (for unnamed vector) or a dictionary
                 mapping vector field names to QdrantVectorDef

    Example:
        ```python
        from cocoindex.connectors.qdrant import CollectionSchema, QdrantVectorDef
        from cocoindex.resources.schema import VectorSchema
        import numpy as np

        schema = CollectionSchema(
            vectors={
                "embedding": QdrantVectorDef(
                    schema=VectorSchema(dtype=np.dtype(np.float32), size=384),
                    distance="cosine"
                ),
            }
        )
        ```
    """

    _vectors: _ResolvedQdrantVectorDef | _ResolvedQdrantNamedVectorsDef

    def __init__(
        self,
        vectors: _ResolvedQdrantVectorDef | _ResolvedQdrantNamedVectorsDef,
    ) -> None:
        """
        Create a CollectionSchema from pre-resolved vector definitions.

        For constructing from unresolved ``QdrantVectorDef``, use the async
        classmethod ``create`` instead.
        """
        self._vectors = vectors

    @classmethod
    async def create(
        cls,
        vectors: QdrantVectorDef
        | dict[str, QdrantVectorDef | QdrantSparseVectorDef]
        | None = None,
    ) -> "CollectionSchema":
        """
        Create a CollectionSchema by resolving vector definitions.

        Args:
            vectors: Either a single QdrantVectorDef (for an unnamed dense
                     vector) or a dictionary mapping vector names to
                     QdrantVectorDef or QdrantSparseVectorDef. Dense and
                     sparse vectors share one namespace in Qdrant, so both
                     kinds live in the same dict; sparse vectors are always
                     named.
        """
        resolved: _ResolvedQdrantVectorDef | _ResolvedQdrantNamedVectorsDef
        if isinstance(vectors, QdrantVectorDef):
            resolved = await _resolve_vector_def(vectors)
        elif isinstance(vectors, QdrantSparseVectorDef):
            raise ValueError(
                "Qdrant sparse vectors are always named; pass them in a dict, "
                'e.g. vectors={"sparse": QdrantSparseVectorDef()}'
            )
        elif isinstance(vectors, dict):
            if not vectors:
                raise ValueError("Qdrant named vectors must not be empty")
            _validate_vector_names(vectors.keys(), "vector")
            resolved_entries: dict[
                str, _ResolvedQdrantVectorDef | _ResolvedQdrantSparseVectorDef
            ] = {}
            for name, vector_def in vectors.items():
                if isinstance(vector_def, QdrantVectorDef):
                    resolved_entries[name] = await _resolve_vector_def(vector_def)
                elif isinstance(vector_def, QdrantSparseVectorDef):
                    resolved_entries[name] = _resolve_sparse_vector_def(vector_def)
                else:
                    raise ValueError(f"Invalid vector definition: {vector_def}")
            resolved = _ResolvedQdrantNamedVectorsDef(vectors=resolved_entries)
        elif vectors is None:
            raise ValueError(
                "Qdrant collection schema must declare at least one vector"
            )
        else:
            raise ValueError(f"Invalid vector definition: {vectors}")
        return cls(resolved)

    @property
    def vectors(
        self,
    ) -> _ResolvedQdrantVectorDef | _ResolvedQdrantNamedVectorsDef:
        """Get vector definitions (all VectorSchemaProviders resolved)."""
        return self._vectors


class _PointAction(NamedTuple):
    point_id: _PointId
    point: qdrant_models.PointStruct | None


class _PointHandler(coco.TargetHandler[qdrant_models.PointStruct, _PointFingerprint]):
    _client: QdrantClient
    _collection_name: str
    _sink: coco.TargetActionSink[_PointAction]

    def __init__(
        self,
        client: QdrantClient,
        collection_name: str,
    ) -> None:
        self._client = client
        self._collection_name = collection_name
        self._sink = coco.TargetActionSink.from_async_fn(self._apply_actions)

    async def _apply_actions(
        self, context_provider: ContextProvider, actions: Sequence[_PointAction]
    ) -> None:
        if not actions:
            return

        upserts: list[qdrant_models.PointStruct] = []
        upserted_ids: set[_PointId] = set()
        deletes: list[_PointId] = []

        for action in actions:
            if action.point is None:
                deletes.append(_canonical_point_id(action.point_id))
            else:
                upserts.append(action.point)
                upserted_ids.add(_canonical_point_id(action.point_id))

        # A key from before point IDs were canonicalized (e.g. an upper-case
        # UUID) names the same point as its canonical key upserted here, so
        # deleting it would remove the point just written.
        deletes = [point_id for point_id in deletes if point_id not in upserted_ids]

        for points in split_by_size(
            upserts, _MAX_WRITE_REQUEST_BYTES, lambda p: len(p.model_dump_json())
        ):
            await asyncio.to_thread(
                self._client.upsert,
                collection_name=self._collection_name,
                points=points,
            )

        for ids in split_by_size(
            deletes, _MAX_WRITE_REQUEST_BYTES, lambda i: len(str(i)) + 3
        ):
            selector = qdrant_models.PointIdsList(
                points=cast(list[qdrant_models.ExtendedPointId], ids)
            )
            await asyncio.to_thread(
                self._client.delete,
                collection_name=self._collection_name,
                points_selector=selector,
            )

    def reconcile(
        self,
        key: coco.StableKey,
        desired_state: qdrant_models.PointStruct | coco.NonExistenceType,
        prev_possible_records: Collection[_PointFingerprint],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[_PointAction, _PointFingerprint] | None:
        key = _POINT_ID_CHECKER.check(key)
        if coco.is_non_existence(desired_state):
            if not prev_possible_records and not prev_may_be_missing:
                return None
            return coco.TargetReconcileOutput(
                action=_PointAction(point_id=key, point=None),
                sink=self._sink,
                tracking_record=coco.NON_EXISTENCE,
            )

        target_fp = fingerprint_object((desired_state.vector, desired_state.payload))
        if not prev_may_be_missing and all(
            prev == target_fp for prev in prev_possible_records
        ):
            return None

        return coco.TargetReconcileOutput(
            action=_PointAction(point_id=key, point=desired_state),
            sink=self._sink,
            tracking_record=target_fp,
        )


class _CollectionKey(NamedTuple):
    db_key: str
    collection_name: str


_COLLECTION_KEY_CHECKER = TypeChecker(tuple[str, str])


@dataclass
class _CollectionSpec:
    schema: CollectionSchema
    managed_by: target.ManagedBy = target.ManagedBy.SYSTEM
    # See `_container.ContainerSpec`.
    missing: bool = False


class _CollectionTrackingRecordCore(msgspec.Struct, frozen=True, array_like=True):
    vectors: _ResolvedQdrantVectorDef | _ResolvedQdrantNamedVectorsDef


_FLOAT32 = np.dtype(np.float32)
_FLOAT64 = np.dtype(np.float64)
_FLOAT16 = np.dtype(np.float16)


def _stored_vector_def(
    vector_def: _ResolvedQdrantVectorDef | _ResolvedQdrantSparseVectorDef,
) -> _ResolvedQdrantVectorDef | _ResolvedQdrantSparseVectorDef:
    """`vector_def` with a float64 dtype read as float32 (see `_as_stored`)."""
    if not isinstance(vector_def, _ResolvedQdrantVectorDef):
        return vector_def
    schema = vector_def.schema
    vector_schema = (
        schema.vector_schema
        if isinstance(schema, res_schema.MultiVectorSchema)
        else schema
    )
    if vector_schema.dtype != _FLOAT64:
        return vector_def
    stored = res_schema.VectorSchema(dtype=_FLOAT32, size=vector_schema.size)
    return msgspec.structs.replace(
        vector_def,
        schema=(
            res_schema.MultiVectorSchema(vector_schema=stored)
            if isinstance(schema, res_schema.MultiVectorSchema)
            else stored
        ),
    )


def _as_stored(record: _CollectionTrackingRecordCore) -> _CollectionTrackingRecordCore:
    """The record as Qdrant stores the collection.

    Qdrant has no 64-bit float vectors and stores those of a float64 schema as
    float32, so a float64 dtype reads as float32 here, and a change between the
    two alone recreates nothing. A record stored with float64 reads the same
    way. float16 is stored as such, so a change to or from it recreates.
    """
    vectors = record.vectors
    if isinstance(vectors, _ResolvedQdrantNamedVectorsDef):
        stored: _ResolvedQdrantVectorDef | _ResolvedQdrantNamedVectorsDef = (
            _ResolvedQdrantNamedVectorsDef(
                vectors={
                    name: _stored_vector_def(vector_def)
                    for name, vector_def in vectors.vectors.items()
                }
            )
        )
    else:
        stored = cast(_ResolvedQdrantVectorDef, _stored_vector_def(vectors))
    return _CollectionTrackingRecordCore(vectors=stored)


_CollectionTrackingRecord = statediff.MutualTrackingRecord[
    _CollectionTrackingRecordCore
]


class _CollectionAction(NamedTuple):
    key: _CollectionKey
    spec: _CollectionSpec | coco.NonExistenceType
    main_action: statediff.DiffAction | None


def create_client(url: str, *, prefer_grpc: bool = True, **kwargs: Any) -> QdrantClient:
    """Create a Qdrant client.

    Args:
        url: Qdrant server URL
        prefer_grpc: Whether to prefer gRPC over HTTP
        **kwargs: Additional arguments to pass to QdrantClient

    Returns:
        QdrantClient instance
    """
    return QdrantClient(url=url, prefer_grpc=prefer_grpc, **kwargs)


class _CollectionHandler(
    coco.TargetHandler[_CollectionSpec, _CollectionTrackingRecord, _PointHandler]
):
    _sink: coco.TargetActionSink[_CollectionAction]

    def __init__(self) -> None:
        self._sink = coco.TargetActionSink.from_async_fn(
            self._apply_actions, child_handler=self._point_handler
        )

    @staticmethod
    def _point_handler(
        context_provider: ContextProvider, action: _CollectionAction, /
    ) -> _PointHandler:
        """The point handler for a created or updated collection's points."""
        return _PointHandler(
            client=context_provider.get(action.key.db_key, QdrantClient),
            collection_name=action.key.collection_name,
        )

    async def _apply_actions(
        self,
        context_provider: ContextProvider,
        actions: Sequence[_CollectionAction],
        /,
    ) -> None:
        by_key: dict[_CollectionKey, list[_CollectionAction]] = {}
        for action in actions:
            by_key.setdefault(action.key, []).append(action)

        for key, key_actions in by_key.items():
            client = context_provider.get(key.db_key, QdrantClient)
            for action in key_actions:
                if action.main_action in ("replace", "delete"):
                    try:
                        await asyncio.to_thread(
                            client.delete_collection,
                            collection_name=key.collection_name,
                        )
                    except Exception:
                        pass

                if coco.is_non_existence(action.spec):
                    continue

                spec = action.spec

                if action.main_action in ("insert", "upsert", "replace"):
                    await self._create_collection(
                        client,
                        key.collection_name,
                        spec.schema,
                        if_not_exists=(action.main_action == "upsert"),
                    )

    async def _create_collection(
        self,
        client: QdrantClient,
        collection_name: str,
        schema: CollectionSchema,
        *,
        if_not_exists: bool,
    ) -> None:
        if if_not_exists and await asyncio.to_thread(
            _collection_exists, client, collection_name
        ):
            return

        # Dense and sparse defs share one namespace on our side; split them
        # into Qdrant's two config maps here.
        vectors_config: (
            dict[str, qdrant_models.VectorParams] | qdrant_models.VectorParams | None
        ) = None
        sparse_vectors_config: dict[str, qdrant_models.SparseVectorParams] | None = None
        if isinstance(schema.vectors, _ResolvedQdrantNamedVectorsDef):
            dense = {
                name: _vector_params_from_def(vector_def)
                for name, vector_def in schema.vectors.vectors.items()
                if isinstance(vector_def, _ResolvedQdrantVectorDef)
            }
            sparse = {
                name: _sparse_vector_params_from_def(vector_def)
                for name, vector_def in schema.vectors.vectors.items()
                if isinstance(vector_def, _ResolvedQdrantSparseVectorDef)
            }
            vectors_config = dense or None
            sparse_vectors_config = sparse or None
        else:
            vectors_config = _vector_params_from_def(schema.vectors)

        await asyncio.to_thread(
            client.create_collection,
            collection_name=collection_name,
            vectors_config=vectors_config,
            sparse_vectors_config=sparse_vectors_config,
        )

    def reconcile(
        self,
        key: coco.StableKey,
        desired_state: _CollectionSpec | coco.NonExistenceType,
        prev_possible_records: Collection[_CollectionTrackingRecord],
        prev_may_be_missing: bool,
        /,
    ) -> (
        coco.TargetReconcileOutput[
            _CollectionAction, _CollectionTrackingRecord, _PointHandler
        ]
        | None
    ):
        key = _CollectionKey(*_COLLECTION_KEY_CHECKER.check(key))
        tracking_record: _CollectionTrackingRecord | coco.NonExistenceType

        if coco.is_non_existence(desired_state):
            tracking_record = coco.NON_EXISTENCE
        else:
            tracking_record = statediff.MutualTrackingRecord(
                tracking_record=_as_stored(
                    _CollectionTrackingRecordCore(vectors=desired_state.schema.vectors)
                ),
                managed_by=desired_state.managed_by,
            )

        prev_records = [
            statediff.MutualTrackingRecord(
                tracking_record=_as_stored(prev.tracking_record),
                managed_by=prev.managed_by,
            )
            for prev in prev_possible_records
        ]
        # A collection CocoIndex created before that is gone now was deleted
        # outside CocoIndex: create it as if new, and invalidate its points,
        # which went with it.
        recreated = _container.recreate_if_dropped(
            desired_state, prev_records, prev_may_be_missing
        )

        transition = statediff.TrackingRecordTransition(
            tracking_record,
            recreated.prev_possible_records,
            recreated.prev_may_be_missing,
        )
        resolved = statediff.resolve_system_transition(transition)
        main_action = statediff.diff(resolved)

        # Collection replacement destroys all points.
        child_invalidation: Literal["destructive"] | None = (
            "destructive" if recreated.dropped or main_action == "replace" else None
        )

        return coco.TargetReconcileOutput(
            action=_CollectionAction(
                key=key,
                spec=desired_state,
                main_action=main_action,
            ),
            sink=self._sink,
            tracking_record=tracking_record,
            child_invalidation=child_invalidation,
        )


_collection_provider = coco.register_root_target_states_provider(
    "cocoindex/qdrant/collection", _CollectionHandler()
)


class CollectionTarget(
    Generic[coco.MaybePendingS], coco.ResolvesTo["CollectionTarget"]
):
    """Target for declaring points in a Qdrant collection.

    Use this to declare individual points (documents) to be stored in the collection.
    Points are specified using Qdrant's PointStruct model.

    Example:
        ```python
        from qdrant_client.http import models as qdrant_models

        @coco.fn
        def process_doc(doc: Doc, target: CollectionTarget) -> None:
            point = qdrant_models.PointStruct(
                id=doc.id,
                vector={"embedding": doc.embedding.tolist()},
                payload={"text": doc.text, "metadata": doc.metadata},
            )
            target.declare_point(point)
        ```
    """

    _provider: coco.TargetStateProvider[
        qdrant_models.PointStruct, None, coco.MaybePendingS
    ]
    _schema: CollectionSchema

    def __init__(
        self,
        provider: coco.TargetStateProvider[
            qdrant_models.PointStruct, None, coco.MaybePendingS
        ],
        schema: CollectionSchema,
    ) -> None:
        self._provider = provider
        self._schema = schema

    def declare_point(
        self: "CollectionTarget[coco.ResolvedS]",
        point: qdrant_models.PointStruct,
    ) -> None:
        """Declare a point to be stored in the collection.

        Args:
            point: PointStruct defining the point ID, vectors, and payload

        A UUID point ID may be given in any text form Qdrant accepts (upper
        case, 32-char hex, URN) or as a ``uuid.UUID``; all forms of one UUID
        name the same point, so declaring two of them in one run fails as a
        duplicate declaration.

        Raises:
            ValueError: If the point ID is not an unsigned 64-bit integer or
                a UUID (Qdrant rejects all other IDs at write time), or if a
                vector does not fit the collection schema: a dense vector of
                the wrong length, an empty multivector, a NaN or an infinity,
                a sparse vector with repeated indices or a values list of
                another length, or a vector name the schema does not declare.
                Qdrant would reject the whole batch the point is written in.
        """
        point_id = _validate_point_id(point.id)
        _check_point_vectors(point_id, point.vector, self._schema.vectors)
        coco.declare_target_state(self._provider.target_state(point_id, point))

    def __coco_memo_key__(self) -> str:
        return self._provider.memo_key


def collection_target(
    db: ContextKey[QdrantClient],
    collection_name: str,
    schema: CollectionSchema,
    *,
    managed_by: target.ManagedBy = target.ManagedBy.SYSTEM,
) -> "coco.TargetState[_PointHandler]":
    """
    Create a TargetState for a Qdrant collection target.

    Use with ``coco.mount_target()`` to mount and get a child provider,
    or with ``mount_collection_target()`` for a convenience wrapper.

    Args:
        db: ContextKey for the QdrantClient connection.
        collection_name: Name of the collection in Qdrant.
        schema: CollectionSchema defining vector fields.
        managed_by: Whether the collection is managed by the system or user.

    Returns:
        A TargetState that can be passed to ``mount_target()``.
    """
    return _collection_target_state(
        db, collection_name, schema, managed_by=managed_by, missing=False
    )


def _collection_target_state(
    db: ContextKey[QdrantClient],
    collection_name: str,
    schema: CollectionSchema,
    *,
    managed_by: target.ManagedBy,
    missing: bool,
) -> "coco.TargetState[_PointHandler]":
    """`collection_target`, with `missing` as in `_CollectionSpec.missing`."""
    key = _CollectionKey(db_key=db.key, collection_name=collection_name)
    spec = _CollectionSpec(schema=schema, managed_by=managed_by, missing=missing)
    return _collection_provider.target_state(key, spec)


def declare_collection_target(
    db: ContextKey[QdrantClient],
    collection_name: str,
    schema: CollectionSchema,
    *,
    managed_by: target.ManagedBy = target.ManagedBy.SYSTEM,
) -> "CollectionTarget[coco.PendingS]":
    """Declare a Qdrant collection target.

    Args:
        db: ContextKey for the QdrantClient connection.
        collection_name: Name of the collection in Qdrant
        schema: CollectionSchema defining vector fields
        managed_by: Whether the collection is managed by the system or user

    Returns:
        CollectionTarget for declaring points
    """
    provider = coco.declare_target_state_with_child(
        collection_target(db, collection_name, schema, managed_by=managed_by)
    )
    return CollectionTarget(provider, schema)


async def mount_collection_target(
    db: ContextKey[QdrantClient],
    collection_name: str,
    schema: CollectionSchema,
    *,
    managed_by: target.ManagedBy = target.ManagedBy.SYSTEM,
) -> "CollectionTarget[coco.ResolvedS]":
    """
    Mount a collection target and return a ready-to-use CollectionTarget.

    Sugar over ``collection_target()`` + ``coco.mount_target()`` + wrapping.

    Args:
        db: ContextKey for the QdrantClient connection.
        collection_name: Name of the collection in Qdrant.
        schema: CollectionSchema defining vector fields.
        managed_by: Whether the collection is managed by the system or user.

    Returns:
        A CollectionTarget for declaring points.
    """
    # A system-managed collection that is not on the server: if CocoIndex
    # created it on an earlier run, it was deleted outside CocoIndex.
    missing = managed_by == target.ManagedBy.SYSTEM and not await asyncio.to_thread(
        _collection_exists, coco.use_context(db), collection_name
    )
    provider = await coco.mount_target(
        _collection_target_state(
            db, collection_name, schema, managed_by=managed_by, missing=missing
        )
    )
    return CollectionTarget(provider, schema)


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


def _multivector_comparator(
    comparator: str,
) -> qdrant_models.MultiVectorComparator:
    """Convert multivector comparator string to Qdrant enum."""
    if comparator.lower() == "max_sim":
        return qdrant_models.MultiVectorComparator.MAX_SIM
    raise ValueError(f"Unsupported multivector comparator: {comparator}")


def _sparse_modifier_from_spec(
    modifier: Literal["idf"] | None,
) -> qdrant_models.Modifier | None:
    if modifier is None:
        return None
    if modifier == "idf":
        return qdrant_models.Modifier.IDF
    raise ValueError(f"Unsupported Qdrant sparse vector modifier: {modifier}")


def _vector_params_from_def(
    vector_def: _ResolvedQdrantVectorDef,
) -> qdrant_models.VectorParams:
    """Convert a resolved vector definition to Qdrant VectorParams."""
    resolved_schema = vector_def.schema
    multivector_config = None

    if isinstance(resolved_schema, res_schema.VectorSchema):
        vector_schema = resolved_schema
    elif isinstance(resolved_schema, res_schema.MultiVectorSchema):
        vector_schema = resolved_schema.vector_schema
        # For multivector, use the specified comparator
        multivector_config = qdrant_models.MultiVectorConfig(
            comparator=_multivector_comparator(vector_def.multivector_comparator)
        )
    else:
        raise ValueError(f"Unexpected schema type: {type(resolved_schema)}")

    return qdrant_models.VectorParams(
        size=vector_schema.size,
        distance=_distance_from_spec(vector_def.distance),
        multivector_config=multivector_config,
        # Qdrant stores float16 vectors as such; every other dtype, float64
        # included (Qdrant has no 64-bit vectors), as its default, float32.
        datatype=(
            qdrant_models.Datatype.FLOAT16 if vector_schema.dtype == _FLOAT16 else None
        ),
    )


def _sparse_vector_params_from_def(
    sparse_vector_def: _ResolvedQdrantSparseVectorDef,
) -> qdrant_models.SparseVectorParams:
    """Convert a resolved sparse vector definition to Qdrant SparseVectorParams."""
    return qdrant_models.SparseVectorParams(
        modifier=_sparse_modifier_from_spec(sparse_vector_def.modifier)
    )


def _check_dense_vector(
    vector: list[float] | list[list[float]], vector_def: _ResolvedQdrantVectorDef
) -> str | None:
    """What is wrong with a dense vector or multivector, or ``None``."""
    vector_schema = vector_def.schema
    is_multi = bool(vector) and isinstance(vector[0], list)
    if isinstance(vector_schema, res_schema.MultiVectorSchema):
        if not vector:
            return "is an empty multivector"
        if not is_multi:
            return "is a single vector but the collection expects a multivector"
        size = vector_schema.vector_schema.size
        rows = cast(list[list[float]], vector)
    else:
        if is_multi:
            return "is a multivector but the collection expects a single vector"
        size = vector_schema.size
        rows = [cast(list[float], vector)]
    for row in rows:
        if len(row) != size:
            return f"has {len(row)} elements but the collection expects {size}"
        if not np.isfinite(np.asarray(row, dtype=np.float64)).all():
            return "contains a non-finite (NaN/Inf) element"
    return None


def _check_sparse_vector(vector: qdrant_models.SparseVector) -> str | None:
    """What is wrong with a sparse vector, or ``None``."""
    if len(vector.indices) != len(vector.values):
        return f"has {len(vector.indices)} indices but {len(vector.values)} values"
    if not np.isfinite(np.asarray(vector.values, dtype=np.float64)).all():
        return "contains a non-finite (NaN/Inf) value"
    if len(set(vector.indices)) != len(vector.indices):
        return "repeats an index"
    return None


def _check_point_vectors(
    point_id: _PointId,
    vector: qdrant_models.VectorStruct,
    vectors_def: _ResolvedQdrantVectorDef | _ResolvedQdrantNamedVectorsDef,
) -> None:
    """Check a point's vectors against the collection schema, at declare time.

    Qdrant rejects a point whose vectors do not fit the collection when it is
    written, failing the whole batch, and stores a NaN that then poisons every
    score it is in. Vectors Qdrant computes itself (``Document``, ``Image``,
    ``InferenceObject``) are left to the server.
    """
    problem: str | None = None
    if isinstance(vectors_def, _ResolvedQdrantVectorDef):
        if isinstance(vector, list):
            problem = _check_dense_vector(vector, vectors_def)
        if problem is not None:
            raise ValueError(f"Qdrant point {point_id!r}: vector {problem}")
        return

    if isinstance(vector, list):
        raise ValueError(
            f"Qdrant point {point_id!r}: vector is unnamed but the collection "
            f"declares named vectors {sorted(vectors_def.vectors)}"
        )
    if not isinstance(vector, dict):
        return
    for name, named_vector in vector.items():
        vector_def = vectors_def.vectors.get(name)
        if vector_def is None:
            raise ValueError(
                f"Qdrant point {point_id!r}: vector name {name!r} is not declared "
                f"in the collection schema {sorted(vectors_def.vectors)}"
            )
        if isinstance(named_vector, qdrant_models.SparseVector):
            problem = (
                _check_sparse_vector(named_vector)
                if isinstance(vector_def, _ResolvedQdrantSparseVectorDef)
                else "is sparse but the field is dense"
            )
        elif isinstance(named_vector, list):
            problem = (
                _check_dense_vector(named_vector, vector_def)
                if isinstance(vector_def, _ResolvedQdrantVectorDef)
                else "is dense but the field is sparse"
            )
        if problem is not None:
            raise ValueError(
                f"Qdrant point {point_id!r}: vector field {name!r} {problem}"
            )


def _validate_vector_names(names: Collection[str], kind: str) -> None:
    for name in names:
        if not name:
            raise ValueError(f"Qdrant {kind} name must not be empty")


_POINT_ID_RULE = (
    "Qdrant point IDs must be an unsigned 64-bit integer or a UUID "
    "(str or uuid.UUID); see "
    "https://qdrant.tech/documentation/manage-data/points/#point-ids. For a "
    "stable ID derived from an arbitrary string key, use uuid.uuid5, e.g. "
    'str(uuid.uuid5(uuid.NAMESPACE_URL, f"doc/{key}")).'
)


def _canonical_point_id(point_id: _PointId) -> _PointId:
    """The ID Qdrant stores a point under.

    Qdrant parses every UUID text form (upper case, 32-char hex, URN) to the
    same point, so a UUID string becomes its hyphenated lower-case form. A
    string that is not a UUID names no point; it stays as it is.
    """
    if isinstance(point_id, str):
        try:
            return str(uuid.UUID(point_id))
        except ValueError:
            return point_id
    return point_id


def _validate_point_id(raw: object) -> _PointId:
    """Validate a point ID against Qdrant's server-side rules, eagerly, and
    return its canonical form (see :func:`_canonical_point_id`).

    Qdrant only accepts unsigned 64-bit integers and UUIDs (any textual
    form: hyphenated, 32-char hex, or URN); everything else is rejected at
    upsert time with an opaque transport error, so fail at declare time
    with an actionable one instead.
    """
    if isinstance(raw, int):
        if not 0 <= raw < 1 << 64:
            raise ValueError(
                f"Invalid Qdrant point ID {raw!r}: out of unsigned 64-bit range. "
                f"{_POINT_ID_RULE}"
            )
        return raw
    if isinstance(raw, uuid.UUID):
        return str(raw)
    if isinstance(raw, str):
        try:
            return str(uuid.UUID(raw))
        except ValueError:
            raise ValueError(
                f"Invalid Qdrant point ID {raw!r}: strings must be UUIDs. "
                f"{_POINT_ID_RULE}"
            ) from None
    raise ValueError(
        f"Invalid Qdrant point ID of type {type(raw).__name__}. {_POINT_ID_RULE}"
    )


__all__ = [
    "CollectionSchema",
    "CollectionTarget",
    "PointStruct",
    "QdrantSparseVectorDef",
    "QdrantVectorDef",
    "collection_target",
    "create_client",
    "declare_collection_target",
    "mount_collection_target",
]
