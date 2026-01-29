"""
Qdrant target for CocoIndex.

This module provides a two-level target state system for Qdrant:
1. Collection level: Creates/drops collections in Qdrant
2. Point level: Upserts/deletes points within collections
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import (
    Any,
    Collection,
    Generic,
    Literal,
    Mapping,
    NamedTuple,
    Sequence,
    cast,
)


try:
    from qdrant_client import QdrantClient
    from qdrant_client.http import models as qdrant_models
except ImportError as e:
    raise ImportError(
        "qdrant-client is required to use the Qdrant connector. Please install cocoindex[qdrant]."
    ) from e

import cocoindex as coco
from cocoindex.connectorkits import connection, statediff
from cocoindex.connectorkits.fingerprint import fingerprint_object
from cocoindex.resources import schema

# Public alias for Qdrant point model
PointStruct = qdrant_models.PointStruct

# Type aliases
_PointId = str | int
_PointFingerprint = bytes


class QdrantVectorDef(NamedTuple):
    """Qdrant vector specification with optional distance and multivector config.

    Args:
        schema: VectorSchemaProvider or MultiVectorSchemaProvider
        distance: Distance metric to use (cosine, dot, or euclid)
        multivector_comparator: Comparator to use for multivector (only applies when schema
                                is MultiVectorSchemaProvider)
    """

    schema: schema.VectorSchemaProvider | schema.MultiVectorSchemaProvider
    distance: Literal["cosine", "dot", "euclid"] = "cosine"
    multivector_comparator: Literal["max_sim"] = "max_sim"


class _ResolvedQdrantVectorDef(NamedTuple):
    """Resolved Qdrant vector specification with concrete schema.

    This is the internal resolved form after calling __coco_vector_schema__()
    or __coco_multi_vector_schema__() on the providers.

    Args:
        schema: Resolved VectorSchema or MultiVectorSchema
        distance: Distance metric to use (cosine, dot, or euclid)
        multivector_comparator: Comparator to use for multivector
    """

    schema: schema.VectorSchema | schema.MultiVectorSchema
    distance: Literal["cosine", "dot", "euclid"]
    multivector_comparator: Literal["max_sim"]


def _resolve_vector_def(vector_def: QdrantVectorDef) -> _ResolvedQdrantVectorDef:
    resolved_schema: schema.VectorSchema | schema.MultiVectorSchema
    if isinstance(vector_def.schema, schema.VectorSchemaProvider):
        resolved_schema = vector_def.schema.__coco_vector_schema__()
    elif isinstance(vector_def.schema, schema.MultiVectorSchemaProvider):
        resolved_schema = vector_def.schema.__coco_multi_vector_schema__()
    else:
        raise ValueError(f"Invalid vector definition: {vector_def}")
    return _ResolvedQdrantVectorDef(
        schema=resolved_schema,
        distance=vector_def.distance,
        multivector_comparator=vector_def.multivector_comparator,
    )


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
                    schema=VectorSchema(dtype=np.float32, size=384),
                    distance="cosine"
                ),
            }
        )
        ```
    """

    _vectors: _ResolvedQdrantVectorDef | dict[str, _ResolvedQdrantVectorDef]

    def __init__(self, vectors: QdrantVectorDef | dict[str, QdrantVectorDef]) -> None:
        if isinstance(vectors, QdrantVectorDef):
            self._vectors = _resolve_vector_def(vectors)
        elif isinstance(vectors, dict):
            self._vectors = {
                name: _resolve_vector_def(vector_def)
                for name, vector_def in vectors.items()
            }
        else:
            raise ValueError(f"Invalid vector definition: {vectors}")

    @property
    def vectors(
        self,
    ) -> _ResolvedQdrantVectorDef | Mapping[str, _ResolvedQdrantVectorDef]:
        """Get vector definitions (all VectorSchemaProviders resolved)."""
        return self._vectors


class _PointAction(NamedTuple):
    point_id: _PointId
    point: qdrant_models.PointStruct | None


class _PointHandler(
    coco.TargetHandler[_PointId, qdrant_models.PointStruct, _PointFingerprint]
):
    _db_key: str
    _collection_name: str
    _sink: coco.TargetActionSink[_PointAction]

    def __init__(
        self,
        db_key: str,
        collection_name: str,
    ) -> None:
        self._db_key = db_key
        self._collection_name = collection_name
        self._sink = coco.TargetActionSink.from_async_fn(self._apply_actions)

    async def _apply_actions(self, actions: Sequence[_PointAction]) -> None:
        if not actions:
            return

        upserts: list[qdrant_models.PointStruct] = []
        deletes: list[_PointId] = []

        for action in actions:
            if action.point is None:
                deletes.append(action.point_id)
            else:
                upserts.append(action.point)

        client = _get_client(self._db_key)

        if upserts:
            await asyncio.to_thread(
                client.upsert,
                collection_name=self._collection_name,
                points=upserts,
            )

        if deletes:
            selector = qdrant_models.PointIdsList(
                points=cast(list[qdrant_models.ExtendedPointId], deletes)
            )
            await asyncio.to_thread(
                client.delete,
                collection_name=self._collection_name,
                points_selector=selector,
            )

    def reconcile(
        self,
        key: _PointId,
        desired_state: qdrant_models.PointStruct | coco.NonExistenceType,
        prev_possible_states: Collection[_PointFingerprint],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[_PointAction, _PointFingerprint] | None:
        if coco.is_non_existence(desired_state):
            if not prev_possible_states and not prev_may_be_missing:
                return None
            return coco.TargetReconcileOutput(
                action=_PointAction(point_id=key, point=None),
                sink=self._sink,
                tracking_record=coco.NON_EXISTENCE,
            )

        target_fp = fingerprint_object((desired_state.vector, desired_state.payload))
        if not prev_may_be_missing and all(
            prev == target_fp for prev in prev_possible_states
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


@dataclass
class _CollectionSpec:
    schema: CollectionSchema
    managed_by: Literal["system", "user"] = "system"


class _CollectionTrackingRecordCore(NamedTuple):
    vectors: _ResolvedQdrantVectorDef | Mapping[str, _ResolvedQdrantVectorDef]


_CollectionTrackingRecord = statediff.MutualTrackingRecord[
    _CollectionTrackingRecordCore
]


class _CollectionAction(NamedTuple):
    key: _CollectionKey
    spec: _CollectionSpec | coco.NonExistenceType
    main_action: statediff.DiffAction | None


_db_registry: connection.ConnectionRegistry[QdrantClient] = (
    connection.ConnectionRegistry("cocoindex/qdrant")
)


def _get_client(db_key: str) -> QdrantClient:
    return _db_registry.get(db_key)


def register_db(key: str, client: QdrantClient) -> "QdrantDatabase":
    """Register a Qdrant client with a key.

    Args:
        key: Unique identifier for this client
        client: QdrantClient instance

    Returns:
        QdrantDatabase handle for declaring collections
    """
    _db_registry.register(key, client)
    return QdrantDatabase(_db_registry.name, key, client, _db_registry)


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
    coco.TargetHandler[
        _CollectionKey, _CollectionSpec, _CollectionTrackingRecord, _PointHandler
    ]
):
    _sink: coco.TargetActionSink[_CollectionAction, _PointHandler]

    def __init__(self) -> None:
        self._sink = coco.TargetActionSink.from_async_fn(self._apply_actions)

    async def _apply_actions(
        self, actions: Collection[_CollectionAction]
    ) -> list[coco.ChildTargetDef[_PointHandler] | None]:
        actions_list = list(actions)
        outputs: list[coco.ChildTargetDef[_PointHandler] | None] = [None] * len(
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
                outputs[i] = coco.ChildTargetDef(
                    handler=_PointHandler(
                        db_key=key.db_key,
                        collection_name=key.collection_name,
                    )
                )

                if action.main_action in ("insert", "upsert", "replace"):
                    await self._create_collection(
                        client,
                        key.collection_name,
                        spec.schema,
                        if_not_exists=(action.main_action == "upsert"),
                    )

        return outputs

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

        # Configure vectors based on whether it's named or unnamed
        vectors_config: (
            dict[str, qdrant_models.VectorParams] | qdrant_models.VectorParams
        )
        if isinstance(schema.vectors, Mapping):
            # Named vectors: use dict
            vectors_config = {
                name: _vector_params_from_def(vector_def)
                for name, vector_def in schema.vectors.items()
            }
        else:
            # Unnamed vector: pass VectorParams directly (not in a dict)
            vectors_config = _vector_params_from_def(schema.vectors)

        await asyncio.to_thread(
            client.create_collection,
            collection_name=collection_name,
            vectors_config=vectors_config,
        )

    def reconcile(
        self,
        key: _CollectionKey,
        desired_state: _CollectionSpec | coco.NonExistenceType,
        prev_possible_states: Collection[_CollectionTrackingRecord],
        prev_may_be_missing: bool,
        /,
    ) -> (
        coco.TargetReconcileOutput[
            _CollectionAction, _CollectionTrackingRecord, _PointHandler
        ]
        | None
    ):
        tracking_record: _CollectionTrackingRecord | coco.NonExistenceType

        if coco.is_non_existence(desired_state):
            tracking_record = coco.NON_EXISTENCE
        else:
            tracking_record = statediff.MutualTrackingRecord(
                tracking_record=_CollectionTrackingRecordCore(
                    vectors=desired_state.schema.vectors
                ),
                managed_by=desired_state.managed_by,
            )

        transition = statediff.TrackingRecordTransition(
            tracking_record,
            prev_possible_states,
            prev_may_be_missing,
        )
        resolved = statediff.resolve_system_transition(transition)
        main_action = statediff.diff(resolved)

        return coco.TargetReconcileOutput(
            action=_CollectionAction(
                key=key,
                spec=desired_state,
                main_action=main_action,
            ),
            sink=self._sink,
            tracking_record=tracking_record,
        )


_collection_provider = coco.register_root_target_states_provider(
    "cocoindex.io/qdrant/collection", _CollectionHandler()
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

        @coco.function
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
        _PointId, qdrant_models.PointStruct, None, coco.MaybePendingS
    ]

    def __init__(
        self,
        provider: coco.TargetStateProvider[
            _PointId, qdrant_models.PointStruct, None, coco.MaybePendingS
        ],
    ) -> None:
        self._provider = provider

    def declare_point(
        self: "CollectionTarget[coco.ResolvedS]",
        point: qdrant_models.PointStruct,
    ) -> None:
        """Declare a point to be stored in the collection.

        Args:
            point: PointStruct defining the point ID, vectors, and payload
        """
        # Extract point ID
        point_id: _PointId
        if isinstance(point.id, (str, int)):
            point_id = point.id
        else:
            point_id = str(point.id)

        coco.declare_target_state(self._provider.target_state(point_id, point))

    def __coco_memo_key__(self) -> str:
        return self._provider.memo_key


class QdrantDatabase(connection.KeyedConnection[QdrantClient]):
    """Handle for a registered Qdrant client.

    Use this to declare collections and their schemas.
    """

    def declare_collection_target(
        self,
        collection_name: str,
        schema: CollectionSchema,
        *,
        managed_by: Literal["system", "user"] = "system",
    ) -> "CollectionTarget[coco.PendingS]":
        """Declare a Qdrant collection target.

        Args:
            collection_name: Name of the collection in Qdrant
            schema: CollectionSchema defining vector fields
            managed_by: Whether the collection is managed by the system or user

        Returns:
            CollectionTarget for declaring points

        Example:
            ```python
            @coco.function
            def app_main() -> None:
                db = coco.use_context(QDRANT_DB)
                target = coco.mount_run(
                    coco.component_subpath("setup"),
                    db.declare_collection_target,
                    collection_name="my_collection",
                    schema=CollectionSchema(
                        vectors={"embedding": QdrantVectorSpec(dim=384)}
                    ),
                ).result()
                # Use target to declare points...
            ```
        """
        key = _CollectionKey(db_key=self.key, collection_name=collection_name)
        spec = _CollectionSpec(schema=schema, managed_by=managed_by)
        provider = coco.declare_target_state_with_child(
            _collection_provider.target_state(key, spec)
        )
        return CollectionTarget(provider)


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


def _vector_params_from_def(
    vector_def: _ResolvedQdrantVectorDef,
) -> qdrant_models.VectorParams:
    """Convert a resolved vector definition to Qdrant VectorParams."""
    resolved_schema = vector_def.schema
    multivector_config = None

    if isinstance(resolved_schema, schema.VectorSchema):
        dim = resolved_schema.size
    elif isinstance(resolved_schema, schema.MultiVectorSchema):
        dim = resolved_schema.vector_schema.size
        # For multivector, use the specified comparator
        multivector_config = qdrant_models.MultiVectorConfig(
            comparator=_multivector_comparator(vector_def.multivector_comparator)
        )
    else:
        raise ValueError(f"Unexpected schema type: {type(resolved_schema)}")

    return qdrant_models.VectorParams(
        size=dim,
        distance=_distance_from_spec(vector_def.distance),
        multivector_config=multivector_config,
    )


__all__ = [
    "CollectionSchema",
    "CollectionTarget",
    "PointStruct",
    "QdrantDatabase",
    "QdrantVectorDef",
    "create_client",
    "register_db",
]
