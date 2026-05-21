"""
Valkey target for CocoIndex.

This module provides a two-level target state system for Valkey with vector search:
1. Index level: Creates/drops search indexes via FT.CREATE / FT.DROPINDEX
2. Document level: Upserts/deletes hash documents within indexes

Requires the valkey-search module to be loaded in the Valkey server.
"""

from __future__ import annotations

import struct as _struct
from dataclasses import dataclass as _dataclass
from typing import (
    Collection as _Collection,
    Generic as _Generic,
    Literal as _Literal,
    NamedTuple as _NamedTuple,
    Sequence as _Sequence,
)

import msgspec as _msgspec
import numpy as _np

try:
    from glide import GlideClient
    from glide import GlideClientConfiguration  # noqa: F401 — re-exported
    from glide.async_commands import ft as _ft
except ImportError as e:
    raise ImportError(
        "valkey-glide>=2.4.0 is required to use the Valkey connector. "
        "Please install cocoindex[valkey]."
    ) from e

import cocoindex as coco
from cocoindex.connectorkits import statediff as _statediff
from cocoindex.connectorkits import target as _target
from cocoindex.connectorkits.fingerprint import (
    fingerprint_object as _fingerprint_object,
)
from cocoindex.resources import schema as _res_schema
from cocoindex._internal.context_keys import ContextKey, ContextProvider
from cocoindex._internal.datatype import TypeChecker as _TypeChecker


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


class VectorDef(_NamedTuple):
    """Valkey vector field specification.

    Args:
        schema: VectorSchemaProvider (or ContextKey wrapping one) for dimension inference.
        distance: Distance metric (cosine, l2, or ip).
        algorithm: Vector index algorithm (hnsw or flat).
    """

    schema: (
        _res_schema.VectorSchemaProvider
        | coco.ContextKey[_res_schema.VectorSchemaProvider]
    )
    distance: _Literal["cosine", "l2", "ip"] = "cosine"
    algorithm: _Literal["hnsw", "flat"] = "hnsw"


class FieldDef(_NamedTuple):
    """Definition of an indexed payload field in the search schema.

    Fields declared here will be included in FT.CREATE and can be used for
    filtering and searching via FT.SEARCH queries.

    Args:
        name: The field name (must match the key in Document.payload).
        type: Field type — "text" for full-text search, "tag" for exact-match
              filtering, "numeric" for range filtering.
        sortable: Whether the field can be used for sorting results.
    """

    name: str
    type: _Literal["text", "tag", "numeric"]
    sortable: bool = False


class _ResolvedVectorDef(_msgspec.Struct, frozen=True, tag=True):
    """Internal resolved form after calling __coco_vector_schema__()."""

    schema: _res_schema.VectorSchema
    distance: _Literal["cosine", "l2", "ip"]
    algorithm: _Literal["hnsw", "flat"]


async def _resolve_vector_def(vector_def: VectorDef) -> _ResolvedVectorDef:
    vs = await _res_schema.get_vector_schema(vector_def.schema)
    if vs is None:
        raise ValueError(
            f"VectorDef schema must implement VectorSchemaProvider: {vector_def.schema}"
        )
    return _ResolvedVectorDef(
        schema=vs,
        distance=vector_def.distance,
        algorithm=vector_def.algorithm,
    )


@_dataclass(slots=True)
class IndexSchema:
    """Schema definition for a Valkey search index.

    Defines the vector field and optional indexed payload fields. Use the async
    ``create`` classmethod to resolve vector dimensions from a provider.

    Example:
        ```python
        schema = await valkey.IndexSchema.create(
            vectors=valkey.VectorDef(schema=EMBEDDER, distance="cosine"),
            fields=[
                valkey.FieldDef("text", "text"),
                valkey.FieldDef("category", "tag"),
                valkey.FieldDef("price", "numeric", sortable=True),
            ],
        )
        ```
    """

    _vectors: _ResolvedVectorDef
    _fields: tuple[FieldDef, ...]

    def __init__(
        self,
        vectors: _ResolvedVectorDef,
        fields: tuple[FieldDef, ...] = (),
    ) -> None:
        self._vectors = vectors
        self._fields = fields

    @classmethod
    async def create(
        cls,
        vectors: VectorDef,
        fields: list[FieldDef] | None = None,
    ) -> "IndexSchema":
        """Create an IndexSchema by resolving vector definitions.

        Args:
            vectors: A VectorDef specifying the vector field.
            fields: Optional list of payload fields to index for search/filtering.
        """
        resolved = await _resolve_vector_def(vectors)
        return cls(resolved, tuple(fields) if fields else ())

    @property
    def vectors(self) -> _ResolvedVectorDef:
        """Get resolved vector definition."""
        return self._vectors

    @property
    def fields(self) -> tuple[FieldDef, ...]:
        """Get indexed field definitions."""
        return self._fields


@_dataclass(slots=True)
class Document:
    """A document to store in the Valkey index.

    Args:
        id: Unique document identifier (string).
        vector: Vector as a list of floats or numpy array.
        payload: Optional dict of additional fields stored alongside the vector.
    """

    id: str
    vector: list[float] | _np.ndarray  # type: ignore[type-arg]
    payload: dict[str, str | int | float] | None = None


# ---------------------------------------------------------------------------
# Internal types
# ---------------------------------------------------------------------------


class _IndexKey(_NamedTuple):
    db_key: str
    index_name: str


_INDEX_KEY_CHECKER = _TypeChecker(tuple[str, str])


class _DocumentAction(_NamedTuple):
    """Action for a single document: upsert (doc not None) or delete (doc is None)."""

    hash_key: str
    fields: dict[str, bytes | str] | None  # None means delete


_DocumentFingerprint = bytes


class _IndexAction(_NamedTuple):
    key: _IndexKey
    spec: _IndexSpec | coco.NonExistenceType
    main_action: _statediff.DiffAction | None


@_dataclass(slots=True)
class _IndexSpec:
    schema: IndexSchema
    managed_by: _target.ManagedBy = _target.ManagedBy.SYSTEM


class _IndexTrackingRecordCore(_msgspec.Struct, frozen=True, array_like=True):
    vectors: _ResolvedVectorDef
    fields: tuple[FieldDef, ...]


_IndexTrackingRecord = _statediff.MutualTrackingRecord[_IndexTrackingRecordCore]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _vector_to_bytes(vector: list[float] | _np.ndarray) -> bytes:  # type: ignore[type-arg]
    """Pack a vector into little-endian float32 bytes for Valkey HASH storage."""
    if isinstance(vector, _np.ndarray):
        return vector.astype(_np.float32).tobytes()
    return _struct.pack(f"<{len(vector)}f", *vector)


def _make_prefix(index_name: str) -> str:
    """Create the key prefix for documents in an index."""
    return f"{index_name}:"


def _make_hash_key(index_name: str, doc_id: str) -> str:
    """Create the full hash key for a document."""
    return f"{_make_prefix(index_name)}{doc_id}"


def _distance_metric_arg(distance: _Literal["cosine", "l2", "ip"]) -> str:
    """Convert distance literal to the Valkey FT.CREATE DISTANCE_METRIC argument.

    Returns the enum member name string (COSINE, L2, IP) which will be
    converted to a DistanceMetricType enum at index creation time.
    """
    return distance.upper()


# ---------------------------------------------------------------------------
# Document handler (child level)
# ---------------------------------------------------------------------------


class _DocumentHandler(coco.TargetHandler[Document, _DocumentFingerprint]):
    """Handles upsert/delete of individual documents within a Valkey index."""

    _client: GlideClient
    _index_name: str
    _sink: coco.TargetActionSink[_DocumentAction]

    def __init__(self, client: GlideClient, index_name: str) -> None:
        self._client = client
        self._index_name = index_name
        self._sink = coco.TargetActionSink.from_async_fn(self._apply_actions)

    async def _apply_actions(
        self, context_provider: ContextProvider, actions: _Sequence[_DocumentAction]
    ) -> None:
        if not actions:
            return

        for action in actions:
            if action.fields is None:
                # Delete
                try:
                    await self._client.delete([action.hash_key])
                except Exception:
                    pass  # Key may not exist
            else:
                # Upsert via HSET
                await self._client.hset(action.hash_key, action.fields)  # type: ignore[arg-type]

    def reconcile(
        self,
        key: coco.StableKey,
        desired_state: Document | coco.NonExistenceType,
        prev_possible_records: _Collection[_DocumentFingerprint],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[_DocumentAction, _DocumentFingerprint] | None:
        if not isinstance(key, str):
            raise TypeError(f"Document key must be a string, got {type(key)}")

        hash_key = _make_hash_key(self._index_name, key)

        if coco.is_non_existence(desired_state):
            if not prev_possible_records and not prev_may_be_missing:
                return None
            return coco.TargetReconcileOutput(
                action=_DocumentAction(hash_key=hash_key, fields=None),
                sink=self._sink,
                tracking_record=coco.NON_EXISTENCE,
            )

        # Build fingerprint from vector + payload
        target_fp = _fingerprint_object(
            (desired_state.vector, desired_state.payload)
            if not isinstance(desired_state.vector, _np.ndarray)
            else (desired_state.vector.tolist(), desired_state.payload)
        )

        if not prev_may_be_missing and all(
            prev == target_fp for prev in prev_possible_records
        ):
            return None

        # Build hash fields
        fields: dict[str, bytes | str] = {
            "vector": _vector_to_bytes(desired_state.vector),
        }
        if desired_state.payload:
            for k, v in desired_state.payload.items():
                fields[k] = str(v)

        return coco.TargetReconcileOutput(
            action=_DocumentAction(hash_key=hash_key, fields=fields),
            sink=self._sink,
            tracking_record=target_fp,
        )


# ---------------------------------------------------------------------------
# Index handler (parent level)
# ---------------------------------------------------------------------------


class _IndexHandler(
    coco.TargetHandler[_IndexSpec, _IndexTrackingRecord, _DocumentHandler]
):
    """Handles creation/deletion of Valkey search indexes."""

    _sink: coco.TargetActionSink[_IndexAction, _DocumentHandler]

    def __init__(self) -> None:
        self._sink = coco.TargetActionSink.from_async_fn(self._apply_actions)

    async def _apply_actions(
        self,
        context_provider: ContextProvider,
        actions: _Collection[_IndexAction],
    ) -> list[coco.ChildTargetDef[_DocumentHandler] | None]:
        actions_list = list(actions)
        outputs: list[coco.ChildTargetDef[_DocumentHandler] | None] = [None] * len(
            actions_list
        )

        by_key: dict[_IndexKey, list[int]] = {}
        for i, action in enumerate(actions_list):
            by_key.setdefault(action.key, []).append(i)

        for key, idxs in by_key.items():
            client = context_provider.get(key.db_key, GlideClient)  # type: ignore[type-abstract]
            for i in idxs:
                action = actions_list[i]

                if action.main_action in ("replace", "delete"):
                    try:
                        await _ft.dropindex(client, key.index_name)
                    except Exception:
                        pass  # Index may not exist

                    # Delete all documents with the prefix
                    if action.main_action == "replace":
                        await self._delete_prefix_keys(client, key.index_name)

                if coco.is_non_existence(action.spec):
                    outputs[i] = None
                    continue

                spec = action.spec
                outputs[i] = coco.ChildTargetDef(
                    handler=_DocumentHandler(
                        client=client,
                        index_name=key.index_name,
                    )
                )

                if action.main_action in ("insert", "upsert", "replace"):
                    await self._create_index(
                        client,
                        key.index_name,
                        spec.schema,
                        if_not_exists=(action.main_action == "upsert"),
                    )

        return outputs

    async def _delete_prefix_keys(self, client: GlideClient, index_name: str) -> None:
        """Delete all hash keys with the index prefix."""
        prefix = _make_prefix(index_name)
        # Use SCAN to find and delete keys with prefix
        cursor: str | bytes = "0"
        while True:
            result = await client.custom_command(
                ["SCAN", cursor, "MATCH", f"{prefix}*", "COUNT", "100"]
            )
            if isinstance(result, list) and len(result) == 2:
                cursor = result[0]
                keys = result[1]
                if keys:
                    key_list = [k for k in keys if isinstance(k, (str, bytes))]
                    if key_list:
                        await client.delete(key_list)
                if cursor in (b"0", "0"):
                    break
            else:
                break

    async def _create_index(
        self,
        client: GlideClient,
        index_name: str,
        schema: IndexSchema,
        *,
        if_not_exists: bool,
    ) -> None:
        if if_not_exists:
            try:
                await _ft.info(client, index_name)
                return  # Index already exists
            except Exception:
                pass  # Index doesn't exist, create it

        vec_def = schema.vectors
        dim = vec_def.schema.size
        algorithm = vec_def.algorithm.upper()

        # Build field schema for FT.CREATE
        from glide import (
            DistanceMetricType as _DistanceMetricType,
            VectorAlgorithm as _VectorAlgorithm,
            VectorField as _VectorField,
            VectorFieldAttributesFlat as _VectorFieldAttributesFlat,
            VectorFieldAttributesHnsw as _VectorFieldAttributesHnsw,
            VectorType as _VectorType,
            FtCreateOptions as _LocalFtCreateOptions,
            DataType as _DataType,
        )

        distance_enum = _DistanceMetricType[_distance_metric_arg(vec_def.distance)]

        if algorithm == "HNSW":
            attributes = _VectorFieldAttributesHnsw(
                dimensions=dim,
                distance_metric=distance_enum,
                type=_VectorType.FLOAT32,
            )
            algo_enum = _VectorAlgorithm.HNSW
        else:
            attributes = _VectorFieldAttributesFlat(
                dimensions=dim,
                distance_metric=distance_enum,
                type=_VectorType.FLOAT32,
            )
            algo_enum = _VectorAlgorithm.FLAT

        vector_field = _VectorField(
            name="vector",
            algorithm=algo_enum,
            attributes=attributes,
        )

        # Build additional indexed fields
        from glide import (
            TextField as _TextField,
            TagField as _TagField,
            NumericField as _NumericField,
            Field as _Field,
        )

        all_fields: list[_Field] = [vector_field]
        for field_def in schema.fields:
            if field_def.type == "text":
                all_fields.append(
                    _TextField(name=field_def.name, sortable=field_def.sortable)
                )
            elif field_def.type == "tag":
                all_fields.append(
                    _TagField(name=field_def.name, sortable=field_def.sortable)
                )
            elif field_def.type == "numeric":
                all_fields.append(
                    _NumericField(name=field_def.name, sortable=field_def.sortable)
                )

        prefix = _make_prefix(index_name)
        options = _LocalFtCreateOptions(data_type=_DataType.HASH, prefixes=[prefix])

        await _ft.create(client, index_name, schema=all_fields, options=options)

    def reconcile(
        self,
        key: coco.StableKey,
        desired_state: _IndexSpec | coco.NonExistenceType,
        prev_possible_records: _Collection[_IndexTrackingRecord],
        prev_may_be_missing: bool,
        /,
    ) -> (
        coco.TargetReconcileOutput[_IndexAction, _IndexTrackingRecord, _DocumentHandler]
        | None
    ):
        if not isinstance(key, tuple) or len(key) != 2:
            raise TypeError(
                f"Index key must be a (db_key, index_name) tuple, got {key!r}"
            )
        key = _IndexKey(*_INDEX_KEY_CHECKER.check(key))
        tracking_record: _IndexTrackingRecord | coco.NonExistenceType

        if coco.is_non_existence(desired_state):
            tracking_record = coco.NON_EXISTENCE
        else:
            tracking_record = _statediff.MutualTrackingRecord(
                tracking_record=_IndexTrackingRecordCore(
                    vectors=desired_state.schema.vectors,
                    fields=desired_state.schema.fields,
                ),
                managed_by=desired_state.managed_by,
            )

        transition = _statediff.TrackingRecordTransition(
            tracking_record,
            prev_possible_records,
            prev_may_be_missing,
        )
        resolved = _statediff.resolve_system_transition(transition)
        main_action = _statediff.diff(resolved)

        # Index replacement destroys all documents.
        child_invalidation: _Literal["destructive"] | None = (
            "destructive" if main_action == "replace" else None
        )

        return coco.TargetReconcileOutput(
            action=_IndexAction(
                key=key,
                spec=desired_state,
                main_action=main_action,
            ),
            sink=self._sink,
            tracking_record=tracking_record,
            child_invalidation=child_invalidation,
        )


# ---------------------------------------------------------------------------
# Provider registration
# ---------------------------------------------------------------------------

_index_provider = coco.register_root_target_states_provider(
    "cocoindex/valkey/index", _IndexHandler()
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


class IndexTarget(_Generic[coco.MaybePendingS], coco.ResolvesTo["IndexTarget"]):
    """Target for declaring documents in a Valkey search index.

    Use this to declare individual documents to be stored in the index.

    Example:
        ```python
        index = await valkey.mount_index_target(VALKEY_DB, "embeddings", schema)
        index.declare_document(valkey.Document(
            id="doc1",
            vector=embedding.tolist(),
            payload={"text": "hello world"},
        ))
        ```
    """

    _provider: coco.TargetStateProvider[Document, None, coco.MaybePendingS]

    def __init__(
        self,
        provider: coco.TargetStateProvider[Document, None, coco.MaybePendingS],
    ) -> None:
        self._provider = provider

    def declare_document(
        self: "IndexTarget[coco.ResolvedS]",
        document: Document,
    ) -> None:
        """Declare a document to be stored in the index.

        Args:
            document: Document with id, vector, and optional payload.
        """
        coco.declare_target_state(self._provider.target_state(document.id, document))

    def __coco_memo_key__(self) -> str:
        return self._provider.memo_key


def index_target(
    db: ContextKey[GlideClient],
    index_name: str,
    schema: IndexSchema,
    *,
    managed_by: _target.ManagedBy = _target.ManagedBy.SYSTEM,
) -> "coco.TargetState[_DocumentHandler]":
    """Create a TargetState for a Valkey index target.

    Use with ``coco.mount_target()`` to mount and get a child provider,
    or with ``mount_index_target()`` for a convenience wrapper.

    Args:
        db: ContextKey for the GlideClient connection.
        index_name: Name of the search index in Valkey.
        schema: IndexSchema defining vector fields.
        managed_by: Whether the index is managed by the system or user.

    Returns:
        A TargetState that can be passed to ``mount_target()``.
    """
    key = _IndexKey(db_key=db.key, index_name=index_name)
    spec = _IndexSpec(schema=schema, managed_by=managed_by)
    return _index_provider.target_state(key, spec)


def declare_index_target(
    db: ContextKey[GlideClient],
    index_name: str,
    schema: IndexSchema,
    *,
    managed_by: _target.ManagedBy = _target.ManagedBy.SYSTEM,
) -> "IndexTarget[coco.PendingS]":
    """Declare a Valkey index target.

    Args:
        db: ContextKey for the GlideClient connection.
        index_name: Name of the search index in Valkey.
        schema: IndexSchema defining vector fields.
        managed_by: Whether the index is managed by the system or user.

    Returns:
        IndexTarget for declaring documents.
    """
    provider = coco.declare_target_state_with_child(
        index_target(db, index_name, schema, managed_by=managed_by)
    )
    return IndexTarget(provider)


async def mount_index_target(
    db: ContextKey[GlideClient],
    index_name: str,
    schema: IndexSchema,
    *,
    managed_by: _target.ManagedBy = _target.ManagedBy.SYSTEM,
) -> "IndexTarget[coco.ResolvedS]":
    """Mount an index target and return a ready-to-use IndexTarget.

    Sugar over ``index_target()`` + ``coco.mount_target()`` + wrapping.

    Args:
        db: ContextKey for the GlideClient connection.
        index_name: Name of the search index in Valkey.
        schema: IndexSchema defining vector fields.
        managed_by: Whether the index is managed by the system or user.

    Returns:
        An IndexTarget for declaring documents.
    """
    provider = await coco.mount_target(
        index_target(db, index_name, schema, managed_by=managed_by)
    )
    return IndexTarget(provider)


def create_client_config(
    host: str = "localhost",
    port: int = 6379,
) -> "GlideClientConfiguration":
    """Create a GlideClientConfiguration for connecting to Valkey.

    Args:
        host: Valkey server host.
        port: Valkey server port.

    Returns:
        GlideClientConfiguration instance.
    """
    from glide import NodeAddress

    return GlideClientConfiguration([NodeAddress(host=host, port=port)])


__all__ = [
    "Document",
    "FieldDef",
    "GlideClient",
    "GlideClientConfiguration",
    "IndexSchema",
    "IndexTarget",
    "VectorDef",
    "create_client_config",
    "declare_index_target",
    "index_target",
    "mount_index_target",
]
