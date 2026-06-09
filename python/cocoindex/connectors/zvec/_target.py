"""
zvec target for CocoIndex.

zvec (https://zvec.org) is an embedded, in-process vector database. This module
provides a two-level target state system:

1. Collection level: creates/destroys collections on disk.
2. Document level: upserts/deletes documents within a collection.

zvec is path-based: a ``ManagedConnection`` owns a base directory and each
collection lives in a subdirectory under it. Each document has a single string
``id`` (the primary key), a set of named vector fields (dense or sparse), and a
set of scalar fields used for filtering.
"""

from __future__ import annotations

import base64
import datetime
import decimal
import json
import re
import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import (
    Any,
    Callable,
    Collection,
    Generic,
    Iterator,
    Literal,
    NamedTuple,
    Sequence,
    cast,
)

import numpy as np
from typing_extensions import TypeVar

try:
    import zvec as _zvec
except ImportError as e:
    raise ImportError(
        "zvec is required to use the zvec connector. Please install cocoindex[zvec]."
    ) from e

import msgspec

import cocoindex as coco
from cocoindex.connectorkits import statediff, target
from cocoindex.connectorkits.fingerprint import fingerprint_object
from cocoindex._internal.context_keys import ContextKey, ContextProvider
from cocoindex._internal.datatype import (
    RecordType,
    SequenceType,
    TypeChecker,
    analyze_type_info,
    is_record_type,
)
from cocoindex.resources import schema as res_schema

ValueEncoder = Callable[[Any], Any]

# Document id is always a string in zvec.
_DocId = str
_DOC_ID_CHECKER: TypeChecker[str] = TypeChecker(str)
_COLLECTION_KEY_CHECKER = TypeChecker(tuple[str, str])

RowT = TypeVar("RowT", default=dict[str, Any])


# zvec rejects names that don't match its internal rule. We guard with a
# conservative identifier allow-list before handing the name to zvec, which
# mirrors the input-safety guidance for other connectors and gives a clear error
# early.
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# zvec additionally rejects collection names shorter than 3 characters.
_MIN_COLLECTION_NAME_LEN = 3


def _validate_identifier(name: str, kind: str = "identifier") -> None:
    if not isinstance(name, str) or not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid {kind}: {name!r}")


def _validate_collection_name(name: str) -> None:
    _validate_identifier(name, "collection name")
    if len(name) < _MIN_COLLECTION_NAME_LEN:
        raise ValueError(
            f"Invalid collection name {name!r}: zvec requires at least "
            f"{_MIN_COLLECTION_NAME_LEN} characters."
        )


# =============================================================================
# Connection
# =============================================================================


def _collection_option(enable_mmap: bool, *, read_only: bool = False) -> Any:
    return _zvec.CollectionOption(read_only=read_only, enable_mmap=enable_mmap)


@dataclass
class ManagedConnection:
    """A handle to a base directory holding zvec collections.

    zvec opens each collection as a live handle and takes an exclusive write
    lock on it. Opening the same collection twice (even within one process)
    fails, so this class caches open handles by collection name and reuses them.
    """

    base_path: Path
    enable_mmap: bool = True
    _open: dict[str, Any] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def collection_path(self, name: str) -> Path:
        return self.base_path / name

    def open_or_create(self, name: str, schema: Any) -> Any:
        """Open the collection, creating it from ``schema`` if it doesn't exist."""
        with self._lock:
            col = self._open.get(name)
            if col is not None:
                return col
            path = self.collection_path(name)
            if path.exists():
                col = _zvec.open(str(path), option=_collection_option(self.enable_mmap))
            else:
                self.base_path.mkdir(parents=True, exist_ok=True)
                col = _zvec.create_and_open(
                    path=str(path),
                    schema=schema,
                    option=_collection_option(self.enable_mmap),
                )
            self._open[name] = col
            return col

    def open_existing(self, name: str) -> Any:
        """Open an existing collection (used for user-managed collections)."""
        with self._lock:
            col = self._open.get(name)
            if col is not None:
                return col
            col = _zvec.open(
                str(self.collection_path(name)),
                option=_collection_option(self.enable_mmap),
            )
            self._open[name] = col
            return col

    def destroy(self, name: str) -> None:
        """Permanently delete a collection from disk."""
        with self._lock:
            col = self._open.pop(name, None)
            if col is None:
                path = self.collection_path(name)
                if not path.exists():
                    return
                col = _zvec.open(str(path), option=_collection_option(self.enable_mmap))
            col.destroy()

    def close(self) -> None:
        """Release all open collection handles (drops their write locks)."""
        with self._lock:
            self._open.clear()


def connect(base_path: str | Path, *, enable_mmap: bool = True) -> ManagedConnection:
    """Create a ManagedConnection rooted at ``base_path``.

    Args:
        base_path: Directory under which collections are stored. Created if missing.
        enable_mmap: Whether zvec uses memory-mapped I/O for data files.
    """
    path = Path(base_path)
    path.mkdir(parents=True, exist_ok=True)
    return ManagedConnection(base_path=path, enable_mmap=enable_mmap)


@contextmanager
def managed_connection(
    base_path: str | Path, *, enable_mmap: bool = True
) -> Iterator[ManagedConnection]:
    """Create a ManagedConnection as a context manager.

    Suitable for ``builder.provide_with()`` in a lifespan function. Closes all
    open collection handles on exit.
    """
    conn = connect(base_path, enable_mmap=enable_mmap)
    try:
        yield conn
    finally:
        conn.close()


# =============================================================================
# Schema and type mapping
# =============================================================================


class ZvecType(NamedTuple):
    """Annotation to override the scalar field type for a column.

    Use with ``typing.Annotated``:

    ```python
    from typing import Annotated
    import zvec
    from cocoindex.connectors.zvec import ZvecType

    @dataclass
    class MyRow:
        # Store as INT32 instead of the default INT64, without a filter index.
        count: Annotated[int, ZvecType(zvec.DataType.INT32, indexed=False)]
    ```
    """

    data_type: Any  # zvec.DataType
    encoder: ValueEncoder | None = None
    indexed: bool = True


class ZvecVectorDef(NamedTuple):
    """Annotation to configure a vector field.

    Dense vectors are inferred from a NumPy ``ndarray`` field plus a
    ``VectorSchema`` (via ``Annotated`` or ``column_overrides``). This annotation
    tunes the index and marks sparse fields.

    For sparse vectors, set ``sparse=True`` on a ``dict[int, float]`` field.
    """

    metric: Literal["cosine", "ip", "l2"] = "cosine"
    quantize: Literal["none", "fp16", "int8", "int4"] = "none"
    sparse: bool = False


_ColumnKind = Literal["scalar", "dense", "sparse"]


@dataclass(slots=True)
class _Column:
    name: str
    kind: _ColumnKind
    data_type: Any  # zvec.DataType
    nullable: bool = True
    encoder: ValueEncoder | None = None
    dimension: int | None = None  # dense vectors only
    metric: str | None = None  # vectors only
    quantize: str | None = None  # dense vectors only
    indexed: bool = False  # scalar fields only (invert index for filtering)


_LEAF_SCALAR_MAPPINGS: dict[type, tuple[Any, ValueEncoder | None]] = {
    bool: (_zvec.DataType.BOOL, None),
    int: (_zvec.DataType.INT64, None),
    float: (_zvec.DataType.DOUBLE, None),
    str: (_zvec.DataType.STRING, None),
    bytes: (_zvec.DataType.STRING, lambda v: base64.b64encode(v).decode("ascii")),
    uuid.UUID: (_zvec.DataType.STRING, str),
    decimal.Decimal: (_zvec.DataType.STRING, str),
    datetime.date: (_zvec.DataType.STRING, lambda v: v.isoformat()),
    datetime.datetime: (_zvec.DataType.STRING, lambda v: v.isoformat()),
    datetime.time: (_zvec.DataType.STRING, lambda v: v.isoformat()),
    datetime.timedelta: (_zvec.DataType.DOUBLE, lambda v: v.total_seconds()),
}

_ARRAY_ELEM_MAPPINGS: dict[type, Any] = {
    str: _zvec.DataType.ARRAY_STRING,
    int: _zvec.DataType.ARRAY_INT64,
    float: _zvec.DataType.ARRAY_DOUBLE,
    bool: _zvec.DataType.ARRAY_BOOL,
}


def _json_encoder(value: Any) -> str:
    return json.dumps(value, default=str)


def _dense_vector_data_type(dtype: np.dtype) -> Any:
    # zvec's dense vector index only accepts FP32 and FP16. For smaller storage,
    # keep an FP32 vector and set quantize on ZvecVectorDef (e.g. "int8").
    if dtype == np.float32:
        return _zvec.DataType.VECTOR_FP32
    if dtype == np.float16:
        return _zvec.DataType.VECTOR_FP16
    raise ValueError(
        f"Unsupported dense vector dtype {dtype!r}; zvec dense vectors must be "
        "float32 or float16. For compressed storage, use a float32 vector with "
        'ZvecVectorDef(quantize="int8").'
    )


def _scalar_data_type(type_info: Any) -> tuple[Any, ValueEncoder | None]:
    base_type = type_info.base_type
    if base_type in _LEAF_SCALAR_MAPPINGS:
        return _LEAF_SCALAR_MAPPINGS[base_type]
    if isinstance(type_info.variant, SequenceType):
        elem_info = analyze_type_info(type_info.variant.elem_type)
        mapped = _ARRAY_ELEM_MAPPINGS.get(elem_info.base_type)
        if mapped is not None:
            return mapped, None
    # Fallback: store complex/unknown types as a JSON string.
    return _zvec.DataType.STRING, _json_encoder


async def _resolve_column(
    name: str,
    type_hint: Any,
    override: ZvecType | ZvecVectorDef | res_schema.VectorSchemaProvider | None,
) -> _Column:
    type_info = analyze_type_info(type_hint)

    annotations: list[Any] = []
    if override is not None:
        annotations.append(override)
    annotations.extend(type_info.annotations)

    vector_schema: res_schema.VectorSchema | None = None
    for annot in annotations:
        vs = await res_schema.get_vector_schema(annot)
        if vs is not None:
            vector_schema = vs
            break

    vector_def = next((a for a in annotations if isinstance(a, ZvecVectorDef)), None)
    zvec_type = next((a for a in annotations if isinstance(a, ZvecType)), None)

    # Dense vector: NumPy ndarray with a VectorSchema.
    if vector_schema is not None:
        if vector_schema.size <= 0:
            raise ValueError(
                f"Invalid vector dimension for {name!r}: {vector_schema.size}"
            )
        vd = vector_def or ZvecVectorDef()
        return _Column(
            name=name,
            kind="dense",
            data_type=_dense_vector_data_type(vector_schema.dtype),
            nullable=type_info.nullable,
            dimension=vector_schema.size,
            metric=vd.metric,
            quantize=vd.quantize,
        )

    # Sparse vector: explicitly marked via ZvecVectorDef(sparse=True).
    if vector_def is not None and vector_def.sparse:
        return _Column(
            name=name,
            kind="sparse",
            data_type=_zvec.DataType.SPARSE_VECTOR_FP32,
            nullable=type_info.nullable,
            metric=vector_def.metric,
        )

    if type_info.base_type is np.ndarray:
        raise ValueError(
            f"Vector column {name!r} requires a VectorSchema (provide it via an "
            "Annotated NDArray or column_overrides)."
        )

    # Scalar field.
    if zvec_type is not None:
        return _Column(
            name=name,
            kind="scalar",
            data_type=zvec_type.data_type,
            nullable=type_info.nullable,
            encoder=zvec_type.encoder,
            indexed=zvec_type.indexed,
        )

    data_type, encoder = _scalar_data_type(type_info)
    return _Column(
        name=name,
        kind="scalar",
        data_type=data_type,
        nullable=type_info.nullable,
        encoder=encoder,
        indexed=True,
    )


@dataclass(slots=True)
class CollectionSchema(Generic[RowT]):
    """Schema definition for a zvec collection.

    Built from a record type via ``from_class``. The single primary-key column
    becomes the document ``id``; the remaining columns become scalar fields or
    vector fields.
    """

    columns: dict[str, _Column]
    primary_key: str
    row_type: type[RowT] | None

    def __init__(
        self,
        columns: dict[str, _Column],
        primary_key: str,
        *,
        row_type: type[RowT] | None = None,
    ) -> None:
        self.columns = columns
        self.primary_key = primary_key
        self.row_type = row_type
        if primary_key not in columns:
            raise ValueError(
                f"Primary key column {primary_key!r} not found in columns: "
                f"{list(columns.keys())}"
            )
        if columns[primary_key].kind != "scalar":
            raise ValueError(
                f"Primary key column {primary_key!r} must be a scalar field, "
                f"got kind {columns[primary_key].kind!r}."
            )

    @classmethod
    async def from_class(
        cls,
        record_type: type[RowT],
        primary_key: list[str],
        *,
        column_overrides: dict[
            str, ZvecType | ZvecVectorDef | res_schema.VectorSchemaProvider
        ]
        | None = None,
    ) -> "CollectionSchema[RowT]":
        """Build a CollectionSchema from a record type.

        Args:
            record_type: A dataclass, NamedTuple, or Pydantic model.
            primary_key: Exactly one column name. Its value becomes the document
                id (converted to ``str``).
            column_overrides: Optional per-column type/vector overrides.
        """
        if not is_record_type(record_type):
            raise TypeError(
                "record_type must be a record type (dataclass, NamedTuple, "
                f"Pydantic model), got {type(record_type)}"
            )
        if len(primary_key) != 1:
            raise ValueError(
                "zvec collections require exactly one primary key column "
                f"(mapped to the document id), got {primary_key}."
            )

        record_info = RecordType(record_type)
        columns: dict[str, _Column] = {}
        for fld in record_info.fields:
            override = column_overrides.get(fld.name) if column_overrides else None
            columns[fld.name] = await _resolve_column(fld.name, fld.type_hint, override)
        return cls(columns, primary_key[0], row_type=record_type)


def _metric_type(metric: str) -> Any:
    key = metric.lower()
    if key == "cosine":
        return _zvec.MetricType.COSINE
    if key == "ip":
        return _zvec.MetricType.IP
    if key == "l2":
        return _zvec.MetricType.L2
    raise ValueError(f"Unsupported metric type: {metric!r}")


def _quantize_type(quantize: str) -> Any | None:
    key = quantize.lower()
    if key == "none":
        return None
    if key == "fp16":
        return _zvec.QuantizeType.FP16
    if key == "int8":
        return _zvec.QuantizeType.INT8
    if key == "int4":
        return _zvec.QuantizeType.INT4
    raise ValueError(f"Unsupported quantize type: {quantize!r}")


def _build_zvec_schema(collection_name: str, schema: CollectionSchema[Any]) -> Any:
    fields: list[Any] = []
    vectors: list[Any] = []
    for name, col in schema.columns.items():
        if name == schema.primary_key:
            continue  # primary key maps to the document id, not a field
        if col.kind == "scalar":
            index_param = _zvec.InvertIndexParam() if col.indexed else None
            fields.append(
                _zvec.FieldSchema(
                    name=name,
                    data_type=col.data_type,
                    nullable=col.nullable,
                    index_param=index_param,
                )
            )
        elif col.kind == "dense":
            quantize = _quantize_type(col.quantize or "none")
            hnsw_kwargs: dict[str, Any] = {
                "metric_type": _metric_type(col.metric or "cosine")
            }
            if quantize is not None:
                hnsw_kwargs["quantize_type"] = quantize
            vectors.append(
                _zvec.VectorSchema(
                    name=name,
                    data_type=col.data_type,
                    dimension=col.dimension,
                    index_param=_zvec.HnswIndexParam(**hnsw_kwargs),
                )
            )
        else:  # sparse
            vectors.append(_zvec.VectorSchema(name=name, data_type=col.data_type))
    return _zvec.CollectionSchema(name=collection_name, fields=fields, vectors=vectors)


# =============================================================================
# Document (row) level
# =============================================================================


class _DocValue(NamedTuple):
    doc_id: str
    vectors: dict[str, Any]
    fields: dict[str, Any]


class _DocAction(NamedTuple):
    doc_id: _DocId
    value: _DocValue | None  # None means delete


def _check_status(result: Any) -> None:
    """Raise if any zvec Status in the result is not OK."""
    statuses = result if isinstance(result, list) else [result]
    for status in statuses:
        if not status.ok():
            raise RuntimeError(
                f"zvec operation failed: code={status.code()} "
                f"message={status.message()}"
            )


def _build_doc(value: _DocValue) -> Any:
    vectors = {k: v for k, v in value.vectors.items() if v is not None}
    fields = {k: v for k, v in value.fields.items() if v is not None}
    return _zvec.Doc(
        id=value.doc_id,
        vectors=vectors or None,
        fields=fields or None,
    )


class _DocHandler(coco.TargetHandler[_DocValue, bytes]):
    """Handler for document-level target states within a collection."""

    _conn: ManagedConnection
    _collection_name: str
    _sink: coco.TargetActionSink[_DocAction, None]

    def __init__(self, conn: ManagedConnection, collection_name: str) -> None:
        self._conn = conn
        self._collection_name = collection_name
        self._sink = coco.TargetActionSink[_DocAction, None].from_fn(
            self._apply_actions
        )

    def _apply_actions(
        self, context_provider: ContextProvider, actions: Sequence[_DocAction]
    ) -> None:
        if not actions:
            return
        col = self._conn.open_existing(self._collection_name)

        upserts: list[Any] = []
        deletes: list[str] = []
        for action in actions:
            if action.value is None:
                deletes.append(action.doc_id)
            else:
                upserts.append(_build_doc(action.value))

        if upserts:
            _check_status(col.upsert(upserts))
        if deletes:
            _check_status(col.delete(ids=deletes))
        if upserts or deletes:
            col.optimize()

    def reconcile(
        self,
        key: coco.StableKey,
        desired_state: _DocValue | coco.NonExistenceType,
        prev_possible_records: Collection[bytes],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[_DocAction, bytes] | None:
        doc_id = _DOC_ID_CHECKER.check(key)
        if coco.is_non_existence(desired_state):
            if not prev_possible_records and not prev_may_be_missing:
                return None
            return coco.TargetReconcileOutput(
                action=_DocAction(doc_id=doc_id, value=None),
                sink=self._sink,
                tracking_record=coco.NON_EXISTENCE,
            )

        target_fp = fingerprint_object(
            (desired_state.doc_id, desired_state.vectors, desired_state.fields)
        )
        if not prev_may_be_missing and all(
            prev == target_fp for prev in prev_possible_records
        ):
            return None

        return coco.TargetReconcileOutput(
            action=_DocAction(doc_id=doc_id, value=desired_state),
            sink=self._sink,
            tracking_record=target_fp,
        )


# =============================================================================
# Collection level
# =============================================================================


class _CollectionKey(NamedTuple):
    db_key: str
    collection_name: str


@dataclass
class _CollectionSpec:
    schema: CollectionSchema[Any]
    managed_by: target.ManagedBy = target.ManagedBy.SYSTEM


class _ColumnTrackingRecord(msgspec.Struct, frozen=True, array_like=True):
    name: str
    kind: str
    data_type: str
    nullable: bool
    dimension: int | None
    metric: str | None
    quantize: str | None
    indexed: bool


class _CollectionTrackingRecordCore(msgspec.Struct, frozen=True, array_like=True):
    primary_key: str
    columns: tuple[_ColumnTrackingRecord, ...]


_CollectionTrackingRecord = statediff.MutualTrackingRecord[
    _CollectionTrackingRecordCore
]


def _tracking_core_from_spec(spec: _CollectionSpec) -> _CollectionTrackingRecordCore:
    schema = spec.schema
    columns = tuple(
        _ColumnTrackingRecord(
            name=col.name,
            kind=col.kind,
            data_type=str(col.data_type),
            nullable=col.nullable,
            dimension=col.dimension,
            metric=col.metric,
            quantize=col.quantize,
            indexed=col.indexed,
        )
        for name, col in sorted(schema.columns.items())
        if name != schema.primary_key
    )
    return _CollectionTrackingRecordCore(
        primary_key=schema.primary_key, columns=columns
    )


class _CollectionAction(NamedTuple):
    key: _CollectionKey
    spec: _CollectionSpec | coco.NonExistenceType
    main_action: statediff.DiffAction | None


def _apply_collection_actions(
    context_provider: ContextProvider, actions: Sequence[_CollectionAction]
) -> list[coco.ChildTargetDef[_DocHandler] | None]:
    actions_list = list(actions)
    outputs: list[coco.ChildTargetDef[_DocHandler] | None] = [None] * len(actions_list)

    by_key: dict[_CollectionKey, list[int]] = {}
    for i, action in enumerate(actions_list):
        by_key.setdefault(action.key, []).append(i)

    for key, idxs in by_key.items():
        conn = context_provider.get(key.db_key, ManagedConnection)
        for i in idxs:
            action = actions_list[i]

            # A non-None main action implies a system-managed collection
            # (resolve_system_transition yields None for user-managed ones).
            if action.main_action in ("replace", "delete"):
                conn.destroy(key.collection_name)

            if coco.is_non_existence(action.spec):
                outputs[i] = None
                continue

            spec = action.spec
            outputs[i] = coco.ChildTargetDef(
                handler=_DocHandler(conn, key.collection_name)
            )

            if action.main_action in ("insert", "upsert", "replace"):
                conn.open_or_create(
                    key.collection_name,
                    _build_zvec_schema(key.collection_name, spec.schema),
                )

    return outputs


_collection_action_sink = coco.TargetActionSink[_CollectionAction, _DocHandler].from_fn(
    _apply_collection_actions
)


class _CollectionHandler(
    coco.TargetHandler[_CollectionSpec, _CollectionTrackingRecord, _DocHandler]
):
    def reconcile(
        self,
        key: coco.StableKey,
        desired_state: _CollectionSpec | coco.NonExistenceType,
        prev_possible_records: Collection[_CollectionTrackingRecord],
        prev_may_be_missing: bool,
        /,
    ) -> (
        coco.TargetReconcileOutput[
            _CollectionAction, _CollectionTrackingRecord, _DocHandler
        ]
        | None
    ):
        key = _CollectionKey(*_COLLECTION_KEY_CHECKER.check(key))
        tracking_record: _CollectionTrackingRecord | coco.NonExistenceType
        if coco.is_non_existence(desired_state):
            tracking_record = coco.NON_EXISTENCE
        else:
            tracking_record = statediff.MutualTrackingRecord(
                tracking_record=_tracking_core_from_spec(desired_state),
                managed_by=desired_state.managed_by,
            )

        resolved = statediff.resolve_system_transition(
            statediff.TrackingRecordTransition(
                tracking_record, prev_possible_records, prev_may_be_missing
            )
        )
        main_action = statediff.diff(resolved)

        # v1 has no in-place schema evolution: any schema change rebuilds the
        # collection, which destroys all documents.
        child_invalidation: Literal["destructive"] | None = (
            "destructive" if main_action == "replace" else None
        )

        return coco.TargetReconcileOutput(
            action=_CollectionAction(
                key=key, spec=desired_state, main_action=main_action
            ),
            sink=_collection_action_sink,
            tracking_record=tracking_record,
            child_invalidation=child_invalidation,
        )


_collection_provider = coco.register_root_target_states_provider(
    "cocoindex/zvec/collection", _CollectionHandler()
)


# =============================================================================
# User-facing API
# =============================================================================


def _row_get(row: Any, name: str) -> Any:
    if isinstance(row, dict):
        return row.get(name)
    return getattr(row, name)


def _to_float_list(value: Any) -> list[float]:
    if isinstance(value, np.ndarray):
        return cast(list[float], value.astype(float).tolist())
    return [float(x) for x in value]


class CollectionTarget(
    Generic[RowT, coco.MaybePendingS], coco.ResolvesTo["CollectionTarget[RowT]"]
):
    """A target for writing documents to a zvec collection."""

    _provider: coco.TargetStateProvider[_DocValue, None, coco.MaybePendingS]
    _schema: CollectionSchema[RowT]

    def __init__(
        self,
        provider: coco.TargetStateProvider[_DocValue, None, coco.MaybePendingS],
        schema: CollectionSchema[RowT],
    ) -> None:
        self._provider = provider
        self._schema = schema

    def declare_row(self: "CollectionTarget[RowT]", *, row: RowT) -> None:
        """Declare a document (row) to be upserted to this collection.

        The primary-key value becomes the document id (converted to ``str``).
        """
        schema = self._schema
        pk_value = _row_get(row, schema.primary_key)
        if pk_value is None:
            raise ValueError(
                f"Primary key {schema.primary_key!r} value cannot be None."
            )
        doc_id = str(pk_value)

        vectors: dict[str, Any] = {}
        fields: dict[str, Any] = {}
        for name, col in schema.columns.items():
            if name == schema.primary_key:
                continue
            value = _row_get(row, name)
            if col.kind == "dense":
                vectors[name] = None if value is None else _to_float_list(value)
            elif col.kind == "sparse":
                vectors[name] = (
                    None
                    if value is None
                    else {int(k): float(v) for k, v in dict(value).items()}
                )
            else:
                if value is not None and col.encoder is not None:
                    value = col.encoder(value)
                fields[name] = value

        coco.declare_target_state(
            self._provider.target_state(
                doc_id, _DocValue(doc_id=doc_id, vectors=vectors, fields=fields)
            )
        )

    def __coco_memo_key__(self) -> str:
        return self._provider.memo_key


def collection_target(
    db: ContextKey[ManagedConnection],
    collection_name: str,
    schema: CollectionSchema[RowT],
    *,
    managed_by: target.ManagedBy = target.ManagedBy.SYSTEM,
) -> "coco.TargetState[_DocHandler]":
    """Create a TargetState for a zvec collection target.

    Use with ``coco.mount_target()`` or the convenience wrappers
    ``declare_collection_target()`` / ``mount_collection_target()``.

    Args:
        db: A ContextKey for the ManagedConnection (provided via lifespan).
        collection_name: Name of the collection (a subdirectory under the
            connection's base path).
        schema: Schema definition built via ``CollectionSchema.from_class``.
        managed_by: Whether CocoIndex manages the collection lifecycle
            ("system") or it must already exist ("user", documents only).
    """
    _validate_collection_name(collection_name)
    for name in schema.columns:
        if name != schema.primary_key:
            _validate_identifier(name, "field name")

    if not any(
        col.kind in ("dense", "sparse")
        for name, col in schema.columns.items()
        if name != schema.primary_key
    ):
        raise ValueError(
            "zvec collections require at least one vector field (dense or sparse)."
        )

    key = _CollectionKey(db_key=db.key, collection_name=collection_name)
    spec = _CollectionSpec(schema=schema, managed_by=managed_by)
    return _collection_provider.target_state(key, spec)


def declare_collection_target(
    db: ContextKey[ManagedConnection],
    collection_name: str,
    schema: CollectionSchema[RowT],
    *,
    managed_by: target.ManagedBy = target.ManagedBy.SYSTEM,
) -> "CollectionTarget[RowT, coco.PendingS]":
    """Declare a zvec collection target and return a CollectionTarget for rows."""
    provider = coco.declare_target_state_with_child(
        collection_target(db, collection_name, schema, managed_by=managed_by)
    )
    return CollectionTarget(provider, schema)


async def mount_collection_target(
    db: ContextKey[ManagedConnection],
    collection_name: str,
    schema: CollectionSchema[RowT],
    *,
    managed_by: target.ManagedBy = target.ManagedBy.SYSTEM,
) -> "CollectionTarget[RowT]":
    """Mount a zvec collection target and return a ready-to-use CollectionTarget."""
    provider = await coco.mount_target(
        collection_target(db, collection_name, schema, managed_by=managed_by)
    )
    return CollectionTarget(provider, schema)


__all__ = [
    "CollectionSchema",
    "CollectionTarget",
    "ManagedConnection",
    "ValueEncoder",
    "ZvecType",
    "ZvecVectorDef",
    "collection_target",
    "connect",
    "declare_collection_target",
    "managed_connection",
    "mount_collection_target",
]
