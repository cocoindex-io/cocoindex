"""
LadybugDB target for CocoIndex.

Two-level state system:
1. Table level — creates/drops node tables (``CREATE NODE TABLE``) and
   relationship tables (``CREATE REL TABLE``), and evolves their columns
   with ``ALTER TABLE ... ADD`` / ``DROP``. A system-managed table owns its
   label / relationship type: dropping it deletes its contents first.
2. Record level — upserts/deletes nodes via Cypher ``MERGE`` and edges via
   triple-MERGE (source, target, relationship).

Node tables are declared with the node API (``mount_node_target``) and
relationship tables with the relationship API
(``mount_relationship_target``).

LadybugDB is an embedded graph database (a Kuzu-derived fork): the database
lives in a file (or ``:memory:``), addressed via a ``ManagedConnection``
provided through a ``ContextKey``. Unlike server graph stores, there is no
secondary or vector index DDL — ``PRIMARY KEY`` is the only index, and
vectors live in fixed-size ``DOUBLE[n]`` / ``FLOAT[n]`` columns (compared
with functions like ``array_cosine_similarity``).

Unlike FalkorDB/Neo4j, LadybugDB enforces column types server-side, so a
``TableSchema`` is required (not optional) for every table.
"""

from __future__ import annotations

import datetime
import decimal
import json
import logging
import threading
import uuid as uuid_mod
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import (
    Any,
    Callable,
    Collection,
    Generic,
    Literal,
    NamedTuple,
    Sequence,
)

from typing_extensions import TypeVar

from cocoindex._internal.extras import install_hint

try:
    import ladybug as _ladybug  # type: ignore[import-not-found]
except ImportError as e:
    raise ImportError(
        "ladybug is required to use the LadybugDB connector. " + install_hint("ladybug")
    ) from e

import msgspec
import numpy as np

import cocoindex as coco
from cocoindex.connectorkits import statediff, target
from cocoindex.connectorkits._graph_relationship_ids import relationship_id
from cocoindex.connectorkits.fingerprint import fingerprint_object
from cocoindex._internal.datatype import (
    AnyType,
    MappingType,
    SequenceType,
    RecordType,
    TypeChecker,
    UnionType,
    analyze_type_info,
    is_record_type,
)
from cocoindex.resources import schema as res_schema
from cocoindex._internal.context_keys import ContextKey, ContextProvider

from . import _cypher

_logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Identifier validation
# ---------------------------------------------------------------------------

_IDENTIFIER_RE = _cypher.IDENTIFIER_RE
_validate_identifier = _cypher.validate_identifier


# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------


class ManagedConnection:
    """
    A LadybugDB database with a shared connection for CocoIndex.

    LadybugDB is embedded: the database lives in a file (or ``:memory:``).
    The connection serializes queries with a lock — one ``ManagedConnection``
    is shared by every table and apply batch of its database.

    Create with :func:`connect`, or use :func:`managed_connection` as a
    context manager suitable for ``builder.provide_with()`` in a lifespan.
    """

    _database: Any
    _conn: Any
    _lock: threading.Lock

    def __init__(self, database: Any) -> None:
        self._database = database
        self._conn = _ladybug.Connection(database)
        self._lock = threading.Lock()

    @property
    def database(self) -> Any:
        """The underlying ``ladybug.Database`` (e.g. for ad-hoc reads)."""
        return self._database

    def execute(self, cypher: str, parameters: dict[str, Any] | None = None) -> None:
        """Execute a write query; raises on failure."""
        with self._lock:
            self._conn.execute(cypher, parameters or {})

    def close(self) -> None:
        """Close the underlying connection."""
        with self._lock:
            self._conn.close()


def connect(database: str | Path, **kwargs: Any) -> ManagedConnection:
    """
    Open (or create) a LadybugDB database and return a ``ManagedConnection``.

    Args:
        database: Path to the database file, or ``":memory:"`` for an
            in-memory database.
        **kwargs: Additional arguments forwarded to ``ladybug.Database``
            (e.g. ``buffer_pool_size``, ``max_num_threads``).

    Example:
        ```python
        managed_conn = connect("knowledge.lbdb")
        # Or for in-memory:
        managed_conn = connect(":memory:")
        ```
    """
    database_str = str(database) if isinstance(database, Path) else database
    return ManagedConnection(_ladybug.Database(database_str, **kwargs))


@contextmanager
def managed_connection(
    database: str | Path, **kwargs: Any
) -> Iterator[ManagedConnection]:
    """
    Open a LadybugDB database as a context manager.

    Yields a ManagedConnection and closes it on exit. Suitable for use with
    ``builder.provide_with()`` in a lifespan function.

    Example:
        ```python
        @coco.lifespan
        async def _lifespan(builder: EnvironmentBuilder) -> None:
            builder.provide_with(LADYBUG_DB, managed_connection("knowledge.lbdb"))
            yield
        ```
    """
    managed_conn = connect(database, **kwargs)
    try:
        yield managed_conn
    finally:
        managed_conn.close()


# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------

_RowKey = tuple[Any, ...]  # The primary key tuple — single-element in v1.0
_ROW_KEY_CHECKER = TypeChecker(tuple[Any, ...])
_RowFingerprint = bytes


class _RelationRowValue(NamedTuple):
    """Value type for relation records.

    Endpoint metadata is structured (not pre-formatted strings) so values can
    bind via ``$``-parameters at query time rather than being string-interpolated.
    """

    from_label: str
    from_pk_field: str
    from_id: Any
    to_label: str
    to_pk_field: str
    to_id: Any
    fields: dict[str, Any]


_RowValue = dict[str, Any] | _RelationRowValue
ValueEncoder = Callable[[Any], Any]


# ---------------------------------------------------------------------------
# LadybugType annotation
# ---------------------------------------------------------------------------


class LadybugType(NamedTuple):
    """
    Annotation to override the default Python → LadybugDB type mapping for a
    column. The string is the Cypher DDL type used in ``CREATE NODE TABLE``
    (and contributes to the schema fingerprint).

    Use with ``typing.Annotated``::

        from typing import Annotated
        from cocoindex.connectors.ladybug import LadybugType

        @dataclass
        class Row:
            id: str
            score: Annotated[float, LadybugType("DECIMAL", encoder=str)]
    """

    ladybug_type: str
    encoder: ValueEncoder | None = None


# ---------------------------------------------------------------------------
# Value encoders
# ---------------------------------------------------------------------------


def _uuid_str(value: Any) -> str:
    if isinstance(value, str):
        return value
    return str(value)


def _ndarray_to_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    return value.tolist()  # type: ignore[no-any-return]


def _json_encoder(value: Any) -> str:
    """Encode a value to a JSON string for LadybugDB JSON columns."""
    return json.dumps(value, default=str)


# ---------------------------------------------------------------------------
# Type mapping
# ---------------------------------------------------------------------------


class _TypeMapping(NamedTuple):
    ladybug_type: str
    encoder: ValueEncoder | None = None


_LEAF_TYPE_MAPPINGS: dict[type, _TypeMapping] = {
    # Boolean
    bool: _TypeMapping("BOOL"),
    # Integers
    int: _TypeMapping("INT64"),
    np.int8: _TypeMapping("INT8"),
    np.int16: _TypeMapping("INT16"),
    np.int32: _TypeMapping("INT32"),
    np.int64: _TypeMapping("INT64"),
    np.uint8: _TypeMapping("UINT8"),
    np.uint16: _TypeMapping("UINT16"),
    np.uint32: _TypeMapping("UINT32"),
    np.uint64: _TypeMapping("UINT64"),
    np.int_: _TypeMapping("INT64"),
    np.uint: _TypeMapping("UINT64"),
    # Floats
    float: _TypeMapping("DOUBLE"),
    np.float16: _TypeMapping("FLOAT"),
    np.float32: _TypeMapping("FLOAT"),
    np.float64: _TypeMapping("DOUBLE"),
    # Decimal — LadybugDB has a native DECIMAL type.
    decimal.Decimal: _TypeMapping("DECIMAL"),
    # Strings
    str: _TypeMapping("STRING"),
    bytes: _TypeMapping("BLOB"),
    uuid_mod.UUID: _TypeMapping("UUID", _uuid_str),
    # Date/time
    datetime.date: _TypeMapping("DATE"),
    datetime.time: _TypeMapping("TIME"),
    datetime.datetime: _TypeMapping("TIMESTAMP"),
    datetime.timedelta: _TypeMapping("INTERVAL"),
}

_JSON_MAPPING = _TypeMapping("JSON", _json_encoder)


def _get_type_mapping(
    python_type: Any, *, vector_schema: res_schema.VectorSchema | None = None
) -> _TypeMapping:
    """Map a leaf Python type to a LadybugDB DDL type.

    Complex types (nested records, mappings, unions, ``Any``) map to the
    native ``JSON`` type, which stays queryable (``u.data.name``). Fixed-size
    ``list[T]`` maps to ``T[]``; variable-shape values map to ``JSON``.
    """
    type_info = analyze_type_info(python_type)

    for annotation in type_info.annotations:
        if isinstance(annotation, LadybugType):
            return _TypeMapping(annotation.ladybug_type, annotation.encoder)

    base_type = type_info.base_type

    if base_type in _LEAF_TYPE_MAPPINGS:
        return _LEAF_TYPE_MAPPINGS[base_type]

    if base_type is np.ndarray:
        if vector_schema is None:
            raise ValueError("VectorSchemaProvider is required for NumPy ndarray type.")
        if vector_schema.size <= 0:
            raise ValueError(f"Invalid vector dimension: {vector_schema.size}")
        elem = (
            "FLOAT"
            if vector_schema.dtype in (np.half, np.float16, np.float32)
            else "DOUBLE"
        )
        return _TypeMapping(
            ladybug_type=f"{elem}[{vector_schema.size}]",
            encoder=_ndarray_to_list,
        )
    elif vector_schema is not None:
        raise ValueError(
            "VectorSchemaProvider is only supported for NumPy ndarray type. "
            f"Got type: {python_type}"
        )

    if isinstance(type_info.variant, SequenceType):
        elem_mapping = _get_type_mapping(type_info.variant.elem_type)
        if elem_mapping.encoder is None and elem_mapping.ladybug_type not in ("JSON",):
            return _TypeMapping(f"{elem_mapping.ladybug_type}[]")
        if elem_mapping.encoder is not None and elem_mapping.ladybug_type not in (
            "JSON",
        ):
            elem_encoder = elem_mapping.encoder

            def _list_encoder(value: Any) -> Any:
                if isinstance(value, (str, bytes)) or value is None:
                    return value
                assert elem_encoder is not None
                return [elem_encoder(v) if v is not None else None for v in value]

            return _TypeMapping(f"{elem_mapping.ladybug_type}[]", _list_encoder)
        return _JSON_MAPPING

    if isinstance(type_info.variant, (MappingType, RecordType, UnionType, AnyType)):
        return _JSON_MAPPING

    return _JSON_MAPPING


# ---------------------------------------------------------------------------
# ColumnDef
# ---------------------------------------------------------------------------


class ColumnDef(NamedTuple):
    """Definition of a column (property) in a LadybugDB table.

    ``type`` is the Cypher DDL type emitted in ``CREATE NODE TABLE`` /
    ``CREATE REL TABLE`` — LadybugDB enforces it server-side. It also
    participates in the schema fingerprint, so two flows declaring the same
    table must agree.
    """

    type: str
    nullable: bool = True
    encoder: ValueEncoder | None = None


# ---------------------------------------------------------------------------
# TableSchema
# ---------------------------------------------------------------------------

RowT = TypeVar("RowT", default=dict[str, Any])


@dataclass(slots=True)
class TableSchema(Generic[RowT]):
    """Schema definition for a LadybugDB table (node table or relationship table).

    Single-field primary key (named via ``primary_key``, default ``"id"``).
    Compound primary keys are not supported in v1.0. The schema is required:
    LadybugDB enforces column types server-side, so every table declaration
    must carry one.
    """

    columns: dict[str, ColumnDef]
    primary_key: str
    row_type: type[RowT] | None

    def __init__(
        self,
        columns: dict[str, ColumnDef],
        *,
        primary_key: str = "id",
        row_type: type[RowT] | None = None,
    ) -> None:
        for col_name in columns:
            _validate_identifier(col_name, "column name")
        if primary_key not in columns:
            raise ValueError(
                f"primary_key {primary_key!r} not found in columns "
                f"({sorted(columns)!r})"
            )
        self.columns = columns
        self.primary_key = primary_key
        self.row_type = row_type

    @property
    def value_field_names(self) -> list[str]:
        """Column names other than the primary key, in declared order."""
        return [c for c in self.columns if c != self.primary_key]

    @classmethod
    async def from_class(
        cls,
        record_type: type[RowT],
        *,
        primary_key: str = "id",
        column_overrides: dict[str, LadybugType | res_schema.VectorSchemaProvider]
        | None = None,
    ) -> "TableSchema[RowT]":
        """Build a TableSchema by introspecting a dataclass / NamedTuple / Pydantic model."""
        if not is_record_type(record_type):
            raise TypeError(
                f"record_type must be a record type (dataclass, NamedTuple, "
                f"Pydantic model), got {type(record_type)}"
            )
        columns = await cls._columns_from_record_type(record_type, column_overrides)
        return cls(columns, primary_key=primary_key, row_type=record_type)

    @staticmethod
    async def _columns_from_record_type(
        record_type: type,
        column_overrides: dict[str, LadybugType | res_schema.VectorSchemaProvider]
        | None,
    ) -> dict[str, ColumnDef]:
        record_info = RecordType(record_type)
        columns: dict[str, ColumnDef] = {}

        for field in record_info.fields:
            type_info = analyze_type_info(field.type_hint)

            all_annotations: list[Any] = []
            if (
                override := column_overrides and column_overrides.get(field.name)
            ) is not None:
                all_annotations.append(override)
            all_annotations.extend(type_info.annotations)

            ladybug_type_annotation = next(
                (t for t in all_annotations if isinstance(t, LadybugType)), None
            )
            vector_schema = None
            for annot in all_annotations:
                vs = await res_schema.get_vector_schema(annot)
                if vs is not None:
                    vector_schema = vs
                    break

            if ladybug_type_annotation is not None:
                type_mapping = _TypeMapping(
                    ladybug_type_annotation.ladybug_type,
                    ladybug_type_annotation.encoder,
                )
            else:
                type_mapping = _get_type_mapping(
                    field.type_hint, vector_schema=vector_schema
                )

            columns[field.name] = ColumnDef(
                type=type_mapping.ladybug_type.strip(),
                nullable=type_info.nullable,
                encoder=type_mapping.encoder,
            )

        return columns


# ---------------------------------------------------------------------------
# _RecordAction + shared record sink
# ---------------------------------------------------------------------------


class _RecordAction(NamedTuple):
    """Action to perform on a record (upsert or delete)."""

    table_name: str
    is_relation: bool
    pk_field: str
    record_id: Any
    value: dict[str, Any] | None  # None = delete
    # Relation endpoints (None for non-relation actions).
    from_label: str | None
    from_pk_field: str | None
    from_id: Any | None
    to_label: str | None
    to_pk_field: str | None
    to_id: Any | None


@dataclass(frozen=True)
class _SharedRecordApplier:
    """The record sink callback shared by every record handler of one database.

    Equal for the same database key, so the record sinks of all tables of one
    database — node and relation tables alike — share one identity (see
    ``coco.TargetActionSink``) and the engine batches their actions together.

    Actions are grouped into node upserts → relation upserts → relation
    deletes → node deletes, so an edge is never written before its endpoints
    exist. Each action is one ``execute`` call; all writes are idempotent
    (``MERGE``), so a retried batch is safe.
    """

    db_key: str

    def __call__(
        self, context_provider: ContextProvider, actions: Sequence[_RecordAction]
    ) -> None:
        if not actions:
            return

        upsert_normal: list[_RecordAction] = []
        upsert_relation: list[_RecordAction] = []
        delete_relation: list[_RecordAction] = []
        delete_normal: list[_RecordAction] = []

        for action in actions:
            if action.value is not None:
                if action.is_relation:
                    upsert_relation.append(action)
                else:
                    upsert_normal.append(action)
            else:
                if action.is_relation:
                    delete_relation.append(action)
                else:
                    delete_normal.append(action)

        conn = context_provider.get(self.db_key, ManagedConnection)
        for action in upsert_normal:
            self._apply_node_upsert(conn, action)
        for action in upsert_relation:
            self._apply_relation_upsert(conn, action)
        for action in delete_relation:
            self._apply_relation_delete(conn, action)
        for action in delete_normal:
            self._apply_node_delete(conn, action)

    @staticmethod
    def _apply_node_upsert(conn: ManagedConnection, action: _RecordAction) -> None:
        assert action.value is not None
        # PK is always single-field in v1.0; every other field is SET one by one
        # (LadybugDB has no SET n += $props map form).
        value_fields = [k for k in action.value if k != action.pk_field]
        cypher = _cypher.build_node_upsert(
            label=action.table_name,
            pk_fields=[action.pk_field],
            value_fields=value_fields,
        )
        params: dict[str, Any] = {"key_0": action.value.get(action.pk_field)}
        for i, field in enumerate(value_fields):
            params[f"prop_{i}"] = action.value[field]
        conn.execute(cypher, params)

    @staticmethod
    def _apply_node_delete(conn: ManagedConnection, action: _RecordAction) -> None:
        cypher = _cypher.build_node_delete(
            label=action.table_name, pk_fields=[action.pk_field]
        )
        conn.execute(cypher, {"key_0": action.record_id})

    @staticmethod
    def _apply_relation_upsert(conn: ManagedConnection, action: _RecordAction) -> None:
        assert action.value is not None
        assert action.from_label is not None and action.from_pk_field is not None
        assert action.to_label is not None and action.to_pk_field is not None
        value_fields = [k for k in action.value if k != action.pk_field]
        cypher = _cypher.build_relationship_upsert(
            rel_type=action.table_name,
            from_label=action.from_label,
            from_pk_fields=[action.from_pk_field],
            to_label=action.to_label,
            to_pk_fields=[action.to_pk_field],
            rel_pk_fields=[action.pk_field],
            value_fields=value_fields,
            has_legacy_key=False,
        )
        params: dict[str, Any] = {
            "from_key_0": action.from_id,
            "to_key_0": action.to_id,
            "rel_key_0": action.record_id,
        }
        for i, field in enumerate(value_fields):
            params[f"rel_prop_{i}"] = action.value[field]
        conn.execute(cypher, params)

    @staticmethod
    def _apply_relation_delete(conn: ManagedConnection, action: _RecordAction) -> None:
        cypher = _cypher.build_relationship_delete(
            rel_type=action.table_name, pk_fields=[action.pk_field]
        )
        conn.execute(cypher, {"key_0": action.record_id})


def _record_sink(db_key: str) -> coco.TargetActionSink[_RecordAction]:
    # One sink identity per database so all of that database's record writes
    # batch together. The frozen dataclass callback keys the identity by value.
    return coco.TargetActionSink.from_fn(_SharedRecordApplier(db_key))


# ---------------------------------------------------------------------------
# _RecordHandler
# ---------------------------------------------------------------------------


class _RecordHandler(coco.TargetHandler[_RowValue, _RowFingerprint]):
    """Handler for record-level target states within a LadybugDB table.

    Built by the table sink's ``child_handler`` factory, so it holds no
    connection: its sinks resolve one from the context provider at apply time.
    """

    tracks_value_fingerprint = True

    _table_name: str
    _is_relation: bool
    _pk_field: str
    _table_schema: TableSchema[Any]
    _db_key: str
    _sink: coco.TargetActionSink[_RecordAction]

    def __init__(
        self,
        table_name: str,
        is_relation: bool,
        pk_field: str,
        table_schema: TableSchema[Any],
        db_key: str,
    ) -> None:
        self._table_name = table_name
        self._is_relation = is_relation
        self._pk_field = pk_field
        self._table_schema = table_schema
        self._db_key = db_key
        self._sink = _record_sink(db_key)

    def _encode_row(self, row_dict: dict[str, Any]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for k, v in row_dict.items():
            col = self._table_schema.columns.get(k)
            if col is not None and col.encoder is not None and v is not None:
                out[k] = col.encoder(v)
            else:
                out[k] = v
        return out

    def reconcile(
        self,
        key: coco.StableKey,
        desired_state: _RowValue | coco.NonExistenceType,
        prev_possible_records: Collection[_RowFingerprint],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[_RecordAction, _RowFingerprint] | None:
        key = _ROW_KEY_CHECKER.check(key)

        if coco.is_non_existence(desired_state):
            if not prev_possible_records and not prev_may_be_missing:
                return None
            return coco.TargetReconcileOutput(
                action=_RecordAction(
                    table_name=self._table_name,
                    is_relation=self._is_relation,
                    pk_field=self._pk_field,
                    record_id=key[0],
                    value=None,
                    from_label=None,
                    from_pk_field=None,
                    from_id=None,
                    to_label=None,
                    to_pk_field=None,
                    to_id=None,
                ),
                sink=self._sink,
                tracking_record=coco.NON_EXISTENCE,
            )

        target_fp = fingerprint_object(desired_state)
        if not prev_may_be_missing and all(
            prev == target_fp for prev in prev_possible_records
        ):
            return None

        if isinstance(desired_state, _RelationRowValue):
            from_label = desired_state.from_label
            from_pk_field = desired_state.from_pk_field
            from_id = desired_state.from_id
            to_label = desired_state.to_label
            to_pk_field = desired_state.to_pk_field
            to_id = desired_state.to_id
            encoded = self._encode_row(desired_state.fields)
        else:
            from_label = None
            from_pk_field = None
            from_id = None
            to_label = None
            to_pk_field = None
            to_id = None
            encoded = self._encode_row(desired_state)

        return coco.TargetReconcileOutput(
            action=_RecordAction(
                table_name=self._table_name,
                is_relation=self._is_relation,
                pk_field=self._pk_field,
                record_id=key[0],
                value=encoded,
                from_label=from_label,
                from_pk_field=from_pk_field,
                from_id=from_id,
                to_label=to_label,
                to_pk_field=to_pk_field,
                to_id=to_id,
            ),
            sink=self._sink,
            tracking_record=target_fp,
        )


# ---------------------------------------------------------------------------
# Table-level types
# ---------------------------------------------------------------------------


class _TableKey(NamedTuple):
    db_key: str
    table_name: str


_TABLE_KEY_CHECKER = TypeChecker(tuple[str, str])


@dataclass
class _TableSpec:
    table_schema: TableSchema[Any]
    primary_key: str
    is_relation: bool
    from_label: str | None
    from_pk_field: str | None
    to_label: str | None
    to_pk_field: str | None
    managed_by: target.ManagedBy = target.ManagedBy.SYSTEM


class _TableMainRecord(msgspec.Struct, frozen=True):
    """Identity-defining table properties.

    A change here means existing nodes / relationships can no longer be
    matched record by record (their MERGE key pattern changed), so the table
    is rebuilt: contents deleted, table dropped and recreated, children
    re-declared from scratch. Everything else about the schema is tracked per
    field and evolved with ``ALTER TABLE``.
    """

    is_relation: bool
    primary_key: str
    from_label: str | None
    from_pk_field: str | None
    to_label: str | None
    to_pk_field: str | None


class _FieldTrackingRecord(msgspec.Struct, frozen=True):
    """Per-field tracking record, primary key included."""

    ladybug_type: str
    nullable: bool


_FIELD_SUBKEY_PREFIX: str = "field:"


def _field_subkey(name: str) -> str:
    return f"{_FIELD_SUBKEY_PREFIX}{name}"


class _TableAction(NamedTuple):
    key: _TableKey
    spec: _TableSpec | coco.NonExistenceType
    is_relation: bool
    main_action: statediff.DiffAction | None
    column_actions: dict[str, statediff.DiffAction]
    # Recovered from the most recent system-managed prev tracking record.
    # Needed on "delete"/"replace" to know what to destroy.
    prev_pk_field: str | None
    prev_is_relation: bool


def _table_composite_tracking_record_from_spec(
    spec: _TableSpec,
) -> statediff.CompositeTrackingRecord[_TableMainRecord, str, _FieldTrackingRecord]:
    sub: dict[str, _FieldTrackingRecord] = {}
    for col_name, col_def in spec.table_schema.columns.items():
        sub[_field_subkey(col_name)] = _FieldTrackingRecord(
            ladybug_type=col_def.type,
            nullable=col_def.nullable,
        )

    main = _TableMainRecord(
        is_relation=spec.is_relation,
        primary_key=spec.primary_key,
        from_label=spec.from_label,
        from_pk_field=spec.from_pk_field,
        to_label=spec.to_label,
        to_pk_field=spec.to_pk_field,
    )
    return statediff.CompositeTrackingRecord(main=main, sub=sub)


_TableTrackingRecord = statediff.MutualTrackingRecord[
    statediff.CompositeTrackingRecord[_TableMainRecord, str, _FieldTrackingRecord]
]


# ---------------------------------------------------------------------------
# _TableHandler
# ---------------------------------------------------------------------------


class _TableHandler(
    coco.TargetHandler[_TableSpec, _TableTrackingRecord, _RecordHandler]
):
    """Handler for table-level state — ``CREATE`` / ``DROP`` / ``ALTER TABLE``."""

    _sink: coco.TargetActionSink[_TableAction]

    def __init__(self) -> None:
        self._sink = coco.TargetActionSink.from_fn(
            self._apply_actions, child_handler=self._record_handler
        )

    @staticmethod
    def _record_handler(
        context_provider: ContextProvider, action: _TableAction, /
    ) -> _RecordHandler:
        """The record handler for a created or updated table's records."""
        spec = action.spec
        assert not coco.is_non_existence(spec)
        return _RecordHandler(
            table_name=action.key.table_name,
            is_relation=spec.is_relation,
            pk_field=spec.primary_key,
            table_schema=spec.table_schema,
            db_key=action.key.db_key,
        )

    def reconcile(
        self,
        key: coco.StableKey,
        desired_state: _TableSpec | coco.NonExistenceType,
        prev_possible_records: Collection[_TableTrackingRecord],
        prev_may_be_missing: bool,
        /,
    ) -> (
        coco.TargetReconcileOutput[_TableAction, _TableTrackingRecord, _RecordHandler]
        | None
    ):
        key = _TableKey(*_TABLE_KEY_CHECKER.check(key))

        if coco.is_non_existence(desired_state):
            tracking_record: _TableTrackingRecord | coco.NonExistenceType = (
                coco.NON_EXISTENCE
            )
            is_relation = False
        else:
            tracking_record = statediff.MutualTrackingRecord(
                tracking_record=_table_composite_tracking_record_from_spec(
                    desired_state
                ),
                managed_by=desired_state.managed_by,
            )
            is_relation = desired_state.is_relation

        resolved = statediff.resolve_system_transition(
            statediff.TrackingRecordTransition(
                tracking_record, prev_possible_records, prev_may_be_missing
            )
        )
        main_action, column_transitions = statediff.diff_composite(resolved)

        # "upsert" means the table may or may not already exist: the CREATE can
        # land on a table carrying the previous column set, so its columns still
        # need reconciling. "insert" / "replace" define the table from the
        # desired schema, so there is nothing left to reconcile.
        column_actions: dict[str, statediff.DiffAction] = {}
        if main_action is None or main_action == "upsert":
            for sub_key, t in column_transitions.items():
                action = statediff.diff(t)
                if action is not None:
                    column_actions[sub_key] = action

        if (
            main_action is None
            and not column_actions
            and coco.is_non_existence(desired_state)
        ):
            return None

        # Recover prev PK + entity kind from the most recent system-managed
        # tracking record so the drop path knows what to destroy.
        prev_pk_field: str | None = None
        prev_is_relation = False
        for prev in prev_possible_records:
            if prev.managed_by != target.ManagedBy.SYSTEM:
                continue
            prev_pk_field = prev.tracking_record.main.primary_key
            prev_is_relation = prev.tracking_record.main.is_relation
            break

        child_invalidation: Literal["destructive", "lossy"] | None = None
        if main_action == "replace":
            # The table is rebuilt: its contents are deleted and re-declared.
            child_invalidation = "destructive"
        elif any(a != "insert" for a in column_actions.values()):
            # A removed or re-typed column loses data; dependents re-upsert
            # defensively.
            child_invalidation = "lossy"

        return coco.TargetReconcileOutput(
            action=_TableAction(
                key=key,
                spec=desired_state,
                is_relation=is_relation,
                main_action=main_action,
                column_actions=column_actions,
                prev_pk_field=prev_pk_field,
                prev_is_relation=prev_is_relation,
            ),
            sink=self._sink,
            tracking_record=tracking_record,
            child_invalidation=child_invalidation,
        )

    def _apply_actions(
        self,
        context_provider: ContextProvider,
        actions: Sequence[_TableAction],
        /,
    ) -> None:
        # Group by db_key so each database is handled in one pass.
        by_db: dict[str, list[_TableAction]] = {}
        for action in actions:
            by_db.setdefault(action.key.db_key, []).append(action)

        for db_key, db_actions in by_db.items():
            conn = context_provider.get(db_key, ManagedConnection)

            # Order: create nodes → create relations → drop relations → drop
            # nodes. Dropping a node table fails while a relationship table
            # still references it, so relations always go first.
            create_normal: list[_TableAction] = []
            create_relation: list[_TableAction] = []
            remove_relation: list[_TableAction] = []
            remove_normal: list[_TableAction] = []

            for action in db_actions:
                if coco.is_non_existence(action.spec):
                    if action.is_relation:
                        remove_relation.append(action)
                    else:
                        remove_normal.append(action)
                else:
                    if action.is_relation:
                        create_relation.append(action)
                    else:
                        create_normal.append(action)

            for action in (
                create_normal + create_relation + remove_relation + remove_normal
            ):
                spec = action.spec

                if action.main_action in ("replace", "delete"):
                    self._drop_table(conn, action.key, action)

                if coco.is_non_existence(spec):
                    continue

                if action.main_action in ("insert", "upsert", "replace"):
                    self._create_table(
                        conn,
                        action.key,
                        spec,
                        if_not_exists=(action.main_action == "upsert"),
                    )

                # "insert" / "replace" just built the table from the desired
                # schema, so its columns already match. "upsert" may have
                # landed on a pre-existing table carrying an older column set,
                # so reconcile non-PK columns in place.
                if action.main_action in (None, "upsert") and action.column_actions:
                    self._apply_column_actions(
                        conn,
                        action.key,
                        spec.table_schema,
                        action.column_actions,
                    )

    @staticmethod
    def _create_table(
        conn: ManagedConnection,
        key: _TableKey,
        spec: _TableSpec,
        *,
        if_not_exists: bool,
    ) -> None:
        """Create a node or relationship table."""
        columns = [(name, col.type) for name, col in spec.table_schema.columns.items()]
        if spec.is_relation:
            assert spec.from_label is not None and spec.to_label is not None
            cypher = _cypher.build_rel_table_create(
                key.table_name, spec.from_label, spec.to_label, columns
            )
        else:
            cypher = _cypher.build_node_table_create(
                key.table_name, columns, spec.primary_key
            )
        if if_not_exists:
            # LadybugDB supports IF NOT EXISTS on CREATE TABLE.
            cypher = cypher.replace("CREATE ", "CREATE ", 1)
            marker = "TABLE "
            idx = cypher.find(marker) + len(marker)
            cypher = cypher[:idx] + "IF NOT EXISTS " + cypher[idx:]
        conn.execute(cypher)

    @staticmethod
    def _drop_table(
        conn: ManagedConnection, key: _TableKey, action: _TableAction
    ) -> None:
        """Destroy the table: delete its contents, then drop it.

        A system-managed table owns its label / relationship type: every node
        carrying the label (with the relationships attached to it) or every
        relationship of the type is deleted, whoever wrote it. The engine
        prunes the records' tracking without reconciling them once their
        container is gone, so this is where their data goes away. A label
        shared with data CocoIndex does not own belongs in a
        ``managed_by="user"`` table, which never gets here.

        Uses ``prev_pk_field`` / ``prev_is_relation`` recovered during
        reconcile — that's what was actually created.
        """
        pk_field = action.prev_pk_field
        is_relation = action.prev_is_relation
        if pk_field is None and isinstance(action.spec, _TableSpec):
            pk_field = action.spec.primary_key
            is_relation = action.spec.is_relation
        if pk_field is None:
            return  # Nothing to drop.
        if is_relation:
            conn.execute(_cypher.build_relationship_delete_all(key.table_name))
        else:
            conn.execute(_cypher.build_node_delete_all(key.table_name))
        conn.execute(_cypher.build_table_drop(key.table_name))

    @staticmethod
    def _apply_column_actions(
        conn: ManagedConnection,
        key: _TableKey,
        table_schema: TableSchema[Any],
        column_actions: dict[str, statediff.DiffAction],
    ) -> None:
        """Apply additive/destructive column schema changes in place."""
        existing = _table_columns(conn, key.table_name)
        pk_cols = {table_schema.primary_key}

        for sub_key, col_action in column_actions.items():
            if not sub_key.startswith(_FIELD_SUBKEY_PREFIX):
                raise ValueError(
                    f"Unexpected column subkey format: {sub_key!r}, expected to "
                    f"start with {_FIELD_SUBKEY_PREFIX!r}"
                )
            col_name = sub_key[len(_FIELD_SUBKEY_PREFIX) :]

            # Defensive: never ALTER PK columns in place; a PK change rebuilds
            # the table via main_action="replace".
            if col_name in pk_cols:
                continue

            desired_col = table_schema.columns.get(col_name)

            if col_action == "delete":
                if col_name in existing:
                    conn.execute(_cypher.build_drop_column(key.table_name, col_name))
                continue

            if desired_col is None:
                continue

            if col_action in ("insert", "upsert"):
                if col_name not in existing:
                    conn.execute(
                        _cypher.build_add_column(
                            key.table_name, col_name, desired_col.type
                        )
                    )
                continue

            # "replace": the column type changed — drop and re-add it. The
            # column's data is lost but the rows survive.
            if col_name in existing:
                conn.execute(_cypher.build_drop_column(key.table_name, col_name))
            conn.execute(
                _cypher.build_add_column(key.table_name, col_name, desired_col.type)
            )


def _table_columns(conn: ManagedConnection, table_name: str) -> set[str]:
    """Return the current column names of a table via ``table_info``."""
    # table_info() takes a string literal, not a parameter. Interpolating is
    # safe: table names pass _validate_identifier at every API entry point,
    # so they cannot contain quotes or escape the literal.
    _validate_identifier(table_name, "table name")
    with conn._lock:  # noqa: SLF001
        result = conn._conn.execute(  # noqa: SLF001
            f"CALL table_info('{table_name}') RETURN *"
        )
        return {row[0] for row in result.get_all()}


# ---------------------------------------------------------------------------
# Root provider registration
# ---------------------------------------------------------------------------

# Node tables and relationship tables each have a root provider of their own,
# so a label and a relationship type of one name never share a key.
_table_handler = _TableHandler()
_node_provider = coco.register_root_target_states_provider(
    "cocoindex/ladybug/node", _table_handler
)
_relationship_provider = coco.register_root_target_states_provider(
    "cocoindex/ladybug/relationship", _table_handler
)


# ---------------------------------------------------------------------------
# NodeTarget
# ---------------------------------------------------------------------------


class NodeTarget(
    Generic[RowT, coco.MaybePendingS], coco.ResolvesTo["NodeTarget[RowT]"]
):
    """A target for writing nodes to a LadybugDB node table (label)."""

    _provider: coco.TargetStateProvider[_RowValue, None, coco.MaybePendingS]
    _table_schema: TableSchema[RowT]
    _table_name: str
    _primary_key: str

    def __init__(
        self,
        provider: coco.TargetStateProvider[_RowValue, None, coco.MaybePendingS],
        table_schema: TableSchema[RowT],
        table_name: str,
        primary_key: str,
    ) -> None:
        self._provider = provider
        self._table_schema = table_schema
        self._table_name = table_name
        self._primary_key = primary_key

    @property
    def table_name(self) -> str:
        return self._table_name

    @property
    def primary_key(self) -> str:
        return self._primary_key

    def _row_to_dict(self, row: RowT) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for col_name, col in self._table_schema.columns.items():
            if isinstance(row, dict):
                value = row.get(col_name)
            else:
                value = getattr(row, col_name)
            if value is not None and col.encoder is not None:
                value = col.encoder(value)
            out[col_name] = value
        return out

    def declare_record(self: NodeTarget[RowT], *, row: RowT) -> None:
        """Declare a record (node) to be upserted to this table."""
        row_dict = self._row_to_dict(row)
        if self._primary_key not in row_dict:
            raise ValueError(f"row is missing primary key field {self._primary_key!r}")
        pk_values = (row_dict[self._primary_key],)
        coco.declare_target_state(self._provider.target_state(pk_values, row_dict))

    declare_row = declare_record

    def __coco_memo_key__(self) -> str:
        return self._provider.memo_key


# ---------------------------------------------------------------------------
# RelationshipTarget
# ---------------------------------------------------------------------------


def _relationship_state(
    table_schema: TableSchema[Any] | None,
    primary_key: str,
    from_table: NodeTarget[Any],
    to_table: NodeTarget[Any],
    from_id: Any,
    to_id: Any,
    record: Any,
) -> tuple[Any, _RelationRowValue]:
    """The record id and value of a declared relationship. A record without
    the primary key gets an id derived from its endpoints."""
    from_label = from_table.table_name
    to_label = to_table.table_name

    record_id: Any = None
    row_dict: dict[str, Any] = {}
    if record is not None:
        if table_schema is None:
            raise ValueError(
                "Declaring a relationship with a record carrying properties "
                "requires a TableSchema for the relationship table, so the "
                "columns can be created."
            )
        for col_name, col in table_schema.columns.items():
            if col_name == primary_key:
                continue
            if isinstance(record, dict):
                value = record.get(col_name)
            else:
                value = getattr(record, col_name)
            if value is not None and col.encoder is not None:
                value = col.encoder(value)
            row_dict[col_name] = value
        record_id = (
            record.get(primary_key)
            if isinstance(record, dict)
            else getattr(record, primary_key, None)
        )

    if record_id is None:
        record_id = relationship_id(from_label, from_id, to_label, to_id)

    return record_id, _RelationRowValue(
        from_label=from_label,
        from_pk_field=from_table.primary_key,
        from_id=from_id,
        to_label=to_label,
        to_pk_field=to_table.primary_key,
        to_id=to_id,
        fields=row_dict,
    )


class RelationshipTarget(
    Generic[RowT, coco.MaybePendingS], coco.ResolvesTo["RelationshipTarget[RowT]"]
):
    """A target for writing relationships (edges) to a LadybugDB
    relationship table (relationship type)."""

    _provider: coco.TargetStateProvider[_RowValue, None, coco.MaybePendingS]
    _table_schema: TableSchema[RowT] | None
    _primary_key: str
    _from_table: NodeTarget[Any]
    _to_table: NodeTarget[Any]

    def __init__(
        self,
        provider: coco.TargetStateProvider[_RowValue, None, coco.MaybePendingS],
        table_schema: TableSchema[RowT] | None,
        primary_key: str,
        from_table: NodeTarget[Any],
        to_table: NodeTarget[Any],
    ) -> None:
        self._provider = provider
        self._table_schema = table_schema
        self._primary_key = primary_key
        self._from_table = from_table
        self._to_table = to_table

    def declare_relationship(
        self: RelationshipTarget[RowT],
        *,
        from_id: Any,
        to_id: Any,
        record: RowT | None = None,
    ) -> None:
        """Declare a relationship (edge) from the ``from_table`` node keyed
        ``from_id`` to the ``to_table`` node keyed ``to_id``.

        Without a ``record`` carrying the primary key, the relationship's id
        is derived from its endpoints (ids must then be ``int`` or ``str``).
        """
        record_id, row_value = _relationship_state(
            self._table_schema,
            self._primary_key,
            self._from_table,
            self._to_table,
            from_id,
            to_id,
            record,
        )
        coco.declare_target_state(self._provider.target_state((record_id,), row_value))

    def __coco_memo_key__(self) -> str:
        return self._provider.memo_key


# ---------------------------------------------------------------------------
# Module-level entry points
# ---------------------------------------------------------------------------


def _node_table_state(
    provider: coco.TargetStateProvider[_TableSpec, _RecordHandler],
    db: ContextKey[ManagedConnection],
    table_name: str,
    table_schema: TableSchema[Any],
    primary_key: str,
    managed_by: target.ManagedBy,
) -> coco.TargetState[_RecordHandler]:
    _validate_identifier(table_name, "table name")
    _validate_identifier(primary_key, "primary key")
    if table_schema.primary_key != primary_key:
        raise ValueError(
            f"primary_key {primary_key!r} does not match the schema's "
            f"declared primary_key {table_schema.primary_key!r}"
        )
    key = _TableKey(db_key=db.key, table_name=table_name)
    spec = _TableSpec(
        table_schema=table_schema,
        primary_key=primary_key,
        is_relation=False,
        from_label=None,
        from_pk_field=None,
        to_label=None,
        to_pk_field=None,
        managed_by=managed_by,
    )
    return provider.target_state(key, spec)


def _relationship_table_state(
    provider: coco.TargetStateProvider[_TableSpec, _RecordHandler],
    db: ContextKey[ManagedConnection],
    table_name: str,
    from_table: NodeTarget[Any],
    to_table: NodeTarget[Any],
    table_schema: TableSchema[Any] | None,
    primary_key: str,
    managed_by: target.ManagedBy,
) -> coco.TargetState[_RecordHandler]:
    _validate_identifier(table_name, "relation table name")
    _validate_identifier(primary_key, "primary key")
    _validate_identifier(from_table.table_name, "from table name")
    _validate_identifier(to_table.table_name, "to table name")
    if table_schema is not None and table_schema.primary_key != primary_key:
        raise ValueError(
            f"primary_key {primary_key!r} does not match schema's "
            f"{table_schema.primary_key!r}"
        )
    key = _TableKey(db_key=db.key, table_name=table_name)
    spec = _TableSpec(
        table_schema=table_schema
        or TableSchema(
            columns={primary_key: ColumnDef("STRING")}, primary_key=primary_key
        ),
        primary_key=primary_key,
        is_relation=True,
        from_label=from_table.table_name,
        from_pk_field=from_table.primary_key,
        to_label=to_table.table_name,
        to_pk_field=to_table.primary_key,
        managed_by=managed_by,
    )
    return provider.target_state(key, spec)


def node_target(
    db: ContextKey[ManagedConnection],
    table_name: str,
    table_schema: TableSchema[RowT],
    *,
    primary_key: str = "id",
    managed_by: target.ManagedBy = target.ManagedBy.SYSTEM,
) -> coco.TargetState[_RecordHandler]:
    """Create a ``TargetState`` for a LadybugDB node table (label)."""
    return _node_table_state(
        _node_provider, db, table_name, table_schema, primary_key, managed_by
    )


def declare_node_target(
    db: ContextKey[ManagedConnection],
    table_name: str,
    table_schema: TableSchema[RowT],
    *,
    primary_key: str = "id",
    managed_by: target.ManagedBy = target.ManagedBy.SYSTEM,
) -> NodeTarget[RowT, coco.PendingS]:
    """Declare a node table target.

    Use this for tables that exist only as relationship endpoints — no
    records flow into this declaration's own handler.
    """
    provider = coco.declare_target_state_with_child(
        node_target(
            db, table_name, table_schema, primary_key=primary_key, managed_by=managed_by
        )
    )
    return NodeTarget(provider, table_schema, table_name, primary_key)


async def mount_node_target(
    db: ContextKey[ManagedConnection],
    table_name: str,
    table_schema: TableSchema[RowT],
    *,
    primary_key: str = "id",
    managed_by: target.ManagedBy = target.ManagedBy.SYSTEM,
) -> NodeTarget[RowT]:
    """Mount a node table target ready to receive ``declare_record`` calls."""
    provider = await coco.mount_target(
        node_target(
            db, table_name, table_schema, primary_key=primary_key, managed_by=managed_by
        )
    )
    return NodeTarget(provider, table_schema, table_name, primary_key)


def relationship_target(
    db: ContextKey[ManagedConnection],
    table_name: str,
    from_table: NodeTarget[Any],
    to_table: NodeTarget[Any],
    table_schema: TableSchema[RowT] | None = None,
    *,
    primary_key: str = "id",
    managed_by: target.ManagedBy = target.ManagedBy.SYSTEM,
) -> coco.TargetState[_RecordHandler]:
    """Create a ``TargetState`` for a LadybugDB relationship table
    (relationship type)."""
    return _relationship_table_state(
        _relationship_provider,
        db,
        table_name,
        from_table,
        to_table,
        table_schema,
        primary_key,
        managed_by,
    )


def declare_relationship_target(
    db: ContextKey[ManagedConnection],
    table_name: str,
    from_table: NodeTarget[Any],
    to_table: NodeTarget[Any],
    table_schema: TableSchema[RowT] | None = None,
    *,
    primary_key: str = "id",
    managed_by: target.ManagedBy = target.ManagedBy.SYSTEM,
) -> RelationshipTarget[RowT, coco.PendingS]:
    """Declare a relationship table target."""
    provider = coco.declare_target_state_with_child(
        relationship_target(
            db,
            table_name,
            from_table,
            to_table,
            table_schema,
            primary_key=primary_key,
            managed_by=managed_by,
        )
    )
    return RelationshipTarget(provider, table_schema, primary_key, from_table, to_table)


async def mount_relationship_target(
    db: ContextKey[ManagedConnection],
    table_name: str,
    from_table: NodeTarget[Any],
    to_table: NodeTarget[Any],
    table_schema: TableSchema[RowT] | None = None,
    *,
    primary_key: str = "id",
    managed_by: target.ManagedBy = target.ManagedBy.SYSTEM,
) -> RelationshipTarget[RowT]:
    """Mount a relationship table target ready to receive
    ``declare_relationship`` calls."""
    provider = await coco.mount_target(
        relationship_target(
            db,
            table_name,
            from_table,
            to_table,
            table_schema,
            primary_key=primary_key,
            managed_by=managed_by,
        )
    )
    return RelationshipTarget(provider, table_schema, primary_key, from_table, to_table)


# ---------------------------------------------------------------------------
# Public exports
# ---------------------------------------------------------------------------

__all__ = [
    "ColumnDef",
    "LadybugType",
    "ManagedConnection",
    "NodeTarget",
    "RelationshipTarget",
    "TableSchema",
    "ValueEncoder",
    "connect",
    "declare_node_target",
    "declare_relationship_target",
    "managed_connection",
    "mount_node_target",
    "mount_relationship_target",
    "node_target",
    "relationship_target",
]
