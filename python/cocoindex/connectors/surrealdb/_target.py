"""
SurrealDB target for CocoIndex.

This module provides a two-level target state system for SurrealDB:
1. Table level: Creates/drops tables in the database (DEFINE TABLE / REMOVE TABLE)
2. Record level: Upserts/deletes records within tables (UPSERT / DELETE / RELATE)

Supports both normal tables and relation (graph edge) tables, with optional
schema enforcement (SCHEMAFULL/SCHEMALESS) and vector index support.
"""

from __future__ import annotations

import datetime
import decimal
import json
import re
import uuid
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Collection,
    Generic,
    Literal,
    NamedTuple,
    Sequence,
)

from typing_extensions import TypeVar

try:
    import surrealdb as _surrealdb  # type: ignore[import-untyped]
except ImportError as e:
    raise ImportError(
        "surrealdb is required to use the SurrealDB connector. "
        "Please install cocoindex[surrealdb]."
    ) from e

if TYPE_CHECKING:
    # surrealdb is untyped; use Any so mypy doesn't complain about attribute access.
    AsyncSurreal = Any
else:
    AsyncSurreal = _surrealdb.AsyncSurreal

import numpy as np

import cocoindex as coco
from cocoindex.connectorkits import statediff
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
from cocoindex._internal.serde import unpickle_safe
from cocoindex._internal.context_keys import ContextKey, ContextProvider

# ---------------------------------------------------------------------------
# Identifier validation & record ID formatting
# ---------------------------------------------------------------------------

_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _validate_identifier(name: str, kind: str) -> None:
    """Validate that *name* is a safe SurrealQL identifier.

    Raises :class:`ValueError` if the name contains characters that are not
    alphanumeric or underscore, or starts with a digit.
    """
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(
            f"Invalid SurrealDB {kind}: {name!r}. Must match [a-zA-Z_][a-zA-Z0-9_]*."
        )


def _format_record_id(value: Any) -> str:
    """Format a record ID for inline use in SurrealQL, preserving type.

    * ``int`` / ``float`` → bare numeric literal (``123``, ``3.14``)
    * ``str`` (and everything else) → backtick-quoted with ``\\`` and
      backtick escaping (`` `alice` ``, `` `has\\`tick` ``)
    """
    if isinstance(value, (int, float)):
        return str(value)
    s = str(value)
    s = s.replace("\\", "\\\\").replace("`", "\\`")
    return f"`{s}`"


# ---------------------------------------------------------------------------
# Connection factory
# ---------------------------------------------------------------------------


class ConnectionFactory:
    """
    Connection factory for SurrealDB.

    Holds connection parameters and creates authenticated connections on demand.

    Example::

        factory = surrealdb.ConnectionFactory(
            url="ws://localhost:8000/rpc",
            namespace="test",
            database="test",
            credentials={"username": "root", "password": "root"},
        )
        builder.provide(SURREAL_DB, factory)
    """

    def __init__(
        self,
        url: str,
        *,
        namespace: str,
        database: str,
        credentials: dict[str, str] | None = None,
    ) -> None:
        self._url = url
        self._namespace = namespace
        self._database = database
        self._credentials = credentials

    async def acquire(self) -> AsyncSurreal:
        """Create a new authenticated connection on the current event loop."""
        conn = AsyncSurreal(self._url)
        await conn.connect()  # type: ignore[call-arg]
        if self._credentials:
            await conn.signin(self._credentials)  # type: ignore[arg-type]
        await conn.use(self._namespace, self._database)
        return conn


# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------

_RowKey = tuple[Any, ...]  # Primary key values as tuple (always (id,))
_ROW_KEY_CHECKER = TypeChecker(tuple[Any, ...])
_RowFingerprint = bytes


class _RelationRowValue(NamedTuple):
    """Value type for relation records, carrying endpoint metadata separately from field data."""

    from_record: str  # e.g. "person:`alice`"
    to_record: str  # e.g. "post:`p1`"
    fields: dict[str, Any]


_RowValue = dict[str, Any] | _RelationRowValue
ValueEncoder = Callable[[Any], Any]


# ---------------------------------------------------------------------------
# SurrealType annotation
# ---------------------------------------------------------------------------


class SurrealType(NamedTuple):
    """
    Annotation to specify a SurrealDB field type.

    Use with ``typing.Annotated`` to override the default type mapping::

        from typing import Annotated
        from dataclasses import dataclass
        from cocoindex.connectors.surrealdb import SurrealType

        @dataclass
        class MyRow:
            id: str
            value: Annotated[float, SurrealType("decimal")]
    """

    surreal_type: str
    encoder: ValueEncoder | None = None


# ---------------------------------------------------------------------------
# Value encoders
# ---------------------------------------------------------------------------


def _json_encoder(value: Any) -> str:
    """Encode a value to JSON string for SurrealDB."""
    return json.dumps(value, default=str)


def _ndarray_encoder(value: Any) -> list[Any]:
    """Convert a NumPy ndarray to a Python list for SurrealDB."""
    if isinstance(value, list):
        return value
    return value.tolist()  # type: ignore[no-any-return]


# ---------------------------------------------------------------------------
# Type mapping
# ---------------------------------------------------------------------------


class _TypeMapping(NamedTuple):
    """Mapping from Python type to SurrealDB type with optional encoder."""

    surreal_type: str
    encoder: ValueEncoder | None = None


# Global mapping for leaf types
_LEAF_TYPE_MAPPINGS: dict[type, _TypeMapping] = {
    # Boolean
    bool: _TypeMapping("bool"),
    # Numeric types
    int: _TypeMapping("int"),
    float: _TypeMapping("float"),
    decimal.Decimal: _TypeMapping("decimal"),
    # NumPy scalar integer types
    np.int8: _TypeMapping("int"),
    np.int16: _TypeMapping("int"),
    np.int32: _TypeMapping("int"),
    np.int64: _TypeMapping("int"),
    np.uint8: _TypeMapping("int"),
    np.uint16: _TypeMapping("int"),
    np.uint32: _TypeMapping("int"),
    np.uint64: _TypeMapping("int"),
    np.int_: _TypeMapping("int"),
    np.uint: _TypeMapping("int"),
    # NumPy scalar float types
    np.float16: _TypeMapping("float"),
    np.float32: _TypeMapping("float"),
    np.float64: _TypeMapping("float"),
    # String types
    str: _TypeMapping("string"),
    bytes: _TypeMapping("bytes"),
    # UUID
    uuid.UUID: _TypeMapping("uuid"),
    # Date/time types
    datetime.date: _TypeMapping("datetime"),
    datetime.time: _TypeMapping("datetime"),
    datetime.datetime: _TypeMapping("datetime"),
    datetime.timedelta: _TypeMapping("duration"),
}

# Default mapping for complex types that need JSON encoding
_OBJECT_MAPPING = _TypeMapping("object", _json_encoder)


async def _get_type_mapping(
    python_type: Any, *, vector_schema: res_schema.VectorSchema | None = None
) -> _TypeMapping:
    """
    Get the SurrealDB type mapping for a Python type.

    For types that map to multiple SurrealDB types, uses the broader one.
    Use ``SurrealType`` annotation with ``typing.Annotated`` to override.
    """
    type_info = analyze_type_info(python_type)

    # Check for SurrealType annotation override
    for annotation in type_info.annotations:
        if isinstance(annotation, SurrealType):
            return _TypeMapping(annotation.surreal_type, annotation.encoder)

    base_type = type_info.base_type

    # Check direct leaf type mappings
    if base_type in _LEAF_TYPE_MAPPINGS:
        return _LEAF_TYPE_MAPPINGS[base_type]

    # NumPy ndarray: map to array<float, N>
    if base_type is np.ndarray:
        if vector_schema is None:
            raise ValueError("VectorSchemaProvider is required for NumPy ndarray type.")
        if vector_schema.size <= 0:
            raise ValueError(f"Invalid vector dimension: {vector_schema.size}")

        return _TypeMapping(
            surreal_type=f"array<float, {vector_schema.size}>",
            encoder=_ndarray_encoder,
        )

    elif vector_schema is not None:
        raise ValueError(
            f"VectorSchemaProvider is only supported for NumPy ndarray type. "
            f"Got type: {python_type}"
        )

    # Complex types that need JSON encoding
    if isinstance(
        type_info.variant, (SequenceType, MappingType, RecordType, UnionType, AnyType)
    ):
        return _OBJECT_MAPPING

    # Default fallback
    return _OBJECT_MAPPING


# ---------------------------------------------------------------------------
# ColumnDef
# ---------------------------------------------------------------------------


class ColumnDef(NamedTuple):
    """Definition of a table field."""

    type: str  # SurrealDB type (e.g., "string", "int", "object", "array<f32, 384>")
    nullable: bool = True
    encoder: ValueEncoder | None = (
        None  # Optional encoder to convert value before sending to SurrealDB
    )


# ---------------------------------------------------------------------------
# TableSchema
# ---------------------------------------------------------------------------

# Type variable for row type
RowT = TypeVar("RowT", default=dict[str, Any])


@dataclass(slots=True)
class TableSchema(Generic[RowT]):
    """Schema definition for a SurrealDB table."""

    columns: dict[str, ColumnDef]  # field name -> definition
    row_type: type[RowT] | None  # The row type, if provided

    def __init__(
        self,
        columns: dict[str, ColumnDef],
        *,
        row_type: type[RowT] | None = None,
    ) -> None:
        """
        Create a TableSchema from pre-resolved column definitions.

        For constructing from a record type, use the async classmethod
        ``from_class`` instead.

        Args:
            columns: A dict mapping field names to ColumnDef.
            row_type: Optional original record type.
        """
        for col_name in columns:
            _validate_identifier(col_name, "column name")
        self.columns = columns
        self.row_type = row_type

    @classmethod
    async def from_class(
        cls,
        record_type: type[RowT],
        *,
        column_overrides: dict[str, SurrealType | res_schema.VectorSchemaProvider]
        | None = None,
    ) -> "TableSchema[RowT]":
        """
        Create a TableSchema from a record type (dataclass, NamedTuple, or Pydantic model).

        Python types are automatically mapped to SurrealDB types.

        Args:
            record_type: A record type (dataclass, NamedTuple, or Pydantic model).
            column_overrides: Optional dict mapping field names to SurrealType or
                              VectorSchemaProvider to override the default type mapping.
        """
        if not is_record_type(record_type):
            raise TypeError(
                f"record_type must be a record type (dataclass, NamedTuple, Pydantic model), "
                f"got {type(record_type)}"
            )
        columns = await cls._columns_from_record_type(record_type, column_overrides)
        return cls(columns, row_type=record_type)

    @staticmethod
    async def _columns_from_record_type(
        record_type: type,
        column_overrides: dict[str, SurrealType | res_schema.VectorSchemaProvider]
        | None,
    ) -> dict[str, ColumnDef]:
        """Convert a record type to a dict of field name -> ColumnDef."""
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

            # Extract SurrealType and VectorSchemaProvider from annotations
            surreal_type_annotation = next(
                (t for t in all_annotations if isinstance(t, SurrealType)), None
            )
            vector_schema = await anext(
                (
                    s
                    for annot in all_annotations
                    if (s := await res_schema.get_vector_schema(annot)) is not None
                ),
                None,
            )

            # Determine type mapping
            if surreal_type_annotation is not None:
                type_mapping = _TypeMapping(
                    surreal_type_annotation.surreal_type,
                    surreal_type_annotation.encoder,
                )
            else:
                type_mapping = await _get_type_mapping(
                    field.type_hint, vector_schema=vector_schema
                )

            columns[field.name] = ColumnDef(
                type=type_mapping.surreal_type.strip(),
                nullable=type_info.nullable,
                encoder=type_mapping.encoder,
            )

        return columns


# ---------------------------------------------------------------------------
# _RecordAction + _SharedRecordApplier
# ---------------------------------------------------------------------------


class _RecordAction(NamedTuple):
    """Action to perform on a record (upsert or delete)."""

    table_name: str
    is_relation: bool
    record_id: Any
    value: dict[str, Any] | None  # None = delete
    from_record: str | None  # e.g. "person:alice" (relations only)
    to_record: str | None  # e.g. "post:123" (relations only)


class _SharedRecordApplier:
    """Owns a TargetActionSink shared by all record handlers for one database."""

    _conn: AsyncSurreal
    sink: coco.TargetActionSink[_RecordAction, None]

    def __init__(self, conn: AsyncSurreal) -> None:
        self._conn = conn
        self.sink = coco.TargetActionSink.from_async_fn(self._apply_actions)

    async def _apply_actions(
        self, context_provider: ContextProvider, actions: Sequence[_RecordAction]
    ) -> None:
        if not actions:
            return

        # Sort actions into 4 ordered buckets
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

        await self._conn.query("BEGIN TRANSACTION")
        try:
            for action in upsert_normal:
                assert action.value is not None
                content = {k: v for k, v in action.value.items() if k != "id"}
                surql = f"UPSERT {action.table_name}:{_format_record_id(action.record_id)} CONTENT $content"
                await self._conn.query(surql, {"content": content})

            for action in upsert_relation:
                assert action.value is not None
                assert action.from_record is not None
                assert action.to_record is not None
                # Delete before RELATE: SurrealDB relation records bind
                # in/out as part of identity, so changing endpoints requires
                # removing the old record first.
                await self._conn.query(
                    f"DELETE {action.table_name}:{_format_record_id(action.record_id)}"
                )
                content = {k: v for k, v in action.value.items() if k != "id"}
                surql = (
                    f"RELATE {action.from_record}"
                    f"->{action.table_name}:{_format_record_id(action.record_id)}"
                    f"->{action.to_record}"
                    f" CONTENT $content"
                )
                await self._conn.query(surql, {"content": content})

            for action in delete_relation:
                await self._conn.query(
                    f"DELETE {action.table_name}:{_format_record_id(action.record_id)}"
                )

            for action in delete_normal:
                await self._conn.query(
                    f"DELETE {action.table_name}:{_format_record_id(action.record_id)}"
                )

            await self._conn.query("COMMIT TRANSACTION")
        except BaseException:
            await self._conn.query("CANCEL TRANSACTION")
            raise


# ---------------------------------------------------------------------------
# Vector index types and handler
# ---------------------------------------------------------------------------


class _VectorIndexSpec(NamedTuple):
    """Specification for a vector index on a SurrealDB table."""

    field: str
    metric: str  # "cosine", "euclidean", "manhattan"
    method: str  # "mtree", "hnsw"
    dimension: int
    vector_type: str  # "f32", "f64", "i16", "i32", "i64"


_VectorIndexFingerprint = bytes


class _VectorIndexAction(NamedTuple):
    """Action to create or remove a vector index."""

    name: str
    table_name: str
    spec: _VectorIndexSpec | None  # None = delete


class _VectorIndexHandler:
    """Attachment handler for vector indexes on a SurrealDB table."""

    _conn: AsyncSurreal
    _table_name: str
    _sink: coco.TargetActionSink[_VectorIndexAction, None]

    def __init__(self, conn: AsyncSurreal, table_name: str) -> None:
        self._conn = conn
        self._table_name = table_name
        self._sink = coco.TargetActionSink.from_async_fn(self._apply_actions)

    async def _apply_actions(
        self, context_provider: ContextProvider, actions: Sequence[_VectorIndexAction]
    ) -> None:
        for action in actions:
            if action.spec is None:
                await self._conn.query(
                    f"REMOVE INDEX {action.name} ON TABLE {action.table_name}"
                )
            else:
                # Drop and recreate
                await self._conn.query(
                    f"REMOVE INDEX IF EXISTS {action.name} ON TABLE {action.table_name}"
                )
                method = action.spec.method.upper()
                dist = action.spec.metric.upper()
                surql = (
                    f"DEFINE INDEX {action.name} ON {action.table_name} "
                    f"FIELDS {action.spec.field} "
                    f"{method} DIMENSION {action.spec.dimension} "
                    f"DIST {dist} TYPE {action.spec.vector_type.upper()}"
                )
                await self._conn.query(surql)

    def reconcile(
        self,
        key: coco.StableKey,
        desired_state: _VectorIndexSpec | coco.NonExistenceType,
        prev_possible_states: Collection[_VectorIndexFingerprint],
        prev_may_be_missing: bool,
        /,
    ) -> (
        coco.TargetReconcileOutput[_VectorIndexAction, _VectorIndexFingerprint, None]
        | None
    ):
        assert isinstance(key, str)
        if coco.is_non_existence(desired_state):
            if not prev_possible_states and not prev_may_be_missing:
                return None
            return coco.TargetReconcileOutput(
                action=_VectorIndexAction(
                    name=key, table_name=self._table_name, spec=None
                ),
                sink=self._sink,
                tracking_record=coco.NON_EXISTENCE,
            )

        target_fp = fingerprint_object(desired_state)
        if not prev_may_be_missing and all(
            prev == target_fp for prev in prev_possible_states
        ):
            return None

        return coco.TargetReconcileOutput(
            action=_VectorIndexAction(
                name=key, table_name=self._table_name, spec=desired_state
            ),
            sink=self._sink,
            tracking_record=target_fp,
        )


# ---------------------------------------------------------------------------
# _RecordHandler
# ---------------------------------------------------------------------------


class _RecordHandler(coco.TargetHandler[_RowValue, _RowFingerprint]):
    """Handler for record-level target states within a SurrealDB table."""

    _table_name: str
    _is_relation: bool
    _table_schema: TableSchema[Any] | None
    _conn: AsyncSurreal
    _sink: coco.TargetActionSink[_RecordAction, None]

    def __init__(
        self,
        table_name: str,
        is_relation: bool,
        table_schema: TableSchema[Any] | None,
        conn: AsyncSurreal,
        sink: coco.TargetActionSink[_RecordAction, None],
    ) -> None:
        self._table_name = table_name
        self._is_relation = is_relation
        self._table_schema = table_schema
        self._conn = conn
        self._sink = sink

    def attachment(self, att_type: str) -> _VectorIndexHandler | None:
        if att_type == "vector_index":
            return _VectorIndexHandler(self._conn, self._table_name)
        return None

    def _encode_row(self, row_dict: dict[str, Any]) -> dict[str, Any]:
        """Apply column encoders from schema if present."""
        if self._table_schema is None:
            return row_dict
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
        prev_possible_states: Collection[_RowFingerprint],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[_RecordAction, _RowFingerprint, None] | None:
        key = _ROW_KEY_CHECKER.check(key)

        if coco.is_non_existence(desired_state):
            if not prev_possible_states and not prev_may_be_missing:
                return None
            return coco.TargetReconcileOutput(
                action=_RecordAction(
                    table_name=self._table_name,
                    is_relation=self._is_relation,
                    record_id=key[0],
                    value=None,
                    from_record=None,
                    to_record=None,
                ),
                sink=self._sink,
                tracking_record=coco.NON_EXISTENCE,
            )

        target_fp = fingerprint_object(desired_state)
        if not prev_may_be_missing and all(
            prev == target_fp for prev in prev_possible_states
        ):
            return None

        # Extract relation metadata and field data
        if isinstance(desired_state, _RelationRowValue):
            from_record = desired_state.from_record
            to_record = desired_state.to_record
            encoded = self._encode_row(desired_state.fields)
        else:
            from_record = None
            to_record = None
            encoded = self._encode_row(desired_state)

        return coco.TargetReconcileOutput(
            action=_RecordAction(
                table_name=self._table_name,
                is_relation=self._is_relation,
                record_id=key[0],
                value=encoded,
                from_record=from_record,
                to_record=to_record,
            ),
            sink=self._sink,
            tracking_record=target_fp,
        )


# ---------------------------------------------------------------------------
# Table-level types
# ---------------------------------------------------------------------------


class _TableKey(NamedTuple):
    """Key for identifying a table within a database."""

    db_key: str
    table_name: str


_TABLE_KEY_CHECKER = TypeChecker(tuple[str, str])


@dataclass
class _TableSpec:
    """Specification for a SurrealDB table."""

    table_schema: TableSchema[Any] | None  # None = SCHEMALESS
    is_relation: bool
    from_tables: tuple[str, ...] | None  # sorted table names for FROM clause
    to_tables: tuple[str, ...] | None  # sorted table names for TO clause
    managed_by: Literal["system", "user"] = "system"


@unpickle_safe
@dataclass(frozen=True, slots=True)
class _TableMainRecord:
    """Main tracking record for table-level properties requiring DROP+CREATE if changed."""

    has_schema: bool
    is_relation: bool
    id_type: str | None  # SurrealDB type of the id field, or None for schemaless
    from_tables: tuple[str, ...] | None
    to_tables: tuple[str, ...] | None


@unpickle_safe
@dataclass(frozen=True, slots=True)
class _FieldTrackingRecord:
    """Per-field tracking record for incremental DEFINE FIELD / REMOVE FIELD."""

    surreal_type: str
    nullable: bool


_FIELD_SUBKEY_PREFIX: str = "field:"


def _field_subkey(name: str) -> str:
    return f"{_FIELD_SUBKEY_PREFIX}{name}"


class _TableAction(NamedTuple):
    """Action to perform on a table (DDL)."""

    key: _TableKey
    spec: _TableSpec | coco.NonExistenceType
    is_relation: bool
    main_action: statediff.DiffAction | None
    column_actions: dict[str, statediff.DiffAction]


def _table_composite_tracking_record_from_spec(
    spec: _TableSpec,
) -> statediff.CompositeTrackingRecord[_TableMainRecord, str, _FieldTrackingRecord]:
    """Build composite tracking record from table spec."""
    schema = spec.table_schema
    has_schema = schema is not None
    id_type: str | None = None
    sub: dict[str, _FieldTrackingRecord] = {}

    if schema is not None:
        id_col = schema.columns.get("id")
        if id_col is not None:
            id_type = id_col.type
        for col_name, col_def in schema.columns.items():
            if col_name != "id":
                sub[_field_subkey(col_name)] = _FieldTrackingRecord(
                    surreal_type=col_def.type,
                    nullable=col_def.nullable,
                )

    main = _TableMainRecord(
        has_schema=has_schema,
        is_relation=spec.is_relation,
        id_type=id_type,
        from_tables=spec.from_tables,
        to_tables=spec.to_tables,
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
    """Handler for table-level target states (DDL)."""

    _sink: coco.TargetActionSink[_TableAction, _RecordHandler]

    def __init__(self) -> None:
        self._sink = coco.TargetActionSink.from_async_fn(self._apply_actions)

    def reconcile(
        self,
        key: coco.StableKey,
        desired_state: _TableSpec | coco.NonExistenceType,
        prev_possible_states: Collection[_TableTrackingRecord],
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
                tracking_record, prev_possible_states, prev_may_be_missing
            )
        )
        main_action, column_transitions = statediff.diff_composite(resolved)

        column_actions: dict[str, statediff.DiffAction] = {}
        if main_action is None:
            for sub_key, t in column_transitions.items():
                action = statediff.diff(t)
                if action is not None:
                    column_actions[sub_key] = action

        if main_action is None and not column_actions:
            if coco.is_non_existence(desired_state):
                return None
            # Still need to return output for child handler creation
            # even if no DDL changes needed
        elif main_action is None and not column_actions:
            return None

        child_invalidation: Literal["destructive", "lossy"] | None = None
        if main_action == "replace":
            child_invalidation = "destructive"
        elif main_action is None and any(
            a != "insert" for a in column_actions.values()
        ):
            child_invalidation = "lossy"

        return coco.TargetReconcileOutput(
            action=_TableAction(
                key=key,
                spec=desired_state,
                is_relation=is_relation,
                main_action=main_action,
                column_actions=column_actions,
            ),
            sink=self._sink,
            tracking_record=tracking_record,
            child_invalidation=child_invalidation,
        )

    async def _apply_actions(
        self, context_provider: ContextProvider, actions: Sequence[_TableAction]
    ) -> list[coco.ChildTargetDef[_RecordHandler] | None]:
        actions_list = list(actions)
        outputs: list[coco.ChildTargetDef[_RecordHandler] | None] = [None] * len(
            actions_list
        )

        # Group by db_key
        by_db: dict[str, list[int]] = {}
        for i, action in enumerate(actions_list):
            by_db.setdefault(action.key.db_key, []).append(i)

        for db_key, idxs in by_db.items():
            factory: ConnectionFactory = context_provider.get(db_key)  # type: ignore[assignment]
            conn = await factory.acquire()
            shared_applier = _SharedRecordApplier(conn)

            # Sort into DDL ordering
            create_normal: list[int] = []
            create_relation: list[int] = []
            remove_relation: list[int] = []
            remove_normal: list[int] = []

            for i in idxs:
                action = actions_list[i]
                if coco.is_non_existence(action.spec):
                    if action.is_relation:
                        remove_relation.append(i)
                    else:
                        remove_normal.append(i)
                else:
                    if action.is_relation:
                        create_relation.append(i)
                    else:
                        create_normal.append(i)

            ordered = create_normal + create_relation + remove_relation + remove_normal

            for i in ordered:
                action = actions_list[i]

                if action.main_action in ("replace", "delete"):
                    await conn.query(f"REMOVE TABLE IF EXISTS {action.key.table_name}")

                if coco.is_non_existence(action.spec):
                    outputs[i] = None
                    continue

                spec = action.spec

                if action.main_action in ("insert", "upsert", "replace"):
                    await self._create_table(conn, action.key, spec)
                elif action.main_action is None and action.column_actions:
                    await self._apply_column_actions(
                        conn, action.key, spec, action.column_actions
                    )

                outputs[i] = coco.ChildTargetDef(
                    handler=_RecordHandler(
                        table_name=action.key.table_name,
                        is_relation=spec.is_relation,
                        table_schema=spec.table_schema,
                        conn=conn,
                        sink=shared_applier.sink,
                    )
                )

        return outputs

    @staticmethod
    async def _create_table(
        conn: AsyncSurreal, key: _TableKey, spec: _TableSpec
    ) -> None:
        """Create a table with DEFINE TABLE + DEFINE FIELD statements."""
        schema = spec.table_schema
        schema_mode = "SCHEMAFULL" if schema is not None else "SCHEMALESS"

        if spec.is_relation:
            from_clause = "|".join(spec.from_tables) if spec.from_tables else ""
            to_clause = "|".join(spec.to_tables) if spec.to_tables else ""
            surql = (
                f"DEFINE TABLE {key.table_name} TYPE RELATION "
                f"FROM {from_clause} TO {to_clause} {schema_mode}"
            )
        else:
            surql = f"DEFINE TABLE {key.table_name} {schema_mode}"

        await conn.query(surql)

        if schema is not None:
            for col_name, col_def in schema.columns.items():
                if col_name == "id":
                    continue
                type_expr = col_def.type
                if col_def.nullable:
                    type_expr = f"option<{type_expr}>"
                await conn.query(
                    f"DEFINE FIELD {col_name} ON {key.table_name} TYPE {type_expr}"
                )

    @staticmethod
    async def _apply_column_actions(
        conn: AsyncSurreal,
        key: _TableKey,
        spec: _TableSpec,
        column_actions: dict[str, statediff.DiffAction],
    ) -> None:
        """Apply incremental column changes (DEFINE FIELD / REMOVE FIELD)."""
        schema = spec.table_schema
        if schema is None:
            return

        for sub_key, action in column_actions.items():
            if not sub_key.startswith(_FIELD_SUBKEY_PREFIX):
                continue
            col_name = sub_key[len(_FIELD_SUBKEY_PREFIX) :]
            if col_name == "id":
                continue

            if action == "delete":
                await conn.query(f"REMOVE FIELD {col_name} ON {key.table_name}")
                continue

            col_def = schema.columns.get(col_name)
            if col_def is None:
                continue

            type_expr = col_def.type
            if col_def.nullable:
                type_expr = f"option<{type_expr}>"

            if action in ("insert", "upsert", "replace"):
                await conn.query(
                    f"DEFINE FIELD {col_name} ON {key.table_name} TYPE {type_expr}"
                )


# ---------------------------------------------------------------------------
# Root provider registration
# ---------------------------------------------------------------------------

_table_provider = coco.register_root_target_states_provider(
    "cocoindex/surrealdb/table", _TableHandler()
)


# ---------------------------------------------------------------------------
# TableTarget
# ---------------------------------------------------------------------------


class TableTarget(
    Generic[RowT, coco.MaybePendingS], coco.ResolvesTo["TableTarget[RowT]"]
):
    """A target for writing records to a SurrealDB table."""

    _provider: coco.TargetStateProvider[_RowValue, None, coco.MaybePendingS]
    _table_schema: TableSchema[RowT] | None
    _table_name: str

    def __init__(
        self,
        provider: coco.TargetStateProvider[_RowValue, None, coco.MaybePendingS],
        table_schema: TableSchema[RowT] | None,
        table_name: str,
    ) -> None:
        self._provider = provider
        self._table_schema = table_schema
        self._table_name = table_name

    @property
    def table_name(self) -> str:
        return self._table_name

    def declare_record(self: TableTarget[RowT], *, row: RowT) -> None:
        """Declare a record to be upserted to this table."""
        row_dict = self._row_to_dict(row)
        pk_values = (row_dict["id"],)
        coco.declare_target_state(self._provider.target_state(pk_values, row_dict))

    declare_row = declare_record

    def _row_to_dict(self, row: RowT) -> dict[str, Any]:
        """Convert a row (dict or struct) to dict, applying encoders if schema exists."""
        if self._table_schema is not None:
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
        else:
            if isinstance(row, dict):
                return dict(row)
            record_info = RecordType(type(row))
            return {f.name: getattr(row, f.name) for f in record_info.fields}

    def declare_vector_index(
        self: TableTarget[RowT],
        *,
        name: str | None = None,
        field: str,
        metric: Literal["cosine", "euclidean", "manhattan"] = "cosine",
        method: Literal["mtree", "hnsw"] = "mtree",
        dimension: int | None = None,
        vector_type: Literal["f32", "f64", "i16", "i32", "i64"] = "f32",
    ) -> None:
        """Declare a vector index on this table."""
        _validate_identifier(field, "vector index field")
        if name is None:
            name = f"idx_{self._table_name}__{field}"
        _validate_identifier(name, "vector index name")
        if dimension is None:
            raise ValueError("dimension is required for declare_vector_index()")
        spec = _VectorIndexSpec(
            field=field,
            metric=metric,
            method=method,
            dimension=dimension,
            vector_type=vector_type,
        )
        att_provider = self._provider.attachment("vector_index")
        coco.declare_target_state(att_provider.target_state(name, spec))

    def __coco_memo_key__(self) -> str:
        return self._provider.memo_key


# ---------------------------------------------------------------------------
# RelationTarget
# ---------------------------------------------------------------------------


class RelationTarget(
    Generic[RowT, coco.MaybePendingS], coco.ResolvesTo["RelationTarget[RowT]"]
):
    """A target for writing relation records to a SurrealDB relation table."""

    _provider: coco.TargetStateProvider[_RowValue, None, coco.MaybePendingS]
    _table_name: str
    _table_schema: TableSchema[RowT] | None
    _from_table_names: list[str]
    _to_table_names: list[str]
    _default_from_table: str | None
    _default_to_table: str | None

    def __init__(
        self,
        provider: coco.TargetStateProvider[_RowValue, None, coco.MaybePendingS],
        table_name: str,
        table_schema: TableSchema[RowT] | None,
        from_table_names: list[str],
        to_table_names: list[str],
    ) -> None:
        self._provider = provider
        self._table_name = table_name
        self._table_schema = table_schema
        self._from_table_names = from_table_names
        self._to_table_names = to_table_names
        self._default_from_table = (
            from_table_names[0] if len(from_table_names) == 1 else None
        )
        self._default_to_table = to_table_names[0] if len(to_table_names) == 1 else None

    def declare_relation(
        self: RelationTarget[RowT],
        *,
        from_id: Any,
        to_id: Any,
        record: RowT | None = None,
        from_table: TableTarget[Any] | None = None,
        to_table: TableTarget[Any] | None = None,
    ) -> None:
        """Declare a relation record."""
        # Resolve from_table_name
        if from_table is not None:
            from_table_name = from_table.table_name
        elif self._default_from_table is not None:
            from_table_name = self._default_from_table
        else:
            raise ValueError(
                "from_table must be specified for polymorphic relations "
                f"(possible tables: {self._from_table_names})"
            )

        # Resolve to_table_name
        if to_table is not None:
            to_table_name = to_table.table_name
        elif self._default_to_table is not None:
            to_table_name = self._default_to_table
        else:
            raise ValueError(
                "to_table must be specified for polymorphic relations "
                f"(possible tables: {self._to_table_names})"
            )

        # Build the value dict from the record (exclude 'id' — it's the key, not content)
        if record is not None:
            if self._table_schema is not None:
                row_dict: dict[str, Any] = {}
                for col_name, col in self._table_schema.columns.items():
                    if col_name == "id":
                        continue
                    if isinstance(record, dict):
                        value = record.get(col_name)
                    else:
                        value = getattr(record, col_name)
                    if value is not None and col.encoder is not None:
                        value = col.encoder(value)
                    row_dict[col_name] = value
                record_id = (
                    record.get("id")
                    if isinstance(record, dict)
                    else getattr(record, "id", None)
                )
            elif isinstance(record, dict):
                row_dict = {k: v for k, v in record.items() if k != "id"}
                record_id = record.get("id")
            else:
                record_info = RecordType(type(record))
                row_dict = {
                    f.name: getattr(record, f.name)
                    for f in record_info.fields
                    if f.name != "id"
                }
                record_id = getattr(record, "id", None)
        else:
            row_dict = {}
            record_id = None

        # Auto-derive id from endpoints when not explicitly provided
        if record_id is None:
            record_id = f"{from_table_name}_{from_id}_{to_table_name}_{to_id}"

        # Wrap in _RelationRowValue
        row_value: _RowValue = _RelationRowValue(
            from_record=f"{from_table_name}:{_format_record_id(from_id)}",
            to_record=f"{to_table_name}:{_format_record_id(to_id)}",
            fields=row_dict,
        )

        pk_values = (record_id,)
        coco.declare_target_state(self._provider.target_state(pk_values, row_value))

    def __coco_memo_key__(self) -> str:
        return self._provider.memo_key


# ---------------------------------------------------------------------------
# Module-level target functions
# ---------------------------------------------------------------------------


def _normalize_table_names(
    tables: TableTarget[Any] | Collection[TableTarget[Any]],
) -> list[str]:
    if isinstance(tables, TableTarget):
        return [tables.table_name]
    return [t.table_name for t in tables]


def table_target(
    db: ContextKey[ConnectionFactory],
    table_name: str,
    table_schema: TableSchema[RowT] | None = None,
    *,
    managed_by: Literal["system", "user"] = "system",
) -> coco.TargetState[_RecordHandler]:
    """Create a TargetState for a SurrealDB table."""
    _validate_identifier(table_name, "table name")
    key = _TableKey(db_key=db.key, table_name=table_name)
    spec = _TableSpec(
        table_schema=table_schema,
        is_relation=False,
        from_tables=None,
        to_tables=None,
        managed_by=managed_by,
    )
    return _table_provider.target_state(key, spec)


def declare_table_target(
    db: ContextKey[ConnectionFactory],
    table_name: str,
    table_schema: TableSchema[RowT] | None = None,
    *,
    managed_by: Literal["system", "user"] = "system",
) -> TableTarget[RowT, coco.PendingS]:
    """Declare a table target and return a pending TableTarget."""
    provider = coco.declare_target_state_with_child(
        table_target(db, table_name, table_schema, managed_by=managed_by)
    )
    return TableTarget(provider, table_schema, table_name)


async def mount_table_target(
    db: ContextKey[ConnectionFactory],
    table_name: str,
    table_schema: TableSchema[RowT] | None = None,
    *,
    managed_by: Literal["system", "user"] = "system",
) -> TableTarget[RowT]:
    """Mount a table target and return a ready-to-use TableTarget."""
    provider = await coco.mount_target(
        table_target(db, table_name, table_schema, managed_by=managed_by)
    )
    return TableTarget(provider, table_schema, table_name)


def relation_target(
    db: ContextKey[ConnectionFactory],
    table_name: str,
    from_table: TableTarget[Any] | Collection[TableTarget[Any]],
    to_table: TableTarget[Any] | Collection[TableTarget[Any]],
    table_schema: TableSchema[RowT] | None = None,
    *,
    managed_by: Literal["system", "user"] = "system",
) -> coco.TargetState[_RecordHandler]:
    """Create a TargetState for a SurrealDB relation table."""
    _validate_identifier(table_name, "relation table name")
    from_names = _normalize_table_names(from_table)
    to_names = _normalize_table_names(to_table)
    for n in from_names:
        _validate_identifier(n, "from table name")
    for n in to_names:
        _validate_identifier(n, "to table name")
    key = _TableKey(db_key=db.key, table_name=table_name)
    spec = _TableSpec(
        table_schema=table_schema,
        is_relation=True,
        from_tables=tuple(sorted(from_names)),
        to_tables=tuple(sorted(to_names)),
        managed_by=managed_by,
    )
    return _table_provider.target_state(key, spec)


def declare_relation_target(
    db: ContextKey[ConnectionFactory],
    table_name: str,
    from_table: TableTarget[Any] | Collection[TableTarget[Any]],
    to_table: TableTarget[Any] | Collection[TableTarget[Any]],
    table_schema: TableSchema[RowT] | None = None,
    *,
    managed_by: Literal["system", "user"] = "system",
) -> RelationTarget[RowT, coco.PendingS]:
    """Declare a relation target and return a pending RelationTarget."""
    from_names = _normalize_table_names(from_table)
    to_names = _normalize_table_names(to_table)
    provider = coco.declare_target_state_with_child(
        relation_target(
            db,
            table_name,
            from_table,
            to_table,
            table_schema,
            managed_by=managed_by,
        )
    )
    return RelationTarget(provider, table_name, table_schema, from_names, to_names)


async def mount_relation_target(
    db: ContextKey[ConnectionFactory],
    table_name: str,
    from_table: TableTarget[Any] | Collection[TableTarget[Any]],
    to_table: TableTarget[Any] | Collection[TableTarget[Any]],
    table_schema: TableSchema[RowT] | None = None,
    *,
    managed_by: Literal["system", "user"] = "system",
) -> RelationTarget[RowT]:
    """Mount a relation target and return a ready-to-use RelationTarget."""
    from_names = _normalize_table_names(from_table)
    to_names = _normalize_table_names(to_table)
    provider = await coco.mount_target(
        relation_target(
            db,
            table_name,
            from_table,
            to_table,
            table_schema,
            managed_by=managed_by,
        )
    )
    return RelationTarget(provider, table_name, table_schema, from_names, to_names)


# ---------------------------------------------------------------------------
# Public exports
# ---------------------------------------------------------------------------

__all__ = [
    "ColumnDef",
    "ConnectionFactory",
    "RelationTarget",
    "SurrealType",
    "TableSchema",
    "TableTarget",
    "ValueEncoder",
    "declare_relation_target",
    "declare_table_target",
    "mount_relation_target",
    "mount_table_target",
    "relation_target",
    "table_target",
]
