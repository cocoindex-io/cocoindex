"""
Neo4j target for CocoIndex.

Two-level state system:
1. Table level — creates/drops Cypher indexes and uniqueness constraints
   for node labels and relationship types (real Cypher DDL, not best-effort
   like FalkorDB's GRAPH.CONSTRAINT redis command).
2. Record level — upserts/deletes nodes via Cypher MERGE and edges via
   triple-MERGE (source, target, relationship).

Multitenancy is by Neo4j database name (one Neo4j cluster, many isolated
databases); the database is part of the ``ConnectionFactory``.

Targets Neo4j 5.18+. Vector indexes use the CREATE VECTOR INDEX DDL form
that shipped in 5.18; older Neo4j 5 servers will reject the DDL.
"""

from __future__ import annotations

import datetime
import decimal
import logging
import re
import uuid as uuid_mod
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
    import neo4j as _neo4j  # type: ignore[import-not-found]
except ImportError as e:
    raise ImportError(
        "neo4j is required to use the Neo4j connector. Please install cocoindex[neo4j]."
    ) from e

if TYPE_CHECKING:
    AsyncDriver = Any
else:
    AsyncDriver = _neo4j.AsyncDriver

import msgspec
import numpy as np

import cocoindex as coco
from cocoindex._internal.context_keys import ContextKey, ContextProvider
from cocoindex._internal.datatype import (
    AnyType,
    MappingType,
    RecordType,
    SequenceType,
    TypeChecker,
    UnionType,
    analyze_type_info,
    is_record_type,
)
from cocoindex.connectorkits import statediff, target
from cocoindex.connectorkits.fingerprint import fingerprint_object
from cocoindex.resources import schema as res_schema

from . import _cypher

_logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Identifier validation
# ---------------------------------------------------------------------------

_IDENTIFIER_RE = _cypher.IDENTIFIER_RE
_validate_identifier = _cypher.validate_identifier


# ---------------------------------------------------------------------------
# Connection factory
# ---------------------------------------------------------------------------


class _GraphHandle:
    """Thin wrapper around a Neo4j async driver+database that exposes a
    ``query(cypher, params)`` method for parity with the FalkorDB connector.

    A new session is opened per ``query`` call and closed before returning.
    The driver itself is shared and connection-pooled internally.

    For batched apply paths that need atomicity, callers can use
    :meth:`begin_tx` to wrap multiple statements in a single managed
    transaction.
    """

    _driver: AsyncDriver
    _database: str

    def __init__(self, driver: AsyncDriver, database: str) -> None:
        self._driver = driver
        self._database = database

    @property
    def database(self) -> str:
        return self._database

    async def query(
        self, cypher: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Run a single statement against this database and return the rows."""
        async with self._driver.session(database=self._database) as session:
            result = await session.run(cypher, **(params or {}))
            return [dict(record) async for record in result]


class ConnectionFactory:
    """
    Connection factory for Neo4j.

    Holds connection parameters and creates an authenticated, pooled async
    driver on demand. The database name is part of the factory (not the
    table key) — different databases on the same Neo4j cluster are
    addressed via separate ``ConnectionFactory`` / ``ContextKey`` pairs.

    Example::

        factory = neo4j.ConnectionFactory(
            uri="bolt://localhost:7687",
            auth=("neo4j", "cocoindex"),
            database="neo4j",
        )
        builder.provide(NEO4J_DB, factory)
    """

    def __init__(
        self,
        uri: str,
        *,
        auth: tuple[str, str] | None = None,
        database: str = "neo4j",
    ) -> None:
        # Database identifiers in Neo4j 5 may contain hyphens and dots; be
        # less strict than the Cypher identifier validator. Just reject the
        # obviously dangerous characters so we don't have to escape.
        if not re.match(r"^[A-Za-z0-9._-]+$", database):
            raise ValueError(
                f"Invalid Neo4j database name: {database!r}. "
                "Must match [A-Za-z0-9._-]+."
            )
        self._uri = uri
        self._auth = auth
        self._database = database

    @property
    def database(self) -> str:
        return self._database

    async def acquire(self) -> _GraphHandle:
        """Return a graph handle ready to issue ``query(cypher, params)``."""
        driver = _neo4j.AsyncGraphDatabase.driver(self._uri, auth=self._auth)
        return _GraphHandle(driver, self._database)


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
# Neo4jType annotation
# ---------------------------------------------------------------------------


class Neo4jType(NamedTuple):
    """
    Annotation to override the default Python → Neo4j type mapping for a
    column. The Neo4j Bolt protocol has rich native types (bytes, datetime,
    duration, point) so most overrides are not needed, but this hook is
    available for cases where you want to encode a value differently before
    it's sent over the wire.

    Use with ``typing.Annotated``::

        from typing import Annotated
        from cocoindex.connectors.neo4j import Neo4jType

        @dataclass
        class Row:
            id: str
            score: Annotated[float, Neo4jType("decimal", encoder=str)]
    """

    neo4j_type: str
    encoder: ValueEncoder | None = None


# ---------------------------------------------------------------------------
# Value encoders
# ---------------------------------------------------------------------------


def _decimal_str(value: Any) -> str:
    return str(value)


def _ndarray_to_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    return value.tolist()  # type: ignore[no-any-return]


def _uuid_str(value: Any) -> str:
    if isinstance(value, str):
        return value
    return str(value)


# ---------------------------------------------------------------------------
# Type mapping
# ---------------------------------------------------------------------------


class _TypeMapping(NamedTuple):
    neo4j_type: str
    encoder: ValueEncoder | None = None


# Neo4j Bolt has native bool/int/float/str/bytes/list/map/datetime/date/time/
# duration/point — most types pass through unencoded. The exceptions are:
# Decimal (no native), UUID (sent as string by convention), and ndarray
# (send as list, paired with vector-index DDL).
_LEAF_TYPE_MAPPINGS: dict[type, _TypeMapping] = {
    # Boolean
    bool: _TypeMapping("BOOLEAN"),
    # Integers
    int: _TypeMapping("INTEGER"),
    np.int8: _TypeMapping("INTEGER"),
    np.int16: _TypeMapping("INTEGER"),
    np.int32: _TypeMapping("INTEGER"),
    np.int64: _TypeMapping("INTEGER"),
    np.uint8: _TypeMapping("INTEGER"),
    np.uint16: _TypeMapping("INTEGER"),
    np.uint32: _TypeMapping("INTEGER"),
    np.uint64: _TypeMapping("INTEGER"),
    np.int_: _TypeMapping("INTEGER"),
    np.uint: _TypeMapping("INTEGER"),
    # Floats
    float: _TypeMapping("FLOAT"),
    np.float16: _TypeMapping("FLOAT"),
    np.float32: _TypeMapping("FLOAT"),
    np.float64: _TypeMapping("FLOAT"),
    # Decimal — Neo4j has no decimal; store as string.
    decimal.Decimal: _TypeMapping("STRING", _decimal_str),
    # Strings — UUID encodes as STRING.
    str: _TypeMapping("STRING"),
    bytes: _TypeMapping("BYTES"),
    uuid_mod.UUID: _TypeMapping("STRING", _uuid_str),
    # Date/time — pass-through; Neo4j Bolt accepts datetime/date/time/timedelta.
    datetime.date: _TypeMapping("DATE"),
    datetime.time: _TypeMapping("LOCAL_TIME"),
    datetime.datetime: _TypeMapping("ZONED_DATETIME"),
    datetime.timedelta: _TypeMapping("DURATION"),
}

_OBJECT_MAPPING = _TypeMapping("MAP")
_ARRAY_MAPPING = _TypeMapping("LIST<ANY>")


async def _get_type_mapping(
    python_type: Any, *, vector_schema: res_schema.VectorSchema | None = None
) -> _TypeMapping:
    type_info = analyze_type_info(python_type)

    for annotation in type_info.annotations:
        if isinstance(annotation, Neo4jType):
            return _TypeMapping(annotation.neo4j_type, annotation.encoder)

    base_type = type_info.base_type

    if base_type in _LEAF_TYPE_MAPPINGS:
        return _LEAF_TYPE_MAPPINGS[base_type]

    if base_type is np.ndarray:
        if vector_schema is None:
            raise ValueError("VectorSchemaProvider is required for NumPy ndarray type.")
        if vector_schema.size <= 0:
            raise ValueError(f"Invalid vector dimension: {vector_schema.size}")
        return _TypeMapping(
            neo4j_type="LIST<FLOAT>",
            encoder=_ndarray_to_list,
        )
    elif vector_schema is not None:
        raise ValueError(
            "VectorSchemaProvider is only supported for NumPy ndarray type. "
            f"Got type: {python_type}"
        )

    if isinstance(type_info.variant, (SequenceType,)):
        return _ARRAY_MAPPING
    if isinstance(type_info.variant, (MappingType, RecordType, UnionType, AnyType)):
        return _OBJECT_MAPPING

    return _OBJECT_MAPPING


# ---------------------------------------------------------------------------
# ColumnDef
# ---------------------------------------------------------------------------


class ColumnDef(NamedTuple):
    """Definition of a column (property) in a Neo4j table.

    ``type`` is metadata-only — Neo4j does not enforce per-property types
    server-side without optional property type constraints (Neo4j 5.9+).
    The string contributes to the schema fingerprint and is surfaced in
    error messages, but no DDL is emitted from it in this version.
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
    """Schema definition for a Neo4j table (node label or relationship type).

    Single-field primary key (named via ``primary_key``, default ``"id"``).
    Compound primary keys are not supported in v1.0.
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
        column_overrides: dict[str, Neo4jType | res_schema.VectorSchemaProvider]
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
        column_overrides: dict[str, Neo4jType | res_schema.VectorSchemaProvider] | None,
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

            neo4j_type_annotation = next(
                (t for t in all_annotations if isinstance(t, Neo4jType)), None
            )
            vector_schema = None
            for annot in all_annotations:
                vs = await res_schema.get_vector_schema(annot)
                if vs is not None:
                    vector_schema = vs
                    break

            if neo4j_type_annotation is not None:
                type_mapping = _TypeMapping(
                    neo4j_type_annotation.neo4j_type,
                    neo4j_type_annotation.encoder,
                )
            else:
                type_mapping = await _get_type_mapping(
                    field.type_hint, vector_schema=vector_schema
                )

            columns[field.name] = ColumnDef(
                type=type_mapping.neo4j_type.strip(),
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


class _SharedRecordApplier:
    """Owns a TargetActionSink shared by all record handlers for one
    Neo4j database.

    Unlike FalkorDB which can't multi-statement transact, we wrap each
    apply batch in a single Neo4j transaction so partial writes roll back
    on failure. Actions are still grouped into the four-bucket ordering
    so an edge is never written before its endpoints exist.
    """

    _graph: _GraphHandle
    sink: coco.TargetActionSink[_RecordAction, None]

    def __init__(self, graph: _GraphHandle) -> None:
        self._graph = graph
        self.sink = coco.TargetActionSink.from_async_fn(self._apply_actions)

    async def _apply_actions(
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

        async with self._graph._driver.session(  # noqa: SLF001
            database=self._graph.database
        ) as session:
            tx = await session.begin_transaction()
            try:
                for action in upsert_normal:
                    await self._apply_node_upsert(tx, action)
                for action in upsert_relation:
                    await self._apply_relation_upsert(tx, action)
                for action in delete_relation:
                    await self._apply_relation_delete(tx, action)
                for action in delete_normal:
                    await self._apply_node_delete(tx, action)
                await tx.commit()
            except BaseException:
                await tx.rollback()
                raise

    @staticmethod
    async def _apply_node_upsert(tx: Any, action: _RecordAction) -> None:
        assert action.value is not None
        # PK is always single-field in v1.0; props are everything except the PK.
        pk_value = action.value.get(action.pk_field, action.record_id)
        props = {k: v for k, v in action.value.items() if k != action.pk_field}
        cypher = _cypher.build_node_upsert(
            label=action.table_name,
            pk_fields=[action.pk_field],
            has_value_fields=bool(props),
        )
        params: dict[str, Any] = {"key_0": pk_value}
        if props:
            params["props"] = props
        await tx.run(cypher, **params)

    @staticmethod
    async def _apply_node_delete(tx: Any, action: _RecordAction) -> None:
        cypher = _cypher.build_node_delete(
            label=action.table_name, pk_fields=[action.pk_field]
        )
        await tx.run(cypher, key_0=action.record_id)

    @staticmethod
    async def _apply_relation_upsert(tx: Any, action: _RecordAction) -> None:
        assert action.value is not None
        assert action.from_label is not None and action.from_pk_field is not None
        assert action.to_label is not None and action.to_pk_field is not None
        props = {k: v for k, v in action.value.items() if k != action.pk_field}
        cypher = _cypher.build_relationship_upsert(
            rel_type=action.table_name,
            from_label=action.from_label,
            from_pk_fields=[action.from_pk_field],
            to_label=action.to_label,
            to_pk_fields=[action.to_pk_field],
            rel_pk_fields=[action.pk_field],
            has_value_fields=bool(props),
        )
        params: dict[str, Any] = {
            "from_key_0": action.from_id,
            "to_key_0": action.to_id,
            "rel_key_0": action.record_id,
        }
        if props:
            params["props"] = props
        await tx.run(cypher, **params)

    @staticmethod
    async def _apply_relation_delete(tx: Any, action: _RecordAction) -> None:
        cypher = _cypher.build_relationship_delete(
            rel_type=action.table_name, pk_fields=[action.pk_field]
        )
        await tx.run(cypher, key_0=action.record_id)


# ---------------------------------------------------------------------------
# Vector index
# ---------------------------------------------------------------------------


# Neo4j accepts only "cosine" and "euclidean" for vector.similarity_function.
_METRIC_TO_NEO4J: dict[str, str] = {
    "cosine": "cosine",
    "euclidean": "euclidean",
}


class _VectorIndexSpec(NamedTuple):
    field: str
    metric: str
    dimension: int


_VectorIndexFingerprint = bytes


class _VectorIndexAction(NamedTuple):
    """Vector index DDL action.

    ``index_name`` is always populated (carries the persisted index name
    even on delete, so DROP can identify the index). ``spec`` is ``None``
    only on delete.
    """

    name: str
    table_name: str
    index_name: str
    spec: _VectorIndexSpec | None


# Tracking record for a vector index — store the spec itself rather than just
# its fingerprint so a subsequent delete can recover the field name.
_VectorIndexTrackingRecord = _VectorIndexSpec


class _VectorIndexHandler:
    """Attachment handler for vector indexes on a Neo4j node label."""

    _graph: _GraphHandle
    _table_name: str
    _sink: coco.TargetActionSink[_VectorIndexAction, None]

    def __init__(self, graph: _GraphHandle, table_name: str) -> None:
        self._graph = graph
        self._table_name = table_name
        self._sink = coco.TargetActionSink.from_async_fn(self._apply_actions)

    async def _apply_actions(
        self,
        context_provider: ContextProvider,
        actions: Sequence[_VectorIndexAction],
    ) -> None:
        for action in actions:
            if action.spec is None:
                try:
                    await self._graph.query(
                        _cypher.build_vector_index_drop(action.index_name)
                    )
                except Exception as e:  # noqa: BLE001
                    _logger.debug(
                        "Neo4j DROP VECTOR INDEX %s (best-effort) failed: %s",
                        action.index_name,
                        e,
                    )
                continue

            # Drop-and-recreate so a metric/dimension change takes effect.
            try:
                await self._graph.query(
                    _cypher.build_vector_index_drop(action.index_name)
                )
            except Exception as e:  # noqa: BLE001
                _logger.debug(
                    "Neo4j DROP VECTOR INDEX (pre-create best-effort) failed: %s",
                    e,
                )
            await self._graph.query(
                _cypher.build_vector_index_create(
                    name=action.index_name,
                    label=action.table_name,
                    field=action.spec.field,
                    dimension=action.spec.dimension,
                    metric=_METRIC_TO_NEO4J.get(action.spec.metric, action.spec.metric),
                )
            )

    def reconcile(
        self,
        key: coco.StableKey,
        desired_state: _VectorIndexSpec | coco.NonExistenceType,
        prev_possible_records: Collection[_VectorIndexTrackingRecord],
        prev_may_be_missing: bool,
        /,
    ) -> (
        coco.TargetReconcileOutput[_VectorIndexAction, _VectorIndexTrackingRecord, None]
        | None
    ):
        assert isinstance(key, str)
        if coco.is_non_existence(desired_state):
            if not prev_possible_records and not prev_may_be_missing:
                return None
            # Recover the field from the most recent tracked spec — needed
            # to mint the index name for the DROP statement (Neo4j drops
            # by name, but we don't persist the name separately).
            prev_field: str | None = None
            for prev in prev_possible_records:
                prev_field = prev.field
                break
            if prev_field is None:
                return None
            return coco.TargetReconcileOutput(
                action=_VectorIndexAction(
                    name=key,
                    table_name=self._table_name,
                    index_name=_cypher.vector_index_name(self._table_name, prev_field),
                    spec=None,
                ),
                sink=self._sink,
                tracking_record=coco.NON_EXISTENCE,
            )

        if not prev_may_be_missing and all(
            prev == desired_state for prev in prev_possible_records
        ):
            return None

        return coco.TargetReconcileOutput(
            action=_VectorIndexAction(
                name=key,
                table_name=self._table_name,
                index_name=_cypher.vector_index_name(
                    self._table_name, desired_state.field
                ),
                spec=desired_state,
            ),
            sink=self._sink,
            tracking_record=desired_state,
        )


# ---------------------------------------------------------------------------
# _RecordHandler
# ---------------------------------------------------------------------------


class _RecordHandler(coco.TargetHandler[_RowValue, _RowFingerprint]):
    """Handler for record-level target states within a Neo4j table."""

    _table_name: str
    _is_relation: bool
    _pk_field: str
    _table_schema: TableSchema[Any] | None
    _graph: _GraphHandle
    _sink: coco.TargetActionSink[_RecordAction, None]

    def __init__(
        self,
        table_name: str,
        is_relation: bool,
        pk_field: str,
        table_schema: TableSchema[Any] | None,
        graph: _GraphHandle,
        sink: coco.TargetActionSink[_RecordAction, None],
    ) -> None:
        self._table_name = table_name
        self._is_relation = is_relation
        self._pk_field = pk_field
        self._table_schema = table_schema
        self._graph = graph
        self._sink = sink

    def attachments(self) -> dict[str, _VectorIndexHandler]:
        # Eagerly declare all attachment types so the engine can clean up
        # orphaned attachments even on runs that don't re-declare them.
        return {
            "vector_index": _VectorIndexHandler(self._graph, self._table_name),
        }

    def _encode_row(self, row_dict: dict[str, Any]) -> dict[str, Any]:
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
        prev_possible_records: Collection[_RowFingerprint],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[_RecordAction, _RowFingerprint, None] | None:
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
    table_schema: TableSchema[Any] | None
    primary_key: str
    is_relation: bool
    from_label: str | None
    from_pk_field: str | None
    to_label: str | None
    to_pk_field: str | None
    managed_by: target.ManagedBy = target.ManagedBy.SYSTEM


class _TableMainRecord(msgspec.Struct, frozen=True):
    """Tracking record for table-level properties — change ⇒ DROP+CREATE index."""

    has_schema: bool
    is_relation: bool
    primary_key: str
    pk_type: str | None
    from_label: str | None
    from_pk_field: str | None
    to_label: str | None
    to_pk_field: str | None


class _FieldTrackingRecord(msgspec.Struct, frozen=True):
    """Per-field tracking record. Neo4j has optional property-type
    constraints (5.9+) but this connector does not emit them yet, so the
    record is fingerprint-only — schema fingerprint stability lets two
    flows share a table only if they declare matching columns.
    """

    neo4j_type: str
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
    # Needed on "delete"/"replace" to know what artifact to drop.
    prev_pk_field: str | None
    prev_is_relation: bool


def _table_composite_tracking_record_from_spec(
    spec: _TableSpec,
) -> statediff.CompositeTrackingRecord[_TableMainRecord, str, _FieldTrackingRecord]:
    schema = spec.table_schema
    has_schema = schema is not None
    pk_type: str | None = None
    sub: dict[str, _FieldTrackingRecord] = {}

    if schema is not None:
        pk_col = schema.columns.get(spec.primary_key)
        if pk_col is not None:
            pk_type = pk_col.type
        for col_name, col_def in schema.columns.items():
            if col_name == spec.primary_key:
                continue
            sub[_field_subkey(col_name)] = _FieldTrackingRecord(
                neo4j_type=col_def.type,
                nullable=col_def.nullable,
            )

    main = _TableMainRecord(
        has_schema=has_schema,
        is_relation=spec.is_relation,
        primary_key=spec.primary_key,
        pk_type=pk_type,
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
    """Handler for table-level state — Cypher index DDL + uniqueness constraints."""

    _sink: coco.TargetActionSink[_TableAction, _RecordHandler]

    def __init__(self) -> None:
        self._sink = coco.TargetActionSink.from_async_fn(self._apply_actions)

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

        column_actions: dict[str, statediff.DiffAction] = {}
        if main_action is None:
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
        # tracking record so DROP can identify the underlying artifact.
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
            child_invalidation = "destructive"
        elif main_action is None and any(
            a != "insert" for a in column_actions.values()
        ):
            # No incremental property DDL emitted in v1; treat column
            # changes as lossy so dependents re-upsert defensively.
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

    async def _apply_actions(
        self, context_provider: ContextProvider, actions: Sequence[_TableAction]
    ) -> list[coco.ChildTargetDef[_RecordHandler] | None]:
        actions_list = list(actions)
        outputs: list[coco.ChildTargetDef[_RecordHandler] | None] = [None] * len(
            actions_list
        )

        # Group by db_key so each Neo4j driver is acquired once per batch.
        by_db: dict[str, list[int]] = {}
        for i, action in enumerate(actions_list):
            by_db.setdefault(action.key.db_key, []).append(i)

        for db_key, idxs in by_db.items():
            factory: ConnectionFactory = context_provider.get(db_key)  # type: ignore[assignment]
            graph = await factory.acquire()
            shared_applier = _SharedRecordApplier(graph)

            # Order: create nodes → create relations → drop relations → drop nodes.
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
                spec = action.spec

                if action.main_action in ("replace", "delete"):
                    await self._drop_table_artifacts(graph, action.key, action)

                if coco.is_non_existence(spec):
                    outputs[i] = None
                    continue

                if action.main_action in ("insert", "upsert", "replace"):
                    await self._create_table(graph, action.key, spec)
                # No incremental column DDL — column_actions are tracked
                # for fingerprint stability but not applied here.

                outputs[i] = coco.ChildTargetDef(
                    handler=_RecordHandler(
                        table_name=action.key.table_name,
                        is_relation=spec.is_relation,
                        pk_field=spec.primary_key,
                        table_schema=spec.table_schema,
                        graph=graph,
                        sink=shared_applier.sink,
                    )
                )

        return outputs

    @staticmethod
    async def _create_table(
        graph: _GraphHandle, key: _TableKey, spec: _TableSpec
    ) -> None:
        """Create the supporting Cypher index and uniqueness constraint.

        Neo4j 5 has real CREATE CONSTRAINT … IS UNIQUE on node labels (and
        IS RELATIONSHIP UNIQUE on relationship types in 5.7+). Neo4j auto-
        creates a backing index for each constraint, so a separate CREATE
        INDEX on the same property is redundant for nodes — but we still
        emit one for relationship types where the constraint isn't a
        full primary-key.
        """
        idx_name = _cypher.index_name(
            "rel" if spec.is_relation else "node",
            key.table_name,
            [spec.primary_key],
        )
        if spec.is_relation:
            await graph.query(
                _cypher.build_relationship_index_create(
                    idx_name, key.table_name, [spec.primary_key]
                )
            )
        else:
            constraint_name = _cypher.constraint_name(
                key.table_name, [spec.primary_key]
            )
            await graph.query(
                _cypher.build_constraint_create(
                    constraint_name, key.table_name, [spec.primary_key]
                )
            )

    @staticmethod
    async def _drop_table_artifacts(
        graph: _GraphHandle, key: _TableKey, action: _TableAction
    ) -> None:
        """Drop the supporting Cypher index + uniqueness constraint on
        table teardown.

        Uses ``prev_pk_field`` recovered during reconcile from the previous
        tracking record — that's what was actually CREATEd, so it's what
        we need to DROP.
        """
        pk_field = action.prev_pk_field
        is_relation = action.prev_is_relation
        if pk_field is None and isinstance(action.spec, _TableSpec):
            pk_field = action.spec.primary_key
            is_relation = action.spec.is_relation
        if pk_field is None:
            return  # Nothing to drop.

        if is_relation:
            idx_name = _cypher.index_name("rel", key.table_name, [pk_field])
            await graph.query(_cypher.build_relationship_index_drop(idx_name))
        else:
            cn = _cypher.constraint_name(key.table_name, [pk_field])
            await graph.query(_cypher.build_constraint_drop(cn))


# ---------------------------------------------------------------------------
# Root provider registration
# ---------------------------------------------------------------------------

_table_provider = coco.register_root_target_states_provider(
    "cocoindex/neo4j/table", _TableHandler()
)


# ---------------------------------------------------------------------------
# TableTarget
# ---------------------------------------------------------------------------


class TableTarget(
    Generic[RowT, coco.MaybePendingS], coco.ResolvesTo["TableTarget[RowT]"]
):
    """A target for writing records to a Neo4j node table."""

    _provider: coco.TargetStateProvider[_RowValue, None, coco.MaybePendingS]
    _table_schema: TableSchema[RowT] | None
    _table_name: str
    _primary_key: str

    def __init__(
        self,
        provider: coco.TargetStateProvider[_RowValue, None, coco.MaybePendingS],
        table_schema: TableSchema[RowT] | None,
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

    def declare_record(self: TableTarget[RowT], *, row: RowT) -> None:
        """Declare a record (node) to be upserted to this table."""
        row_dict = self._row_to_dict(row)
        if self._primary_key not in row_dict:
            raise ValueError(f"row is missing primary key field {self._primary_key!r}")
        pk_values = (row_dict[self._primary_key],)
        coco.declare_target_state(self._provider.target_state(pk_values, row_dict))

    declare_row = declare_record

    def _row_to_dict(self, row: RowT) -> dict[str, Any]:
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
        if isinstance(row, dict):
            return dict(row)
        record_info = RecordType(type(row))
        return {f.name: getattr(row, f.name) for f in record_info.fields}

    def declare_vector_index(
        self: TableTarget[RowT],
        *,
        name: str | None = None,
        field: str,
        metric: Literal["cosine", "euclidean"] = "cosine",
        dimension: int,
    ) -> None:
        """Declare a vector index on a column of this table."""
        _validate_identifier(field, "vector index field")
        if name is None:
            name = f"vec_{self._table_name}__{field}"
        _validate_identifier(name, "vector index name")
        if dimension <= 0:
            raise ValueError(f"Invalid vector dimension: {dimension}")
        spec = _VectorIndexSpec(field=field, metric=metric, dimension=dimension)
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
    """A target for writing relation records (edges) to a Neo4j relationship type."""

    _provider: coco.TargetStateProvider[_RowValue, None, coco.MaybePendingS]
    _table_name: str
    _table_schema: TableSchema[RowT] | None
    _primary_key: str
    _from_table: TableTarget[Any]
    _to_table: TableTarget[Any]

    def __init__(
        self,
        provider: coco.TargetStateProvider[_RowValue, None, coco.MaybePendingS],
        table_name: str,
        table_schema: TableSchema[RowT] | None,
        primary_key: str,
        from_table: TableTarget[Any],
        to_table: TableTarget[Any],
    ) -> None:
        self._provider = provider
        self._table_name = table_name
        self._table_schema = table_schema
        self._primary_key = primary_key
        self._from_table = from_table
        self._to_table = to_table

    def declare_relation(
        self: RelationTarget[RowT],
        *,
        from_id: Any,
        to_id: Any,
        record: RowT | None = None,
    ) -> None:
        """Declare a relation record (edge)."""
        from_label = self._from_table.table_name
        from_pk_field = self._from_table.primary_key
        to_label = self._to_table.table_name
        to_pk_field = self._to_table.primary_key

        if record is not None:
            if self._table_schema is not None:
                row_dict: dict[str, Any] = {}
                for col_name, col in self._table_schema.columns.items():
                    if col_name == self._primary_key:
                        continue
                    if isinstance(record, dict):
                        value = record.get(col_name)
                    else:
                        value = getattr(record, col_name)
                    if value is not None and col.encoder is not None:
                        value = col.encoder(value)
                    row_dict[col_name] = value
                record_id = (
                    record.get(self._primary_key)
                    if isinstance(record, dict)
                    else getattr(record, self._primary_key, None)
                )
            elif isinstance(record, dict):
                row_dict = {k: v for k, v in record.items() if k != self._primary_key}
                record_id = record.get(self._primary_key)
            else:
                record_info = RecordType(type(record))
                row_dict = {
                    f.name: getattr(record, f.name)
                    for f in record_info.fields
                    if f.name != self._primary_key
                }
                record_id = getattr(record, self._primary_key, None)
        else:
            row_dict = {}
            record_id = None

        if record_id is None:
            record_id = f"{from_label}_{from_id}_{to_label}_{to_id}"

        row_value: _RowValue = _RelationRowValue(
            from_label=from_label,
            from_pk_field=from_pk_field,
            from_id=from_id,
            to_label=to_label,
            to_pk_field=to_pk_field,
            to_id=to_id,
            fields=row_dict,
        )

        pk_values = (record_id,)
        coco.declare_target_state(self._provider.target_state(pk_values, row_value))

    def __coco_memo_key__(self) -> str:
        return self._provider.memo_key


# ---------------------------------------------------------------------------
# Module-level entry points
# ---------------------------------------------------------------------------


def table_target(
    db: ContextKey[ConnectionFactory],
    table_name: str,
    table_schema: TableSchema[RowT] | None = None,
    *,
    primary_key: str = "id",
    managed_by: target.ManagedBy = target.ManagedBy.SYSTEM,
) -> coco.TargetState[_RecordHandler]:
    """Create a ``TargetState`` for a Neo4j node table (label)."""
    _validate_identifier(table_name, "table name")
    _validate_identifier(primary_key, "primary key")
    if table_schema is not None and table_schema.primary_key != primary_key:
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
    return _table_provider.target_state(key, spec)


def declare_table_target(
    db: ContextKey[ConnectionFactory],
    table_name: str,
    table_schema: TableSchema[RowT] | None = None,
    *,
    primary_key: str = "id",
    managed_by: target.ManagedBy = target.ManagedBy.SYSTEM,
) -> TableTarget[RowT, coco.PendingS]:
    """Declare a node table target.

    Use this for tables that exist only as relationship endpoints — no
    records flow into this declaration's own handler.
    """
    if table_schema is not None and table_schema.primary_key != primary_key:
        raise ValueError(
            f"primary_key {primary_key!r} does not match schema's "
            f"{table_schema.primary_key!r}"
        )
    pk = table_schema.primary_key if table_schema is not None else primary_key
    provider = coco.declare_target_state_with_child(
        table_target(
            db, table_name, table_schema, primary_key=pk, managed_by=managed_by
        )
    )
    return TableTarget(provider, table_schema, table_name, pk)


async def mount_table_target(
    db: ContextKey[ConnectionFactory],
    table_name: str,
    table_schema: TableSchema[RowT] | None = None,
    *,
    primary_key: str = "id",
    managed_by: target.ManagedBy = target.ManagedBy.SYSTEM,
) -> TableTarget[RowT]:
    """Mount a node table target ready to receive ``declare_record`` calls."""
    if table_schema is not None and table_schema.primary_key != primary_key:
        raise ValueError(
            f"primary_key {primary_key!r} does not match schema's "
            f"{table_schema.primary_key!r}"
        )
    pk = table_schema.primary_key if table_schema is not None else primary_key
    provider = await coco.mount_target(
        table_target(
            db, table_name, table_schema, primary_key=pk, managed_by=managed_by
        )
    )
    return TableTarget(provider, table_schema, table_name, pk)


def relation_target(
    db: ContextKey[ConnectionFactory],
    table_name: str,
    from_table: TableTarget[Any],
    to_table: TableTarget[Any],
    table_schema: TableSchema[RowT] | None = None,
    *,
    primary_key: str = "id",
    managed_by: target.ManagedBy = target.ManagedBy.SYSTEM,
) -> coco.TargetState[_RecordHandler]:
    """Create a ``TargetState`` for a Neo4j relationship type."""
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
        table_schema=table_schema,
        primary_key=primary_key,
        is_relation=True,
        from_label=from_table.table_name,
        from_pk_field=from_table.primary_key,
        to_label=to_table.table_name,
        to_pk_field=to_table.primary_key,
        managed_by=managed_by,
    )
    return _table_provider.target_state(key, spec)


def declare_relation_target(
    db: ContextKey[ConnectionFactory],
    table_name: str,
    from_table: TableTarget[Any],
    to_table: TableTarget[Any],
    table_schema: TableSchema[RowT] | None = None,
    *,
    primary_key: str = "id",
    managed_by: target.ManagedBy = target.ManagedBy.SYSTEM,
) -> RelationTarget[RowT, coco.PendingS]:
    """Declare a relation table target."""
    pk = table_schema.primary_key if table_schema is not None else primary_key
    provider = coco.declare_target_state_with_child(
        relation_target(
            db,
            table_name,
            from_table,
            to_table,
            table_schema,
            primary_key=pk,
            managed_by=managed_by,
        )
    )
    return RelationTarget(provider, table_name, table_schema, pk, from_table, to_table)


async def mount_relation_target(
    db: ContextKey[ConnectionFactory],
    table_name: str,
    from_table: TableTarget[Any],
    to_table: TableTarget[Any],
    table_schema: TableSchema[RowT] | None = None,
    *,
    primary_key: str = "id",
    managed_by: target.ManagedBy = target.ManagedBy.SYSTEM,
) -> RelationTarget[RowT]:
    """Mount a relation table target ready to receive ``declare_relation`` calls."""
    pk = table_schema.primary_key if table_schema is not None else primary_key
    provider = await coco.mount_target(
        relation_target(
            db,
            table_name,
            from_table,
            to_table,
            table_schema,
            primary_key=pk,
            managed_by=managed_by,
        )
    )
    return RelationTarget(provider, table_name, table_schema, pk, from_table, to_table)


# ---------------------------------------------------------------------------
# Public exports
# ---------------------------------------------------------------------------

__all__ = [
    "ColumnDef",
    "ConnectionFactory",
    "Neo4jType",
    "RelationTarget",
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
