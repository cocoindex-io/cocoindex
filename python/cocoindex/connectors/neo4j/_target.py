"""
Neo4j target for CocoIndex.

Two-level state system:
1. Table level — creates/drops Cypher indexes and uniqueness constraints
   for node labels and relationship types (real Cypher DDL, not best-effort
   like FalkorDB's GRAPH.CONSTRAINT redis command). A system-managed table
   owns its label / relationship type: dropping it deletes every node or
   relationship there before the DDL artifact goes.
2. Record level — upserts/deletes nodes via Cypher MERGE and edges via
   triple-MERGE (source, target, relationship).

Node tables are declared with the node API (``mount_node_target``) and
relationship tables with the relationship API (``mount_relationship_target``).
The table and relation APIs (``mount_table_target``, ``mount_relation_target``)
are their deprecated predecessors: they keep the keys and relation ids of
existing tables.

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
import warnings
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Collection,
    Generic,
    Literal,
    NamedTuple,
    Sequence,
    TYPE_CHECKING,
)

from typing_extensions import TypeVar

from cocoindex._internal.extras import install_hint

try:
    import neo4j as _neo4j  # type: ignore[import-not-found]
except ImportError as e:
    raise ImportError(
        "neo4j is required to use the Neo4j connector. " + install_hint("neo4j")
    ) from e

if TYPE_CHECKING:
    AsyncDriver = Any
else:
    AsyncDriver = _neo4j.AsyncDriver

import msgspec
import numpy as np

import cocoindex as coco
from cocoindex._internal import deadline
from cocoindex._internal.context_keys import ContextKey, ContextProvider
from cocoindex.connectorkits._per_loop_connections import PerLoopConnections
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
from cocoindex.connectorkits._graph_relationship_ids import (
    legacy_relation_id,
    relationship_id,
)
from cocoindex.connectorkits.fingerprint import fingerprint_object
from cocoindex.resources import schema as res_schema

from . import _cypher

_logger = logging.getLogger(__name__)

# Retry policy for an apply batch that hits a transient error (deadlock
# between concurrent batches): a handful of attempts, sub-second backoff.
_TXN_MAX_ATTEMPTS = 8
# One transaction applies at most this many record actions: a merged batch
# of every table of a database is otherwise unbounded. A chunk can still go
# over `db.memory.transaction.max` (large properties); it is then split.
_MAX_ACTIONS_PER_TXN = 1000


def _is_memory_limit(err: BaseException) -> bool:
    """A transaction over the server's memory limit. Neo4j reports it as a
    TransientError, but retrying the same transaction fails the same way."""
    return isinstance(err, _neo4j.exceptions.TransientError) and "Memory" in (
        err.code or ""
    )


def _is_retryable(err: Exception) -> bool:
    return isinstance(err, _neo4j.exceptions.TransientError) and not _is_memory_limit(
        err
    )


_TXN_RETRY_INITIAL_BACKOFF_SECONDS = 0.05
_TXN_RETRY_MAX_BACKOFF_SECONDS = 2.0


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

    Holds connection parameters and lazily creates an authenticated,
    connection-pooled async driver per event loop, shared by every table and
    apply batch on that loop.
    The database name is part of the factory (not the table key) — different
    databases on the same Neo4j cluster are addressed via separate
    ``ConnectionFactory`` / ``ContextKey`` pairs. Call :meth:`close` at
    shutdown to close the driver.

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
        self._connections: PerLoopConnections[AsyncDriver] = PerLoopConnections(
            self._open_driver, _close_driver, owner="Neo4j ConnectionFactory"
        )

    @property
    def database(self) -> str:
        return self._database

    def __coco_memo_key__(self) -> tuple[str, str]:
        # Where the data lives, not how to sign in: the open drivers are
        # not part of it, and a credential change keeps the memos.
        return (self._uri, self._database)

    async def acquire(self) -> _GraphHandle:
        """Return a graph handle ready to issue ``query(cypher, params)``.

        The factory keeps one driver per event loop (a driver is bound to the
        loop it is used on), created on the loop's first ``acquire()`` and
        reused after.
        """
        return _GraphHandle(await self._connections.acquire(), self._database)

    async def _open_driver(self) -> AsyncDriver:
        return _neo4j.AsyncGraphDatabase.driver(self._uri, auth=self._auth)

    async def close(self) -> None:
        """Close the driver of the running event loop, if any. Call it on the
        loop that used the factory (e.g. in the lifespan, after ``yield``); a
        later ``acquire()`` creates a new driver. Drivers of other loops can't
        be closed here; they are left open, with a warning."""
        await self._connections.close()


async def _close_driver(driver: AsyncDriver) -> None:
    await driver.close()


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
    # On a relation upsert: the id the deprecated relation API derives for
    # this edge. The edge between the same endpoints carrying it is deleted.
    legacy_id: str | None


@dataclass(frozen=True)
class _SharedRecordApplier:
    """The record sink callback shared by every record handler of one Neo4j
    database.

    Equal for the same ``ConnectionFactory``, so the record sinks of all tables
    of one database — node and relation tables alike — share one identity
    (see ``coco.TargetActionSink``) and the engine batches their actions
    together. That is what lets one batch order node upserts → relation
    upserts → relation deletes → node deletes, so an edge is never written
    before its endpoints exist.

    Unlike FalkorDB which can't multi-statement transact, a batch is applied
    in Neo4j transactions of at most ``_MAX_ACTIONS_PER_TXN`` actions each, in
    order (:meth:`_apply_batch`). A failed transaction rolls back and raises,
    so the batch's components are retried; earlier transactions stay
    committed, which is safe as the writes are idempotent (``MERGE``).
    """

    factory: ConnectionFactory

    async def __call__(
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

        graph = await self.factory.acquire()
        await self._apply_batch(
            graph, upsert_normal + upsert_relation + delete_relation + delete_normal
        )

    @classmethod
    async def _apply_batch(
        cls, graph: _GraphHandle, ordered: Sequence[_RecordAction]
    ) -> None:
        """Apply ``ordered`` (already in dependency order) in transactions of
        at most ``_MAX_ACTIONS_PER_TXN`` actions, one after the other."""
        for start in range(0, len(ordered), _MAX_ACTIONS_PER_TXN):
            await cls._apply_chunk(graph, ordered[start : start + _MAX_ACTIONS_PER_TXN])

    @classmethod
    async def _apply_chunk(
        cls, graph: _GraphHandle, chunk: Sequence[_RecordAction]
    ) -> None:
        """Apply ``chunk`` in one transaction, retried on a transient error;
        split in two when it goes over the server's transaction memory limit."""

        async def apply_in_txn() -> None:
            async with graph._driver.session(  # noqa: SLF001
                database=graph.database
            ) as session:
                tx = await session.begin_transaction()
                try:
                    for action in chunk:
                        if action.value is not None:
                            if action.is_relation:
                                await cls._apply_relation_upsert(tx, action)
                            else:
                                await cls._apply_node_upsert(tx, action)
                        elif action.is_relation:
                            await cls._apply_relation_delete(tx, action)
                        else:
                            await cls._apply_node_delete(tx, action)
                    await tx.commit()
                except BaseException:
                    await tx.rollback()
                    raise

        # Neo4j reports a deadlock as a TransientError and expects the client
        # to retry the transaction. All record writes of one database share
        # this sink, which applies one batch at a time, so a deadlock comes
        # from another client writing the same nodes.
        try:
            await deadline.retry_transient(
                apply_in_txn,
                retry_on=_is_retryable,
                max_attempts=_TXN_MAX_ATTEMPTS,
                backoff=deadline.exponential_backoff(
                    initial=_TXN_RETRY_INITIAL_BACKOFF_SECONDS,
                    multiplier=2.0,
                    max_delay=_TXN_RETRY_MAX_BACKOFF_SECONDS,
                    jitter=0.5,
                ),
                operation_name="neo4j apply batch",
            )
        except _neo4j.exceptions.TransientError as err:
            if not _is_memory_limit(err) or len(chunk) == 1:
                raise
            mid = len(chunk) // 2
            await cls._apply_chunk(graph, chunk[:mid])
            await cls._apply_chunk(graph, chunk[mid:])

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
            has_legacy_key=action.legacy_id is not None,
        )
        params: dict[str, Any] = {
            "from_key_0": action.from_id,
            "to_key_0": action.to_id,
            "rel_key_0": action.record_id,
        }
        if props:
            params["props"] = props
        if action.legacy_id is not None:
            params["legacy_key_0"] = action.legacy_id
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


@dataclass(frozen=True)
class _VectorIndexApplier:
    """The vector index sink callback of one Neo4j database (equal for the
    same ``ConnectionFactory``, so one sink identity per database).

    An index is tracked by its declared name, but the Neo4j index is named
    after its label and field (``index_name``). A renamed index therefore
    arrives as a create under the new name and a drop under the old one,
    both of one Neo4j index: the drop is skipped. Two declared indexes of one
    Neo4j index are an error.
    """

    factory: ConnectionFactory

    async def __call__(
        self,
        context_provider: ContextProvider,
        actions: Sequence[_VectorIndexAction],
    ) -> None:
        if not actions:
            return
        created: dict[str, str] = {}
        for action in actions:
            if action.spec is None:
                continue
            declared = created.setdefault(action.index_name, action.name)
            if declared != action.name:
                raise ValueError(
                    f"Vector indexes {declared!r} and {action.name!r} map to one "
                    f"Neo4j index {action.index_name!r}: declare one vector index "
                    f"per field of {action.table_name!r}."
                )
        graph = await self.factory.acquire()
        for action in actions:
            if action.spec is None:
                if action.index_name in created:
                    continue
                try:
                    await graph.query(
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
                await graph.query(_cypher.build_vector_index_drop(action.index_name))
            except Exception as e:  # noqa: BLE001
                _logger.debug(
                    "Neo4j DROP VECTOR INDEX (pre-create best-effort) failed: %s",
                    e,
                )
            await graph.query(
                _cypher.build_vector_index_create(
                    name=action.index_name,
                    label=action.table_name,
                    field=action.spec.field,
                    dimension=action.spec.dimension,
                    metric=_METRIC_TO_NEO4J.get(action.spec.metric, action.spec.metric),
                )
            )


class _VectorIndexHandler:
    """Attachment handler for vector indexes on a Neo4j node label."""

    _table_name: str
    _sink: coco.TargetActionSink[_VectorIndexAction]

    def __init__(self, factory: ConnectionFactory, table_name: str) -> None:
        self._table_name = table_name
        self._sink = coco.TargetActionSink.from_async_fn(_VectorIndexApplier(factory))

    def reconcile(
        self,
        key: coco.StableKey,
        desired_state: _VectorIndexSpec | coco.NonExistenceType,
        prev_possible_records: Collection[_VectorIndexTrackingRecord],
        prev_may_be_missing: bool,
        /,
    ) -> (
        coco.TargetReconcileOutput[_VectorIndexAction, _VectorIndexTrackingRecord]
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
    """Handler for record-level target states within a Neo4j table.

    Built by the table sink's ``child_handler`` factory, so it holds no
    connection: its sinks acquire one when they apply.
    """

    tracks_value_fingerprint = True

    _table_name: str
    _is_relation: bool
    _pk_field: str
    _table_schema: TableSchema[Any] | None
    _replaces_legacy_ids: bool
    _factory: ConnectionFactory
    _sink: coco.TargetActionSink[_RecordAction]

    def __init__(
        self,
        table_name: str,
        is_relation: bool,
        pk_field: str,
        table_schema: TableSchema[Any] | None,
        replaces_legacy_ids: bool,
        factory: ConnectionFactory,
    ) -> None:
        self._table_name = table_name
        self._is_relation = is_relation
        self._pk_field = pk_field
        self._table_schema = table_schema
        self._replaces_legacy_ids = replaces_legacy_ids
        self._factory = factory
        self._sink = coco.TargetActionSink.from_async_fn(_SharedRecordApplier(factory))

    def attachments(self) -> dict[str, _VectorIndexHandler]:
        # Eagerly declare all attachment types so the engine can clean up
        # orphaned attachments even on runs that don't re-declare them.
        return {
            "vector_index": _VectorIndexHandler(self._factory, self._table_name),
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
                    legacy_id=None,
                ),
                sink=self._sink,
                tracking_record=coco.NON_EXISTENCE,
            )

        target_fp = fingerprint_object(desired_state)
        if not prev_may_be_missing and all(
            prev == target_fp for prev in prev_possible_records
        ):
            return None

        legacy_id: str | None = None
        if isinstance(desired_state, _RelationRowValue):
            from_label = desired_state.from_label
            from_pk_field = desired_state.from_pk_field
            from_id = desired_state.from_id
            to_label = desired_state.to_label
            to_pk_field = desired_state.to_pk_field
            to_id = desired_state.to_id
            encoded = self._encode_row(desired_state.fields)
            # An edge new to this table may have been written by the
            # deprecated relation API, under the id it derives (a table
            # switched to the relationship API), and is replaced.
            if self._replaces_legacy_ids and not prev_possible_records:
                legacy_id = legacy_relation_id(from_label, from_id, to_label, to_id)
                if legacy_id == key[0]:
                    legacy_id = None
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
                legacy_id=legacy_id,
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
    # A table of the relationship API: an edge new to it replaces the edge
    # the deprecated relation API wrote between the same endpoints.
    replaces_legacy_ids: bool = False


class _TableMainRecord(msgspec.Struct, frozen=True):
    """Identity-defining table properties.

    A change here means existing nodes / relationships can no longer be
    matched record by record (their MERGE key pattern changed), so the table
    is rebuilt: contents deleted, DDL artifact recreated, children re-declared
    from scratch. Everything else about the schema is tracked per field —
    Neo4j has no server-side property types, so a field change at most
    re-upserts the records (lossy), never rebuilds.
    """

    is_relation: bool
    primary_key: str
    from_label: str | None
    from_pk_field: str | None
    to_label: str | None
    to_pk_field: str | None


class _FieldTrackingRecord(msgspec.Struct, frozen=True):
    """Per-field tracking record, primary key included. Neo4j has optional
    property-type constraints (5.9+) but this connector does not emit them
    yet, so the record is fingerprint-only — schema fingerprint stability
    lets two flows share a table only if they declare matching columns.
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
    # Needed on "delete"/"replace" to know what to destroy.
    prev_pk_field: str | None
    prev_is_relation: bool


def _table_composite_tracking_record_from_spec(
    spec: _TableSpec,
) -> statediff.CompositeTrackingRecord[_TableMainRecord, str, _FieldTrackingRecord]:
    sub: dict[str, _FieldTrackingRecord] = {}
    if spec.table_schema is not None:
        for col_name, col_def in spec.table_schema.columns.items():
            sub[_field_subkey(col_name)] = _FieldTrackingRecord(
                neo4j_type=col_def.type,
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
    """Handler for table-level state — Cypher index DDL + uniqueness constraints."""

    _sink: coco.TargetActionSink[_TableAction]

    def __init__(self) -> None:
        self._sink = coco.TargetActionSink.from_async_fn(
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
            replaces_legacy_ids=spec.replaces_legacy_ids,
            factory=context_provider.get(action.key.db_key, ConnectionFactory),
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

        # "upsert" means the table may or may not already exist: the DDL can
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
            # The table is rebuilt: its contents are deleted and re-declared.
            child_invalidation = "destructive"
        elif any(a != "insert" for a in column_actions.values()):
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
        self,
        context_provider: ContextProvider,
        actions: Sequence[_TableAction],
        /,
    ) -> None:
        # Group by db_key so each database's graph is acquired once per batch.
        by_db: dict[str, list[_TableAction]] = {}
        for action in actions:
            by_db.setdefault(action.key.db_key, []).append(action)

        for db_key, db_actions in by_db.items():
            graph = await context_provider.get(db_key, ConnectionFactory).acquire()

            # Order: create nodes → create relations → drop relations → drop nodes.
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
                    await self._drop_table(graph, action.key, action)

                if coco.is_non_existence(spec):
                    continue

                if action.main_action in ("insert", "upsert", "replace"):
                    await self._create_table(graph, action.key, spec)
                # No incremental column DDL — column_actions are tracked
                # for fingerprint stability but not applied here.

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
    async def _drop_table(
        graph: _GraphHandle, key: _TableKey, action: _TableAction
    ) -> None:
        """Destroy the table: delete its contents, then its DDL artifact.

        A system-managed table owns its label / relationship type the way a
        Postgres table owns its rows: every node carrying the label (with the
        relationships attached to it) or every relationship of the type is
        deleted, whoever wrote it. The engine prunes the records' tracking
        without reconciling them once their container is gone, so this is
        where their data goes away. A label shared with data CocoIndex does
        not own belongs in a ``managed_by="user"`` table, which never gets
        here.

        Uses ``prev_pk_field`` / ``prev_is_relation`` recovered during
        reconcile from the previous tracking record — that's what was
        actually CREATEd, so it's what we need to DROP.
        """
        pk_field = action.prev_pk_field
        is_relation = action.prev_is_relation
        if pk_field is None and isinstance(action.spec, _TableSpec):
            pk_field = action.spec.primary_key
            is_relation = action.spec.is_relation
        if pk_field is None:
            return  # Nothing to drop.

        if is_relation:
            await graph.query(_cypher.build_relationship_delete_all(key.table_name))
            idx_name = _cypher.index_name("rel", key.table_name, [pk_field])
            await graph.query(_cypher.build_relationship_index_drop(idx_name))
        else:
            await graph.query(_cypher.build_node_delete_all(key.table_name))
            cn = _cypher.constraint_name(key.table_name, [pk_field])
            await graph.query(_cypher.build_constraint_drop(cn))


# ---------------------------------------------------------------------------
# Root provider registration
# ---------------------------------------------------------------------------

# Node tables and relationship tables each have a root provider of their own,
# so a label and a relationship type of one name never share a key. The
# deprecated table and relation APIs keep the provider they always shared, so
# existing tables keep their keys. One handler serves all three: one sink then
# applies the table actions of a database, in order.
_table_handler = _TableHandler()
_table_provider = coco.register_root_target_states_provider(
    "cocoindex/neo4j/table", _table_handler
)
_node_provider = coco.register_root_target_states_provider(
    "cocoindex/neo4j/node", _table_handler
)
_relationship_provider = coco.register_root_target_states_provider(
    "cocoindex/neo4j/relationship", _table_handler
)


def _warn_deprecated(name: str, replacement: str) -> None:
    warnings.warn(
        f"neo4j.{name} is deprecated; use neo4j.{replacement}. To move an "
        "existing table over without rebuilding it, follow the switching steps "
        "in the Neo4j connector docs.",
        DeprecationWarning,
        stacklevel=3,
    )


# ---------------------------------------------------------------------------
# NodeTarget
# ---------------------------------------------------------------------------


class NodeTarget(
    Generic[RowT, coco.MaybePendingS], coco.ResolvesTo["NodeTarget[RowT]"]
):
    """A target for writing nodes to a Neo4j node table (label)."""

    _provider: coco.TargetStateProvider[_RowValue, None, coco.MaybePendingS]
    _table_schema: TableSchema[RowT] | None
    _table_name: str
    _primary_key: str
    # Field -> name of the vector indexes declared through this target.
    _vector_index_names: dict[str, str]

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
        self._vector_index_names = {}

    @property
    def table_name(self) -> str:
        return self._table_name

    @property
    def primary_key(self) -> str:
        return self._primary_key

    def declare_record(self: NodeTarget[RowT], *, row: RowT) -> None:
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
        self: NodeTarget[RowT],
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
        # The database keeps one vector index per label and field.
        declared = self._vector_index_names.setdefault(field, name)
        if declared != name:
            raise ValueError(
                f"Vector indexes {declared!r} and {name!r} both index "
                f"{self._table_name}.{field}: declare one vector index per field."
            )
        spec = _VectorIndexSpec(field=field, metric=metric, dimension=dimension)
        att_provider = self._provider.attachment("vector_index")
        coco.declare_target_state(att_provider.target_state(name, spec))

    def __coco_memo_key__(self) -> str:
        return self._provider.memo_key


class TableTarget(
    NodeTarget[RowT, coco.MaybePendingS], coco.ResolvesTo["TableTarget[RowT]"]
):
    """The node table target of the deprecated table API
    (:func:`mount_table_target`). Same methods as :class:`NodeTarget`."""


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
    derive_id: Callable[[str, Any, str, Any], str],
) -> tuple[Any, _RelationRowValue]:
    """The record id and value of a declared relationship. A record without
    the primary key gets the id ``derive_id`` makes from the endpoints."""
    from_label = from_table.table_name
    to_label = to_table.table_name

    record_id: Any = None
    row_dict: dict[str, Any] = {}
    if record is not None:
        if table_schema is not None:
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
        elif isinstance(record, dict):
            row_dict = {k: v for k, v in record.items() if k != primary_key}
            record_id = record.get(primary_key)
        else:
            record_info = RecordType(type(record))
            row_dict = {
                f.name: getattr(record, f.name)
                for f in record_info.fields
                if f.name != primary_key
            }
            record_id = getattr(record, primary_key, None)

    if record_id is None:
        record_id = derive_id(from_label, from_id, to_label, to_id)

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
    """A target for writing relationships (edges) to a Neo4j relationship
    table (relationship type)."""

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
            relationship_id,
        )
        coco.declare_target_state(self._provider.target_state((record_id,), row_value))

    def __coco_memo_key__(self) -> str:
        return self._provider.memo_key


class RelationTarget(
    Generic[RowT, coco.MaybePendingS], coco.ResolvesTo["RelationTarget[RowT]"]
):
    """The relation table target of the deprecated relation API
    (:func:`mount_relation_target`); see :class:`RelationshipTarget`."""

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
        """Deprecated: use :meth:`RelationshipTarget.declare_relationship`.
        Declare a relation record (edge)."""
        _warn_deprecated(
            "RelationTarget.declare_relation", "RelationshipTarget.declare_relationship"
        )
        record_id, row_value = _relationship_state(
            self._table_schema,
            self._primary_key,
            self._from_table,
            self._to_table,
            from_id,
            to_id,
            record,
            legacy_relation_id,
        )
        coco.declare_target_state(self._provider.target_state((record_id,), row_value))

    def __coco_memo_key__(self) -> str:
        return self._provider.memo_key


# ---------------------------------------------------------------------------
# Module-level entry points
# ---------------------------------------------------------------------------


def _node_table_state(
    provider: coco.TargetStateProvider[_TableSpec, _RecordHandler],
    db: ContextKey[ConnectionFactory],
    table_name: str,
    table_schema: TableSchema[Any] | None,
    primary_key: str,
    managed_by: target.ManagedBy,
) -> coco.TargetState[_RecordHandler]:
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
    return provider.target_state(key, spec)


def _relationship_table_state(
    provider: coco.TargetStateProvider[_TableSpec, _RecordHandler],
    db: ContextKey[ConnectionFactory],
    table_name: str,
    from_table: NodeTarget[Any],
    to_table: NodeTarget[Any],
    table_schema: TableSchema[Any] | None,
    primary_key: str,
    managed_by: target.ManagedBy,
    *,
    replaces_legacy_ids: bool,
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
        table_schema=table_schema,
        primary_key=primary_key,
        is_relation=True,
        from_label=from_table.table_name,
        from_pk_field=from_table.primary_key,
        to_label=to_table.table_name,
        to_pk_field=to_table.primary_key,
        managed_by=managed_by,
        replaces_legacy_ids=replaces_legacy_ids,
    )
    return provider.target_state(key, spec)


def node_target(
    db: ContextKey[ConnectionFactory],
    table_name: str,
    table_schema: TableSchema[RowT] | None = None,
    *,
    primary_key: str = "id",
    managed_by: target.ManagedBy = target.ManagedBy.SYSTEM,
) -> coco.TargetState[_RecordHandler]:
    """Create a ``TargetState`` for a Neo4j node table (label)."""
    _cypher.validate_label(table_name, "node label")
    return _node_table_state(
        _node_provider, db, table_name, table_schema, primary_key, managed_by
    )


def declare_node_target(
    db: ContextKey[ConnectionFactory],
    table_name: str,
    table_schema: TableSchema[RowT] | None = None,
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
    db: ContextKey[ConnectionFactory],
    table_name: str,
    table_schema: TableSchema[RowT] | None = None,
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
    db: ContextKey[ConnectionFactory],
    table_name: str,
    from_table: NodeTarget[Any],
    to_table: NodeTarget[Any],
    table_schema: TableSchema[RowT] | None = None,
    *,
    primary_key: str = "id",
    managed_by: target.ManagedBy = target.ManagedBy.SYSTEM,
) -> coco.TargetState[_RecordHandler]:
    """Create a ``TargetState`` for a Neo4j relationship table (relationship
    type). ``from_table`` and ``to_table`` may come from the node API or the
    deprecated table API."""
    _cypher.validate_label(table_name, "relationship type")
    return _relationship_table_state(
        _relationship_provider,
        db,
        table_name,
        from_table,
        to_table,
        table_schema,
        primary_key,
        managed_by,
        replaces_legacy_ids=True,
    )


def declare_relationship_target(
    db: ContextKey[ConnectionFactory],
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
    db: ContextKey[ConnectionFactory],
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


def table_target(
    db: ContextKey[ConnectionFactory],
    table_name: str,
    table_schema: TableSchema[RowT] | None = None,
    *,
    primary_key: str = "id",
    managed_by: target.ManagedBy = target.ManagedBy.SYSTEM,
) -> coco.TargetState[_RecordHandler]:
    """Deprecated: use :func:`node_target`. Create a ``TargetState`` for a
    Neo4j node table (label)."""
    _warn_deprecated("table_target", "node_target")
    return _node_table_state(
        _table_provider, db, table_name, table_schema, primary_key, managed_by
    )


def declare_table_target(
    db: ContextKey[ConnectionFactory],
    table_name: str,
    table_schema: TableSchema[RowT] | None = None,
    *,
    primary_key: str = "id",
    managed_by: target.ManagedBy = target.ManagedBy.SYSTEM,
) -> TableTarget[RowT, coco.PendingS]:
    """Deprecated: use :func:`declare_node_target`. Declare a node table
    target."""
    _warn_deprecated("declare_table_target", "declare_node_target")
    provider = coco.declare_target_state_with_child(
        _node_table_state(
            _table_provider, db, table_name, table_schema, primary_key, managed_by
        )
    )
    return TableTarget(provider, table_schema, table_name, primary_key)


async def mount_table_target(
    db: ContextKey[ConnectionFactory],
    table_name: str,
    table_schema: TableSchema[RowT] | None = None,
    *,
    primary_key: str = "id",
    managed_by: target.ManagedBy = target.ManagedBy.SYSTEM,
) -> TableTarget[RowT]:
    """Deprecated: use :func:`mount_node_target`. Mount a node table target
    ready to receive ``declare_record`` calls."""
    _warn_deprecated("mount_table_target", "mount_node_target")
    provider = await coco.mount_target(
        _node_table_state(
            _table_provider, db, table_name, table_schema, primary_key, managed_by
        )
    )
    return TableTarget(provider, table_schema, table_name, primary_key)


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
    """Deprecated: use :func:`relationship_target`. Create a ``TargetState``
    for a Neo4j relationship type."""
    _warn_deprecated("relation_target", "relationship_target")
    return _relationship_table_state(
        _table_provider,
        db,
        table_name,
        from_table,
        to_table,
        table_schema,
        primary_key,
        managed_by,
        replaces_legacy_ids=False,
    )


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
    """Deprecated: use :func:`declare_relationship_target`. Declare a
    relation table target."""
    _warn_deprecated("declare_relation_target", "declare_relationship_target")
    pk = table_schema.primary_key if table_schema is not None else primary_key
    provider = coco.declare_target_state_with_child(
        _relationship_table_state(
            _table_provider,
            db,
            table_name,
            from_table,
            to_table,
            table_schema,
            pk,
            managed_by,
            replaces_legacy_ids=False,
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
    """Deprecated: use :func:`mount_relationship_target`. Mount a relation
    table target ready to receive ``declare_relation`` calls."""
    _warn_deprecated("mount_relation_target", "mount_relationship_target")
    pk = table_schema.primary_key if table_schema is not None else primary_key
    provider = await coco.mount_target(
        _relationship_table_state(
            _table_provider,
            db,
            table_name,
            from_table,
            to_table,
            table_schema,
            pk,
            managed_by,
            replaces_legacy_ids=False,
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
    "NodeTarget",
    "RelationTarget",
    "RelationshipTarget",
    "TableSchema",
    "TableTarget",
    "ValueEncoder",
    "declare_node_target",
    "declare_relation_target",
    "declare_relationship_target",
    "declare_table_target",
    "mount_node_target",
    "mount_relation_target",
    "mount_relationship_target",
    "mount_table_target",
    "node_target",
    "relation_target",
    "relationship_target",
    "table_target",
]
