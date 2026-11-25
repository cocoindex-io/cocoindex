"""
SurrealDB target for CocoIndex.

This module provides a two-level target state system for SurrealDB:
1. Table level: Creates/drops tables in the database (DEFINE TABLE / REMOVE TABLE)
2. Record level: Upserts/deletes records within tables (UPSERT / DELETE / RELATE)

Record-level writes send every value as a typed CBOR parameter, raise on any
statement error, and run in bounded transactions (see ``_SharedRecordApplier``).

Supports both normal tables and relation (graph edge) tables, with optional
schema enforcement (SCHEMAFULL/SCHEMALESS) and vector index support.
"""

from __future__ import annotations

import asyncio
import datetime
import decimal
import random
import re
import threading
import uuid
import weakref
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Collection,
    Generic,
    Literal,
    Mapping,
    NamedTuple,
    Sequence,
    TYPE_CHECKING,
)

from typing_extensions import TypeVar

from cocoindex._internal.extras import install_hint

try:
    import surrealdb as _surrealdb  # type: ignore[import-untyped]
except ImportError as e:
    raise ImportError(
        "surrealdb is required to use the SurrealDB connector. "
        + install_hint("surrealdb")
    ) from e

if TYPE_CHECKING:
    # surrealdb is untyped; use Any so mypy doesn't complain about attribute access.
    AsyncSurreal = Any
    RecordID = Any
else:
    AsyncSurreal = _surrealdb.AsyncSurreal
    RecordID = _surrealdb.RecordID

import numpy as np
from surrealdb.cbor import CBORTag as _CBORTag  # type: ignore[import-untyped]

import cocoindex as coco
from cocoindex.connectorkits import statediff, target
from cocoindex.connectorkits.fingerprint import fingerprint_object
from cocoindex._internal.datatype import (
    MappingType,
    SequenceType,
    RecordType,
    TypeChecker,
    analyze_type_info,
    is_record_type,
)
import msgspec

from cocoindex.resources import schema as res_schema
from cocoindex._internal.context_keys import ContextKey, ContextProvider
from cocoindex.connectorkits._per_loop_connections import PerLoopConnections

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


def _to_record_id(table: str, value: Any) -> Any:
    """Build a typed record ID. A tuple / list ID becomes an array ID (``t:['a', 1]``)."""
    if isinstance(value, (tuple, list)):
        value = [_sanitize(v) for v in value]
    return RecordID(table, value)


def _relation_id_part(value: str) -> str:
    """Length-prefix ``value``, so concatenated parts split back only one way."""
    return f"{len(value)}:{value};"


def _endpoint_id_fragment(value: Any) -> str:
    """Encode an endpoint record ID as ``<type tag>:<value>``, so ``1`` and ``"1"`` differ.

    A tuple and a list name the same array ID (see :func:`_to_record_id`), so they
    encode the same. Other types are tagged with their qualified type name, which
    always contains a ``.`` and so never equals the ``str`` / ``int`` / ``array`` tags.
    """
    if isinstance(value, str):
        return f"str:{value}"
    if isinstance(value, int) and not isinstance(value, bool):
        return f"int:{value}"
    if isinstance(value, (tuple, list)):
        parts = "".join(_relation_id_part(_endpoint_id_fragment(v)) for v in value)
        return f"array:{parts}"
    value_type = type(value)
    return f"{value_type.__module__}.{value_type.__qualname__}:{value}"


def _auto_relation_id(from_table: str, from_id: Any, to_table: str, to_id: Any) -> str:
    """The record ID of a relation declared without an explicit ``id``.

    Each part is length-prefixed and each endpoint ID type-tagged, so distinct
    endpoints never give the same ID.
    """
    return "".join(
        (
            _relation_id_part(from_table),
            _relation_id_part(_endpoint_id_fragment(from_id)),
            _relation_id_part(to_table),
            _relation_id_part(_endpoint_id_fragment(to_id)),
        )
    )


# ---------------------------------------------------------------------------
# Connection factory
# ---------------------------------------------------------------------------


class ConnectionFactory:
    """
    Connection factory for SurrealDB.

    Holds connection parameters and creates authenticated connections on demand,
    one per event loop.

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
        self._connections: PerLoopConnections[_Connection] = PerLoopConnections(
            self._connect, _close_connection, owner="SurrealDB ConnectionFactory"
        )

    def __coco_memo_key__(self) -> tuple[str, str, str]:
        # Where the data lives, not how to sign in: the credentials and the
        # open connections are not part of it.
        return (self._url, self._namespace, self._database)

    async def acquire(self) -> AsyncSurreal:
        """Return the authenticated connection for the current event loop.

        The connection is opened once per event loop (a connection is bound to
        the loop it is used on) and reused by every table and batch on that
        loop (a SurrealDB WebSocket multiplexes concurrent requests).
        """
        return (await self._connection()).conn

    async def _connection(self) -> _Connection:
        """The connection of the current event loop, with what its server can do."""
        # ponytail: one shared connection per loop; add a pool if a single
        # WebSocket becomes the throughput limit.
        return await self._connections.acquire()

    async def _connect(self) -> _Connection:
        conn = AsyncSurreal(self._url)
        await conn.connect()  # type: ignore[call-arg]
        try:
            if self._credentials:
                await conn.signin(self._credentials)  # type: ignore[arg-type]
            await conn.use(self._namespace, self._database)
            recreates = _recreates_in_transaction(await conn.version())
        except BaseException:
            await conn.close()
            raise
        return _Connection(conn, recreates)

    async def close(self) -> None:
        """Close the connection of the running event loop, if any. Call it on
        the loop that used the factory (e.g. in the lifespan, after ``yield``);
        a later ``acquire()`` opens a new connection. Connections of other
        loops can't be closed here; they are left open, with a warning."""
        await self._connections.close()


class _Connection(NamedTuple):
    """An open connection, and its server's answer to `_recreates_in_transaction`,
    read once when it connects."""

    conn: AsyncSurreal
    recreates_in_transaction: bool


async def _close_connection(connection: _Connection) -> None:
    await connection.conn.close()


# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------

_RowKey = tuple[Any, ...]  # (id,) for record mode; (id, group) for fields mode
_ROW_KEY_CHECKER = TypeChecker(tuple[Any, ...])
_RowFingerprint = bytes


class _FieldsTracking(msgspec.Struct, frozen=True, array_like=True):
    """Tracking record of a ``fields``-mode target: fingerprint plus owned field names.

    The names let a delete (or a shrunk field group) ``UNSET`` exactly the fields this
    group owned.
    """

    fp: bytes
    names: tuple[str, ...]


_RowTracking = bytes | _FieldsTracking


class _RelationRowValue(NamedTuple):
    """Value type for relation records, carrying endpoint metadata separately from field data."""

    from_record: tuple[str, Any]  # (table, id), e.g. ("person", "alice")
    to_record: tuple[str, Any]  # (table, id), e.g. ("post", ["p", 1])
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


def _ndarray_encoder(value: Any) -> list[Any]:
    """Convert a NumPy ndarray to a Python list for SurrealDB."""
    if isinstance(value, list):
        return value
    return value.tolist()  # type: ignore[no-any-return]


def _sanitize(value: Any) -> Any:
    """Convert a Python value to one the SurrealDB SDK can encode as CBOR.

    Applied exactly once per value, when the action is built. datetime, Decimal, UUID,
    bytes and RecordID pass through, so they arrive typed.
    """
    if value is None or isinstance(  # type: ignore[misc]
        value, (bool, int, float, str, bytes, decimal.Decimal, uuid.UUID, RecordID)
    ):
        return value
    if isinstance(value, datetime.datetime):
        return value
    if isinstance(value, datetime.date):
        return datetime.datetime.combine(value, datetime.time(), tzinfo=datetime.UTC)
    if isinstance(value, datetime.time):
        # ponytail: SurrealDB has no time-of-day type; stored as an ISO string.
        return value.isoformat()
    if isinstance(value, datetime.timedelta):
        # Raw CBOR duration tag (14): surrealdb SDK 2.0.0 hangs when sent a ``Duration``.
        seconds, rem = divmod(value // datetime.timedelta(microseconds=1), 1_000_000)
        return _CBORTag(_CBOR_TAG_DURATION, [seconds, rem * 1000])
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, Mapping):
        return {str(k): _sanitize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_sanitize(v) for v in value]
    if isinstance(value, msgspec.Struct) or is_record_type(type(value)):
        return _sanitize(msgspec.to_builtins(value, builtin_types=_BUILTIN_TYPES))
    return value


_CBOR_TAG_DURATION = 14

_BUILTIN_TYPES = (
    bytes,
    datetime.datetime,
    datetime.date,
    datetime.time,
    datetime.timedelta,
    decimal.Decimal,
    uuid.UUID,
)


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
    datetime.time: _TypeMapping("string"),
    datetime.datetime: _TypeMapping("datetime"),
    datetime.timedelta: _TypeMapping("duration"),
}

# Complex values are sent as native SurrealDB objects / arrays, not JSON strings
_OBJECT_MAPPING = _TypeMapping("object")
_ARRAY_MAPPING = _TypeMapping("array")
_ANY_MAPPING = _TypeMapping("any")


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

    if isinstance(type_info.variant, SequenceType):
        return _ARRAY_MAPPING
    if isinstance(type_info.variant, (MappingType, RecordType)):
        return _OBJECT_MAPPING

    # Unions, Any and unknown types: no type constraint
    return _ANY_MAPPING


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
    """Action to perform on a record: write fields, unset fields, or delete it."""

    table_name: str
    is_relation: bool
    record_id: Any
    # The fields to write (``UPSERT … MERGE``, or ``RELATE`` for a relation),
    # or None to delete the whole record.
    value: dict[str, Any] | None
    from_record: tuple[str, Any] | None  # (table, id) (relations only)
    to_record: tuple[str, Any] | None  # (table, id) (relations only)
    # ``value`` names the fields an owner stopped declaring, all None: they are
    # unset (``UPDATE … MERGE`` with NONE values; a missing record is fine).
    unset: bool = False
    # With ``unset``, for the record's own declaration: once every write of the
    # batch ran, the record is deleted if nothing but its ``id`` is left, so
    # fields other owners (field groups) declare keep it.
    delete_if_empty: bool = False


_MAX_ACTIONS_PER_TXN = 500
_MAX_ATTEMPTS = 5


def _recreates_in_transaction(version: str) -> bool:
    """Whether the server (``version()``, e.g. ``surrealdb-3.2.4``) can create a
    record deleted earlier in the same transaction: SurrealDB 3.2 cannot (the
    create fails with "already exists"), 3.3 and later can."""
    match = re.search(r"(\d+)\.(\d+)", version)
    return match is None or (int(match[1]), int(match[2])) >= (3, 3)


# The records to delete: ``$dels``, and the records of ``$del_if_empty`` that
# have nothing but their ``id`` left (a missing record answers NONE).
_RECORDS_TO_DELETE = (
    "array::concat($dels, array::filter($del_if_empty, "
    '|$d| (SELECT VALUE object::keys($this) FROM ONLY $d) = ["id"]))'
)

# The edges of the records ``$gone`` names, as they are: their whole records,
# ids and endpoints included.
_EDGES_OF_DELETED = (
    "array::distinct(array::flatten(array::map($gone, "
    "|$d| array::concat($d->?, $d<-?)))).map(|$e| $e.*)"
)


def _batch_surql(recreate_in_transaction: bool) -> str:
    """One transaction of record writes. Values are bound as typed CBOR parameters.

    ``MERGE`` with a NONE value removes the field, so owned fields that became
    None are unset without touching fields written by other owners. Field
    unsets come first, so that a removed group cannot erase a field another
    owner takes over in the same batch. A record whose own declaration went
    away is deleted after every write, and only if nothing but its ``id`` is
    left: a field group declared in the same batch, or still declared, keeps it.

    An edge that exists between the same endpoints is updated in place; an edge
    whose endpoints change is deleted and related again. A deleted record that
    is not an edge keeps its edges: SurrealDB deletes them with it, while an
    edge needs no endpoint record, so they are related again as they were.
    SurrealDB 3.2 cannot create a record deleted earlier in the same
    transaction: there, the batch only deletes, and ends with the edges to
    relate in a second transaction (``_RECREATE_EDGES_SURQL``).
    """
    if recreate_in_transaction:
        relate = """\
FOR $r IN $rels {
    IF (SELECT VALUE [in, out] FROM ONLY $r.id) = [$r.f, $r.t] {
        UPDATE $r.id CONTENT $r.c RETURN NONE;
    } ELSE {
        DELETE $r.id RETURN NONE;
        RELATE ($r.f) -> ($r.id) -> ($r.t) CONTENT $r.c RETURN NONE;
    };
};
"""
        restore = """\
FOR $e IN $edges {
    IF (SELECT VALUE id FROM ONLY $e.id) = NONE {
        RELATE ($e.in) -> ($e.id) -> ($e.out) CONTENT $e RETURN NONE;
    };
};
"""
    else:
        relate = """\
LET $moved = array::filter($rels,
    |$r| (SELECT VALUE [in, out] FROM ONLY $r.id) NOT IN [NONE, [$r.f, $r.t]]);
FOR $r IN $rels {
    LET $cur = (SELECT VALUE [in, out] FROM ONLY $r.id);
    IF $cur = [$r.f, $r.t] { UPDATE $r.id CONTENT $r.c RETURN NONE; }
    ELSE IF $cur = NONE { RELATE ($r.f) -> ($r.id) -> ($r.t) CONTENT $r.c RETURN NONE; }
    ELSE { DELETE $r.id RETURN NONE; };
};
"""
        restore = "LET $restore = $edges;\n"
    return (
        "BEGIN;\n"
        "LET $moved = [];\n"
        "LET $restore = [];\n"
        "FOR $r IN $unf { UPDATE $r.id MERGE $r.c RETURN NONE; };\n"
        "FOR $r IN $ups { UPSERT $r.id MERGE $r.c RETURN NONE; };\n"
        f"{relate}"
        "FOR $d IN $rdels { DELETE $d RETURN NONE; };\n"
        f"LET $gone = {_RECORDS_TO_DELETE};\n"
        f"LET $edges = {_EDGES_OF_DELETED};\n"
        "FOR $d IN $gone { DELETE $d RETURN NONE; };\n"
        f"{restore}"
        "[$moved, $restore];\n"
        "COMMIT;\n"
    )


def _moved_and_restore(res: Any) -> tuple[list[Any], list[Any]]:
    """The ``[$moved, $restore]`` of a `_batch_surql` response.

    It is the last statement that answers a value: every other statement of the
    transaction answers NONE, ``COMMIT`` included, so the value is found
    whether or not the server gives ``BEGIN`` and ``COMMIT`` entries of their own.
    """
    statements = res.get("result") if isinstance(res, dict) else None
    value = None
    if isinstance(statements, list):
        for statement in reversed(statements):
            if not isinstance(statement, dict):
                break
            if (value := statement.get("result")) is not None:
                break
    if not (
        isinstance(value, list)
        and len(value) == 2
        and all(isinstance(v, list) for v in value)
    ):
        raise RuntimeError(
            "SurrealDB answered the batch transaction with an unexpected shape; "
            f"expected [$moved, $restore] as its last value, got: {res!r}"
        )
    return value[0], value[1]


# The second transaction on SurrealDB 3.2: relate each moved edge (``$moved``)
# and each edge of a deleted record (``$restore``), unless another writer
# created it in between. A moved edge another writer created between other
# endpoints fails the batch, and the next run moves it again. Between the two
# transactions the edges are missing; if the second never lands, a moved edge
# is written on the next run, but an edge of a deleted record stays missing
# until its declaration changes or a full reprocess.
_RECREATE_EDGES_SURQL = """\
BEGIN;
FOR $r IN $moved {
    LET $cur = (SELECT VALUE [in, out] FROM ONLY $r.id);
    IF $cur = NONE { RELATE ($r.f) -> ($r.id) -> ($r.t) CONTENT $r.c RETURN NONE; }
    ELSE IF $cur = [$r.f, $r.t] { UPDATE $r.id CONTENT $r.c RETURN NONE; }
    ELSE { THROW 'edge ' + <string>$r.id + ' was written by another writer while it moved'; };
};
FOR $e IN $restore {
    IF (SELECT VALUE id FROM ONLY $e.id) = NONE {
        RELATE ($e.in) -> ($e.id) -> ($e.out) CONTENT $e RETURN NONE;
    };
};
COMMIT;
"""


class _RetryableError(RuntimeError):
    """A transaction conflict: safe to retry the whole chunk."""


def _check_response(res: Any) -> None:
    """Raise if any statement of a ``query_raw`` response failed."""
    if isinstance(res, dict) and res.get("error"):
        err = res["error"]
        msg = err.get("message", err) if isinstance(err, dict) else err
        raise RuntimeError(f"SurrealDB batch failed: {msg}")
    for stmt in res.get("result", []) if isinstance(res, dict) else []:
        if stmt.get("status") == "OK":
            continue
        # Skip the "not executed" echoes that follow the real failure.
        if (stmt.get("details") or {}).get("kind") in ("NotExecuted", "Cancelled"):
            continue
        msg = str(stmt.get("result"))
        if "can be retried" in msg or "conflict" in msg.lower():
            raise _RetryableError(msg)
        raise RuntimeError(f"SurrealDB batch failed: {msg}")


async def _query_with_retry(
    conn: AsyncSurreal, surql: str, params: dict[str, Any]
) -> Any:
    """Run ``surql``, retried when its transaction conflicts; its response.

    The last attempt's conflict is raised.
    """
    for attempt in range(_MAX_ATTEMPTS - 1):
        try:
            return await _query_checked(conn, surql, params)
        except _RetryableError:
            await asyncio.sleep(0.05 * 2**attempt * (1 + random.random()))
    return await _query_checked(conn, surql, params)


async def _query_checked(conn: AsyncSurreal, surql: str, params: dict[str, Any]) -> Any:
    """Run ``surql``; its response, once no statement of it failed."""
    res = await conn.query_raw(surql, params)
    _check_response(res)
    return res


@dataclass(frozen=True)
class _SharedRecordApplier:
    """The record and index sink callback shared by every record handler of one database.

    Equal for the same ``ConnectionFactory``, so the record sinks of all tables
    of one database — node and relation tables alike — and their index sinks
    share one identity (see ``coco.TargetActionSink``) and the engine batches
    their actions together; each batch is ordered by :meth:`_rank` so a
    relation is never written before its endpoints exist. Index changes wrap the
    records: a changed index is removed before the records are written and
    defined after them, since it can be defined neither over the old records (a
    new dimension) nor before the new ones are written.

    Atomicity scope: the engine batches the actions of all components that share
    this sink; they are applied in chunks of ``_MAX_ACTIONS_PER_TXN``, each chunk one
    transaction. A failed chunk rolls back and raises, so the components are
    retried on the next run. Earlier chunks stay committed (writes are idempotent).
    """

    factory: ConnectionFactory

    @staticmethod
    def _rank(action: _RecordAction) -> int:
        # Order: field unsets, upserts (records and field groups), relation
        # upserts, relation deletes, deletes.
        if action.unset:
            return 0
        if action.value is not None:
            return 2 if action.is_relation else 1
        return 3 if action.is_relation else 4

    async def __call__(
        self,
        context_provider: ContextProvider,
        actions: Sequence[_RecordAction | _VectorIndexAction],
    ) -> None:
        if not actions:
            return
        connection = await self.factory._connection()
        conn = connection.conn
        indexes = [a for a in actions if isinstance(a, _VectorIndexAction)]
        records = [a for a in actions if isinstance(a, _RecordAction)]
        for index in indexes:
            _check_response(
                await conn.query_raw(
                    f"REMOVE INDEX IF EXISTS {index.name} ON TABLE {index.table_name}"
                )
            )
        ordered = sorted(records, key=self._rank)  # stable
        for start in range(0, len(ordered), _MAX_ACTIONS_PER_TXN):
            await self._apply_chunk(
                connection, ordered[start : start + _MAX_ACTIONS_PER_TXN]
            )
        for index in indexes:
            if index.spec is not None:
                _check_response(
                    await conn.query_raw(_define_vector_index(index.name, index))
                )

    async def _apply_chunk(
        self, connection: _Connection, chunk: Sequence[_RecordAction]
    ) -> None:
        ups: list[dict[str, Any]] = []
        unf: list[dict[str, Any]] = []
        del_if_empty: list[Any] = []
        rels: list[dict[str, Any]] = []
        rdels: list[Any] = []
        dels: list[Any] = []
        for a in chunk:
            rid = _to_record_id(a.table_name, a.record_id)
            if a.value is None:
                (rdels if a.is_relation else dels).append(rid)
                continue
            content = {k: _sanitize(v) for k, v in a.value.items() if k != "id"}
            if a.unset:
                unf.append({"id": rid, "c": content})
                if a.delete_if_empty:
                    del_if_empty.append(rid)
            elif a.is_relation:
                assert a.from_record is not None and a.to_record is not None
                f = _to_record_id(*a.from_record)
                t = _to_record_id(*a.to_record)
                # The content carries the endpoints, so that `UPDATE … CONTENT`
                # keeps them.
                rels.append(
                    {"id": rid, "f": f, "t": t, "c": {**content, "in": f, "out": t}}
                )
            else:
                ups.append({"id": rid, "c": content})
        params = {
            "ups": ups,
            "unf": unf,
            "del_if_empty": del_if_empty,
            "rels": rels,
            "rdels": rdels,
            "dels": dels,
        }
        conn = connection.conn
        recreate_in_transaction = connection.recreates_in_transaction
        res = await _query_with_retry(
            conn, _batch_surql(recreate_in_transaction), params
        )
        if recreate_in_transaction:
            return
        moved, restore = _moved_and_restore(res)
        if moved or restore:
            await _query_with_retry(
                conn, _RECREATE_EDGES_SURQL, {"moved": moved, "restore": restore}
            )


# ---------------------------------------------------------------------------
# Vector index types and handler
# ---------------------------------------------------------------------------


class _VectorIndexSpec(NamedTuple):
    """Specification for a vector index on a SurrealDB table."""

    field: str
    metric: str  # "cosine", "euclidean", "manhattan"
    method: str  # "hnsw"
    dimension: int
    vector_type: str  # "f32", "f64", "i16", "i32", "i64"


_VectorIndexFingerprint = bytes


class _VectorIndexAction(NamedTuple):
    """Action to create or remove a vector index."""

    name: str
    table_name: str
    spec: _VectorIndexSpec | None  # None = delete


def _define_vector_index(name: str, action: _VectorIndexAction) -> str:
    assert action.spec is not None
    return (
        f"DEFINE INDEX OVERWRITE {name} ON {action.table_name} "
        f"FIELDS {action.spec.field} "
        f"{action.spec.method.upper()} DIMENSION {action.spec.dimension} "
        f"DIST {action.spec.metric.upper()} TYPE {action.spec.vector_type.upper()}"
    )


class _VectorIndexHandler:
    """Attachment handler for vector indexes on a SurrealDB table.

    Its actions go to the table's record sink (see ``_SharedRecordApplier``),
    which orders them around the records.
    """

    _table_name: str
    _sink: coco.TargetActionSink[_RecordAction | _VectorIndexAction]

    def __init__(self, factory: ConnectionFactory, table_name: str) -> None:
        self._table_name = table_name
        self._sink = coco.TargetActionSink.from_async_fn(_SharedRecordApplier(factory))

    def reconcile(
        self,
        key: coco.StableKey,
        desired_state: _VectorIndexSpec | coco.NonExistenceType,
        prev_possible_records: Collection[_VectorIndexFingerprint],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[_VectorIndexAction, _VectorIndexFingerprint] | None:
        assert isinstance(key, str)
        if coco.is_non_existence(desired_state):
            if not prev_possible_records and not prev_may_be_missing:
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
            prev == target_fp for prev in prev_possible_records
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


def _owned_names(prev_possible_records: Collection[_RowTracking]) -> set[str]:
    """Field names any previous run may have written (legacy bytes records: none)."""
    names: set[str] = set()
    for prev in prev_possible_records:
        if isinstance(prev, _FieldsTracking):
            names.update(prev.names)
    return names


class _RecordHandler(coco.TargetHandler[_RowValue, _RowTracking]):
    """Handler for record-level target states within a SurrealDB table.

    Built by the table sink's ``child_handler`` factory, so it holds no
    connection: its sinks acquire one when they apply.
    """

    _table_name: str
    _is_relation: bool
    _table_schema: TableSchema[Any] | None
    _factory: ConnectionFactory
    _db_key: str
    _sink: coco.TargetActionSink[_RecordAction]

    def __init__(
        self,
        table_name: str,
        is_relation: bool,
        table_schema: TableSchema[Any] | None,
        factory: ConnectionFactory,
        db_key: str,
    ) -> None:
        self._db_key = db_key
        self._table_name = table_name
        self._is_relation = is_relation
        self._table_schema = table_schema
        self._factory = factory
        self._sink = coco.TargetActionSink.from_async_fn(_SharedRecordApplier(factory))

    def attachments(self) -> dict[str, _VectorIndexHandler]:
        return {"vector_index": _VectorIndexHandler(self._factory, self._table_name)}

    def reconcile(
        self,
        key: coco.StableKey,
        desired_state: _RowValue | coco.NonExistenceType,
        prev_possible_records: Collection[_RowTracking],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[_RecordAction, _RowTracking] | None:
        key = _ROW_KEY_CHECKER.check(key)
        if len(key) == 2:
            return self._reconcile_fields(
                key, desired_state, prev_possible_records, prev_may_be_missing
            )

        if coco.is_non_existence(desired_state):
            if not self._is_relation:
                _release_field_owner(self._db_key, self._table_name, key[0], None)
            if not prev_possible_records and not prev_may_be_missing:
                return None
            return coco.TargetReconcileOutput(
                action=self._undeclared_record_action(key[0], prev_possible_records),
                sink=self._sink,
                tracking_record=coco.NON_EXISTENCE,
            )

        # Column encoders already ran once, when the row was declared.
        if isinstance(desired_state, _RelationRowValue):
            # Relations are single-owner: DELETE + RELATE replaces the whole edge.
            target_fp = fingerprint_object(desired_state)
            if not prev_may_be_missing and all(
                prev == target_fp for prev in prev_possible_records
            ):
                return None
            return coco.TargetReconcileOutput(
                action=_RecordAction(
                    table_name=self._table_name,
                    is_relation=True,
                    record_id=key[0],
                    value=desired_state.fields,
                    from_record=desired_state.from_record,
                    to_record=desired_state.to_record,
                ),
                sink=self._sink,
                tracking_record=target_fp,
            )
        return self._merge_output(
            key, desired_state, prev_possible_records, prev_may_be_missing
        )

    def _undeclared_record_action(
        self, record_id: Any, prev_possible_records: Collection[_RowTracking]
    ) -> _RecordAction:
        """The record's own declaration went away.

        A relation, which has one owner, is deleted. A record's existence has
        two owners, its own declaration and its field groups (``declare_fields``
        creates the record too): the fields it declared are unset, and the record
        is deleted only if nothing but its ``id`` is left. A record is deleted
        whole when the fields it declared are not known (tracked by an earlier
        version), or when one of them is a required field of the table's schema,
        which the record cannot be without.
        """
        names = _owned_names(prev_possible_records)
        columns = self._table_schema.columns if self._table_schema else {}
        if (
            self._is_relation
            or any(
                not isinstance(prev, _FieldsTracking) for prev in prev_possible_records
            )
            or any(
                (col := columns.get(name)) is not None and not col.nullable
                for name in names
                if name != "id"
            )
        ):
            return _RecordAction(
                table_name=self._table_name,
                is_relation=self._is_relation,
                record_id=record_id,
                value=None,
                from_record=None,
                to_record=None,
            )
        return _RecordAction(
            table_name=self._table_name,
            is_relation=False,
            record_id=record_id,
            value=dict.fromkeys(sorted(names)),
            from_record=None,
            to_record=None,
            unset=True,
            delete_if_empty=True,
        )

    def _merge_output(
        self,
        key: _RowKey,
        desired: dict[str, Any],
        prev_possible_records: Collection[_RowTracking],
        prev_may_be_missing: bool,
    ) -> coco.TargetReconcileOutput[_RecordAction, _RowTracking] | None:
        """Record mode and ``fields`` mode alike: ``UPSERT … MERGE`` of the
        declared fields, which creates the record when it is missing, and unsets
        the fields declared before and dropped since. Never replaces the whole
        record, so fields other owners declare stay.
        """
        names = tuple(sorted(desired))
        tracking = _FieldsTracking(fingerprint_object(desired), names)
        if not prev_may_be_missing and all(
            prev == tracking for prev in prev_possible_records
        ):
            return None
        # Fields owned before but no longer declared become NONE.
        prev_names = _owned_names(prev_possible_records)
        value = {**dict.fromkeys(sorted(prev_names - set(names))), **desired}
        return coco.TargetReconcileOutput(
            action=_RecordAction(
                table_name=self._table_name,
                is_relation=False,
                record_id=key[0],
                value=value,
                from_record=None,
                to_record=None,
            ),
            sink=self._sink,
            tracking_record=tracking,
        )

    def _reconcile_fields(
        self,
        key: _RowKey,
        desired_state: Any,
        prev_possible_records: Collection[_RowTracking],
        prev_may_be_missing: bool,
    ) -> coco.TargetReconcileOutput[_RecordAction, _RowTracking] | None:
        """``fields`` mode: key ``(id, group)``; MERGE this group's fields onto the record.

        When the group goes away, its fields are unset and the record stays,
        also when nothing but its ``id`` is left: the group cannot tell whether
        the record's own declaration, which may name no field but ``id``,
        still declares it.
        """
        if coco.is_non_existence(desired_state):
            _release_field_owner(self._db_key, self._table_name, key[0], key[1])
            if not prev_possible_records and not prev_may_be_missing:
                return None
            return coco.TargetReconcileOutput(
                action=_RecordAction(
                    table_name=self._table_name,
                    is_relation=False,
                    record_id=key[0],
                    value=dict.fromkeys(sorted(_owned_names(prev_possible_records))),
                    from_record=None,
                    to_record=None,
                    unset=True,
                ),
                sink=self._sink,
                tracking_record=coco.NON_EXISTENCE,
            )
        return self._merge_output(
            key, desired_state, prev_possible_records, prev_may_be_missing
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
    managed_by: target.ManagedBy = target.ManagedBy.SYSTEM


class _TableMainRecord(msgspec.Struct, frozen=True):
    """Main tracking record for table-level properties requiring DROP+CREATE if changed."""

    has_schema: bool
    is_relation: bool
    id_type: str | None  # SurrealDB type of the id field, or None for schemaless
    from_tables: tuple[str, ...] | None
    to_tables: tuple[str, ...] | None


class _FieldTrackingRecord(msgspec.Struct, frozen=True):
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
            table_schema=spec.table_schema,
            factory=context_provider.get(action.key.db_key, ConnectionFactory),
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

        # "upsert" means the table may or may not already exist: the DDL can land on
        # a table carrying the previous column set, so its columns still need
        # reconciling. "insert" / "replace" define the table from the desired
        # schema, so there is nothing left to reconcile.
        column_actions: dict[str, statediff.DiffAction] = {}
        if main_action is None or main_action == "upsert":
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
        elif any(a != "insert" for a in column_actions.values()):
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
        self,
        context_provider: ContextProvider,
        actions: Sequence[_TableAction],
        /,
    ) -> None:
        # Group by db_key
        by_db: dict[str, list[_TableAction]] = {}
        for action in actions:
            by_db.setdefault(action.key.db_key, []).append(action)

        for db_key, db_actions in by_db.items():
            conn = await context_provider.get(db_key, ConnectionFactory).acquire()

            # Sort into DDL ordering
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
                if action.main_action in ("replace", "delete"):
                    await conn.query(f"REMOVE TABLE IF EXISTS {action.key.table_name}")

                if coco.is_non_existence(action.spec):
                    continue

                spec = action.spec

                if action.main_action in ("insert", "upsert", "replace"):
                    await self._create_table(conn, action.key, spec)
                # "insert" / "replace" just defined the table from the desired
                # schema. "upsert" may have landed on a pre-existing table, whose
                # dropped fields `_create_table` does not remove — so reconcile
                # columns for it as well as for the no-main-change case.
                if action.main_action in (None, "upsert") and action.column_actions:
                    await self._apply_column_actions(
                        conn, action.key, spec, action.column_actions
                    )

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
                f"DEFINE TABLE OVERWRITE {key.table_name} TYPE RELATION "
                f"FROM {from_clause} TO {to_clause} {schema_mode}"
            )
        else:
            surql = f"DEFINE TABLE OVERWRITE {key.table_name} {schema_mode}"

        await conn.query(surql)

        if schema is not None:
            for col_name, col_def in schema.columns.items():
                if col_name == "id":
                    continue
                type_expr = col_def.type
                if col_def.nullable:
                    type_expr = f"option<{type_expr}>"
                await conn.query(
                    f"DEFINE FIELD OVERWRITE {col_name} ON {key.table_name} TYPE {type_expr}"
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
                    f"DEFINE FIELD OVERWRITE {col_name} ON {key.table_name} TYPE {type_expr}"
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


class _FieldOwners:
    """The owners of the fields declared through one ``TableTarget``, so that a
    field declared by two owners (the record itself, ``None``, or a field group)
    is refused when it is declared.

    A claim lasts while its owner declares it: declaring again replaces it, and
    the engine's removal of the owner's declaration (see `_release_field_owner`)
    releases it. An update mounts the table, and with it a new target, so its
    claims are those of one run; in live mode the target outlives cycles, and an
    owner that stops declaring a field in one cycle leaves it free for another
    owner in the next. A claim's removal is seen only once it is reconciled, so
    a field that moves between owners within one cycle may be refused there.

    Every record declared through the target is in it, so it is kept small: a
    record's own fields as one of the few distinct field-name sets of the table,
    shared between records. Both maps are keyed by the record id's encoding
    (`_endpoint_id_fragment`), so ``1`` and ``"1"`` stay apart.
    """

    def __init__(self, table_name: str) -> None:
        self._table_name = table_name
        self._lock = threading.Lock()
        self._record_fields: dict[str, frozenset[str]] = {}
        self._field_sets: dict[frozenset[str], frozenset[str]] = {}
        self._groups: dict[str, dict[str, frozenset[str]]] = {}

    def claim(
        self, record_id: Any, group: str | None, fields: Mapping[str, Any]
    ) -> None:
        names = frozenset(name for name in fields if name != "id")
        key = _endpoint_id_fragment(record_id)
        with self._lock:
            others: list[tuple[str, frozenset[str]]] = []
            if group is not None and (own := self._record_fields.get(key)):
                others.append(("the record's own declaration", own))
            for other, other_names in self._groups.get(key, {}).items():
                if other != group:
                    others.append((f"field group {other!r}", other_names))
            for other_owner, other_names in others:
                if shared := sorted(names & other_names):
                    owner = (
                        "the record's own declaration"
                        if group is None
                        else f"field group {group!r}"
                    )
                    raise ValueError(
                        f"SurrealDB field {shared[0]!r} of record {record_id!r} of "
                        f"table {self._table_name!r} is declared by both "
                        f"{other_owner} and {owner}: a field can have only one "
                        "owner, since it is unset when either stops declaring it"
                    )
            if group is None:
                self._record_fields[key] = self._field_sets.setdefault(names, names)
            else:
                self._groups.setdefault(key, {})[group] = names

    def release(self, record_id: Any, group: str | None) -> None:
        """The owner ``group`` (``None``: the record itself) no longer declares
        the record."""
        key = _endpoint_id_fragment(record_id)
        with self._lock:
            if group is None:
                self._record_fields.pop(key, None)
            elif (groups := self._groups.get(key)) is not None:
                groups.pop(group, None)
                if not groups:
                    del self._groups[key]


# The `_FieldOwners` of the live table targets, by (database key, table name).
_field_owners_of_table: dict[tuple[str, str], weakref.WeakSet[_FieldOwners]] = {}
_field_owners_lock = threading.Lock()


def _register_field_owners(db_key: str, table_name: str, owners: _FieldOwners) -> None:
    with _field_owners_lock:
        _field_owners_of_table.setdefault((db_key, table_name), weakref.WeakSet()).add(
            owners
        )


def _release_field_owner(
    db_key: str, table_name: str, record_id: Any, group: str | None
) -> None:
    """Release the claim of ``group`` on ``record_id`` in every live target of the
    table: the engine is removing that owner's declaration. A target that a
    claim of its own was not released from keeps refusing; one released by
    another app's removal misses a conflict, never refuses a valid field."""
    with _field_owners_lock:
        targets = list(_field_owners_of_table.get((db_key, table_name), ()))
    for owners in targets:
        owners.release(record_id, group)


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
        self._field_owners = _FieldOwners(table_name)

    @property
    def table_name(self) -> str:
        return self._table_name

    def declare_record(self: TableTarget[RowT], *, row: RowT) -> None:
        """Declare a record to be upserted to this table.

        Writes ``UPSERT $id MERGE`` with the record's fields, so fields that field
        groups (``declare_fields``) or writers outside CocoIndex add stay. When the
        declaration goes away, its fields are unset, and the record is deleted
        only if nothing but its ``id`` is left; a record that declared a required
        (non-nullable) field of the table's schema cannot be without it, and is
        deleted whole.
        """
        row_dict = self._row_to_dict(row)
        pk_values = (row_dict["id"],)
        self._field_owners.claim(row_dict["id"], None, row_dict)
        coco.declare_target_state(self._provider.target_state(pk_values, row_dict))

    declare_row = declare_record

    def declare_fields(
        self: TableTarget[RowT], *, id: Any, group: str, fields: dict[str, Any]
    ) -> None:
        """Declare the fields one owner (``group``) contributes to a record whose
        other fields other owners declare.

        ``fields`` mode: writes ``UPSERT $id MERGE`` with only these fields, so fields
        written by other owners (or by ``declare_record``) are never touched, and the
        record is created when it does not exist yet: a field group lands in the run
        that declares it, also when the record itself is declared by the parent
        component, which commits after its children. When the declaration goes away,
        or a field is dropped from it, those fields are unset; the record stays,
        also with nothing but its ``id`` left.

        A field has one owner: declaring a field of a record that the record itself,
        or another group, declares through this target raises ``ValueError``, since
        the field would be unset when either owner stops declaring it. A field an
        owner stopped declaring in an earlier run, or an earlier live cycle, is
        free.
        """
        columns = self._table_schema.columns if self._table_schema else {}
        encoded: dict[str, Any] = {}
        for name, value in fields.items():
            _validate_identifier(name, "field name")
            col = columns.get(name)
            if col is not None and col.encoder is not None and value is not None:
                value = col.encoder(value)
            encoded[name] = value
        self._field_owners.claim(id, group, encoded)
        coco.declare_target_state(self._provider.target_state((id, group), encoded))

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
        method: Literal["hnsw"] = "hnsw",
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
            record_id = _auto_relation_id(
                from_table_name, from_id, to_table_name, to_id
            )

        # Wrap in _RelationRowValue
        row_value: _RowValue = _RelationRowValue(
            from_record=(from_table_name, from_id),
            to_record=(to_table_name, to_id),
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
    managed_by: target.ManagedBy = target.ManagedBy.SYSTEM,
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
    managed_by: target.ManagedBy = target.ManagedBy.SYSTEM,
) -> TableTarget[RowT, coco.PendingS]:
    """Declare a table target and return a pending TableTarget."""
    provider = coco.declare_target_state_with_child(
        table_target(db, table_name, table_schema, managed_by=managed_by)
    )
    return _registered_table_target(db, provider, table_schema, table_name)


async def mount_table_target(
    db: ContextKey[ConnectionFactory],
    table_name: str,
    table_schema: TableSchema[RowT] | None = None,
    *,
    managed_by: target.ManagedBy = target.ManagedBy.SYSTEM,
) -> TableTarget[RowT]:
    """Mount a table target and return a ready-to-use TableTarget."""
    provider = await coco.mount_target(
        table_target(db, table_name, table_schema, managed_by=managed_by)
    )
    return _registered_table_target(db, provider, table_schema, table_name)


def _registered_table_target(
    db: ContextKey[ConnectionFactory],
    provider: coco.TargetStateProvider[_RowValue, None, coco.MaybePendingS],
    table_schema: TableSchema[RowT] | None,
    table_name: str,
) -> TableTarget[RowT, coco.MaybePendingS]:
    """A new `TableTarget`, whose field claims the record handler releases."""
    table = TableTarget(provider, table_schema, table_name)
    _register_field_owners(db.key, table_name, table._field_owners)
    return table


def relation_target(
    db: ContextKey[ConnectionFactory],
    table_name: str,
    from_table: TableTarget[Any] | Collection[TableTarget[Any]],
    to_table: TableTarget[Any] | Collection[TableTarget[Any]],
    table_schema: TableSchema[RowT] | None = None,
    *,
    managed_by: target.ManagedBy = target.ManagedBy.SYSTEM,
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
    managed_by: target.ManagedBy = target.ManagedBy.SYSTEM,
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
    managed_by: target.ManagedBy = target.ManagedBy.SYSTEM,
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
