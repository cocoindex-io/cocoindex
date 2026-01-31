"""
SQLite target for CocoIndex.

This module provides a two-level target state system for SQLite:
1. Table level: Creates/drops tables in the database
2. Row level: Upserts/deletes rows within tables

Vector support is provided via the sqlite-vec extension.
"""

from __future__ import annotations

import datetime
import decimal
import json
import sqlite3
import threading
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from contextlib import contextmanager
from collections.abc import Set
from typing import (
    Any,
    Callable,
    Collection,
    Generic,
    Iterator,
    Literal,
    NamedTuple,
    Sequence,
    overload,
)

from typing_extensions import TypeVar
import numpy as np

import cocoindex as coco
from cocoindex.connectorkits import connection, statediff
from cocoindex.connectorkits.fingerprint import fingerprint_object
from cocoindex._internal.datatype import (
    AnyType,
    MappingType,
    SequenceType,
    RecordType,
    UnionType,
    analyze_type_info,
    is_record_type,
)
from cocoindex.resources.schema import VectorSchemaProvider

# Type aliases
_RowKey = tuple[Any, ...]  # Primary key values as tuple
_RowValue = dict[str, Any]  # Column name -> value
_RowFingerprint = bytes
ValueEncoder = Callable[[Any], Any]


_VEC_EXTENSION = "sqlite-vec"


@dataclass
class ManagedConnection:
    """
    A SQLite connection with thread-safe access and extension tracking.

    The connection uses autocommit mode (isolation_level=None). Use `transaction()`
    for write operations that need atomic commits, or `readonly()` for read-only
    operations that don't need transaction management.
    """

    _conn: sqlite3.Connection
    _lock: threading.Lock = field(default_factory=threading.Lock)
    _loaded_extensions: set[str] = field(default_factory=set)

    @property
    def loaded_extensions(self) -> Set[str]:
        """Return the set of loaded extensions (read-only view)."""
        return self._loaded_extensions

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """
        Acquire lock and execute within a transaction (BEGIN...COMMIT/ROLLBACK).

        Use for write operations that should be atomic.
        """
        with self._lock:
            self._conn.execute("BEGIN")
            try:
                yield self._conn
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    @contextmanager
    def readonly(self) -> Iterator[sqlite3.Connection]:
        """
        Acquire lock for read-only operations.

        No transaction is started since the connection uses autocommit mode.
        """
        with self._lock:
            yield self._conn

    def close(self) -> None:
        """Close the underlying connection."""
        self._conn.close()


@dataclass
class Vec0TableDef:
    """
    Configuration for vec0 virtual tables (sqlite-vec).

    Vec0 virtual tables provide optimized vector similarity search with SIMD acceleration.
    Use this for KNN queries on larger datasets. For small datasets or simple vector storage,
    regular tables with float[N] columns are sufficient.

    Attributes:
        partition_key_columns: Column names to use as partition keys for sharding the index.
        auxiliary_columns: Column names to mark as auxiliary (+prefix in vec0 DDL).
            Auxiliary columns store additional data but cannot be used in KNN WHERE filters.
    """

    partition_key_columns: list[str] = field(default_factory=list)
    auxiliary_columns: list[str] = field(default_factory=list)

    @property
    def module_name(self) -> str:
        """Return the SQLite virtual table module name."""
        return "vec0"


# SQLite has a limit of 999 variables per query (SQLITE_MAX_VARIABLE_NUMBER)
_BIND_LIMIT: int = 999


def _qualified_table_name(table_name: str) -> str:
    """Return a properly quoted table name."""
    # SQLite uses double quotes for identifiers
    return f'"{table_name}"'


class SqliteType(NamedTuple):
    """
    Annotation to specify a SQLite column type.

    Use with `typing.Annotated` to override the default type mapping:

    ```python
    from typing import Annotated
    from dataclasses import dataclass
    from cocoindex.connectors.sqlite import SqliteType

    @dataclass
    class MyRow:
        # Use INTEGER instead of default
        id: Annotated[int, SqliteType("INTEGER")]
        # Use REAL instead of default
        value: Annotated[float, SqliteType("REAL")]
    ```
    """

    sqlite_type: str
    encoder: ValueEncoder | None = None


class _TypeMapping(NamedTuple):
    """Mapping from Python type to SQLite type with optional encoder."""

    sqlite_type: str
    encoder: ValueEncoder | None = None


# Global mapping for leaf types
# SQLite has limited types: NULL, INTEGER, REAL, TEXT, BLOB
# We map Python types to the closest SQLite affinity
_LEAF_TYPE_MAPPINGS: dict[type, _TypeMapping] = {
    # Boolean (SQLite stores as INTEGER 0/1)
    bool: _TypeMapping("INTEGER"),
    # Numeric types
    int: _TypeMapping("INTEGER"),
    float: _TypeMapping("REAL"),
    decimal.Decimal: _TypeMapping("TEXT", str),
    # String types
    str: _TypeMapping("TEXT"),
    bytes: _TypeMapping("BLOB"),
    # UUID (stored as TEXT)
    uuid.UUID: _TypeMapping("TEXT", str),
    # Date/time types (stored as TEXT in ISO format)
    datetime.date: _TypeMapping("TEXT", lambda v: v.isoformat()),
    datetime.time: _TypeMapping("TEXT", lambda v: v.isoformat()),
    datetime.datetime: _TypeMapping("TEXT", lambda v: v.isoformat()),
    datetime.timedelta: _TypeMapping("REAL", lambda v: v.total_seconds()),
}

# Default mapping for complex types that need JSON encoding
_JSON_MAPPING = _TypeMapping("TEXT", lambda v: json.dumps(v, default=str))


def _get_type_mapping(
    python_type: Any, *, vector_schema_provider: VectorSchemaProvider | None = None
) -> _TypeMapping:
    """
    Get the SQLite type mapping for a Python type.

    SQLite has dynamic typing with type affinities:
    - INTEGER: for int, bool
    - REAL: for float
    - TEXT: for str, datetime, uuid, json
    - BLOB: for bytes, vectors (via sqlite-vec)

    Use `SqliteType` annotation with `typing.Annotated` to override the default.
    """
    type_info = analyze_type_info(python_type)

    # Check for SqliteType annotation override
    for annotation in type_info.annotations:
        if isinstance(annotation, SqliteType):
            return _TypeMapping(annotation.sqlite_type, annotation.encoder)

    base_type = type_info.base_type

    # Check direct leaf type mappings
    if base_type in _LEAF_TYPE_MAPPINGS:
        return _LEAF_TYPE_MAPPINGS[base_type]

    # NumPy ndarray: serialize to sqlite-vec compatible format
    if base_type is np.ndarray:
        if vector_schema_provider is None:
            raise ValueError("VectorSchemaProvider is required for NumPy ndarray type.")
        vector_schema = vector_schema_provider.__coco_vector_schema__()

        if vector_schema.size <= 0:
            raise ValueError(f"Invalid vector dimension: {vector_schema.size}")

        # sqlite-vec uses float[N] type (e.g., float[384])
        import sqlite_vec  # type: ignore

        return _TypeMapping(
            f"float[{vector_schema.size}]", sqlite_vec.serialize_float32
        )

    elif vector_schema_provider is not None:
        raise ValueError(
            f"VectorSchemaProvider is only supported for NumPy ndarray type. Got type: {python_type}"
        )

    # Complex types that need JSON encoding
    if isinstance(
        type_info.variant, (SequenceType, MappingType, RecordType, UnionType, AnyType)
    ):
        return _JSON_MAPPING

    # Default fallback
    return _JSON_MAPPING


class ColumnDef(NamedTuple):
    """Definition of a table column."""

    type: str  # SQLite type (e.g., "TEXT", "INTEGER", "REAL", "BLOB")
    nullable: bool = True
    encoder: ValueEncoder | None = (
        None  # Optional encoder to convert value before sending to SQLite
    )
    is_vector: bool = False  # Whether this column stores vector data


# Type variable for row type
RowT = TypeVar("RowT", default=dict[str, Any])


@dataclass(slots=True)
class TableSchema(Generic[RowT]):
    """Schema definition for a SQLite table."""

    columns: dict[str, ColumnDef]  # column name -> definition
    primary_key: list[str]  # Column names that form the primary key
    row_type: type[RowT] | None  # The row type, if provided

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
        column_overrides: dict[str, SqliteType | VectorSchemaProvider] | None = None,
    ) -> None: ...

    def __init__(
        self,
        columns: type[RowT] | dict[str, ColumnDef],
        primary_key: list[str],
        *,
        column_overrides: dict[str, SqliteType | VectorSchemaProvider] | None = None,
    ) -> None:
        """
        Create a TableSchema.

        Args:
            columns: Either a record type (dataclass, NamedTuple, or Pydantic model)
                     or a dict mapping column names to ColumnDef.
                     When a record type is provided, Python types are automatically
                     mapped to SQLite types.
            primary_key: List of column names that form the primary key.
            column_overrides: Optional dict mapping column names to SqliteType or
                              VectorSchemaProvider to override the default type mapping.
        """
        if isinstance(columns, dict):
            self.columns = columns
            self.row_type = None
        elif is_record_type(columns):
            self.columns = self._columns_from_record_type(columns, column_overrides)
            self.row_type = columns
        else:
            raise TypeError(
                f"columns must be a record type (dataclass, NamedTuple, Pydantic model) "
                f"or a dict[str, ColumnDef], got {type(columns)}"
            )

        self.primary_key = primary_key

        # Validate primary key columns exist
        for pk in self.primary_key:
            if pk not in self.columns:
                raise ValueError(
                    f"Primary key column '{pk}' not found in columns: {list(self.columns.keys())}"
                )

    @staticmethod
    def _columns_from_record_type(
        record_type: type,
        column_overrides: dict[str, SqliteType | VectorSchemaProvider] | None,
    ) -> dict[str, ColumnDef]:
        """Convert a record type to a dict of column name -> ColumnDef."""
        record_info = RecordType(record_type)
        columns: dict[str, ColumnDef] = {}

        for field in record_info.fields:
            override = column_overrides.get(field.name) if column_overrides else None
            type_info = analyze_type_info(field.type_hint)

            # Extract SqliteType and VectorSchemaProvider from annotations
            sqlite_type_annotation: SqliteType | None = None
            vector_schema_provider: VectorSchemaProvider | None = None
            for annotation in type_info.annotations:
                if isinstance(annotation, SqliteType):
                    sqlite_type_annotation = annotation
                elif isinstance(annotation, VectorSchemaProvider):
                    vector_schema_provider = annotation

            # Override takes precedence over annotation
            if isinstance(override, SqliteType):
                sqlite_type_annotation = override
            elif isinstance(override, VectorSchemaProvider):
                vector_schema_provider = override

            # Determine type mapping
            if sqlite_type_annotation is not None:
                type_mapping = _TypeMapping(
                    sqlite_type_annotation.sqlite_type, sqlite_type_annotation.encoder
                )
            else:
                type_mapping = _get_type_mapping(
                    field.type_hint, vector_schema_provider=vector_schema_provider
                )

            columns[field.name] = ColumnDef(
                type=type_mapping.sqlite_type.strip(),
                nullable=type_info.nullable,
                encoder=type_mapping.encoder,
                is_vector=(vector_schema_provider is not None),
            )

        return columns


class _RowAction(NamedTuple):
    """Action to perform on a row."""

    key: _RowKey
    value: _RowValue | None  # None means delete


class _RowHandler(coco.TargetHandler[_RowKey, _RowValue, _RowFingerprint]):
    """Handler for row-level target states within a table."""

    _managed_conn: ManagedConnection
    _table_name: str
    _table_schema: TableSchema[Any]
    _is_virtual_table: bool
    _sink: coco.TargetActionSink[_RowAction, None]

    def __init__(
        self,
        managed_conn: ManagedConnection,
        table_name: str,
        table_schema: TableSchema[Any],
        is_virtual_table: bool = False,
    ) -> None:
        self._managed_conn = managed_conn
        self._table_name = table_name
        self._table_schema = table_schema
        self._is_virtual_table = is_virtual_table
        self._sink = coco.TargetActionSink[_RowAction, None].from_fn(
            self._apply_actions
        )

    def _apply_actions(self, actions: Sequence[_RowAction]) -> None:
        """Apply row actions (upserts and deletes) to the database."""

        if not actions:
            return

        upserts: list[_RowAction] = []
        deletes: list[_RowAction] = []

        for action in actions:
            if action.value is None:
                deletes.append(action)
            else:
                upserts.append(action)

        with self._managed_conn.transaction() as conn:
            # Process upserts
            if upserts:
                self._execute_upserts(conn, upserts)

            # Process deletes
            if deletes:
                self._execute_deletes(conn, deletes)

    def _execute_upserts(
        self, conn: sqlite3.Connection, upserts: list[_RowAction]
    ) -> None:
        """Execute upsert operations."""
        table_name = _qualified_table_name(self._table_name)
        columns = self._table_schema.columns
        pk_cols = self._table_schema.primary_key
        all_col_names = list(columns.keys())
        non_pk_cols = [c for c in all_col_names if c not in pk_cols]

        # Build column lists
        col_list = ", ".join(f'"{c}"' for c in all_col_names)

        num_parameters = len(all_col_names)
        if num_parameters == 0:
            return

        # Virtual tables (like vec0) don't support standard UPSERT syntax
        # Use DELETE + INSERT for each row
        if self._is_virtual_table:
            pk_list = ", ".join(f'"{c}"' for c in pk_cols)
            pk_placeholders = " AND ".join(f'"{c}" = ?' for c in pk_cols)
            delete_sql = f"DELETE FROM {table_name} WHERE {pk_placeholders}"

            placeholders = ", ".join("?" for _ in range(num_parameters))
            insert_sql = (
                f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})"
            )

            for action in upserts:
                assert action.value is not None
                # Delete existing row if it exists
                pk_params = [action.value.get(pk_col) for pk_col in pk_cols]
                conn.execute(delete_sql, pk_params)
                # Insert new row
                row_params = [action.value.get(col_name) for col_name in all_col_names]
                conn.execute(insert_sql, row_params)
        else:
            # Build ON CONFLICT clause for regular tables
            insert_clause = "INSERT"
            pk_list = ", ".join(f'"{c}"' for c in pk_cols)
            if non_pk_cols:
                update_list = ", ".join(f'"{c}" = excluded."{c}"' for c in non_pk_cols)
                conflict_clause = f"ON CONFLICT ({pk_list}) DO UPDATE SET {update_list}"
            else:
                conflict_clause = f"ON CONFLICT ({pk_list}) DO NOTHING"

            # Batch multiple rows into one INSERT, respecting SQLite's bind parameter limit.
            chunk_size = max(1, _BIND_LIMIT // num_parameters)
            for upsert_chunk in (
                upserts[i : i + chunk_size] for i in range(0, len(upserts), chunk_size)
            ):
                placeholders = ", ".join("?" for _ in range(num_parameters))
                values_sql_parts: list[str] = []
                params: list[Any] = []
                for action in upsert_chunk:
                    assert action.value is not None
                    values_sql_parts.append(f"({placeholders})")
                    # Values are encoded by TableTarget before being stored as target state values.
                    params.extend(
                        action.value.get(col_name) for col_name in all_col_names
                    )

                values_sql = ", ".join(values_sql_parts)
                sql = f"{insert_clause} INTO {table_name} ({col_list}) VALUES {values_sql} {conflict_clause}"
                conn.execute(sql, params)

    def _execute_deletes(
        self, conn: sqlite3.Connection, deletes: list[_RowAction]
    ) -> None:
        """Execute delete operations."""
        table_name = _qualified_table_name(self._table_name)
        pk_cols = self._table_schema.primary_key

        # Build WHERE clause for primary key
        where_parts = [f'"{c}" = ?' for c in pk_cols]
        where_clause = " AND ".join(where_parts)
        sql = f"DELETE FROM {table_name} WHERE {where_clause}"

        for action in deletes:
            conn.execute(sql, list(action.key))

    def reconcile(
        self,
        key: _RowKey,
        desired_state: _RowValue | coco.NonExistenceType,
        prev_possible_states: Collection[_RowFingerprint],
        prev_may_be_missing: bool,
        /,
    ) -> coco.TargetReconcileOutput[_RowAction, _RowFingerprint] | None:
        if coco.is_non_existence(desired_state):
            # Delete case - only if it might exist
            if not prev_possible_states and not prev_may_be_missing:
                return None
            return coco.TargetReconcileOutput(
                action=_RowAction(key=key, value=None),
                sink=self._sink,
                tracking_record=coco.NON_EXISTENCE,
            )

        # Upsert case
        target_fp = fingerprint_object(desired_state)
        if not prev_may_be_missing and all(
            prev == target_fp for prev in prev_possible_states
        ):
            # No change needed
            return None

        return coco.TargetReconcileOutput(
            action=_RowAction(key=key, value=desired_state),
            sink=self._sink,
            tracking_record=target_fp,
        )


class _TableKey(NamedTuple):
    """Key identifying a table: (database_key, table_name)."""

    db_key: str  # Stable key for the database
    table_name: str


@dataclass
class _TableSpec:
    """Specification for a SQLite table."""

    table_schema: TableSchema[Any]
    managed_by: Literal["system", "user"] = "system"
    virtual_table_def: Vec0TableDef | None = None


class _PkColumnInfo(NamedTuple):
    """Information for a single primary key column."""

    name: str
    type: str


class _TablePrimaryTrackingRecord(NamedTuple):
    """Primary tracking information for a table (PK columns + virtual table config)."""

    primary_key_columns: tuple[_PkColumnInfo, ...]
    virtual_table_def: Vec0TableDef | None = None


class _NonPkColumnTrackingRecord(NamedTuple):
    """Per-non-PK column tracking record used for incremental ALTER TABLE operations."""

    type: str
    nullable: bool


_COL_SUBKEY_PREFIX: str = "col:"


def _col_subkey(col_name: str) -> str:
    return f"{_COL_SUBKEY_PREFIX}{col_name}"


_TableSubTrackingRecord = _NonPkColumnTrackingRecord | None


def _table_composite_tracking_record_from_spec(
    spec: _TableSpec,
) -> statediff.CompositeTrackingRecord[
    _TablePrimaryTrackingRecord, str, _TableSubTrackingRecord
]:
    schema = spec.table_schema
    col_by_name = schema.columns
    pk_sig = _TablePrimaryTrackingRecord(
        primary_key_columns=tuple(
            _PkColumnInfo(name=pk, type=col_by_name[pk].type)
            for pk in schema.primary_key
        ),
        virtual_table_def=spec.virtual_table_def,
    )
    sub: dict[str, _TableSubTrackingRecord] = {
        _col_subkey(col_name): _NonPkColumnTrackingRecord(
            type=col_def.type, nullable=col_def.nullable
        )
        for col_name, col_def in schema.columns.items()
        if col_name not in schema.primary_key
    }
    return statediff.CompositeTrackingRecord(main=pk_sig, sub=sub)


_TableTrackingRecord = statediff.MutualTrackingRecord[
    statediff.CompositeTrackingRecord[
        _TablePrimaryTrackingRecord, str, _TableSubTrackingRecord
    ]
]


class _TableAction(NamedTuple):
    """Action to perform on a table."""

    key: _TableKey
    spec: _TableSpec | coco.NonExistenceType
    main_action: statediff.DiffAction | None
    column_actions: dict[str, statediff.DiffAction]


# Database registry: maps stable keys to managed connections
_db_registry: connection.ConnectionRegistry[ManagedConnection] = (
    connection.ConnectionRegistry("cocoindex/sqlite")
)


# =============================================================================
# Table DDL helper functions
# =============================================================================


def _drop_table(conn: sqlite3.Connection, table_name: str) -> None:
    """Drop a table if it exists."""
    qualified_name = _qualified_table_name(table_name)
    conn.execute(f"DROP TABLE IF EXISTS {qualified_name}")


def _create_virtual_table(
    conn: sqlite3.Connection,
    table_name: str,
    schema: TableSchema[Any],
    virtual_table_def: Vec0TableDef,
    *,
    has_vec_extension: bool,
) -> None:
    """
    Create a virtual table.

    For vec0, the syntax is:
        CREATE VIRTUAL TABLE name USING vec0(
            id integer primary key,
            embedding float[384],
            year integer partition key,
            metadata text,
            +auxiliary_data text
        )
    """
    module_name = virtual_table_def.module_name

    # Validate extension is loaded
    if module_name == "vec0" and not has_vec_extension:
        raise RuntimeError(
            f"sqlite-vec extension required for {module_name} virtual tables"
        )

    qualified_name = _qualified_table_name(table_name)

    # Build column definitions for virtual table syntax
    col_defs = []
    partition_keys = set(virtual_table_def.partition_key_columns)
    auxiliary_cols = set(virtual_table_def.auxiliary_columns)

    for col_name, col_def in schema.columns.items():
        parts = []

        # Add + prefix for auxiliary columns
        if col_name in auxiliary_cols:
            parts.append(f"+{col_name}")
        else:
            parts.append(col_name)

        # Add type
        parts.append(col_def.type)

        # Add PRIMARY KEY marker
        if col_name in schema.primary_key:
            parts.append("primary key")

        # Add PARTITION KEY marker
        if col_name in partition_keys:
            parts.append("partition key")

        col_defs.append(" ".join(parts))

    columns_sql = ",\n    ".join(col_defs)
    sql = f"CREATE VIRTUAL TABLE {qualified_name} USING {module_name}(\n    {columns_sql}\n)"

    conn.execute(sql)


def _create_table(
    conn: sqlite3.Connection,
    table_name: str,
    schema: TableSchema[Any],
    *,
    if_not_exists: bool,
    has_vec_extension: bool,
) -> None:
    """Create a table."""
    # Check if vector columns are used but sqlite-vec is not loaded
    vector_cols = [name for name, col in schema.columns.items() if col.is_vector]
    if vector_cols and not has_vec_extension:
        raise RuntimeError(
            f"Table '{table_name}' has vector column(s) {vector_cols}, but sqlite-vec "
            "extension is not loaded. Use connect(..., load_vec=True) to enable it."
        )

    qualified_name = _qualified_table_name(table_name)

    # Build column definitions
    col_defs = []
    for col_name, col in schema.columns.items():
        nullable = (
            "" if col.nullable and col_name not in schema.primary_key else " NOT NULL"
        )
        col_defs.append(f'"{col_name}" {col.type}{nullable}')

    # Build primary key constraint
    pk_cols = ", ".join(f'"{c}"' for c in schema.primary_key)
    col_defs.append(f"PRIMARY KEY ({pk_cols})")

    columns_sql = ", ".join(col_defs)
    if_not_exists_sql = " IF NOT EXISTS" if if_not_exists else ""
    sql = f"CREATE TABLE{if_not_exists_sql} {qualified_name} ({columns_sql})"
    conn.execute(sql)


def _apply_column_actions(
    conn: sqlite3.Connection,
    table_name: str,
    schema: TableSchema[Any],
    column_actions: dict[str, statediff.DiffAction],
) -> None:
    """Apply column-level changes to the table.

    Note: SQLite has limited ALTER TABLE support. Adding columns is supported,
    but modifying or dropping columns requires recreating the table (in older SQLite).
    SQLite 3.35.0+ supports DROP COLUMN.
    """
    qualified_name = _qualified_table_name(table_name)
    pk_cols = set(schema.primary_key)
    non_pk_col_by_name = {n: c for n, c in schema.columns.items() if n not in pk_cols}

    for sub_key, action in column_actions.items():
        if not sub_key.startswith(_COL_SUBKEY_PREFIX):
            raise ValueError(
                f"Unexpected column subkey format: {sub_key!r}, expected to start with {_COL_SUBKEY_PREFIX!r}"
            )
        col_name = sub_key[len(_COL_SUBKEY_PREFIX) :]

        # Defensive: we never ALTER PK columns here.
        if col_name in pk_cols:
            continue

        if action == "delete":
            # SQLite 3.35.0+ supports DROP COLUMN
            try:
                conn.execute(f'ALTER TABLE {qualified_name} DROP COLUMN "{col_name}"')
            except sqlite3.OperationalError:
                # Older SQLite doesn't support DROP COLUMN - silently skip
                pass
            continue

        desired_col = non_pk_col_by_name.get(col_name)
        if desired_col is None:
            # If the desired schema no longer mentions this column, treat
            # it as a no-op here; "delete" should have been emitted.
            continue

        if action in ("insert", "upsert"):
            # SQLite supports ADD COLUMN
            nullable = "" if desired_col.nullable else " NOT NULL"
            try:
                conn.execute(
                    f"ALTER TABLE {qualified_name} "
                    f'ADD COLUMN "{col_name}" {desired_col.type}{nullable}'
                )
            except sqlite3.OperationalError:
                # Column might already exist (upsert case)
                pass
            continue

        if action == "replace":
            # SQLite doesn't support ALTER COLUMN TYPE directly.
            # For type changes, we'd need to recreate the table.
            # For now, we'll drop and re-add if possible.
            try:
                conn.execute(f'ALTER TABLE {qualified_name} DROP COLUMN "{col_name}"')
                nullable = "" if desired_col.nullable else " NOT NULL"
                conn.execute(
                    f"ALTER TABLE {qualified_name} "
                    f'ADD COLUMN "{col_name}" {desired_col.type}{nullable}'
                )
            except sqlite3.OperationalError:
                # Can't modify column - skip
                pass


def _apply_table_actions(
    actions: Sequence[_TableAction],
) -> list[coco.ChildTargetDef["_RowHandler"] | None]:
    """Apply table actions (DDL) and return child row handlers."""
    actions_list = list(actions)
    outputs: list[coco.ChildTargetDef[_RowHandler] | None] = [None] * len(actions_list)

    # Group actions by table key so we can apply all DDL for the same table
    by_key: dict[_TableKey, list[int]] = {}
    for i, action in enumerate(actions_list):
        by_key.setdefault(action.key, []).append(i)

    for key, idxs in by_key.items():
        managed_conn = _db_registry.get(key.db_key)
        has_vec = _VEC_EXTENSION in managed_conn.loaded_extensions

        with managed_conn.transaction() as conn:
            for i in idxs:
                action = actions_list[i]
                assert action.key == key

                # Check if this is a virtual table (for special handling)
                is_virtual = (
                    not coco.is_non_existence(action.spec)
                    and action.spec.virtual_table_def is not None
                )

                # Virtual tables can't use ALTER TABLE - force DROP+CREATE for column changes
                if is_virtual and action.column_actions and action.main_action is None:
                    # Upgrade to replace action
                    action = _TableAction(
                        key=action.key,
                        spec=action.spec,
                        main_action="replace",
                        column_actions={},
                    )

                if action.main_action in ("replace", "delete"):
                    _drop_table(conn, key.table_name)

                if coco.is_non_existence(action.spec):
                    outputs[i] = None
                    continue

                spec = action.spec
                outputs[i] = coco.ChildTargetDef(
                    handler=_RowHandler(
                        managed_conn=managed_conn,
                        table_name=key.table_name,
                        table_schema=spec.table_schema,
                        is_virtual_table=(spec.virtual_table_def is not None),
                    )
                )

                if action.main_action in ("insert", "upsert", "replace"):
                    # Route to virtual or regular table creation
                    if spec.virtual_table_def is not None:
                        _create_virtual_table(
                            conn,
                            key.table_name,
                            spec.table_schema,
                            spec.virtual_table_def,
                            has_vec_extension=has_vec,
                        )
                    else:
                        _create_table(
                            conn,
                            key.table_name,
                            spec.table_schema,
                            if_not_exists=(action.main_action == "upsert"),
                            has_vec_extension=has_vec,
                        )
                    continue

                # No main change: reconcile non-PK columns incrementally.
                # (Virtual tables never reach here - they're forced to replace above)
                if action.column_actions:
                    _apply_column_actions(
                        conn,
                        key.table_name,
                        spec.table_schema,
                        action.column_actions,
                    )

    return outputs


# Shared action sink for table-level actions
_table_action_sink = coco.TargetActionSink[_TableAction, _RowHandler].from_fn(
    _apply_table_actions
)


class _TableHandler(
    coco.TargetHandler[_TableKey, _TableSpec, _TableTrackingRecord, _RowHandler]
):
    """Handler for table-level target states."""

    def reconcile(
        self,
        key: _TableKey,
        desired_state: _TableSpec | coco.NonExistenceType,
        prev_possible_states: Collection[_TableTrackingRecord],
        prev_may_be_missing: bool,
        /,
    ) -> (
        coco.TargetReconcileOutput[_TableAction, _TableTrackingRecord, _RowHandler]
        | None
    ):
        tracking_record: _TableTrackingRecord | coco.NonExistenceType

        if coco.is_non_existence(desired_state):
            tracking_record = coco.NON_EXISTENCE
        else:
            tracking_record = statediff.MutualTrackingRecord(
                tracking_record=_table_composite_tracking_record_from_spec(
                    desired_state
                ),
                managed_by=desired_state.managed_by,
            )

        resolved = statediff.resolve_system_transition(
            statediff.TrackingRecordTransition(
                tracking_record,
                prev_possible_states,
                prev_may_be_missing,
            )
        )
        main_action, column_transitions = statediff.diff_composite(resolved)

        column_actions: dict[str, statediff.DiffAction] = {}
        if main_action is None:
            for sub_key, t in column_transitions.items():
                action = statediff.diff(t)
                if action is not None:
                    column_actions[sub_key] = action

        return coco.TargetReconcileOutput(
            action=_TableAction(
                key=key,
                spec=desired_state,
                main_action=main_action,
                column_actions=column_actions,
            ),
            sink=_table_action_sink,
            tracking_record=tracking_record,
        )


# Register the root target states provider
_table_provider = coco.register_root_target_states_provider(
    "cocoindex.io/sqlite/table", _TableHandler()
)


class TableTarget(
    Generic[RowT, coco.MaybePendingS], coco.ResolvesTo["TableTarget[RowT]"]
):
    """
    A target for writing rows to a SQLite table.

    The table is managed as a target state, with the scope used to scope the target state.

    Type Parameters:
        RowT: The type of row objects (dict, dataclass, NamedTuple, or Pydantic model).
    """

    _provider: coco.TargetStateProvider[_RowKey, _RowValue, None, coco.MaybePendingS]
    _table_schema: TableSchema[RowT]

    def __init__(
        self,
        provider: coco.TargetStateProvider[
            _RowKey, _RowValue, None, coco.MaybePendingS
        ],
        table_schema: TableSchema[RowT],
    ) -> None:
        self._provider = provider
        self._table_schema = table_schema

    def declare_row(self: "TableTarget[RowT]", *, row: RowT) -> None:
        """
        Declare a row to be upserted to this table.

        Args:
            row: A row object (dict, dataclass, NamedTuple, or Pydantic model).
                 Must include all primary key columns.
        """
        row_dict = self._row_to_dict(row)
        # Extract primary key values
        pk_values = tuple(row_dict[pk] for pk in self._table_schema.primary_key)
        coco.declare_target_state(self._provider.target_state(pk_values, row_dict))

    def _row_to_dict(self, row: RowT) -> dict[str, Any]:
        """
        Convert a row (dict or object) into dict[str, Any] using the schema columns,
        and apply column encoders for both dict and object inputs.
        """
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

    def __coco_memo_key__(self) -> str:
        return self._provider.memo_key


class SqliteDatabase(connection.KeyedConnection[ManagedConnection]):
    """
    Handle for a registered SQLite database.

    Use `register_db()` to create an instance. Can be used as a context manager
    to automatically unregister on exit.

    Example:
        ```python
        # Without context manager (manual lifecycle)
        db = register_db("my_db", conn)
        # ... use db ...

        # With context manager (auto-unregister on exit)
        with register_db("my_db", conn) as db:
            # ... use db ...
        # db is automatically unregistered here
        ```
    """

    def declare_table_target(
        self,
        table_name: str,
        table_schema: TableSchema[RowT],
        *,
        managed_by: Literal["system", "user"] = "system",
        virtual_table_def: Vec0TableDef | None = None,
    ) -> TableTarget[RowT, coco.PendingS]:
        """
        Create a TableTarget for writing rows to a SQLite table (regular or virtual).

        Args:
            table_name: Name of the table.
            table_schema: Schema definition including columns and primary key.
            managed_by: Whether the table is managed by "system" (CocoIndex creates/drops it)
                        or "user" (table must exist, CocoIndex only manages rows).
            virtual_table_def: Optional virtual table configuration.
                - None (default): Create a regular table
                - Vec0TableDef: Create a vec0 virtual table (requires sqlite-vec extension)

        Returns:
            A TableTarget that can be used to declare rows.

        Raises:
            RuntimeError: If vec0 virtual table is requested but sqlite-vec extension is not loaded.
            ValueError: If vec0 table configuration is invalid (e.g., no vector columns,
                       invalid partition/auxiliary columns).

        Note:
            Virtual tables cannot be altered with ALTER TABLE. Schema changes
            automatically trigger DROP + CREATE operations.
        """
        # Validate vec0 virtual table configuration
        if isinstance(virtual_table_def, Vec0TableDef):
            # Check extension is loaded
            if _VEC_EXTENSION not in self._value.loaded_extensions:
                raise RuntimeError(
                    "sqlite-vec extension must be loaded for vec0 virtual tables. "
                    "Use connect(..., load_vec=True)"
                )

            # Validate at least one vector column exists
            has_vector_col = any(
                col_def.type.startswith("float[")
                for col_def in table_schema.columns.values()
            )
            if not has_vector_col:
                raise ValueError(
                    "vec0 virtual tables require at least one float[N] vector column"
                )

            # Validate partition_key_columns exist in schema
            all_cols = set(table_schema.columns.keys())
            invalid_partition = set(virtual_table_def.partition_key_columns) - all_cols
            if invalid_partition:
                raise ValueError(
                    f"Partition key columns not in schema: {invalid_partition}"
                )

            # Validate auxiliary_columns exist in schema
            invalid_aux = set(virtual_table_def.auxiliary_columns) - all_cols
            if invalid_aux:
                raise ValueError(f"Auxiliary columns not in schema: {invalid_aux}")

            # Validate vec0 primary key: must have exactly one integer PK column
            if len(table_schema.primary_key) != 1:
                raise ValueError(
                    f"vec0 virtual tables require exactly one primary key column, "
                    f"got {len(table_schema.primary_key)}: {table_schema.primary_key}"
                )
            pk_col_name = table_schema.primary_key[0]
            pk_col_type = table_schema.columns[pk_col_name].type
            if pk_col_type != "INTEGER":
                raise ValueError(
                    f"vec0 virtual tables require INTEGER primary key, "
                    f"got {pk_col_type} for column '{pk_col_name}'"
                )

        key = _TableKey(
            db_key=self.key,
            table_name=table_name,
        )
        spec = _TableSpec(
            table_schema=table_schema,
            managed_by=managed_by,
            virtual_table_def=virtual_table_def,
        )
        provider = coco.declare_target_state_with_child(
            _table_provider.target_state(key, spec)
        )
        return TableTarget(provider, table_schema)


def register_db(key: str, managed_conn: ManagedConnection) -> SqliteDatabase:
    """
    Register a SQLite database connection with a stable key.

    The key should be stable across runs - it identifies the logical database.
    The connection can be recreated with different parameters as long as the
    same key is used.

    Can be used as a context manager to automatically unregister on exit.

    Args:
        key: A stable identifier for this database (e.g., "main_db", "analytics").
             Must be unique - raises ValueError if a database with this key
             is already registered.
        managed_conn: A ManagedConnection object from connect().

    Returns:
        A SqliteDatabase handle that can be used to create table targets.

    Raises:
        ValueError: If a database with the given key is already registered.

    Example:
        ```python
        def setup():
            managed_conn = connect("mydb.sqlite")

            # Option 1: Manual lifecycle
            db = register_db("my_db", managed_conn)

            # Option 2: Context manager (auto-unregister on exit)
            with register_db("my_db", managed_conn) as db:
                table = db.declare_table_target("my_table", schema)
            # db is automatically unregistered here
        ```
    """
    _db_registry.register(key, managed_conn)
    return SqliteDatabase(_db_registry.name, key, managed_conn, _db_registry)


def connect(
    database: str | Path,
    *,
    timeout: float = 5.0,
    load_vec: bool | Literal["auto"] = "auto",
    **kwargs: Any,
) -> ManagedConnection:
    """
    Create a SQLite connection with common defaults for CocoIndex.

    The connection uses autocommit mode internally. Use `ManagedConnection.transaction()`
    for write operations that need atomic commits, or `ManagedConnection.readonly()`
    for read-only operations.

    Args:
        database: Path to the database file, or ":memory:" for in-memory database.
        timeout: How long to wait for locks (default 5.0 seconds).
        load_vec: Whether to load the sqlite-vec extension for vector support.
            - "auto" (default): Try to load, silently ignore if unavailable.
            - True: Load and raise an error if unavailable.
            - False: Don't attempt to load.
        **kwargs: Additional arguments passed to sqlite3.connect().

    Returns:
        A ManagedConnection object with thread-safe access.

    Example:
        ```python
        managed_conn = connect("mydb.sqlite")
        # Or for in-memory:
        managed_conn = connect(":memory:")
        # Or explicitly require vector support:
        managed_conn = connect("mydb.sqlite", load_vec=True)
        ```
    """
    database_str = str(database) if isinstance(database, Path) else database
    conn: sqlite3.Connection = sqlite3.connect(
        database_str,
        timeout=timeout,
        isolation_level=None,  # Autocommit mode - transactions managed explicitly
        check_same_thread=False,
        **kwargs,
    )

    managed_conn = ManagedConnection(conn)

    if load_vec is True:
        _load_sqlite_vec(managed_conn)
    elif load_vec == "auto":
        _load_sqlite_vec(managed_conn, ignore_error=True)

    return managed_conn


def _load_sqlite_vec(
    managed_conn: ManagedConnection, *, ignore_error: bool = False
) -> None:
    """Load the sqlite-vec extension into a managed connection."""
    try:
        import sqlite_vec  # type: ignore
    except ImportError as e:
        if ignore_error:
            return
        raise ImportError(
            "sqlite-vec is required for vector support. "
            "Install it with: pip install sqlite-vec"
        ) from e

    try:
        managed_conn._conn.enable_load_extension(True)
        sqlite_vec.load(managed_conn._conn)
        managed_conn._conn.enable_load_extension(False)
        managed_conn._loaded_extensions.add(_VEC_EXTENSION)
    except sqlite3.OperationalError:
        if ignore_error:
            return
        raise


__all__ = [
    "ColumnDef",
    "ManagedConnection",
    "ValueEncoder",
    "SqliteDatabase",
    "SqliteType",
    "TableSchema",
    "TableTarget",
    "Vec0TableDef",
    "connect",
    "register_db",
]
