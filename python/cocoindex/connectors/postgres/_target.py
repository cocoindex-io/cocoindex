"""
PostgreSQL target for CocoIndex.

This module provides a two-level target state system for PostgreSQL:
1. Table level: Creates/drops tables in the database
2. Row level: Upserts/deletes rows within tables
"""

from __future__ import annotations

import datetime
import decimal
import ipaddress
import inspect
import json
import uuid
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Collection,
    Generic,
    Literal,
    NamedTuple,
    Sequence,
    overload,
)

from typing_extensions import TypeVar

try:
    import asyncpg  # type: ignore
    import pgvector.asyncpg  # type: ignore
except ImportError as e:
    raise ImportError(
        "asyncpg and pgvector are required to use the PostgreSQL connector. Please install cocoindex[postgres]."
    ) from e

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

# Postgres protocol parameter limit (also used in the Rust implementation).
_BIND_LIMIT: int = 65535


def _qualified_table_name(table_name: str, pg_schema_name: str | None) -> str:
    """Return a properly quoted (optionally schema-qualified) table name."""

    if pg_schema_name:
        return f'"{pg_schema_name}"."{table_name}"'
    return f'"{table_name}"'


class PgType(NamedTuple):
    """
    Annotation to specify a PostgreSQL column type.

    Use with `typing.Annotated` to override the default type mapping:

    ```python
    from typing import Annotated
    from dataclasses import dataclass
    from cocoindex.connectors.postgres import PgType

    @dataclass
    class MyRow:
        # Use integer instead of default bigint
        id: Annotated[int, PgType("integer")]
        # Use real instead of default double precision
        value: Annotated[float, PgType("real")]
        # Use timestamp without timezone
        created_at: Annotated[datetime.datetime, PgType("timestamp")]
    ```
    """

    pg_type: str
    encoder: ValueEncoder | None = None


def _json_encoder(value: Any) -> str:
    """Encode a value to JSON string for asyncpg."""
    return json.dumps(value, default=str)


_PGVECTOR_TYPE_BASES: frozenset[str] = frozenset({"vector", "halfvec"})
_PGVECTOR_TYPE_PREFIXES: frozenset[str] = frozenset(
    {f"{base}(" for base in _PGVECTOR_TYPE_BASES}
)


def _is_pgvector_pg_type(pg_type: str) -> bool:
    """
    Return True if `pg_type` is a pgvector type (`vector(n)`, ...).

    This is used for extension checks and validation.
    """
    t = pg_type.lower().strip()
    return any(t.startswith(p) for p in _PGVECTOR_TYPE_PREFIXES)


class _TypeMapping(NamedTuple):
    """Mapping from Python type to PostgreSQL type with optional encoder."""

    pg_type: str
    encoder: ValueEncoder | None = None


# Global mapping for leaf types
# Based on asyncpg's type conversion: https://magicstack.github.io/asyncpg/current/usage.html#type-conversion
# For types that map to multiple PostgreSQL types, uses the broader one.
_LEAF_TYPE_MAPPINGS: dict[type, _TypeMapping] = {
    # Boolean
    bool: _TypeMapping("boolean"),
    # Numeric types (use broader types)
    int: _TypeMapping("bigint"),
    float: _TypeMapping("double precision"),
    decimal.Decimal: _TypeMapping("numeric"),
    # NumPy scalar integer types (finer-grained)
    np.int8: _TypeMapping("smallint"),
    np.int16: _TypeMapping("smallint"),
    np.int32: _TypeMapping("integer"),
    np.int64: _TypeMapping("bigint"),
    # NumPy scalar unsigned integer types (Postgres has no unsigned ints)
    np.uint8: _TypeMapping("smallint"),  # always fits
    np.uint16: _TypeMapping("integer"),  # can exceed smallint
    np.uint32: _TypeMapping("bigint"),  # can exceed integer
    np.uint64: _TypeMapping("numeric"),  # can exceed bigint
    # Platform-dependent aliases
    np.int_: _TypeMapping("bigint"),
    np.uint: _TypeMapping("numeric"),
    # NumPy scalar float types (finer-grained)
    np.float16: _TypeMapping("real"),
    np.float32: _TypeMapping("real"),
    np.float64: _TypeMapping("double precision"),
    # String types
    str: _TypeMapping("text"),
    bytes: _TypeMapping("bytea"),
    # UUID
    uuid.UUID: _TypeMapping("uuid"),
    # Date/time types (use timezone-aware variants as broader)
    datetime.date: _TypeMapping("date"),
    datetime.time: _TypeMapping("time with time zone"),
    datetime.datetime: _TypeMapping("timestamp with time zone"),
    datetime.timedelta: _TypeMapping("interval"),
    # Network types
    ipaddress.IPv4Network: _TypeMapping("cidr"),
    ipaddress.IPv6Network: _TypeMapping("cidr"),
    ipaddress.IPv4Address: _TypeMapping("inet"),
    ipaddress.IPv6Address: _TypeMapping("inet"),
    ipaddress.IPv4Interface: _TypeMapping("inet"),
    ipaddress.IPv6Interface: _TypeMapping("inet"),
}

# Default mapping for complex types that need JSON encoding
_JSONB_MAPPING = _TypeMapping("jsonb", _json_encoder)


def _get_type_mapping(
    python_type: Any, *, vector_schema_provider: VectorSchemaProvider | None = None
) -> _TypeMapping:
    """
    Get the PostgreSQL type mapping for a Python type.

    Based on asyncpg's type conversion table:
    https://magicstack.github.io/asyncpg/current/usage.html#type-conversion

    For types that map to multiple PostgreSQL types, uses the broader one.
    Use `PgType` annotation with `typing.Annotated` to override the default.
    """
    type_info = analyze_type_info(python_type)

    # Check for PgType annotation override
    for annotation in type_info.annotations:
        if isinstance(annotation, PgType):
            return _TypeMapping(annotation.pg_type, annotation.encoder)

    base_type = type_info.base_type

    # Check direct leaf type mappings
    if base_type in _LEAF_TYPE_MAPPINGS:
        return _LEAF_TYPE_MAPPINGS[base_type]

    # NumPy ndarray: map to pgvector type bases; dimension is handled at the schema layer.
    if base_type is np.ndarray:
        if vector_schema_provider is None:
            raise ValueError("VectorSpecProvider is required for NumPy ndarray type.")
        vector_schema = vector_schema_provider.__coco_vector_schema__()

        if vector_schema.size <= 0:
            raise ValueError(f"Invalid pgvector dimension: {vector_schema.size}")

        # Default to `vector` (float32/float64/int64/etc.). Use `halfvec` for float16.
        base = "halfvec" if vector_schema.dtype in (np.half, np.float16) else "vector"
        return _TypeMapping(pg_type=f"{base}({vector_schema.size})")

    elif vector_schema_provider is not None:
        raise ValueError(
            f"VectorSpecProvider is only supported for NumPy ndarray type. Got type: {python_type}"
        )

    # Complex types that need JSON encoding
    if isinstance(
        type_info.variant, (SequenceType, MappingType, RecordType, UnionType, AnyType)
    ):
        return _JSONB_MAPPING

    # Default fallback
    return _JSONB_MAPPING


class ColumnDef(NamedTuple):
    """Definition of a table column."""

    type: str  # PostgreSQL type (e.g., "text", "bigint", "jsonb", "vector(384)")
    nullable: bool = True
    encoder: ValueEncoder | None = (
        None  # Optional encoder to convert value before sending to asyncpg
    )


# Type variable for row type
RowT = TypeVar("RowT", default=dict[str, Any])


@dataclass(slots=True)
class TableSchema(Generic[RowT]):
    """Schema definition for a PostgreSQL table."""

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
        column_overrides: dict[str, PgType | VectorSchemaProvider] | None = None,
    ) -> None: ...

    def __init__(
        self,
        columns: type[RowT] | dict[str, ColumnDef],
        primary_key: list[str],
        *,
        column_overrides: dict[str, PgType | VectorSchemaProvider] | None = None,
    ) -> None:
        """
        Create a TableSchema.

        Args:
            columns: Either a record type (dataclass, NamedTuple, or Pydantic model)
                     or a dict mapping column names to ColumnDef.
                     When a record type is provided, Python types are automatically
                     mapped to PostgreSQL types based on asyncpg's type conversion.
            primary_key: List of column names that form the primary key.
            column_overrides: Optional dict mapping column names to PgType or VectorSchemaProvider
                              to override the default type mapping.
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
        column_overrides: dict[str, PgType | VectorSchemaProvider] | None,
    ) -> dict[str, ColumnDef]:
        """Convert a record type to a dict of column name -> ColumnDef."""
        record_info = RecordType(record_type)
        columns: dict[str, ColumnDef] = {}

        for field in record_info.fields:
            override = column_overrides.get(field.name) if column_overrides else None
            type_info = analyze_type_info(field.type_hint)

            # Extract PgType and VectorSchemaProvider from annotations
            pg_type_annotation: PgType | None = None
            vector_schema_provider: VectorSchemaProvider | None = None
            for annotation in type_info.annotations:
                if isinstance(annotation, PgType):
                    pg_type_annotation = annotation
                elif isinstance(annotation, VectorSchemaProvider):
                    vector_schema_provider = annotation

            # Override takes precedence over annotation
            if isinstance(override, PgType):
                pg_type_annotation = override
            elif isinstance(override, VectorSchemaProvider):
                vector_schema_provider = override

            # Determine type mapping
            if pg_type_annotation is not None:
                type_mapping = _TypeMapping(
                    pg_type_annotation.pg_type, pg_type_annotation.encoder
                )
            else:
                type_mapping = _get_type_mapping(
                    field.type_hint, vector_schema_provider=vector_schema_provider
                )

            columns[field.name] = ColumnDef(
                type=type_mapping.pg_type.strip(),
                nullable=type_info.nullable,
                encoder=type_mapping.encoder,
            )

        return columns


class _RowAction(NamedTuple):
    """Action to perform on a row."""

    key: _RowKey
    value: _RowValue | None  # None means delete


class _RowHandler(coco.TargetHandler[_RowKey, _RowValue, _RowFingerprint]):
    """Handler for row-level target states within a table."""

    _pool: asyncpg.Pool
    _table_name: str
    _schema_name: str | None
    _table_schema: TableSchema
    _sink: coco.TargetActionSink[_RowAction]

    def __init__(
        self,
        pool: asyncpg.Pool,
        table_name: str,
        pg_schema_name: str | None,
        table_schema: TableSchema,
    ) -> None:
        self._pool = pool
        self._table_name = table_name
        self._schema_name = pg_schema_name
        self._table_schema = table_schema
        self._sink = coco.TargetActionSink.from_async_fn(self._apply_actions)

    async def _apply_actions(self, actions: Sequence[_RowAction]) -> None:
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

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                # Process upserts
                if upserts:
                    await self._execute_upserts(conn, upserts)

                # Process deletes
                if deletes:
                    await self._execute_deletes(conn, deletes)

    async def _execute_upserts(
        self,
        conn: asyncpg.pool.PoolConnectionProxy[asyncpg.Record],
        upserts: list[_RowAction],
    ) -> None:
        """Execute upsert operations."""
        table_name = _qualified_table_name(self._table_name, self._schema_name)
        columns = self._table_schema.columns
        pk_cols = self._table_schema.primary_key
        all_col_names = list(columns.keys())
        non_pk_cols = [c for c in all_col_names if c not in pk_cols]

        # Build column lists
        col_list = ", ".join(f'"{c}"' for c in all_col_names)
        pk_list = ", ".join(f'"{c}"' for c in pk_cols)

        # Build ON CONFLICT clause
        if non_pk_cols:
            update_list = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in non_pk_cols)
            conflict_clause = f"ON CONFLICT ({pk_list}) DO UPDATE SET {update_list}"
        else:
            conflict_clause = f"ON CONFLICT ({pk_list}) DO NOTHING"

        num_parameters = len(all_col_names)
        if num_parameters == 0:
            return

        # Batch multiple rows into one INSERT, respecting Postgres' bind parameter limit.
        chunk_size = max(1, _BIND_LIMIT // num_parameters)
        for upsert_chunk in (
            upserts[i : i + chunk_size] for i in range(0, len(upserts), chunk_size)
        ):
            values_sql_parts: list[str] = []
            params: list[Any] = []
            for row_idx, action in enumerate(upsert_chunk):
                assert action.value is not None
                base = row_idx * num_parameters
                placeholders = ", ".join(
                    f"${base + j + 1}" for j in range(num_parameters)
                )
                values_sql_parts.append(f"({placeholders})")
                # Values are encoded by TableTarget before being stored as target state values.
                params.extend(action.value.get(col_name) for col_name in all_col_names)

            values_sql = ", ".join(values_sql_parts)
            sql = f"INSERT INTO {table_name} ({col_list}) VALUES {values_sql} {conflict_clause}"
            await conn.execute(sql, *params)

    async def _execute_deletes(
        self,
        conn: asyncpg.pool.PoolConnectionProxy[asyncpg.Record],
        deletes: list[_RowAction],
    ) -> None:
        """Execute delete operations."""
        table_name = _qualified_table_name(self._table_name, self._schema_name)
        pk_cols = self._table_schema.primary_key

        # Build WHERE clause for primary key
        where_parts = [f'"{c}" = ${i + 1}' for i, c in enumerate(pk_cols)]
        where_clause = " AND ".join(where_parts)
        sql = f"DELETE FROM {table_name} WHERE {where_clause}"

        for action in deletes:
            await conn.execute(sql, *action.key)

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
    """Key identifying a table: (database_key, pg_schema_name, table_name)."""

    db_key: str  # Stable key for the database
    pg_schema_name: str | None
    table_name: str


@dataclass
class _TableSpec:
    """Specification for a PostgreSQL table."""

    table_schema: TableSchema[Any]
    managed_by: Literal["system", "user"] = "system"


class _PkColumnTrackingRecord(NamedTuple):
    """Primary-key column signature used for table-level main tracking record."""

    name: str
    type: str


class _NonPkColumnTrackingRecord(NamedTuple):
    """Per-non-PK column tracking record used for incremental ALTER TABLE operations."""

    type: str
    nullable: bool


_EXT_PGVECTOR_SUBKEY: str = "ext:pgvector"
_COL_SUBKEY_PREFIX: str = "col:"


def _schema_uses_pgvector(schema: TableSchema[Any]) -> bool:
    return any(_is_pgvector_pg_type(c.type) for c in schema.columns.values())


def _col_subkey(col_name: str) -> str:
    return f"{_COL_SUBKEY_PREFIX}{col_name}"


_TableSubTrackingRecord = _NonPkColumnTrackingRecord | None


def _table_composite_tracking_record_from_spec(
    spec: _TableSpec,
) -> statediff.CompositeTrackingRecord[
    tuple[_PkColumnTrackingRecord, ...], str, _TableSubTrackingRecord
]:
    schema = spec.table_schema
    col_by_name = schema.columns
    pk_sig = tuple(
        _PkColumnTrackingRecord(name=pk, type=col_by_name[pk].type)
        for pk in schema.primary_key
    )
    sub: dict[str, _TableSubTrackingRecord] = {
        _col_subkey(col_name): _NonPkColumnTrackingRecord(
            type=col_def.type, nullable=col_def.nullable
        )
        for col_name, col_def in schema.columns.items()
        if col_name not in schema.primary_key
    }
    if _schema_uses_pgvector(schema):
        sub[_EXT_PGVECTOR_SUBKEY] = None
    return statediff.CompositeTrackingRecord(main=pk_sig, sub=sub)


_TableTrackingRecord = statediff.MutualTrackingRecord[
    statediff.CompositeTrackingRecord[
        tuple[_PkColumnTrackingRecord, ...], str, _TableSubTrackingRecord
    ]
]


class _TableAction(NamedTuple):
    """Action to perform on a table."""

    key: _TableKey
    spec: _TableSpec | coco.NonExistenceType
    main_action: statediff.DiffAction | None
    column_actions: dict[str, statediff.DiffAction]


# Database registry: maps stable keys to connection pools
_db_registry: connection.ConnectionRegistry[asyncpg.Pool] = (
    connection.ConnectionRegistry("cocoindex/postgres")
)


class _TableHandler(
    coco.TargetHandler[_TableKey, _TableSpec, _TableTrackingRecord, _RowHandler]
):
    """Handler for table-level target states."""

    _sink: coco.TargetActionSink[_TableAction, _RowHandler]

    def __init__(self) -> None:
        self._sink = coco.TargetActionSink.from_async_fn(self._apply_actions)

    async def _apply_actions(
        self, actions: Collection[_TableAction]
    ) -> list[coco.ChildTargetDef[_RowHandler] | None]:
        """Apply table actions (DDL) and return child row handlers."""
        actions_list = list(actions)
        outputs: list[coco.ChildTargetDef[_RowHandler] | None] = [None] * len(
            actions_list
        )

        # Group actions by table key so we can apply all DDL for the same table
        # within a single transaction/connection.
        by_key: dict[_TableKey, list[int]] = {}
        for i, action in enumerate(actions_list):
            by_key.setdefault(action.key, []).append(i)

        for key, idxs in by_key.items():
            pool = _db_registry.get(key.db_key)
            async with pool.acquire() as conn:
                async with conn.transaction():
                    for i in idxs:
                        action = actions_list[i]
                        assert action.key == key

                        if action.main_action in ("replace", "delete"):
                            await self._drop_table(
                                conn, key.table_name, key.pg_schema_name
                            )

                        if coco.is_non_existence(action.spec):
                            outputs[i] = None
                            continue

                        spec = action.spec
                        outputs[i] = coco.ChildTargetDef(
                            handler=_RowHandler(
                                pool=pool,
                                table_name=key.table_name,
                                pg_schema_name=key.pg_schema_name,
                                table_schema=spec.table_schema,
                            )
                        )

                        if action.main_action in ("insert", "upsert", "replace"):
                            await self._create_table(
                                conn,
                                key,
                                spec.table_schema,
                                if_not_exists=(action.main_action == "upsert"),
                            )
                            continue

                        # No main change: reconcile non-PK columns incrementally.
                        if action.column_actions:
                            await self._apply_column_actions(
                                conn, key, spec.table_schema, action.column_actions
                            )

        return outputs

    async def _drop_table(
        self,
        conn: asyncpg.pool.PoolConnectionProxy[asyncpg.Record],
        table_name: str,
        pg_schema_name: str | None,
    ) -> None:
        """Drop a table if it exists."""
        qualified_name = _qualified_table_name(table_name, pg_schema_name)
        await conn.execute(f"DROP TABLE IF EXISTS {qualified_name}")

    async def _ensure_pgvector_extension(
        self,
        conn: asyncpg.pool.PoolConnectionProxy[asyncpg.Record],
        pg_schema_name: str | None,
    ) -> None:
        """
        Ensure the pgvector extension is installed.

        Postgres extensions are installed per-database but can live in a chosen
        schema; when `pg_schema_name` is provided we request installing into it.
        """
        schema_clause = f' WITH SCHEMA "{pg_schema_name}"' if pg_schema_name else ""
        await conn.execute(f"CREATE EXTENSION IF NOT EXISTS vector{schema_clause}")

    async def _create_table(
        self,
        conn: asyncpg.pool.PoolConnectionProxy[asyncpg.Record],
        key: _TableKey,
        schema: TableSchema[Any],
        *,
        if_not_exists: bool,
    ) -> None:
        """Create a table."""
        qualified_name = _qualified_table_name(key.table_name, key.pg_schema_name)

        # Create schema if specified
        if key.pg_schema_name:
            await conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{key.pg_schema_name}"')

        # Ensure pgvector extension exists if needed by any column type.
        if _schema_uses_pgvector(schema):
            await self._ensure_pgvector_extension(conn, key.pg_schema_name)

        # Build column definitions
        col_defs = []
        for col_name, col in schema.columns.items():
            nullable = (
                ""
                if col.nullable and col_name not in schema.primary_key
                else " NOT NULL"
            )
            col_defs.append(f'"{col_name}" {col.type}{nullable}')

        # Build primary key constraint
        pk_cols = ", ".join(f'"{c}"' for c in schema.primary_key)
        col_defs.append(f"PRIMARY KEY ({pk_cols})")

        columns_sql = ", ".join(col_defs)
        if_not_exists_sql = " IF NOT EXISTS" if if_not_exists else ""
        sql = f"CREATE TABLE{if_not_exists_sql} {qualified_name} ({columns_sql})"
        await conn.execute(sql)

    async def _apply_column_actions(
        self,
        conn: asyncpg.pool.PoolConnectionProxy[asyncpg.Record],
        key: _TableKey,
        schema: TableSchema[Any],
        column_actions: dict[str, statediff.DiffAction],
    ) -> None:
        qualified_name = _qualified_table_name(key.table_name, key.pg_schema_name)
        pk_cols = set(schema.primary_key)
        non_pk_col_by_name = {
            n: c for n, c in schema.columns.items() if n not in pk_cols
        }
        for sub_key, action in column_actions.items():
            if sub_key == _EXT_PGVECTOR_SUBKEY:
                if action != "delete":
                    await self._ensure_pgvector_extension(conn, key.pg_schema_name)
                continue

            if not sub_key.startswith(_COL_SUBKEY_PREFIX):
                raise ValueError(
                    f"Unexpected column subkey format: {sub_key!r}, expected to start with {_COL_SUBKEY_PREFIX!r}"
                )
            col_name = sub_key[len(_COL_SUBKEY_PREFIX) :]

            # Defensive: we never ALTER PK columns here.
            if col_name in pk_cols:
                continue

            if action == "delete":
                await conn.execute(
                    f'ALTER TABLE {qualified_name} DROP COLUMN IF EXISTS "{col_name}"'
                )
                continue

            desired_col = non_pk_col_by_name.get(col_name)
            if desired_col is None:
                # If the desired schema no longer mentions this column, treat
                # it as a no-op here; "delete" should have been emitted.
                continue

            if action == "insert":
                async with conn.transaction():
                    await conn.execute(
                        f"ALTER TABLE {qualified_name} "
                        f'ADD COLUMN "{col_name}" {desired_col.type}'
                    )
                continue

            if action == "upsert":
                await conn.execute(
                    f"ALTER TABLE {qualified_name} "
                    f'ADD COLUMN IF NOT EXISTS "{col_name}" {desired_col.type}'
                )
                continue

            if action == "replace":
                # Type change may fail depending on existing data. Try ALTER TYPE first
                # inside a savepoint; if it fails, fall back to drop+add.
                try:
                    async with conn.transaction():
                        nullable = "" if desired_col.nullable else " NOT NULL"
                        await conn.execute(
                            f"ALTER TABLE {qualified_name} "
                            f'ALTER COLUMN "{col_name}" TYPE {desired_col.type}{nullable}'
                        )
                except asyncpg.PostgresError:
                    await conn.execute(
                        f'ALTER TABLE {qualified_name} DROP COLUMN IF EXISTS "{col_name}"'
                    )
                    await conn.execute(
                        f"ALTER TABLE {qualified_name} "
                        f'ADD COLUMN "{col_name}" {desired_col.type}'
                    )

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
            sink=self._sink,
            tracking_record=tracking_record,
        )


# Register the root target states provider
_table_provider = coco.register_root_target_states_provider(
    "cocoindex.io/postgres/table", _TableHandler()
)


class TableTarget(
    Generic[RowT, coco.MaybePendingS], coco.ResolvesTo["TableTarget[RowT]"]
):
    """
    A target for writing rows to a PostgreSQL table.

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


class PgDatabase(connection.KeyedConnection[asyncpg.Pool]):
    """
    Handle for a registered PostgreSQL database.

    Use `register_db()` to create an instance. Can be used as a context manager
    to automatically unregister on exit.

    Example:
        ```python
        # Without context manager (manual lifecycle)
        db = register_db("my_db", pool)
        # ... use db ...

        # With context manager (auto-unregister on exit)
        with register_db("my_db", pool) as db:
            # ... use db ...
        # db is automatically unregistered here
        ```
    """

    def declare_table_target(
        self,
        table_name: str,
        table_schema: TableSchema[RowT],
        *,
        pg_schema_name: str | None = None,
        managed_by: Literal["system", "user"] = "system",
    ) -> TableTarget[RowT, coco.PendingS]:
        """
        Create a TableTarget for writing rows to a PostgreSQL table.

        Args:
            table_name: Name of the table.
            table_schema: Schema definition including columns and primary key.
            pg_schema_name: Optional PostgreSQL schema name (default is "public").
            managed_by: Whether the table is managed by "system" (CocoIndex creates/drops it)
                        or "user" (table must exist, CocoIndex only manages rows).

        Returns:
            A TableTarget that can be used to declare rows.
        """
        key = _TableKey(
            db_key=self.key,
            pg_schema_name=pg_schema_name,
            table_name=table_name,
        )
        spec = _TableSpec(
            table_schema=table_schema,
            managed_by=managed_by,
        )
        provider = coco.declare_target_state_with_child(
            _table_provider.target_state(key, spec)
        )
        return TableTarget(provider, table_schema)


def register_db(key: str, pool: asyncpg.Pool) -> PgDatabase:
    """
    Register a PostgreSQL database connection pool with a stable key.

    The key should be stable across runs - it identifies the logical database.
    The pool can be recreated with different connection parameters (host, password, etc.)
    as long as the same key is used.

    Can be used as a context manager to automatically unregister on exit.

    Args:
        key: A stable identifier for this database (e.g., "main_db", "analytics").
             Must be unique - raises ValueError if a database with this key
             is already registered.
        pool: An asyncpg connection pool.

    Returns:
        A PgDatabase handle that can be used to create table targets.

    Raises:
        ValueError: If a database with the given key is already registered.

    Example:
        ```python
        async def setup():
            pool = await asyncpg.create_pool("postgresql://localhost/mydb")

            # Option 1: Manual lifecycle
            db = register_db("my_db", pool)

            # Option 2: Context manager (auto-unregister on exit)
            with register_db("my_db", pool) as db:
                table = db.table_target(scope, "my_table", schema)
            # db is automatically unregistered here
        ```
    """
    _db_registry.register(key, pool)
    return PgDatabase(_db_registry.name, key, pool, _db_registry)


async def create_pool(
    dsn: str | None = None,
    *,
    init: Callable[[asyncpg.Connection], Any] | None = None,
    **kwargs: Any,
) -> asyncpg.Pool:
    """
    Create an asyncpg connection pool, registering extensions needed by the postgres connector on each connection.

    Args:
        dsn: Connection string or None.
        init: Optional connection initialization callback. If provided,
              it will be called for each connection in the pool.
        **kwargs: All other arguments are passed to `asyncpg.create_pool()`.

    Returns:
        An asyncpg connection pool.

    Example:
        ```python
        pool = await create_pool("postgresql://localhost/mydb")
        ```
    """

    async def _init_with_pgvector(conn: asyncpg.Connection) -> None:
        await pgvector.asyncpg.register_vector(conn)
        if init is not None:
            result = init(conn)
            if inspect.isawaitable(result):
                await result

    return await asyncpg.create_pool(dsn, init=_init_with_pgvector, **kwargs)


__all__ = [
    "ColumnDef",
    "ValueEncoder",
    "PgDatabase",
    "PgType",
    "TableSchema",
    "TableTarget",
    "create_pool",
    "register_db",
]
