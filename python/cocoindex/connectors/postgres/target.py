"""
PostgreSQL target for CocoIndex.

This module provides a two-level effect system for PostgreSQL:
1. Table level: Creates/drops tables in the database
2. Row level: Upserts/deletes rows within tables
"""

from __future__ import annotations

import datetime
import decimal
import hashlib
import ipaddress
import json
import threading
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

import numpy as np

import cocoindex as coco
from cocoindex._internal.datatype import (
    AnyType,
    MappingType,
    SequenceType,
    StructType,
    UnionType,
    analyze_type_info,
    is_struct_type,
)

try:
    import asyncpg
except ImportError as e:
    raise ImportError(
        "asyncpg is required for PostgreSQL target. "
        "Install it with: pip install asyncpg"
    ) from e


# Type aliases
_RowKey = tuple[Any, ...]  # Primary key values as tuple
_RowValue = dict[str, Any]  # Column name -> value
_RowFingerprint = bytes
Encoder = Callable[[Any], Any]


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
    encoder: Encoder | None = None


def _json_encoder(value: Any) -> str:
    """Encode a value to JSON string for asyncpg."""
    return json.dumps(value, default=str)


class _TypeMapping(NamedTuple):
    """Mapping from Python type to PostgreSQL type with optional encoder."""

    pg_type: str
    encoder: Encoder | None = None


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


def _get_type_mapping(python_type: Any) -> _TypeMapping:
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

    # NumPy number types
    if isinstance(base_type, type):
        if issubclass(base_type, np.integer):
            return _TypeMapping("bigint")
        if issubclass(base_type, np.floating):
            return _TypeMapping("double precision")

    # Complex types that need JSON encoding
    if isinstance(
        type_info.variant, (SequenceType, MappingType, StructType, UnionType, AnyType)
    ):
        return _JSONB_MAPPING

    # Default fallback
    return _JSONB_MAPPING


class ColumnDef(NamedTuple):
    """Definition of a table column."""

    name: str
    type: str  # PostgreSQL type (e.g., "text", "bigint", "jsonb", "vector(384)")
    nullable: bool = True
    encoder: Encoder | None = (
        None  # Optional encoder to convert value before sending to asyncpg
    )


# Type variable for row type
RowT = TypeVar("RowT", default=dict[str, Any])


class TableSchema(Generic[RowT]):
    """Schema definition for a PostgreSQL table."""

    columns: list[ColumnDef]
    primary_key: list[str]  # Column names that form the primary key
    row_type: type[RowT] | None  # The row type, if provided

    @overload
    def __init__(
        self: "TableSchema[dict[str, Any]]",
        columns: list[ColumnDef],
        primary_key: list[str],
    ) -> None: ...

    @overload
    def __init__(
        self: "TableSchema[RowT]",
        columns: type[RowT],
        primary_key: list[str],
    ) -> None: ...

    def __init__(
        self,
        columns: type[RowT] | list[ColumnDef],
        primary_key: list[str],
    ) -> None:
        """
        Create a TableSchema.

        Args:
            columns: Either a struct type (dataclass, NamedTuple, or Pydantic model)
                     or a list of ColumnDef objects.
                     When a struct type is provided, Python types are automatically
                     mapped to PostgreSQL types based on asyncpg's type conversion.
            primary_key: List of column names that form the primary key.
        """
        if isinstance(columns, list):
            self.columns = columns
            self.row_type = None
        elif is_struct_type(columns):
            self.columns = self._columns_from_struct_type(columns)
            self.row_type = columns  # type: ignore[assignment]
        else:
            raise TypeError(
                f"columns must be a struct type (dataclass, NamedTuple, Pydantic model) "
                f"or a list of ColumnDef, got {type(columns)}"
            )

        self.primary_key = primary_key

        # Validate primary key columns exist
        col_names = {c.name for c in self.columns}
        for pk in self.primary_key:
            if pk not in col_names:
                raise ValueError(
                    f"Primary key column '{pk}' not found in columns: {col_names}"
                )

    @staticmethod
    def _columns_from_struct_type(struct_type: type) -> list[ColumnDef]:
        """Convert a struct type to a list of ColumnDef."""
        struct_info = StructType(struct_type)
        columns: list[ColumnDef] = []

        for field in struct_info.fields:
            type_info = analyze_type_info(field.type_hint)
            type_mapping = _get_type_mapping(field.type_hint)
            columns.append(
                ColumnDef(
                    name=field.name,
                    type=type_mapping.pg_type,
                    nullable=type_info.nullable,
                    encoder=type_mapping.encoder,
                )
            )

        return columns


class _RowAction(NamedTuple):
    """Action to perform on a row."""

    key: _RowKey
    value: _RowValue | None  # None means delete


class _RowHandler(coco.EffectHandler[_RowKey, _RowValue, _RowFingerprint]):
    """Handler for row-level effects within a table."""

    _pool: asyncpg.Pool
    _table_name: str
    _schema_name: str | None
    _table_schema: TableSchema
    _sink: coco.EffectSink[_RowAction]

    def __init__(
        self,
        pool: asyncpg.Pool,
        table_name: str,
        schema_name: str | None,
        table_schema: TableSchema,
    ) -> None:
        self._pool = pool
        self._table_name = table_name
        self._schema_name = schema_name
        self._table_schema = table_schema
        self._sink = coco.EffectSink.from_async_fn(self._apply_actions)

    def _qualified_table_name(self) -> str:
        if self._schema_name:
            return f'"{self._schema_name}"."{self._table_name}"'
        return f'"{self._table_name}"'

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
        table_name = self._qualified_table_name()
        columns = self._table_schema.columns
        pk_cols = self._table_schema.primary_key
        all_col_names = [c.name for c in columns]
        non_pk_cols = [c for c in all_col_names if c not in pk_cols]

        # Build column lists
        col_list = ", ".join(f'"{c}"' for c in all_col_names)
        placeholders = ", ".join(f"${i + 1}" for i in range(len(all_col_names)))
        pk_list = ", ".join(f'"{c}"' for c in pk_cols)

        # Build ON CONFLICT clause
        if non_pk_cols:
            update_list = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in non_pk_cols)
            conflict_clause = f"ON CONFLICT ({pk_list}) DO UPDATE SET {update_list}"
        else:
            conflict_clause = f"ON CONFLICT ({pk_list}) DO NOTHING"

        sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders}) {conflict_clause}"

        for action in upserts:
            assert action.value is not None
            # Values are encoded by TableTarget before being stored as effect values.
            values = [action.value.get(col.name) for col in columns]
            await conn.execute(sql, *values)

    async def _execute_deletes(
        self,
        conn: asyncpg.pool.PoolConnectionProxy[asyncpg.Record],
        deletes: list[_RowAction],
    ) -> None:
        """Execute delete operations."""
        table_name = self._qualified_table_name()
        pk_cols = self._table_schema.primary_key

        # Build WHERE clause for primary key
        where_parts = [f'"{c}" = ${i + 1}' for i, c in enumerate(pk_cols)]
        where_clause = " AND ".join(where_parts)
        sql = f"DELETE FROM {table_name} WHERE {where_clause}"

        for action in deletes:
            await conn.execute(sql, *action.key)

    def _compute_fingerprint(self, value: _RowValue) -> _RowFingerprint:
        """Compute a fingerprint for row data."""
        # Serialize deterministically
        serialized = json.dumps(value, sort_keys=True, default=str)
        return hashlib.blake2b(serialized.encode()).digest()

    def reconcile(
        self,
        key: _RowKey,
        desired_effect: _RowValue | coco.NonExistenceType,
        prev_possible_states: Collection[_RowFingerprint],
        prev_may_be_missing: bool,
        /,
    ) -> coco.EffectReconcileOutput[_RowAction, _RowFingerprint] | None:
        if coco.is_non_existence(desired_effect):
            # Delete case - only if it might exist
            if not prev_possible_states and not prev_may_be_missing:
                return None
            return coco.EffectReconcileOutput(
                action=_RowAction(key=key, value=None),
                sink=self._sink,
                state=coco.NON_EXISTENCE,
            )

        # Upsert case
        target_fp = self._compute_fingerprint(desired_effect)
        if not prev_may_be_missing and all(
            prev == target_fp for prev in prev_possible_states
        ):
            # No change needed
            return None

        return coco.EffectReconcileOutput(
            action=_RowAction(key=key, value=desired_effect),
            sink=self._sink,
            state=target_fp,
        )


class _TableKey(NamedTuple):
    """Key identifying a table: (database_key, table_name)."""

    db_key: str  # Stable key for the database
    table_name: str


@dataclass
class _TableSpec:
    """Specification for a PostgreSQL table."""

    db_key: str  # Stable key for looking up the database pool
    table_name: str
    table_schema: TableSchema[Any]
    schema_name: str | None = None
    managed_by: Literal["system", "user"] = "system"


@dataclass
class _TableState:
    """Stored state for a table."""

    db_key: str
    table_name: str
    schema_name: str | None
    table_schema_hash: str  # Hash of the schema for change detection
    managed_by: Literal["system", "user"]


class _TableAction(NamedTuple):
    """Action to perform on a table."""

    db_key: str  # Database key for looking up the pool
    spec: _TableSpec | None  # None means table should not exist
    action_type: (
        Literal["create", "ensure", "drop"] | None
    )  # None means no action needed
    prev_table_names_to_drop: list[tuple[str, str | None]]  # (table_name, schema_name)


# Database registry: maps stable keys to connection pools
_db_registry: dict[str, asyncpg.Pool] = {}
_db_registry_lock = threading.Lock()


def _get_pool(db_key: str) -> asyncpg.Pool:
    """Get the connection pool for the given database key."""
    with _db_registry_lock:
        pool = _db_registry.get(db_key)
    if pool is None:
        raise RuntimeError(
            f"No database registered with key '{db_key}'. Call register_db() first."
        )
    return pool


def _register_db(key: str, pool: asyncpg.Pool) -> None:
    """Register a database pool (internal, with lock)."""
    with _db_registry_lock:
        if key in _db_registry:
            raise ValueError(
                f"Database with key '{key}' is already registered. "
                f"Use a different key or unregister the existing one first."
            )
        _db_registry[key] = pool


def _unregister_db(key: str) -> None:
    """Unregister a database pool (internal, with lock)."""
    with _db_registry_lock:
        _db_registry.pop(key, None)


def _schema_hash(table_schema: TableSchema) -> str:
    """Compute a hash of the table schema for change detection."""
    data = {
        "columns": [
            {"name": c.name, "type": c.type, "nullable": c.nullable}
            for c in table_schema.columns
        ],
        "primary_key": table_schema.primary_key,
    }
    serialized = json.dumps(data, sort_keys=True)
    return hashlib.sha256(serialized.encode()).hexdigest()


class _TableHandler(
    coco.EffectHandler[_TableKey, _TableSpec, _TableState, _RowHandler]
):
    """Handler for table-level effects."""

    _sink: coco.EffectSink[_TableAction, _RowHandler]

    def __init__(self) -> None:
        self._sink = coco.EffectSink.from_async_fn(self._apply_actions)

    async def _apply_actions(
        self, actions: Collection[_TableAction]
    ) -> list[coco.ChildEffectDef[_RowHandler] | None]:
        """Apply table actions and return child handlers."""
        outputs: list[coco.ChildEffectDef[_RowHandler] | None] = []

        for action in actions:
            pool = _get_pool(action.db_key)

            # Handle dropping previous tables
            for table_name, schema_name in action.prev_table_names_to_drop:
                await self._drop_table(pool, table_name, schema_name)

            # Handle current table
            if action.spec is None or action.action_type == "drop":
                if action.spec and action.action_type == "drop":
                    await self._drop_table(
                        pool, action.spec.table_name, action.spec.schema_name
                    )
                outputs.append(None)
            else:
                if action.action_type == "create":
                    await self._create_table(pool, action.spec)
                elif action.action_type == "ensure":
                    await self._ensure_table(pool, action.spec)

                outputs.append(
                    coco.ChildEffectDef(
                        handler=_RowHandler(
                            pool=pool,
                            table_name=action.spec.table_name,
                            schema_name=action.spec.schema_name,
                            table_schema=action.spec.table_schema,
                        )
                    )
                )

        return outputs

    async def _drop_table(
        self, pool: asyncpg.Pool, table_name: str, schema_name: str | None
    ) -> None:
        """Drop a table if it exists."""
        qualified_name = self._qualified_table_name(table_name, schema_name)
        async with pool.acquire() as conn:
            await conn.execute(f"DROP TABLE IF EXISTS {qualified_name}")

    async def _create_table(self, pool: asyncpg.Pool, spec: _TableSpec) -> None:
        """Create a table."""
        qualified_name = self._qualified_table_name(spec.table_name, spec.schema_name)
        schema = spec.table_schema

        # Create schema if specified
        if spec.schema_name:
            async with pool.acquire() as conn:
                await conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{spec.schema_name}"')

        # Build column definitions
        col_defs = []
        for col in schema.columns:
            nullable = (
                ""
                if col.nullable and col.name not in schema.primary_key
                else " NOT NULL"
            )
            col_defs.append(f'"{col.name}" {col.type}{nullable}')

        # Build primary key constraint
        pk_cols = ", ".join(f'"{c}"' for c in schema.primary_key)
        col_defs.append(f"PRIMARY KEY ({pk_cols})")

        columns_sql = ", ".join(col_defs)
        sql = f"CREATE TABLE IF NOT EXISTS {qualified_name} ({columns_sql})"

        async with pool.acquire() as conn:
            await conn.execute(sql)

    async def _ensure_table(self, pool: asyncpg.Pool, spec: _TableSpec) -> None:
        """Ensure a table exists with the correct schema."""
        # For now, just create if not exists
        # TODO: Add schema migration support
        await self._create_table(pool, spec)

    @staticmethod
    def _qualified_table_name(table_name: str, schema_name: str | None) -> str:
        if schema_name:
            return f'"{schema_name}"."{table_name}"'
        return f'"{table_name}"'

    def reconcile(
        self,
        key: _TableKey,
        desired_effect: _TableSpec | coco.NonExistenceType,
        prev_possible_states: Collection[_TableState],
        prev_may_be_missing: bool,
        /,
    ) -> coco.EffectReconcileOutput[_TableAction, _TableState, _RowHandler]:
        db_key = key.db_key
        # Determine what previous tables need to be dropped (table_name, schema_name)
        prev_tables_to_drop: list[tuple[str, str | None]] = []

        if not coco.is_non_existence(desired_effect):
            # Check if we need to drop any previous tables that differ
            for prev in prev_possible_states:
                if prev.managed_by == "system":
                    # Check if this is a different table (name or schema changed)
                    if (
                        prev.table_name != desired_effect.table_name
                        or prev.schema_name != desired_effect.schema_name
                    ):
                        prev_tables_to_drop.append((prev.table_name, prev.schema_name))

        if coco.is_non_existence(desired_effect):
            # Drop the table
            for prev in prev_possible_states:
                if prev.managed_by == "system":
                    prev_tables_to_drop.append((prev.table_name, prev.schema_name))

            return coco.EffectReconcileOutput(
                action=_TableAction(
                    db_key=db_key,
                    spec=None,
                    action_type=None,
                    prev_table_names_to_drop=prev_tables_to_drop,
                ),
                sink=self._sink,
                state=coco.NON_EXISTENCE,
            )

        # Determine action type
        current_hash = _schema_hash(desired_effect.table_schema)
        must_exist = not prev_may_be_missing and all(
            prev.table_name == desired_effect.table_name
            and prev.schema_name == desired_effect.schema_name
            and prev.table_schema_hash == current_hash
            and prev.managed_by == desired_effect.managed_by
            for prev in prev_possible_states
        )

        may_exist = any(
            prev.table_name == desired_effect.table_name
            and prev.schema_name == desired_effect.schema_name
            for prev in prev_possible_states
        )

        if must_exist:
            action_type: Literal["create", "ensure", "drop"] | None = None
        elif may_exist:
            action_type = "ensure"
        else:
            action_type = "create"

        new_state = _TableState(
            db_key=db_key,
            table_name=desired_effect.table_name,
            schema_name=desired_effect.schema_name,
            table_schema_hash=current_hash,
            managed_by=desired_effect.managed_by,
        )

        return coco.EffectReconcileOutput(
            action=_TableAction(
                db_key=db_key,
                spec=desired_effect,
                action_type=action_type,
                prev_table_names_to_drop=prev_tables_to_drop,
            ),
            sink=self._sink,
            state=new_state,
        )


# Register the root effect provider
_table_provider = coco.register_root_effect_provider(
    "cocoindex.io/postgres/table", _TableHandler()
)


class TableTarget(
    Generic[RowT, coco.MaybePendingS], coco.ResolvesTo["TableTarget[RowT]"]
):
    """
    A target for writing rows to a PostgreSQL table.

    The table is managed as an effect, with the scope used to scope the effect.

    Type Parameters:
        RowT: The type of row objects (dict, dataclass, NamedTuple, or Pydantic model).
    """

    _provider: coco.EffectProvider[_RowKey, _RowValue, None, coco.MaybePendingS]
    _table_schema: TableSchema[RowT]

    def __init__(
        self,
        provider: coco.EffectProvider[_RowKey, _RowValue, None, coco.MaybePendingS],
        table_schema: TableSchema[RowT],
    ) -> None:
        self._provider = provider
        self._table_schema = table_schema

    def declare_row(
        self: "TableTarget[RowT, Any]", scope: coco.Scope, *, row: RowT
    ) -> None:
        """
        Declare a row to be upserted to this table.

        Args:
            scope: The scope for effect declaration.
            row: A row object (dict, dataclass, NamedTuple, or Pydantic model).
                 Must include all primary key columns.
        """
        row_dict = self._row_to_dict(row)
        # Extract primary key values
        pk_values = tuple(row_dict[pk] for pk in self._table_schema.primary_key)
        coco.declare_effect(scope, self._provider.effect(pk_values, row_dict))

    def _row_to_dict(self, row: RowT) -> dict[str, Any]:
        """
        Convert a row (dict or object) into dict[str, Any] using the schema columns,
        and apply column encoders for both dict and object inputs.
        """
        out: dict[str, Any] = {}
        for col in self._table_schema.columns:
            if isinstance(row, dict):
                value = row.get(col.name)  # type: ignore[union-attr]
            else:
                value = getattr(row, col.name)

            if value is not None and col.encoder is not None:
                value = col.encoder(value)
            out[col.name] = value
        return out


class PgDatabase:
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

    _key: str

    def __init__(self, key: str) -> None:
        self._key = key

    @property
    def key(self) -> str:
        """The stable key for this database."""
        return self._key

    def __enter__(self) -> "PgDatabase":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        _unregister_db(self._key)

    def table_target(
        self,
        scope: coco.Scope,
        table_name: str,
        table_schema: TableSchema[RowT],
        *,
        schema_name: str | None = None,
        managed_by: Literal["system", "user"] = "system",
    ) -> TableTarget[RowT, coco.PendingS]:
        """
        Create a TableTarget for writing rows to a PostgreSQL table.

        Args:
            scope: The scope for effect declaration.
            table_name: Name of the table.
            table_schema: Schema definition including columns and primary key.
            schema_name: Optional PostgreSQL schema name (default is "public").
            managed_by: Whether the table is managed by "system" (CocoIndex creates/drops it)
                        or "user" (table must exist, CocoIndex only manages rows).

        Returns:
            A TableTarget that can be used to declare rows.
        """
        key = _TableKey(db_key=self._key, table_name=table_name)
        spec = _TableSpec(
            db_key=self._key,
            table_name=table_name,
            table_schema=table_schema,
            schema_name=schema_name,
            managed_by=managed_by,
        )
        provider = coco.declare_effect_with_child(
            scope, _table_provider.effect(key, spec)
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
    _register_db(key, pool)
    return PgDatabase(key)


__all__ = [
    "ColumnDef",
    "Encoder",
    "PgDatabase",
    "PgType",
    "TableSchema",
    "TableTarget",
    "register_db",
]
