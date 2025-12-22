"""
PostgreSQL target for CocoIndex.

This module provides a two-level effect system for PostgreSQL:
1. Table level: Creates/drops tables in the database
2. Row level: Upserts/deletes rows within tables
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import (
    Any,
    Collection,
    Generic,
    Literal,
    NamedTuple,
    Sequence,
)

import cocoindex as coco

try:
    import asyncpg  # type: ignore[import-untyped]
except ImportError as e:
    raise ImportError(
        "asyncpg is required for PostgreSQL target. "
        "Install it with: pip install asyncpg"
    ) from e


# Type aliases
_RowKey = tuple[Any, ...]  # Primary key values as tuple
_RowValue = dict[str, Any]  # Column name -> value
_RowFingerprint = bytes


@dataclass
class ColumnDef:
    """Definition of a table column."""

    name: str
    type: str  # PostgreSQL type (e.g., "text", "integer", "jsonb", "vector(384)")
    nullable: bool = True


@dataclass
class TableSchema:
    """Schema definition for a PostgreSQL table."""

    columns: list[ColumnDef]
    primary_key: list[str]  # Column names that form the primary key

    def __post_init__(self) -> None:
        col_names = {c.name for c in self.columns}
        for pk in self.primary_key:
            if pk not in col_names:
                raise ValueError(
                    f"Primary key column '{pk}' not found in columns: {col_names}"
                )


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
        self, conn: asyncpg.Connection, upserts: list[_RowAction]
    ) -> None:
        """Execute upsert operations."""
        table_name = self._qualified_table_name()
        pk_cols = self._table_schema.primary_key
        all_cols = [c.name for c in self._table_schema.columns]
        non_pk_cols = [c for c in all_cols if c not in pk_cols]

        # Build column lists
        col_list = ", ".join(f'"{c}"' for c in all_cols)
        placeholders = ", ".join(f"${i + 1}" for i in range(len(all_cols)))
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
            # Build values in column order
            values = [self._convert_value(action.value.get(c)) for c in all_cols]
            await conn.execute(sql, *values)

    async def _execute_deletes(
        self, conn: asyncpg.Connection, deletes: list[_RowAction]
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

    def _convert_value(self, value: Any) -> Any:
        """Convert Python values to PostgreSQL-compatible values."""
        if value is None:
            return None
        if isinstance(value, (list, dict)):
            return json.dumps(value)
        return value

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
    """Key identifying a table."""

    # Exactly one should be set
    stable_key: coco.StableKey | None = None
    table_name: str | None = None


@dataclass
class _TableSpec:
    """Specification for a PostgreSQL table."""

    database_url: str
    table_name: str
    table_schema: TableSchema
    schema_name: str | None = None
    managed_by: Literal["system", "user"] = "system"
    # Optional credentials (if not in URL)
    username: str | None = None
    password: str | None = None


@dataclass
class _TableState:
    """Stored state for a table."""

    database_url: str
    table_name: str
    schema_name: str | None
    table_schema_hash: str  # Hash of the schema for change detection
    managed_by: Literal["system", "user"]


class _TableAction(NamedTuple):
    """Action to perform on a table."""

    spec: _TableSpec | None  # None means table should not exist
    action_type: (
        Literal["create", "ensure", "drop"] | None
    )  # None means no action needed
    prev_specs_to_drop: list[_TableSpec]  # Previous tables to drop


# Connection pool cache
_pool_cache: dict[str, asyncpg.Pool] = {}


async def _get_pool(spec: _TableSpec) -> asyncpg.Pool:
    """Get or create a connection pool for the given spec."""
    # Build connection string
    url = spec.database_url
    cache_key = url

    if cache_key in _pool_cache:
        pool = _pool_cache[cache_key]
        # Check if pool is still valid
        try:
            async with pool.acquire() as conn:
                await conn.execute("SELECT 1")
            return pool
        except (asyncpg.PostgresError, OSError):
            # Pool is invalid, remove from cache
            del _pool_cache[cache_key]

    # Create new pool
    connect_kwargs: dict[str, Any] = {}
    if spec.username:
        connect_kwargs["user"] = spec.username
    if spec.password:
        connect_kwargs["password"] = spec.password

    pool = await asyncpg.create_pool(url, **connect_kwargs)
    if pool is None:
        raise RuntimeError(f"Failed to create connection pool for {url}")

    _pool_cache[cache_key] = pool
    return pool


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
        self._sink = coco.EffectSink.from_async_fn(self._apply_actions)  # type: ignore[arg-type]

    async def _apply_actions(
        self, actions: Sequence[_TableAction]
    ) -> Sequence[coco.ChildEffectDef[_RowHandler] | None]:
        """Apply table actions and return child handlers."""
        outputs: list[coco.ChildEffectDef[_RowHandler] | None] = []

        for action in actions:
            # Handle dropping previous tables
            for prev_spec in action.prev_specs_to_drop:
                await self._drop_table(prev_spec)

            # Handle current table
            if action.spec is None or action.action_type == "drop":
                if action.spec and action.action_type == "drop":
                    await self._drop_table(action.spec)
                outputs.append(None)
            else:
                pool = await _get_pool(action.spec)

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

    async def _drop_table(self, spec: _TableSpec) -> None:
        """Drop a table if it exists and is system-managed."""
        if spec.managed_by != "system":
            return

        pool = await _get_pool(spec)
        table_name = self._qualified_table_name(spec)

        async with pool.acquire() as conn:
            await conn.execute(f"DROP TABLE IF EXISTS {table_name}")

    async def _create_table(self, pool: asyncpg.Pool, spec: _TableSpec) -> None:
        """Create a table."""
        table_name = self._qualified_table_name(spec)
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
        sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_sql})"

        async with pool.acquire() as conn:
            await conn.execute(sql)

    async def _ensure_table(self, pool: asyncpg.Pool, spec: _TableSpec) -> None:
        """Ensure a table exists with the correct schema."""
        # For now, just create if not exists
        # TODO: Add schema migration support
        await self._create_table(pool, spec)

    def _qualified_table_name(self, spec: _TableSpec) -> str:
        if spec.schema_name:
            return f'"{spec.schema_name}"."{spec.table_name}"'
        return f'"{spec.table_name}"'

    def reconcile(
        self,
        key: _TableKey,
        desired_effect: _TableSpec | coco.NonExistenceType,
        prev_possible_states: Collection[_TableState],
        prev_may_be_missing: bool,
        /,
    ) -> coco.EffectReconcileOutput[_TableAction, _TableState, _RowHandler]:
        # Determine what previous tables need to be dropped
        prev_specs_to_drop: list[_TableSpec] = []

        if not coco.is_non_existence(desired_effect):
            # Check if we need to drop any previous tables that differ
            for prev in prev_possible_states:
                if prev.managed_by == "system":
                    # Check if this is a different table
                    if (
                        prev.table_name != desired_effect.table_name
                        or prev.schema_name != desired_effect.schema_name
                        or prev.database_url != desired_effect.database_url
                    ):
                        prev_specs_to_drop.append(
                            _TableSpec(
                                database_url=prev.database_url,
                                table_name=prev.table_name,
                                schema_name=prev.schema_name,
                                table_schema=TableSchema(columns=[], primary_key=[]),
                                managed_by=prev.managed_by,
                            )
                        )

        if coco.is_non_existence(desired_effect):
            # Drop the table
            for prev in prev_possible_states:
                if prev.managed_by == "system":
                    prev_specs_to_drop.append(
                        _TableSpec(
                            database_url=prev.database_url,
                            table_name=prev.table_name,
                            schema_name=prev.schema_name,
                            table_schema=TableSchema(columns=[], primary_key=[]),
                            managed_by=prev.managed_by,
                        )
                    )

            return coco.EffectReconcileOutput(
                action=_TableAction(
                    spec=None,
                    action_type=None,
                    prev_specs_to_drop=prev_specs_to_drop,
                ),
                sink=self._sink,
                state=coco.NON_EXISTENCE,
            )

        # Determine action type
        current_hash = _schema_hash(desired_effect.table_schema)
        must_exist = not prev_may_be_missing and all(
            prev.table_name == desired_effect.table_name
            and prev.schema_name == desired_effect.schema_name
            and prev.database_url == desired_effect.database_url
            and prev.table_schema_hash == current_hash
            and prev.managed_by == desired_effect.managed_by
            for prev in prev_possible_states
        )

        may_exist = any(
            prev.table_name == desired_effect.table_name
            and prev.schema_name == desired_effect.schema_name
            and prev.database_url == desired_effect.database_url
            for prev in prev_possible_states
        )

        if must_exist:
            action_type: Literal["create", "ensure", "drop"] | None = None
        elif may_exist:
            action_type = "ensure"
        else:
            action_type = "create"

        new_state = _TableState(
            database_url=desired_effect.database_url,
            table_name=desired_effect.table_name,
            schema_name=desired_effect.schema_name,
            table_schema_hash=current_hash,
            managed_by=desired_effect.managed_by,
        )

        return coco.EffectReconcileOutput(
            action=_TableAction(
                spec=desired_effect,
                action_type=action_type,
                prev_specs_to_drop=prev_specs_to_drop,
            ),
            sink=self._sink,
            state=new_state,
        )


# Register the root effect provider
_table_provider = coco.register_root_effect_provider(
    "cocoindex.io/postgres/table", _TableHandler()
)


class TableTarget(Generic[coco.MaybePendingS], coco.ResolvesTo["TableTarget"]):
    """
    A target for writing rows to a PostgreSQL table.

    The table is managed as an effect, with the scope used to scope the effect.
    """

    _provider: coco.EffectProvider[_RowKey, _RowValue, None, coco.MaybePendingS]
    _table_schema: TableSchema

    def __init__(
        self,
        provider: coco.EffectProvider[_RowKey, _RowValue, None, coco.MaybePendingS],
        table_schema: TableSchema,
    ) -> None:
        self._provider = provider
        self._table_schema = table_schema

    def declare_row(
        self: TableTarget, scope: coco.Scope, *, row: dict[str, Any]
    ) -> None:
        """
        Declare a row to be upserted to this table.

        Args:
            scope: The scope for effect declaration.
            row: A dictionary mapping column names to values.
                 Must include all primary key columns.
        """
        # Extract primary key values
        pk_values = tuple(row[pk] for pk in self._table_schema.primary_key)
        coco.declare_effect(scope, self._provider.effect(pk_values, row))


@coco.function
def table_target(
    scope: coco.Scope,
    database_url: str,
    table_name: str,
    table_schema: TableSchema,
    *,
    stable_key: coco.StableKey | None = None,
    schema_name: str | None = None,
    managed_by: Literal["system", "user"] = "system",
    username: str | None = None,
    password: str | None = None,
) -> TableTarget[coco.PendingS]:
    """
    Create a TableTarget for writing rows to a PostgreSQL table.

    Args:
        scope: The scope for effect declaration.
        database_url: PostgreSQL connection URL (e.g., "postgresql://localhost:5432/mydb").
        table_name: Name of the table.
        table_schema: Schema definition including columns and primary key.
        stable_key: Optional stable key for identifying the table across schema changes.
        schema_name: Optional PostgreSQL schema name (default is "public").
        managed_by: Whether the table is managed by "system" (CocoIndex creates/drops it)
                    or "user" (table must exist, CocoIndex only manages rows).
        username: Optional username (if not in URL).
        password: Optional password (if not in URL).

    Returns:
        A TableTarget that can be used to declare rows.
    """
    key = (
        _TableKey(stable_key=stable_key)
        if stable_key is not None
        else _TableKey(table_name=table_name)
    )
    spec = _TableSpec(
        database_url=database_url,
        table_name=table_name,
        table_schema=table_schema,
        schema_name=schema_name,
        managed_by=managed_by,
        username=username,
        password=password,
    )
    provider = coco.declare_effect_with_child(scope, _table_provider.effect(key, spec))
    return TableTarget(provider, table_schema)


__all__ = ["ColumnDef", "TableSchema", "TableTarget", "table_target"]
