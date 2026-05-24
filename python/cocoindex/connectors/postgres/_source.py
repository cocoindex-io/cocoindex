"""PostgreSQL source utilities.

These helpers provide a read API for PostgreSQL tables. Change notifications
will be added later.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Generic,
    Iterator,
    Sequence,
    cast,
    overload,
)

from typing_extensions import TypeVar

import re

from cocoindex._internal.datatype import RecordType, is_record_type
from cocoindex._internal.stable_path import StableKey
from cocoindex.connectorkits.async_adapters import async_to_sync_iter


# Valid SQL identifier pattern: starts with letter or underscore, contains only letters, digits, underscores, or $ (for temp tables)
_VALID_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_$]*$")

try:
    import asyncpg  # type: ignore
except ImportError as e:
    raise ImportError(
        "asyncpg is required to use the PostgreSQL source connector. "
        "Please install cocoindex[postgres]."
    ) from e


RowT = TypeVar("RowT", default=dict[str, Any])


def _validate_identifier(name: str, identifier_type: str) -> None:
    """Validate that a string is a valid SQL identifier."""
    if not name:
        raise ValueError(f"{identifier_type} cannot be empty")
    if not _VALID_IDENTIFIER_RE.match(name):
        raise ValueError(
            f"Invalid {identifier_type}: '{name}'. "
            f"Must start with a letter or underscore and contain only letters, digits, underscores, or $"
        )


def _create_row_factory(
    row_type: type[RowT],
    field_names: frozenset[str],
) -> Callable[[dict[str, Any]], RowT]:
    """Create a row factory function from a record type and its field names."""

    def factory(row: dict[str, Any]) -> RowT:
        # Extract only fields that exist in the record type
        kwargs = {k: v for k, v in row.items() if k in field_names}
        return row_type(**kwargs)

    return factory


@dataclass
class PgSourceSpec:
    """Specification for a PostgreSQL source table."""

    table_name: str
    columns: Sequence[str] | None = None  # None means SELECT *
    pg_schema_name: str | None = None


class RowFetcher(Generic[RowT]):
    """A dual-mode iterator for fetching rows from a PostgreSQL table.

    Use as a sync iterator:
        for row in source.fetch_rows():
            ...

    Use as an async iterator:
        async for row in source.fetch_rows():
            ...
    """

    _pool: asyncpg.Pool
    _spec: PgSourceSpec
    _row_factory: Callable[[dict[str, Any]], RowT] | None

    def __init__(
        self,
        pool: asyncpg.Pool,
        spec: PgSourceSpec,
        row_factory: Callable[[dict[str, Any]], RowT] | None,
    ) -> None:
        self._pool = pool
        self._spec = spec
        self._row_factory = row_factory

    def _transform_row(self, row: dict[str, Any]) -> RowT:
        if self._row_factory is None:
            return cast(RowT, row)
        return self._row_factory(row)

    async def __aiter__(self) -> AsyncIterator[RowT]:
        """Asynchronously iterate over rows."""
        spec = self._spec

        # Validate identifiers to prevent SQL injection
        _validate_identifier(spec.table_name, "table name")
        if spec.pg_schema_name:
            _validate_identifier(spec.pg_schema_name, "schema name")
        if spec.columns:
            for col in spec.columns:
                _validate_identifier(col, "column name")

        if spec.columns:
            cols_sql = ", ".join(f'"{c}"' for c in spec.columns)
        else:
            cols_sql = "*"

        if spec.pg_schema_name:
            table_sql = f'"{spec.pg_schema_name}"."{spec.table_name}"'
        else:
            table_sql = f'"{spec.table_name}"'

        query = f"SELECT {cols_sql} FROM {table_sql}"

        async with self._pool.acquire() as conn:
            async with conn.transaction(isolation="repeatable_read", readonly=True):
                async for record in conn.cursor(query):
                    yield self._transform_row(dict(record))

    def __iter__(self) -> Iterator[RowT]:
        """Synchronously iterate over rows."""
        return async_to_sync_iter(self.__aiter__)

    async def items(
        self, key: Callable[[RowT], StableKey]
    ) -> AsyncIterator[tuple[StableKey, RowT]]:
        """Async iterate as (key, row) pairs for use with mount_each().

        Args:
            key: A function that extracts a StableKey from each row.
        """
        async for row in self:
            yield (key(row), row)


class PgTableSource(Generic[RowT]):
    """Source wrapper for PostgreSQL tables."""

    @overload
    def __init__(
        self: "PgTableSource[dict[str, Any]]",
        pool: asyncpg.Pool,
        *,
        table_name: str,
        columns: Sequence[str] | None = ...,
        pg_schema_name: str | None = ...,
        row_factory: None = ...,
        row_type: None = ...,
    ) -> None: ...

    @overload
    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        table_name: str,
        columns: Sequence[str] | None = ...,
        pg_schema_name: str | None = ...,
        row_factory: Callable[[dict[str, Any]], RowT],
        row_type: None = ...,
    ) -> None: ...

    @overload
    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        table_name: str,
        columns: Sequence[str] | None = ...,
        pg_schema_name: str | None = ...,
        row_factory: None = ...,
        row_type: type[RowT],
    ) -> None: ...

    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        table_name: str,
        columns: Sequence[str] | None = None,
        pg_schema_name: str | None = None,
        row_factory: Callable[[dict[str, Any]], RowT] | None = None,
        row_type: type[RowT] | None = None,
    ) -> None:
        if row_factory is not None and row_type is not None:
            raise ValueError("Cannot specify both row_factory and row_type")

        # Determine columns based on row_type
        resolved_columns: Sequence[str] | None = columns
        if row_type is not None:
            if not is_record_type(row_type):
                raise TypeError(
                    f"row_type must be a record type (dataclass, NamedTuple, or Pydantic model), "
                    f"got {row_type}"
                )
            record_info = RecordType(row_type)
            field_names = [f.name for f in record_info.fields]
            field_set = frozenset(field_names)

            if columns is not None:
                # Validate that all specified columns exist in the record type
                invalid_cols = [c for c in columns if c not in field_set]
                if invalid_cols:
                    raise ValueError(
                        f"Columns {invalid_cols} not found in row_type fields: {field_names}"
                    )
            else:
                # Use record type fields as columns
                resolved_columns = field_names

            row_factory = _create_row_factory(row_type, field_set)

        self._pool = pool
        self._spec = PgSourceSpec(
            table_name=table_name,
            columns=resolved_columns,
            pg_schema_name=pg_schema_name,
        )
        self._row_factory = row_factory

    def fetch_rows(self) -> RowFetcher[RowT]:
        """
        Return a dual-mode iterator for fetching rows from the table.

        Use as a sync iterator:
            for row in source.fetch_rows():
                ...

        Use as an async iterator:
            async for row in source.fetch_rows():
                ...
        """
        return RowFetcher(self._pool, self._spec, self._row_factory)


__all__ = [
    "PgSourceSpec",
    "PgTableSource",
    "RowFetcher",
]
