"""PostgreSQL source utilities.

These helpers provide a read API for PostgreSQL tables. Change notifications
will be added later.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Callable, Iterator, Sequence, TypeVar, cast

try:
    import asyncpg  # type: ignore
except ImportError as e:
    raise ImportError(
        "asyncpg is required to use the PostgreSQL source connector. "
        "Please install cocoindex[postgres]."
    ) from e


RowT = TypeVar("RowT")


@dataclass
class PgSourceSpec:
    """Specification for a PostgreSQL source table."""

    table_name: str
    columns: Sequence[str]
    pg_schema_name: str | None = None


async def read_table_async(
    pool: asyncpg.Pool,
    spec: PgSourceSpec,
) -> list[dict[str, Any]]:
    """Read all rows from a PostgreSQL table asynchronously."""
    if not spec.columns:
        raise ValueError("columns must be non-empty")

    cols_sql = ", ".join(f'"{c}"' for c in spec.columns)
    if spec.pg_schema_name:
        table_sql = f'"{spec.pg_schema_name}"."{spec.table_name}"'
    else:
        table_sql = f'"{spec.table_name}"'

    query = f"SELECT {cols_sql} FROM {table_sql}"

    async with pool.acquire() as conn:
        records = await conn.fetch(query)

    return [dict(record) for record in records]


def read_table(
    pool: asyncpg.Pool,
    spec: PgSourceSpec,
) -> list[dict[str, Any]]:
    """Read all rows from a PostgreSQL table synchronously."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(read_table_async(pool, spec))
    raise RuntimeError("read_table must not be called from a running event loop")


class PgTableSource:
    """Source wrapper for PostgreSQL tables."""

    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        table_name: str,
        columns: Sequence[str],
        pg_schema_name: str | None = None,
        row_factory: Callable[[dict[str, Any]], RowT] | None = None,
    ) -> None:
        self._pool = pool
        self._spec = PgSourceSpec(
            table_name=table_name,
            columns=columns,
            pg_schema_name=pg_schema_name,
        )
        self._row_factory = row_factory

    async def rows_async(self) -> list[RowT] | list[dict[str, Any]]:
        rows = await read_table_async(self._pool, self._spec)
        if self._row_factory is None:
            return rows
        return cast(list[RowT], [self._row_factory(row) for row in rows])

    def rows(self) -> list[RowT] | list[dict[str, Any]]:
        rows = read_table(self._pool, self._spec)
        if self._row_factory is None:
            return rows
        return cast(list[RowT], [self._row_factory(row) for row in rows])


__all__ = ["PgSourceSpec", "PgTableSource", "read_table", "read_table_async"]
