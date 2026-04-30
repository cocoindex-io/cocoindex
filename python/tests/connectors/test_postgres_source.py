"""Tests for PostgreSQL source connector.

Uses testcontainers to spin up a real PostgreSQL instance automatically.

Run with:
    pytest python/tests/connectors/test_postgres_source.py -v -s
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import asyncpg

import pytest
import pytest_asyncio

from cocoindex.connectors.postgres._source import PgTableSource

# =============================================================================
# Check dependencies
# =============================================================================

try:
    __import__("asyncpg")
    ASYNCPG_AVAILABLE = True
except ImportError:
    ASYNCPG_AVAILABLE = False

try:
    __import__("testcontainers")
    TESTCONTAINERS_AVAILABLE = True
except ImportError:
    TESTCONTAINERS_AVAILABLE = False

pytestmark = [
    pytest.mark.skipif(not ASYNCPG_AVAILABLE, reason="asyncpg not installed"),
    pytest.mark.skipif(
        not TESTCONTAINERS_AVAILABLE, reason="testcontainers not installed"
    ),
    pytest.mark.requires_docker,
    pytest.mark.timeout(120),
]


# =============================================================================
# Test utilities
# =============================================================================


def _unique_name(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


async def _create_table(
    pool: "asyncpg.Pool", table_name: str, schema: str | None = None
) -> None:
    qualified = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'
    async with pool.acquire() as conn:
        if schema:
            await conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        await conn.execute(
            f"CREATE TABLE {qualified} ("
            f"  id SERIAL PRIMARY KEY,"
            f"  name TEXT NOT NULL,"
            f"  value INT NOT NULL"
            f")"
        )


async def _insert_rows(
    pool: "asyncpg.Pool",
    table_name: str,
    rows: list[tuple[str, int]],
    schema: str | None = None,
) -> None:
    qualified = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'
    async with pool.acquire() as conn:
        await conn.executemany(
            f"INSERT INTO {qualified} (name, value) VALUES ($1, $2)", rows
        )


async def _drop_table(
    pool: "asyncpg.Pool", table_name: str, schema: str | None = None
) -> None:
    qualified = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'
    async with pool.acquire() as conn:
        await conn.execute(f"DROP TABLE IF EXISTS {qualified} CASCADE")
        if schema:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


# =============================================================================
# Fixtures
# =============================================================================


# Module-scoped: start the container once, share the DSN across all tests.
@pytest.fixture(scope="module")
def pg_dsn() -> Any:
    from testcontainers.postgres import PostgresContainer  # type: ignore[import-untyped]

    with PostgresContainer("postgres:16-alpine") as pg:
        dsn = pg.get_connection_url()
        # testcontainers may return a SQLAlchemy-style URL; normalize for asyncpg.
        dsn = dsn.replace("postgresql+psycopg2://", "postgresql://")
        yield dsn


# Function-scoped: each test gets a fresh pool bound to its own event loop.
@pytest_asyncio.fixture
async def pool(pg_dsn: str) -> Any:
    import asyncpg

    p = await asyncpg.create_pool(pg_dsn)
    assert p is not None
    yield p
    await p.close()


# =============================================================================
# Row types
# =============================================================================


@dataclass
class SimpleRow:
    id: int
    name: str
    value: int


# =============================================================================
# Tests — basic iteration
# =============================================================================


@pytest.mark.asyncio
async def test_fetch_rows_dict(pool: "asyncpg.Pool") -> None:
    """fetch_rows() returns dicts by default."""
    table_name = _unique_name("test_src")
    try:
        await _create_table(pool, table_name)
        await _insert_rows(pool, table_name, [("alice", 10), ("bob", 20)])

        source = PgTableSource(pool, table_name=table_name)
        rows: list[dict[str, Any]] = []
        async for row in source.fetch_rows():
            rows.append(row)

        assert len(rows) == 2
        names = {r["name"] for r in rows}
        assert names == {"alice", "bob"}
    finally:
        await _drop_table(pool, table_name)


@pytest.mark.asyncio
async def test_fetch_rows_row_type(pool: "asyncpg.Pool") -> None:
    """fetch_rows() with row_type returns typed dataclass instances."""
    table_name = _unique_name("test_src")
    try:
        await _create_table(pool, table_name)
        await _insert_rows(pool, table_name, [("alice", 10)])

        source = PgTableSource(pool, table_name=table_name, row_type=SimpleRow)
        rows: list[SimpleRow] = []
        async for row in source.fetch_rows():
            rows.append(row)

        assert len(rows) == 1
        assert isinstance(rows[0], SimpleRow)
        assert rows[0].name == "alice"
        assert rows[0].value == 10
    finally:
        await _drop_table(pool, table_name)


@pytest.mark.asyncio
async def test_fetch_rows_row_factory(pool: "asyncpg.Pool") -> None:
    """fetch_rows() with a custom row_factory."""
    table_name = _unique_name("test_src")
    try:
        await _create_table(pool, table_name)
        await _insert_rows(pool, table_name, [("alice", 10), ("bob", 20)])

        source = PgTableSource(
            pool,
            table_name=table_name,
            row_factory=lambda r: f"{r['name']}:{r['value']}",
        )
        rows: list[str] = []
        async for row in source.fetch_rows():
            rows.append(row)

        assert sorted(rows) == ["alice:10", "bob:20"]
    finally:
        await _drop_table(pool, table_name)


@pytest.mark.asyncio
async def test_fetch_rows_select_columns(pool: "asyncpg.Pool") -> None:
    """fetch_rows() with columns= only selects the specified columns."""
    table_name = _unique_name("test_src")
    try:
        await _create_table(pool, table_name)
        await _insert_rows(pool, table_name, [("alice", 10)])

        source = PgTableSource(pool, table_name=table_name, columns=["name"])
        rows: list[dict[str, Any]] = []
        async for row in source.fetch_rows():
            rows.append(row)

        assert len(rows) == 1
        assert "name" in rows[0]
        assert "value" not in rows[0]
        assert "id" not in rows[0]
    finally:
        await _drop_table(pool, table_name)


@pytest.mark.asyncio
async def test_fetch_rows_empty_table(pool: "asyncpg.Pool") -> None:
    """fetch_rows() on an empty table yields nothing."""
    table_name = _unique_name("test_src")
    try:
        await _create_table(pool, table_name)

        source = PgTableSource(pool, table_name=table_name)
        rows: list[dict[str, Any]] = []
        async for row in source.fetch_rows():
            rows.append(row)

        assert rows == []
    finally:
        await _drop_table(pool, table_name)


@pytest.mark.asyncio
async def test_fetch_rows_pg_schema(pool: "asyncpg.Pool") -> None:
    """fetch_rows() with pg_schema_name reads from the correct schema."""
    table_name = _unique_name("test_src")
    schema_name = _unique_name("test_schema")
    try:
        await _create_table(pool, table_name, schema=schema_name)
        await _insert_rows(pool, table_name, [("alice", 10)], schema=schema_name)

        source = PgTableSource(pool, table_name=table_name, pg_schema_name=schema_name)
        rows: list[dict[str, Any]] = []
        async for row in source.fetch_rows():
            rows.append(row)

        assert len(rows) == 1
        assert rows[0]["name"] == "alice"
    finally:
        await _drop_table(pool, table_name, schema=schema_name)


# =============================================================================
# Tests — items() keyed iteration
# =============================================================================


@pytest.mark.asyncio
async def test_items_iteration(pool: "asyncpg.Pool") -> None:
    """items() yields (key, row) pairs."""
    table_name = _unique_name("test_src")
    try:
        await _create_table(pool, table_name)
        await _insert_rows(pool, table_name, [("alice", 10), ("bob", 20)])

        source = PgTableSource(pool, table_name=table_name)
        items: list[tuple[Any, dict[str, Any]]] = []
        async for item in source.fetch_rows().items(key=lambda r: r["name"]):
            items.append(item)

        keys = {k for k, _ in items}
        assert keys == {"alice", "bob"}
        for k, row in items:
            assert row["name"] == k
    finally:
        await _drop_table(pool, table_name)


# =============================================================================
# Tests — snapshot isolation
# =============================================================================


@pytest.mark.asyncio
async def test_snapshot_isolation(pool: "asyncpg.Pool") -> None:
    """Rows inserted by another connection mid-iteration are not visible.

    This verifies that the cursor runs inside a REPEATABLE READ transaction,
    giving a consistent point-in-time snapshot.
    """
    table_name = _unique_name("test_src")
    try:
        await _create_table(pool, table_name)
        await _insert_rows(
            pool,
            table_name,
            [(f"row_{i}", i) for i in range(10)],
        )

        source = PgTableSource(pool, table_name=table_name)
        rows_seen: list[dict[str, Any]] = []
        inserted_mid_iteration = False

        async for row in source.fetch_rows():
            rows_seen.append(row)
            # After reading the first row, insert a new row from a separate
            # connection. Under REPEATABLE READ this should NOT be visible.
            if not inserted_mid_iteration:
                inserted_mid_iteration = True
                await _insert_rows(pool, table_name, [("intruder", 999)])

        assert inserted_mid_iteration
        names = {r["name"] for r in rows_seen}
        assert "intruder" not in names, (
            "Row inserted mid-iteration should not be visible under REPEATABLE READ"
        )
        assert len(rows_seen) == 10
    finally:
        await _drop_table(pool, table_name)


# =============================================================================
# Tests — error cases
# =============================================================================


@pytest.mark.asyncio
async def test_row_factory_and_row_type_exclusive(pool: "asyncpg.Pool") -> None:
    """Cannot specify both row_factory and row_type."""
    with pytest.raises(ValueError, match="Cannot specify both"):
        PgTableSource(
            pool,
            table_name="dummy",
            row_factory=lambda r: r,
            row_type=SimpleRow,  # type: ignore[call-overload]
        )
