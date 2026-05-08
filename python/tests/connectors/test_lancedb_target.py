"""Tests for LanceDB target connector."""

from __future__ import annotations

import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

import pytest

import cocoindex as coco
from tests import common

try:
    import pyarrow as pa  # type: ignore
    from cocoindex.connectors import lancedb

    HAS_LANCEDB = True
except ImportError:
    HAS_LANCEDB = False

requires_lancedb = pytest.mark.skipif(
    not HAS_LANCEDB, reason="lancedb dependencies not installed"
)

if HAS_LANCEDB:
    LANCEDB_DB = coco.ContextKey[lancedb.LanceAsyncConnection]("lancedb_test_db")


@dataclass
class SimpleRow:
    id: str
    name: str


@dataclass
class ExtendedRow:
    id: str
    name: str
    extra: str | None = None


@dataclass
class MultiExtendedRow:
    id: str
    name: str
    extra: str | None = None
    score: float | None = None


@pytest.fixture
def lancedb_dir() -> Iterator[Path]:
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


if HAS_LANCEDB:

    async def _read_rows(
        conn: lancedb.LanceAsyncConnection, table_name: str
    ) -> list[dict[str, Any]]:
        table = await conn.open_table(table_name)
        arrow_table = await table.to_arrow()
        return arrow_table.to_pylist()

    async def _read_column_names(
        conn: lancedb.LanceAsyncConnection, table_name: str
    ) -> list[str]:
        table = await conn.open_table(table_name)
        return list((await table.schema()).names)

    async def _read_table_version(
        conn: lancedb.LanceAsyncConnection, table_name: str
    ) -> int:
        table = await conn.open_table(table_name)
        return await table.version()

    def _make_env(
        conn: lancedb.LanceAsyncConnection, env_name: str
    ) -> coco.Environment:
        ctx = coco.ContextProvider()
        ctx.provide(LANCEDB_DB, conn)
        settings = coco.Settings.from_env(
            db_path=common.get_env_db_path(
                f"connectors__test_lancedb_target__{env_name}"
            )
        )
        return coco.Environment(settings, context_provider=ctx)


@pytest.mark.asyncio
@requires_lancedb
async def test_add_column_preserves_existing_rows(lancedb_dir: Path) -> None:
    conn = await lancedb.connect_async(str(lancedb_dir))
    table_name = "test_add_column"
    source_rows: list[Any] = []
    row_type: type[Any] = SimpleRow

    async def declare_table_and_rows() -> None:
        table = await coco.use_mount(
            coco.component_subpath("setup", "table"),
            lancedb.declare_table_target,
            LANCEDB_DB,
            table_name,
            await lancedb.TableSchema.from_class(row_type, primary_key=["id"]),
        )
        for row in source_rows:
            table.declare_row(row=row)

    env = _make_env(conn, "test_add_column_preserves_existing_rows")
    app = coco.App(
        coco.AppConfig(name="test_lancedb_add_column", environment=env),
        declare_table_and_rows,
    )

    source_rows = [
        SimpleRow(id="1", name="Alice"),
        SimpleRow(id="2", name="Bob"),
    ]
    await app.update()

    assert await _read_column_names(conn, table_name) == ["id", "name"]
    assert sorted(await _read_rows(conn, table_name), key=lambda row: row["id"]) == [
        {"id": "1", "name": "Alice"},
        {"id": "2", "name": "Bob"},
    ]
    initial_version = await _read_table_version(conn, table_name)

    row_type = ExtendedRow
    source_rows = [
        ExtendedRow(id="1", name="Alice", extra="vip"),
        ExtendedRow(id="2", name="Bob", extra="std"),
    ]
    await app.update()

    assert await _read_column_names(conn, table_name) == ["id", "name", "extra"]
    assert sorted(await _read_rows(conn, table_name), key=lambda row: row["id"]) == [
        {"id": "1", "name": "Alice", "extra": "vip"},
        {"id": "2", "name": "Bob", "extra": "std"},
    ]
    final_version = await _read_table_version(conn, table_name)
    assert final_version == initial_version + 2
    assert final_version != 1


@pytest.mark.asyncio
@requires_lancedb
async def test_add_column_keeps_old_rows_before_backfill(lancedb_dir: Path) -> None:
    conn = await lancedb.connect_async(str(lancedb_dir))
    table_name = "test_add_column_existing_rows"
    source_rows: list[Any] = []
    row_type: type[Any] = SimpleRow

    async def declare_table_and_rows() -> None:
        table = await coco.use_mount(
            coco.component_subpath("setup", "table"),
            lancedb.declare_table_target,
            LANCEDB_DB,
            table_name,
            await lancedb.TableSchema.from_class(row_type, primary_key=["id"]),
        )
        for row in source_rows:
            table.declare_row(row=row)

    env = _make_env(conn, "test_add_column_keeps_old_rows_before_backfill")
    app = coco.App(
        coco.AppConfig(name="test_lancedb_add_column_existing_rows", environment=env),
        declare_table_and_rows,
    )

    source_rows = [SimpleRow(id="1", name="Alice")]
    await app.update()

    row_type = ExtendedRow
    source_rows = [
        ExtendedRow(id="1", name="Alice", extra="vip"),
        ExtendedRow(id="2", name="Bob", extra="std"),
    ]
    await app.update()

    rows = await _read_rows(conn, table_name)
    assert sorted(rows, key=lambda row: row["id"]) == [
        {"id": "1", "name": "Alice", "extra": "vip"},
        {"id": "2", "name": "Bob", "extra": "std"},
    ]
    assert "extra" in await _read_column_names(conn, table_name)


@pytest.mark.asyncio
@requires_lancedb
async def test_add_non_nullable_column_is_materialized_as_nullable(
    lancedb_dir: Path,
) -> None:
    @dataclass
    class NonNullableExtendedRow:
        id: str
        name: str
        score: float

    conn = await lancedb.connect_async(str(lancedb_dir))
    table_name = "test_add_non_nullable_column"
    source_rows: list[Any] = []
    row_type: type[Any] = SimpleRow

    async def declare_table_and_rows() -> None:
        table = await coco.use_mount(
            coco.component_subpath("setup", "table"),
            lancedb.declare_table_target,
            LANCEDB_DB,
            table_name,
            await lancedb.TableSchema.from_class(row_type, primary_key=["id"]),
        )
        for row in source_rows:
            table.declare_row(row=row)

    env = _make_env(conn, "test_add_non_nullable_column_is_materialized_as_nullable")
    app = coco.App(
        coco.AppConfig(
            name="test_lancedb_add_non_nullable_column",
            environment=env,
        ),
        declare_table_and_rows,
    )

    source_rows = [SimpleRow(id="1", name="Alice")]
    await app.update()

    row_type = NonNullableExtendedRow
    source_rows = [
        NonNullableExtendedRow(id="1", name="Alice", score=1.5),
        NonNullableExtendedRow(id="2", name="Bob", score=2.0),
    ]
    await app.update()

    schema = await (await conn.open_table(table_name)).schema()
    score_field = schema.field("score")
    assert score_field.nullable is True
    assert sorted(await _read_rows(conn, table_name), key=lambda row: row["id"]) == [
        {"id": "1", "name": "Alice", "score": 1.5},
        {"id": "2", "name": "Bob", "score": 2.0},
    ]


@pytest.mark.asyncio
@requires_lancedb
async def test_add_multiple_columns_in_place(lancedb_dir: Path) -> None:
    conn = await lancedb.connect_async(str(lancedb_dir))
    table_name = "test_add_multiple_columns"
    source_rows: list[Any] = []
    row_type: type[Any] = SimpleRow

    async def declare_table_and_rows() -> None:
        table = await coco.use_mount(
            coco.component_subpath("setup", "table"),
            lancedb.declare_table_target,
            LANCEDB_DB,
            table_name,
            await lancedb.TableSchema.from_class(row_type, primary_key=["id"]),
        )
        for row in source_rows:
            table.declare_row(row=row)

    env = _make_env(conn, "test_add_multiple_columns_in_place")
    app = coco.App(
        coco.AppConfig(name="test_lancedb_add_multiple_columns", environment=env),
        declare_table_and_rows,
    )

    source_rows = [SimpleRow(id="1", name="Alice")]
    await app.update()
    initial_version = await _read_table_version(conn, table_name)

    row_type = MultiExtendedRow
    source_rows = [
        MultiExtendedRow(id="1", name="Alice", extra="vip", score=1.5),
        MultiExtendedRow(id="2", name="Bob", extra="std", score=2.0),
    ]
    await app.update()

    assert await _read_column_names(conn, table_name) == [
        "id",
        "name",
        "extra",
        "score",
    ]
    assert sorted(await _read_rows(conn, table_name), key=lambda row: row["id"]) == [
        {"id": "1", "name": "Alice", "extra": "vip", "score": 1.5},
        {"id": "2", "name": "Bob", "extra": "std", "score": 2.0},
    ]
    assert await _read_table_version(conn, table_name) == initial_version + 2


@requires_lancedb
def test_lancedb_async_table_supports_add_columns_api() -> None:
    from lancedb.table import AsyncTable

    assert hasattr(AsyncTable, "add_columns")
    assert callable(AsyncTable.add_columns)
    assert pa.field("x", pa.string())
