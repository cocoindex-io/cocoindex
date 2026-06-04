"""Tests for LanceDB target connector."""

from __future__ import annotations

import asyncio
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, cast

import pytest

import cocoindex as coco
from tests import common

try:
    import pyarrow as pa  # type: ignore
    from cocoindex.connectorkits import target
    from cocoindex.connectors import lancedb
    from cocoindex.connectors.lancedb import _target

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

    class _FakeAsyncTable:
        def __init__(
            self,
            *,
            block: asyncio.Event | None = None,
            fail_once: bool = False,
        ) -> None:
            self.optimize_count = 0
            self._block = block
            self._fail_once = fail_once

        async def optimize(self) -> None:
            self.optimize_count += 1
            if self._block is not None:
                await self._block.wait()
            if self._fail_once:
                self._fail_once = False
                raise RuntimeError("optimize failed")

    class _FakeAsyncConnection:
        def __init__(self, *, table_exists: bool = True) -> None:
            self.table = _FakeAsyncTable()
            self.table_exists = table_exists
            self.open_table_count = 0
            self.create_table_count = 0
            self.drop_table_count = 0

        async def table_names(self) -> list[str]:
            return ["test_table"] if self.table_exists else []

        async def open_table(self, table_name: str) -> _FakeAsyncTable:
            assert table_name == "test_table"
            self.open_table_count += 1
            return self.table

        async def create_table(
            self, table_name: str, data: Any, *, mode: str
        ) -> _FakeAsyncTable:
            assert table_name == "test_table"
            assert mode == "overwrite"
            self.create_table_count += 1
            self.table_exists = True
            return self.table

        async def drop_table(self, table_name: str) -> None:
            assert table_name == "test_table"
            self.drop_table_count += 1
            self.table_exists = False

    class _FakeContextProvider:
        def __init__(self, conn: _FakeAsyncConnection) -> None:
            self._conn = conn

        def get(self, key: str, t: type[Any] | None = None) -> _FakeAsyncConnection:
            assert key == "test_db"
            return self._conn

    async def _read_rows(
        conn: lancedb.LanceAsyncConnection, table_name: str
    ) -> list[dict[str, Any]]:
        table = await conn.open_table(table_name)
        arrow_table = await table.to_arrow()
        return cast(list[dict[str, Any]], arrow_table.to_pylist())

    async def _read_column_names(
        conn: lancedb.LanceAsyncConnection, table_name: str
    ) -> list[str]:
        table = await conn.open_table(table_name)
        return list((await table.schema()).names)

    async def _read_table_version(
        conn: lancedb.LanceAsyncConnection, table_name: str
    ) -> int:
        table = await conn.open_table(table_name)
        return cast(int, await table.version())

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

    def _make_table_schema() -> lancedb.TableSchema[dict[str, Any]]:
        return lancedb.TableSchema(
            columns={
                "id": lancedb.ColumnDef(type=pa.string(), nullable=False),
                "name": lancedb.ColumnDef(type=pa.string()),
            },
            primary_key=["id"],
        )

    async def _wait_for_optimize_task(handler: _target._RowHandler) -> None:
        task = handler._optimize_task
        if task is not None:
            await task


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


@pytest.mark.asyncio
@requires_lancedb
async def test_row_handler_optimizes_after_configured_mutation_count() -> None:
    table_schema = lancedb.TableSchema(
        columns={
            "id": lancedb.ColumnDef(type=pa.string(), nullable=False),
            "name": lancedb.ColumnDef(type=pa.string()),
        },
        primary_key=["id"],
    )
    handler = _target._RowHandler(
        conn=cast(Any, None),
        table_name="test_table",
        table_schema=table_schema,
        num_transactions_before_optimize=2,
    )
    table = _FakeAsyncTable()

    await handler._maybe_optimize(cast(Any, table))
    assert table.optimize_count == 0

    await handler._maybe_optimize(cast(Any, table))
    await _wait_for_optimize_task(handler)
    assert table.optimize_count == 1

    await handler._maybe_optimize(cast(Any, table))
    await _wait_for_optimize_task(handler)
    assert table.optimize_count == 1


@pytest.mark.asyncio
@requires_lancedb
async def test_row_handler_does_not_overlap_optimize_tasks() -> None:
    handler = _target._RowHandler(
        conn=cast(Any, None),
        table_name="test_table",
        table_schema=_make_table_schema(),
        num_transactions_before_optimize=1,
    )
    unblock = asyncio.Event()
    table = _FakeAsyncTable(block=unblock)

    await handler._maybe_optimize(cast(Any, table))
    await asyncio.sleep(0)
    assert table.optimize_count == 1

    await handler._maybe_optimize(cast(Any, table))
    await asyncio.sleep(0)
    assert table.optimize_count == 1

    unblock.set()
    await _wait_for_optimize_task(handler)


@pytest.mark.asyncio
@requires_lancedb
async def test_row_handler_preserves_mutations_during_optimize() -> None:
    handler = _target._RowHandler(
        conn=cast(Any, None),
        table_name="test_table",
        table_schema=_make_table_schema(),
        num_transactions_before_optimize=2,
    )
    unblock = asyncio.Event()
    table = _FakeAsyncTable(block=unblock)

    await handler._maybe_optimize(cast(Any, table))
    assert table.optimize_count == 0

    await handler._maybe_optimize(cast(Any, table))
    await asyncio.sleep(0)
    assert table.optimize_count == 1

    await handler._maybe_optimize(cast(Any, table))
    await asyncio.sleep(0)
    assert table.optimize_count == 1

    unblock.set()
    await _wait_for_optimize_task(handler)

    await handler._maybe_optimize(cast(Any, table))
    await _wait_for_optimize_task(handler)
    assert table.optimize_count == 2


@pytest.mark.asyncio
@requires_lancedb
async def test_row_handler_retries_after_optimize_failure() -> None:
    handler = _target._RowHandler(
        conn=cast(Any, None),
        table_name="test_table",
        table_schema=_make_table_schema(),
        num_transactions_before_optimize=1,
    )
    table = _FakeAsyncTable(fail_once=True)

    await handler._maybe_optimize(cast(Any, table))
    await _wait_for_optimize_task(handler)
    assert table.optimize_count == 1

    await handler._maybe_optimize(cast(Any, table))
    await _wait_for_optimize_task(handler)
    assert table.optimize_count == 2


@pytest.mark.asyncio
@requires_lancedb
async def test_table_handler_skips_optimize_for_existing_table() -> None:
    conn = _FakeAsyncConnection()
    handler = _target._TableHandler()
    action = _target._TableAction(
        key=_target._TableKey(db_key="test_db", table_name="test_table"),
        spec=_target._TableSpec(
            table_schema=_make_table_schema(),
            managed_by=target.ManagedBy.USER,
            num_transactions_before_optimize=50,
        ),
        main_action=None,
        column_actions={},
    )

    await handler._apply_actions(cast(Any, _FakeContextProvider(conn)), [action])
    await asyncio.sleep(0)

    assert conn.open_table_count == 0
    assert conn.table.optimize_count == 0


@pytest.mark.asyncio
@requires_lancedb
async def test_table_handler_does_not_optimize_new_table_before_row_mutations() -> None:
    conn = _FakeAsyncConnection(table_exists=False)
    handler = _target._TableHandler()
    action = _target._TableAction(
        key=_target._TableKey(db_key="test_db", table_name="test_table"),
        spec=_target._TableSpec(
            table_schema=_make_table_schema(),
            managed_by=target.ManagedBy.SYSTEM,
            num_transactions_before_optimize=50,
        ),
        main_action="insert",
        column_actions={},
    )

    await handler._apply_actions(cast(Any, _FakeContextProvider(conn)), [action])
    await asyncio.sleep(0)

    assert conn.create_table_count == 1
    assert conn.open_table_count == 0
    assert conn.table.optimize_count == 0


@requires_lancedb
def test_table_target_rejects_non_positive_optimize_interval() -> None:
    with pytest.raises(
        ValueError, match="num_transactions_before_optimize must be positive"
    ):
        lancedb.table_target(
            db=cast(Any, None),
            table_name="test_table",
            table_schema=_make_table_schema(),
            num_transactions_before_optimize=0,
        )


@requires_lancedb
def test_lancedb_async_table_supports_add_columns_api() -> None:
    async_table = cast(Any, lancedb).table.AsyncTable

    assert hasattr(async_table, "add_columns")
    assert callable(async_table.add_columns)
    assert pa.field("x", pa.string())
