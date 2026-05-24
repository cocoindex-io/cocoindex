"""Tests for LanceDB target connector."""

from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest

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

    async def _wait_for_optimize_task(handler: _target._RowHandler) -> None:
        task = handler._optimize_task
        if task is not None:
            await task


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
        sub_actions={},
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
        sub_actions={},
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


if HAS_LANCEDB:

    def _make_table_schema() -> lancedb.TableSchema[dict[str, Any]]:
        return lancedb.TableSchema(
            columns={
                "id": lancedb.ColumnDef(type=pa.string(), nullable=False),
                "name": lancedb.ColumnDef(type=pa.string()),
            },
            primary_key=["id"],
        )
