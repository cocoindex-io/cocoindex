"""Tests for LanceDB target connector."""

from __future__ import annotations

import datetime as _datetime
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, NamedTuple, cast

import numpy as np
import pytest

import cocoindex as coco
from tests import common
from cocoindex.connectorkits.fingerprint import fingerprint_object

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

    class _FakeIndexConfig(NamedTuple):
        name: str
        columns: list[str]

    class _FakeIndexStats(NamedTuple):
        num_indexed_rows: int
        num_unindexed_rows: int

    def _fake_version(
        *,
        timestamp: _datetime.datetime | None = None,
        metadata: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        return {
            "version": 1,
            "timestamp": timestamp or _datetime.datetime.now(),
            "metadata": metadata or {},
        }

    class _FakeAsyncTable:
        def __init__(
            self,
            *,
            fail_once: bool = False,
            stats_failure: bool = False,
            num_small_fragments: int = 0,
            index_stats: dict[str, _FakeIndexStats] | None = None,
            versions: list[dict[str, Any]] | None = None,
        ) -> None:
            self.optimize_count = 0
            self.stats_count = 0
            self._fail_once = fail_once
            self._stats_failure = stats_failure
            self._num_small_fragments = num_small_fragments
            self._index_stats = index_stats or {}
            self._versions = versions if versions is not None else [_fake_version()]

        async def stats(self) -> dict[str, Any]:
            self.stats_count += 1
            if self._stats_failure:
                raise RuntimeError("stats failed")
            return {
                "total_bytes": 0,
                "num_rows": 0,
                "num_indices": len(self._index_stats),
                "fragment_stats": {
                    "num_fragments": self._num_small_fragments,
                    "num_small_fragments": self._num_small_fragments,
                    "lengths": {},
                },
            }

        async def list_indices(self) -> list[_FakeIndexConfig]:
            return [
                _FakeIndexConfig(name=name, columns=[name])
                for name in self._index_stats
            ]

        async def index_stats(self, index_name: str) -> _FakeIndexStats | None:
            return self._index_stats.get(index_name)

        async def list_versions(self) -> list[dict[str, Any]]:
            return self._versions

        async def optimize(self) -> None:
            self.optimize_count += 1
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
async def test_nullable_schema_only_add_does_not_upsert_rows(
    lancedb_dir: Path,
) -> None:
    conn = await lancedb.connect_async(str(lancedb_dir))
    table_name = "test_nullable_schema_only_add"
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

    env = _make_env(conn, "test_nullable_schema_only_add_does_not_upsert_rows")
    app = coco.App(
        coco.AppConfig(name="test_lancedb_nullable_schema_only_add", environment=env),
        declare_table_and_rows,
    )

    source_rows = [
        SimpleRow(id="1", name="Alice"),
        SimpleRow(id="2", name="Bob"),
    ]
    await app.update()

    initial_version = await _read_table_version(conn, table_name)

    row_type = ExtendedRow
    source_rows = [
        ExtendedRow(id="1", name="Alice"),
        ExtendedRow(id="2", name="Bob"),
    ]
    await app.update()

    assert await _read_column_names(conn, table_name) == ["id", "name", "extra"]
    assert sorted(await _read_rows(conn, table_name), key=lambda row: row["id"]) == [
        {"id": "1", "name": "Alice", "extra": None},
        {"id": "2", "name": "Bob", "extra": None},
    ]

    schema_only_version = await _read_table_version(conn, table_name)
    assert schema_only_version == initial_version + 1

    await app.update()
    assert await _read_table_version(conn, table_name) == schema_only_version


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


@requires_lancedb
def test_row_reconcile_tracks_only_nulls_from_new_nullable_columns() -> None:
    table_schema = lancedb.TableSchema(
        columns={
            "id": lancedb.ColumnDef(type=pa.string(), nullable=False),
            "name": lancedb.ColumnDef(type=pa.string()),
            "extra": lancedb.ColumnDef(type=pa.string()),
        },
        primary_key=["id"],
    )
    handler = _target._RowHandler(
        conn=cast(Any, None),
        table_name="test_table",
        table_schema=table_schema,
        null_backfilled_columns={"extra"},
    )

    old_row = {"id": "1", "name": "Alice"}
    new_row = {"id": "1", "name": "Alice", "extra": None}

    result = handler.reconcile(
        ("1",),
        new_row,
        [fingerprint_object(old_row)],
        False,
    )

    assert result is not None
    assert result.action.track_only is True
    assert result.tracking_record == fingerprint_object(new_row)


@requires_lancedb
def test_row_reconcile_upserts_non_null_value_for_new_nullable_column() -> None:
    table_schema = lancedb.TableSchema(
        columns={
            "id": lancedb.ColumnDef(type=pa.string(), nullable=False),
            "name": lancedb.ColumnDef(type=pa.string()),
            "extra": lancedb.ColumnDef(type=pa.string()),
        },
        primary_key=["id"],
    )
    handler = _target._RowHandler(
        conn=cast(Any, None),
        table_name="test_table",
        table_schema=table_schema,
        null_backfilled_columns={"extra"},
    )

    old_row = {"id": "1", "name": "Alice"}
    new_row = {"id": "1", "name": "Alice", "extra": "vip"}

    result = handler.reconcile(
        ("1",),
        new_row,
        [fingerprint_object(old_row)],
        False,
    )

    assert result is not None
    assert result.action.track_only is False
    assert result.action.value == new_row


@requires_lancedb
def test_row_reconcile_upserts_existing_column_change_with_new_null_column() -> None:
    table_schema = lancedb.TableSchema(
        columns={
            "id": lancedb.ColumnDef(type=pa.string(), nullable=False),
            "name": lancedb.ColumnDef(type=pa.string()),
            "extra": lancedb.ColumnDef(type=pa.string()),
        },
        primary_key=["id"],
    )
    handler = _target._RowHandler(
        conn=cast(Any, None),
        table_name="test_table",
        table_schema=table_schema,
        null_backfilled_columns={"extra"},
    )

    old_row = {"id": "1", "name": "Alice"}
    new_row = {"id": "1", "name": "Alicia", "extra": None}

    result = handler.reconcile(
        ("1",),
        new_row,
        [fingerprint_object(old_row)],
        False,
    )

    assert result is not None
    assert result.action.track_only is False
    assert result.action.value == new_row


@requires_lancedb
def test_row_reconcile_upserts_when_previous_row_may_be_missing() -> None:
    table_schema = lancedb.TableSchema(
        columns={
            "id": lancedb.ColumnDef(type=pa.string(), nullable=False),
            "name": lancedb.ColumnDef(type=pa.string()),
            "extra": lancedb.ColumnDef(type=pa.string()),
        },
        primary_key=["id"],
    )
    handler = _target._RowHandler(
        conn=cast(Any, None),
        table_name="test_table",
        table_schema=table_schema,
        null_backfilled_columns={"extra"},
    )

    old_row = {"id": "1", "name": "Alice"}
    new_row = {"id": "1", "name": "Alice", "extra": None}

    result = handler.reconcile(
        ("1",),
        new_row,
        [fingerprint_object(old_row)],
        True,
    )

    assert result is not None
    assert result.action.track_only is False
    assert result.action.value == new_row


@requires_lancedb
def test_row_reconcile_upserts_non_nullable_added_column() -> None:
    table_schema = lancedb.TableSchema(
        columns={
            "id": lancedb.ColumnDef(type=pa.string(), nullable=False),
            "name": lancedb.ColumnDef(type=pa.string()),
            "score": lancedb.ColumnDef(type=pa.float64(), nullable=False),
        },
        primary_key=["id"],
    )
    handler = _target._RowHandler(
        conn=cast(Any, None),
        table_name="test_table",
        table_schema=table_schema,
    )

    old_row = {"id": "1", "name": "Alice"}
    new_row = {"id": "1", "name": "Alice", "score": 1.0}

    result = handler.reconcile(
        ("1",),
        new_row,
        [fingerprint_object(old_row)],
        False,
    )

    assert result is not None
    assert result.action.track_only is False
    assert result.action.value == new_row


@pytest.mark.asyncio
@requires_lancedb
async def test_row_handler_track_only_actions_do_not_open_table_or_optimize() -> None:
    conn = _FakeAsyncConnection()
    handler = _target._RowHandler(
        conn=cast(Any, conn),
        table_name="test_table",
        table_schema=_make_table_schema(),
    )

    await handler._apply_actions(
        cast(Any, None),
        [_target._RowAction(key=("1",), value=None, track_only=True)],
    )

    assert conn.open_table_count == 0
    assert conn.table.optimize_count == 0


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
async def test_row_handler_optimizes_for_small_fragments() -> None:
    handler = _target._RowHandler(
        conn=cast(Any, None),
        table_name="test_table",
        table_schema=_make_table_schema(),
    )
    table = _FakeAsyncTable(num_small_fragments=_target._MAX_SMALL_FRAGMENTS)

    await handler._maybe_optimize(cast(Any, table), mutation_count=1)

    assert table.optimize_count == 1


@pytest.mark.asyncio
@requires_lancedb
async def test_row_handler_optimizes_for_deletion_files() -> None:
    handler = _target._RowHandler(
        conn=cast(Any, None),
        table_name="test_table",
        table_schema=_make_table_schema(),
    )
    table = _FakeAsyncTable(
        versions=[
            _fake_version(
                metadata={
                    "total_deletion_files": str(_target._MAX_DELETION_FILES),
                }
            )
        ]
    )

    await handler._maybe_optimize(cast(Any, table), mutation_count=1)

    assert table.optimize_count == 1


@pytest.mark.asyncio
@requires_lancedb
async def test_row_handler_optimizes_for_unindexed_tail_on_all_index_types() -> None:
    handler = _target._RowHandler(
        conn=cast(Any, None),
        table_name="test_table",
        table_schema=_make_table_schema(),
    )
    table = _FakeAsyncTable(
        index_stats={
            "vector_idx": _FakeIndexStats(
                num_indexed_rows=100_000,
                num_unindexed_rows=_target._MAX_UNINDEXED_ROWS,
            ),
            "fts_idx": _FakeIndexStats(
                num_indexed_rows=100_000,
                num_unindexed_rows=_target._MAX_UNINDEXED_ROWS,
            ),
        }
    )

    decision = await handler._evaluate_optimize(cast(Any, table))
    assert decision.should is True
    assert any(
        reason.startswith("unindexed[vector_idx]=") for reason in decision.reasons
    )
    assert any(reason.startswith("unindexed[fts_idx]=") for reason in decision.reasons)


@requires_lancedb
def test_count_prunable_old_versions_ignores_young_versions() -> None:
    now = _datetime.datetime.now(_datetime.timezone.utc)
    old_enough = now - (
        _target._DEFAULT_VERSION_PRUNE_AGE
        + _target._VERSION_PRUNE_MARGIN
        + _datetime.timedelta(seconds=1)
    )
    too_young = now - _target._DEFAULT_VERSION_PRUNE_AGE
    versions = [
        _fake_version(timestamp=old_enough),
        _fake_version(timestamp=too_young),
    ]

    assert _target._count_prunable_old_versions(versions) == 1


@requires_lancedb
def test_count_prunable_old_versions_treats_naive_timestamps_as_local() -> None:
    cutoff = _datetime.datetime.now(_datetime.timezone.utc) - (
        _target._DEFAULT_VERSION_PRUNE_AGE + _target._VERSION_PRUNE_MARGIN
    )
    local_tz = _datetime.datetime.now().astimezone().tzinfo
    assert local_tz is not None
    old_enough_local = (cutoff - _datetime.timedelta(seconds=1)).astimezone(local_tz)

    versions = [_fake_version(timestamp=old_enough_local.replace(tzinfo=None))]

    assert _target._count_prunable_old_versions(versions) == 1


@pytest.mark.asyncio
@requires_lancedb
async def test_row_handler_checks_stats_after_large_mutation_batch() -> None:
    handler = _target._RowHandler(
        conn=cast(Any, None),
        table_name="test_table",
        table_schema=_make_table_schema(),
    )
    table = _FakeAsyncTable()

    await handler._maybe_optimize(cast(Any, table), mutation_count=1)
    table._num_small_fragments = _target._MAX_SMALL_FRAGMENTS

    await handler._maybe_optimize(
        cast(Any, table),
        mutation_count=_target._MUTATED_ROWS_BETWEEN_STATS_CHECKS,
    )

    assert table.optimize_count == 1


@pytest.mark.asyncio
@requires_lancedb
async def test_row_handler_skips_stats_until_mutated_row_threshold() -> None:
    handler = _target._RowHandler(
        conn=cast(Any, None),
        table_name="test_table",
        table_schema=_make_table_schema(),
    )
    table = _FakeAsyncTable()

    await handler._maybe_optimize(cast(Any, table), mutation_count=1)
    table._num_small_fragments = _target._MAX_SMALL_FRAGMENTS
    await handler._maybe_optimize(
        cast(Any, table),
        mutation_count=_target._MUTATED_ROWS_BETWEEN_STATS_CHECKS - 1,
    )

    assert table.optimize_count == 0
    assert table.stats_count == 1

    await handler._maybe_optimize(cast(Any, table), mutation_count=1)
    assert table.optimize_count == 1


@pytest.mark.asyncio
@requires_lancedb
async def test_row_handler_throttles_optimize_attempts_after_trigger() -> None:
    handler = _target._RowHandler(
        conn=cast(Any, None),
        table_name="test_table",
        table_schema=_make_table_schema(),
    )
    table = _FakeAsyncTable(num_small_fragments=_target._MAX_SMALL_FRAGMENTS)

    await handler._maybe_optimize(cast(Any, table), mutation_count=1)
    assert table.optimize_count == 1

    await handler._maybe_optimize(
        cast(Any, table),
        mutation_count=_target._MUTATED_ROWS_BETWEEN_STATS_CHECKS,
    )
    assert table.optimize_count == 1

    await handler._maybe_optimize(
        cast(Any, table),
        mutation_count=(
            _target._MIN_MUTATED_ROWS_BETWEEN_OPTIMIZE_ATTEMPTS
            - _target._MUTATED_ROWS_BETWEEN_STATS_CHECKS
        ),
    )
    assert table.optimize_count == 2


@pytest.mark.asyncio
@requires_lancedb
async def test_row_handler_stats_failure_is_non_fatal(
    caplog: pytest.LogCaptureFixture,
) -> None:
    handler = _target._RowHandler(
        conn=cast(Any, None),
        table_name="test_table",
        table_schema=_make_table_schema(),
    )
    table = _FakeAsyncTable(stats_failure=True)

    await handler._maybe_optimize(cast(Any, table), mutation_count=1)

    assert table.optimize_count == 0
    assert "Exception evaluating LanceDB optimize decision" in caplog.text


@pytest.mark.asyncio
@requires_lancedb
async def test_row_handler_optimize_failure_is_non_fatal(
    caplog: pytest.LogCaptureFixture,
) -> None:
    handler = _target._RowHandler(
        conn=cast(Any, None),
        table_name="test_table",
        table_schema=_make_table_schema(),
    )
    table = _FakeAsyncTable(
        fail_once=True,
        num_small_fragments=_target._MAX_SMALL_FRAGMENTS,
    )

    await handler._maybe_optimize(cast(Any, table), mutation_count=1)

    assert table.optimize_count == 1
    assert "Exception in optimizing LanceDB table" in caplog.text


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
        ),
        main_action=None,
        column_actions={},
    )

    await handler._apply_actions(cast(Any, _FakeContextProvider(conn)), [action])

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
        ),
        main_action="insert",
        column_actions={},
    )

    await handler._apply_actions(cast(Any, _FakeContextProvider(conn)), [action])

    assert conn.create_table_count == 1
    assert conn.open_table_count == 0
    assert conn.table.optimize_count == 0


@requires_lancedb
def test_lancedb_async_table_supports_add_columns_api() -> None:
    import lancedb as real_lancedb  # type: ignore[import-not-found, import-untyped]

    async_table = real_lancedb.table.AsyncTable

    assert hasattr(async_table, "add_columns")
    assert callable(async_table.add_columns)
    assert pa.field("x", pa.string())


# =============================================================================
# Vector index tests
# =============================================================================

if HAS_LANCEDB:
    import numpy as np

    async def _create_test_table_with_vectors(
        conn: lancedb.LanceAsyncConnection,
        table_name: str,
        num_rows: int = 256,
        vec_size: int = 4,
    ) -> None:
        """Create a table with id, content, and embedding columns populated."""
        data = {
            "id": [str(i) for i in range(num_rows)],
            "content": [f"doc {i}" for i in range(num_rows)],
            "embedding": [
                np.array([float(i) % 10, 1.0, 1.0, 1.0], dtype=np.float32)
                for i in range(num_rows)
            ],
        }
        arrays = [
            pa.array(data["id"]),
            pa.array(data["content"]),
            pa.array(data["embedding"], type=pa.list_(pa.float32(), vec_size)),
        ]
        schema = pa.schema(
            [
                pa.field("id", pa.string(), nullable=False),
                pa.field("content", pa.string()),
                pa.field("embedding", pa.list_(pa.float32(), vec_size)),
            ]
        )
        batch = pa.RecordBatch.from_arrays(arrays, schema=schema)
        await conn.create_table(table_name, batch, mode="overwrite")

    async def _create_test_table_with_text(
        conn: lancedb.LanceAsyncConnection,
        table_name: str,
    ) -> None:
        """Create a table with id and content columns."""
        data = {
            "id": ["1", "2"],
            "content": [
                "His first language is Spanish",
                "Her first language is English",
            ],
        }
        arrays = [pa.array(data["id"]), pa.array(data["content"])]
        schema = pa.schema(
            [
                pa.field("id", pa.string(), nullable=False),
                pa.field("content", pa.string()),
            ]
        )
        batch = pa.RecordBatch.from_arrays(arrays, schema=schema)
        await conn.create_table(table_name, batch, mode="overwrite")

    async def _list_indices(
        conn: lancedb.LanceAsyncConnection, table_name: str
    ) -> list[Any]:
        """Return the list of index metadata dicts for a table."""
        table = await conn.open_table(table_name)
        return list(await table.list_indices())


@pytest.mark.asyncio
@requires_lancedb
async def test_vector_index_handler_creates_ivf_pq_index(lancedb_dir: Path) -> None:
    """_VectorIndexHandler creates an IVF-PQ vector index on a populated table."""
    conn = await lancedb.connect_async(str(lancedb_dir))
    table_name = "test_vec_idx_handler"
    # IVF-PQ needs data to train on (at least num_partitions rows)
    await _create_test_table_with_vectors(conn, table_name, num_rows=256)

    handler = _target._VectorIndexHandler(conn=conn, table_name=table_name)
    spec = _target._VectorIndexSpec(
        column="embedding",
        metric="cosine",
        index_type="ivf_pq",
        num_partitions=2,
        num_sub_vectors=2,
        num_bits=None,
        m=None,
        ef_construction=None,
    )
    action = _target._VectorIndexAction(name="embedding_idx", spec=spec)

    # Apply the action directly (context_provider not used for index creation)
    await handler._apply_actions(cast(Any, None), [action])

    # Verify index was created
    indices = await _list_indices(conn, table_name)
    index_names = [
        getattr(idx, "name", None)
        or getattr(idx, "index_name", None)
        or getattr(idx, "columns", [""])[0]
        for idx in indices
    ]
    assert any("embedding" in str(n) for n in index_names), (
        f"Expected an embedding vector index, got: {index_names}"
    )


@pytest.mark.asyncio
@requires_lancedb
async def test_vector_index_handler_replace_replaces_existing_index(
    lancedb_dir: Path,
) -> None:
    """_VectorIndexHandler with replace=True recreates an existing index without error."""
    conn = await lancedb.connect_async(str(lancedb_dir))
    table_name = "test_vec_replace"
    await _create_test_table_with_vectors(conn, table_name, num_rows=256)

    handler = _target._VectorIndexHandler(conn=conn, table_name=table_name)
    spec = _target._VectorIndexSpec(
        column="embedding",
        metric="cosine",
        index_type="ivf_pq",
        num_partitions=2,
        num_sub_vectors=2,
        num_bits=None,
        m=None,
        ef_construction=None,
    )
    action = _target._VectorIndexAction(name="emb", spec=spec)

    # Apply twice — second call should not raise (uses replace=True)
    await handler._apply_actions(cast(Any, None), [action])
    await handler._apply_actions(cast(Any, None), [action])  # no-error on re-create

    indices = await _list_indices(conn, table_name)
    assert len(indices) >= 1


@pytest.mark.asyncio
@requires_lancedb
async def test_vector_index_handler_drops_index_on_delete(lancedb_dir: Path) -> None:
    """A delete action (spec=None) drops the previously created vector index."""
    conn = await lancedb.connect_async(str(lancedb_dir))
    table_name = "test_vec_drop"
    await _create_test_table_with_vectors(conn, table_name, num_rows=256)

    handler = _target._VectorIndexHandler(conn=conn, table_name=table_name)
    spec = _target._VectorIndexSpec(
        column="embedding",
        metric="cosine",
        index_type="ivf_pq",
        num_partitions=2,
        num_sub_vectors=2,
        num_bits=None,
        m=None,
        ef_construction=None,
    )
    await handler._apply_actions(
        cast(Any, None), [_target._VectorIndexAction(name="emb_idx", spec=spec)]
    )
    names_before = {idx.name for idx in await _list_indices(conn, table_name)}
    assert "emb_idx" in names_before

    # Delete action (spec=None) drops the index by name.
    delete = _target._VectorIndexAction(name="emb_idx", spec=None)
    await handler._apply_actions(cast(Any, None), [delete])
    names_after = {idx.name for idx in await _list_indices(conn, table_name)}
    assert "emb_idx" not in names_after

    # Dropping again is a safe no-op when the index is already gone.
    await handler._apply_actions(cast(Any, None), [delete])


@requires_lancedb
def test_vector_index_handler_reconcile_no_op_when_fingerprint_unchanged(
    lancedb_dir: Path,
) -> None:
    """Reconcile returns None when the fingerprint is unchanged (no-op)."""
    # handler instance (conn/table_name don't matter for pure reconcile logic)
    handler = _target._VectorIndexHandler(conn=cast(Any, None), table_name="dummy")
    spec = _target._VectorIndexSpec(
        column="embedding",
        metric="cosine",
        index_type="ivf_pq",
        num_partitions=2,
        num_sub_vectors=None,
        num_bits=None,
        m=None,
        ef_construction=None,
    )

    fp = fingerprint_object(spec)
    # Simulate: previous run recorded the same fingerprint, and prev_may_be_missing=False.
    result = handler.reconcile("emb", spec, [fp], False)
    assert result is None, "Expected no-op when fingerprint matches"


@requires_lancedb
def test_vector_index_handler_reconcile_action_when_spec_changes() -> None:
    """Reconcile emits an action when the spec has changed."""
    handler = _target._VectorIndexHandler(conn=cast(Any, None), table_name="dummy")
    old_spec = _target._VectorIndexSpec(
        column="embedding",
        metric="cosine",
        index_type="ivf_pq",
        num_partitions=2,
        num_sub_vectors=None,
        num_bits=None,
        m=None,
        ef_construction=None,
    )
    new_spec = _target._VectorIndexSpec(
        column="embedding",
        metric="l2",  # changed
        index_type="ivf_pq",
        num_partitions=2,
        num_sub_vectors=None,
        num_bits=None,
        m=None,
        ef_construction=None,
    )

    old_fp = fingerprint_object(old_spec)
    result = handler.reconcile("emb", new_spec, [old_fp], False)
    assert result is not None, "Expected action when spec changed"
    assert result.action.spec == new_spec


@requires_lancedb
def test_declare_vector_index_rejects_unknown_column() -> None:
    """declare_vector_index() raises ValueError for a column not in the schema."""
    schema = lancedb.TableSchema(
        columns={
            "id": lancedb.ColumnDef(type=pa.string(), nullable=False),
        },
        primary_key=["id"],
    )
    from unittest.mock import MagicMock

    fake_provider = MagicMock()
    tbl: lancedb.TableTarget[Any, Any] = cast(
        lancedb.TableTarget,
        lancedb.TableTarget(fake_provider, schema),
    )
    with pytest.raises(ValueError, match="Column 'nonexistent' not found"):
        tbl.declare_vector_index(column="nonexistent")


# =============================================================================
# FTS index tests
# =============================================================================


@pytest.mark.asyncio
@requires_lancedb
async def test_fts_index_handler_creates_fts_index(lancedb_dir: Path) -> None:
    """_FtsIndexHandler creates an FTS index on a text column."""
    conn = await lancedb.connect_async(str(lancedb_dir))
    table_name = "test_fts_idx_handler"
    await _create_test_table_with_text(conn, table_name)

    handler = _target._FtsIndexHandler(conn=conn, table_name=table_name)
    spec = _target._FtsIndexSpec(
        column="content",
        language="English",
        with_position=True,
    )
    action = _target._FtsIndexAction(name="content_fts", spec=spec)

    await handler._apply_actions(cast(Any, None), [action])

    # Verify FTS index was created
    indices = await _list_indices(conn, table_name)
    index_names = [
        getattr(idx, "name", None)
        or getattr(idx, "index_name", None)
        or getattr(idx, "columns", [""])[0]
        for idx in indices
    ]
    assert any("content" in str(n) for n in index_names), (
        f"Expected a content FTS index, got: {index_names}"
    )


@pytest.mark.asyncio
@requires_lancedb
async def test_fts_index_handler_search_returns_results(lancedb_dir: Path) -> None:
    """After _FtsIndexHandler creates an FTS index, full-text search works."""
    conn = await lancedb.connect_async(str(lancedb_dir))
    table_name = "test_fts_search"
    await _create_test_table_with_text(conn, table_name)

    handler = _target._FtsIndexHandler(conn=conn, table_name=table_name)
    spec = _target._FtsIndexSpec(
        column="content",
        language="English",
        with_position=True,
    )
    action = _target._FtsIndexAction(name="content_fts", spec=spec)
    await handler._apply_actions(cast(Any, None), [action])

    # FTS search for "spanish"
    fts_tbl = await conn.open_table(table_name)
    result_arrow = (
        await (await fts_tbl.search("spanish", query_type="fts")).limit(5).to_arrow()
    )
    rows = result_arrow.to_pylist()
    assert len(rows) >= 1
    assert rows[0]["id"] == "1"


@pytest.mark.asyncio
@requires_lancedb
async def test_fts_index_handler_replace_is_idempotent(lancedb_dir: Path) -> None:
    """Calling _apply_actions twice with replace=True should not raise."""
    conn = await lancedb.connect_async(str(lancedb_dir))
    table_name = "test_fts_replace"
    await _create_test_table_with_text(conn, table_name)

    handler = _target._FtsIndexHandler(conn=conn, table_name=table_name)
    spec = _target._FtsIndexSpec(
        column="content",
        language="English",
        with_position=True,
    )
    action = _target._FtsIndexAction(name="content_fts", spec=spec)

    await handler._apply_actions(cast(Any, None), [action])
    await handler._apply_actions(cast(Any, None), [action])  # no error on re-create


@pytest.mark.asyncio
@requires_lancedb
async def test_fts_index_handler_drops_index_on_delete(lancedb_dir: Path) -> None:
    """A delete action (spec=None) drops the previously created FTS index."""
    conn = await lancedb.connect_async(str(lancedb_dir))
    table_name = "test_fts_drop"
    await _create_test_table_with_text(conn, table_name)

    handler = _target._FtsIndexHandler(conn=conn, table_name=table_name)
    spec = _target._FtsIndexSpec(
        column="content",
        language="English",
        with_position=True,
    )
    await handler._apply_actions(
        cast(Any, None), [_target._FtsIndexAction(name="content_fts", spec=spec)]
    )
    assert "content_fts" in {idx.name for idx in await _list_indices(conn, table_name)}

    # Delete action (spec=None) drops the index by name.
    delete = _target._FtsIndexAction(name="content_fts", spec=None)
    await handler._apply_actions(cast(Any, None), [delete])
    assert "content_fts" not in {
        idx.name for idx in await _list_indices(conn, table_name)
    }


@requires_lancedb
def test_fts_index_handler_reconcile_no_op_when_fingerprint_unchanged() -> None:
    """Reconcile returns None when the fingerprint is unchanged (no-op)."""
    handler = _target._FtsIndexHandler(conn=cast(Any, None), table_name="dummy")
    spec = _target._FtsIndexSpec(
        column="content",
        language="English",
        with_position=True,
    )

    fp = fingerprint_object(spec)
    result = handler.reconcile("content_fts", spec, [fp], False)
    assert result is None, "Expected no-op when fingerprint matches"


@requires_lancedb
def test_fts_index_handler_reconcile_action_when_spec_changes() -> None:
    """Reconcile emits an action when the spec has changed."""
    handler = _target._FtsIndexHandler(conn=cast(Any, None), table_name="dummy")
    old_spec = _target._FtsIndexSpec(
        column="content",
        language="English",
        with_position=True,
    )
    new_spec = _target._FtsIndexSpec(
        column="content",
        language="Chinese",  # changed
        with_position=False,
    )

    old_fp = fingerprint_object(old_spec)
    result = handler.reconcile("content_fts", new_spec, [old_fp], False)
    assert result is not None, "Expected action when spec changed"
    assert result.action.spec == new_spec


@requires_lancedb
def test_declare_fts_index_rejects_unknown_column() -> None:
    """declare_fts_index() raises ValueError for a column not in the schema."""
    from unittest.mock import MagicMock

    schema = lancedb.TableSchema(
        columns={
            "id": lancedb.ColumnDef(type=pa.string(), nullable=False),
        },
        primary_key=["id"],
    )
    fake_provider = MagicMock()
    tbl: lancedb.TableTarget[Any, Any] = cast(
        lancedb.TableTarget,
        lancedb.TableTarget(fake_provider, schema),
    )
    with pytest.raises(ValueError, match="Column 'no_such_col' not found"):
        tbl.declare_fts_index(column="no_such_col")


# =============================================================================
# SQL string escaping tests (delete path)
# =============================================================================


@requires_lancedb
def test_escape_sql_string_doubles_single_quotes() -> None:
    """_escape_sql_string doubles embedded single quotes for DataFusion SQL."""
    assert _target._escape_sql_string("hello") == "hello"
    assert _target._escape_sql_string("it's") == "it''s"
    assert _target._escape_sql_string("O'Brien") == "O''Brien"
    assert _target._escape_sql_string("a''b") == "a''''b"
    assert _target._escape_sql_string("") == ""
    # Backslashes pass through unchanged (DataFusion does not use backslash escaping)
    assert _target._escape_sql_string("path\\to\\file") == "path\\to\\file"


@pytest.mark.asyncio
@requires_lancedb
async def test_execute_deletes_escapes_string_pk() -> None:
    """_execute_deletes properly escapes single quotes in string primary key values."""
    table_schema = lancedb.TableSchema(
        columns={
            "id": lancedb.ColumnDef(type=pa.string(), nullable=False),
            "name": lancedb.ColumnDef(type=pa.string()),
        },
        primary_key=["id"],
    )

    delete_filters: list[str] = []

    class _DeletableFakeTable(_FakeAsyncTable):
        async def delete(self, filter: str) -> None:  # noqa: A002
            delete_filters.append(filter)

    fake_table = _DeletableFakeTable()
    handler = _target._RowHandler(
        conn=cast(Any, None),
        table_name="test_table",
        table_schema=table_schema,
    )
    await handler._execute_deletes(
        cast(Any, fake_table),
        [
            _target._RowAction(key=("O'Brien",), value=None),
        ],
    )
    assert delete_filters == ["id = 'O''Brien'"]


@pytest.mark.asyncio
@requires_lancedb
async def test_execute_deletes_with_real_lancedb(lancedb_dir: Path) -> None:
    """Integration test: insert and then delete a row whose PK contains a single quote."""
    conn = await lancedb.connect_async(str(lancedb_dir))
    table_name = "test_delete_escape"

    # Insert a row with a quote in the PK
    batch = pa.RecordBatch.from_arrays(
        [pa.array(["O'Brien", "normal"]), pa.array(["Alice", "Bob"])],
        schema=pa.schema(
            [
                pa.field("id", pa.string(), nullable=False),
                pa.field("name", pa.string()),
            ]
        ),
    )
    await conn.create_table(table_name, batch, mode="overwrite")

    # Verify both rows exist
    rows = await _read_rows(conn, table_name)
    assert sorted(rows, key=lambda r: r["id"]) == [
        {"id": "O'Brien", "name": "Alice"},
        {"id": "normal", "name": "Bob"},
    ]

    # Delete the row with the problematic PK via _execute_deletes
    table_schema = lancedb.TableSchema(
        columns={
            "id": lancedb.ColumnDef(type=pa.string(), nullable=False),
            "name": lancedb.ColumnDef(type=pa.string()),
        },
        primary_key=["id"],
    )
    table = await conn.open_table(table_name)
    handler = _target._RowHandler(
        conn=conn,
        table_name=table_name,
        table_schema=table_schema,
    )
    await handler._execute_deletes(
        table,
        [
            _target._RowAction(key=("O'Brien",), value=None),
        ],
    )

    # Verify only the quoted-PK row was deleted
    rows = await _read_rows(conn, table_name)
    assert rows == [{"id": "normal", "name": "Bob"}]
