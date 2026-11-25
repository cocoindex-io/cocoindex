"""Sliced execution with a real container target: SQLite tables and full_reprocess."""

from __future__ import annotations

import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

import pytest

import cocoindex as coco
from cocoindex._internal.context_keys import ContextProvider
from cocoindex.connectors import sqlite

from tests import common

SQLITE_DB = coco.ContextKey[sqlite.ManagedConnection]("selector_probe_sqlite_db")
_source: dict[str, int] = {}


@dataclass
class Row:
    id: str
    value: int


@pytest.fixture
def conn() -> Iterator[sqlite.ManagedConnection]:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = Path(f.name)
    managed = sqlite.connect(db_path, load_vec=False)
    yield managed
    managed.close()
    db_path.unlink(missing_ok=True)


def _env(conn: sqlite.ManagedConnection, name: str) -> coco.Environment:
    ctx = ContextProvider()
    ctx.provide(SQLITE_DB, conn)
    settings = coco.Settings.from_env(
        db_path=common.get_env_db_path(f"selector_probe_sqlite__{name}")
    )
    return coco.Environment(settings, context_provider=ctx)


@coco.fn(memo=True)
async def _row(key: str, value: int, table: Any) -> None:
    table.declare_row(row=Row(id=key, value=value))


@coco.fn
async def _main() -> None:
    table = await sqlite.mount_table_target(
        SQLITE_DB, "t", await sqlite.TableSchema.from_class(Row, primary_key=["id"])
    )
    for key, value in _source.items():
        await coco.mount(coco.component_subpath("items", key), _row, key, value, table)


def _rows(conn: sqlite.ManagedConnection) -> dict[str, int]:
    with conn.readonly() as c:
        return dict(c.execute('SELECT id, value FROM "t"').fetchall())


def test_full_reprocess_with_a_selector_under_a_sqlite_table(
    conn: sqlite.ManagedConnection,
) -> None:
    _source.clear()
    _source.update(a=1, b=1)
    app = coco.App(coco.AppConfig(name="probe_fr", environment=_env(conn, "fr")), _main)
    app.update_blocking()
    _source.update(a=2, b=2)
    app.update_blocking(
        component_selector=[coco.ROOT_PATH / "items" / "a"], full_reprocess=True
    )
    assert _rows(conn) == {"a": 2, "b": 1}


@coco.fn
async def _owns_table(name: str) -> None:
    table = await coco.use_mount(
        coco.component_subpath("setup"),
        sqlite.declare_table_target,
        SQLITE_DB,
        name,
        await sqlite.TableSchema.from_class(Row, primary_key=["id"]),
    )
    for key, value in _source.items():
        await coco.mount(coco.component_subpath("rows", key), _row, key, value, table)


@coco.fn
async def _owner_main() -> None:
    await coco.mount(coco.component_subpath("p1"), _owns_table, "t")


def test_full_reprocess_of_a_selected_table_owner(
    conn: sqlite.ManagedConnection,
) -> None:
    """full_reprocess makes the table's reconcile see prev_may_be_missing. The
    selected subtree owns the table: is the unchanged table refused as a rebuild?"""
    _source.clear()
    _source.update(a=1, b=1)
    app = coco.App(
        coco.AppConfig(name="probe_owner_fr", environment=_env(conn, "owner_fr")),
        _owner_main,
    )
    app.update_blocking()
    _source.update(a=2, b=2)
    app.update_blocking(component_selector=[coco.ROOT_PATH / "p1"], full_reprocess=True)
    assert _rows(conn) == {"a": 2, "b": 2}
