"""Tests for Doris target connector.

Run with:
    pytest python/tests/connectors/test_doris_target.py -v -s

Environment variables (all required for tests to run):
    DORIS_FE_HOST     - FE host address
    DORIS_PASSWORD    - Password for authentication
    DORIS_HTTP_PORT   - HTTP port for Stream Load (default: 8080)
    DORIS_QUERY_PORT  - MySQL protocol port  (default: 9030)
    DORIS_USERNAME    - Username              (default: admin)
    DORIS_DATABASE    - Test database         (default: cocoindex_test)
"""

from __future__ import annotations

import os
import time
import uuid
from dataclasses import dataclass
from typing import Any

import numpy as np
import pytest
from numpy.typing import NDArray
from typing import Annotated

import cocoindex as coco
from cocoindex.resources.schema import VectorSchema

from tests import common

coco_env = common.create_test_env(__file__)

# ============================================================
# Check dependencies and Doris configuration
# ============================================================

try:
    import pymysql  # type: ignore[import-untyped]
    import aiohttp  # type: ignore[import-untyped]  # noqa: F401
    DEPS_AVAILABLE = True
except ImportError:
    DEPS_AVAILABLE = False

_FE_HOST = os.getenv("DORIS_FE_HOST")
_PASSWORD = os.getenv("DORIS_PASSWORD")
DORIS_CONFIGURED = bool(_FE_HOST and _PASSWORD)

pytestmark = [
    pytest.mark.skipif(not DEPS_AVAILABLE, reason="pymysql/aiohttp not installed"),
    pytest.mark.skipif(
        not DORIS_CONFIGURED,
        reason="Doris not configured (set DORIS_FE_HOST and DORIS_PASSWORD)",
    ),
]

# Lazy import — only when tests actually run
if DEPS_AVAILABLE:
    from cocoindex.connectors import doris


# ============================================================
# Helpers
# ============================================================


def _doris_config() -> "doris.DorisConnectionConfig":
    """Build a DorisConnectionConfig from env vars."""
    return doris.DorisConnectionConfig(
        fe_host=os.environ["DORIS_FE_HOST"],
        database=os.getenv("DORIS_DATABASE", "cocoindex_test"),
        fe_http_port=int(os.getenv("DORIS_HTTP_PORT", "8080")),
        query_port=int(os.getenv("DORIS_QUERY_PORT", "9030")),
        username=os.getenv("DORIS_USERNAME", "admin"),
        password=os.environ["DORIS_PASSWORD"],
        replication_num=1,
        buckets=1,
    )


def _query(config: "doris.DorisConnectionConfig", sql: str) -> list[dict[str, Any]]:
    """Run a query via pymysql and return rows as dicts."""
    conn = pymysql.connect(
        host=config.fe_host,
        port=config.query_port,
        user=config.username,
        password=config.password,
        database=config.database,
        autocommit=True,
        connect_timeout=10,
    )
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(sql)
            return list(cur.fetchall())
    finally:
        conn.close()


def _exec(config: "doris.DorisConnectionConfig", sql: str) -> None:
    """Execute a DDL/DML statement."""
    conn = pymysql.connect(
        host=config.fe_host,
        port=config.query_port,
        user=config.username,
        password=config.password,
        database=config.database,
        autocommit=True,
        connect_timeout=10,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
    finally:
        conn.close()


def _table_exists(config: "doris.DorisConnectionConfig", table_name: str) -> bool:
    rows = _query(config, f"SHOW TABLES LIKE '{table_name}'")
    return len(rows) > 0


def _unique_table() -> str:
    return f"test_{int(time.time())}_{uuid.uuid4().hex[:6]}"


# ============================================================
# Row types
# ============================================================


@dataclass
class SimpleRow:
    id: str
    name: str
    value: int


@dataclass
class VectorRow:
    id: str
    content: str
    embedding: Annotated[
        NDArray[np.float32], VectorSchema(dtype=np.dtype(np.float32), size=4)
    ]


# ============================================================
# Fixtures
# ============================================================


@pytest.fixture
def config() -> "doris.DorisConnectionConfig":
    return _doris_config()


@pytest.fixture
def managed_conn(config: "doris.DorisConnectionConfig") -> "doris.ManagedConnection":
    return doris.connect(config)


@pytest.fixture
def table_name() -> str:
    return _unique_table()


@pytest.fixture(autouse=True)
def cleanup_table(config: "doris.DorisConnectionConfig", table_name: str):
    """Ensure the test table is cleaned up after each test."""
    yield
    try:
        _exec(config, f"DROP TABLE IF EXISTS `{config.database}`.`{table_name}`")
    except Exception as e:
        print(f"Cleanup warning: {e}")


@pytest.fixture(autouse=True, scope="session")
def ensure_database():
    """Make sure the test database exists."""
    if not DORIS_CONFIGURED:
        return
    c = _doris_config()
    conn = pymysql.connect(
        host=c.fe_host, port=c.query_port,
        user=c.username, password=c.password,
        autocommit=True, connect_timeout=10,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE IF NOT EXISTS `{c.database}`")
    finally:
        conn.close()


# ============================================================
# Tests: table lifecycle
# ============================================================

# Global mutable state used by the App callback (same pattern as SQLite tests)
_source_rows: list[Any] = []
_row_type: type = SimpleRow
_table_name: str = ""
_doris_db: "doris.DorisDatabase | None" = None


async def _declare_table_and_rows() -> None:
    assert _doris_db is not None
    table = coco.use_mount(
        coco.component_subpath("setup", "table"),
        _doris_db.declare_table_target,
        _table_name,
        await doris.TableSchema.from_class(_row_type, primary_key=["id"]),
    )
    for row in _source_rows:
        table.declare_row(row=row)


def test_create_table_and_insert_rows(
    managed_conn: "doris.ManagedConnection",
    config: "doris.DorisConnectionConfig",
    table_name: str,
) -> None:
    """Test creating a table and inserting rows via coco.App."""
    global _source_rows, _row_type, _table_name, _doris_db

    _source_rows = []
    _row_type = SimpleRow
    _table_name = table_name

    with doris.register_db("test_create_db", managed_conn) as db:
        _doris_db = db

        app = coco.App(
            coco.AppConfig(name="test_doris_create", environment=coco_env),
            _declare_table_and_rows,
        )

        _source_rows = [
            SimpleRow(id="1", name="Alice", value=100),
            SimpleRow(id="2", name="Bob", value=200),
        ]
        app.update()

        # Doris needs a moment to make data visible
        time.sleep(2)

        assert _table_exists(config, table_name)
        data = _query(config, f"SELECT * FROM `{config.database}`.`{table_name}` ORDER BY id")
        assert len(data) == 2
        assert data[0]["name"] == "Alice"
        assert data[1]["name"] == "Bob"

        # Insert one more row
        _source_rows.append(SimpleRow(id="3", name="Charlie", value=300))
        app.update()
        time.sleep(2)

        data = _query(config, f"SELECT * FROM `{config.database}`.`{table_name}` ORDER BY id")
        assert len(data) == 3
        assert data[2]["name"] == "Charlie"


def test_update_row(
    managed_conn: "doris.ManagedConnection",
    config: "doris.DorisConnectionConfig",
    table_name: str,
) -> None:
    """Test updating an existing row."""
    global _source_rows, _row_type, _table_name, _doris_db

    _source_rows = []
    _row_type = SimpleRow
    _table_name = table_name

    with doris.register_db("test_update_db", managed_conn) as db:
        _doris_db = db

        app = coco.App(
            coco.AppConfig(name="test_doris_update", environment=coco_env),
            _declare_table_and_rows,
        )

        _source_rows = [
            SimpleRow(id="1", name="Alice", value=100),
            SimpleRow(id="2", name="Bob", value=200),
        ]
        app.update()
        time.sleep(2)

        # Update a row
        _source_rows = [
            SimpleRow(id="1", name="Alice Updated", value=150),
            SimpleRow(id="2", name="Bob", value=200),
        ]
        app.update()
        time.sleep(2)

        data = _query(config, f"SELECT * FROM `{config.database}`.`{table_name}` ORDER BY id")
        assert len(data) == 2
        alice = next(r for r in data if r["id"] == "1")
        assert alice["name"] == "Alice Updated"
        assert alice["value"] == 150


def test_delete_row(
    managed_conn: "doris.ManagedConnection",
    config: "doris.DorisConnectionConfig",
    table_name: str,
) -> None:
    """Test deleting a row by removing it from the declared set."""
    global _source_rows, _row_type, _table_name, _doris_db

    _source_rows = []
    _row_type = SimpleRow
    _table_name = table_name

    with doris.register_db("test_delete_db", managed_conn) as db:
        _doris_db = db

        app = coco.App(
            coco.AppConfig(name="test_doris_delete", environment=coco_env),
            _declare_table_and_rows,
        )

        _source_rows = [
            SimpleRow(id="1", name="Alice", value=100),
            SimpleRow(id="2", name="Bob", value=200),
            SimpleRow(id="3", name="Charlie", value=300),
        ]
        app.update()
        time.sleep(2)

        data = _query(config, f"SELECT * FROM `{config.database}`.`{table_name}` ORDER BY id")
        assert len(data) == 3

        # Remove Bob
        _source_rows = [
            SimpleRow(id="1", name="Alice", value=100),
            SimpleRow(id="3", name="Charlie", value=300),
        ]
        app.update()
        time.sleep(2)

        data = _query(config, f"SELECT * FROM `{config.database}`.`{table_name}` ORDER BY id")
        assert len(data) == 2
        ids = [r["id"] for r in data]
        assert "2" not in ids


def test_dict_rows(
    managed_conn: "doris.ManagedConnection",
    config: "doris.DorisConnectionConfig",
    table_name: str,
) -> None:
    """Test using dict rows instead of dataclass rows."""
    dict_rows: list[dict[str, Any]] = []

    with doris.register_db("test_dict_db", managed_conn) as db:

        def declare_dict_table() -> None:
            table = coco.use_mount(
                coco.component_subpath("setup", "table"),
                db.declare_table_target,
                table_name,
                doris.TableSchema(
                    {
                        "id": doris.ColumnDef(type="VARCHAR(512)", nullable=False),
                        "name": doris.ColumnDef(type="TEXT"),
                        "count": doris.ColumnDef(type="BIGINT"),
                    },
                    primary_key=["id"],
                ),
            )
            for row in dict_rows:
                table.declare_row(row=row)

        app = coco.App(
            coco.AppConfig(name="test_doris_dict", environment=coco_env),
            declare_dict_table,
        )

        dict_rows.extend([
            {"id": "1", "name": "Item1", "count": 10},
            {"id": "2", "name": "Item2", "count": 20},
        ])
        app.update()
        time.sleep(2)

        data = _query(config, f"SELECT * FROM `{config.database}`.`{table_name}` ORDER BY id")
        assert len(data) == 2
        assert data[0]["name"] == "Item1"


# ============================================================
# Tests: vector index
# ============================================================


def test_vector_index_creation(
    managed_conn: "doris.ManagedConnection",
    config: "doris.DorisConnectionConfig",
    table_name: str,
) -> None:
    """Test creating a table with vector column and HNSW ANN index."""
    vec_rows: list[VectorRow] = []

    with doris.register_db("test_vec_db", managed_conn) as db:

        async def declare_vec_table() -> None:
            table = coco.use_mount(
                coco.component_subpath("setup", "table"),
                db.declare_table_target,
                table_name,
                await doris.TableSchema.from_class(VectorRow, primary_key=["id"]),
                vector_indexes=[
                    doris.VectorIndexDef(
                        field_name="embedding",
                        index_type="hnsw",
                        metric_type="l2_distance",
                    )
                ],
            )
            for row in vec_rows:
                table.declare_row(row=row)

        app = coco.App(
            coco.AppConfig(name="test_doris_vector", environment=coco_env),
            declare_vec_table,
        )

        vec_rows = [
            VectorRow(
                id="1", content="hello world",
                embedding=np.array([1.0, 2.0, 3.0, 4.0], dtype=np.float32),
            ),
            VectorRow(
                id="2", content="foo bar",
                embedding=np.array([5.0, 6.0, 7.0, 8.0], dtype=np.float32),
            ),
        ]
        app.update()
        time.sleep(2)

        assert _table_exists(config, table_name)

        # Verify table schema contains ANN index
        result = _query(
            config,
            f"SHOW CREATE TABLE `{config.database}`.`{table_name}`",
        )
        create_stmt = result[0].get("Create Table", "")
        assert "USING ANN" in create_stmt or "using ann" in create_stmt.lower(), (
            f"Expected ANN index, got: {create_stmt}"
        )

        # Verify data was inserted
        data = _query(config, f"SELECT id, content FROM `{config.database}`.`{table_name}` ORDER BY id")
        assert len(data) == 2
        assert data[0]["content"] == "hello world"
        assert data[1]["content"] == "foo bar"


def test_no_change_optimization(
    managed_conn: "doris.ManagedConnection",
    config: "doris.DorisConnectionConfig",
    table_name: str,
) -> None:
    """Test that unchanged data doesn't cause unnecessary updates."""
    global _source_rows, _row_type, _table_name, _doris_db

    _source_rows = []
    _row_type = SimpleRow
    _table_name = table_name

    with doris.register_db("test_noop_db", managed_conn) as db:
        _doris_db = db

        app = coco.App(
            coco.AppConfig(name="test_doris_noop", environment=coco_env),
            _declare_table_and_rows,
        )

        _source_rows = [
            SimpleRow(id="1", name="Alice", value=100),
            SimpleRow(id="2", name="Bob", value=200),
        ]
        app.update()
        time.sleep(2)

        data1 = _query(config, f"SELECT * FROM `{config.database}`.`{table_name}` ORDER BY id")
        assert len(data1) == 2

        # Run update again with the same data — should be a no-op
        app.update()
        time.sleep(1)

        data2 = _query(config, f"SELECT * FROM `{config.database}`.`{table_name}` ORDER BY id")
        assert data1 == data2
