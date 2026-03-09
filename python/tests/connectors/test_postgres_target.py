"""Tests for PostgreSQL target connector attachment features (vector index, SQL command).

Run with:
    POSTGRES_DSN="postgresql://localhost/cocoindex_test" pytest python/tests/connectors/test_postgres_target.py -v -s

Environment variables:
    POSTGRES_DSN - PostgreSQL connection string (required for tests to run)
"""

from __future__ import annotations

import asyncio
import os
import uuid
from dataclasses import dataclass
from typing import Annotated, Any

import numpy as np
import pytest
from numpy.typing import NDArray

import cocoindex as coco
from cocoindex.resources.schema import VectorSchema

from tests import common

coco_env = common.create_test_env(__file__)

# =============================================================================
# Check dependencies and Postgres configuration
# =============================================================================

try:
    import asyncpg  # type: ignore[import-untyped]

    DEPS_AVAILABLE = True
except ImportError:
    DEPS_AVAILABLE = False

PG_DSN = os.getenv("POSTGRES_DSN")
PG_CONFIGURED = bool(PG_DSN)

pytestmark = [
    pytest.mark.skipif(not DEPS_AVAILABLE, reason="asyncpg not installed"),
    pytest.mark.skipif(not PG_CONFIGURED, reason="POSTGRES_DSN not set"),
]

# Lazy import — only when tests actually run
if DEPS_AVAILABLE:
    from cocoindex.connectors import postgres


# =============================================================================
# Test utilities
# =============================================================================


def _unique_name(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _run(loop: asyncio.AbstractEventLoop, coro: Any) -> Any:
    return loop.run_until_complete(coro)


async def _index_info(pool: "asyncpg.Pool", index_name: str) -> dict[str, Any] | None:
    """Return index info (amname) or None if index doesn't exist."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT am.amname FROM pg_index i "
            "JOIN pg_class c ON i.indexrelid = c.oid "
            "JOIN pg_am am ON c.relam = am.oid "
            "WHERE c.relname = $1",
            index_name,
        )
        return dict(row) if row else None


async def _table_exists(pool: "asyncpg.Pool", table_name: str) -> bool:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_name = $1 AND table_schema = 'public'",
            table_name,
        )
        return row is not None


async def _row_count(pool: "asyncpg.Pool", table_name: str) -> int:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(f'SELECT count(*) as cnt FROM "{table_name}"')
        assert row is not None
        return int(row["cnt"])


async def _drop_table(pool: "asyncpg.Pool", table_name: str) -> None:
    async with pool.acquire() as conn:
        await conn.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE')


async def _drop_index(pool: "asyncpg.Pool", index_name: str) -> None:
    async with pool.acquire() as conn:
        await conn.execute(f'DROP INDEX IF EXISTS "{index_name}"')


# =============================================================================
# Fixture
# =============================================================================


@pytest.fixture
def pg_env() -> Any:
    """Create an asyncpg pool and event loop for tests."""
    loop = asyncio.new_event_loop()
    pool = loop.run_until_complete(postgres.create_pool(PG_DSN))
    yield pool, loop
    loop.run_until_complete(pool.close())
    loop.close()


# =============================================================================
# Row types
# =============================================================================


@dataclass
class VectorRow:
    id: str
    content: str
    embedding: Annotated[
        NDArray[np.float32], VectorSchema(dtype=np.dtype(np.float32), size=4)
    ]


@dataclass
class TextRow:
    id: str
    content: str


# =============================================================================
# Tests
# =============================================================================


def test_postgres_declare_vector_index(pg_env: Any) -> None:
    """Vector index lifecycle: create with ivfflat → change to hnsw → remove table."""
    pool, loop = pg_env
    table_name = _unique_name("test_vec_idx")
    logical_name = "idx1"
    pg_index_name = f"{table_name}__vector__{logical_name}"
    tables_to_clean = [table_name]

    source_rows: list[VectorRow] = []
    declare_table: bool = True
    index_method: str = "ivfflat"

    try:
        with postgres.register_db(f"db_{table_name}", pool) as db:

            async def declare_fn() -> None:
                if not declare_table:
                    return
                table = await coco.use_mount(
                    coco.component_subpath("setup", "table"),
                    db.declare_table_target,
                    table_name,
                    await postgres.TableSchema.from_class(
                        VectorRow, primary_key=["id"]
                    ),
                )
                for row in source_rows:
                    table.declare_row(row=row)
                table.declare_vector_index(
                    name=logical_name,
                    column="embedding",
                    metric="cosine",
                    method=index_method,
                )

            app = coco.App(
                coco.AppConfig(name=f"test_vec_idx_{table_name}", environment=coco_env),
                declare_fn,
            )

            # Run 1: Create table + ivfflat index
            source_rows = [
                VectorRow(
                    id="1",
                    content="hello",
                    embedding=np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float32),
                ),
            ]
            app.update_blocking()

            info = _run(loop, _index_info(pool, pg_index_name))
            assert info is not None, f"Index {pg_index_name} should exist"
            assert info["amname"] == "ivfflat"

            # Run 2: Change to hnsw
            index_method = "hnsw"
            app.update_blocking()

            info = _run(loop, _index_info(pool, pg_index_name))
            assert info is not None, f"Index {pg_index_name} should still exist"
            assert info["amname"] == "hnsw"

            # Run 3: Remove table entirely
            declare_table = False
            app.update_blocking()

            assert not _run(loop, _table_exists(pool, table_name)), (
                "Table should be dropped"
            )

    finally:
        for t in tables_to_clean:
            _run(loop, _drop_table(pool, t))


def test_postgres_declare_vector_index_fingerprint_no_change(pg_env: Any) -> None:
    """Identical vector index spec across runs should not recreate the index."""
    pool, loop = pg_env
    table_name = _unique_name("test_vec_fp")
    logical_name = "idx1"
    pg_index_name = f"{table_name}__vector__{logical_name}"

    source_rows: list[VectorRow] = [
        VectorRow(
            id="1",
            content="hello",
            embedding=np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float32),
        ),
    ]

    try:
        with postgres.register_db(f"db_{table_name}", pool) as db:

            async def declare_fn() -> None:
                table = await coco.use_mount(
                    coco.component_subpath("setup", "table"),
                    db.declare_table_target,
                    table_name,
                    await postgres.TableSchema.from_class(
                        VectorRow, primary_key=["id"]
                    ),
                )
                for row in source_rows:
                    table.declare_row(row=row)
                table.declare_vector_index(
                    name=logical_name,
                    column="embedding",
                    metric="cosine",
                    method="ivfflat",
                )

            app = coco.App(
                coco.AppConfig(name=f"test_vec_fp_{table_name}", environment=coco_env),
                declare_fn,
            )

            # Run 1: Create
            app.update_blocking()
            info1 = _run(loop, _index_info(pool, pg_index_name))
            assert info1 is not None

            # Run 2: Identical spec — should be a no-op
            app.update_blocking()
            info2 = _run(loop, _index_info(pool, pg_index_name))
            assert info2 is not None
            assert info2["amname"] == "ivfflat"

    finally:
        _run(loop, _drop_table(pool, table_name))


def test_postgres_declare_sql_command_attachment(pg_env: Any) -> None:
    """SQL command attachment lifecycle: create index → change → remove (with teardown)."""
    pool, loop = pg_env
    table_name = _unique_name("test_sql_cmd")
    idx_name_v1 = f"{table_name}_fts_v1"
    idx_name_v2 = f"{table_name}_fts_v2"

    source_rows: list[TextRow] = []
    declare_table: bool = True
    current_setup_sql: str | None = None
    current_teardown_sql: str | None = None

    try:
        with postgres.register_db(f"db_{table_name}", pool) as db:

            async def declare_fn() -> None:
                if not declare_table:
                    return
                table = await coco.use_mount(
                    coco.component_subpath("setup", "table"),
                    db.declare_table_target,
                    table_name,
                    await postgres.TableSchema.from_class(TextRow, primary_key=["id"]),
                )
                for row in source_rows:
                    table.declare_row(row=row)
                if current_setup_sql is not None:
                    table.declare_sql_command_attachment(
                        name="custom_idx",
                        setup_sql=current_setup_sql,
                        teardown_sql=current_teardown_sql,
                    )

            app = coco.App(
                coco.AppConfig(name=f"test_sql_cmd_{table_name}", environment=coco_env),
                declare_fn,
            )

            # Run 1: Create table + btree index via SQL command
            source_rows = [TextRow(id="1", content="hello world")]
            current_setup_sql = (
                f'CREATE INDEX "{idx_name_v1}" ON "{table_name}" ("content")'
            )
            current_teardown_sql = f'DROP INDEX IF EXISTS "{idx_name_v1}"'
            app.update_blocking()

            info = _run(loop, _index_info(pool, idx_name_v1))
            assert info is not None, f"Index {idx_name_v1} should exist"

            # Run 2: Change setup_sql — teardown of v1 should run first
            current_setup_sql = f'CREATE INDEX "{idx_name_v2}" ON "{table_name}" ("id")'
            current_teardown_sql = f'DROP INDEX IF EXISTS "{idx_name_v2}"'
            app.update_blocking()

            # Old index should be torn down
            assert _run(loop, _index_info(pool, idx_name_v1)) is None, (
                f"Index {idx_name_v1} should have been torn down"
            )
            # New index should exist
            info_v2 = _run(loop, _index_info(pool, idx_name_v2))
            assert info_v2 is not None, f"Index {idx_name_v2} should exist"

            # Run 3: Remove attachment — teardown of v2 should run
            current_setup_sql = None
            current_teardown_sql = None
            app.update_blocking()

            # Teardown of v2 should have run
            assert _run(loop, _index_info(pool, idx_name_v2)) is None, (
                f"Index {idx_name_v2} should have been torn down"
            )
            # Table should still exist (only attachment removed)
            assert _run(loop, _table_exists(pool, table_name))

    finally:
        _run(loop, _drop_table(pool, table_name))


def test_postgres_sql_command_attachment_no_teardown(pg_env: Any) -> None:
    """Declare SQL command with teardown_sql=None, then remove. Should not error."""
    pool, loop = pg_env
    table_name = _unique_name("test_sql_notd")
    idx_name = f"{table_name}_idx"

    source_rows: list[TextRow] = [TextRow(id="1", content="hello")]
    declare_attachment: bool = True

    try:
        with postgres.register_db(f"db_{table_name}", pool) as db:

            async def declare_fn() -> None:
                table = await coco.use_mount(
                    coco.component_subpath("setup", "table"),
                    db.declare_table_target,
                    table_name,
                    await postgres.TableSchema.from_class(TextRow, primary_key=["id"]),
                )
                for row in source_rows:
                    table.declare_row(row=row)
                if declare_attachment:
                    table.declare_sql_command_attachment(
                        name="temp_idx",
                        setup_sql=f'CREATE INDEX "{idx_name}" ON "{table_name}" ("content")',
                        teardown_sql=None,
                    )

            app = coco.App(
                coco.AppConfig(
                    name=f"test_sql_notd_{table_name}", environment=coco_env
                ),
                declare_fn,
            )

            # Run 1: Create
            app.update_blocking()
            assert _run(loop, _index_info(pool, idx_name)) is not None

            # Run 2: Remove attachment — no teardown, should not error
            declare_attachment = False
            app.update_blocking()

            # Table should still exist
            assert _run(loop, _table_exists(pool, table_name))
            # Index persists since no teardown SQL was provided
            assert _run(loop, _index_info(pool, idx_name)) is not None

    finally:
        _run(loop, _drop_table(pool, table_name))


def test_postgres_mixed_rows_and_attachments(pg_env: Any) -> None:
    """Rows and vector index coexist correctly under the same table."""
    pool, loop = pg_env
    table_name = _unique_name("test_mixed")
    logical_name = "idx1"
    pg_index_name = f"{table_name}__vector__{logical_name}"

    source_rows: list[VectorRow] = []
    index_method: str = "ivfflat"
    declare_table: bool = True

    try:
        with postgres.register_db(f"db_{table_name}", pool) as db:

            async def declare_fn() -> None:
                if not declare_table:
                    return
                table = await coco.use_mount(
                    coco.component_subpath("setup", "table"),
                    db.declare_table_target,
                    table_name,
                    await postgres.TableSchema.from_class(
                        VectorRow, primary_key=["id"]
                    ),
                )
                for row in source_rows:
                    table.declare_row(row=row)
                table.declare_vector_index(
                    name=logical_name,
                    column="embedding",
                    metric="cosine",
                    method=index_method,
                )

            app = coco.App(
                coco.AppConfig(name=f"test_mixed_{table_name}", environment=coco_env),
                declare_fn,
            )

            # Run 1: Declare rows + vector index
            source_rows = [
                VectorRow(
                    id="1",
                    content="alpha",
                    embedding=np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float32),
                ),
                VectorRow(
                    id="2",
                    content="beta",
                    embedding=np.array([0.0, 1.0, 0.0, 0.0], dtype=np.float32),
                ),
            ]
            app.update_blocking()

            assert _run(loop, _row_count(pool, table_name)) == 2
            info = _run(loop, _index_info(pool, pg_index_name))
            assert info is not None
            assert info["amname"] == "ivfflat"

            # Run 2: Change rows only, keep index same
            source_rows = [
                VectorRow(
                    id="1",
                    content="alpha updated",
                    embedding=np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float32),
                ),
                VectorRow(
                    id="3",
                    content="gamma",
                    embedding=np.array([0.0, 0.0, 1.0, 0.0], dtype=np.float32),
                ),
            ]
            app.update_blocking()

            assert _run(loop, _row_count(pool, table_name)) == 2
            info = _run(loop, _index_info(pool, pg_index_name))
            assert info is not None
            assert info["amname"] == "ivfflat"  # unchanged

            # Run 3: Change index only, keep rows same
            index_method = "hnsw"
            app.update_blocking()

            assert _run(loop, _row_count(pool, table_name)) == 2
            info = _run(loop, _index_info(pool, pg_index_name))
            assert info is not None
            assert info["amname"] == "hnsw"  # changed

    finally:
        _run(loop, _drop_table(pool, table_name))
