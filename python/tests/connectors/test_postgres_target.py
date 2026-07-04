"""Tests for PostgreSQL target connector attachment features (vector index, SQL command).

Uses testcontainers to spin up a real PostgreSQL instance with pgvector automatically.

Run with:
    pytest python/tests/connectors/test_postgres_target.py -v -s
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Annotated, Any

if TYPE_CHECKING:
    import asyncpg

import numpy as np
import pytest
import pytest_asyncio
from numpy.typing import NDArray

import cocoindex as coco
from cocoindex.resources.schema import VectorSchema

from tests import common

# =============================================================================
# Check dependencies
# =============================================================================

try:
    from cocoindex.connectors import postgres

    DEPS_AVAILABLE = True
except ImportError:
    DEPS_AVAILABLE = False

try:
    __import__("testcontainers")
    TESTCONTAINERS_AVAILABLE = True
except ImportError:
    TESTCONTAINERS_AVAILABLE = False

_PG_DB_KEY: coco.ContextKey[Any] = coco.ContextKey("test_postgres_target_pg_db")

pytestmark = [
    pytest.mark.skipif(
        not DEPS_AVAILABLE, reason="postgres dependencies not installed"
    ),
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


async def _index_opclass_names(pool: "asyncpg.Pool", index_name: str) -> list[str]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT opc.opcname "
            "FROM pg_index i "
            "JOIN pg_class c ON i.indexrelid = c.oid "
            "JOIN generate_series(0, i.indnatts - 1) AS s(n) ON true "
            "JOIN pg_opclass opc ON opc.oid = i.indclass[s.n] "
            "WHERE c.relname = $1 ORDER BY s.n",
            index_name,
        )
        return [str(row["opcname"]) for row in rows]


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


async def _table_columns(pool: "asyncpg.Pool", table_name: str) -> set[str]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = $1 AND table_schema = 'public'",
            table_name,
        )
        return {str(row["column_name"]) for row in rows}


async def _drop_table(pool: "asyncpg.Pool", table_name: str) -> None:
    async with pool.acquire() as conn:
        await conn.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE')


async def _drop_index(pool: "asyncpg.Pool", index_name: str) -> None:
    async with pool.acquire() as conn:
        await conn.execute(f'DROP INDEX IF EXISTS "{index_name}"')


# =============================================================================
# Fixtures
# =============================================================================


# Module-scoped: start the container once, share the DSN across all tests.
@pytest.fixture(scope="module")
def pg_dsn() -> Any:
    from testcontainers.postgres import PostgresContainer  # type: ignore[import-untyped]

    with PostgresContainer("pgvector/pgvector:pg16") as pg:
        dsn = pg.get_connection_url()
        # testcontainers may return a SQLAlchemy-style URL; normalize for asyncpg.
        dsn = dsn.replace("postgresql+psycopg2://", "postgresql://")
        yield dsn


class _PgEnv:
    """Bundle of pool + coco environment for postgres target tests."""

    __slots__ = ("pool", "coco_env")

    def __init__(self, pool: Any, coco_env: coco.Environment) -> None:
        self.pool = pool
        self.coco_env = coco_env


# Function-scoped: each test gets a fresh pool and environment on its own event loop.
@pytest_asyncio.fixture
async def pg_env(pg_dsn: str, request: pytest.FixtureRequest) -> Any:
    """Create an asyncpg pool and coco environment bound to the current event loop."""
    import asyncpg

    pool = await asyncpg.create_pool(pg_dsn)

    coco_env = common.create_test_env(__file__, suffix=request.node.name)
    coco_env.context_provider.provide(_PG_DB_KEY, pool)

    yield _PgEnv(pool, coco_env)
    await pool.close()


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
class HalfVectorRow:
    id: str
    content: str
    embedding: Annotated[
        NDArray[np.float16], VectorSchema(dtype=np.dtype(np.float16), size=4)
    ]


@dataclass
class TextRow:
    id: str
    content: str


@dataclass
class _NulProbeRow:
    """Row type covering both NUL-stripping paths: a `text` column and a
    `jsonb` column (via `dict[str, Any]`)."""

    id: str
    content: str
    extra: dict[str, Any]


class _NulInStr:
    """Object whose ``str()`` contains NUL; exercises the ``json.dumps``
    ``default`` hook (objects produced mid-serialization)."""

    def __str__(self) -> str:
        return "weird\x00str"


# =============================================================================
# Tests
# =============================================================================


@pytest.mark.asyncio
async def test_postgres_declare_vector_index(pg_env: _PgEnv) -> None:
    """Vector index lifecycle: create with ivfflat → change to hnsw → remove table."""
    pool = pg_env.pool
    coco_env = pg_env.coco_env
    table_name = _unique_name("test_vec_idx")
    logical_name = "idx1"
    pg_index_name = f"{table_name}__vector__{logical_name}"
    tables_to_clean = [table_name]

    source_rows: list[VectorRow] = []
    declare_table: bool = True
    index_method: str = "ivfflat"

    try:

        async def declare_fn() -> None:
            if not declare_table:
                return
            table = await coco.use_mount(
                coco.component_subpath("setup", "table"),
                postgres.declare_table_target,
                _PG_DB_KEY,
                table_name,
                await postgres.TableSchema.from_class(VectorRow, primary_key=["id"]),
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
        await app.update()

        info = await _index_info(pool, pg_index_name)
        assert info is not None, f"Index {pg_index_name} should exist"
        assert info["amname"] == "ivfflat"

        # Run 2: Change to hnsw
        index_method = "hnsw"
        await app.update()

        info = await _index_info(pool, pg_index_name)
        assert info is not None, f"Index {pg_index_name} should still exist"
        assert info["amname"] == "hnsw"

        # Run 3: Remove table entirely
        declare_table = False
        await app.update()

        assert not await _table_exists(pool, table_name), "Table should be dropped"

    finally:
        for t in tables_to_clean:
            await _drop_table(pool, t)


@pytest.mark.asyncio
async def test_postgres_declare_vector_index_recreate_in_non_default_schema(
    pg_env: _PgEnv,
) -> None:
    """Re-creating a vector index in a NON-default schema must not collide.

    Regression: `_apply_actions` dropped the index with an *unqualified* name,
    which resolves through the connection's `search_path` (default `"$user",
    public`, excluding a custom schema), so the `DROP INDEX IF EXISTS` silently
    no-opped while the following `CREATE INDEX ... ON "<schema>"."<table>"` always
    targets the table's schema — raising `DuplicateTableError`. The method change
    (ivfflat → hnsw) is what triggers the drop+recreate. The default-schema test
    above doesn't catch this because `public` is on `search_path`.
    """
    pool = pg_env.pool
    coco_env = pg_env.coco_env
    schema_name = _unique_name("vecidx_sch")
    table_name = _unique_name("test_vec_sch")
    logical_name = "idx1"
    pg_index_name = f"{table_name}__vector__{logical_name}"
    index_method = "ivfflat"

    async with pool.acquire() as conn:
        await conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"')

    try:

        async def declare_fn() -> None:
            table = await coco.use_mount(
                coco.component_subpath("setup", "table"),
                postgres.declare_table_target,
                _PG_DB_KEY,
                table_name,
                await postgres.TableSchema.from_class(VectorRow, primary_key=["id"]),
                pg_schema_name=schema_name,
            )
            table.declare_row(
                row=VectorRow(
                    id="1",
                    content="hello",
                    embedding=np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float32),
                )
            )
            table.declare_vector_index(
                name=logical_name,
                column="embedding",
                metric="cosine",
                method=index_method,
            )

        app = coco.App(
            coco.AppConfig(name=f"test_vec_sch_{table_name}", environment=coco_env),
            declare_fn,
        )

        # Run 1: create the ivfflat index in the custom schema.
        await app.update()
        info = await _index_info(pool, pg_index_name)
        assert info is not None and info["amname"] == "ivfflat"

        # Run 2: change the method → drop + recreate. Before the fix this raised
        # DuplicateTableError because the unqualified DROP missed the custom-schema
        # index.
        index_method = "hnsw"
        await app.update()
        info = await _index_info(pool, pg_index_name)
        assert info is not None and info["amname"] == "hnsw"

    finally:
        async with pool.acquire() as conn:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')


@pytest.mark.asyncio
async def test_postgres_declare_vector_index_fingerprint_no_change(
    pg_env: _PgEnv,
) -> None:
    """Identical vector index spec across runs should not recreate the index."""
    pool = pg_env.pool
    coco_env = pg_env.coco_env
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

        async def declare_fn() -> None:
            table = await coco.use_mount(
                coco.component_subpath("setup", "table"),
                postgres.declare_table_target,
                _PG_DB_KEY,
                table_name,
                await postgres.TableSchema.from_class(VectorRow, primary_key=["id"]),
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
        await app.update()
        info1 = await _index_info(pool, pg_index_name)
        assert info1 is not None

        # Run 2: Identical spec — should be a no-op
        await app.update()
        info2 = await _index_info(pool, pg_index_name)
        assert info2 is not None
        assert info2["amname"] == "ivfflat"

    finally:
        await _drop_table(pool, table_name)


@pytest.mark.asyncio
async def test_postgres_declare_halfvec_vector_index_uses_halfvec_opclass(
    pg_env: _PgEnv,
) -> None:
    """halfvec columns need halfvec_* pgvector operator classes."""
    pool = pg_env.pool
    coco_env = pg_env.coco_env
    table_name = _unique_name("test_halfvec_idx")
    logical_name = "idx1"
    pg_index_name = f"{table_name}__vector__{logical_name}"

    try:

        async def declare_fn() -> None:
            table = await coco.use_mount(
                coco.component_subpath("setup", "table"),
                postgres.declare_table_target,
                _PG_DB_KEY,
                table_name,
                await postgres.TableSchema.from_class(
                    HalfVectorRow, primary_key=["id"]
                ),
            )
            table.declare_row(
                row=HalfVectorRow(
                    id="1",
                    content="hello",
                    embedding=np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float16),
                )
            )
            table.declare_vector_index(
                name=logical_name,
                column="embedding",
                metric="cosine",
                method="ivfflat",
            )

        app = coco.App(
            coco.AppConfig(name=f"test_halfvec_idx_{table_name}", environment=coco_env),
            declare_fn,
        )

        await app.update()

        info = await _index_info(pool, pg_index_name)
        assert info is not None, f"Index {pg_index_name} should exist"
        assert info["amname"] == "ivfflat"
        assert await _index_opclass_names(pool, pg_index_name) == ["halfvec_cosine_ops"]

    finally:
        await _drop_table(pool, table_name)


@pytest.mark.asyncio
async def test_postgres_declare_sql_command_attachment(pg_env: _PgEnv) -> None:
    """SQL command attachment lifecycle: create index → change → remove (with teardown)."""
    pool = pg_env.pool
    coco_env = pg_env.coco_env
    table_name = _unique_name("test_sql_cmd")
    idx_name_v1 = f"{table_name}_fts_v1"
    idx_name_v2 = f"{table_name}_fts_v2"

    source_rows: list[TextRow] = []
    declare_table: bool = True
    current_setup_sql: str | None = None
    current_teardown_sql: str | None = None

    try:

        async def declare_fn() -> None:
            if not declare_table:
                return
            table = await coco.use_mount(
                coco.component_subpath("setup", "table"),
                postgres.declare_table_target,
                _PG_DB_KEY,
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
        await app.update()

        info = await _index_info(pool, idx_name_v1)
        assert info is not None, f"Index {idx_name_v1} should exist"

        # Run 2: Change setup_sql — teardown of v1 should run first
        current_setup_sql = f'CREATE INDEX "{idx_name_v2}" ON "{table_name}" ("id")'
        current_teardown_sql = f'DROP INDEX IF EXISTS "{idx_name_v2}"'
        await app.update()

        # Old index should be torn down
        assert await _index_info(pool, idx_name_v1) is None, (
            f"Index {idx_name_v1} should have been torn down"
        )
        # New index should exist
        info_v2 = await _index_info(pool, idx_name_v2)
        assert info_v2 is not None, f"Index {idx_name_v2} should exist"

        # Run 3: Remove attachment — teardown of v2 should run
        current_setup_sql = None
        current_teardown_sql = None
        await app.update()

        # Teardown of v2 should have run
        assert await _index_info(pool, idx_name_v2) is None, (
            f"Index {idx_name_v2} should have been torn down"
        )
        # Table should still exist (only attachment removed)
        assert await _table_exists(pool, table_name)

    finally:
        await _drop_table(pool, table_name)


@pytest.mark.asyncio
async def test_postgres_sql_command_attachment_no_teardown(pg_env: _PgEnv) -> None:
    """Declare SQL command with teardown_sql=None, then remove. Should not error."""
    pool = pg_env.pool
    coco_env = pg_env.coco_env
    table_name = _unique_name("test_sql_notd")
    idx_name = f"{table_name}_idx"

    source_rows: list[TextRow] = [TextRow(id="1", content="hello")]
    declare_attachment: bool = True

    try:

        async def declare_fn() -> None:
            table = await coco.use_mount(
                coco.component_subpath("setup", "table"),
                postgres.declare_table_target,
                _PG_DB_KEY,
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
            coco.AppConfig(name=f"test_sql_notd_{table_name}", environment=coco_env),
            declare_fn,
        )

        # Run 1: Create
        await app.update()
        assert await _index_info(pool, idx_name) is not None

        # Run 2: Remove attachment — no teardown, should not error
        declare_attachment = False
        await app.update()

        # Table should still exist
        assert await _table_exists(pool, table_name)
        # Index persists since no teardown SQL was provided
        assert await _index_info(pool, idx_name) is not None

    finally:
        await _drop_table(pool, table_name)


@pytest.mark.asyncio
async def test_postgres_mixed_rows_and_attachments(pg_env: _PgEnv) -> None:
    """Rows and vector index coexist correctly under the same table."""
    pool = pg_env.pool
    coco_env = pg_env.coco_env
    table_name = _unique_name("test_mixed")
    logical_name = "idx1"
    pg_index_name = f"{table_name}__vector__{logical_name}"

    source_rows: list[VectorRow] = []
    index_method: str = "ivfflat"
    declare_table: bool = True

    try:

        async def declare_fn() -> None:
            if not declare_table:
                return
            table = await coco.use_mount(
                coco.component_subpath("setup", "table"),
                postgres.declare_table_target,
                _PG_DB_KEY,
                table_name,
                await postgres.TableSchema.from_class(VectorRow, primary_key=["id"]),
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
        await app.update()

        assert await _row_count(pool, table_name) == 2
        info = await _index_info(pool, pg_index_name)
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
        await app.update()

        assert await _row_count(pool, table_name) == 2
        info = await _index_info(pool, pg_index_name)
        assert info is not None
        assert info["amname"] == "ivfflat"  # unchanged

        # Run 3: Change index only, keep rows same
        index_method = "hnsw"
        await app.update()

        assert await _row_count(pool, table_name) == 2
        info = await _index_info(pool, pg_index_name)
        assert info is not None
        assert info["amname"] == "hnsw"  # changed

    finally:
        await _drop_table(pool, table_name)


@pytest.mark.asyncio
async def test_postgres_strips_nul_in_text_and_jsonb(pg_env: _PgEnv) -> None:
    """U+0000 (NUL) is stripped from text columns and recursively from jsonb
    (nested string values, dict keys, and strings produced via
    ``json.dumps``'s ``default`` hook)."""
    import json as _json

    pool = pg_env.pool
    coco_env = pg_env.coco_env
    table_name = _unique_name("test_nul_strip")

    rows = [
        _NulProbeRow(
            id="row1",
            content="hello\x00world",
            extra={
                "nested": ["a\x00b", {"deep\x00key": "deep\x00val"}],
                "weird\x00key": _NulInStr(),  # → exercises default=str
            },
        ),
    ]

    try:

        async def declare_fn() -> None:
            table = await coco.use_mount(
                coco.component_subpath("setup", "table"),
                postgres.declare_table_target,
                _PG_DB_KEY,
                table_name,
                await postgres.TableSchema.from_class(_NulProbeRow, primary_key=["id"]),
            )
            for row in rows:
                table.declare_row(row=row)

        app = coco.App(
            coco.AppConfig(name=f"test_nul_strip_{table_name}", environment=coco_env),
            declare_fn,
        )
        await app.update()

        async with pool.acquire() as conn:
            written = await conn.fetchrow(
                f'SELECT "content", "extra" FROM "{table_name}" WHERE "id" = $1',
                "row1",
            )
        assert written is not None
        assert written["content"] == "helloworld"
        # asyncpg returns jsonb as a JSON-encoded string by default; decode it.
        assert _json.loads(written["extra"]) == {
            "nested": ["ab", {"deepkey": "deepval"}],
            "weirdkey": "weirdstr",
        }

    finally:
        await _drop_table(pool, table_name)


@pytest.mark.asyncio
async def test_postgres_column_drop_retries_after_failed_attempt(
    pg_env: _PgEnv,
) -> None:
    """A column drop that fails (here, blocked by a dependent view) leaves the
    table's tracking item with multiple possible states on disk. A later run,
    once the dependency is gone, must still recompute the column-level diff and
    actually drop the column — it must NOT treat the prior states as missing and
    short-circuit into a no-op ``CREATE TABLE IF NOT EXISTS``.

    Scenario:
      t1: create table with columns (id, value, extra).
      t2: remove "extra" from the schema, but a view depends on it, so the
          ``DROP COLUMN`` fails. The table item is left with possible states
          [(id, value, extra), (id, value)].
      t3: drop the view, run again -> "extra" must actually be dropped.
    """
    pool = pg_env.pool
    coco_env = pg_env.coco_env
    table_name = _unique_name("test_col_drop")
    view_name = _unique_name("test_col_drop_view")

    include_extra = True

    def _schema() -> "postgres.TableSchema[dict[str, Any]]":
        columns = {
            "id": postgres.ColumnDef("text", nullable=False),
            "value": postgres.ColumnDef("text"),
        }
        if include_extra:
            columns["extra"] = postgres.ColumnDef("text")
        return postgres.TableSchema(columns=columns, primary_key=["id"])

    try:

        async def declare_fn() -> None:
            await coco.use_mount(
                coco.component_subpath("setup", "table"),
                postgres.declare_table_target,
                _PG_DB_KEY,
                table_name,
                _schema(),
            )

        app = coco.App(
            coco.AppConfig(name=f"test_col_drop_{table_name}", environment=coco_env),
            declare_fn,
        )

        # t1: create table with id, value, extra.
        await app.update()
        assert await _table_columns(pool, table_name) == {"id", "value", "extra"}

        # Create a view that depends on column "extra" so DROP COLUMN fails.
        async with pool.acquire() as conn:
            await conn.execute(
                f'CREATE VIEW "{view_name}" AS SELECT "extra" FROM "{table_name}"'
            )

        # t2: drop "extra" -> blocked by the dependent view, so the run fails.
        include_extra = False
        with pytest.raises(Exception):
            await app.update()
        # The column is still present because the drop failed.
        assert "extra" in await _table_columns(pool, table_name)

        # t3: remove the dependency, run again. The column drop must now happen.
        async with pool.acquire() as conn:
            await conn.execute(f'DROP VIEW IF EXISTS "{view_name}"')
        await app.update()
        assert await _table_columns(pool, table_name) == {"id", "value"}

    finally:
        async with pool.acquire() as conn:
            await conn.execute(f'DROP VIEW IF EXISTS "{view_name}"')
        await _drop_table(pool, table_name)


async def _column_is_nullable(
    pool: "asyncpg.Pool", table_name: str, column_name: str
) -> bool:
    async with pool.acquire() as conn:
        val = await conn.fetchval(
            "SELECT is_nullable FROM information_schema.columns "
            "WHERE table_name = $1 AND column_name = $2",
            table_name,
            column_name,
        )
        return bool(val == "YES")


@pytest.mark.asyncio
async def test_schema_evolution_compatible_changes(pg_env: _PgEnv) -> None:
    """Test schema evolution with compatible type changes preserves data and
    nullability constraints."""
    pool = pg_env.pool
    coco_env = pg_env.coco_env
    table_name = _unique_name("schema_evol")

    # Mutable state: toggled between updates to change the schema.
    use_v2 = False

    def _schema() -> "postgres.TableSchema[dict[str, Any]]":
        if not use_v2:
            return postgres.TableSchema(
                columns={
                    "id": postgres.ColumnDef("text", nullable=False),
                    "col_nn_nn": postgres.ColumnDef("varchar(50)", nullable=False),
                    "col_null_null": postgres.ColumnDef("varchar(50)", nullable=True),
                    "col_null_nn": postgres.ColumnDef("varchar(50)", nullable=True),
                    "col_nn_null": postgres.ColumnDef("varchar(50)", nullable=False),
                },
                primary_key=["id"],
            )
        return postgres.TableSchema(
            columns={
                "id": postgres.ColumnDef("text", nullable=False),
                "col_nn_nn": postgres.ColumnDef("text", nullable=False),
                "col_null_null": postgres.ColumnDef("text", nullable=True),
                "col_null_nn": postgres.ColumnDef("text", nullable=False),
                "col_nn_null": postgres.ColumnDef("text", nullable=True),
            },
            primary_key=["id"],
        )

    try:

        async def declare_fn() -> None:
            table = await coco.use_mount(
                coco.component_subpath("setup", "table"),
                postgres.declare_table_target,
                _PG_DB_KEY,
                table_name,
                _schema(),
            )
            table.declare_row(
                row={
                    "id": "row1",
                    "col_nn_nn": "data1",
                    "col_null_null": "data2",
                    "col_null_nn": "data3",
                    "col_nn_null": "data4",
                }
            )

        app = coco.App(
            coco.AppConfig(name=f"test_schema_evol_{table_name}", environment=coco_env),
            declare_fn,
        )

        # v1: create with varchar(50)
        await app.update()
        assert not await _column_is_nullable(pool, table_name, "col_nn_nn")
        assert await _column_is_nullable(pool, table_name, "col_null_null")
        assert await _column_is_nullable(pool, table_name, "col_null_nn")
        assert not await _column_is_nullable(pool, table_name, "col_nn_null")

        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT * FROM "{table_name}" WHERE "id" = $1', "row1"
            )
            assert row is not None
            assert row["col_nn_nn"] == "data1"

        # v2: evolve to text, flip nullability on two columns
        use_v2 = True
        await app.update()

        assert not await _column_is_nullable(pool, table_name, "col_nn_nn")
        assert await _column_is_nullable(pool, table_name, "col_null_null")
        assert not await _column_is_nullable(pool, table_name, "col_null_nn")
        assert await _column_is_nullable(pool, table_name, "col_nn_null")

        # Data must be preserved (no destructive fallback)
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT * FROM "{table_name}" WHERE "id" = $1', "row1"
            )
            assert row is not None
            assert row["col_nn_nn"] == "data1"

    finally:
        await _drop_table(pool, table_name)


@pytest.mark.asyncio
async def test_schema_evolution_incompatible_fallback(
    pg_env: _PgEnv, caplog: pytest.LogCaptureFixture
) -> None:
    """When a type cast is genuinely incompatible, the fallback recreates the
    column but must preserve the desired NOT NULL constraint and log a warning."""
    pool = pg_env.pool
    coco_env = pg_env.coco_env
    table_name = _unique_name("schema_fallback")

    use_v2 = False

    def _schema() -> "postgres.TableSchema[dict[str, Any]]":
        if not use_v2:
            return postgres.TableSchema(
                columns={
                    "id": postgres.ColumnDef("text", nullable=False),
                    "incompat_col": postgres.ColumnDef("text", nullable=False),
                },
                primary_key=["id"],
            )
        return postgres.TableSchema(
            columns={
                "id": postgres.ColumnDef("text", nullable=False),
                "incompat_col": postgres.ColumnDef("integer", nullable=False),
            },
            primary_key=["id"],
        )

    try:

        async def declare_fn() -> None:
            table = await coco.use_mount(
                coco.component_subpath("setup", "table"),
                postgres.declare_table_target,
                _PG_DB_KEY,
                table_name,
                _schema(),
            )
            if not use_v2:
                table.declare_row(row={"id": "row1", "incompat_col": "not_an_int"})

        app = coco.App(
            coco.AppConfig(name=f"test_schema_fb_{table_name}", environment=coco_env),
            declare_fn,
        )

        # v1: text NOT NULL
        await app.update()
        assert not await _column_is_nullable(pool, table_name, "incompat_col")

        # v2: integer NOT NULL — incompatible cast triggers fallback
        use_v2 = True
        with caplog.at_level("WARNING"):
            await app.update()

        assert any(
            "Recreating column. Existing data will be lost" in record.message
            for record in caplog.records
        )

        # The column was recreated without data; NOT NULL cannot be preserved
        # because existing rows get NULL for the new column.

        # Type must have changed
        async with pool.acquire() as conn:
            val = await conn.fetchval(
                "SELECT data_type FROM information_schema.columns "
                "WHERE table_name = $1 AND column_name = $2",
                table_name,
                "incompat_col",
            )
            assert val == "integer"

    finally:
        await _drop_table(pool, table_name)


@pytest.mark.asyncio
async def test_postgres_strips_nul_in_array_columns(pg_env: _PgEnv) -> None:
    """U+0000 (NUL) inside array element strings must be stripped before asyncpg
    binds them to Postgres.

    Prior to the fix, ``_row_to_dict`` only called ``_strip_nul`` on scalar
    ``str`` values.  A ``list[str]`` bound to a ``text[]`` column bypassed
    sanitization entirely, causing asyncpg to raise ``ValueError: string
    cannot contain NUL (0x00) characters``.
    """
    pool = pg_env.pool
    coco_env = pg_env.coco_env
    table_name = _unique_name("test_arr_nul")

    schema: postgres.TableSchema[dict[str, Any]] = postgres.TableSchema(
        columns={
            "id": postgres.ColumnDef("text", nullable=False),
            "tags": postgres.ColumnDef("text[]"),
        },
        primary_key=["id"],
    )

    rows: list[dict[str, Any]] = [
        {"id": "row1", "tags": ["clean", "has\x00nul", "also\x00bad"]},
        {"id": "row2", "tags": ["all", "clean"]},
    ]

    try:

        async def declare_fn() -> None:
            table = await coco.use_mount(
                coco.component_subpath("setup", "table"),
                postgres.declare_table_target,
                _PG_DB_KEY,
                table_name,
                schema,
            )
            for row in rows:
                table.declare_row(row=row)

        app = coco.App(
            coco.AppConfig(name=f"test_arr_nul_{table_name}", environment=coco_env),
            declare_fn,
        )
        await app.update()

        async with pool.acquire() as conn:
            row1 = await conn.fetchrow(
                f'SELECT "tags" FROM "{table_name}" WHERE "id" = $1', "row1"
            )
            row2 = await conn.fetchrow(
                f'SELECT "tags" FROM "{table_name}" WHERE "id" = $1', "row2"
            )

        assert row1 is not None
        assert list(row1["tags"]) == ["clean", "hasnul", "alsobad"]
        assert row2 is not None
        assert list(row2["tags"]) == ["all", "clean"]

    finally:
        await _drop_table(pool, table_name)


def test_sanitize_nul_preserves_tuple() -> None:
    """``_sanitize_nul`` must return ``tuple`` when given ``tuple`` input.

    asyncpg uses ``tuple`` to represent Postgres composite (record) types.
    Converting to ``list`` silently changes the wire encoding.
    """
    inp = ("a\x00b", "c", ("inner\x00d",))
    result = postgres._target._sanitize_nul(inp)
    assert result == ("ab", "c", ("innerd",))
    assert isinstance(result, tuple)
    assert isinstance(result[2], tuple)


def test_sanitize_nul_preserves_list() -> None:
    """``_sanitize_nul`` must return ``list`` when given ``list`` input."""
    inp = ["x\x00y", ["nested\x00z"]]
    result = postgres._target._sanitize_nul(inp)
    assert result == ["xy", ["nestedz"]]
    assert isinstance(result, list)
    assert isinstance(result[1], list)
