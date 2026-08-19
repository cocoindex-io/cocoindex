"""End-to-end tests for optimistic writes through the PostgreSQL target.

Two capabilities are covered, deliberately kept apart:

* ``TableTarget.optimistic_declare_row`` combines immediate Postgres
  visibility, normal submit confirmation, cleanup, and AppStore CAS winner
  election for one primary key.

Uses testcontainers to spin up a real PostgreSQL instance automatically.

Run with:
    pytest python/tests/connectors/test_postgres_optimistic_target.py -v -s
"""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import asyncpg

import pytest
import pytest_asyncio

import cocoindex as coco

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

_PG_DB_KEY: coco.ContextKey[Any] = coco.ContextKey("test_postgres_optimistic_pg_db")

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
# Fixtures
# =============================================================================


@pytest.fixture(scope="module")
def pg_dsn() -> Any:
    from testcontainers.postgres import PostgresContainer  # type: ignore[import-untyped]

    with PostgresContainer("postgres:16-alpine") as pg:
        yield pg.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")


class _PgEnv:
    """Bundle of pool + coco environment for optimistic-write tests."""

    __slots__ = ("pool", "coco_env")

    def __init__(self, pool: Any, coco_env: coco.Environment) -> None:
        self.pool = pool
        self.coco_env = coco_env


@pytest_asyncio.fixture
async def pg_env(pg_dsn: str, request: pytest.FixtureRequest) -> Any:
    import asyncpg

    pool = await asyncpg.create_pool(pg_dsn)
    coco_env = common.create_test_env(__file__, suffix=request.node.name)
    coco_env.context_provider.provide(_PG_DB_KEY, pool)
    yield _PgEnv(pool, coco_env)
    await pool.close()


# =============================================================================
# Shared state, row type, and component definitions
# =============================================================================


@dataclass
class Entity:
    """`name` is the identity; `id` is the payload each writer proposes."""

    name: str
    id: str


class _Scenario:
    """Per-test knobs read by the module-level component functions.

    Components must be module-level `@coco.fn`s (so their stable paths are
    stable across runs), so the per-test wiring travels through here rather
    than through closures.
    """

    table_name: str = ""
    # Proposed entity id per component name.
    proposals: dict[str, str] = {}
    # Component name -> what `optimistic_declare_row` returned.
    claim_results: dict[str, bool] = {}
    # Component name -> id read back from Postgres right after its own write.
    observed: dict[str, str | None] = {}
    # Names whose component should raise after writing optimistically.
    fail_after_write: set[str] = set()
    # Rendezvous so every component reaches the write at the same moment.
    barrier: asyncio.Barrier | None = None
    # Set when a component caught an error from its own eager write.
    caught_eager_error: dict[str, str] = {}
    executions: int = 0

    @classmethod
    def reset(cls, table_name: str) -> None:
        cls.table_name = table_name
        cls.proposals = {}
        cls.claim_results = {}
        cls.observed = {}
        cls.fail_after_write = set()
        cls.barrier = None
        cls.caught_eager_error = {}
        cls.executions = 0


async def _select_id(pool: "asyncpg.Pool", table: str, name: str) -> str | None:
    async with pool.acquire() as conn:
        row_id = await conn.fetchval(f'SELECT id FROM "{table}" WHERE name = $1', name)
    return None if row_id is None else str(row_id)


async def _all_rows(pool: "asyncpg.Pool", table: str) -> list[dict[str, Any]]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(f'SELECT name, id FROM "{table}" ORDER BY name')
        return [dict(r) for r in rows]


async def _drop_table(pool: "asyncpg.Pool", table: str) -> None:
    async with pool.acquire() as conn:
        await conn.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')


async def _entity_schema() -> Any:
    return await postgres.TableSchema.from_class(Entity, primary_key=["name"])


@coco.fn
async def _optimistic_writer(
    table: postgres.TableTarget[Entity], comp: str, name: str
) -> None:
    """Claims/writes one row, then reads it back through raw SQL."""
    if _Scenario.barrier is not None:
        await _Scenario.barrier.wait()
    won = await table.optimistic_declare_row(
        row=Entity(name=name, id=_Scenario.proposals[comp])
    )
    _Scenario.claim_results[comp] = won
    pool = coco.use_context(_PG_DB_KEY)
    _Scenario.observed[comp] = await _select_id(pool, _Scenario.table_name, name)
    if comp in _Scenario.fail_after_write:
        raise RuntimeError(f"component {comp} failed after its optimistic write")


@coco.fn(memo=True)
async def _memoized_optimistic_writer(
    table: postgres.TableTarget[Entity], comp: str, name: str
) -> None:
    _Scenario.executions += 1
    won = await table.optimistic_declare_row(
        row=Entity(name=name, id=_Scenario.proposals[comp])
    )
    if not won:
        raise RuntimeError("first memoized execution unexpectedly lost its claim")


@coco.fn
async def _fail_before_optimistic_write(
    table: postgres.TableTarget[Entity], comp: str, name: str
) -> None:
    raise RuntimeError(f"component {comp} failed before writing {name} to {table}")


@coco.fn
async def _conditional_writer(
    table: postgres.TableTarget[Entity], comp: str, name: str
) -> None:
    """Get-or-create: look up first, claim only when the row is missing."""
    pool = coco.use_context(_PG_DB_KEY)
    existing = await _select_id(pool, _Scenario.table_name, name)
    if _Scenario.barrier is not None:
        # Everyone has now observed "not found"; race the creation.
        await _Scenario.barrier.wait()

    if existing is not None:
        # Reuse the visible row without claiming ownership of a new one.
        _Scenario.claim_results[comp] = False
        table.declare_row(row=Entity(name=name, id=existing))
        _Scenario.observed[comp] = existing
        return

    won = await table.optimistic_declare_row(
        row=Entity(name=name, id=_Scenario.proposals[comp])
    )
    _Scenario.claim_results[comp] = won
    _Scenario.observed[comp] = await _select_id(pool, _Scenario.table_name, name)
    if won and comp in _Scenario.fail_after_write:
        raise RuntimeError(f"component {comp} failed after winning the claim")


@coco.fn
async def _blocked_then_healing_writer(
    table: postgres.TableTarget[Entity], comp: str, name: str
) -> None:
    """Eager write hits a Postgres CHECK constraint; the error is caught and
    the constraint removed, so normal submit still lands the row."""
    pool = coco.use_context(_PG_DB_KEY)
    async with pool.acquire() as conn:
        await conn.execute(
            f'ALTER TABLE "{_Scenario.table_name}" '
            f"ADD CONSTRAINT block_eager CHECK (name <> '{name}')"
        )
    try:
        await table.optimistic_declare_row(
            row=Entity(name=name, id=_Scenario.proposals[comp])
        )
    except Exception as e:  # noqa: BLE001 - the point of the test
        _Scenario.caught_eager_error[comp] = str(e)
    finally:
        async with pool.acquire() as conn:
            await conn.execute(
                f'ALTER TABLE "{_Scenario.table_name}" DROP CONSTRAINT block_eager'
            )


def _make_app(
    coco_env: coco.Environment,
    app_name: str,
    writer: Any,
    components: list[str],
    name: str,
) -> "coco.App[..., None]":
    async def app_main() -> None:
        table = await coco.use_mount(
            coco.component_subpath("setup", "table"),
            postgres.declare_table_target,
            _PG_DB_KEY,
            _Scenario.table_name,
            await _entity_schema(),
        )
        with coco.component_subpath("writers"):
            for comp in components:
                await coco.mount(
                    coco.component_subpath(comp), writer, table, comp, name
                )

    return coco.App(coco.AppConfig(name=app_name, environment=coco_env), app_main)


def _unique_table(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


# =============================================================================
# Unified optimistic-write API
# =============================================================================


@pytest.mark.asyncio
async def test_optimistic_row_is_visible_during_processing(pg_env: _PgEnv) -> None:
    """The row is readable through raw SQL before the component returns, and
    normal submit re-applies it."""
    table_name = _unique_table("opt_visible")
    _Scenario.reset(table_name)
    _Scenario.proposals = {"c1": "id-1"}
    try:
        app = _make_app(
            pg_env.coco_env, table_name, _optimistic_writer, ["c1"], "Einstein"
        )
        await app.update()

        assert _Scenario.observed["c1"] == "id-1", (
            "the eager write must be visible to a plain SELECT mid-processing"
        )
        assert await _all_rows(pg_env.pool, table_name) == [
            {"name": "Einstein", "id": "id-1"}
        ]
    finally:
        await _drop_table(pg_env.pool, table_name)


@pytest.mark.asyncio
async def test_caught_eager_error_heals_at_submit(pg_env: _PgEnv) -> None:
    """Scenario 11: the eager write fails, the caller swallows the error, and
    the row still lands through normal submit."""
    table_name = _unique_table("opt_heal")
    _Scenario.reset(table_name)
    _Scenario.proposals = {"c1": "id-1"}
    try:
        app = _make_app(
            pg_env.coco_env,
            table_name,
            _blocked_then_healing_writer,
            ["c1"],
            "Einstein",
        )
        await app.update()

        assert "c1" in _Scenario.caught_eager_error, "eager write should have failed"
        assert await _all_rows(pg_env.pool, table_name) == [
            {"name": "Einstein", "id": "id-1"}
        ], "submit must heal the caught eager failure"
    finally:
        await _drop_table(pg_env.pool, table_name)


@pytest.mark.asyncio
async def test_component_failure_removes_the_eager_row(pg_env: _PgEnv) -> None:
    """Scenarios 4/5: a component that fails after writing eagerly has its row
    deleted before the marker is cleared."""
    table_name = _unique_table("opt_cleanup")
    _Scenario.reset(table_name)
    _Scenario.proposals = {"c1": "id-1"}
    _Scenario.fail_after_write = {"c1"}
    try:
        app = _make_app(
            pg_env.coco_env, table_name, _optimistic_writer, ["c1"], "Einstein"
        )
        # A background-mounted child's failure is logged and swallowed by the
        # default error handler, so the update itself still completes.
        await app.update()

        assert _Scenario.observed["c1"] == "id-1", "row existed during processing"
        assert await _all_rows(pg_env.pool, table_name) == [], (
            "engine cleanup must delete the eagerly-written row"
        )
    finally:
        await _drop_table(pg_env.pool, table_name)


@pytest.mark.asyncio
async def test_failure_before_optimistic_call_writes_nothing(pg_env: _PgEnv) -> None:
    """Scenario 4: no call means no claim, row, or cleanup delete."""
    table_name = _unique_table("opt_fail_before")
    _Scenario.reset(table_name)
    try:
        app = _make_app(
            pg_env.coco_env,
            table_name,
            _fail_before_optimistic_write,
            ["c1"],
            "Einstein",
        )
        await app.update()
        assert await _all_rows(pg_env.pool, table_name) == []
    finally:
        await _drop_table(pg_env.pool, table_name)


@pytest.mark.asyncio
async def test_unchanged_rerun_reuses_the_confirmed_row(pg_env: _PgEnv) -> None:
    """Scenario 2: an unchanged memoized component does no new work."""
    table_name = _unique_table("opt_rerun")
    _Scenario.reset(table_name)
    _Scenario.proposals = {"c1": "id-1"}
    try:
        app = _make_app(
            pg_env.coco_env,
            table_name,
            _memoized_optimistic_writer,
            ["c1"],
            "Einstein",
        )
        await app.update()
        await app.update()

        assert _Scenario.executions == 1
        assert await _all_rows(pg_env.pool, table_name) == [
            {"name": "Einstein", "id": "id-1"}
        ]
    finally:
        await _drop_table(pg_env.pool, table_name)


@pytest.mark.asyncio
async def test_optimistic_write_elects_one_winner(pg_env: _PgEnv) -> None:
    """Two components propose *different* ids for one name at the same moment:
    exactly one wins, one row exists, and the loser observes the winner's."""
    table_name = _unique_table("opt_cas")
    _Scenario.reset(table_name)
    _Scenario.proposals = {"c1": "id-from-c1", "c2": "id-from-c2"}
    _Scenario.barrier = asyncio.Barrier(2)
    try:
        app = _make_app(
            pg_env.coco_env, table_name, _conditional_writer, ["c1", "c2"], "Einstein"
        )
        await app.update()

        assert sorted(_Scenario.claim_results) == ["c1", "c2"]
        winners = [c for c, won in _Scenario.claim_results.items() if won]
        assert len(winners) == 1, f"expected one winner, got {_Scenario.claim_results}"

        rows = await _all_rows(pg_env.pool, table_name)
        assert rows == [{"name": "Einstein", "id": _Scenario.proposals[winners[0]]}]
    finally:
        await _drop_table(pg_env.pool, table_name)


@pytest.mark.asyncio
async def test_changed_rerun_reuses_confirmed_owner(pg_env: _PgEnv) -> None:
    """A row confirmed by an earlier run is found by the SELECT, so the second
    run reuses it instead of claiming a new one."""
    table_name = _unique_table("opt_cas_confirmed")
    _Scenario.reset(table_name)
    _Scenario.proposals = {"c1": "id-first"}
    try:
        app = _make_app(
            pg_env.coco_env, table_name, _conditional_writer, ["c1"], "Einstein"
        )
        await app.update()
        assert _Scenario.claim_results == {"c1": True}

        _Scenario.claim_results = {}
        _Scenario.proposals = {"c1": "id-second"}
        await app.update()
        assert _Scenario.claim_results == {"c1": False}, (
            "an existing row must be reused, not re-claimed"
        )

        assert await _all_rows(pg_env.pool, table_name) == [
            {"name": "Einstein", "id": "id-first"}
        ]
    finally:
        await _drop_table(pg_env.pool, table_name)


@pytest.mark.asyncio
async def test_optimistic_writes_on_independent_keys_both_win(
    pg_env: _PgEnv,
) -> None:
    table_name = _unique_table("opt_cas_indep")
    _Scenario.reset(table_name)
    _Scenario.proposals = {"c1": "id-1", "c2": "id-2"}
    try:

        async def app_main() -> None:
            table = await coco.use_mount(
                coco.component_subpath("setup", "table"),
                postgres.declare_table_target,
                _PG_DB_KEY,
                table_name,
                await _entity_schema(),
            )
            with coco.component_subpath("writers"):
                for comp, name in (("c1", "Einstein"), ("c2", "Curie")):
                    await coco.mount(
                        coco.component_subpath(comp),
                        _conditional_writer,
                        table,
                        comp,
                        name,
                    )

        app = coco.App(
            coco.AppConfig(name=table_name, environment=pg_env.coco_env), app_main
        )
        await app.update()

        assert _Scenario.claim_results == {"c1": True, "c2": True}
        assert await _all_rows(pg_env.pool, table_name) == [
            {"name": "Curie", "id": "id-2"},
            {"name": "Einstein", "id": "id-1"},
        ]
    finally:
        await _drop_table(pg_env.pool, table_name)


@pytest.mark.asyncio
async def test_optimistic_winner_failure_frees_the_key(pg_env: _PgEnv) -> None:
    """A winner whose component then fails releases both its recovery marker
    and its claim, so a later run can win the same key."""
    table_name = _unique_table("opt_cas_retry")
    _Scenario.reset(table_name)
    _Scenario.proposals = {"c1": "id-first"}
    _Scenario.fail_after_write = {"c1"}
    try:
        app = _make_app(
            pg_env.coco_env, table_name, _conditional_writer, ["c1"], "Einstein"
        )
        await app.update()
        assert _Scenario.claim_results == {"c1": True}
        assert await _all_rows(pg_env.pool, table_name) == [], (
            "the winner's row must be cleaned up"
        )

        _Scenario.fail_after_write = set()
        _Scenario.claim_results = {}
        _Scenario.proposals = {"c1": "id-second"}
        await app.update()

        assert _Scenario.claim_results == {"c1": True}, (
            "the key must be claimable again"
        )
        assert await _all_rows(pg_env.pool, table_name) == [
            {"name": "Einstein", "id": "id-second"}
        ]
    finally:
        await _drop_table(pg_env.pool, table_name)
