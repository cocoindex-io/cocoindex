"""Doris connector tests that need no live Doris cluster.

They stub the connector's SQL execution (`_execute_ddl_sync`) and Stream Load,
and check the SQL it sends.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
import threading
from typing import Any

import msgspec
import pytest

import cocoindex as coco
from cocoindex.connectorkits import statediff, target

from tests import common

try:
    import pymysql  # type: ignore[import-untyped]  # noqa: F401
    import aiohttp
    import aiohttp.web

    DEPS_AVAILABLE = True
except ImportError:
    DEPS_AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not DEPS_AVAILABLE, reason="pymysql/aiohttp not installed"
)

if DEPS_AVAILABLE:
    from cocoindex._internal import serde
    from cocoindex.connectors import doris
    from cocoindex.connectors.doris import _target as doris_target


def _config() -> "doris.DorisConnectionConfig":
    return doris.DorisConnectionConfig(fe_host="doris-test-host", database="testdb")


@pytest.fixture
def dropped_tables(monkeypatch: pytest.MonkeyPatch) -> set[str]:
    """Tables the stubbed catalog does not list; it lists every other one."""
    dropped: set[str] = set()

    def fake_query(
        config: "doris.DorisConnectionConfig", sql: str, args: Any = ()
    ) -> list[tuple[Any, ...]]:
        assert sql == doris_target._TABLE_EXISTS_SQL
        _, table_name, _ = args
        return [] if table_name in dropped else [(1,)]

    monkeypatch.setattr(doris_target, "_query_sync", fake_query)
    return dropped


@pytest.fixture
def sql_log(monkeypatch: pytest.MonkeyPatch, dropped_tables: set[str]) -> list[str]:
    """Every SQL statement the connector runs, in order; none reaches a server."""
    log: list[str] = []

    def fake_execute(config: "doris.DorisConnectionConfig", sql: str) -> None:
        log.append(sql)

    async def fake_stream_load(*args: Any, **kwargs: Any) -> Any:
        return doris_target._PrecommittedLoad(txn_id=1, label="l")

    async def fake_finish(*args: Any, **kwargs: Any) -> None:
        pass

    monkeypatch.setattr(doris_target, "_execute_ddl_sync", fake_execute)
    monkeypatch.setattr(doris_target, "_stream_load", fake_stream_load)
    monkeypatch.setattr(doris_target, "_finish_two_phase", fake_finish)
    monkeypatch.setattr(doris_target, "_wait_until_visible", fake_finish)
    return log


# ============================================================
# Delete SQL escaping
# ============================================================


def test_delete_escapes_backslash_and_quote(sql_log: list[str]) -> None:
    """Doris string literals follow MySQL rules: `\\` is an escape character.
    Escaping only `'` made `C:\\temp\\a` match nothing, and a key ending in
    `\\` escaped the closing quote."""
    keys = ["C:\\temp\\a", "ends\\", "it's", "\\'"]
    asyncio.run(
        doris_target._execute_delete(_config(), "t1", ["id"], [{"id": k} for k in keys])
    )
    assert sql_log == [
        "DELETE FROM `testdb`.`t1` WHERE "
        "(`id` = 'C:\\\\temp\\\\a') OR (`id` = 'ends\\\\') OR "
        "(`id` = 'it\\'s') OR (`id` = '\\\\\\'')"
    ]


# ============================================================
# Index tracking
# ============================================================


@dataclasses.dataclass
class _IndexSpec:
    index_type: str = "HNSW"
    metric_type: str = "l2_distance"
    max_degree: int | None = None
    ef_construction: int | None = None
    nlist: int | None = None
    parser: str | None = "unicode"
    dimension: int = 4


def _index_app(env: Any, spec: _IndexSpec) -> coco.App[[], None]:
    db_key = coco.ContextKey[doris.ManagedConnection]("test_doris_offline_db")
    env.context_provider.provide(db_key, doris.connect(_config()))

    @coco.fn
    async def main() -> None:
        schema = doris.TableSchema(
            columns={
                "id": doris.ColumnDef(type="VARCHAR(64)", nullable=False),
                "text": doris.ColumnDef(type="TEXT"),
                "embedding": doris.ColumnDef(
                    type="ARRAY<FLOAT>",
                    nullable=False,
                    is_vector=True,
                    vector_dimension=spec.dimension,
                ),
            },
            primary_key=["id"],
        )
        await doris.mount_table_target(
            db_key,
            "docs",
            schema,
            vector_indexes=[
                doris.VectorIndexDef(
                    field_name="embedding",
                    index_type=spec.index_type,
                    metric_type=spec.metric_type,
                    max_degree=spec.max_degree,
                    ef_construction=spec.ef_construction,
                    nlist=spec.nlist,
                )
            ],
            inverted_indexes=[
                doris.InvertedIndexDef(field_name="text", parser=spec.parser)
            ],
        )

    return coco.App(coco.AppConfig(name="test_doris_offline", environment=env), main)


def _strip_index_params(
    record: Any,
) -> Any:
    """The tracking record as the connector wrote it before index parameters
    were tracked: only the indexed field names."""
    return msgspec.structs.replace(
        record,
        main=msgspec.structs.replace(
            record.main, vector_index_params=None, inverted_index_parsers=None
        ),
    )


def test_index_param_changes_rebuild_the_table_but_upgrade_does_not(
    sql_log: list[str], request: pytest.FixtureRequest
) -> None:
    """Index parameters are tracked: changing one rebuilds the table, as
    changing an indexed field does. A record from before they were tracked
    takes them from the desired spec, so the upgrade alone rebuilds nothing."""
    env = common.create_test_env(__file__, suffix=request.node.name)
    spec = _IndexSpec()
    app = _index_app(env, spec)

    # Run 1, before index parameters were tracked.
    record_from_spec = doris_target._table_composite_tracking_record_from_spec
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            doris_target,
            "_table_composite_tracking_record_from_spec",
            lambda s: _strip_index_params(record_from_spec(s)),
        )
        app.update_blocking()
    assert [sql.split("(")[0].strip() for sql in sql_log] == [
        "CREATE TABLE IF NOT EXISTS `testdb`.`docs`"
    ]

    # Run 2, after the upgrade, nothing changed: the table is kept.
    sql_log.clear()
    app.update_blocking()
    assert sql_log == []

    # Spelling an index type or metric in another case builds the same index.
    spec.index_type, spec.metric_type = "hnsw", "L2_DISTANCE"
    app.update_blocking()
    assert sql_log == []

    # Each real index change drops and rebuilds the table with it.
    changes: list[tuple[str, Any, str]] = [
        ("metric_type", "cosine_distance", '"metric_type" = "cosine_distance"'),
        ("index_type", "IVF", '"index_type" = "ivf"'),
        ("nlist", 128, '"nlist" = "128"'),
        ("max_degree", 32, '"max_degree" = "32"'),
        ("ef_construction", 64, '"ef_construction" = "64"'),
        ("dimension", 8, '"dim" = "8"'),
        ("parser", "english", '"parser" = "english"'),
    ]
    for field, value, expected_ddl in changes:
        setattr(spec, field, value)
        sql_log.clear()
        app.update_blocking()
        assert len(sql_log) == 2, field
        assert sql_log[0] == "DROP TABLE IF EXISTS `testdb`.`docs`", field
        assert expected_ddl in sql_log[1], field


def test_old_tracking_record_decodes_with_no_index_params() -> None:
    """A record written before index parameters were tracked has three fields;
    it must still decode, with the new trailing fields None."""

    class OldRecord(msgspec.Struct, frozen=True, array_like=True):
        primary_key_columns: tuple[doris_target._PkColumnInfo, ...]
        vector_indexes: tuple[str, ...] | None = None
        inverted_indexes: tuple[str, ...] | None = None

    old = OldRecord(
        primary_key_columns=(doris_target._PkColumnInfo(name="id", type="TEXT"),),
        vector_indexes=("embedding",),
        inverted_indexes=("text",),
    )
    decoded = msgspec.msgpack.Decoder(
        type=doris_target._TablePrimaryTrackingRecord,
        ext_hook=serde._ext_hook,
        dec_hook=serde._dec_hook,
    ).decode(serde._msgspec_encoder.encode(old))
    assert decoded == doris_target._TablePrimaryTrackingRecord(
        primary_key_columns=old.primary_key_columns,
        vector_indexes=("embedding",),
        inverted_indexes=("text",),
    )
    assert decoded.vector_index_params is None
    assert decoded.inverted_index_parsers is None


# ============================================================
# A table dropped outside CocoIndex
# ============================================================


def test_table_exists_matches_the_name_and_its_lower_case_form(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The catalog query names the configured database and the table, also
    lower-cased (a cluster with `lower_case_table_names = 1` stores it so)."""
    calls: list[tuple[str, Any]] = []
    found: list[tuple[Any, ...]] = []

    def fake_query(
        config: "doris.DorisConnectionConfig", sql: str, args: Any = ()
    ) -> list[tuple[Any, ...]]:
        calls.append((sql, args))
        return found

    monkeypatch.setattr(doris_target, "_query_sync", fake_query)
    assert not asyncio.run(doris_target._table_exists(_config(), "MyDocs"))
    found.append((1,))
    assert asyncio.run(doris_target._table_exists(_config(), "MyDocs"))
    assert calls[0] == (
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name IN (%s, %s)",
        ("testdb", "MyDocs", "mydocs"),
    )


def _simple_schema() -> "doris.TableSchema[Any]":
    return doris.TableSchema(
        columns={
            "id": doris.ColumnDef(type="VARCHAR(64)", nullable=False),
            "text": doris.ColumnDef(type="TEXT"),
        },
        primary_key=["id"],
    )


def test_a_missing_system_table_is_created_and_its_rows_invalidated() -> None:
    present = doris_target._TableSpec(table_schema=_simple_schema(), missing=False)
    prev = statediff.MutualTrackingRecord(
        tracking_record=doris_target._table_composite_tracking_record_from_spec(
            present
        ),
        managed_by=present.managed_by,
    )
    handler = doris_target._TableHandler()
    key = ("db", "docs")

    out = handler.reconcile(key, present, [prev], False)
    assert out is not None
    assert (out.action.main_action, out.child_invalidation) == (None, None)

    out = handler.reconcile(
        key, dataclasses.replace(present, missing=True), [prev], False
    )
    assert out is not None
    assert out.action.main_action == "insert"
    assert out.action.column_actions == {}
    assert out.child_invalidation == "destructive"
    # `missing` is not part of the tracking record.
    assert out.tracking_record == prev

    # A user-managed table is never created: `missing` changes nothing.
    user = dataclasses.replace(present, managed_by=target.ManagedBy.USER)
    user_prev = statediff.MutualTrackingRecord(
        tracking_record=prev.tracking_record, managed_by=target.ManagedBy.USER
    )
    out = handler.reconcile(
        key, dataclasses.replace(user, missing=True), [user_prev], False
    )
    assert out is not None
    assert (out.action.main_action, out.child_invalidation) == (None, None)


_row_component_runs = 0


@coco.fn(memo=True)
async def _declare_row_in_component(
    row: dict[str, Any], table: "doris.DorisTableTarget[dict[str, Any]]"
) -> None:
    global _row_component_runs
    _row_component_runs += 1
    table.declare_row(row=row)


def test_a_table_dropped_outside_cocoindex_comes_back_with_its_rows(
    sql_log: list[str],
    dropped_tables: set[str],
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    """A table the catalog no longer lists is created again on the next run,
    with all its rows: the rows of child components that would otherwise be
    skipped as unchanged too."""
    loaded: list[str] = []

    async def fake_stream_load(
        managed_conn: Any, table_name: str, rows: list[dict[str, Any]]
    ) -> Any:
        loaded.extend(row["id"] for row in rows)
        return doris_target._PrecommittedLoad(txn_id=1, label="l")

    monkeypatch.setattr(doris_target, "_stream_load", fake_stream_load)
    env = common.create_test_env(__file__, suffix=request.node.name)
    db_key = coco.ContextKey[doris.ManagedConnection]("test_doris_dropped_db")
    env.context_provider.provide(db_key, doris.connect(_config()))
    rows = [{"id": "1", "text": "one"}, {"id": "2", "text": "two"}]

    @coco.fn
    async def main() -> None:
        table = await doris.mount_table_target(db_key, "docs", _simple_schema())
        await coco.mount_each(
            _declare_row_in_component, [(row["id"], row) for row in rows], table
        )

    app = coco.App(coco.AppConfig(name="test_doris_dropped", environment=env), main)

    def creates() -> list[str]:
        return [
            sql
            for sql in sql_log
            if sql.startswith("CREATE TABLE IF NOT EXISTS `testdb`.`docs`")
        ]

    app.update_blocking()
    assert len(creates()) == 1
    assert sorted(loaded) == ["1", "2"]
    assert _row_component_runs == 2

    sql_log.clear()
    loaded.clear()
    app.update_blocking()
    assert (sql_log, loaded, _row_component_runs) == ([], [], 2)

    dropped_tables.add("docs")
    app.update_blocking()
    assert len(creates()) == 1
    assert sorted(loaded) == ["1", "2"]
    assert _row_component_runs == 4, "the children run again to write their rows"

    dropped_tables.clear()
    sql_log.clear()
    loaded.clear()
    app.update_blocking()
    assert (sql_log, loaded, _row_component_runs) == ([], [], 4)


# ============================================================
# Two-phase upsert, against a fake FE over HTTP
# ============================================================


class _FakeDoris:
    """A fake Doris FE serving Stream Load, `_stream_load_2pc` and
    `get_load_state` over HTTP on 127.0.0.1, with the SQL the connector runs
    stubbed. Every step lands in `events`, in order."""

    def __init__(self) -> None:
        self.events: list[tuple[Any, ...]] = []
        self.fail_load = False
        self.fail_delete = False
        # The states `get_load_state` answers for a committed load, in order;
        # the last one repeats.
        self.states_after_commit = ["VISIBLE"]
        self._next_txn_id = 100
        self._txn_of_label: dict[str, int] = {}
        self._polls: dict[int, int] = {}
        self.port = 0

    async def _stream_load(self, request: "aiohttp.web.Request") -> Any:
        rows = json.loads(await request.text())
        self.events.append(
            (
                "load",
                request.headers.get("two_phase_commit"),
                tuple(row["id"] for row in rows),
            )
        )
        if self.fail_load:
            return aiohttp.web.json_response(
                {"Status": "Fail", "Message": "too many filtered rows"}
            )
        self._next_txn_id += 1
        self._txn_of_label[request.headers["label"]] = self._next_txn_id
        return aiohttp.web.json_response(
            {"Status": "Success", "TxnId": self._next_txn_id}
        )

    async def _fe_two_phase(self, request: "aiohttp.web.Request") -> Any:
        # The FE redirects `_stream_load_2pc` to a BE, as Doris does.
        raise aiohttp.web.HTTPTemporaryRedirect(
            f"http://127.0.0.1:{self.port}/be{request.path}"
        )

    async def _be_two_phase(self, request: "aiohttp.web.Request") -> Any:
        txn_id = int(request.headers["txn_id"])
        operation = request.headers["txn_operation"]
        self.events.append((operation, txn_id))
        return aiohttp.web.json_response(
            {"status": "Success", "msg": f"transaction [{txn_id}] {operation}"}
        )

    def execute_sql(self, config: Any, sql: str) -> None:
        if sql.startswith("DELETE"):
            self.events.append(("delete", sql))
            if self.fail_delete:
                raise RuntimeError("delete failed")

    async def _load_state(self, request: "aiohttp.web.Request") -> Any:
        txn_id = self._txn_of_label[request.query["label"]]
        poll = self._polls.get(txn_id, 0)
        self._polls[txn_id] = poll + 1
        state = self.states_after_commit[min(poll, len(self.states_after_commit) - 1)]
        self.events.append(("state", txn_id, state))
        return aiohttp.web.json_response(
            {"msg": "success", "code": 0, "data": state, "count": 0}
        )

    def kinds(self) -> list[str]:
        return [event[0] for event in self.events]

    def serve(self) -> Any:
        """Serve on a thread of its own until the returned callable stops it."""
        started = threading.Event()
        loop = asyncio.new_event_loop()
        app = aiohttp.web.Application()
        app.router.add_put("/api/testdb/{table}/_stream_load", self._stream_load)
        app.router.add_put("/api/testdb/_stream_load_2pc", self._fe_two_phase)
        app.router.add_put("/be/api/testdb/_stream_load_2pc", self._be_two_phase)
        app.router.add_get("/api/testdb/get_load_state", self._load_state)
        runner = aiohttp.web.AppRunner(app)

        async def start() -> None:
            await runner.setup()
            site = aiohttp.web.TCPSite(runner, "127.0.0.1", 0)
            await site.start()
            self.port = runner.addresses[0][1]
            started.set()

        def run() -> None:
            loop.run_until_complete(start())
            loop.run_forever()
            loop.run_until_complete(runner.cleanup())
            loop.close()

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        started.wait()

        def stop() -> None:
            loop.call_soon_threadsafe(loop.stop)
            thread.join()

        return stop


@pytest.fixture
def fake_doris(monkeypatch: pytest.MonkeyPatch, dropped_tables: set[str]) -> Any:
    fake = _FakeDoris()
    stop = fake.serve()
    monkeypatch.setattr(doris_target, "_execute_ddl_sync", fake.execute_sql)
    yield fake
    stop()


_UPSERT_DB_KEY = coco.ContextKey["doris.ManagedConnection"]("test_doris_2pc_db")


def _upsert_app(
    fake: _FakeDoris,
    request: pytest.FixtureRequest,
    rows: list[dict[str, Any]],
    *,
    be_load_host: str | None = None,
) -> coco.App[[], None]:
    """An app that declares `rows` (read at each run) on table `docs`."""
    env = common.create_test_env(__file__, suffix=request.node.name)
    db_key = _UPSERT_DB_KEY
    config = doris.DorisConnectionConfig(
        fe_host="127.0.0.1",
        database="testdb",
        fe_http_port=fake.port,
        be_load_host=be_load_host,
    )
    env.context_provider.provide(db_key, doris.connect(config))

    @coco.fn
    async def main() -> None:
        table = await doris.mount_table_target(db_key, "docs", _simple_schema())
        for row in rows:
            table.declare_row(row=row)

    return coco.App(coco.AppConfig(name="test_doris_2pc", environment=env), main)


def _delete_of(*ids: str) -> tuple[str, str]:
    conditions = " OR ".join(f"(`id` = '{i}')" for i in ids)
    return ("delete", f"DELETE FROM `testdb`.`docs` WHERE {conditions}")


@pytest.mark.parametrize("be_load_host", [None, "127.0.0.1"])
def test_an_upsert_loads_then_deletes_then_commits(
    fake_doris: _FakeDoris, request: pytest.FixtureRequest, be_load_host: str | None
) -> None:
    """Each chunk is Stream Loaded with two-phase commit, then the old rows of
    its keys are deleted, then the load is committed through the FE (which
    redirects to a BE, followed by hand to `be_load_host` when it is set), and
    the run waits until the transaction is visible. The old code deleted first
    and loaded in one phase."""
    rows = [{"id": "1", "text": "one"}, {"id": "2", "text": "two"}]
    app = _upsert_app(fake_doris, request, rows, be_load_host=be_load_host)
    app.update_blocking()
    assert fake_doris.events == [
        ("load", "true", ("1", "2")),
        _delete_of("1", "2"),
        ("commit", 101),
        ("state", 101, "VISIBLE"),
    ]

    fake_doris.events.clear()
    rows[0] = {"id": "1", "text": "uno"}
    app.update_blocking()
    assert fake_doris.events == [
        ("load", "true", ("1",)),
        _delete_of("1"),
        ("commit", 102),
        ("state", 102, "VISIBLE"),
    ]


def test_a_failed_load_deletes_nothing(
    fake_doris: _FakeDoris, request: pytest.FixtureRequest
) -> None:
    """A load Doris rejects leaves the old rows in place: nothing is deleted
    and no transaction is committed. The old code deleted the old row before
    the load, so it stayed gone until a later run."""
    rows = [{"id": "1", "text": "one"}]
    app = _upsert_app(fake_doris, request, rows)
    app.update_blocking()

    fake_doris.events.clear()
    fake_doris.fail_load = True
    rows[0] = {"id": "1", "text": "uno"}
    with pytest.raises(
        doris_target.DorisStreamLoadError, match="too many filtered rows"
    ):
        app.update_blocking()
    assert fake_doris.kinds() == ["load"]

    # The next run loads the row again, as an unchanged desired state that was
    # never written.
    fake_doris.events.clear()
    fake_doris.fail_load = False
    app.update_blocking()
    assert fake_doris.kinds() == ["load", "delete", "commit", "state"]


def test_a_failed_delete_aborts_the_load(
    fake_doris: _FakeDoris, request: pytest.FixtureRequest
) -> None:
    """A delete that fails after the load is staged aborts that load, so the
    new rows never land next to the old ones."""
    rows = [{"id": "1", "text": "one"}]
    app = _upsert_app(fake_doris, request, rows)
    app.update_blocking()

    fake_doris.events.clear()
    fake_doris.fail_delete = True
    rows[0] = {"id": "1", "text": "uno"}
    with pytest.raises(RuntimeError, match="delete failed"):
        app.update_blocking()
    assert fake_doris.events == [
        ("load", "true", ("1",)),
        _delete_of("1"),
        ("abort", 102),
    ]


def test_a_commit_waits_until_the_transaction_is_visible(
    fake_doris: _FakeDoris, request: pytest.FixtureRequest
) -> None:
    """A commit returns before the transaction is published: the run polls
    `get_load_state` until it is VISIBLE, as a one-phase load waits before
    it answers."""
    fake_doris.states_after_commit = ["COMMITTED", "COMMITTED", "VISIBLE"]
    app = _upsert_app(fake_doris, request, [{"id": "1", "text": "one"}])
    app.update_blocking()
    assert fake_doris.events[2:] == [
        ("commit", 101),
        ("state", 101, "COMMITTED"),
        ("state", 101, "COMMITTED"),
        ("state", 101, "VISIBLE"),
    ]


def test_a_transaction_unpublished_past_the_load_timeout_warns(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A committed transaction still unpublished after `stream_load_timeout`
    is left to publish later, with a warning, as a one-phase load's `Publish
    Timeout` is; one Doris reports aborted fails the run."""
    state = "COMMITTED"

    async def fake_load_state(config: Any, label: str) -> str:
        assert label == "l7"
        return state

    monkeypatch.setattr(doris_target, "_load_state", fake_load_state)
    config = dataclasses.replace(_config(), stream_load_timeout=0)
    load = doris_target._PrecommittedLoad(txn_id=7, label="l7")
    with caplog.at_level(logging.WARNING, logger=doris_target._logger.name):
        asyncio.run(doris_target._wait_until_visible(config, load))
    assert any(
        "transaction 7 is committed but not yet published" in r.getMessage()
        for r in caplog.records
    )

    state = "ABORTED"
    with pytest.raises(
        doris_target.DorisStreamLoadError, match="aborted after its commit"
    ):
        asyncio.run(doris_target._wait_until_visible(config, load))
