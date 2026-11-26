"""Tests for BigQuery target connector."""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import os
import uuid
from typing import Annotated, Any, cast

import pytest

import cocoindex as coco
from cocoindex.connectorkits import statediff, target
from cocoindex.connectors import bigquery
from cocoindex.connectors.bigquery import _target

from tests import common

BIGQUERY_DB = coco.ContextKey[bigquery.ConnectionConfig]("bigquery_test_db")


@dataclasses.dataclass
class SimpleRow:
    id: int
    text: str


@dataclasses.dataclass
class TypedRow:
    id: int
    flag: bool
    amount: decimal.Decimal
    created_on: datetime.date
    created_at: datetime.datetime
    elapsed: datetime.timedelta
    external_id: uuid.UUID
    payload: dict[str, object]
    tags: list[str]


@dataclasses.dataclass
class OverrideRow:
    id: Annotated[int, bigquery.BigQueryType("NUMERIC")]
    vector: Annotated[list[float], bigquery.BigQueryType("ARRAY<FLOAT64>")]


class FakeClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[_target.QueryParam, ...]]] = []
        self.close_count = 0
        # `tables.get` finds every table but these.
        self.missing_tables: set[str] = set()
        self.table_gets: list[str] = []

    def get_table(self, table_id: str) -> object:
        from google.api_core.exceptions import NotFound

        self.table_gets.append(table_id)
        if table_id in self.missing_tables:
            raise NotFound(f"Not found: Table {table_id}")  # type: ignore[no-untyped-call]
        return object()

    def close(self) -> None:
        self.close_count += 1


class FakeConnectionContext:
    def __init__(self, client: FakeClient) -> None:
        self.client = client

    def __enter__(self) -> FakeClient:
        return self.client

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object,
    ) -> None:
        self.client.close()


class FakeContextProvider:
    def __init__(self, config: bigquery.ConnectionConfig) -> None:
        self.config = config

    def get(self, key: str, typ: type[Any]) -> Any:
        assert key == BIGQUERY_DB.key
        assert typ is bigquery.ConnectionConfig
        return self.config


def _connection_config() -> bigquery.ConnectionConfig:
    return bigquery.ConnectionConfig(project="test_project", location="US")


class FakeTargetStateProvider:
    memo_key = "fake-bigquery-table"

    def __init__(self) -> None:
        self.key: tuple[Any, ...] | None = None
        self.value: dict[str, Any] | None = None

    def target_state(self, key: tuple[Any, ...], value: dict[str, Any]) -> object:
        self.key = key
        self.value = value
        return object()


@pytest.mark.asyncio
async def test_table_target_rejects_invalid_table_identifier() -> None:
    schema = await bigquery.TableSchema.from_class(SimpleRow, primary_key=["id"])

    with pytest.raises(ValueError, match="Invalid BigQuery table name"):
        bigquery.table_target(
            BIGQUERY_DB,
            table_name="bad-table",
            table_schema=schema,
            dataset="analytics",
        )


@pytest.mark.asyncio
async def test_table_target_rejects_invalid_column_identifier() -> None:
    schema = bigquery.TableSchema(
        columns={"bad-column": bigquery.ColumnDef(type="STRING")},
        primary_key=["bad-column"],
    )

    with pytest.raises(ValueError, match="Invalid BigQuery column name"):
        bigquery.table_target(
            BIGQUERY_DB,
            table_name="events",
            table_schema=schema,
            dataset="analytics",
        )


@pytest.mark.asyncio
async def test_table_schema_maps_python_types_to_bigquery_types() -> None:
    schema = await bigquery.TableSchema.from_class(TypedRow, primary_key=["id"])

    assert schema.columns["id"].type == "INT64"
    assert schema.columns["flag"].type == "BOOL"
    assert schema.columns["amount"].type == "NUMERIC"
    assert schema.columns["created_on"].type == "DATE"
    assert schema.columns["created_at"].type == "TIMESTAMP"
    assert schema.columns["elapsed"].type == "FLOAT64"
    assert schema.columns["external_id"].type == "STRING"
    assert schema.columns["payload"].type == "JSON"
    assert schema.columns["payload"].use_parse_json is True
    assert schema.columns["tags"].type == "JSON"
    assert schema.columns["tags"].use_parse_json is True


@pytest.mark.asyncio
async def test_bigquery_type_override_is_used() -> None:
    schema = await bigquery.TableSchema.from_class(OverrideRow, primary_key=["id"])

    assert schema.columns["id"].type == "NUMERIC"
    assert schema.columns["vector"].type == "ARRAY<FLOAT64>"
    assert schema.columns["vector"].use_parse_json is False


def test_qualified_table_name_quotes_each_part() -> None:
    assert (
        _target._qualified_table_name("demo-project", "analytics", "events")
        == "`demo-project.analytics.events`"
    )
    assert (
        _target._qualified_table_name(None, "analytics", "events")
        == "`analytics.events`"
    )


def test_merge_sql_uses_merge_and_parse_json_for_json_columns() -> None:
    schema = bigquery.TableSchema(
        columns={
            "id": bigquery.ColumnDef(type="INT64", nullable=False),
            "payload": bigquery.ColumnDef(
                type="JSON", nullable=True, use_parse_json=True
            ),
        },
        primary_key=["id"],
    )

    sql = _target._merge_sql("`demo-project.analytics.events`", schema)

    assert "MERGE `demo-project.analytics.events` AS target" in sql
    assert "SELECT @p0 AS `id`, PARSE_JSON(@p1) AS `payload`" in sql
    assert "ON target.`id` = source.`id`" in sql
    assert "WHEN MATCHED THEN UPDATE SET `payload` = source.`payload`" in sql
    assert (
        "WHEN NOT MATCHED THEN INSERT (`id`, `payload`) VALUES "
        "(source.`id`, source.`payload`)"
    ) in sql


def test_merge_sql_without_non_primary_key_columns_does_nothing_on_match() -> None:
    schema = bigquery.TableSchema(
        columns={"id": bigquery.ColumnDef(type="INT64", nullable=False)},
        primary_key=["id"],
    )

    sql = _target._merge_sql("`events`", schema)

    assert "WHEN MATCHED THEN UPDATE" not in sql
    assert "WHEN NOT MATCHED THEN INSERT" in sql


def test_delete_sql_supports_single_and_composite_primary_keys() -> None:
    single = bigquery.TableSchema(
        columns={"id": bigquery.ColumnDef(type="INT64", nullable=False)},
        primary_key=["id"],
    )
    composite = bigquery.TableSchema(
        columns={
            "tenant": bigquery.ColumnDef(type="STRING", nullable=False),
            "id": bigquery.ColumnDef(type="INT64", nullable=False),
        },
        primary_key=["tenant", "id"],
    )

    assert (
        _target._delete_sql("`events`", single, row_count=3)
        == "DELETE FROM `events` WHERE `id` IN (@p0, @p1, @p2)"
    )
    assert (
        _target._delete_sql("`events`", composite, row_count=2)
        == "DELETE FROM `events` WHERE (`tenant` = @p0 AND `id` = @p1) "
        "OR (`tenant` = @p2 AND `id` = @p3)"
    )


def test_encode_row_serializes_json_deterministically() -> None:
    schema = bigquery.TableSchema(
        columns={
            "id": bigquery.ColumnDef(type="INT64", nullable=False),
            "payload": bigquery.ColumnDef(type="JSON", use_parse_json=True),
        },
        primary_key=["id"],
    )

    assert _target._encode_row(schema, {"id": 1, "payload": {"b": 2, "a": [1]}}) == (
        1,
        '{"a":[1],"b":2}',
    )


def test_table_target_tracks_raw_rows_and_encodes_on_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema = bigquery.TableSchema(
        columns={
            "id": bigquery.ColumnDef(type="INT64", nullable=False),
            "elapsed": bigquery.ColumnDef(
                type="FLOAT64", encoder=lambda v: v.total_seconds()
            ),
        },
        primary_key=["id"],
    )
    provider = FakeTargetStateProvider()
    declared: list[object] = []
    monkeypatch.setattr(coco, "declare_target_state", declared.append)

    bigquery.TableTarget(cast(Any, provider), schema).declare_row(
        row={"id": 1, "elapsed": datetime.timedelta(seconds=90)}
    )

    assert provider.key == (1,)
    assert provider.value == {"id": 1, "elapsed": datetime.timedelta(seconds=90)}
    assert len(declared) == 1
    assert _target._encode_row(schema, provider.value) == (1, 90.0)


def test_row_handler_executes_merge_and_delete(monkeypatch: pytest.MonkeyPatch) -> None:
    schema = bigquery.TableSchema(
        columns={
            "id": bigquery.ColumnDef(type="INT64", nullable=False),
            "payload": bigquery.ColumnDef(type="JSON", use_parse_json=True),
        },
        primary_key=["id"],
    )
    client = FakeClient()
    monkeypatch.setattr(
        _target, "_connect", lambda config: FakeConnectionContext(client)
    )
    monkeypatch.setattr(
        _target,
        "_run_query",
        lambda client, sql, params=(): client.calls.append((sql, tuple(params))),
    )

    handler = _target._RowHandler(
        db_key=BIGQUERY_DB.key,
        project="demo-project",
        dataset="analytics",
        table_name="events",
        table_schema=schema,
    )

    handler._apply_actions(
        cast(Any, FakeContextProvider(_connection_config())),
        [
            _target._RowAction(key=(1,), value={"id": 1, "payload": {"b": 2}}),
            _target._RowAction(key=(2,), value=None),
        ],
    )

    assert len(client.calls) == 2
    assert client.calls[0][0].startswith("MERGE `demo-project.analytics.events`")
    assert client.calls[0][1] == (
        _target.QueryParam("p0", "INT64", 1),
        _target.QueryParam("p1", "STRING", '{"b":2}'),
    )
    assert client.calls[1] == (
        "DELETE FROM `demo-project.analytics.events` WHERE `id` IN (@p0)",
        (_target.QueryParam("p0", "INT64", 2),),
    )
    assert client.close_count == 1


def test_table_handler_creates_dataset_and_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema = bigquery.TableSchema(
        columns={
            "id": bigquery.ColumnDef(type="INT64", nullable=False),
            "payload": bigquery.ColumnDef(type="JSON", use_parse_json=True),
        },
        primary_key=["id"],
    )
    client = FakeClient()
    monkeypatch.setattr(
        _target, "_connect", lambda config: FakeConnectionContext(client)
    )
    monkeypatch.setattr(
        _target,
        "_run_query",
        lambda client, sql, params=(): client.calls.append((sql, tuple(params))),
    )

    action = _target._TableAction(
        key=_target._TableKey(
            db_key=BIGQUERY_DB.key,
            project="demo-project",
            dataset="analytics",
            table_name="events",
        ),
        spec=_target._TableSpec(
            table_schema=schema,
            managed_by=target.ManagedBy.SYSTEM,
            missing=False,
        ),
        main_action="insert",
        column_actions={},
    )

    ctx = cast(Any, FakeContextProvider(_connection_config()))
    handler = _target._TableHandler()
    handler._apply_actions(ctx, [action])

    assert isinstance(handler._row_handler(ctx, action), _target._RowHandler)
    assert client.calls == [
        ("CREATE SCHEMA IF NOT EXISTS `demo-project.analytics`", ()),
        (
            "CREATE TABLE `demo-project.analytics.events` "
            "(`id` INT64 NOT NULL, `payload` JSON, "
            "PRIMARY KEY (`id`) NOT ENFORCED)",
            (),
        ),
    ]
    assert client.close_count == 1


def _payload_schema() -> bigquery.TableSchema[Any]:
    return bigquery.TableSchema(
        columns={
            "id": bigquery.ColumnDef(type="INT64", nullable=False),
            "payload": bigquery.ColumnDef(type="JSON", use_parse_json=True),
        },
        primary_key=["id"],
    )


def test_table_exists_gets_the_table_and_takes_not_found_as_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("google.api_core.exceptions")
    client = FakeClient()
    monkeypatch.setattr(
        _target, "_connect", lambda config: FakeConnectionContext(client)
    )
    key = _target._TableKey(
        db_key=BIGQUERY_DB.key, project=None, dataset="analytics", table_name="Events"
    )
    config = _connection_config()

    assert _target._table_exists(config, key)
    client.missing_tables.add("analytics.Events")
    assert not _target._table_exists(config, key)
    assert _target._table_exists(config, key._replace(project="demo-project"))
    # Without a project, the client's own project holds the table; names are
    # exact.
    assert client.table_gets == [
        "analytics.Events",
        "analytics.Events",
        "demo-project.analytics.Events",
    ]
    assert client.close_count == 3


def test_a_missing_system_table_is_created_and_its_rows_invalidated() -> None:
    present = _target._TableSpec(table_schema=_payload_schema(), missing=False)
    prev = statediff.MutualTrackingRecord(
        tracking_record=_target._table_composite_tracking_record_from_spec(present),
        managed_by=present.managed_by,
    )
    handler = _target._TableHandler()
    key = (BIGQUERY_DB.key, None, "analytics", "events")

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
    user_prev = statediff.MutualTrackingRecord(
        tracking_record=prev.tracking_record, managed_by=target.ManagedBy.USER
    )
    out = handler.reconcile(
        key,
        dataclasses.replace(present, managed_by=target.ManagedBy.USER, missing=True),
        [user_prev],
        False,
    )
    assert out is not None
    assert (out.action.main_action, out.child_invalidation) == (None, None)


_row_component_runs = 0


@coco.fn(memo=True)
async def _declare_row_in_component(
    row: dict[str, Any], table: bigquery.TableTarget[dict[str, Any]]
) -> None:
    global _row_component_runs
    _row_component_runs += 1
    table.declare_row(row=row)


def test_a_table_dropped_outside_cocoindex_comes_back_with_its_rows(
    monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> None:
    """A table `tables.get` no longer finds is created again on the next run,
    with all its rows: the rows of child components that would otherwise be
    skipped as unchanged too."""
    pytest.importorskip("google.api_core.exceptions")
    client = FakeClient()
    monkeypatch.setattr(
        _target, "_connect", lambda config: FakeConnectionContext(client)
    )
    monkeypatch.setattr(
        _target,
        "_run_query",
        lambda client, sql, params=(): client.calls.append((sql, tuple(params))),
    )
    env = common.create_test_env(__file__, suffix=request.node.name)
    env.context_provider.provide(BIGQUERY_DB, _connection_config())
    rows = [{"id": 1, "payload": {"v": 1}}, {"id": 2, "payload": {"v": 2}}]

    @coco.fn
    async def main() -> None:
        table = await bigquery.mount_table_target(
            BIGQUERY_DB, "events", _payload_schema(), dataset="analytics"
        )
        await coco.mount_each(
            _declare_row_in_component, [(str(row["id"]), row) for row in rows], table
        )

    app = coco.App(coco.AppConfig(name="test_bigquery_dropped", environment=env), main)

    def statements() -> tuple[list[str], int]:
        ddl = [sql for sql, _ in client.calls if sql.startswith("CREATE")]
        merged = sum(
            len(params) // 2 for sql, params in client.calls if sql.startswith("MERGE")
        )
        return ddl, merged

    created = [
        "CREATE SCHEMA IF NOT EXISTS `analytics`",
        "CREATE TABLE `analytics.events` (`id` INT64 NOT NULL, `payload` JSON, "
        "PRIMARY KEY (`id`) NOT ENFORCED)",
    ]

    app.update_blocking()
    assert statements() == (created, 2)
    assert _row_component_runs == 2

    client.calls.clear()
    app.update_blocking()
    assert statements() == ([], 0)
    assert _row_component_runs == 2, "unchanged children are skipped"

    client.missing_tables.add("analytics.events")
    app.update_blocking()
    assert statements() == (created, 2)
    assert _row_component_runs == 4, "the children run again to write their rows"

    client.missing_tables.clear()
    client.calls.clear()
    app.update_blocking()
    assert statements() == ([], 0)
    assert _row_component_runs == 4, "unchanged children are skipped again"


def _reconcile_change(
    old_columns: dict[str, bigquery.ColumnDef],
    new_columns: dict[str, bigquery.ColumnDef],
) -> Any:
    def spec(columns: dict[str, bigquery.ColumnDef]) -> Any:
        return _target._TableSpec(
            table_schema=bigquery.TableSchema(columns=columns, primary_key=["id"]),
            managed_by=target.ManagedBy.SYSTEM,
            missing=False,
        )

    prev = statediff.MutualTrackingRecord(
        tracking_record=_target._table_composite_tracking_record_from_spec(
            spec(old_columns)
        ),
        managed_by=target.ManagedBy.SYSTEM,
    )
    return _target._TableHandler().reconcile(
        (BIGQUERY_DB.key, "demo-project", "analytics", "events"),
        spec(new_columns),
        [prev],
        False,
    )


def test_required_column_added_or_changed_recreates_the_table() -> None:
    """BigQuery adds no REQUIRED column and makes none REQUIRED: a declared NOT
    NULL column that is added or changed recreates the table (rows written again).
    Before, `ADD COLUMN ... NOT NULL` failed every run."""
    base = {
        "id": bigquery.ColumnDef(type="INT64", nullable=False),
        "a": bigquery.ColumnDef(type="STRING"),
    }
    for new in [
        {**base, "b": bigquery.ColumnDef(type="INT64", nullable=False)},
        {**base, "a": bigquery.ColumnDef(type="STRING", nullable=False)},
    ]:
        output = _reconcile_change(base, new)
        assert output.action.main_action == "replace"
        assert output.action.column_actions == {}
        assert output.child_invalidation == "destructive"

    # A nullable column is still added in place.
    output = _reconcile_change(base, {**base, "b": bigquery.ColumnDef(type="INT64")})
    assert output.action.main_action is None
    assert output.action.column_actions == {"col:b": "insert"}
    assert output.child_invalidation is None


REQUIRED_BIGQUERY_ENV = [
    "BIGQUERY_PROJECT",
    "BIGQUERY_DATASET",
]


def _bigquery_env_available() -> bool:
    return all(os.environ.get(name) for name in REQUIRED_BIGQUERY_ENV)


@pytest.mark.skipif(
    not _bigquery_env_available(), reason="BigQuery credentials are not configured"
)
def test_live_bigquery_upsert_and_delete() -> None:
    pytest.importorskip("google.cloud.bigquery")
    table_name = f"cocoindex_test_{uuid.uuid4().hex}"
    config = bigquery.ConnectionConfig(
        project=os.environ["BIGQUERY_PROJECT"],
        location=os.environ.get("BIGQUERY_LOCATION"),
    )
    dataset = os.environ["BIGQUERY_DATASET"]
    schema = bigquery.TableSchema(
        columns={
            "id": bigquery.ColumnDef(type="INT64", nullable=False),
            "payload": bigquery.ColumnDef(type="JSON", use_parse_json=True),
        },
        primary_key=["id"],
    )
    ctx = cast(Any, FakeContextProvider(config))
    table_key = _target._TableKey(
        db_key=BIGQUERY_DB.key,
        project=os.environ["BIGQUERY_PROJECT"],
        dataset=dataset,
        table_name=table_name,
    )

    try:
        _target._TableHandler()._apply_actions(
            ctx,
            [
                _target._TableAction(
                    key=table_key,
                    spec=_target._TableSpec(
                        schema, target.ManagedBy.SYSTEM, missing=False
                    ),
                    main_action="insert",
                    column_actions={},
                )
            ],
        )
        _target._RowHandler(
            db_key=BIGQUERY_DB.key,
            project=os.environ["BIGQUERY_PROJECT"],
            dataset=dataset,
            table_name=table_name,
            table_schema=schema,
        )._apply_actions(
            ctx,
            [
                _target._RowAction(key=(1,), value={"id": 1, "payload": {"v": "one"}}),
                _target._RowAction(key=(2,), value={"id": 2, "payload": {"v": "two"}}),
                _target._RowAction(
                    key=(1,), value={"id": 1, "payload": {"v": "one-updated"}}
                ),
                _target._RowAction(key=(2,), value=None),
            ],
        )

        with _target._connect(config) as client:
            rows = list(
                _target._run_query(
                    client,
                    f"SELECT `id`, JSON_VALUE(`payload`, '$.v') AS v FROM "
                    f"{_target._qualified_table_name(os.environ['BIGQUERY_PROJECT'], dataset, table_name)}",
                )
            )
            assert len(rows) == 1
            assert int(rows[0]["id"]) == 1
            assert rows[0]["v"] == "one-updated"
    finally:
        with _target._connect(config) as client:
            _target._run_query(
                client,
                f"DROP TABLE IF EXISTS "
                f"{_target._qualified_table_name(os.environ['BIGQUERY_PROJECT'], dataset, table_name)}",
            )


def test_required_column_of_a_failed_run_still_recreates_the_table() -> None:
    """A run that added a REQUIRED column and failed leaves two previous records,
    only one with the column, so the column plans as "upsert". The table may still
    lack it, so the next run recreates the table instead of an `ADD COLUMN` that
    BigQuery refuses. A full reprocess, where every previous record has the
    column, does not recreate it."""
    base = {
        "id": bigquery.ColumnDef(type="STRING", nullable=False),
        "a": bigquery.ColumnDef(type="STRING"),
    }
    with_b = {**base, "b": bigquery.ColumnDef(type="INT64", nullable=False)}

    def record(columns: dict[str, bigquery.ColumnDef]) -> Any:
        return statediff.MutualTrackingRecord(
            tracking_record=_target._table_composite_tracking_record_from_spec(
                _target._TableSpec(
                    table_schema=bigquery.TableSchema(
                        columns=columns, primary_key=["id"]
                    ),
                    managed_by=target.ManagedBy.SYSTEM,
                    missing=False,
                )
            ),
            managed_by=target.ManagedBy.SYSTEM,
        )

    def reconcile(prev: list[Any], prev_may_be_missing: bool) -> Any:
        return _target._TableHandler().reconcile(
            (BIGQUERY_DB.key, "demo-project", "analytics", "events"),
            _target._TableSpec(
                table_schema=bigquery.TableSchema(columns=with_b, primary_key=["id"]),
                managed_by=target.ManagedBy.SYSTEM,
                missing=False,
            ),
            prev,
            prev_may_be_missing,
        )

    after_failed_run = reconcile([record(base), record(with_b)], True)
    assert after_failed_run is not None
    assert after_failed_run.action.main_action == "replace"
    assert after_failed_run.child_invalidation == "destructive"

    full_reprocess = reconcile([record(with_b)], True)
    assert full_reprocess is not None
    assert full_reprocess.action.main_action != "replace"
    assert full_reprocess.child_invalidation is None
