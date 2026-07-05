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
from cocoindex.connectorkits import target
from cocoindex.connectors import bigquery
from cocoindex.connectors.bigquery import _target

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
        ),
        main_action="insert",
        column_actions={},
    )

    children = _target._TableHandler()._apply_actions(
        cast(Any, FakeContextProvider(_connection_config())), [action]
    )

    assert len(children) == 1
    assert children[0] is not None
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
                    spec=_target._TableSpec(schema, target.ManagedBy.SYSTEM),
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
