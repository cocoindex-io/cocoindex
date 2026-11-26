"""Tests for Snowflake target connector."""

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
from cocoindex.connectors import snowflake
from cocoindex.connectors.snowflake import _target

from tests import common

SNOWFLAKE_DB = coco.ContextKey[snowflake.ConnectionConfig]("snowflake_test_db")


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
    external_id: uuid.UUID
    payload: dict[str, object]
    tags: list[str]


@dataclasses.dataclass
class OverrideRow:
    id: Annotated[int, snowflake.SnowflakeType("NUMBER(38, 0)")]
    vector: Annotated[list[float], snowflake.SnowflakeType("ARRAY")]


class FakeCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...] | None]] = []
        self.rows: list[tuple[Any, ...]] = []
        # What `SHOW TERSE TABLES` lists: `None` lists the table asked for.
        self.listed_tables: list[str] | None = None
        self.show_error: Exception | None = None

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> FakeCursor:
        self.calls.append((sql, params))
        if sql.startswith("SHOW TERSE TABLES"):
            if self.show_error is not None:
                raise self.show_error
            names = self.listed_tables
            if names is None:
                names = [sql.split("'")[1]]
            self.rows = [("2026-01-01", name, "TABLE", "DB", "S") for name in names]
        return self

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows

    def close(self) -> None:
        pass


class FakeConnection:
    def __init__(self) -> None:
        self.cursor_obj = FakeCursor()
        self.commit_count = 0
        self.rollback_count = 0
        self.close_count = 0

    def cursor(self) -> FakeCursor:
        return self.cursor_obj

    def commit(self) -> None:
        self.commit_count += 1

    def rollback(self) -> None:
        self.rollback_count += 1

    def close(self) -> None:
        self.close_count += 1


class FakeConnectionContext:
    def __init__(self, conn: FakeConnection) -> None:
        self.conn = conn

    def __enter__(self) -> FakeConnection:
        return self.conn

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object,
    ) -> None:
        self.conn.close()


class FakeContextProvider:
    def __init__(self, config: snowflake.ConnectionConfig) -> None:
        self.config = config

    def get(self, key: str, typ: type[Any]) -> Any:
        assert key == SNOWFLAKE_DB.key
        assert typ is snowflake.ConnectionConfig
        return self.config


def _connection_config() -> snowflake.ConnectionConfig:
    return snowflake.ConnectionConfig(
        account="test_account",
        user="test_user",
        password="test_password",
        warehouse="test_warehouse",
    )


@pytest.mark.asyncio
async def test_table_target_rejects_invalid_table_identifier() -> None:
    schema = await snowflake.TableSchema.from_class(SimpleRow, primary_key=["id"])

    with pytest.raises(ValueError, match="Invalid Snowflake table name"):
        snowflake.table_target(
            SNOWFLAKE_DB,
            table_name="bad-table",
            table_schema=schema,
        )


@pytest.mark.asyncio
async def test_table_target_rejects_invalid_column_identifier() -> None:
    schema = snowflake.TableSchema(
        columns={"bad-column": snowflake.ColumnDef(type="VARCHAR")},
        primary_key=["bad-column"],
    )

    with pytest.raises(ValueError, match="Invalid Snowflake column name"):
        snowflake.table_target(
            SNOWFLAKE_DB,
            table_name="events",
            table_schema=schema,
        )


@pytest.mark.asyncio
async def test_table_schema_maps_python_types_to_snowflake_types() -> None:
    schema = await snowflake.TableSchema.from_class(TypedRow, primary_key=["id"])

    assert schema.columns["id"].type == "NUMBER"
    assert schema.columns["flag"].type == "BOOLEAN"
    assert schema.columns["amount"].type == "NUMBER"
    assert schema.columns["created_on"].type == "DATE"
    assert schema.columns["created_at"].type == "TIMESTAMP_TZ"
    assert schema.columns["external_id"].type == "VARCHAR"
    assert schema.columns["payload"].type == "VARIANT"
    assert schema.columns["payload"].use_parse_json is True
    assert schema.columns["tags"].type == "VARIANT"
    assert schema.columns["tags"].use_parse_json is True


@pytest.mark.asyncio
async def test_snowflake_type_override_is_used() -> None:
    schema = await snowflake.TableSchema.from_class(OverrideRow, primary_key=["id"])

    assert schema.columns["id"].type == "NUMBER(38, 0)"
    assert schema.columns["vector"].type == "ARRAY"
    assert schema.columns["vector"].use_parse_json is False


def test_connection_config_repr_omits_password() -> None:
    config = _connection_config()
    assert "test_password" not in repr(config)
    assert "test_account" in repr(config)


def test_connection_configs_with_different_passwords_are_unequal() -> None:
    config = _connection_config()
    assert config != dataclasses.replace(config, password="other_password")


def test_qualified_table_name_quotes_each_part() -> None:
    assert (
        _target._qualified_table_name("analytics", "public", "events")
        == '"analytics"."public"."events"'
    )
    assert (
        _target._qualified_table_name(None, "public", "events") == '"public"."events"'
    )
    assert _target._qualified_table_name(None, None, "events") == '"events"'


def test_merge_sql_uses_merge_and_parse_json_for_variant() -> None:
    schema = snowflake.TableSchema(
        columns={
            "id": snowflake.ColumnDef(type="NUMBER", nullable=False),
            "payload": snowflake.ColumnDef(
                type="VARIANT", nullable=True, use_parse_json=True
            ),
        },
        primary_key=["id"],
    )

    sql = _target._merge_sql('"analytics"."public"."events"', schema)

    assert 'MERGE INTO "analytics"."public"."events" AS target' in sql
    assert 'SELECT %s AS "id", PARSE_JSON(%s) AS "payload"' in sql
    assert 'ON target."id" = source."id"' in sql
    assert 'WHEN MATCHED THEN UPDATE SET "payload" = source."payload"' in sql
    assert (
        'WHEN NOT MATCHED THEN INSERT ("id", "payload") VALUES '
        '(source."id", source."payload")'
    ) in sql


def test_merge_sql_without_non_primary_key_columns_does_nothing_on_match() -> None:
    schema = snowflake.TableSchema(
        columns={"id": snowflake.ColumnDef(type="NUMBER", nullable=False)},
        primary_key=["id"],
    )

    sql = _target._merge_sql('"events"', schema)

    assert "WHEN MATCHED THEN UPDATE" not in sql
    assert "WHEN NOT MATCHED THEN INSERT" in sql


def test_delete_sql_supports_single_and_composite_primary_keys() -> None:
    single = snowflake.TableSchema(
        columns={"id": snowflake.ColumnDef(type="NUMBER", nullable=False)},
        primary_key=["id"],
    )
    composite = snowflake.TableSchema(
        columns={
            "tenant": snowflake.ColumnDef(type="VARCHAR", nullable=False),
            "id": snowflake.ColumnDef(type="NUMBER", nullable=False),
        },
        primary_key=["tenant", "id"],
    )

    assert (
        _target._delete_sql('"events"', single, row_count=3)
        == 'DELETE FROM "events" WHERE "id" IN (%s, %s, %s)'
    )
    assert (
        _target._delete_sql('"events"', composite, row_count=2)
        == 'DELETE FROM "events" WHERE ("tenant" = %s AND "id" = %s) '
        'OR ("tenant" = %s AND "id" = %s)'
    )


def test_encode_row_serializes_variant_deterministically() -> None:
    schema = snowflake.TableSchema(
        columns={
            "id": snowflake.ColumnDef(type="NUMBER", nullable=False),
            "payload": snowflake.ColumnDef(type="VARIANT", use_parse_json=True),
        },
        primary_key=["id"],
    )

    assert _target._encode_row(schema, {"id": 1, "payload": {"b": 2, "a": [1]}}) == (
        1,
        '{"a":[1],"b":2}',
    )


def test_row_handler_executes_merge_and_delete(monkeypatch: pytest.MonkeyPatch) -> None:
    schema = snowflake.TableSchema(
        columns={
            "id": snowflake.ColumnDef(type="NUMBER", nullable=False),
            "payload": snowflake.ColumnDef(type="VARIANT", use_parse_json=True),
        },
        primary_key=["id"],
    )
    conn = FakeConnection()
    monkeypatch.setattr(_target, "_connect", lambda config: FakeConnectionContext(conn))

    handler = _target._RowHandler(
        db_key=SNOWFLAKE_DB.key,
        database="analytics",
        schema="public",
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

    calls = conn.cursor_obj.calls
    assert len(calls) == 2
    assert calls[0][0].startswith('MERGE INTO "analytics"."public"."events"')
    assert calls[0][1] == (1, '{"b":2}')
    assert calls[1] == (
        'DELETE FROM "analytics"."public"."events" WHERE "id" IN (%s)',
        (2,),
    )
    assert conn.commit_count == 1
    assert conn.rollback_count == 0
    assert conn.close_count == 1


def test_table_handler_creates_database_schema_and_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema = snowflake.TableSchema(
        columns={
            "id": snowflake.ColumnDef(type="NUMBER", nullable=False),
            "payload": snowflake.ColumnDef(type="VARIANT", use_parse_json=True),
        },
        primary_key=["id"],
    )
    conn = FakeConnection()
    monkeypatch.setattr(_target, "_connect", lambda config: FakeConnectionContext(conn))

    action = _target._TableAction(
        key=_target._TableKey(
            db_key=SNOWFLAKE_DB.key,
            database="analytics",
            schema="public",
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
    assert conn.cursor_obj.calls == [
        ('CREATE DATABASE IF NOT EXISTS "analytics"', None),
        ('CREATE SCHEMA IF NOT EXISTS "analytics"."public"', None),
        (
            'CREATE TABLE "analytics"."public"."events" '
            '("id" NUMBER NOT NULL, "payload" VARIANT, PRIMARY KEY ("id"))',
            None,
        ),
    ]
    assert conn.commit_count == 1


def _payload_schema() -> snowflake.TableSchema[Any]:
    return snowflake.TableSchema(
        columns={
            "id": snowflake.ColumnDef(type="NUMBER", nullable=False),
            "payload": snowflake.ColumnDef(type="VARIANT", use_parse_json=True),
        },
        primary_key=["id"],
    )


def _key(database: str | None, schema: str | None) -> _target._TableKey:
    return _target._TableKey(
        db_key=SNOWFLAKE_DB.key, database=database, schema=schema, table_name="events"
    )


def test_show_tables_is_scoped_like_the_qualified_table_name() -> None:
    assert (
        _target._show_tables_sql(_key("analytics", "public"))
        == 'SHOW TERSE TABLES LIKE \'events\' IN SCHEMA "analytics"."public"'
    )
    assert (
        _target._show_tables_sql(_key(None, "public"))
        == "SHOW TERSE TABLES LIKE 'events' IN SCHEMA \"public\""
    )
    assert (
        _target._show_tables_sql(_key(None, None))
        == "SHOW TERSE TABLES LIKE 'events' IN SCHEMA"
    )


def test_table_exists_compares_listed_names_exactly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    errors = pytest.importorskip("snowflake.connector.errors")
    conn = FakeConnection()
    monkeypatch.setattr(_target, "_connect", lambda config: FakeConnectionContext(conn))
    config = _connection_config()
    key = _key("analytics", "public")

    assert _target._table_exists(config, key)
    # `LIKE` ignores case, but the quoted identifier is `events`.
    conn.cursor_obj.listed_tables = ["EVENTS"]
    assert not _target._table_exists(config, key)
    # A dropped database or schema holds no table.
    conn.cursor_obj.show_error = errors.ProgrammingError(
        msg="does not exist or not authorized", errno=2003
    )
    assert not _target._table_exists(config, key)
    # Any other failure is not guessed at.
    conn.cursor_obj.show_error = errors.ProgrammingError(
        msg="no current warehouse", errno=606
    )
    with pytest.raises(errors.ProgrammingError):
        _target._table_exists(config, key)
    conn.cursor_obj.show_error = RuntimeError("connection reset")
    with pytest.raises(RuntimeError):
        _target._table_exists(config, key)
    assert conn.close_count == 5


def test_a_missing_system_table_is_created_and_its_rows_invalidated() -> None:
    present = _target._TableSpec(table_schema=_payload_schema(), missing=False)
    prev = statediff.MutualTrackingRecord(
        tracking_record=_target._table_composite_tracking_record_from_spec(present),
        managed_by=present.managed_by,
    )
    handler = _target._TableHandler()
    key = tuple(_key("analytics", "public"))

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
    row: dict[str, Any], table: snowflake.TableTarget[dict[str, Any]]
) -> None:
    global _row_component_runs
    _row_component_runs += 1
    table.declare_row(row=row)


def test_a_table_dropped_outside_cocoindex_comes_back_with_its_rows(
    monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> None:
    """A table `SHOW TERSE TABLES` no longer lists is created again on the next
    run, with all its rows: the rows of child components that would otherwise
    be skipped as unchanged too."""
    # `_table_exists` tells a missing table by the connector's error type.
    pytest.importorskip("snowflake.connector")
    conn = FakeConnection()
    cursor = conn.cursor_obj
    monkeypatch.setattr(_target, "_connect", lambda config: FakeConnectionContext(conn))
    env = common.create_test_env(__file__, suffix=request.node.name)
    env.context_provider.provide(SNOWFLAKE_DB, _connection_config())
    rows = [{"id": 1, "payload": {"v": 1}}, {"id": 2, "payload": {"v": 2}}]

    @coco.fn
    async def main() -> None:
        table = await snowflake.mount_table_target(
            SNOWFLAKE_DB,
            "events",
            _payload_schema(),
            database="analytics",
            schema="public",
        )
        await coco.mount_each(
            _declare_row_in_component, [(str(row["id"]), row) for row in rows], table
        )

    app = coco.App(coco.AppConfig(name="test_snowflake_dropped", environment=env), main)

    def statements() -> tuple[list[str], int]:
        ddl = [sql for sql, _ in cursor.calls if sql.startswith("CREATE")]
        merged = sum(
            len(params or ()) // 2
            for sql, params in cursor.calls
            if sql.startswith("MERGE")
        )
        return ddl, merged

    created = [
        'CREATE DATABASE IF NOT EXISTS "analytics"',
        'CREATE SCHEMA IF NOT EXISTS "analytics"."public"',
        'CREATE TABLE "analytics"."public"."events" '
        '("id" NUMBER NOT NULL, "payload" VARIANT, PRIMARY KEY ("id"))',
    ]

    app.update_blocking()
    assert statements() == (created, 2)
    assert _row_component_runs == 2

    cursor.calls.clear()
    app.update_blocking()
    assert statements() == ([], 0)
    assert _row_component_runs == 2, "unchanged children are skipped"

    cursor.listed_tables = []
    app.update_blocking()
    assert statements() == (created, 2)
    assert _row_component_runs == 4, "the children run again to write their rows"

    cursor.listed_tables = None
    cursor.calls.clear()
    app.update_blocking()
    assert statements() == ([], 0)
    assert _row_component_runs == 4, "unchanged children are skipped again"


REQUIRED_SNOWFLAKE_ENV = [
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA",
]


def _snowflake_env_available() -> bool:
    return all(os.environ.get(name) for name in REQUIRED_SNOWFLAKE_ENV)


@pytest.mark.skipif(
    not _snowflake_env_available(), reason="Snowflake credentials are not configured"
)
def test_live_snowflake_upsert_and_delete() -> None:
    pytest.importorskip("snowflake.connector")
    table_name = f"COCOINDEX_TEST_{uuid.uuid4().hex.upper()}"
    config = snowflake.ConnectionConfig(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        role=os.environ.get("SNOWFLAKE_ROLE"),
    )
    schema_name = os.environ["SNOWFLAKE_SCHEMA"]
    database = os.environ["SNOWFLAKE_DATABASE"]
    schema = snowflake.TableSchema(
        columns={
            "id": snowflake.ColumnDef(type="NUMBER", nullable=False),
            "payload": snowflake.ColumnDef(type="VARIANT", use_parse_json=True),
        },
        primary_key=["id"],
    )
    ctx = cast(Any, FakeContextProvider(config))
    table_key = _target._TableKey(
        db_key=SNOWFLAKE_DB.key,
        database=database,
        schema=schema_name,
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
            db_key=SNOWFLAKE_DB.key,
            database=database,
            schema=schema_name,
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

        with _target._connect(config) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    f'SELECT "id", "payload":v::string FROM '
                    f"{_target._qualified_table_name(database, schema_name, table_name)}"
                )
                rows = cursor.fetchall()
                assert len(rows) == 1
                assert int(rows[0][0]) == 1
                assert rows[0][1] == "one-updated"
            finally:
                cursor.close()
    finally:
        with _target._connect(config) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    f"DROP TABLE IF EXISTS "
                    f"{_target._qualified_table_name(database, schema_name, table_name)}"
                )
                conn.commit()
            finally:
                cursor.close()
