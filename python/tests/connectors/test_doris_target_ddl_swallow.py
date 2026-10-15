"""Regression tests for the Doris connector's column-DDL error handling.

Unlike test_doris_target.py, these do not need a live Doris cluster: they
call `_apply_column_actions` directly and stub `_execute_ddl_sync`, since the
bug and its fix are both pure control flow (which exceptions from a DDL
statement are swallowed vs. re-raised), not something a live cluster
response is needed to observe.
"""

from __future__ import annotations

import logging

import pytest

try:
    import pymysql  # type: ignore[import-untyped]  # noqa: F401
    import aiohttp  # type: ignore[import-untyped]  # noqa: F401

    DEPS_AVAILABLE = True
except ImportError:
    DEPS_AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not DEPS_AVAILABLE, reason="pymysql/aiohttp not installed"
)

if DEPS_AVAILABLE:
    from cocoindex._internal.context_keys import ContextKey, ContextProvider
    from cocoindex.connectorkits import target as connector_target
    from cocoindex.connectors import doris
    from cocoindex.connectors.doris import _target as doris_target
    from cocoindex.connectors.doris._target import (
        DorisConnectionConfig,
        TableSchema,
        _apply_table_actions,
        _TableAction,
        _TableKey,
        _TableSpec,
    )
    # `_apply_column_actions` / `_is_benign_column_ddl_error` are new symbols
    # this fix introduces; imported lazily inside the tests that need them so
    # the rest of this module (incl. the end-to-end test below, which only
    # names pre-existing symbols) still collects and runs against pre-fix
    # code, and demonstrates the actual swallowed-exception behavior rather
    # than just an ImportError.


def _config() -> "DorisConnectionConfig":
    return doris.DorisConnectionConfig(fe_host="doris-test-host", database="testdb")


def _schema() -> "TableSchema[dict[str, object]]":
    return doris.TableSchema(
        columns={
            "id": doris.ColumnDef(type="VARCHAR(64)", nullable=False),
            "col1": doris.ColumnDef(type="VARCHAR(255)", nullable=True),
        },
        primary_key=["id"],
    )


# ============================================================
# _is_benign_column_ddl_error
# ============================================================


def test_benign_add_column_errors_are_recognized() -> None:
    from cocoindex.connectors.doris._target import _is_benign_column_ddl_error

    assert _is_benign_column_ddl_error(
        Exception("Duplicate column name 'col1'"), expect_exists=True
    )
    assert _is_benign_column_ddl_error(
        Exception("errCode = 2, column col1 already exists"), expect_exists=True
    )


def test_benign_drop_column_errors_are_recognized() -> None:
    from cocoindex.connectors.doris._target import _is_benign_column_ddl_error

    assert _is_benign_column_ddl_error(
        Exception("Unknown column 'col1' in 'field list'"), expect_exists=False
    )
    assert _is_benign_column_ddl_error(
        Exception("errCode = 2, column col1 does not exist"), expect_exists=False
    )


def test_genuine_errors_are_not_benign_in_either_direction() -> None:
    from cocoindex.connectors.doris._target import _is_benign_column_ddl_error

    permission_error = Exception("Access denied for user 'ro'@'%' to database 'testdb'")
    assert not _is_benign_column_ddl_error(permission_error, expect_exists=True)
    assert not _is_benign_column_ddl_error(permission_error, expect_exists=False)

    in_progress = Exception("schema change job on table t already in progress")
    assert not _is_benign_column_ddl_error(in_progress, expect_exists=True)
    assert not _is_benign_column_ddl_error(in_progress, expect_exists=False)


# ============================================================
# _apply_column_actions
# ============================================================


def test_add_column_benign_duplicate_is_swallowed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_execute(config: "DorisConnectionConfig", sql: str) -> None:
        raise Exception("Duplicate column name 'col1'")

    monkeypatch.setattr(
        "cocoindex.connectors.doris._target._execute_ddl_sync", fake_execute
    )
    # Should not raise: an ADD COLUMN that already exists is a benign no-op.
    from cocoindex.connectors.doris._target import _apply_column_actions

    _apply_column_actions(_config(), "t1", _schema(), {"col:col1": "insert"})


def test_drop_column_benign_missing_is_swallowed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_execute(config: "DorisConnectionConfig", sql: str) -> None:
        raise Exception("Unknown column 'col1' in 'field list'")

    monkeypatch.setattr(
        "cocoindex.connectors.doris._target._execute_ddl_sync", fake_execute
    )
    # Should not raise: dropping an already-gone column is a benign no-op.
    from cocoindex.connectors.doris._target import _apply_column_actions

    _apply_column_actions(_config(), "t1", _schema(), {"col:col1": "delete"})


def test_add_column_genuine_failure_propagates_and_is_logged(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    def fake_execute(config: "DorisConnectionConfig", sql: str) -> None:
        raise Exception("Access denied for user 'ro'@'%' to database 'testdb'")

    monkeypatch.setattr(
        "cocoindex.connectors.doris._target._execute_ddl_sync", fake_execute
    )
    from cocoindex.connectors.doris._target import _apply_column_actions

    with caplog.at_level(logging.WARNING, logger="cocoindex.connectors.doris._target"):
        with pytest.raises(Exception, match="Access denied"):
            _apply_column_actions(_config(), "t1", _schema(), {"col:col1": "insert"})
    assert any("Failed to add column col1" in r.message for r in caplog.records)


def test_drop_column_genuine_failure_propagates_and_is_logged(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    def fake_execute(config: "DorisConnectionConfig", sql: str) -> None:
        raise Exception("schema change job on table t1 already in progress")

    monkeypatch.setattr(
        "cocoindex.connectors.doris._target._execute_ddl_sync", fake_execute
    )
    from cocoindex.connectors.doris._target import _apply_column_actions

    with caplog.at_level(logging.WARNING, logger="cocoindex.connectors.doris._target"):
        with pytest.raises(Exception, match="already in progress"):
            _apply_column_actions(_config(), "t1", _schema(), {"col:col1": "delete"})
    assert any("Failed to drop column col1" in r.message for r in caplog.records)


def test_add_column_success_reaches_ddl_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = []

    def fake_execute(config: "DorisConnectionConfig", sql: str) -> None:
        calls.append(sql)

    monkeypatch.setattr(
        "cocoindex.connectors.doris._target._execute_ddl_sync", fake_execute
    )
    from cocoindex.connectors.doris._target import _apply_column_actions

    _apply_column_actions(_config(), "t1", _schema(), {"col:col1": "upsert"})
    assert len(calls) == 1
    assert "ADD COLUMN `col1`" in calls[0]


def test_pk_columns_are_never_altered(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_execute(config: "DorisConnectionConfig", sql: str) -> None:
        raise AssertionError(f"should not run DDL for a PK column: {sql}")

    monkeypatch.setattr(
        "cocoindex.connectors.doris._target._execute_ddl_sync", fake_execute
    )
    from cocoindex.connectors.doris._target import _apply_column_actions

    _apply_column_actions(_config(), "t1", _schema(), {"col:id": "delete"})


# ============================================================
# End-to-end through _apply_table_actions (the real entry point Doris'
# TargetActionSink dispatches into, not just the extracted helper)
# ============================================================


def test_end_to_end_genuine_ddl_failure_propagates_through_apply_table_actions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A genuine ALTER TABLE failure during column reconciliation must
    surface out of `_apply_table_actions`, not be swallowed. On the
    pre-fix code this call returns normally, letting the caller commit a
    tracking record for a schema change that never actually happened.
    """
    spec = _TableSpec(
        table_schema=_schema(), managed_by=connector_target.ManagedBy.SYSTEM
    )
    managed_conn = doris.ManagedConnection(config=_config())

    db_key: ContextKey[doris.ManagedConnection] = ContextKey(
        "test_doris_target_ddl_swallow_e2e_db_key"
    )
    ctx = ContextProvider()
    ctx.provide(db_key, managed_conn)

    table_key = _TableKey(db_key=db_key.key, table_name="t1")
    action = _TableAction(
        key=table_key,
        spec=spec,
        main_action=None,
        column_actions={"col:col1": "insert"},
    )

    def fake_execute(config: "DorisConnectionConfig", sql: str) -> None:
        raise Exception("Access denied for user 'ro'@'%' to database 'testdb'")

    monkeypatch.setattr(doris_target, "_execute_ddl_sync", fake_execute)

    with pytest.raises(Exception, match="Access denied"):
        _apply_table_actions(ctx, [action])
