"""Doris connector tests that need no live Doris cluster.

They stub the connector's SQL execution (`_execute_ddl_sync`) and Stream Load,
and check the SQL it sends.
"""

from __future__ import annotations

import asyncio
import dataclasses
from typing import Any

import msgspec
import pytest

import cocoindex as coco

from tests import common

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
    from cocoindex._internal import serde
    from cocoindex.connectors import doris
    from cocoindex.connectors.doris import _target as doris_target


def _config() -> "doris.DorisConnectionConfig":
    return doris.DorisConnectionConfig(fe_host="doris-test-host", database="testdb")


@pytest.fixture
def sql_log(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Every SQL statement the connector runs, in order; none reaches a server."""
    log: list[str] = []

    def fake_execute(config: "doris.DorisConnectionConfig", sql: str) -> None:
        log.append(sql)

    async def fake_stream_load(*args: Any, **kwargs: Any) -> dict[str, Any]:
        return {}

    monkeypatch.setattr(doris_target, "_execute_ddl_sync", fake_execute)
    monkeypatch.setattr(doris_target, "_stream_load", fake_stream_load)
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
