"""Vector-index batch transaction boundaries, without a PostgreSQL server."""

from __future__ import annotations

from unittest.mock import create_autospec

import asyncpg
import pytest

from cocoindex.connectors.postgres import _target as pg_target
from tests import common

coco_env = common.create_test_env(__file__)


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_at", [None, 1, 2, 3])
async def test_vector_index_batch_commits_or_rolls_back_before_releasing_connection(
    fail_at: int | None,
) -> None:
    pool = create_autospec(asyncpg.Pool, instance=True)
    conn = create_autospec(asyncpg.Connection, instance=True)
    pool.acquire.return_value.__aenter__.return_value = conn
    events: list[str] = []
    statements: list[str] = []
    failure = RuntimeError("index DDL failed")

    async def begin() -> None:
        events.append("begin")

    async def finish(exc_type: object, exc: object, tb: object) -> bool:
        events.append("rollback" if exc is not None else "commit")
        return False

    async def release(*exc: object) -> None:
        events.append("release")

    async def execute(sql: str) -> None:
        statements.append(sql)
        events.append("sql")
        if len(statements) == fail_at:
            raise failure

    transaction = conn.transaction.return_value
    transaction.__aenter__.side_effect = begin
    transaction.__aexit__.side_effect = finish
    pool.acquire.return_value.__aexit__.side_effect = release
    conn.execute.side_effect = execute
    handler = pg_target._VectorIndexHandler(pool, "documents", "tenant")
    spec = pg_target._VectorIndexSpec(
        column="embedding",
        metric="cosine",
        op_class="vector_cosine_ops",
        method="hnsw",
        lists=None,
        m=16,
        ef_construction=None,
    )
    # Deliberately put the deletion last: the handler must still apply it
    # first, inside the same transaction as the replacement.
    actions = [
        pg_target._VectorIndexAction("replacement", spec),
        pg_target._VectorIndexAction("old", None),
    ]
    if fail_at is None:
        await handler._apply_actions(coco_env.context_provider, actions)
        assert events == ["begin", "sql", "sql", "sql", "commit", "release"]
    else:
        with pytest.raises(RuntimeError, match="index DDL failed") as error:
            await handler._apply_actions(coco_env.context_provider, actions)
        assert error.value is failure
        assert events == ["begin", *(["sql"] * fail_at), "rollback", "release"]
    assert statements[0] == 'DROP INDEX IF EXISTS "tenant"."documents__vector__old"'
    if fail_at is None or fail_at >= 2:
        assert statements[1] == (
            'DROP INDEX IF EXISTS "tenant"."documents__vector__replacement"'
        )
    if fail_at is None or fail_at == 3:
        assert statements[2] == (
            'CREATE INDEX "documents__vector__replacement" ON "tenant"."documents" '
            'USING hnsw ("embedding" vector_cosine_ops) WITH (m = 16)'
        )
