"""Optional end-to-end check for Postgres-backed internal state.

The Rust contract tests cover state semantics. This test verifies that the
Python `Settings` URL reaches the compiled engine and opens an environment
when a test database is available.
"""

import asyncio
import os

import pytest
from cocoindex._internal import core
from cocoindex._internal.environment import Environment
from cocoindex._internal.setting import Settings


@pytest.mark.skipif(
    not os.getenv("COCOINDEX_TEST_POSTGRES_URL"),
    reason="COCOINDEX_TEST_POSTGRES_URL is not set",
)
def test_postgres_url_opens_environment() -> None:
    url = os.environ["COCOINDEX_TEST_POSTGRES_URL"]
    loop = asyncio.new_event_loop()
    try:
        env = Environment(Settings(db_path=url), event_loop=loop)
        assert core.list_app_names(env._core_env) == []
    finally:
        if loop.is_running():
            loop.call_soon_threadsafe(loop.stop)
        else:
            loop.close()
