import pytest
from typing import Iterator, AsyncIterator

import cocoindex.aio as coco_aio
from cocoindex._internal.environment import reset_default_lifespan_for_tests
from .common import get_env_db_path

_env_db_path = get_env_db_path("_async_default")


@pytest.fixture(scope="module")
def _default_async_env() -> Iterator[None]:
    try:

        @coco_aio.lifespan
        async def default_lifespan(
            builder: coco_aio.EnvironmentBuilder,
        ) -> AsyncIterator[None]:
            builder.settings.db_path = _env_db_path
            yield

        yield
    finally:
        reset_default_lifespan_for_tests()


@pytest.mark.asyncio
async def test_async_default_env(_default_async_env: None) -> None:
    assert not _env_db_path.exists()
    async with coco_aio.runtime():
        await coco_aio.default_env()
    assert _env_db_path.exists()


@coco_aio.function()
async def trivial_fn(_scope: coco_aio.Scope, s: str, i: int) -> str:
    return f"{s} {i}"


@pytest.mark.asyncio
async def test_async_app_in_default_env(_default_async_env: None) -> None:
    app = coco_aio.App(
        trivial_fn,
        coco_aio.AppConfig(name="trivial_app"),
        "Hello",
        1,
    )
    async with coco_aio.runtime():
        assert await app.run() == "Hello 1"
