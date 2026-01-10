from contextlib import asynccontextmanager
import pytest
from typing import Iterator, AsyncIterator

import cocoindex.asyncio as coco_aio
from cocoindex._internal.environment import reset_default_lifespan_for_tests
from ..common import get_env_db_path

_env_db_path = get_env_db_path("_async_default")


class _Resource:
    pass


_RESOURCE_KEY = coco_aio.ContextKey[_Resource]("test_default_env_async/resource")

_num_active_resources = 0


@asynccontextmanager
async def _acquire_resource() -> AsyncIterator[_Resource]:
    global _num_active_resources
    _num_active_resources += 1
    yield _Resource()
    _num_active_resources -= 1


@pytest.fixture(scope="module")
def _default_async_env() -> Iterator[None]:
    try:

        @coco_aio.lifespan
        async def default_lifespan(
            builder: coco_aio.EnvironmentBuilder,
        ) -> AsyncIterator[None]:
            builder.settings.db_path = _env_db_path
            await builder.provide_async_with(_RESOURCE_KEY, _acquire_resource())
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
    assert isinstance(_scope.use(_RESOURCE_KEY), _Resource)
    return f"{s} {i}"


@pytest.mark.asyncio
async def test_async_app_in_default_env(_default_async_env: None) -> None:
    app = coco_aio.App(
        trivial_fn,
        coco_aio.AppConfig(name="trivial_app"),
        "Hello",
        1,
    )

    assert _num_active_resources == 0
    async with coco_aio.runtime():
        assert await app.run() == "Hello 1"
        assert _num_active_resources == 1
    assert _num_active_resources == 0
