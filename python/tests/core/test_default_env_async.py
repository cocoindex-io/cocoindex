from contextlib import asynccontextmanager
import pytest
from typing import Iterator, AsyncIterator

import cocoindex as coco
from cocoindex._internal.environment import reset_default_env_for_tests
from tests.common import get_env_db_path

_env_db_path = get_env_db_path("_async_default")


class _Resource:
    def __init__(self, generation: int) -> None:
        self.generation = generation
        self.closed = False


_RESOURCE_KEY = coco.ContextKey[_Resource]("test_default_env_async/resource")

_num_active_resources = 0
_num_resource_generations = 0


@asynccontextmanager
async def _acquire_resource() -> AsyncIterator[_Resource]:
    global _num_active_resources, _num_resource_generations
    _num_active_resources += 1
    _num_resource_generations += 1
    resource = _Resource(_num_resource_generations)
    try:
        yield resource
    finally:
        resource.closed = True
        _num_active_resources -= 1


@pytest.fixture(scope="module")
def _default_async_env() -> Iterator[None]:
    try:

        @coco.lifespan
        async def default_lifespan(
            builder: coco.EnvironmentBuilder,
        ) -> AsyncIterator[None]:
            builder.settings.db_path = _env_db_path
            await builder.provide_async_with(_RESOURCE_KEY, _acquire_resource())
            yield

        yield
    finally:
        reset_default_env_for_tests()


@pytest.mark.asyncio
async def test_async_default_env(_default_async_env: None) -> None:
    assert not _env_db_path.exists()
    async with coco.runtime():
        await coco.default_env()
    assert _env_db_path.exists()


@coco.fn.as_async()
async def trivial_fn(s: str, i: int) -> str:
    resource = coco.use_context(_RESOURCE_KEY)
    assert isinstance(resource, _Resource)
    assert not resource.closed
    return f"{s} {i}"


@coco.fn.as_async()
async def resource_generation() -> int:
    resource = coco.use_context(_RESOURCE_KEY)
    assert not resource.closed
    return resource.generation


@pytest.mark.asyncio
async def test_async_app(_default_async_env: None) -> None:
    app = coco.App(
        coco.AppConfig(name="trivial_app"),
        trivial_fn,
        "Hello",
        1,
    )

    assert _num_active_resources == 0
    async with coco.runtime():
        assert await app.update() == "Hello 1"
        assert _num_active_resources == 1
    assert _num_active_resources == 0


@pytest.mark.asyncio
async def test_async_app_implicit_startup(_default_async_env: None) -> None:
    app = coco.App(
        coco.AppConfig(name="trivial_app_implicit_startup"),
        trivial_fn,
        "Hello",
        1,
    )

    assert _num_active_resources == 0
    try:
        assert await app.update() == "Hello 1"
        assert _num_active_resources == 1
    finally:
        await coco.stop()


@pytest.mark.asyncio
async def test_retained_async_app_across_runtime_cycles(
    _default_async_env: None,
) -> None:
    app = coco.App("retained_async_app", resource_generation)
    generations = []

    for _ in range(3):
        async with coco.runtime():
            generations.append(await app.update())
            assert _num_active_resources == 1
        assert _num_active_resources == 0

    assert generations[1] == generations[0] + 1
    assert generations[2] == generations[1] + 1


@pytest.mark.asyncio
async def test_retained_async_app_implicitly_restarts_after_stop(
    _default_async_env: None,
) -> None:
    app = coco.App("retained_async_implicit_restart", resource_generation)

    async with coco.runtime():
        first_generation = await app.update()

    second_generation = await app.update()
    try:
        assert second_generation == first_generation + 1
        assert _num_active_resources == 1
    finally:
        await coco.stop()


@pytest.mark.asyncio
async def test_async_stop_edge_cases_and_registration_survive_restart(
    _default_async_env: None,
) -> None:
    await coco.stop()
    await coco.stop()

    app = coco.App("retained_async_registration", resource_generation)
    never_initialized = coco.App(
        "retained_async_never_initialized", resource_generation
    )
    async with coco.runtime():
        await app.update()

    with pytest.raises(ValueError, match="already registered"):
        coco.App("retained_async_registration", resource_generation)

    async with coco.runtime():
        app_generation = await app.update()
        generation = await never_initialized.update()
        assert app_generation == generation
        assert generation > 0
