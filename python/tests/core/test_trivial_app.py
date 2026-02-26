import pytest

import cocoindex as coco
from tests.common import create_test_env

coco_env = create_test_env(__file__)


@coco.fn()
def trivial_fn_sync(s: str, i: int) -> str:
    return f"{s} {i}"


@pytest.mark.asyncio
async def test_sync_app() -> None:
    app = coco.App(
        coco.AppConfig(name="sync_app", environment=coco_env),
        trivial_fn_sync,
        "Hello sync_app",
        1,
    )
    assert await app.update() == "Hello sync_app 1"


def trivial_fn_sync_bare(s: str, i: int) -> str:
    return f"{s} {i}"


@pytest.mark.asyncio
async def test_sync_bare_app() -> None:
    app = coco.App(
        coco.AppConfig(name="sync_bare_app", environment=coco_env),
        trivial_fn_sync_bare,
        "Hello sync_bare_app",
        2,
    )
    assert await app.update() == "Hello sync_bare_app 2"


@coco.fn()
async def trivial_fn_async(s: str, i: int) -> str:
    return f"{s} {i}"


@pytest.mark.asyncio
async def test_async_app() -> None:
    app = coco.App(
        coco.AppConfig(name="async_app", environment=coco_env),
        trivial_fn_async,
        "Hello async_app",
        3,
    )
    assert await app.update() == "Hello async_app 3"


@coco.fn.as_async()
def trivial_fn_async_wrapped(s: str, i: int) -> str:
    return f"{s} {i}"


@pytest.mark.asyncio
async def test_async_wrapped_app() -> None:
    app = coco.App(
        coco.AppConfig(name="async_wrapped_app", environment=coco_env),
        trivial_fn_async_wrapped,
        "Hello async_app",
        3,
    )
    assert await app.update() == "Hello async_app 3"


def test_async_wrapped_app_blocking() -> None:
    app = coco.App(
        coco.AppConfig(name="async_wrapped_app_blocking", environment=coco_env),
        trivial_fn_async_wrapped,
        "Hello async_app",
        3,
    )
    assert app.update_blocking() == "Hello async_app 3"


def trivial_fn_async_bare(s: str, i: int) -> str:
    return f"{s} {i}"


@pytest.mark.asyncio
async def test_async_bare_app() -> None:
    app = coco.App(
        coco.AppConfig(name="async_bare_app", environment=coco_env),
        trivial_fn_async_bare,
        "Hello async_app",
        3,
    )
    assert await app.update() == "Hello async_app 3"


class MyApp:
    def sync_main(self, s: str, i: int) -> str:
        return f"Hello MyApp.sync_main: {s} {i}"

    async def async_main(self, s: str, i: int) -> str:
        return f"Hello MyApp.async_main: {s} {i}"


@pytest.mark.asyncio
async def test_member_fn_app() -> None:
    my_app = MyApp()
    app = coco.App(
        coco.AppConfig(name="member_fn_app", environment=coco_env),
        my_app.async_main,
        "Hello member_fn_app",
        4,
    )
    assert await app.update() == "Hello MyApp.async_main: Hello member_fn_app 4"
