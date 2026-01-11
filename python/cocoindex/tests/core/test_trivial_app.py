import pytest

import cocoindex as coco
import cocoindex.asyncio as coco_aio
from ..common import create_test_env

coco_env = create_test_env(__file__)

# === Sync App ===


@coco.function()
def trivial_fn_sync(scope: coco.Scope, s: str, i: int) -> str:
    return f"{s} {i}"


def test_sync_app_sync_client() -> None:
    app = coco.App(
        trivial_fn_sync,
        coco.AppConfig(name="sync_app_sync_client", environment=coco_env),
        "Hello sync_app",
        1,
    )
    assert app.run() == "Hello sync_app 1"


@pytest.mark.asyncio
async def test_sync_app_async_client() -> None:
    app = coco_aio.App(
        trivial_fn_sync,
        coco.AppConfig(name="sync_app_async_client", environment=coco_env),
        "Hello sync_app",
        1,
    )
    assert await app.run() == "Hello sync_app 1"


# === Sync Bare App ===


def trivial_fn_sync_bare(scope: coco.Scope, s: str, i: int) -> str:
    return f"{s} {i}"


def test_sync_bare_app_sync_client() -> None:
    app = coco.App(
        trivial_fn_sync_bare,
        coco.AppConfig(name="sync_bare_app_sync_client", environment=coco_env),
        "Hello sync_bare_app",
        2,
    )
    assert app.run() == "Hello sync_bare_app 2"


@pytest.mark.asyncio
async def test_sync_bare_app_async_client() -> None:
    app = coco_aio.App(
        trivial_fn_sync_bare,
        coco.AppConfig(name="sync_bare_app_async_client", environment=coco_env),
        "Hello sync_bare_app",
        2,
    )
    assert await app.run() == "Hello sync_bare_app 2"


# === Async App ===


@coco.function()
async def trivial_fn_async(scope: coco.Scope, s: str, i: int) -> str:
    return f"{s} {i}"


def test_async_app_sync_client() -> None:
    app = coco.App(
        trivial_fn_async,
        coco.AppConfig(name="async_app_sync_client", environment=coco_env),
        "Hello async_app",
        3,
    )
    assert app.run() == "Hello async_app 3"


@pytest.mark.asyncio
async def test_async_app_async_client() -> None:
    app = coco_aio.App(
        trivial_fn_async,
        coco.AppConfig(name="async_app_async_client", environment=coco_env),
        "Hello async_app",
        3,
    )
    assert await app.run() == "Hello async_app 3"


# === Async Bare App ===


def trivial_fn_async_bare(scope: coco.Scope, s: str, i: int) -> str:
    return f"{s} {i}"


def test_async_bare_app_sync_client() -> None:
    app = coco.App(
        trivial_fn_async_bare,
        coco.AppConfig(name="async_app_sync_client", environment=coco_env),
        "Hello async_app",
        3,
    )
    assert app.run() == "Hello async_app 3"


@pytest.mark.asyncio
async def test_async_bare_app_async_client() -> None:
    app = coco_aio.App(
        trivial_fn_async_bare,
        coco.AppConfig(name="async_app_async_client", environment=coco_env),
        "Hello async_app",
        3,
    )
    assert await app.run() == "Hello async_app 3"


# === Apps from Member Functions ===


class MyApp:
    def sync_main(self, scope: coco.Scope, s: str, i: int) -> str:
        return f"Hello MyApp.sync_main: {s} {i}"

    async def async_main(self, scope: coco.Scope, s: str, i: int) -> str:
        return f"Hello MyApp.async_main: {s} {i}"


def test_sync_from_member_fn_app() -> None:
    my_app = MyApp()
    app = coco.App(
        my_app.sync_main,
        coco.AppConfig(name="sync_from_member_fn_app", environment=coco_env),
        "Hello sync_from_member_fn_app",
        4,
    )
    assert app.run() == "Hello MyApp.sync_main: Hello sync_from_member_fn_app 4"


@pytest.mark.asyncio
async def test_async_from_member_fn_app() -> None:
    my_app = MyApp()
    app = coco_aio.App(
        my_app.async_main,
        coco.AppConfig(name="async_from_member_fn_app", environment=coco_env),
        "Hello async_from_member_fn_app",
        4,
    )
    assert await app.run() == "Hello MyApp.async_main: Hello async_from_member_fn_app 4"
