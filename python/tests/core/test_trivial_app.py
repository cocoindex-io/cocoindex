import pytest

import cocoindex as coco
import cocoindex.asyncio as coco_aio
from tests.common import create_test_env

coco_env = create_test_env(__file__)

# === Sync App ===


@coco.function()
def trivial_fn_sync(s: str, i: int) -> str:
    return f"{s} {i}"


def test_sync_app_sync_client() -> None:
    app = coco.App(
        coco.AppConfig(name="sync_app_sync_client", environment=coco_env),
        trivial_fn_sync,
        "Hello sync_app",
        1,
    )
    assert app.update() == "Hello sync_app 1"


@pytest.mark.asyncio
async def test_sync_app_async_client() -> None:
    app = coco_aio.App(
        coco.AppConfig(name="sync_app_async_client", environment=coco_env),
        trivial_fn_sync,
        "Hello sync_app",
        1,
    )
    assert await app.update() == "Hello sync_app 1"


# === Sync Bare App ===


def trivial_fn_sync_bare(s: str, i: int) -> str:
    return f"{s} {i}"


def test_sync_bare_app_sync_client() -> None:
    app = coco.App(
        coco.AppConfig(name="sync_bare_app_sync_client", environment=coco_env),
        trivial_fn_sync_bare,
        "Hello sync_bare_app",
        2,
    )
    assert app.update() == "Hello sync_bare_app 2"


@pytest.mark.asyncio
async def test_sync_bare_app_async_client() -> None:
    app = coco_aio.App(
        coco.AppConfig(name="sync_bare_app_async_client", environment=coco_env),
        trivial_fn_sync_bare,
        "Hello sync_bare_app",
        2,
    )
    assert await app.update() == "Hello sync_bare_app 2"


# === Async App ===


@coco.function()
async def trivial_fn_async(s: str, i: int) -> str:
    return f"{s} {i}"


def test_async_app_sync_client() -> None:
    app = coco.App(
        coco.AppConfig(name="async_app_sync_client", environment=coco_env),
        trivial_fn_async,
        "Hello async_app",
        3,
    )
    assert app.update() == "Hello async_app 3"


@pytest.mark.asyncio
async def test_async_app_async_client() -> None:
    app = coco_aio.App(
        coco.AppConfig(name="async_app_async_client", environment=coco_env),
        trivial_fn_async,
        "Hello async_app",
        3,
    )
    assert await app.update() == "Hello async_app 3"


# --- Async App by Wrapping Sync Function ---


@coco_aio.function()
def trivial_fn_async_wrapped(s: str, i: int) -> str:
    return f"{s} {i}"


@pytest.mark.asyncio
async def test_async_app_async_client_wrapped() -> None:
    app = coco_aio.App(
        coco.AppConfig(name="async_app_async_client_wrapped", environment=coco_env),
        trivial_fn_async_wrapped,
        "Hello async_app",
        3,
    )
    assert await app.update() == "Hello async_app 3"


def test_async_app_async_client_wrapped_sync() -> None:
    app = coco.App(
        coco.AppConfig(
            name="async_app_async_client_wrapped_sync", environment=coco_env
        ),
        trivial_fn_async_wrapped,
        "Hello async_app",
        3,
    )
    assert app.update() == "Hello async_app 3"


# === Async Bare App ===


def trivial_fn_async_bare(s: str, i: int) -> str:
    return f"{s} {i}"


def test_async_bare_app_sync_client() -> None:
    app = coco.App(
        coco.AppConfig(name="async_app_sync_client", environment=coco_env),
        trivial_fn_async_bare,
        "Hello async_app",
        3,
    )
    assert app.update() == "Hello async_app 3"


@pytest.mark.asyncio
async def test_async_bare_app_async_client() -> None:
    app = coco_aio.App(
        coco.AppConfig(name="async_app_async_client", environment=coco_env),
        trivial_fn_async_bare,
        "Hello async_app",
        3,
    )
    assert await app.update() == "Hello async_app 3"


# === Apps from Member Functions ===


class MyApp:
    def sync_main(self, s: str, i: int) -> str:
        return f"Hello MyApp.sync_main: {s} {i}"

    async def async_main(self, s: str, i: int) -> str:
        return f"Hello MyApp.async_main: {s} {i}"


def test_sync_from_member_fn_app() -> None:
    my_app = MyApp()
    app = coco.App(
        coco.AppConfig(name="sync_from_member_fn_app", environment=coco_env),
        my_app.sync_main,
        "Hello sync_from_member_fn_app",
        4,
    )
    assert app.update() == "Hello MyApp.sync_main: Hello sync_from_member_fn_app 4"


@pytest.mark.asyncio
async def test_async_from_member_fn_app() -> None:
    my_app = MyApp()
    app = coco_aio.App(
        coco.AppConfig(name="async_from_member_fn_app", environment=coco_env),
        my_app.async_main,
        "Hello async_from_member_fn_app",
        4,
    )
    assert (
        await app.update() == "Hello MyApp.async_main: Hello async_from_member_fn_app 4"
    )
