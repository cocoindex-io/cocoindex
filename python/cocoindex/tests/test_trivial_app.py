import pytest

import cocoindex
from .environment import create_test_env

coco_env = create_test_env("trivial_app")

# === Sync App ===


@cocoindex.function()
def trivial_fn_sync(csp: cocoindex.StatePath, s: str, i: int) -> str:
    return f"{s} {i}"


sync_app = cocoindex.App(
    trivial_fn_sync,
    cocoindex.AppConfig(name="sync_app", environment=coco_env),
    "Hello sync_app",
    1,
)


def test_sync_app_sync_update() -> None:
    assert sync_app.update() == "Hello sync_app 1"


@pytest.mark.asyncio
async def test_sync_app_async_update() -> None:
    assert await sync_app.update_async() == "Hello sync_app 1"


# === Sync Bare App ===


@cocoindex.function
def trivial_fn_sync_bare(csp: cocoindex.StatePath, s: str, i: int) -> str:
    return f"{s} {i}"


sync_bare_app = cocoindex.App(
    trivial_fn_sync_bare,
    cocoindex.AppConfig(name="sync_bare_app", environment=coco_env),
    "Hello sync_bare_app",
    2,
)


def test_sync_bare_app_sync_update() -> None:
    assert sync_bare_app.update() == "Hello sync_bare_app 2"


@pytest.mark.asyncio
async def test_sync_bare_app_async_update() -> None:
    assert await sync_bare_app.update_async() == "Hello sync_bare_app 2"


# === Async App ===


@cocoindex.function()
async def trivial_fn_async(csp: cocoindex.StatePath, s: str, i: int) -> str:
    return f"{s} {i}"


async_app = cocoindex.App(
    trivial_fn_async,
    cocoindex.AppConfig(name="async_app", environment=coco_env),
    "Hello async_app",
    3,
)


def test_async_app_sync_update() -> None:
    assert async_app.update() == "Hello async_app 3"


@pytest.mark.asyncio
async def test_async_app_async_update() -> None:
    assert await async_app.update_async() == "Hello async_app 3"
