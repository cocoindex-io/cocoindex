import pytest

import cocoindex as coco
import cocoindex.aio as coco_aio
from .common import create_test_env

coco_env = create_test_env(__file__)

# === Sync App ===


@coco.function()
def trivial_fn_sync(csp: coco.StatePath, s: str, i: int) -> str:
    return f"{s} {i}"


def test_sync_app_sync_client() -> None:
    app = coco.App("sync_app_sync_client", trivial_fn_sync, environment=coco_env)
    assert app.run("Hello sync_app", 1) == "Hello sync_app 1"


@pytest.mark.asyncio
async def test_sync_app_async_client() -> None:
    app = coco_aio.App("sync_app_async_client", trivial_fn_sync, environment=coco_env)
    assert await app.run("Hello sync_app", 1) == "Hello sync_app 1"


# === Sync Bare App ===


@coco.function
def trivial_fn_sync_bare(csp: coco.StatePath, s: str, i: int) -> str:
    return f"{s} {i}"


def test_sync_bare_app_sync_client() -> None:
    app = coco.App(
        "sync_bare_app_sync_client", trivial_fn_sync_bare, environment=coco_env
    )
    assert app.run("Hello sync_bare_app", 2) == "Hello sync_bare_app 2"


@pytest.mark.asyncio
async def test_sync_bare_app_async_client() -> None:
    app = coco_aio.App(
        "sync_bare_app_async_client", trivial_fn_sync_bare, environment=coco_env
    )
    assert await app.run("Hello sync_bare_app", 2) == "Hello sync_bare_app 2"


# === Async App ===


@coco.function()
async def trivial_fn_async(csp: coco.StatePath, s: str, i: int) -> str:
    return f"{s} {i}"


def test_async_app_sync_client() -> None:
    app = coco.App("async_app_sync_client", trivial_fn_async, environment=coco_env)
    assert app.run("Hello async_app", 3) == "Hello async_app 3"


@pytest.mark.asyncio
async def test_async_app_async_client() -> None:
    app = coco_aio.App("async_app_async_client", trivial_fn_async, environment=coco_env)
    assert await app.run("Hello async_app", 3) == "Hello async_app 3"
