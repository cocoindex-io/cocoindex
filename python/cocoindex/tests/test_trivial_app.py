import pytest

import cocoindex
from .environment import create_test_env

_env = create_test_env("trivial_app")


@cocoindex.function()
def trivial_fn_sync(csp: cocoindex.StatePath, s: str, i: int) -> str:
    return f"{s} {i}"


@cocoindex.function
def trivial_fn_sync_bare(csp: cocoindex.StatePath, s: str, i: int) -> str:
    return f"{s} {i}"


@pytest.mark.asyncio
async def test_trivial_app_sync() -> None:
    app = cocoindex.App(
        trivial_fn_sync,
        cocoindex.AppConfig(name="trivial_app_sync", environment=_env),
        "Hello app_sync",
        1,
    )
    assert app.update() == "Hello app_sync 1"
    assert await app.update_async() == "Hello app_sync 1"


@pytest.mark.asyncio
async def test_trivial_app_sync_bare() -> None:
    app = cocoindex.App(
        trivial_fn_sync_bare,
        cocoindex.AppConfig(name="trivial_app_sync_bare", environment=_env),
        "Hello app_sync_bare",
        2,
    )
    assert app.update() == "Hello app_sync_bare 2"
    assert await app.update_async() == "Hello app_sync_bare 2"


@cocoindex.function()
async def trivial_fn_async(csp: cocoindex.StatePath, s: str, i: int) -> str:
    return f"{s} {i}"


@cocoindex.function
async def trivial_fn_async_bare(csp: cocoindex.StatePath, s: str, i: int) -> str:
    return f"{s} {i}"


@pytest.mark.asyncio
async def test_trivial_app_async() -> None:
    app = cocoindex.App(
        trivial_fn_async,
        cocoindex.AppConfig(name="trivial_app_async", environment=_env),
        "Hello app_async",
        3,
    )
    assert app.update() == "Hello app_async 3"
    assert await app.update_async() == "Hello app_async 3"


@pytest.mark.asyncio
async def test_trivial_app_async_bare() -> None:
    app = cocoindex.App(
        trivial_fn_async_bare,
        cocoindex.AppConfig(name="trivial_app_async_bare", environment=_env),
        "Hello app_async_bare",
        4,
    )
    assert app.update() == "Hello app_async_bare 4"
    assert await app.update_async() == "Hello app_async_bare 4"
