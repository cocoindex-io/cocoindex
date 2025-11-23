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
