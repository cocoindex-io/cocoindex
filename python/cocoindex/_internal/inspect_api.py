import asyncio
from typing import Any

from . import core
from .app import AppBase
from .stable_path import StablePath


async def list_stable_paths(app: AppBase[Any, Any]) -> list[StablePath]:
    core_app = await app._get_core()
    return [StablePath(path) for path in core.list_stable_paths(core_app)]


def list_stable_paths_sync(app: AppBase[Any, Any]) -> list[StablePath]:
    return asyncio.run(list_stable_paths(app))


__all__ = [
    "list_stable_paths",
    "list_stable_paths_sync",
]
