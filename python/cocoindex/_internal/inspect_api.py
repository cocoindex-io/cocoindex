import asyncio
from typing import Any

from . import core
from .app import AppBase
from .stable_path import StablePath


async def list_stable_paths(app: AppBase[Any, Any]) -> list[StablePath]:
    core_app = await app._get_core()
    return [StablePath(path) for path in core.list_stable_paths(core_app)]


async def list_stable_paths_with_types(
    app: AppBase[Any, Any],
) -> list[tuple[StablePath, bool]]:
    """
    Return stable paths along with whether each node is a mounted component.

    The returned list includes both:
    - component nodes (is_component=True)
    - intermediate directory nodes (is_component=False)
    """
    core_app = await app._get_core()
    items = core.list_stable_paths_with_types(core_app)
    return [(StablePath(path), is_component) for (path, is_component) in items]


def list_stable_paths_sync(app: AppBase[Any, Any]) -> list[StablePath]:
    return asyncio.run(list_stable_paths(app))


def list_stable_paths_with_types_sync(
    app: AppBase[Any, Any],
) -> list[tuple[StablePath, bool]]:
    return asyncio.run(list_stable_paths_with_types(app))


__all__ = [
    "list_stable_paths",
    "list_stable_paths_sync",
    "list_stable_paths_with_types",
    "list_stable_paths_with_types_sync",
]
