import asyncio
from typing import Any, AsyncIterator

from . import core
from .app import AppBase
from .stable_path import StablePath


async def list_stable_paths(app: AppBase[Any, Any]) -> list[StablePath]:
    core_app = await app._get_core()
    return [StablePath(path) for path in core.list_stable_paths(core_app)]


async def iter_stable_paths_with_types(
    app: AppBase[Any, Any],
) -> AsyncIterator[core.StablePathWithType]:
    """
    Async iterator of stable paths with their node types (no buffering).

    Yields both:
    - component nodes (node_type=StablePathNodeType.component)
    - intermediate directory nodes (node_type=StablePathNodeType.directory)
    """
    core_app = await app._get_core()
    async for item in core.iter_stable_paths_with_types(core_app):
        yield item


def list_stable_paths_sync(app: AppBase[Any, Any]) -> list[StablePath]:
    return asyncio.run(list_stable_paths(app))


async def _list_stable_paths_with_types_collected(
    app: AppBase[Any, Any],
) -> list[core.StablePathWithType]:
    return [item async for item in iter_stable_paths_with_types(app)]


def list_stable_paths_with_types_sync(
    app: AppBase[Any, Any],
) -> list[core.StablePathWithType]:
    return asyncio.run(_list_stable_paths_with_types_collected(app))


__all__ = [
    "iter_stable_paths_with_types",
    "list_stable_paths",
    "list_stable_paths_sync",
    "list_stable_paths_with_types_sync",
]
