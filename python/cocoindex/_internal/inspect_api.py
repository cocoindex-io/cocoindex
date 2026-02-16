import asyncio
from typing import Any, AsyncIterator

from . import core
from .app import AppBase
from .stable_path import StablePath


async def list_stable_paths(app: AppBase[Any, Any]) -> list[StablePath]:
    """Convenience: collect paths from iter_stable_paths (delegates to new API)."""
    return [StablePath(item.path) async for item in iter_stable_paths(app)]


async def iter_stable_paths(
    app: AppBase[Any, Any],
) -> AsyncIterator[core.StablePathInfo]:
    """
    Async iterator of stable paths with metadata (e.g. node type; no buffering).

    Yields both:
    - component nodes (node_type=StablePathNodeType.component)
    - intermediate directory nodes (node_type=StablePathNodeType.directory)
    """
    core_app = await app._get_core()
    async for item in core.iter_stable_paths(core_app):
        yield item


def list_stable_paths_sync(app: AppBase[Any, Any]) -> list[StablePath]:
    return asyncio.run(list_stable_paths(app))


async def _iter_stable_paths_collected(
    app: AppBase[Any, Any],
) -> list[core.StablePathInfo]:
    return [item async for item in iter_stable_paths(app)]


def list_stable_paths_info_sync(
    app: AppBase[Any, Any],
) -> list[core.StablePathInfo]:
    return asyncio.run(_iter_stable_paths_collected(app))


__all__ = [
    "iter_stable_paths",
    "list_stable_paths",
    "list_stable_paths_info_sync",
    "list_stable_paths_sync",
]
